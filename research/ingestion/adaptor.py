
import ccxt.async_support as ccxt
from typing import List
import logging
import asyncio
from datetime import datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..shared import OHLCV, FetchJob, Ok, Err, Result


logger = logging.getLogger(__name__)

class CCXTAsyncAdaptor:
    """Kita akan implementasi advanced pagination dan rate limiting -> 
    pradictable pagination, clear progress, smart error handling yang sudah kita buat di shared.
    Dibawah adalah Concrete Method yang kita butuhkan"""

    def __init__(self, exchange_id: str ="binance") -> None:
        """ ini adalah Dunder Method yang menginisiasi/ memberikan value pada object Kita"""
        self.exchange_id = exchange_id
        try:
            self.exchange = getattr(ccxt, exchange_id)({
                'enableRateLimit': True,
                'timeout': 30000, # 30000 itu sama dengan 30 detik
                'options': {'defaultType': 'spot'},
            })
            self._markets_loaded = False
            self._rate_limit_semaphore = asyncio.Semaphore(10) # Ini untuk Concurrency control
            logger.info(f"CCXT Adaptor Initialized for {exchange_id}")
        except AttributeError:
            raise ValueError(f"Exchange '{exchange_id}' tidak ditemukan di CCTX")

    async def ensure_connections(self) -> None:
        """method yang bertanggung jawab untuk load data market 
        dan hanya akan melakukan load ketika dibutuhkan"""
        if not self._markets_loaded:
            try:
                await self.exchange.load_markets()
                self._markets_loaded = True
                logger.debug(f"Market Load for {self.exchange_id}")
            except Exception as e:
                logger.error(f"Failed to load markets : {e}")
                raise

    async def close(self) -> None:
        """Method yang bertanggung jawab menutup koneksi dan membersihkannya"""
        try:
            if hasattr(self.exchange, 'close'):
                await self.exchange.close()
                logger.debug(f"Error closing exchange {self.exchange_id} closed")
        except Exception as e:
            logger.warning(f"Error closing exchange: {e}")

    async def fetch(self, job: 'FetchJob') -> 'Result[List[OHLCV], str]':
        """ method yang akan membagi dan memecah ini menjadi bagian bagian kecil dengan rate limiting dan progres tracking
        -> ini membuat langkah menjadi jelas dan mencegah infinit loops"""

        validation_result = self._validate_job(job)
        if validation_result.is_err():
            return validation_result

        connection_result = self._safe_load_markets()
        if connection_result.is_err():
            return Err(f"Connection Failed: {connection_result.error}")

        cursor_result = self._setup_cursor(job)
        if cursor_result.is_err():
            return cursor_result

        start_ms, end_ms = cursor_result.unwrap()

        if start_ms >= end_ms:
            return Err("Start date setelah end date")

        all_candles: List['OHLCV'] = []
        page_count = 0
        max_pages = 5000

        logger.info(f"Start pagination for {job.symbol} from {datetime.fromtimestamp(start_ms/1000)}")

        current_cursor = start_ms

        while current_cursor < end_ms and page_count <= max_pages:

            async with self._rate_limit_semaphore:
                page_result = await self._fetch_page(job, current_cursor)
                
                if page_result.is_err():
                    logger.warning(f"Page fetch error: {page_result.error}")
                    if all_candles:
                        return Ok(all_candles)
                    return page_result
                 
                page_candles, next_cursor = page_result.unwrap()
                
                if not page_candles:
                    break
            
                all_candles.extend(page_candles)

                if next_cursor <= current_cursor:
                    logger.warning(f"cursor stuck at {current_cursor}, breaking loop")
                    break

                current_cursor = next_cursor
                page_count += 1

                self._log_progress(page_count, len(all_candles), job.symbol)


        if page_count >= max_pages:
            logger.warning(f"reached max pages ({max_pages} for ({page_count}) pages")
            return Ok(all_candles)

        if not all_candles:
            return Err(f"No valid data fetch for {job.symbol}")

        logger.info(f"Fetcher selesai: {len(all_candles)} didalam {page_count} pages")
        return Ok(all_candles)

    def _validate_job(self, job: 'FetchJob') -> 'Result[List[OHLCV]]':
        """ Validasi Job: return jika invalid"""
        from ..shared import Ok, Err

        try:
            if not hasattr(job, 'symbol') or not job.symbol:
                return Err("Job missing symbol")
            
            if not hasattr(job, 'timeframe'):
                return Err("Job missing timedate")

            if not hasattr(job, 'start_date'):
                return Err("Job missing start date")

            return Ok(None)

        except Exception as e:
            return Err(f"Job Validation error: {e}")

    def _safe_load_markets(self) -> 'Result[None, str]':
        """ Load market dengan Result pattern yang sudah buat di shared"""
        from ..shared import Ok, Err

        try:
            if not self._markets_loaded:
                await self.exchange.load_markets()
                self._markets_loaded = True
            return Ok(None)
        except Exception as e:
            return Err(f"failed untuk load markets: {e}")

    def _setup_cursor(self, job: 'FetchJob') -> 'Result[tuple[int, int], str]':
        """Setup pagination cursors dengan result pattern"""
        from ..shared import Ok, Err

        try:
            start_ms = int(job.start_date.timestamp() * 1000)
            end_ms = int(job.end_date.timestamp() * 1000) if job.end_date else int(datetime.now(). timestamp() * 1000)
            return Ok((start_ms, end_ms))
        except Exception as e:
            return Err(f" Failed to setup cursor: {e}")

    def _fetch_page(
        self,
        job: FetchJob,
        cursor_ms: int
    ) -> 'Result[tuple[List[OHLCV], int], str]':
        from ..shared import Ok, Err, OHLCV

        max_retries = 3
        last_exception = None

        for attempt in range(max_retries):
            try:
                batch = await self.exchange.fetch_ohlcv(
                    symbol=job.symbol,
                    timeframe=job.timeframe,
                    since=cursor_ms,
                    limit=1000
                )
                if not batch:
                    return Ok(([], cursor_ms))

                parsed_candles = []
                last_timestamp = cursor_ms

                for row in batch:
                    try:
                        candle = OHLCV(
                            timestamp=int(row[0]),
                            open=float(row[1]),
                            high=float(row[2]),
                            low=float(row[3]),
                            close=float(row[4]),
                            volume=float(row[5])
                        )
                        parsed_candles.append(candle)
                        last_timestamp = max(last_timestamp, candle.timestamp)
                    except Exception as parse_err:
                        logger.debug(f"Skipping Invalid row: {parse_err}")
                        continue
                if not parsed_candles:
                    return Ok([], cursor_ms)

                next_cursor = last_timestamp + 1
                return Ok((parsed_candles, next_cursor))

            except ccxt.RateLimitExceeded as e:
                wait_time = 2 ** attempt
                logger.warning(f"Rate Limit hit, waiting {wait_time}s")
                await asyncio.sleep(wait_time)
                last_exception = e
            
            except ccxt.NetworkError as e:
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt
                    logger.warning(f" Network Error, retry in {wait_time}s: {e}")
                    await asyncio.sleep(wait_time)
                    last_exception = e
                else:
                    return Err(f"Exchange Error:{e}")
            
            except Exception as e:
                logger.error(f"Unexpected error in page fetch: {e}")
                return Err(f"Unexpected error {e}")

        return Err(f"Rate Limit consistently exceeded: {last_exception}")

    def _log_progress(self, page_count: int, total_rows: int, symbol: str) -> None:
        """Progres logging yang tidak spammy"""
        if page_count % 20 == 0:
            logger.info(f"{symbol}: page {page_count}, total rows: {total_rows}")
        elif total_rows % 1000 == 0:
            logger.info(f"{symbol}: Collected {total_rows} rows ...")



    async def check_symbol(self, symbol: str) -> 'Result[bool, str]':
        """Cek apakah simbol tersedia di exchange"""
        from ..shared import Ok, Err

        try:
            await self.ensure_connections()
            return Ok(symbol in self.exchange.markets)
        except Exception as e:
            return Err(f" Failed to check symbol: {e}")
    
    async def ger_timeframes(self) -> Result[dict, str]:
        """Dapatkan TF yang tersedia"""
        from ..shared import Ok, Err

        try:
            await self.ensure_connections()
            return Ok(self.exchange.get_timeframes or {})
        except Exception as e:
            return Err(F" Failed to get Timeframe : {e}")

