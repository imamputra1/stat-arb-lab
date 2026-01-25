import ccxt.async_support as ccxt
from typing import List, Tuple
import logging
import asyncio
from datetime import datetime

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from ..shared import OHLCV, FetchJob, Result

logger = logging.getLogger(__name__)

class CCXTAsyncAdaptor:
    """
    Async CCXT adaptor dengan advanced pagination dan rate limiting.
    ADHD-friendly: Predictable pagination, clear progress, smart error handling.
    """

    def __init__(self, exchange_id: str = "binance") -> None:
        self.exchange_id = exchange_id
        try:
            self.exchange = getattr(ccxt, exchange_id)({
                'enableRateLimit': True,
                'timeout': 30000, 
                'options': {'defaultType': 'spot'},
            })
            self._markets_loaded = False
            self._rate_limit_semaphore = asyncio.Semaphore(10) 
            logger.info(f"CCXT Adaptor Initialized for {exchange_id}")
        except AttributeError:
            raise ValueError(f"Exchange '{exchange_id}' tidak ditemukan di CCXT")

    async def ensure_connections(self) -> None:
        """Lazy load markets"""
        if not self._markets_loaded:
            try:
                await self.exchange.load_markets()
                self._markets_loaded = True
                logger.debug(f"Markets loaded for {self.exchange_id}")
            except Exception as e:
                logger.error(f"Failed to load markets: {e}")
                raise

    async def close(self) -> None:
        """Cleanup connection"""
        try:
            if hasattr(self.exchange, 'close'):
                await self.exchange.close()
                logger.debug(f"Exchange {self.exchange_id} closed")
        except Exception as e:
            logger.warning(f"Error closing exchange: {e}")

    async def fetch(self, job: 'FetchJob') -> 'Result[List[OHLCV], str]':
        """ 
        Main Fetch Method: Mengelola pagination dan validasi.
        """
        from ..shared import Ok, Err

        validation_result = self._validate_job(job)
        if validation_result.is_err():
            return Err(validation_result.error)

        connection_result = await self._safe_load_markets()
        if connection_result.is_err():
            return Err(f"Connection Failed: {connection_result.error}")

        cursor_result = self._setup_cursor(job)
        if cursor_result.is_err():
            return Err(cursor_result.error)

        start_ms, end_ms = cursor_result.unwrap()

        if start_ms >= end_ms:
            return Err("Start date must be before end date")

        all_candles: List['OHLCV'] = []
        page_count = 0
        max_pages = 5000 # Safety break

        logger.info(f"Start pagination for {job.symbol} from {datetime.fromtimestamp(start_ms/1000)}")

        current_cursor = start_ms

        while current_cursor < end_ms and page_count <= max_pages:
            
            async with self._rate_limit_semaphore:
                page_result = await self._fetch_page(job, current_cursor)
                
                if page_result.is_err():
                    logger.warning(f"Page fetch error: {page_result.error}")
                    if all_candles:
                        logger.warning("Returning partial data collected so far.")
                        return Ok(all_candles)
                    return Err(page_result.error)
                 
                page_candles, next_cursor = page_result.unwrap()
                
                if not page_candles:
                    break
            
                all_candles.extend(page_candles)

                if next_cursor <= current_cursor:
                    logger.warning(f"Cursor stuck at {current_cursor}, breaking loop")
                    break

                current_cursor = next_cursor
                page_count += 1

                self._log_progress(page_count, len(all_candles), job.symbol)

        if page_count >= max_pages:
            logger.warning(f"Reached max pages ({max_pages}). Stopping.")

        if not all_candles:
            return Err(f"No valid data fetched for {job.symbol}")

        logger.info(f"Fetcher selesai: {len(all_candles)} rows in {page_count} pages")
        return Ok(all_candles)

    # ================== PRIVATE METHODS ==================

    def _validate_job(self, job: 'FetchJob') -> 'Result[None, str]':
        from ..shared import Ok, Err

        try:
            if not hasattr(job, 'symbol') or not job.symbol:
                return Err("Job missing symbol")
            
            if not hasattr(job, 'timeframe'):
                return Err("Job missing timeframe")

            if not hasattr(job, 'start_date'):
                return Err("Job missing start date")

            return Ok(None)

        except Exception as e:
            return Err(f"Job Validation error: {e}")

    async def _safe_load_markets(self) -> 'Result[None, str]':
        """ Load market wrapper """
        from ..shared import Ok, Err

        try:
            if not self._markets_loaded:
                await self.exchange.load_markets()
                self._markets_loaded = True
            return Ok(None)
        except Exception as e:
            return Err(f"Failed to load markets: {e}")

    def _setup_cursor(self, job: 'FetchJob') -> 'Result[Tuple[int, int], str]':
        from ..shared import Ok, Err

        try:
            start_ms = int(job.start_date.timestamp() * 1000)
            if job.end_date:
                end_ms = int(job.end_date.timestamp() * 1000)
            else:
                end_ms = int(datetime.now().timestamp() * 1000)
                
            return Ok((start_ms, end_ms))
        except Exception as e:
            return Err(f"Failed to setup cursor: {e}")

    async def _fetch_page(
        self,
        job: 'FetchJob',
        cursor_ms: int
    ) -> 'Result[Tuple[List[OHLCV], int], str]':
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
                    except Exception:
                        continue
                
                if not parsed_candles:
                    return Ok(([], cursor_ms))

                next_cursor = int(last_timestamp) + 1
                return Ok((parsed_candles, next_cursor))

            except ccxt.RateLimitExceeded as e:
                wait_time = 2 ** attempt
                logger.warning(f"Rate Limit hit, waiting {wait_time}s")
                await asyncio.sleep(wait_time)
                last_exception = e
            
            except ccxt.NetworkError as e:
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt
                    logger.warning(f"Network Error, retry in {wait_time}s: {e}")
                    await asyncio.sleep(wait_time)
                    last_exception = e
                else:
                    return Err(f"Network Error exhausted: {e}")
            
            except Exception as e:
                logger.error(f"Unexpected error in page fetch: {e}")
                return Err(f"Unexpected error: {e}")

        return Err(f"Failed after retries. Last error: {last_exception}")

    def _log_progress(self, page_count: int, total_rows: int, symbol: str) -> None:
        if page_count % 10 == 0:
            logger.info(f"{symbol}: Page {page_count}, Collected {total_rows} rows...")

    async def check_symbol(self, symbol: str) -> 'Result[bool, str]':
        from ..shared import Ok, Err
        try:
            await self.ensure_connections()
            return Ok(symbol in self.exchange.markets)
        except Exception as e:
            return Err(f"Failed to check symbol: {e}")
    
    async def get_timeframes(self) -> 'Result[dict, str]':
        from ..shared import Ok, Err
        try:
            await self.ensure_connections()
            return Ok(self.exchange.timeframes or {})
        except Exception as e:
            return Err(f"Failed to get Timeframes: {e}")
