import asyncio
import logging
from typing import List
import pandas as pd
import yfinance as yf

# Import menggunakan lazy loading untuk menghindari circular imports
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from ..shared import Result, OHLCV, FetchJob

# Logger khusus untuk adapter ini
logger = logging.getLogger(__name__)

class YahooFinanceAdaptor:
    """
    Adaptor untuk Yahoo Finance dengan error handling komprehensif.
    ADHD-friendly: Single responsibility, clear error messages, fast fail.
    """
    
    def __init__(self) -> None:
        # Mapping timeframe Yahoo Finance (ADHD: Fail fast validation)
        self._timeframe_map = {
            '1m': '1m', '2m': '2m', '5m': '5m', '15m': '15m', '30m': '30m',
            '60m': '60m', '90m': '90m', '1h': '60m',  # Yahoo uses '60m' not '1h'
            '1d': '1d', '5d': '5d', '1wk': '1wk', '1mo': '1mo', '3mo': '3mo'
        }
    
    async def close(self) -> None:
        """Cleanup - ADHD: Predictable teardown"""
        pass  # Yahoo tidak perlu cleanup khusus

    async def fetch(self, job: 'FetchJob') -> 'Result[List[OHLCV], str]':
        """
        Fetch data dari Yahoo Finance dengan error handling lengkap.
        ADHD: Clear steps, fail fast, no hidden complexity.
        """
        try:
            # --- STEP 1: VALIDATE INPUT (ADHD: Fail Fast) ---
            validation_error = self._validate_job(job)
            if validation_error:
                return self._err(validation_error)

            # --- STEP 2: PREPARE PARAMETERS ---
            yf_params = self._prepare_yahoo_params(job)
            
            # --- STEP 3: EXECUTE BLOCKING CALL SAFELY ---
            df = await self._execute_yahoo_download(job.symbol, yf_params)
            
            if df.empty:
                return self._err(f"Yahoo Finance returned empty data for {job.symbol}")

            # --- STEP 4: CLEAN RAW DATA ---
            df_clean = self._clean_dataframe(df, job.symbol)
            
            if df_clean.empty:
                return self._err(f"No valid data after cleaning for {job.symbol}")

            # --- STEP 5: PARSE TO DOMAIN OBJECTS ---
            candles = self._parse_to_ohlcv(df_clean, job.symbol)
            
            if not candles:
                return self._err(f"No valid candles parsed for {job.symbol}")

            logger.info(f"✅ Yahoo: Fetched {len(candles)} rows for {job.symbol}")
            return self._ok(candles)

        except Exception as e:
            error_msg = f"Yahoo API Error for {job.symbol if 'job' in locals() else 'unknown'}: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return self._err(error_msg)

    # ====================== PRIVATE HELPER METHODS ======================
    
    def _validate_job(self, job: 'FetchJob') -> str:
        """Validate job parameters - ADHD: Explicit validation"""
        if not hasattr(job, 'symbol') or not job.symbol:
            return "Job missing symbol"
        
        if not hasattr(job, 'timeframe'):
            return "Job missing timeframe"
        
        if job.timeframe not in self._timeframe_map:
            return f"Unsupported timeframe: {job.timeframe}. Supported: {list(self._timeframe_map.keys())}"
        
        if not hasattr(job, 'start_date'):
            return "Job missing start_date"
        
        return ""  # No error

    def _prepare_yahoo_params(self, job: 'FetchJob') -> dict:
        """Prepare parameters for yfinance - ADHD: Single responsibility"""
        params = {
            'start': job.start_date.strftime('%Y-%m-%d'),
            'interval': self._timeframe_map[job.timeframe],
            'progress': False,
            'auto_adjust': True,  # Adjust for splits/dividends
            'actions': False,  # Don't include dividend/split actions
        }
        
        if hasattr(job, 'end_date') and job.end_date:
            params['end'] = job.end_date.strftime('%Y-%m-%d')
        
        return params

    async def _execute_yahoo_download(self, symbol: str, params: dict) -> pd.DataFrame:
        """Execute yfinance download in thread pool - ADHD: Async for I/O"""
        def _download_sync() -> pd.DataFrame:
            try:
                return yf.download(tickers=symbol, **params)
            except Exception as e:
                # Wrap yfinance errors for better debugging
                raise RuntimeError(f"yfinance download failed: {str(e)}")
        
        return await asyncio.to_thread(_download_sync)

    def _clean_dataframe(self, df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """Clean raw Yahoo DataFrame - ADHD: Step-by-step cleaning"""
        df_clean = df.copy()
        
        # 1. Handle MultiIndex columns
        if isinstance(df_clean.columns, pd.MultiIndex):
            # Extract first level (price columns)
            df_clean.columns = df_clean.columns.get_level_values(0)
            logger.debug(f"Fixed MultiIndex columns for {symbol}")
        
        # 2. Drop rows with NaN values (market holidays, errors)
        original_len = len(df_clean)
        df_clean = df_clean.dropna(subset=['Open', 'High', 'Low', 'Close', 'Volume'])
        
        if len(df_clean) < original_len:
            logger.debug(f"Dropped {original_len - len(df_clean)} NaN rows for {symbol}")
        
        # 3. Remove timezone if present (convert to naive UTC)
        if df_clean.index.tz is not None:
            df_clean.index = df_clean.index.tz_convert(None)
        
        # 4. Ensure required columns exist
        required_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
        missing_cols = [col for col in required_cols if col not in df_clean.columns]
        
        if missing_cols:
            raise ValueError(f"Missing columns {missing_cols} in Yahoo data for {symbol}")
        
        return df_clean

    def _parse_to_ohlcv(self, df: pd.DataFrame, symbol: str) -> List['OHLCV']:
        """Parse DataFrame to OHLCV objects - ADHD: Fast batch processing"""
        from ..shared import OHLCV  # Local import untuk menghindari circular
        
        candles: List['OHLCV'] = []
        errors = 0
        
        # Use itertuples for performance (3-4x faster than iterrows)
        for row in df.itertuples():
            try:
                # Convert timestamp to milliseconds
                ts_ms = int(row.Index.timestamp() * 1000)
                
                # Create OHLCV object (validation happens here)
                candle = OHLCV(
                    timestamp=ts_ms,
                    open=float(row.Open),
                    high=float(row.High),
                    low=float(row.Low),
                    close=float(row.Close),
                    volume=float(row.Volume)
                )
                candles.append(candle)
                
            except (ValueError, TypeError):
                errors += 1
                continue  # Skip invalid rows
            except Exception as e:
                # Log unexpected errors but continue
                logger.debug(f"Unexpected error parsing row for {symbol}: {e}")
                errors += 1
                continue
        
        if errors > 0:
            logger.warning(f"Skipped {errors} invalid rows for {symbol}")
        
        return candles

    # ====================== RESULT HELPERS (ADHD: No repetition) ======================
    
    def _ok(self, value):
        """Helper untuk membuat Ok result - ADHD: DRY"""
        from ..shared import Ok
        return Ok(value)
    
    def _err(self, message):
        """Helper untuk membuat Err result - ADHD: DRY"""
        from ..shared import Err
        return Err(message)
