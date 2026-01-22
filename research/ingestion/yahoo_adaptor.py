import logging
from typing import List
import pandas as pd

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
        
        return ""  # No errors

    # ... (bagian atas class tetap sama) ...

    def _prepare_yahoo_params(self, job: 'FetchJob') -> dict:
        """Prepare parameters for yfinance"""
        params = {
            'start': job.start_date.strftime('%Y-%m-%d'),
            'interval': self._timeframe_map[job.timeframe],
            'progress': False,
            'auto_adjust': True,
            'actions': False,
            # FIX 1: Paksa format kolom sederhana jika versi yfinance mendukung
            # Jika versi lama, ini akan diabaikan via **params
            'group_by': 'column', 
        }
        
        if hasattr(job, 'end_date') and job.end_date:
            params['end'] = job.end_date.strftime('%Y-%m-%d')
        
        return params

    def _clean_dataframe(self, df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """Clean raw Yahoo DataFrame - Robuster Column Detection"""
        df_clean = df.copy()
        
        # 1. Handle MultiIndex columns (Masalah Utama)
        if isinstance(df_clean.columns, pd.MultiIndex):
            # Cari level mana yang berisi 'Open'
            found_level = -1
            for i, level in enumerate(df_clean.columns.levels):
                if 'Open' in level:
                    found_level = i
                    break
            
            if found_level != -1:
                # Ambil level yang benar (Price Level)
                df_clean.columns = df_clean.columns.get_level_values(found_level)
            else:
                # Fallback: Coba ambil level 0 (Default lama)
                df_clean.columns = df_clean.columns.get_level_values(0)
            
            logger.debug(f"Flattened MultiIndex columns for {symbol}")

        # 2. Fix Column Names (Kadang Yahoo pakai 'Adj Close' meski auto_adjust=True)
        # Rename agar sesuai standar kita: Open, High, Low, Close, Volume
        col_map = {
            'Adj Close': 'Close',
            'Stock Splits': 'Splits'
        }
        df_clean = df_clean.rename(columns=col_map)

        # 3. Drop rows with NaN
        original_len = len(df_clean)
        # Pastikan kita hanya drop jika OHLCV kosong (Volume 0 boleh ada di Forex kadang-kadang)
        subset_cols = [c for c in ['Open', 'High', 'Low', 'Close'] if c in df_clean.columns]
        df_clean = df_clean.dropna(subset=subset_cols)
        
        if len(df_clean) < original_len:
            logger.debug(f"Dropped {original_len - len(df_clean)} NaN rows for {symbol}")
        
        # 4. Remove timezone
        if df_clean.index.tz is not None:
            df_clean.index = df_clean.index.tz_convert(None)
        
        # 5. Final Validation
        required_cols = {'Open', 'High', 'Low', 'Close', 'Volume'}
        missing_cols = required_cols - set(df_clean.columns)
        
        if missing_cols:
            # DEBUG INFO: Agar kita tahu kolom apa yang sebenarnya didapat
            logger.error(f"Got columns: {list(df_clean.columns)}")
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
