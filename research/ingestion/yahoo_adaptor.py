import asyncio
import logging
from typing import List, Dict, Any
import pandas as pd
import yfinance as yf

# Import Shared Modules (Menggunakan TYPE_CHECKING untuk hindari circular import)
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from ..shared import Result, OHLCV, FetchJob

logger = logging.getLogger(__name__)

class YahooFinanceAdaptor:
    """
    Adaptor Yahoo Finance (Final Version).
    Menggabungkan Error Handling yang kuat dan Modularitas.
    """
    
    def __init__(self) -> None:
        self._timeframe_map = {
            '1m': '1m', '2m': '2m', '5m': '5m', '15m': '15m', '30m': '30m',
            '60m': '60m', '90m': '90m', '1h': '60m', 
            '1d': '1d', '5d': '5d', '1wk': '1wk', '1mo': '1mo', '3mo': '3mo'
        }
    
    async def close(self) -> None:
        pass

    async def fetch(self, job: 'FetchJob') -> 'Result[List[OHLCV], str]':
        try:
            # 1. Validate
            if not job.symbol: return self._err("Job missing symbol")
            if job.timeframe not in self._timeframe_map:
                return self._err(f"Unsupported timeframe: {job.timeframe}")

            # 2. Prepare Params
            yf_params = self._prepare_yahoo_params(job)
            
            # 3. Execute (Async wrapper)
            df = await self._execute_yahoo_download(job.symbol, yf_params)
            
            if df.empty:
                return self._err(f"Yahoo Finance returned empty data for {job.symbol}")

            # 4. Clean Data
            df_clean = self._clean_dataframe(df, job.symbol)
            
            # 5. Parse to Domain
            candles = self._parse_to_ohlcv(df_clean, job.symbol)
            
            if not candles:
                return self._err(f"No valid candles parsed for {job.symbol}")

            logger.info(f"✅ Yahoo: Fetched {len(candles)} rows for {job.symbol}")
            return self._ok(candles)

        except Exception as e:
            logger.error(f"Yahoo Exec Error: {e}")
            return self._err(str(e))

    # ================== PRIVATE METHODS ==================

    def _prepare_yahoo_params(self, job: 'FetchJob') -> Dict[str, Any]:
        start_str = job.start_date.strftime('%Y-%m-%d')
        end_str = job.end_date.strftime('%Y-%m-%d') if job.end_date else None
        
        params = {
            'start': start_str,
            'end': end_str,
            'interval': self._timeframe_map[job.timeframe],
            'progress': False,
            'auto_adjust': True,
            'group_by': 'column'
        }
        return params

    async def _execute_yahoo_download(self, symbol: str, params: Dict[str, Any]) -> pd.DataFrame:
        """Wrapper untuk menjalankan yf.download di thread terpisah"""
        def _download_sync():
            return yf.download(tickers=symbol, **params)
        
        return await asyncio.to_thread(_download_sync)

    def _clean_dataframe(self, df: pd.DataFrame, symbol: str) -> pd.DataFrame:
        df_clean = df.copy()
        
        # 1. Handle MultiIndex
        if isinstance(df_clean.columns, pd.MultiIndex):
            target_level = -1
            for i, level in enumerate(df_clean.columns.levels):
                if 'Open' in level:
                    target_level = i
                    break
            
            if target_level != -1:
                df_clean.columns = df_clean.columns.get_level_values(target_level)
            else:
                df_clean.columns = df_clean.columns.get_level_values(0)

        # 2. Fix Column Names
        col_map = {'Adj Close': 'Close'}
        df_clean = df_clean.rename(columns=col_map)

        # 3. Drop NaN
        cols_to_check = [c for c in ['Open', 'High', 'Low', 'Close'] if c in df_clean.columns]
        df_clean = df_clean.dropna(subset=cols_to_check)
        
        # 4. Remove Timezone
        if df_clean.index.tz is not None:
            df_clean.index = df_clean.index.tz_convert(None)

        return df_clean

    def _parse_to_ohlcv(self, df: pd.DataFrame, symbol: str) -> List['OHLCV']:
        from ..shared import OHLCV
        candles = []
        
        for row in df.itertuples():
            try:
                ts_ms = int(row.Index.timestamp() * 1000)
                o = getattr(row, 'Open')
                h = getattr(row, 'High')
                l = getattr(row, 'Low')
                c = getattr(row, 'Close')
                v = getattr(row, 'Volume', 0.0)

                candle = OHLCV(
                    timestamp=ts_ms,
                    open=float(o),
                    high=float(h),
                    low=float(l),
                    close=float(c),
                    volume=float(v)
                )
                candles.append(candle)
            except Exception:
                continue
                
        return candles

    def _ok(self, val):
        from ..shared import Ok
        return Ok(val)
        
    def _err(self, msg):
        from ..shared import Err
        return Err(msg)
