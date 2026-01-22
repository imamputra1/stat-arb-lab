import ccxt.async_support as ccxt
from typing import List
import logging

from ..shared import Result, Ok, Err, OHLCV, FetchJob

class CCXTAsyncAdaptor:
    def __init__(self, exchange_id: str = "binance") -> None:
        exchange_class_ref = getattr(ccxt, exchange_id)
        self.exchange = exchange_class_ref({'enableRateLimit': True})
        self._markets_loaded = False

    async def ensure_connections(self) -> None:
        if not self._markets_loaded:
            await self.exchange.load_markets()
            self._markets_loaded = True

    async def close(self) -> None:
        await self.exchange.close()

    async def fetch(self, job: FetchJob) -> Result[List[OHLCV], str]:
        try:
            await self.ensure_connections()
            
            cursor = int(job.start_date.timestamp() * 1000)
            
            batch = await self.exchange.fetch_ohlcv(
                symbol=job.symbol, 
                timeframe=job.timeframe, 
                since=cursor, 
                limit=1000
            )

            if not batch:
                 return Err(f"Exchange returned no data for {job.symbol}")

            candles: List[OHLCV] = []
            
            # Parsing Data
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
                    candles.append(candle)
                except Exception as val_err:
                    logging.warning(f"Skipping invalid candle data: {val_err}")
                    continue
            
            if not candles: 
                return Err(f"No valid candles parsed for {job.symbol}")
            
            return Ok(candles)

        except Exception as e:
            return Err(f"CCXT Error: {str(e)}")
