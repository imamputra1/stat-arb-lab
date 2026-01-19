import asyncio
import ccxt.async_support as ccxt
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Protocol, Optional
from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator


logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("ingestion_run.log"),
        logging.StreamHandler()
    ]
)
"""
L1: Domain Types (Memory effiesiensi)
"""
class OHCLV(BaseModel):
    model_config = ConfigDict(frozen=True, validate_assignment=True)

    timestamp: int = Field(..., description="Unix timestamp in milliseconds")
    open: float = Field(..., gt=0)
    high: float = Field(..., gt=0)
    low: float = Field(..., gt=0)
    close: float = Field(..., gt=0)
    volume: float = Field(..., ge=0)

    @field_validator("low")
    @classmethod
    def validate_low(cls, v: float, info: ValidationInfo) -> float:
        values = info.data
        if 'high' in values and v > values['high']:
            logging.warning(f"annomaly detected: low {v} > high {values['high']}")
        return v

@dataclass(frozen=True)
class FetchJob:
    """Job description"""
    symbol: str
    timeframe: str
    start_date: datetime
    end_date: Optional[datetime] = None

"""
L2: Protocol
"""
class ExchangeProvider(Protocol):
    """Structur typing for Exchange Adaptor"""
    async def fetch(self, job: FetchJob) -> List[OHCLV]: ...
    async def close(self): ...

class StorageProvider(Protocol):
    """Structur typing for Storage Adaptor"""
    async def save(self, data: List[OHCLV], job: FetchJob) -> str: ...

"""
L3: Async Adaptor (black box)
"""
class CCXTAsyncAdaptor:
    """Async CCXT wrapper"""

    def __init__(self, exchange_id: str = "binance"): # Perbaikan typo type hint
        exchange_class = getattr(ccxt, exchange_id)
        self.exchange = exchange_class({'enableRateLimit': True})
        self._markets_loaded = False

    async def ensure_connection(self):
        if not self._markets_loaded:
            await self.exchange.load_markets()
            self._markets_loaded = True

    async def close(self):
        await self.exchange.close()

    async def fetch(self, job: FetchJob) -> List[OHCLV]:
        await self.ensure_connection()
        # Panggilan ini sekarang cocok dengan definisi di bawah (cuma 1 argumen)
        return await self._fetch_with_cursor(job)

    # PERBAIKAN UTAMA DI SINI: Hapus parameter 'session'
    async def _fetch_with_cursor(self, job: FetchJob) -> List[OHCLV]:
        """Internal coursor-based pagination"""
        all_candles: List[OHCLV] = []
        cursor = int(job.start_date.timestamp() * 1000)
        end_ts = int(job.end_date.timestamp() * 1000) if job.end_date else None

        while True:
            try:
                batch = await self.exchange.fetch_ohlcv(
                    job.symbol,
                    job.timeframe,
                    since=cursor,
                    limit=1000
                )

                if not batch:
                    break
                
                # convert and validate 
                validated_batch = []
                for row in batch:
                    validated_batch.append(OHCLV(
                        timestamp=row[0],
                        open=float(row[1]),
                        high=float(row[2]),
                        low=float(row[3]),
                        close=float(row[4]),
                        volume=float(row[5])
                    )) 
                all_candles.extend(validated_batch)
                cursor = batch[-1][0] + 1
                
                if len(batch) < 1000 or (end_ts and cursor >= end_ts):
                    break

            except Exception as e:
                logging.error(f"Batch fetch failed: {e}")
                break
        return all_candles

class ParquetAsyncStorage:
        def __init__(self, base_path: str):
            self.base_path = base_path
            os.makedirs(base_path, exist_ok=True)

        async def save(self, data: List[OHCLV], job: FetchJob) -> str:
            if not data:
                return ""

            df = pd.DataFrame([d.model_dump() for d in data])
            df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')

            safe_symbol = job.symbol.replace('/', '_')
            partition_path = os.path.join(
                self.base_path,
                "exchange=binance",
                f"symbol={safe_symbol}",
                f"timeframe={job.timeframe}",
                f"date={job.start_date.strftime('%Y-%m-%d')}"
            )
            os.makedirs(partition_path, exist_ok=True)

            table =  pa.Table.from_pandas(df)
            filename = f"part_{int(datetime.now().timestamp())}_{int(datetime.now().timestamp())}.parquet"
            filepath = os.path.join(partition_path, filename)
            
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(
                None, 
                lambda: pq.write_table(table, filepath, compression='snappy')
            )
            return filepath

"""
L4: Compostition engine
"""
class IngestionPipeline:
    def __init__(
            self,
            fetcher: ExchangeProvider,
            storage: StorageProvider,
            max_concurrent: int = 3
        ):
        self.fetcher = fetcher
        self.storage = storage
        self.max_concurrent = max_concurrent

    async def execute_job(self, job: FetchJob) -> Dict[str, Any]:
        try:
            data = await self.fetcher.fetch(job)
            if not data:
                return {"job": job, "status": "empty", "record": 0}

            saved_path = await self.storage.save(data, job)

            return {
                "job": job,
                "record": len(data),
                "path": saved_path,
                "status": "success"
            }
        except Exception as e:
            logging.error(f"pipeline failed: {e}")
            return {
                    "job": job,
                    "error": str(e),
                    "status": "failed"
                }
    async def run(self, jobs: List[FetchJob]) -> List[Dict[str, Any]]:
        semaphore = asyncio.Semaphore(self.max_concurrent)

        async def limited_execute(job: FetchJob):
            async with semaphore:
                return await self.execute_job(job)
        results = await asyncio.gather(*[limited_execute(job) for job in jobs])

        await self.fetcher.close()
        return results


"""
Factory and orchestration/main
"""

def create_default_pipeline() -> IngestionPipeline:
    fetcher = CCXTAsyncAdaptor("binance")
    storage = ParquetAsyncStorage("./data/minio_storage/bronze")
    return IngestionPipeline(fetcher, storage)

def create_job_list() -> List[FetchJob]:
    base_date = datetime(2023, 1, 1)
    jobs = []

    for weeks_ago in range (0, 4):
        start = base_date + timedelta(weeks=weeks_ago)
        end = start + timedelta(days=6)

        jobs.append(FetchJob(
            symbol="BTC/USDT",
            timeframe="1h",
            start_date=start,
            end_date=end,
            ))
    return jobs

async def main():
    print("HEI PIPELINE v0.1.0 starting ...")

    pipeline = create_default_pipeline()
    jobs = create_job_list()

    print(f"Prepered {len(jobs)} jobs, Executing concurrency ={pipeline.max_concurrent}...")
    results = await pipeline.run(jobs)

    success_count = len([r for r in results if r["status"] == "success"])
    print(f"Completed: {(success_count)}/{len(jobs)} success")

    for res in results:
        if res['status'] == 'success':
            print(f"{res['job'].start_date.date()} -> {res['record']} records save to {res['path']}")

if __name__ == "__main__":
    import time
    start_time = time.time()

    asyncio.run(main())

    elapsed = time.time() - start_time
    print(f"Total time: {elapsed:.2f}s")



