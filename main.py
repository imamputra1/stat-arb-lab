import asyncio
import logging
from datetime import datetime

# Import komponen inti
from research.shared import FetchJob, Ok, Err
from research.ingestion import (
    CCXTAsyncAdaptor, 
    ParquetStorageAdaptor, 
    ExchangeProvider,
)
START_DATE = datetime(2023, 1, 1)
END_DATE = datetime.now()
TIMEFRAME = "1m" 

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger("Orchestrator")

# --- FACTORY PATTERN ---
def get_adaptor_for_job(job: FetchJob) -> ExchangeProvider:
    if job.source == "ccxt":
        return CCXTAsyncAdaptor("binance")
    else:
        raise ValueError(f"Unknown source: {job.source}")

# --- JOB EXECUTOR ---
async def process_job(job: FetchJob, storage: ParquetStorageAdaptor) -> str:
    logger.info(f"MISSION START: {job.symbol} [{job.timeframe}]")
    logger.info(f"Range: {job.start_date.date()} -> {job.end_date.date()}")
    
    adaptor = None
    try:
        # 1. Init Adaptor
        adaptor = get_adaptor_for_job(job)
        
        # 2. Fetching (Proses Lama: Pagination Loop)
        logger.info(f"Fetching stream for {job.symbol} (This may take minutes)...")
        fetch_result = await adaptor.fetch(job)
        
        if isinstance(fetch_result, Err):
            return f"{job.symbol} FETCH FAILED: {fetch_result.error}"
        
        candles = fetch_result.unwrap()
        total_rows = len(candles)
        logger.info(f"Fetched {total_rows} rows. Writing to Storage...")
        
        save_result = await storage.save(candles, job)
        
        if isinstance(save_result, Ok):
            return f"{job.symbol}: SUCCESS. {total_rows} rows stored via Hive Partitioning."
        else:
            return f"{job.symbol}: SAVE FAILED -> {save_result.error}"
            
    except Exception as e:
        logger.error(f"Critical Error processing {job.symbol}", exc_info=True)
        return f"CRITICAL: {e}"
        
    finally:
        if adaptor:
            await adaptor.close()

# --- MAIN LOOP ---
async def main():
    print("\n" + "="*50)
    print("      STAT-ARB DEEP DIVE: M1 HISTORY      ")
    print("      Target: Jan 2023 - Now (Hive Storage)  ")
    print("="*50 + "\n")
    
    storage = ParquetStorageAdaptor(base_path="./data/raw")
    
    jobs = [
        FetchJob(
            symbol="BTC/USDT",
            source="ccxt",
            timeframe=TIMEFRAME,
            start_date=START_DATE,
            end_date=END_DATE
        ),
        FetchJob(
            symbol="DOGE/USDT",
            source="ccxt",
            timeframe=TIMEFRAME,
            start_date=START_DATE,
            end_date=END_DATE
        ),
    ]
    
    logger.info(f"Queued {len(jobs)} jobs. Estimated volume: >1 Million rows.")

    tasks = [process_job(job, storage) for job in jobs]
    results = await asyncio.gather(*tasks)
    
    print("\n" + "="*50)
    print("           MISSION REPORT           ")
    print("="*50)
    for res in results:
        print(res)
    print("="*50 + "\n")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.warning("System stopped by user.")
