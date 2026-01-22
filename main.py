import asyncio
import logging
from datetime import datetime, timedelta

# Import "The Building Blocks"
from research.shared import FetchJob, Ok, Err
from research.ingestion import (
    CCXTAsyncAdaptor, 
    YahooFinanceAdaptor, 
    ParquetStorageAdaptor, 
    ExchangeProvider
)

# --- SETUP LOGGING ---
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
    elif job.source == "yahoo":
        return YahooFinanceAdaptor()
    else:
        raise ValueError(f"Unknown source: {job.source}")

# --- SINGLE JOB EXECUTOR ---
async def process_job(job: FetchJob, storage: ParquetStorageAdaptor) -> str:
    logger.info(f"🚀 Starting Job: {job.symbol} [{job.source}]")
    try:
        adaptor = get_adaptor_for_job(job)
    except Exception as e:
        return f"❌ {job.symbol}: Init Failed ({e})"

    # Fetch
    fetch_result = await adaptor.fetch(job)

    # Error Handling (Railway Pattern)
    if isinstance(fetch_result, Err):
        await adaptor.close()
        return f"⚠️ {job.symbol}: {fetch_result.error}"
    
    candles = fetch_result.unwrap()
    
    # Save
    save_result = await storage.save(candles, job)
    await adaptor.close()

    if isinstance(save_result, Ok):
        return f"✅ {job.symbol}: Saved {len(candles)} rows."
    else:
        return f"❌ {job.symbol}: Save Failed -> {save_result.error}"

# --- MAIN LOOP ---
async def main():
    logger.info("🤖 SYSTEM START: StatArb Data Ingestion")
    
    # Gudang Data
    storage = ParquetStorageAdaptor(base_path="./data/raw")
    
    end_now = datetime.now()
    # Strategi Kalman Filter butuh data historis panjang untuk 'belajar' (Training)
    start_date = end_now - timedelta(days=365*2) # Ambil 2 tahun ke belakang

    # DAFTAR ASET TARGET
    jobs = [
        # --- 1. CRYPTO ANCHOR (BTC) ---
        # Ini biasanya jadi referensi pasar
        FetchJob(
            symbol="BTC/USDT",
            source="ccxt",
            timeframe="1h", 
            start_date=start_date,
            end_date=end_now
        ),
        
        # --- 2. CRYPTO VOLATILE (DOGE) ---
        # Aset yang akan kita cari kointegrasinya dengan BTC
        FetchJob(
            symbol="DOGE/USDT",
            source="ccxt",
            timeframe="1h",
            start_date=start_date,
            end_date=end_now
        ),

        # --- 3. SAHAM MURAH / PENNY STOCKS (Yahoo) ---
        # Contoh: Saham teknologi murah atau mining yang mungkin berkorelasi dengan Crypto
        # Ganti simbol ini dengan saham pilihan Anda.
        
        # Contoh: Marathon Digital (Mining) - Sering berkorelasi dgn BTC
        FetchJob(
            symbol="MARA", 
            source="yahoo",
            timeframe="1h", # Yahoo support 1h untuk 730 hari terakhir
            start_date=end_now - timedelta(days=700),
            end_date=end_now

        ),
        
        # Contoh: Saham Penny Indonesia (Ganti dengan kode saham murah incaran Anda)
        # Misal: BUMI.JK (Mining) atau FREN.JK (Telco)
        FetchJob(
            symbol="FREN.JK",
            source="yahoo",
            timeframe="1d", # Saham lokal biasanya data intraday-nya terbatas di Yahoo
            start_date=start_date,
            end_date=end_now
        ),
        
        # Contoh: Saham Penny US (Di bawah $5)
        FetchJob(
            symbol="SNDL", # Sundial Growers (Contoh Meme Stock)
            source="yahoo",
            timeframe="1h",
            start_date=end_now - timedelta(days=700),
            end_date=end_now
        )
    ]

    logger.info(f"📋 Queued {len(jobs)} jobs for Kalman Filter Dataset...")

    # Eksekusi Paralel
    tasks = [process_job(job, storage) for job in jobs]
    results = await asyncio.gather(*tasks)

    # Report
    print("\n" + "="*40)
    print("       INGESTION REPORT       ")
    print("="*40)
    for res in results:
        print(res)
    print("="*40 + "\n")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
