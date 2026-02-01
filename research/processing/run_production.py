"""
PRODUCTION RUNNER (IGNITION KEY)
Focus: Processing historical data from Brown Lake to Silver Lake.
Location: research/processing/run_production.py
Strategy: Rigid Casting & Hive-Aware Pivoting
"""
import sys
import logging
from pathlib import Path
from datetime import datetime, timezone

# --- PATH INJECTION ---
PROJECT_ROOT = Path(__file__).parent.parent.parent.absolute()
sys.path.append(str(PROJECT_ROOT))

import polars as pl

# Import Pipeline Facade
from research.processing import create_processing_pipeline

# --- SETUP LOGGING ---
def setup_production_logging():
    log_base = PROJECT_ROOT / "logs" / "execution"
    log_base.mkdir(parents=True, exist_ok=True)
    current_date = datetime.now(timezone.utc).strftime('%Y-%m-%d')
    log_file = log_base / f"{current_date}_run.log"
    
    logger = logging.getLogger("ProductionRun")
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s | %(levelname)-8s | %(message)s', datefmt='%H:%M:%S')

    if not logger.handlers:
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        logger.addHandler(ch)
        fh = logging.FileHandler(log_file, mode='a')
        fh.setFormatter(formatter)
        logger.addHandler(fh)
    return logger

logger = setup_production_logging()

class ProductionIgnition:
    def __init__(self):
        self.brown_lake = PROJECT_ROOT / "data" / "raw"
        self.silver_lake = PROJECT_ROOT / "data" / "silver"
        self.pipeline = create_processing_pipeline(str(self.silver_lake))

    def execute(self, years: list[int]):
        logger.info(f"STARTING REFINERY | Target Years: {years}")
        for year in years:
            logger.info(f"District Processing: Year {year}")
            aligned_res = self._load_and_align_year(year)
            
            if aligned_res is None:
                continue
            
            result = self.pipeline.run_batch(aligned_res)
            if result.is_ok():
                logger.info(f"SUCCESS | Year {year} refined and stored.")
            else:
                logger.error(f"FAILURE | Year {year}: {result.error}")

    def _load_and_align_year(self, year: int) -> pl.LazyFrame:
        """
        KOTOR bin SUPERIOR: Handle i64 timestamps and numeric partitions.
        """
        try:
            # 1. Hive Root Scan
            # Kita aktifkan hive_partitioning agar 'symbol' dan 'year' terdeteksi
            raw_scan = pl.scan_parquet(
                self.brown_lake / "**/*.parquet", 
                hive_partitioning=True
            )
            
            # 2. Aggressive Type Normalization
            # - Cast 'year' ke Utf8 agar perbandingan filter valid
            # - Cast 'timestamp' ke Datetime agar dt.year() di storage engine bekerja
            normalized_scan = (
                raw_scan
                .with_columns([
                    pl.col("year").cast(pl.Utf8),
                    pl.col("timestamp").cast(pl.Datetime("ms"))
                ])
                .filter(pl.col("year") == str(year))
            )
            
            # 3. Pivot to Wide Table (Node S Standard)
            # Alignment otomatis terjadi di sini
            df_wide = (
                normalized_scan.select(["timestamp", "symbol", "close"])
                .collect()
                .pivot(on="symbol", index="timestamp", values="close")
                .sort("timestamp")
            )
            
            # 4. Clean Header Prefix
            rename_map = {
                c: f"close_{c.split('-')[0]}" 
                for c in df_wide.columns if c != "timestamp"
            }
            final_df = df_wide.rename(rename_map)
            
            logger.info(f"Aligned Year {year}: {final_df.height} rows, {final_df.width-1} assets")
            return final_df.lazy()

        except Exception as e:
            logger.error(f"Refinery Ingestion Error Year {year}: {str(e)}")
            return None

if __name__ == "__main__":
    # Eksekusi Batch Historis
    ProductionIgnition().execute(years=[2023, 2024, 2025])
