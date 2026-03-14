"""
PRODUCTION RUNNER (IGNITION KEY)
Focus: Processing historical data from Brown Lake to Silver Lake.
Location: research/processing/run_production.py
Strategy: Rigid Casting & Hive-Aware Alignment (using HybridAsofAligner)
"""
import sys
import logging
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Optional, Dict

# --- PATH INJECTION ---
PROJECT_ROOT = Path(__file__).parent.parent.parent.absolute()
sys.path.append(str(PROJECT_ROOT))

import polars as pl

# Import Pipeline Facade
from research.processing import create_processing_pipeline
# Import the new aligner
from research.processing.alignment import align_multiple_series

# --- SETUP LOGGING ---
def setup_production_logging() -> logging.Logger:
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
    def __init__(self) -> None:
        self.brown_lake: Path = PROJECT_ROOT / "data" / "raw"
        self.silver_lake: Path = PROJECT_ROOT / "data" / "silver"
        # Anchor symbol for pipeline must match suffix from aligner: "BTC_USDT"
        self.pipeline = create_processing_pipeline(
            str(self.silver_lake),
            anchor_symbol="BTC_USDT"   # CRITICAL: matches safe symbol from aligner
        )

    def execute(self, years: List[int]) -> None:
        logger.info(f"STARTING REFINERY | Target Years: {years}")
        for year in years:
            logger.info(f"District Processing: Year {year}")
            aligned_lf = self._load_and_align_year(year)
            
            if aligned_lf is None:
                logger.warning(f"Skipping year {year} due to alignment failure.")
                continue
            
            result = self.pipeline.run_batch(aligned_lf)
            if result.is_ok():
                logger.info(f"SUCCESS | Year {year} refined and stored.")
            else:
                logger.error(f"FAILURE | Year {year}: {result.error}")

    def _load_and_align_year(self, year: int) -> Optional[pl.LazyFrame]:
        """
        Reads raw data for the given year, splits by symbol,
        and aligns all symbols into a single wide LazyFrame using HybridAsofAligner.
        """
        try:
            # 1. Scan Parquet with hive partitioning enabled
            raw_scan = pl.scan_parquet(
                self.brown_lake / "**/*.parquet",
                hive_partitioning=True
            )

            # 2. Normalize timestamp and year to proper types
            raw_scan = raw_scan.with_columns([
                pl.col("timestamp").cast(pl.Datetime("ms")),
                pl.col("year").cast(pl.Utf8)          # ensure string for filter
            ])

            # 3. Filter by year (now both sides are strings)
            raw_scan = raw_scan.filter(pl.col("year") == str(year))

            # 4. Quick check if any data exists for this year
            has_data = raw_scan.select(pl.len()).collect().item() > 0
            if not has_data:
                logger.warning(f"No data found for year {year}")
                return None

            # 5. Get all distinct symbols present in this year
            #    Column name from hive partitioning is 'symbol' (singular)
            symbols_df: pl.DataFrame = raw_scan.select(pl.col("symbol").unique()).collect()
            symbols: List[str] = symbols_df["symbol"].to_list()
            logger.info(f"Symbols found for {year}: {symbols}")

            # 6. Build data_map: safe_symbol (underscore) -> LazyFrame with OHLCV columns
            data_map: Dict[str, pl.LazyFrame] = {}
            for sym in symbols:
                # sym from hive is "BTC-USDT" (with hyphen), convert to "BTC_USDT"
                safe_sym: str = sym.replace('-', '_')
                lf: pl.LazyFrame = (
                    raw_scan
                    .filter(pl.col("symbol") == sym)
                    .select(["timestamp", "open", "high", "low", "close", "volume"])
                )
                data_map[safe_sym] = lf

            # 7. Determine anchor symbol – hardcoded to the safe version
            anchor_safe: str = "BTC_USDT"
            if anchor_safe not in data_map:
                logger.error(f"Anchor symbol {anchor_safe} not found in data for year {year}")
                return None

            # 8. Execute alignment (asof join, 1m tolerance, keep all rows)
            align_result = align_multiple_series(
                data_map,
                method="asof",
                tolerance="1m",
                anchor=anchor_safe,
                strict=False           # do not drop rows with nulls from followers
            )

            if align_result.is_err():
                logger.error(f"Alignment failed for year {year}: {align_result.error}")
                return None

            aligned_lf: pl.LazyFrame = align_result.unwrap()

            # 9. Log some stats – avoid .width on LazyFrame (PerformanceWarning)
            row_count: int = aligned_lf.select(pl.len()).collect().item()
            col_count: int = len(aligned_lf.collect_schema().names())
            logger.info(f"Aligned Year {year}: {row_count} rows, {col_count} columns")
            return aligned_lf

        except Exception as e:
            logger.error(f"Refinery Ingestion Error Year {year}: {str(e)}", exc_info=True)
            return None


if __name__ == "__main__":
    # Execute batch for required years (2025 and 2026)
    ProductionIgnition().execute(years=[2025, 2026])
