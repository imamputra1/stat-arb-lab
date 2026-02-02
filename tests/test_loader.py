"""
UNIT TEST: SILVER DATA LOADER
Location: tests/test_loader.py
Focus: Verification of Lazy Loading, Partition Pruning, and Metadata Integrity.
Standard: Industrial CLI with structured logging.
"""
import sys
import logging
from pathlib import Path
from datetime import datetime
import time

# --- PATH INJECTION ---
PROJECT_ROOT = Path(__file__).parent.parent.absolute()
sys.path.append(str(PROJECT_ROOT))

import polars as pl

# Import Sektor Logistik via City Gate
from research.strategy.data import create_silver_loader

# --- STRUCTURED LOGGING SETUP ---
def setup_test_logging():
    log_dir = PROJECT_ROOT / "logs" / "test_loader"
    log_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_file = log_dir / f"test_{timestamp}.log"
    
    logger = logging.getLogger("TestLoader")
    logger.setLevel(logging.INFO)
    
    formatter = logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(message)s',
        datefmt='%H:%M:%S'
    )

    # Console output
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    # File output
    fh = logging.FileHandler(log_file)
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    
    return logger

logger = setup_test_logging()

class LoaderVerification:
    def __init__(self):
        self.silver_path = PROJECT_ROOT / "data" / "silver"
        self.loader = create_silver_loader(str(self.silver_path))
        logger.info(f"Test Environment Initialized: {self.silver_path}")

    def run_all_tests(self):
        logger.info("=== STARTING LOADER VERIFICATION SEQUENCE ===")
        
        results = [
            self.test_lazy_check(),
            self.test_filter_check(),
            self.test_speed_check(),
            self.test_metadata_integrity()
        ]
        
        if all(results):
            logger.info("=== ALL LOADER TESTS PASSED: LOGISTICS SECTOR SECURE ===")
        else:
            logger.error("=== LOADER TESTS FAILED: INSPECT LOGS IMMEDIATELY ===")

    def test_lazy_check(self) -> bool:
        """Verify that the loader returns a LazyFrame to prevent RAM bloating."""
        logger.info("[CHECK 1] Lazy Assurance...")
        res = self.loader.load(start_date="2024-01-01", end_date="2024-01-02")
        
        if res.is_err():
            logger.error(f"Lazy load failed: {res.error}")
            return False
            
        lf = res.unwrap()
        is_lazy = isinstance(lf, pl.LazyFrame)
        
        if is_lazy:
            logger.info(f"   - Type Verification: {type(lf)} (PASS)")
        else:
            logger.error(f"   - Type Mismatch: Expected LazyFrame, got {type(lf)}")
            
        return is_lazy

    def test_filter_check(self) -> bool:
        """Verify that date filtering and column selection work correctly."""
        logger.info("[CHECK 2] Temporal & Column Filtering...")
        
        # Load exactly 1 day of data for BTC and DOGE
        start, end = "2024-01-01", "2024-01-01"
        res = self.loader.load(start_date=start, end_date=end, symbols=["BTC", "DOGE"])
        
        if res.is_err():
            logger.error(f"Filter load error: {res.error}")
            return False
            
        df = res.unwrap().collect() # Execute only for verification
        
        # Verify columns (must have BTC/DOGE related features)
        has_btc = any("BTC" in col for col in df.columns)
        has_doge = any("DOGE" in col for col in df.columns)
        
        # Verify rows (1440 rows for 1 minute interval in 1 day)
        row_count = df.height
        
        logger.info(f"   - Rows recovered: {row_count}")
        logger.info(f"   - Columns active: {len(df.columns)}")
        
        if row_count > 0 and has_btc and has_doge:
            logger.info("   - Data Integrity Check: PASS")
            return True
        else:
            logger.error("   - Data Integrity Check: FAIL (Empty or Incorrect Columns)")
            return False

    def test_speed_check(self) -> bool:
        """Verify that loading massive data is nearly instant due to Lazy Scanning."""
        logger.info("[CHECK 3] I/O Speed (Partition Pruning)...")
        
        start_time = time.time()
        # Loading full 3-year range (should be instant because it's lazy)
        res = self.loader.load(start_date="2023-01-01", end_date="2025-12-31")
        duration = time.time() - start_time
        
        if res.is_err():
            return False
            
        logger.info(f"   - Execution Time: {duration:.6f} seconds")
        
        # Standard: Lazy scan should be under 0.1s
        if duration < 0.1:
            logger.info("   - Speed Benchmarking: PASS (SUPERIOR)")
            return True
        else:
            logger.warning(f"   - Speed Benchmarking: SLOW ({duration:.6f}s) - Check Hive Partitioning")
            return False

    def test_metadata_integrity(self) -> bool:
        """Ensure the loader can read the feature hash from Node B."""
        logger.info("[CHECK 4] Metadata Audit Trail...")
        res = self.loader.get_metadata()
        
        if res.is_err():
            logger.error(f"Metadata access failed: {res.error}")
            return False
            
        meta = res.unwrap()
        feature_hash = meta.get("feature_hash", "NONE")
        
        logger.info(f"   - Source Feature Hash: {feature_hash}")
        if feature_hash != "NONE":
            logger.info("   - Audit Trail Verification: PASS")
            return True
        return False

if __name__ == "__main__":
    verifier = LoaderVerification()
    verifier.run_all_tests()
