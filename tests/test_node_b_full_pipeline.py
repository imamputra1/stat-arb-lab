"""
UNIT TEST: NODE B FULL PIPELINE (THE CONDUCTOR)
Location: tests/test_node_b_full_pipeline.py
Focus: End-to-end integration from raw LazyFrame to Silver Lake Storage.
Standard: Industrial CLI
"""
import sys
import shutil
import tempfile
from pathlib import Path
import logging
from datetime import datetime, timedelta

# --- PATH INJECTION ---
PROJECT_ROOT = Path(__file__).parent.parent.absolute()
sys.path.append(str(PROJECT_ROOT))

import polars as pl
import numpy as np

# Import Pipeline Facade
from research.processing import create_processing_pipeline

# --- SETUP LOGGING ---
def setup_logging():
    log_dir = PROJECT_ROOT / "logs"
    log_dir.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_filename = log_dir / f"TestPipeline_{timestamp}.log"

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)-8s | %(name)-15s | %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(str(log_filename), mode='w')
        ]
    )
    return logging.getLogger("TestPipeline")

logger = setup_logging()

class TestFullPipeline:
    def __init__(self):
        self.test_dir = Path(tempfile.mkdtemp())
        logger.info(f"Sandbox created: {self.test_dir}")

    def cleanup(self):
        shutil.rmtree(self.test_dir)
        logger.info("Sandbox cleaned up.")

    def run(self) -> bool:
        logger.info("=== STARTING FULL PIPELINE INTEGRATION TEST ===")
        
        try:
            # 1. Create Mock Raw Data (100 rows)
            raw_data = self._create_mock_data(n=100)
            
            # 2. Initialize Pipeline via Facade
            # Menggunakan setting "Kotor bin Superior" untuk window kecil agar test cepat
            pipeline = create_processing_pipeline(
                silver_path=str(self.test_dir),
                anchor_symbol="BTC",
                windows=["10m", "30m"],
                beta_window="30m",
                zscore_window="30m"
            )
            
            # 3. Execute Batch
            logger.info("Executing pipeline.run_batch()...")
            result = pipeline.run_batch(raw_data)
            
            if result.is_err():
                logger.error(f"Pipeline failed: {result.error}")
                return False
            
            # 4. Verification
            success = self._verify_persistence()
            
            if success:
                logger.info("✅ FULL PIPELINE INTEGRATION: PASS")
            else:
                logger.error("❌ PIPELINE PERSISTENCE VERIFICATION: FAIL")
            
            return success

        except Exception as e:
            logger.error(f"Test Crash: {str(e)}", exc_info=True)
            return False
        finally:
            self.cleanup()

    def _create_mock_data(self, n=100) -> pl.LazyFrame:
        """Synthetic Brown Lake data."""
        start = datetime(2024, 1, 1)
        ts = [start + timedelta(minutes=i) for i in range(n)]
        return pl.DataFrame({
            "timestamp": ts,
            "close_BTC": np.random.uniform(40000, 41000, n),
            "close_DOGE": np.random.uniform(0.07, 0.08, n)
        }).lazy()

    def _verify_persistence(self) -> bool:
        """Check if metadata and partitions exist."""
        # Check Metadata
        meta_file = self.test_dir / "metadata.json"
        if not meta_file.exists():
            logger.error("Metadata registry not found in Silver Lake")
            return False
            
        # Check Hive Partition (Jan 2024)
        partition = self.test_dir / "year=2024" / "month=01"
        if not partition.exists():
            logger.error("Hive partition year=2024/month=01 missing")
            return False
            
        # Check Data Integrity via Polars Scan
        scan = pl.scan_parquet(f"{self.test_dir}/**/*.parquet").collect()
        
        # Verify critical columns from all tiers
        required = ["ret_BTC", "vol_BTC_10m", "beta_DOGE_BTC", "z_score_DOGE"]
        missing = [c for c in required if c not in scan.columns]
        
        if missing:
            logger.error(f"Missing refined features in storage: {missing}")
            return False
            
        logger.info(f"Verified {scan.shape[0]} rows and {scan.shape[1]} columns in Silver Lake.")
        return True

if __name__ == "__main__":
    success = TestFullPipeline().run()
    sys.exit(0 if success else 1)
