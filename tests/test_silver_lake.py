"""
INTEGRATION TEST: SILVER LAKE STORAGE (THE VAULT)
Location: tests/test_silver_lake.py
Focus: Full Pipeline Integration (Tier 1-3 -> Storage).
Standard: Industrial CLI
"""
import sys
import shutil
from pathlib import Path
import logging
from datetime import datetime, timedelta

# --- PATH INJECTION ---
PROJECT_ROOT = Path(__file__).parent.parent.absolute()
sys.path.append(str(PROJECT_ROOT))

import polars as pl
import numpy as np

# Import All Tiers & Storage
from research.processing.transformation.returns import create_log_returns_transformer
from research.processing.features.market_micro import create_microstructure_transformer
from research.processing.features.stat_arb import create_stat_arb_transformer
from research.processing.storage.metadata_registry import create_metadata_registry
from research.processing.storage.parquet_engine import create_parquet_engine

# --- SETUP LOGGING ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)-8s | %(name)-15s | %(message)s'
)
logger = logging.getLogger("TestSilverLake")

class TestSilverLakeIntegration:
    def __init__(self):
        self.test_lake_path = PROJECT_ROOT / "data" / "test_silver"
        if self.test_lake_path.exists():
            shutil.rmtree(self.test_lake_path)
        self.test_lake_path.mkdir(parents=True)

    def run(self):
        logger.info("=== STARTING INTEGRATION TEST: SILVER LAKE FLOW ===")
        try:
            # 1. GENERATE SYNTHETIC BROWN LAKE DATA (1000 mins)
            df = self._create_mock_brown_lake(n=1000)
            logger.info("Step 1: Mock Brown Lake Data Ready.")

            # 2. RUN TRANSFORMATIONS (Tier 1 -> 2 -> 3)
            # Tier 1
            t1 = create_log_returns_transformer()
            lf_t1 = t1.transform(df).unwrap()
            
            # Tier 2
            t2 = create_microstructure_transformer(windows=["1h"])
            lf_t2 = t2.transform(lf_t1).unwrap()
            
            # Tier 3
            t3 = create_stat_arb_transformer(beta_window="1h", zscore_window="1h")
            final_lf = t3.transform(lf_t2).unwrap()
            logger.info("Step 2: Transformations T1-T3 Successful.")

            # 3. INITIALIZE STORAGE ENGINE
            registry = create_metadata_registry(str(self.test_lake_path))
            engine = create_parquet_engine(str(self.test_lake_path), registry)
            
            # 4. EXECUTE SAVE (The Factory)
            params = {"beta": "1h", "vol": "1h", "anchor": "BTC"}
            save_res = engine.save(final_lf, feature_params=params)
            
            if save_res.is_err():
                logger.error(f"Storage Failed: {save_res.error}")
                return False
            
            logger.info("Step 3: Data Persistent in Silver Lake.")

            # 5. VERIFICATION (The Proof)
            return self._verify_results()

        except Exception as e:
            logger.error(f"Integration Crash: {e}", exc_info=True)
            return False

    def _create_mock_brown_lake(self, n=1000):
        start = datetime(2024, 1, 1) # Jan 2024
        ts = [start + timedelta(minutes=i) for i in range(n)]
        return pl.DataFrame({
            "timestamp": ts,
            "close_BTC": np.random.uniform(40000, 41000, n),
            "close_DOGE": np.random.uniform(0.07, 0.08, n)
        }).lazy()

    def _verify_results(self) -> bool:
        # Check folder structure (Hive)
        # Data starts Jan 1st 2024 -> year=2024/month=01
        partition_path = self.test_lake_path / "year=2024" / "month=01"
        if not partition_path.exists():
            logger.error(f"Hive Partition Missing: {partition_path}")
            return False
        
        # Check Metadata
        meta_file = self.test_lake_path / "metadata.json"
        if not meta_file.exists():
            logger.error("Metadata.json missing!")
            return False
            
        # Check if data is readable by Polars (The Engine)
        # Inilah pembuktian bahwa Polars adalah Engine akses kita.
        scan_df = pl.scan_parquet(f"{self.test_lake_path}/**/*.parquet").collect()
        
        logger.info(f"Verification Success: {scan_df.shape[0]} rows recovered.")
        logger.info(f"Columns present: {len(scan_df.columns)}")
        
        # Check precision Float64
        if scan_df["z_score_DOGE"].dtype != pl.Float64:
            logger.error("Precision Leak! Z-Score is not Float64")
            return False

        print("\n" + "="*50)
        print("SILVER LAKE INTEGRATION: SUCCESS")
        print(f"Location: {self.test_lake_path}")
        print(f"Features: {scan_df.shape[1]} columns saved.")
        print("="*50 + "\n")
        return True

if __name__ == "__main__":
    success = TestSilverLakeIntegration().run()
    sys.exit(0 if success else 1)
