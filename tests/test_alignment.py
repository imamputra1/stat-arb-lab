"""
UNIT TEST: ALIGNMENT LOGIC
Location: tests/test_alignment.py
Focus: Verifying 'Hybrid Asof Join' behavior (UTC Standardized).
"""
import sys
from pathlib import Path

# Path Injection
PROJECT_ROOT = Path(__file__).parent.parent.absolute()
sys.path.append(str(PROJECT_ROOT))

import polars as pl
from datetime import datetime, timedelta, timezone # Import timezone
import logging

from research.processing.alignment import get_aligner

def setup_logging():
    log_dir = PROJECT_ROOT / "logs"
    log_dir.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_filename = log_dir / f"TestAlignment_{timestamp}.log"

    logging.basicConfig(
        format='%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
        datefmt='%H:%M:%S',
        level=logging.INFO,
        handlers=[logging.StreamHandler(), logging.FileHandler(str(log_filename), mode='w')]
    )
    return logging.getLogger("TestAlignment")

logger = setup_logging()

class TestAlignmentLogic:
    
    def run(self):
        logger.info("--- STARTING UNIT TEST: ALIGNMENT LOGIC (UTC) ---")
        
        results = {
            "Creating Dummy Data ": self.test_dummy_creation,
            "Aligner Initialization": self.test_initialization,
            "Logic: Perfect Match  ": self.test_perfect_match,
            "Logic: Latency Match  ": self.test_latency_match,
            "Logic: Out of Tolerance": self.test_out_of_tolerance,
        }
        
        passed_count = 0
        self.data_map = None 
        self.df_result = None 

        for test_name, test_func in results.items():
            try:
                is_pass = test_func()
                status = "PASS" if is_pass else "FAIL"
                if is_pass:
                    passed_count += 1
                    logger.info(f"{test_name} : {status}")
                else:
                    logger.error(f"{test_name} : {status}")
            except Exception as e:
                logger.error(f"{test_name} : ERROR ({str(e)})")

        self.print_summary(passed_count, len(results))

    def test_dummy_creation(self) -> bool:
        """Create controlled dummy data using UTC to avoid timezone drift."""
        try:
            # FIX: Gunakan UTC explicitly agar .timestamp() konsisten dengan Polars
            base_time = datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
            
            # BTC: Data Rapi (10:00 - 10:04 UTC)
            btc_data = {
                "timestamp": [
                    int((base_time + timedelta(minutes=i)).timestamp() * 1000) 
                    for i in range(5)
                ],
                "close": [100.0, 101.0, 102.0, 103.0, 104.0]
            }
            
            # DOGE: Data Kacau
            doge_times = [
                base_time,                          # Match 10:00
                base_time + timedelta(minutes=1, seconds=30), # Late 30s
                base_time + timedelta(minutes=4)    # Match 10:04
            ]
            
            doge_data = {
                "timestamp": [int(t.timestamp() * 1000) for t in doge_times],
                "close": [50.0, 51.5, 54.0]
            }

            self.data_map = {
                "BTC": pl.DataFrame(btc_data).lazy(),
                "DOGE": pl.DataFrame(doge_data).lazy()
            }
            return True
        except Exception as e:
            logger.error(f"Dummy creation failed: {e}")
            return False

    def test_initialization(self) -> bool:
        res = get_aligner(method="asof", tolerance="1m")
        if res.is_ok():
            self.aligner = res.unwrap()
            return True
        return False

    def test_perfect_match(self) -> bool:
        """Scenario 1: Both assets have data at 10:00:00 UTC."""
        if self.df_result is None:
            res = self.aligner.align(self.data_map, strict=False) 
            if res.is_err(): return False
            self.df_result = res.unwrap().collect()
            logger.info("\n--- ALIGNMENT RESULT ---")
            logger.info(f"\n{self.df_result}")
            logger.info("------------------------")

        # FIX: Filter menggunakan UTC datetime
        target_dt = datetime(2024, 1, 1, 10, 0, 0, tzinfo=timezone.utc)
        # Polars Datetime comparison bisa strict, pastikan tidak ada tzinfo mismatch
        # Cara paling aman di Polars: cast kolom timestamp ke string atau hilangkan tz info sebelum compare,
        # TAPI karena strategi kita sudah cast ke Datetime('ms'), biasanya comparison naive vs aware aman di Polars terbaru.
        # Kita coba compare langsung.
        
        # Note: Polars Datetime('ms') biasanya naive (UTC implied).
        # Jadi kita compare dengan naive datetime yang mewakili UTC time.
        target_dt_naive = datetime(2024, 1, 1, 10, 0, 0) # Anggap ini UTC
        
        # Coba filter 1: Naive (jika Polars menyimpan sebagai naive UTC)
        row = self.df_result.filter(pl.col("timestamp") == target_dt_naive)
        
        # Fallback filter 2: Timestamp int match (Pasti akurat)
        if row.height == 0:
             target_ts_int = int(target_dt.timestamp() * 1000)
             # Kita cast balik kolom timestamp ke int untuk matching absolut
             row = self.df_result.filter(pl.col("timestamp").cast(pl.Int64) == target_ts_int)

        if row.height == 0: 
            logger.warning("Row 10:00 UTC not found!")
            return False
        
        return (row["close_BTC"][0] == 100.0) and (row["close_DOGE"][0] == 50.0)

    def test_latency_match(self) -> bool:
        """Scenario 2: Latency Handling. BTC 10:02. DOGE 10:01:30."""
        target_dt = datetime(2024, 1, 1, 10, 2, 0, tzinfo=timezone.utc)
        target_ts_int = int(target_dt.timestamp() * 1000)
        
        row = self.df_result.filter(pl.col("timestamp").cast(pl.Int64) == target_ts_int)
        
        if row.height == 0:
            logger.warning("Row 10:02 UTC not found!")
            return False
            
        doge_val = row["close_DOGE"][0]
        if doge_val == 51.5:
            return True
        else:
            logger.warning(f"Expected 51.5, Got {doge_val}")
            return False

    def test_out_of_tolerance(self) -> bool:
        """Scenario 3: Tolerance Breach. BTC 10:03. DOGE last valid > 1m."""
        target_dt = datetime(2024, 1, 1, 10, 3, 0, tzinfo=timezone.utc)
        target_ts_int = int(target_dt.timestamp() * 1000)
        
        row = self.df_result.filter(pl.col("timestamp").cast(pl.Int64) == target_ts_int)
        
        if row.height == 0: return False
            
        doge_val = row["close_DOGE"][0]
        return doge_val is None

    def print_summary(self, passed, total):
        print("\n" + "="*50)
        print(f"TEST SUMMARY: {passed}/{total} Passed")
        if passed == total:
            print("ALL SYSTEMS GO. LOGIC IS SOUND.")
        else:
            print("LOGIC FAILURE DETECTED.")
        print("="*50 + "\n")

if __name__ == "__main__":
    tester = TestAlignmentLogic()
    tester.run()
