import sys
from pathlib import Path

# --- PATH INJECTION ---
PROJECT_ROOT = Path(__file__).parent.parent.absolute()
sys.path.append(str(PROJECT_ROOT))
# ----------------------

import polars as pl
import logging
from datetime import datetime

# Import Target Module
from research.processing.validation import PolarsValidator, ValidationRules

# --- SETUP LOGGING ---
def setup_logging():
    log_dir = PROJECT_ROOT / "logs"
    log_dir.mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_filename = log_dir / f"TestValidator_{timestamp}.log"

    logging.basicConfig(
        format='%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
        datefmt='%H:%M:%S',
        level=logging.INFO,
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(str(log_filename), mode='w')
        ]
    )
    return logging.getLogger("TestValidator")

logger = setup_logging()

class TestValidatorLogic:
    
    def run(self):
        logger.info("--- STARTING UNIT TEST: DATA VALIDATOR ---")
        
        # Daftar Test Case
        tests = {
            "1. Init & Config      ": self.test_initialization,
            "2. Schema Check       ": self.test_missing_columns,
            "3. Min Rows (Pass)    ": self.test_min_rows_pass,
            "4. Min Rows (Fail)    ": self.test_min_rows_fail,
            "5. Max Nulls (Pass)   ": self.test_nulls_pass,
            "6. Max Nulls (Fail)   ": self.test_nulls_fail,
            "7. Integrity (Sort)   ": self.test_sorting_fail,
            "8. Integrity (Price)  ": self.test_negative_price,
        }
        
        passed = 0
        self.validator = None # State holder
        
        for name, func in tests.items():
            try:
                if func():
                    logger.info(f"{name} : PASS")
                    passed += 1
                else:
                    logger.error(f"{name} : FAIL")
            except Exception as e:
                logger.error(f"{name} : ERROR ({e})")
                
        self.print_summary(passed, len(tests))

    def _create_dummy(self, rows=100, null_ratio=0.0, sorted_ts=True):
        """Helper to generate specific bad/good data"""
        data = {
            "timestamp": [i for i in range(rows)],
            "close": [100.0] * rows,
            "high": [105.0] * rows,
            "low": [95.0] * rows,
            "open": [100.0] * rows,
            "volume": [1000.0] * rows
        }
        
        # Inject Nulls
        if null_ratio > 0:
            n_nulls = int(rows * null_ratio)
            # Timpa 'close' dengan None di akhir
            data["close"] = [100.0] * (rows - n_nulls) + [None] * n_nulls
            
        # Mess up sorting
        if not sorted_ts:
            data["timestamp"] = data["timestamp"][::-1] # Reverse
            
        return pl.DataFrame(data).lazy()

    # --- TEST CASES ---

    def test_initialization(self):
        # Setup Rules: Min 10 baris, Max 10% null
        rules = ValidationRules(min_rows=10, max_null_pct=0.10)
        self.validator = PolarsValidator(default_rules=rules)
        return True

    def test_missing_columns(self):
        # Kirim data tanpa kolom 'close'
        df = pl.DataFrame({"timestamp": [1, 2, 3]}).lazy()
        res = self.validator.validate(df)
        
        # Harus Error karena Rules default butuh 'close'
        if res.is_err() and "Missing required columns" in res.error:
            return True
        logger.warning(f"Failed to catch missing columns. Got: {res}")
        return False

    def test_min_rows_pass(self):
        # 20 baris (Rules min 10) -> OK
        df = self._create_dummy(rows=20)
        res = self.validator.validate(df)
        return res.is_ok()

    def test_min_rows_fail(self):
        # 5 baris (Rules min 10) -> Error
        df = self._create_dummy(rows=5)
        res = self.validator.validate(df)
        
        if res.is_err() and "Insufficient Data" in res.error:
            return True
        logger.warning(f"Failed to catch min rows. Got: {res}")
        return False

    def test_nulls_pass(self):
        # 100 baris, 5% null (Rules max 10%) -> OK
        df = self._create_dummy(rows=100, null_ratio=0.05)
        res = self.validator.validate(df)
        return res.is_ok()

    def test_nulls_fail(self):
        # 100 baris, 20% null (Rules max 10%) -> Error
        df = self._create_dummy(rows=100, null_ratio=0.20)
        res = self.validator.validate(df)
        
        if res.is_err() and "Data Quality Bad" in res.error:
            return True
        logger.warning(f"Failed to catch nulls. Got: {res}")
        return False

    def test_sorting_fail(self):
        # Timestamp terbalik -> Error
        df = self._create_dummy(rows=20, sorted_ts=False)
        # Pastikan check_sorted aktif
        res = self.validator.validate(df, rules={"check_sorted": True})
        
        if res.is_err() and "not sorted" in res.error:
            return True
        logger.warning(f"Failed to catch unsorted data. Got: {res}")
        return False

    def test_negative_price(self):
        # Price Check: Close = -50
        df = pl.DataFrame({
            "timestamp": range(20),
            "close": [-50.0] * 20,
            "open": [10.0] * 20,
            "high": [10.0] * 20,
            "low": [5.0] * 20
        }).lazy()
        
        res = self.validator.validate(df, rules={"check_ohlc_consistency": True})
        
        if res.is_err() and "Price <= 0" in res.error:
            return True
        logger.warning(f"Failed to catch negative price. Got: {res}")
        return False

    def print_summary(self, passed, total):
        print("\n" + "="*50)
        print(f"VALIDATOR STATUS: {passed}/{total} Scenarios Passed")
        if passed == total:
            print("✅ GATEKEEPER IS ACTIVE & STRICT.")
        else:
            print("❌ SECURITY BREACH DETECTED.")
        print("="*50 + "\n")

if __name__ == "__main__":
    TestValidatorLogic().run()
