import sys
from pathlib import Path
import math

# Path Injection
PROJECT_ROOT = Path(__file__).parent.parent.absolute()
sys.path.append(str(PROJECT_ROOT))

import polars as pl
import logging
from datetime import datetime

# Import Target Module
from research.processing.transformation import create_log_returns_transformer

# --- SETUP LOGGING ---
def setup_logging():
    log_dir = PROJECT_ROOT / "logs"
    log_dir.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_filename = log_dir / f"TestReturns_{timestamp}.log"

    logging.basicConfig(
        format='%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
        datefmt='%H:%M:%S',
        level=logging.INFO,
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(str(log_filename), mode='w')
        ]
    )
    return logging.getLogger("TestReturns")

logger = setup_logging()

class TestReturnsLogic:
    
    def run(self):
        logger.info("=== STARTING UNIT TEST: TIER 1 RETURNS ===")
        
        tests = {
            "1. Basic Math Check     ": self.test_basic_math,
            "2. Zero/Neg Handling    ": self.test_zero_handling,
            "3. Auto Column Detection": self.test_auto_detection,
            "4. Missing Column Check ": self.test_missing_column
        }
        
        passed = 0
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

    def test_basic_math(self) -> bool:
        """
        Verify:
        y_t = ln(100)
        r_t = ln(100) - ln(prev)
        """
        # Data: 100 -> 110 (Naik 10%)
        df = pl.DataFrame({
            "timestamp": [1, 2],
            "close_BTC": [100.0, 110.0]
        }).lazy()
        
        transformer = create_log_returns_transformer()
        res = transformer.transform(df)
        
        if res.is_err(): return False
        
        out = res.unwrap().collect()
        
        # Check Columns
        if "log_BTC" not in out.columns or "ret_BTC" not in out.columns:
            return False
            
        # Check Value Row 0 (First return must be 0.0)
        ret_0 = out["ret_BTC"][0]
        if ret_0 != 0.0:
            logger.warning(f"Row 0 return must be 0.0, got {ret_0}")
            return False
            
        # Check Value Row 1
        # Log Return of 100->110 should be ln(110/100) = ln(1.1) ~= 0.09531
        expected_ret = math.log(110.0 / 100.0)
        actual_ret = out["ret_BTC"][1]
        
        if abs(actual_ret - expected_ret) > 1e-6:
            logger.warning(f"Math mismatch. Expected {expected_ret}, got {actual_ret}")
            return False
            
        return True

    def test_zero_handling(self) -> bool:
        """Verify replacement of 0.0 to prevent -inf."""
        df = pl.DataFrame({
            "timestamp": [1],
            "close_DOGE": [0.0] # Fatal for Log
        }).lazy()
        
        transformer = create_log_returns_transformer(replace_zeros=True)
        res = transformer.transform(df)
        
        out = res.unwrap().collect()
        log_val = out["log_DOGE"][0]
        
        # log(epsilon) should be a finite negative number, not -inf
        if math.isinf(log_val):
            logger.warning("Got -inf, zero replacement failed.")
            return False
            
        return True

    def test_auto_detection(self) -> bool:
        """Verify it picks up 'close_' columns automatically."""
        df = pl.DataFrame({
            "timestamp": [1],
            "close_A": [10.0],
            "close_B": [20.0],
            "volume": [1000]
        }).lazy()
        
        transformer = create_log_returns_transformer()
        out = transformer.transform(df).unwrap().collect()
        
        has_A = "ret_A" in out.columns
        has_B = "ret_B" in out.columns
        no_Vol = "ret_volume" not in out.columns # Volume shouldn't be touched
        
        return has_A and has_B and no_Vol

    def test_missing_column(self) -> bool:
        """Verify behavior when specific target is missing."""
        df = pl.DataFrame({"close_A": [10.0]}).lazy()
        
        # Minta close_B padahal tidak ada
        transformer = create_log_returns_transformer(target_columns=["close_B"])
        res = transformer.transform(df)
        
        # Harusnya tidak crash, tapi mungkin return Err atau warning di log
        # Implementasi kita saat ini: return Err("LogReturns: No target columns found...")
        # Karena close_B tidak ada, list target jadi kosong, maka dia return Err.
        
        if res.is_err():
            return True
        return False

    def print_summary(self, passed, total):
        print("\n" + "="*50)
        print(f"RETURNS TEST: {passed}/{total} Passed")
        if passed == total:
            print("TIER 1 MATH ENGINE SECURE.")
        else:
            print("MATH ENGINE FAILURE.")
        print("="*50 + "\n")

if __name__ == "__main__":
    TestReturnsLogic().run()
