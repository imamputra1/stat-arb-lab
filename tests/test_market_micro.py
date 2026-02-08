"""
UNIT TEST: MICROSTRUCTURE FEATURES (TIER 2)
Location: tests/test_market_micro.py
Focus: Validating Rolling Volatility and Correlation logic.
Standard: Industrial CLI
"""
import sys
from pathlib import Path

# Path Injection
PROJECT_ROOT = Path(__file__).parent.parent.absolute()
sys.path.append(str(PROJECT_ROOT))

import polars as pl
import logging
from datetime import datetime, timedelta

# Import Target Module
from research.processing.features import create_microstructure_transformer

# --- SETUP LOGGING ---
def setup_logging():
    log_dir = PROJECT_ROOT / "logs"
    log_dir.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_filename = log_dir / f"TestMicro_{timestamp}.log"

    logging.basicConfig(
        format='%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
        datefmt='%H:%M:%S',
        level=logging.INFO,
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(str(log_filename), mode='w')
        ]
    )
    return logging.getLogger("TestMicro")

logger = setup_logging()

class TestMicroLogic:
    
    def run(self):
        logger.info("=== STARTING UNIT TEST: TIER 2 FEATURES ===")
        
        tests = {
            "1. Volatility Calc     ": self.test_volatility_calculation,
            "2. Correlation (Perf)  ": self.test_perfect_correlation,
            "3. Correlation (Neg)   ": self.test_negative_correlation,
            "4. Missing Anchor      ": self.test_missing_anchor,
            "5. Window Generation   ": self.test_window_generation
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

    def _create_dummy(self, n_rows=120): # Increased rows for stability
        """Buat data dummy dengan Timestamp (Datetime) yang proper"""
        start_date = datetime(2024, 1, 1)
        
        # Buat list datetime object, bukan int
        ts = [start_date + timedelta(minutes=i) for i in range(n_rows)]
        
        return pl.DataFrame({
            "timestamp": ts,
            "ret_BTC": [0.01 if i%2==0 else -0.01 for i in range(n_rows)],
            "ret_STABLE": [0.0 for _ in range(n_rows)],
            "ret_DOGE": [0.01 if i%2==0 else -0.01 for i in range(n_rows)] 
        }).lazy()

    def test_volatility_calculation(self) -> bool:
        df = self._create_dummy()
        transformer = create_microstructure_transformer(windows=["1h"])
        res = transformer.transform(df)
        
        if res.is_err(): 
            logger.error(res.error)
            return False
            
        out = res.unwrap().collect()
        
        if "vol_BTC_1h" not in out.columns: return False
        
        vol_stable = out["vol_STABLE_1h"].tail(1).item()
        if vol_stable != 0.0:
            logger.warning(f"Stable asset vol should be 0.0, got {vol_stable}")
            return False
            
        vol_btc = out["vol_BTC_1h"].tail(1).item()
        if vol_btc <= 0.0:
            logger.warning(f"Volatile asset vol should be > 0.0, got {vol_btc}")
            return False
            
        return True

    def test_perfect_correlation(self) -> bool:
        df = self._create_dummy()
        transformer = create_microstructure_transformer(windows=["1h"], anchor_symbol="BTC")
        out = transformer.transform(df).unwrap().collect()
        
        corr_val = out["corr_DOGE_BTC_1h"].tail(1).item()
        if abs(corr_val - 1.0) < 1e-6:
            return True
        logger.warning(f"Expected Corr 1.0, Got {corr_val}")
        return False

    def test_negative_correlation(self) -> bool:
        # Create inverse data
        n_rows = 120
        start_date = datetime(2024, 1, 1)
        ts = [start_date + timedelta(minutes=i) for i in range(n_rows)]
        btc_ret = [0.01 if i%2==0 else -0.01 for i in range(n_rows)]
        inv_ret = [-x for x in btc_ret]
        
        df = pl.DataFrame({
            "timestamp": ts,
            "ret_BTC": btc_ret,
            "ret_INVERSE": inv_ret
        }).lazy()
        
        transformer = create_microstructure_transformer(windows=["1h"], anchor_symbol="BTC")
        out = transformer.transform(df).unwrap().collect()
        
        corr_val = out["corr_INVERSE_BTC_1h"].tail(1).item()
        if abs(corr_val - (-1.0)) < 1e-6:
            return True
        logger.warning(f"Expected Corr -1.0, Got {corr_val}")
        return False

    def test_missing_anchor(self) -> bool:
        df = self._create_dummy()
        df_no_anchor = df.drop("ret_BTC")
        transformer = create_microstructure_transformer(anchor_symbol="BTC")
        res = transformer.transform(df_no_anchor)
        
        if res.is_err(): return False
        out = res.unwrap().collect()
        
        has_vol = "vol_DOGE_1h" in out.columns
        no_corr = "corr_DOGE_BTC_1h" not in out.columns
        return has_vol and no_corr

    def test_window_generation(self) -> bool:
        df = self._create_dummy()
        windows = ["5m", "15m"]
        transformer = create_microstructure_transformer(windows=windows)
        out = transformer.transform(df).unwrap().collect()
        
        cols = out.columns
        check_5m = "vol_BTC_5m" in cols
        check_15m = "vol_BTC_15m" in cols
        return check_5m and check_15m

    def print_summary(self, passed, total):
        print("\n" + "="*50)
        print(f"TIER 2 TEST: {passed}/{total} Passed")
        if passed == total:
            print("✅ MARKET SENSORS ONLINE.")
        else:
            print("❌ SENSOR CALIBRATION FAILED.")
        print("="*50 + "\n")

if __name__ == "__main__":
    TestMicroLogic().run()
