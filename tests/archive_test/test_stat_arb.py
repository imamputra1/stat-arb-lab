"""
UNIT TEST: STATIONARITY ENGINE (TIER 3)
Location: tests/test_stat_arb.py
Focus: Validating Rolling OLS Beta, Spread, and Z-Score integrity.
Standard: Industrial CLI with strict numerical validation.
"""
import sys
from pathlib import Path
import logging
from datetime import datetime, timedelta
from typing import Tuple, List

# --- PATH INJECTION ---
PROJECT_ROOT = Path(__file__).parent.parent.absolute()
sys.path.append(str(PROJECT_ROOT))

import polars as pl
import numpy as np

# Import Target Module
# Pastikan path import sesuai dengan struktur folder city method kita
from research.processing.features.stat_arb import create_stat_arb_transformer

# --- SETUP LOGGING ---
def setup_logging():
    log_dir = PROJECT_ROOT / "logs"
    log_dir.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_filename = log_dir / f"TestStatArb_{timestamp}.log"

    logging.basicConfig(
        format='%(asctime)s | %(levelname)-8s | %(name)-15s | %(message)s',
        datefmt='%H:%M:%S',
        level=logging.INFO,
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(str(log_filename), mode='w')
        ]
    )
    return logging.getLogger("TestStatArb")

logger = setup_logging()

class TestStatArbLogic:
    """Unit tests for Tier 3 Statistical Arbitrage transformer."""

    def run(self) -> bool:
        logger.info("=== STARTING UNIT TEST: TIER 3 STATIONARITY ENGINE ===")
        
        test_cases = [
            ("1. Beta Accuracy Test", self.test_beta_accuracy),
            ("2. Spread Integrity Test", self.test_spread_logic),
            ("3. Z-Score Normalization", self.test_zscore_logic),
            ("4. Numerical Stability", self.test_stability_zero_var),
            ("5. Feature Chaining Check", self.test_chaining_success),
            ("6. Multi-Asset Support  ", self.test_multi_asset_support)
        ]
        
        results = []
        for name, func in test_cases:
            try:
                success, msg = func()
                results.append((name, success, msg))
                if success:
                    logger.info(f"✅ {name}: PASS")
                else:
                    logger.error(f"❌ {name}: FAIL ({msg})")
            except Exception as e:
                logger.error(f"💥 {name}: ERROR ({str(e)})", exc_info=True)
                results.append((name, False, str(e)))
        
        self.print_summary(results)
        return all(r[1] for r in results)

    def _create_linear_data(self, n_rows: int = 200, beta: float = 2.0) -> pl.LazyFrame:
        """Create high-fidelity synthetic data for OLS verification."""
        np.random.seed(42)
        start_date = datetime(2024, 1, 1)
        ts = [start_date + timedelta(minutes=i) for i in range(n_rows)]
        
        # Anchor (X): Linear trend with noise
        btc_log = np.linspace(1.0, 2.0, n_rows) + np.random.normal(0, 0.001, n_rows)
        
        # Target (Y): Beta * X + small noise
        # Rumus: Y = 2X + noise
        doge_log = btc_log * beta + np.random.normal(0, 0.001, n_rows)
        
        return pl.DataFrame({
            "timestamp": ts,
            "log_BTC": btc_log,
            "log_DOGE": doge_log
        }).lazy()

    # --- TEST CASES ---

    def test_beta_accuracy(self) -> Tuple[bool, str]:
        """Verify Beta calculation: Should be close to the synthetic 2.0."""
        expected = 2.0
        df = self._create_linear_data(n_rows=200, beta=expected)
        
        # Gunakan window 60 baris (1 jam)
        transformer = create_stat_arb_transformer(beta_window="60m", anchor_symbol="BTC")
        res = transformer.transform(df)
        
        if res.is_err(): return False, res.error
        
        out = res.unwrap().collect()
        # Ambil rata-rata dari 10 baris terakhir untuk stabilitas
        actual = out["beta_DOGE_BTC"].tail(10).mean()
        
        if abs(actual - expected) < 0.05: # Toleransi 5% karena ada noise
            return True, f"Beta: {actual:.4f}"
        return False, f"Expected ~{expected}, got {actual:.4f}"

    def test_spread_logic(self) -> Tuple[bool, str]:
        """Verify Spread: Should be near-zero for perfectly hedged linear data."""
        df = self._create_linear_data(n_rows=200, beta=2.0)
        transformer = create_stat_arb_transformer(beta_window="60m")
        out = transformer.transform(df).unwrap().collect()
        
        # Spread = Log_Target - (Beta * Log_Anchor)
        spread_mean = out["spread_DOGE"].tail(50).abs().mean()
        
        if spread_mean < 0.01:
            return True, f"Mean Abs Spread: {spread_mean:.6f}"
        return False, f"Spread too wide: {spread_mean:.6f}"

    def test_zscore_logic(self) -> Tuple[bool, str]:
        """Verify Z-Score: Mean should be near 0, Std Dev near 1."""
        df = self._create_linear_data(n_rows=300)
        transformer = create_stat_arb_transformer(zscore_window="120m")
        out = transformer.transform(df).unwrap().collect()
        
        z_scores = out["z_score_DOGE"].tail(100).drop_nulls()
        z_mean = abs(z_scores.mean())
        z_std = z_scores.std()
        
        if z_mean < 0.2 and 0.8 < z_std < 1.2:
            return True, f"Mean={z_mean:.3f}, Std={z_std:.3f}"
        return False, f"Invalid Stats: Mean={z_mean:.3f}, Std={z_std:.3f}"

    def test_stability_zero_var(self) -> Tuple[bool, str]:
        """Numerical Stability: Check behavior when anchor has zero variance."""
        start_date = datetime(2024, 1, 1)
        ts = [start_date + timedelta(minutes=i) for i in range(50)]
        df = pl.DataFrame({
            "timestamp": ts,
            "log_BTC": [10.0] * 50, # ZERO VARIANCE
            "log_DOGE": np.random.normal(5, 0.1, 50)
        }).lazy()
        
        transformer = create_stat_arb_transformer(beta_window="20m")
        res = transformer.transform(df)
        
        if res.is_err(): return False, res.error
        
        out = res.unwrap().collect()
        beta = out["beta_DOGE_BTC"].tail(1).item()
        
        # Harus 0.0 (guardrail lit(1e-12) + fill_null(0.0) di stat_arb.py)
        if beta == 0.0:
            return True, "Stable at 0.0"
        return False, f"Expected 0.0, got {beta}"

    def test_chaining_success(self) -> Tuple[bool, str]:
        """Feature Chaining: Verify all dependency columns are created."""
        df = self._create_linear_data(n_rows=100)
        transformer = create_stat_arb_transformer()
        out = transformer.transform(df).unwrap().collect()
        
        expected = ["beta_DOGE_BTC", "spread_DOGE", "z_score_DOGE"]
        missing = [c for c in expected if c not in out.columns]
        
        if not missing:
            return True, "All features chained"
        return False, f"Missing columns: {missing}"

    def test_multi_asset_support(self) -> Tuple[bool, str]:
        """Check if transformer handles multiple assets against one anchor."""
        start_date = datetime(2024, 1, 1)
        ts = [start_date + timedelta(minutes=i) for i in range(100)]
        df = pl.DataFrame({
            "timestamp": ts,
            "log_BTC": np.random.normal(10, 0.1, 100),
            "log_ETH": np.random.normal(7, 0.1, 100),
            "log_DOGE": np.random.normal(1, 0.1, 100)
        }).lazy()
        
        transformer = create_stat_arb_transformer(anchor_symbol="BTC")
        out = transformer.transform(df).unwrap().collect()
        
        # Harus ada z_score_ETH dan z_score_DOGE
        if "z_score_ETH" in out.columns and "z_score_DOGE" in out.columns:
            return True, "Handled 2 target assets"
        return False, "Failed to compute multi-asset features"

    # --- CLI SUMMARY ---

    def print_summary(self, results: List):
        total = len(results)
        passed = sum(1 for r in results if r[1])
        print("\n" + "="*70)
        print("TIER 3 STATIONARITY ENGINE - UNIT TEST REPORT")
        print("="*70)
        for name, success, msg in results:
            status = "✓ PASS" if success else "✗ FAIL"
            print(f"{status:<8} {name:<25} | {msg}")
        print("-"*70)
        print(f"TOTAL: {passed}/{total} Passed")
        if passed == total:
            print("✅ ENGINE CALIBRATED FOR NODE S (KALMAN FILTER)")
        else:
            print("⚠️  ENGINE SKEW DETECTED - RE-CHECK CHAINING LOGIC")
        print("="*70 + "\n")

if __name__ == "__main__":
    success = TestStatArbLogic().run()
    sys.exit(0 if success else 1)
