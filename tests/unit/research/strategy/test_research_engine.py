""""
COMPONENT TEST: RESEARCH STRATEGY ENGINE (SYNCED WITH SMART ENGINE)
Location: tests/unit/research/strategy/test_research_engine.py
Focus: Validasi mesin backtest Vectorized dengan metrik presisi.
Status: COMPLIANT & SYNCED.
"""

import pytest
import polars as pl
import numpy as np
from datetime import datetime, timezone, timedelta

# 1. Core Imports (Logic Provider)
from core.signals.strategies.kalman_mr import KalmanMRStrategy

# 2. Research Strategy Imports (System Under Test)
from research.strategy.engine import VectorizedBacktestEngine
from research.strategy.optimization import ParameterSpace, Number
from research.strategy import BacktestPipeline

# ============================================================================
# FIXTURES
# ============================================================================

@pytest.fixture
def backtest_data():
    """
    Data sintetis untuk skenario profit.
    """
    n = 200
    dates = [datetime.now(timezone.utc) + timedelta(minutes=i) for i in range(n)]
    
    # Skenario: 
    # 0-50: Flat
    # 50-100: Drop tajam (Trigger Long di z=-3) -> Harga 100 naik ke 110
    # 100-150: Flat/Recovery
    # 150-200: Spike tajam (Trigger Short di z=3)
    
    z_scores = np.concatenate([
        np.zeros(50),           
        np.full(50, -3.0),      # ENTRY LONG
        np.zeros(50),           # EXIT 
        np.full(50, 3.0)        # ENTRY SHORT
    ])
    
    # Harga naik perlahan (Bullish bias) agar Long untung
    prices = np.linspace(100, 120, n) 
    
    return pl.DataFrame({
        "timestamp": dates,
        "z_score": z_scores,
        "target_price": prices,
        # Engine baru menghitung return sendiri dari target_price,
        # tapi kolom returns kadang dibutuhkan indikator lain.
        "returns": np.random.normal(0, 0.001, n) 
    })

# ============================================================================
# 1. UNIT TEST: PARAMETER SPACE
# ============================================================================

class TestParameterSpace:
    
    def test_space_definition(self):
        """Memastikan ruang parameter terdefinisi dengan benar."""
        space = ParameterSpace()
        space.add("entry", Number(1.0, 3.0, step=0.5))
        space.add("exit", Number(0.0, 1.0, step=0.1))
        
        grid = space.generate_grid()
        assert len(grid) > 0
        assert "entry" in grid[0]
        
        entries = [p["entry"] for p in grid]
        assert min(entries) >= 1.0
        assert max(entries) <= 3.0

# ============================================================================
# 2. COMPONENT TEST: VECTORIZED ENGINE
# ============================================================================

class TestVectorizedEngine:
    
    def test_simulation_mechanics(self, backtest_data):
        """
        Test inti simulasi dengan Engine 'Smart'.
        """
        # 1. Setup
        strategy = KalmanMRStrategy(entry_threshold=2.0, exit_threshold=0.5)
        # Gunakan parameter default atau custom
        engine = VectorizedBacktestEngine(initial_capital=10_000.0, transaction_cost_pct=0.001)
        
        # 2. Run
        res = engine.run(backtest_data, strategy)
        assert res.is_ok(), f"Simulation failed: {res.error}"
        
        results = res.unwrap()
        metrics = results["metrics"]
        
        # 3. Validasi Metrik (SESUAI KEYS BARU DI SMART ENGINE)
        # Perhatikan suffix '_pct' yang baru
        assert "total_return_pct" in metrics
        assert "sharpe_ratio" in metrics
        assert "max_drawdown_pct" in metrics
        assert "total_trades" in metrics
        
        # 4. Validasi Logika Profit
        # Karena harga naik dan kita Long, harusnya profit positif
        assert metrics["total_return_pct"] > 0.0 
        assert metrics["total_trades"] > 0
        
        # 5. Validasi Output DataFrames
        assert not results["equity_curve"].is_empty()
        assert "trade_log" in results

    def test_engine_empty_data(self):
        """Test ketahanan engine terhadap data kosong."""
        strategy = KalmanMRStrategy()
        engine = VectorizedBacktestEngine()
        empty_df = pl.DataFrame({"timestamp": [], "z_score": [], "target_price": []})
        
        res = engine.run(empty_df, strategy)
        assert res.is_ok() 
        
        metrics = res.unwrap()["metrics"]
        assert metrics["total_trades"] == 0
        assert metrics["total_return_pct"] == 0.0

# ============================================================================
# 3. INTEGRATION TEST: PIPELINE
# ============================================================================

class TestBacktestPipeline:
    
    def test_pipeline_execution(self, backtest_data):
        """
        Test alur lengkap Pipeline -> Engine -> Strategy.
        """
        pipeline = BacktestPipeline()
        
        config = {
            "strategy": "kalman_mr",
            "capital": 50_000,
            "params": {"entry_threshold": 2.5, "exit_threshold": 0.1}
        }
        
        # Fallback method checking
        if hasattr(pipeline, 'run'):
            res = pipeline.run(backtest_data, config)
        elif hasattr(pipeline, 'execute'):
            res = pipeline.execute(backtest_data, config)
        else:
            # Jika pipeline belum diimplementasi penuh, kita skip atau fail
            # Untuk sekarang asumsi fail jika tidak ada method
            pytest.fail("Pipeline belum memiliki method run() atau execute()")

        assert res.is_ok()
        output = res.unwrap()
        
        # Validasi Integrasi
        assert "metrics" in output
        # Pipeline harus meneruskan metrik dari engine baru
        assert "total_return_pct" in output["metrics"] or "total_return" in output["metrics"]

if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
