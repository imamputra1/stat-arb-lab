"""
NODE S COMPONENT TEST SUITE (THE NUCLEAR FIX)
Location: tests/unit/core/signals/test_signal_node.py
Focus: Integrasi tanpa kompromi. Menguji seluruh organ vital Signal Node.
Status: SUPERIOR PATCHED (Fixture Aliasing & Registry Fix)
"""

import pytest
import polars as pl
import numpy as np
from datetime import datetime, timezone, timedelta

# Core Imports
from core.signals.types import SignalSide, SignalAction
from core.signals.filters import (
    SignalValidator, PositionFilter
)
from core.signals.generator import SignalGenerator
from core.signals.factory import StrategyFactory, StrategyRegistry, StrategyDescriptor
from core.signals.utils import SignalMechanics
from core.signals.strategies.kalman_mr import KalmanMRStrategy

# ============================================================================
# FIXTURES (The Fuel)
# ============================================================================

@pytest.fixture
def component_market_data():
    """
    Data pasar dummy yang DIBERI NAMA KONSISTEN 'component_market_data'.
    Sebelumnya bernama 'sample_market_data' yang menyebabkan error fixture not found.
    """
    n = 100
    dates = [datetime.now(timezone.utc) + timedelta(minutes=i) for i in range(n)]
    # Z-score pattern: Neutral -> Long -> Neutral -> Short -> Neutral
    z_scores = np.concatenate([
        np.zeros(20),           # Neutral
        np.full(20, -3.0),      # Long Trigger
        np.zeros(20),           # Exit/Neutral
        np.full(20, 3.0),       # Short Trigger
        np.zeros(20)            # Exit/Neutral
    ])
    return pl.DataFrame({
        "timestamp": dates,
        "z_score": z_scores,
        "target_price": np.linspace(100, 110, n),
        "volatility": np.full(n, 0.01) # Low volatility
    })

@pytest.fixture
def live_observation():
    """Observasi tunggal untuk pengujian live."""
    return {
        "timestamp": datetime.now(timezone.utc),
        "z_score": -2.5,
        "target_price": 100.0,
        "volatility": 0.02
    }

# ============================================================================
# 1. FILTER COMPONENT TESTS
# ============================================================================

class TestNodeFilters:
    
    def test_validator_integrity(self):
        """Test validasi skema dataframe."""
        validator = SignalValidator()
        
        # DataFrame valid (hasil output generator)
        valid_df = pl.DataFrame({
            "timestamp": [datetime.now()],
            "position": [1],
            "action": [1]
        })
        assert validator.apply(valid_df).is_ok()
        
        # DataFrame tidak valid (kurang kolom)
        invalid_df = pl.DataFrame({"timestamp": [datetime.now()]})
        res = validator.apply(invalid_df)
        assert res.is_err()
        assert "Missing Columns" in str(res.error)

    def test_position_filter_logic(self):
        """Test pemotongan (clipping) eksposur posisi."""
        # Max exposure 1.0, tapi input meminta 2.0
        p_filter = PositionFilter(max_exposure=1.0)
        
        df = pl.DataFrame({
            "position": [2.0, -5.0, 0.5], # Input ngawur
            "strength": [0.8, 0.8, 0.8]
        })
        
        res = p_filter.apply(df)
        assert res.is_ok()
        
        filtered_df = res.unwrap()
        positions = filtered_df["position"].to_list()
        
        # Harus di-clip ke 1.0 dan -1.0
        assert positions[0] == 1.0
        assert positions[1] == -1.0
        assert positions[2] == 0.5

# ============================================================================
# 2. FACTORY COMPONENT TESTS
# ============================================================================

class TestNodeFactory:
    
    def test_strategy_registration(self):
        """Test apakah strategi bisa didaftarkan."""
        # [FIX] Gunakan StrategyRegistry, BUKAN StrategyFactory
        StrategyRegistry.register(StrategyDescriptor(
            name="kalman_mr",
            strategy_class=KalmanMRStrategy,
            version="11.0.0"
        ))
        assert "kalman_mr" in StrategyRegistry._descriptors

    def test_dynamic_strategy_creation(self):
        """Test pembuatan instance strategi."""
        # Pastikan terdaftar dulu (Safety Injection)
        StrategyRegistry.register(StrategyDescriptor(
            name="kalman_mr",
            strategy_class=KalmanMRStrategy,
            version="11.0.0"
        ))
        
        factory = StrategyFactory()
        res = factory.create("kalman_mr", entry_threshold=3.0)
        
        assert res.is_ok()
        strategy = res.unwrap()
        assert strategy.name == "kalman_mr"
        assert strategy.cfg.entry == 3.0 # Parameter override berhasil

    def test_factory_fallback_mechanism(self):
        """Test fallback jika strategi tidak ditemukan."""
        from core.signals.factory import get_signal_strategy
        
        # Inject KalmanMR sebagai default agar fallback aman
        StrategyRegistry.register(StrategyDescriptor(
            name="kalman_mr",
            strategy_class=KalmanMRStrategy,
            version="11.0.0"
        ))
        
        # Minta strategi ngawur, harusnya dapat KalmanMR (default fallback)
        strategy = get_signal_strategy("non_existent_strategy", {})
        assert "KalmanMRStrategy" in repr(strategy)

# ============================================================================
# 3. GENERATOR COMPONENT TESTS (THE ENGINE)
# ============================================================================

class TestNodeGenerator:
    
    # [FIX] Menggunakan nama fixture yang benar: component_market_data
    def test_batch_execution_flow(self, component_market_data):
        """
        Test ALUR LENGKAP Batch Processing:
        Data Masuk -> Strategy -> Generator -> State Machine -> Output
        """
        strategy = KalmanMRStrategy(entry_threshold=2.0, exit_threshold=0.5)
        gen = SignalGenerator(strategy)
        
        res = gen.process_batch(component_market_data)
        assert res.is_ok(), f"Batch process failed: {res.error}"
        
        df = res.unwrap()
        
        # Validasi Output Generator
        assert "position" in df.columns
        assert "action" in df.columns
        
        # Validasi Logika State Machine
        # Z-Score -3.0 (Baris 20-40) harus Long (1)
        long_segment = df.slice(20, 20)
        assert (long_segment["position"] == 1).all()
        
        # Z-Score 3.0 (Baris 60-80) harus Short (-1)
        short_segment = df.slice(60, 20)
        assert (short_segment["position"] == -1).all()

    def test_live_execution_safety(self, live_observation):
        """Test Integrasi Live: Memastikan Generator menjaga State Posisi."""
        strategy = KalmanMRStrategy(entry_threshold=2.0)
        gen = SignalGenerator(strategy)
        
        # 1. First Tick (Entry)
        res1 = gen.process_live(live_observation)
        assert res1.is_ok()
        event1 = res1.unwrap()
        
        assert event1.side == SignalSide.LONG
        assert event1.action == SignalAction.OPEN
        
        # 2. Second Tick (Hold - same observation)
        # Generator harus sadar kita sudah punya posisi, jadi Action = HOLD
        res2 = gen.process_live(live_observation)
        assert res2.is_ok()
        event2 = res2.unwrap()
        
        assert event2.side == SignalSide.LONG # Masih Long
        assert event2.action == SignalAction.HOLD # Action Hold

# ============================================================================
# 4. UTILS COMPONENT TESTS
# ============================================================================

class TestNodeUtils:
    
    def test_vectorized_pnl_calculation(self):
        """Test perhitungan PnL vektor."""
        df = pl.DataFrame({
            "position": [1, 1, 1],
            "target_price": [100, 105, 102]
        })
        
        res = SignalMechanics.calculate_vectorized_pnl(df, price_col="target_price")
        assert res.is_ok()
        
        pnl_df = res.unwrap()
        final_pnl = pnl_df["cumulative_pnl"].to_list()[-1]
        
        # Logic: (105-100) + (102-105) = 5 - 3 = 2
        assert final_pnl == 2.0

if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
