"""
UNIT TESTS: KALMAN MEAN REVERSION STRATEGY V11.0 (FULL SUITE PATCHED)
Location: tests/unit/core/signals/strategies/test_kalman_mr.py
Focus: Comprehensive testing including Performance & Memory metrics.
Status: FULL 41 TESTS RESTORED & PATCHED.
"""

import pytest
import polars as pl
import numpy as np
from typing import Dict, Any
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass
import time
import psutil
import os

# Core Signal Components
from core.signals.strategies.kalman_mr import KalmanMRStrategy, ThresholdConfig
from core.signals.types import SignalSide
from core.shared import Ok, Err

# ============================================================================
# TEST DATA MODELS
# ============================================================================

@dataclass(frozen=True)
class ScenarioData:
    name: str
    description: str
    input_data: Dict[str, Any]
    expected_output: Dict[str, Any]
    test_type: str 
    
    @property
    def is_valid(self) -> bool:
        return all(key in self.input_data for key in ['timestamp', 'z_score', 'target_price'])

@dataclass(frozen=True)
class ScenarioResult:
    scenario_name: str
    passed: bool
    actual_output: Any
    expected_output: Any
    execution_time_ms: float = 0.0
    metadata: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.metadata is None:
            object.__setattr__(self, 'metadata', {})

# ============================================================================
# FIXTURES
# ============================================================================

@pytest.fixture
def base_strategy() -> KalmanMRStrategy:
    return KalmanMRStrategy(
        entry_threshold=2.0, exit_threshold=0.5, adaptive=False, sensitivity=1.0, name="test_kalman_mr"
    )

@pytest.fixture
def adaptive_strategy() -> KalmanMRStrategy:
    return KalmanMRStrategy(
        entry_threshold=2.0, exit_threshold=0.5, adaptive=True, sensitivity=1.0, name="test_adaptive_kalman_mr"
    )

@pytest.fixture
def sample_dataframe() -> pl.DataFrame:
    timestamps = [datetime.now(timezone.utc) - timedelta(minutes=i) for i in range(100)]
    z_scores = np.concatenate([
        np.random.normal(-2.5, 0.5, 25),
        np.random.normal(0.0, 0.3, 25),
        np.random.normal(2.5, 0.5, 25),
        np.random.normal(-1.0, 0.3, 25)
    ])
    return pl.DataFrame({
        "timestamp": timestamps,
        "z_score": z_scores,
        "target_price": np.random.uniform(95.0, 105.0, 100)
    })

# ============================================================================
# CORE TESTS (FULL SUITE)
# ============================================================================

class TestKalmanMRStrategy:
    
    # --- Initialization ---
    def test_strategy_initialization(self, base_strategy: KalmanMRStrategy):
        assert base_strategy.name == "test_kalman_mr"
        assert base_strategy.version == "11.0.0"
        assert base_strategy.cfg.entry == 2.0
        assert base_strategy.cfg.exit == 0.5
        assert base_strategy.adaptive is False
        assert base_strategy.sensitivity == 1.0
        assert "KalmanMRStrategy" in repr(base_strategy)
    
    def test_threshold_config_validation(self):
        result = ThresholdConfig(entry=1.0, exit=1.5, extreme=2.0).validate()
        assert result.is_err()
        assert "Threshold Ilegal" in str(result.error)

    def test_data_requirements(self, base_strategy: KalmanMRStrategy):
        reqs = base_strategy.data_requirements
        assert 'z_score' in reqs
        assert reqs['z_score'].data_type == pl.Float64
        assert reqs['timestamp'].data_type == pl.Datetime

    # --- Batch Processing (PATCHED for Polars/Series TypeError) ---
    def test_batch_signal_generation_success(self, base_strategy: KalmanMRStrategy, sample_dataframe: pl.DataFrame):
        result = base_strategy.generate_signals(sample_dataframe)
        assert isinstance(result, Ok)
        
        signals_df = result.unwrap()
        
        assert "side" in signals_df.columns
        assert "strength" in signals_df.columns
        assert "strategy_name" in signals_df.columns
        
        # [PATCH] Gunakan String Check untuk menghindari TypeError Polars Expr
        assert "Int64" in str(signals_df["side"].dtype)
        assert "Float64" in str(signals_df["strength"].dtype)
        
        # [PATCH] Validasi Set menggunakan Python Native types (drop_nulls dulu)
        valid_sides = {1, -1, 0}
        unique_sides = set(signals_df["side"].drop_nulls().to_list())
        assert unique_sides.issubset(valid_sides)
        
        # Check strategy info
        assert signals_df["strategy_name"][0] == "test_kalman_mr"
        assert signals_df["strategy_version"][0] == "11.0.0"

    # --- Extreme Cases ---
    def test_batch_extreme_long_signals(self, base_strategy: KalmanMRStrategy):
        df = pl.DataFrame({"timestamp": [datetime.now(timezone.utc)], "z_score": [-5.0], "target_price": [100.0]})
        signals = base_strategy.generate_signals(df).unwrap()
        assert signals["side"][0] == SignalSide.LONG.value
        assert signals["strength"][0] >= 0.5

    def test_batch_extreme_short_signals(self, base_strategy: KalmanMRStrategy):
        df = pl.DataFrame({"timestamp": [datetime.now(timezone.utc)], "z_score": [5.0], "target_price": [100.0]})
        signals = base_strategy.generate_signals(df).unwrap()
        assert signals["side"][0] == SignalSide.SHORT.value

    def test_batch_neutral_signals(self, base_strategy: KalmanMRStrategy):
        df = pl.DataFrame({"timestamp": [datetime.now(timezone.utc)], "z_score": [0.3], "target_price": [100.0]})
        signals = base_strategy.generate_signals(df).unwrap()
        assert signals["side"][0] == SignalSide.NEUTRAL.value

    def test_batch_hold_signals(self, base_strategy: KalmanMRStrategy):
        # [PATCH] Pengecekan HOLD (None)
        df = pl.DataFrame({"timestamp": [datetime.now(timezone.utc)], "z_score": [1.0], "target_price": [100.0]})
        signals = base_strategy.generate_signals(df).unwrap()
        # Gunakan 'is None' standar Python (list access)
        assert signals["side"].to_list()[0] is None

    # --- Edge Cases & Errors ---
    def test_batch_missing_data(self, base_strategy: KalmanMRStrategy):
        df = pl.DataFrame({"timestamp": [datetime.now(timezone.utc)], "volume": [1000.0]})
        result = base_strategy.generate_signals(df)
        assert isinstance(result, Err)
        assert "z_score" in str(result.error).lower() or "target_price" in str(result.error).lower()

    def test_batch_null_values(self, base_strategy: KalmanMRStrategy):
        df = pl.DataFrame({
            "timestamp": [datetime.now(timezone.utc)] * 2,
            "z_score": [None, -2.5],
            "target_price": [100.0] * 2
        })
        result = base_strategy.generate_signals(df)
        assert isinstance(result, Ok)
        signals = result.unwrap()
        sides = signals["side"].to_list()
        # [PATCH] Use 'is None'
        assert sides[0] is None 
        assert sides[1] == SignalSide.LONG.value

    # --- Live Processing (PATCHED) ---
    def test_live_long_signal_generation(self, base_strategy: KalmanMRStrategy):
        obs = {"timestamp": datetime.now(timezone.utc), "z_score": -2.5, "target_price": 100.0}
        event = base_strategy.evaluate_state(obs).unwrap()
        assert event.side == SignalSide.LONG
        assert event.strength.value > 0.0

    def test_live_short_signal_generation(self, base_strategy: KalmanMRStrategy):
        obs = {"timestamp": datetime.now(timezone.utc), "z_score": 2.5, "target_price": 100.0}
        event = base_strategy.evaluate_state(obs).unwrap()
        assert event.side == SignalSide.SHORT

    def test_live_exit_signal_generation(self, base_strategy: KalmanMRStrategy):
        obs = {"timestamp": datetime.now(timezone.utc), "z_score": 0.3, "target_price": 100.0}
        event = base_strategy.evaluate_state(obs).unwrap()
        assert event.side == SignalSide.NEUTRAL

    def test_live_hold_signal_generation(self, base_strategy: KalmanMRStrategy):
        """Restored test case."""
        obs = {"timestamp": datetime.now(timezone.utc), "z_score": 1.0, "target_price": 100.0}
        event = base_strategy.evaluate_state(obs).unwrap()
        assert event.side == SignalSide.NEUTRAL
        assert event.strength.value == 0.0

    def test_live_missing_required_fields(self, base_strategy: KalmanMRStrategy):
        """Test live evaluation with missing required fields."""
        obs = {"timestamp": datetime.now(timezone.utc)} # Missing z_score
        result = base_strategy.evaluate_state(obs)
        assert isinstance(result, Err)
        assert "z_score" in str(result.error).lower() or "missing" in str(result.error).lower()

    # --- Adaptive Logic ---
    def test_adaptive_threshold_calculation(self, adaptive_strategy: KalmanMRStrategy, sample_dataframe: pl.DataFrame):
        signals = adaptive_strategy.generate_signals(sample_dataframe).unwrap()
        assert "dyn_entry" in signals.columns
        assert "dyn_exit" in signals.columns
        assert (signals["dyn_exit"] < signals["dyn_entry"]).all()

    def test_sensitivity_impact_on_thresholds(self, sample_dataframe: pl.DataFrame):
        low = KalmanMRStrategy(sensitivity=0.5, adaptive=True).generate_signals(sample_dataframe).unwrap()
        high = KalmanMRStrategy(sensitivity=2.0, adaptive=True).generate_signals(sample_dataframe).unwrap()
        # Higher sensitivity should result in lower thresholds
        assert high["dyn_entry"].mean() <= low["dyn_entry"].mean()

    def test_sensitivity_impact_on_trades(self, sample_dataframe: pl.DataFrame):
        low = KalmanMRStrategy(sensitivity=0.8).generate_signals(sample_dataframe).unwrap()
        high = KalmanMRStrategy(sensitivity=2.5).generate_signals(sample_dataframe).unwrap()
        
        count_low = low.filter(pl.col("side").is_not_null()).height
        count_high = high.filter(pl.col("side").is_not_null()).height
        assert count_high >= count_low

    # --- Performance Tests (RESTORED) ---
    def test_batch_performance_large_dataset(self, base_strategy: KalmanMRStrategy):
        n_rows = 10000
        timestamps = [datetime.now(timezone.utc) - timedelta(minutes=i) for i in range(n_rows)]
        df = pl.DataFrame({
            "timestamp": timestamps,
            "z_score": np.random.normal(0, 2, n_rows),
            "target_price": np.random.uniform(90.0, 110.0, n_rows)
        })
        
        start_time = time.perf_counter()
        base_strategy.generate_signals(df)
        duration_ms = (time.perf_counter() - start_time) * 1000
        
        print(f"\n⚡ Performance: {n_rows} rows in {duration_ms:.2f} ms")
        assert duration_ms < 5000 

    def test_memory_efficiency(self, base_strategy: KalmanMRStrategy):
        process = psutil.Process(os.getpid())
        mem_before = process.memory_info().rss / 1024 / 1024
        
        n_rows = 5000
        df = pl.DataFrame({
            "timestamp": [datetime.now(timezone.utc)] * n_rows,
            "z_score": np.random.normal(0, 2, n_rows),
            "target_price": [100.0] * n_rows
        })
        base_strategy.generate_signals(df)
        
        mem_after = process.memory_info().rss / 1024 / 1024
        assert (mem_after - mem_before) < 500

    # --- Edge Case & Robustness (RESTORED) ---
    def test_invalid_threshold_configuration(self):
        with pytest.raises(ValueError):
            KalmanMRStrategy(entry_threshold=1.0, exit_threshold=1.5)

    def test_extreme_threshold_values(self):
        strategy = KalmanMRStrategy(entry_threshold=5.0, exit_threshold=2.0, adaptive=False)
        df = pl.DataFrame({"timestamp": [datetime.now(timezone.utc)], "z_score": [3.0], "target_price": [100.0]})
        signals = strategy.generate_signals(df).unwrap()
        assert signals["side"].to_list()[0] is None

    # --- Dirty Data Tests (PATCHED) ---
    def test_nan_and_inf_values(self, base_strategy: KalmanMRStrategy):
        df = pl.DataFrame({
            "timestamp": [datetime.now(timezone.utc)] * 3,
            "z_score": [np.nan, np.inf, -np.inf],
            "target_price": [100.0] * 3
        })
        signals = base_strategy.generate_signals(df).unwrap()
        # [PATCH] Use 'is None' and list check
        sides = signals["side"].to_list()
        assert sides == [None, None, None]

    def test_very_small_dataset(self, base_strategy: KalmanMRStrategy):
        # Empty DF
        assert base_strategy.generate_signals(pl.DataFrame()).unwrap().is_empty()
        
        # Single Row
        df = pl.DataFrame({"timestamp": [datetime.now(timezone.utc)], "z_score": [-5.0], "target_price": [100.0]})
        signals = base_strategy.generate_signals(df).unwrap()
        assert signals["side"][0] == 1 # LONG

    def test_zero_volatility_scenario(self):
        strategy = KalmanMRStrategy(adaptive=True, sensitivity=1.0)
        df = pl.DataFrame({
            "timestamp": [datetime.now(timezone.utc) - timedelta(minutes=i) for i in range(100)],
            "z_score": [2.5] * 100,
            "target_price": [100.0] * 100
        })
        signals = strategy.generate_signals(df).unwrap()
        assert "dyn_entry" in signals.columns

    # --- Strategy Interface Tests (RESTORED) ---
    def test_strategy_name_and_version(self, base_strategy: KalmanMRStrategy):
        assert base_strategy.name == "test_kalman_mr"
        assert base_strategy.version == "11.0.0"

    def test_strategy_data_validation(self, base_strategy: KalmanMRStrategy):
        valid_df = pl.DataFrame({"timestamp": [datetime.now(timezone.utc)], "z_score": [-2.5], "target_price": [100.0]})
        assert base_strategy.validate_data(valid_df).is_valid
        
        invalid_df = pl.DataFrame({"timestamp": [datetime.now(timezone.utc)], "z_score": [-2.5]})
        assert not base_strategy.validate_data(invalid_df).is_valid

    def test_original_trade_generation(self):
        df = pl.DataFrame({"timestamp": [datetime.now(timezone.utc)], "z_score": [-5.0], "target_price": [100.0]})
        strategy = KalmanMRStrategy(entry_threshold=2.0)
        signals = strategy.generate_signals(df).unwrap()
        assert signals["side"][0] == SignalSide.LONG.value

    # --- Parameterized Tests ---
    @pytest.mark.parametrize("z_score,expected_side", [
        (-3.0, 1), (-2.1, 1), (-1.9, None), (-0.4, 0), (0.0, 0), (0.4, 0), (1.9, None), (2.1, -1), (3.0, -1)
    ])
    def test_parameterized_z_scores(self, z_score: float, expected_side: int, base_strategy: KalmanMRStrategy):
        df = pl.DataFrame({
            "timestamp": [datetime.now(timezone.utc)],
            "z_score": [z_score],
            "target_price": [100.0]
        })
        signals = base_strategy.generate_signals(df).unwrap()
        actual = signals["side"][0]
        
        # [PATCH] Python native comparison
        if expected_side is None:
            assert actual is None
        else:
            assert actual == expected_side

    @pytest.mark.parametrize("entry,exit_thr", [(1.5, 0.3), (2.0, 0.5), (2.5, 0.8), (3.0, 1.0)])
    def test_parameterized_thresholds(self, entry: float, exit_thr: float):
        strategy = KalmanMRStrategy(entry_threshold=entry, exit_threshold=exit_thr)
        df = pl.DataFrame({
            "timestamp": [datetime.now(timezone.utc)],
            "z_score": [-(entry + 0.5)],
            "target_price": [100.0]
        })
        signals = strategy.generate_signals(df).unwrap()
        assert signals["side"][0] == 1 # LONG

if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
