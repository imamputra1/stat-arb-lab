"""
UNIT TESTS: KALMAN MEAN REVERSION STRATEGY V11.0
Location: tests/unit/core/signals/strategies/test_kalman_mr.py
Focus: Comprehensive strategy testing with structured composition.
Paradigm: Result-Oriented, Type-Safe, Isolated Testing Framework.
Author: ADHD-Dyslexic Systems Architect
"""

import pytest
import polars as pl
import numpy as np
from typing import Dict, Any, List
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass

# Core Signal Components
from core.signals.strategies.kalman_mr import KalmanMRStrategy, ThresholdConfig
from core.signals.types import SignalSide
from core.shared import Ok, Err

# ============================================================================
# TEST DATA MODELS & FIXTURES
# ============================================================================

# GANTI NAMA: TestScenario -> ScenarioData
@dataclass(frozen=True)
class ScenarioData:
    name: str
    description: str
    input_data: Dict[str, Any]
    expected_output: Dict[str, Any]
    test_type: str 

# GANTI NAMA: TestResult -> ScenarioResult
@dataclass(frozen=True)
class ScenarioResult:
    scenario_name: str
    passed: bool
    actual_output: Any
    expected_output: Any
    execution_time_ms: float = 0.0
    metadata: Dict[str, Any] = None
    
    def __post_init__(self):
        """Initialize metadata if None"""
        if self.metadata is None:
            object.__setattr__(self, 'metadata', {})

# ============================================================================
# TEST FIXTURES (Pytest)
# ============================================================================

@pytest.fixture
def base_strategy() -> KalmanMRStrategy:
    """Base strategy fixture with default parameters"""
    return KalmanMRStrategy(
        entry_threshold=2.0,
        exit_threshold=0.5,
        adaptive=False,
        sensitivity=1.0,
        name="test_kalman_mr"
    )

@pytest.fixture
def adaptive_strategy() -> KalmanMRStrategy:
    """Adaptive strategy fixture for learning tests"""
    return KalmanMRStrategy(
        entry_threshold=2.0,
        exit_threshold=0.5,
        adaptive=True,
        sensitivity=1.0,
        name="test_adaptive_kalman_mr"
    )

@pytest.fixture
def high_sensitivity_strategy() -> KalmanMRStrategy:
    """Strategy with high sensitivity for aggressive trading"""
    return KalmanMRStrategy(
        entry_threshold=2.0,
        exit_threshold=0.5,
        adaptive=True,
        sensitivity=2.5,
        name="test_high_sensitivity_kalman_mr"
    )

@pytest.fixture
def sample_dataframe() -> pl.DataFrame:
    """Sample DataFrame for batch testing"""
    timestamps = [datetime.now(timezone.utc) - timedelta(minutes=i) for i in range(100)]
    z_scores = np.concatenate([
        np.random.normal(-2.5, 0.5, 25),  # Strong negative (LONG signals)
        np.random.normal(0.0, 0.3, 25),   # Neutral zone
        np.random.normal(2.5, 0.5, 25),   # Strong positive (SHORT signals)
        np.random.normal(-1.0, 0.3, 25)   # Near exit threshold
    ])
    
    return pl.DataFrame({
        "timestamp": timestamps,
        "z_score": z_scores,
        "target_price": np.random.uniform(95.0, 105.0, 100)
    })

@pytest.fixture
def extreme_scenarios() -> List[ScenarioData]:
    """Extreme test scenarios for edge case testing"""
    base_time = datetime.now(timezone.utc)
    
    return [
        ScenarioData(
            name="extreme_long_signal",
            description="Extreme negative Z-Score should trigger LONG",
            input_data={
                "timestamp": base_time,
                "z_score": -5.0,
                "target_price": 100.0
            },
            expected_output={
                "side": SignalSide.LONG,
                "should_exit": False
            },
            test_type="edge_case"
        ),
        ScenarioData(
            name="extreme_short_signal",
            description="Extreme positive Z-Score should trigger SHORT",
            input_data={
                "timestamp": base_time,
                "z_score": 5.0,
                "target_price": 100.0
            },
            expected_output={
                "side": SignalSide.SHORT,
                "should_exit": False
            },
            test_type="edge_case"
        ),
        ScenarioData(
            name="exit_signal",
            description="Z-Score within exit threshold should produce NEUTRAL exit",
            input_data={
                "timestamp": base_time,
                "z_score": 0.3,
                "target_price": 100.0
            },
            expected_output={
                "side": SignalSide.NEUTRAL,
                "should_exit": True
            },
            test_type="edge_case"
        ),
        ScenarioData(
            name="hold_signal",
            description="Z-Score between exit and entry should hold position",
            input_data={
                "timestamp": base_time,
                "z_score": 1.0,
                "target_price": 100.0
            },
            expected_output={
                "side": SignalSide.NEUTRAL,
                "should_exit": False
            },
            test_type="edge_case"
        )
    ]

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def create_test_dataframe(scenarios: List[ScenarioData]) -> pl.DataFrame:
    """Create test DataFrame from scenarios"""
    data = {
        "timestamp": [],
        "z_score": [],
        "target_price": []
    }
    
    for scenario in scenarios:
        if scenario.is_ok:
            data["timestamp"].append(scenario.input_data["timestamp"])
            data["z_score"].append(scenario.input_data["z_score"])
            data["target_price"].append(scenario.input_data["target_price"])
    
    return pl.DataFrame(data)

# ============================================================================
# CORE STRATEGY TESTS
# ============================================================================

class TestKalmanMRStrategy:
    """Comprehensive test suite for KalmanMRStrategy V11.0"""
    
    # ========================================================================
    # INITIALIZATION TESTS
    # ========================================================================
    
    def test_strategy_initialization(self, base_strategy: KalmanMRStrategy):
        """Test strategy initialization and basic properties"""
        assert base_strategy.name == "test_kalman_mr"
        assert base_strategy.version == "11.0.0"
        assert base_strategy.cfg.entry == 2.0
        assert base_strategy.cfg.exit == 0.5
        assert base_strategy.adaptive is False
        assert base_strategy.sensitivity == 1.0
        
        # Test string representation
        assert "KalmanMRStrategy" in repr(base_strategy)
    
    def test_threshold_config_validation(self):
        """Test that invalid threshold configurations raise errors"""
        # Create invalid config
        invalid_config = ThresholdConfig(entry=1.0, exit=1.5, extreme=2.0)
        result = invalid_config.validate()
        
        assert result.is_err()
        assert "Threshold Ilegal" in str(result.error)
    
    def test_data_requirements(self, base_strategy: KalmanMRStrategy):
        """Test that data requirements are correctly defined"""
        requirements = base_strategy.data_requirements
        
        assert isinstance(requirements, dict)
        assert len(requirements) == 3  # timestamp, z_score, target_price
        
        # Check required columns
        assert 'timestamp' in requirements
        assert 'z_score' in requirements
        assert 'target_price' in requirements
        
        # Check data types
        assert requirements['timestamp'].data_type == pl.Datetime
        assert requirements['z_score'].data_type == pl.Float64
        assert requirements['target_price'].data_type == pl.Float64
    
    # ========================================================================
    # BATCH PROCESSING TESTS (generate_signals)
    # ========================================================================
    
    def test_batch_signal_generation_success(self, base_strategy: KalmanMRStrategy, sample_dataframe: pl.DataFrame):
        """Test successful batch signal generation"""
        result = base_strategy.generate_signals(sample_dataframe)
        
        assert isinstance(result, Ok), f"Expected Ok, got {result}"
        
        signals_df = result.unwrap()
        
        # Check required columns are present
        assert "side" in signals_df.columns
        assert "strength" in signals_df.columns
        assert "strategy_name" in signals_df.columns
        assert "strategy_version" in signals_df.columns
        assert "dyn_entry" in signals_df.columns
        assert "dyn_exit" in signals_df.columns
        
        # Check data types
        assert signals_df["side"].dtype == pl.Int64
        assert signals_df["strength"].dtype == pl.Float64
        
        # Check that all signals have valid side values
        valid_sides = {SignalSide.LONG.value, SignalSide.SHORT.value, SignalSide.NEUTRAL.value, None}
        unique_sides = set(signals_df["side"].unique().to_list())
        assert unique_sides.issubset(valid_sides), f"Invalid side values: {unique_sides - valid_sides}"
        
        # Check strength values are within bounds (0 to 1)
        strengths = signals_df["strength"].filter(pl.col("strength").is_not_null()).to_numpy()
        if len(strengths) > 0:
            assert np.all(strengths >= 0.0) and np.all(strengths <= 1.0)
        
        # Check strategy info
        assert signals_df["strategy_name"][0] == "test_kalman_mr"
        assert signals_df["strategy_version"][0] == "11.0.0"
    
    def test_batch_extreme_long_signals(self, base_strategy: KalmanMRStrategy):
        """Test that extreme negative Z-Scores trigger LONG signals"""
        df = pl.DataFrame({
            "timestamp": [datetime.now(timezone.utc)],
            "z_score": [-5.0],  # Extreme negative
            "target_price": [100.0]
        })
        
        result = base_strategy.generate_signals(df)
        assert result.is_ok()
        
        signals = result.unwrap()
        assert "side" in signals.columns
        assert signals["side"][0] == SignalSide.LONG.value, \
            f"Expected LONG({SignalSide.LONG.value}), got {signals['side'][0]}"
        
        # Check strength (extreme should have high strength)
        assert "strength" in signals.columns
        assert signals["strength"][0] >= 0.5  # At least moderate strength
    
    def test_batch_extreme_short_signals(self, base_strategy: KalmanMRStrategy):
        """Test that extreme positive Z-Scores trigger SHORT signals"""
        df = pl.DataFrame({
            "timestamp": [datetime.now(timezone.utc)],
            "z_score": [5.0],  # Extreme positive
            "target_price": [100.0]
        })
        
        result = base_strategy.generate_signals(df)
        assert result.is_ok()
        
        signals = result.unwrap()
        assert signals["side"][0] == SignalSide.SHORT.value, \
            f"Expected SHORT({SignalSide.SHORT.value}), got {signals['side'][0]}"
    
    def test_batch_neutral_signals(self, base_strategy: KalmanMRStrategy):
        """Test that Z-Scores within exit threshold trigger NEUTRAL signals"""
        df = pl.DataFrame({
            "timestamp": [datetime.now(timezone.utc)],
            "z_score": [0.3],  # Within exit threshold
            "target_price": [100.0]
        })
        
        result = base_strategy.generate_signals(df)
        assert result.is_ok()
        
        signals = result.unwrap()
        assert signals["side"][0] == SignalSide.NEUTRAL.value, \
            f"Expected NEUTRAL({SignalSide.NEUTRAL.value}), got {signals['side'][0]}"
    
    def test_batch_hold_signals(self, base_strategy: KalmanMRStrategy):
        """Test that Z-Scores between exit and entry produce None (hold)"""
        df = pl.DataFrame({
            "timestamp": [datetime.now(timezone.utc)],
            "z_score": [1.0],  # Between exit (0.5) and entry (2.0)
            "target_price": [100.0]
        })
        
        result = base_strategy.generate_signals(df)
        assert result.is_ok()
        
        signals = result.unwrap()
        # Should be None (which becomes null in Polars)
        assert signals["side"][0] is None or pl.is_null(signals["side"][0]), \
            f"Expected None (hold), got {signals['side'][0]}"
    
    def test_batch_missing_data(self, base_strategy: KalmanMRStrategy):
        """Test that missing required data returns appropriate error"""
        df = pl.DataFrame({
            "timestamp": [datetime.now(timezone.utc)],
            # Missing z_score and target_price
            "volume": [1000.0]
        })
        
        result = base_strategy.generate_signals(df)
        assert isinstance(result, Err)
        error_msg = str(result.error).lower()
        assert "z_score" in error_msg or "target_price" in error_msg
    
    def test_batch_null_values(self, base_strategy: KalmanMRStrategy):
        """Test handling of null values in input data"""
        df = pl.DataFrame({
            "timestamp": [datetime.now(timezone.utc), datetime.now(timezone.utc)],
            "z_score": [None, -2.5],  # One null value
            "target_price": [100.0, 100.0]
        })
        
        result = base_strategy.generate_signals(df)
        # Should handle nulls gracefully
        assert isinstance(result, Ok)
        
        signals = result.unwrap()
        assert len(signals) == 2
        # First row with null z_score should have null side
        assert signals["side"][0] is None or pl.is_null(signals["side"][0])
        assert signals["side"][1] == SignalSide.LONG.value
    
    # ========================================================================
    # LIVE PROCESSING TESTS (evaluate_state)
    # ========================================================================
    
    def test_live_long_signal_generation(self, base_strategy: KalmanMRStrategy):
        """Test live LONG signal generation"""
        observation = {
            "timestamp": datetime.now(timezone.utc),
            "z_score": -2.5,
            "target_price": 100.0
        }
        
        result = base_strategy.evaluate_state(observation)
        assert isinstance(result, Ok)
        
        event = result.unwrap()
        assert event.side == SignalSide.LONG
        assert event.strength.value > 0.0
        assert event.strength.value <= 1.0
    
    def test_live_short_signal_generation(self, base_strategy: KalmanMRStrategy):
        """Test live SHORT signal generation"""
        observation = {
            "timestamp": datetime.now(timezone.utc),
            "z_score": 2.5,
            "target_price": 100.0
        }
        
        result = base_strategy.evaluate_state(observation)
        assert isinstance(result, Ok)
        
        event = result.unwrap()
        assert event.side == SignalSide.SHORT
        assert event.strength.value > 0.0
        assert event.strength.value <= 1.0
    
    def test_live_exit_signal_generation(self, base_strategy: KalmanMRStrategy):
        """Test live exit signal generation"""
        observation = {
            "timestamp": datetime.now(timezone.utc),
            "z_score": 0.3,  # Within exit threshold
            "target_price": 100.0
        }
        
        result = base_strategy.evaluate_state(observation)
        assert isinstance(result, Ok)
        
        event = result.unwrap()
        assert event.side == SignalSide.NEUTRAL
        # Exit signals typically have lower or zero strength
        assert event.strength.value == 0.0
    
    def test_live_hold_signal_generation(self, base_strategy: KalmanMRStrategy):
        """Test live hold signal generation"""
        observation = {
            "timestamp": datetime.now(timezone.utc),
            "z_score": 1.0,  # Between exit and entry
            "target_price": 100.0
        }
        
        result = base_strategy.evaluate_state(observation)
        assert isinstance(result, Ok)
        
        event = result.unwrap()
        assert event.side == SignalSide.NEUTRAL
        assert event.strength.value == 0.0  # Neutral signals have zero strength
    
    def test_live_missing_required_fields(self, base_strategy: KalmanMRStrategy):
        """Test live evaluation with missing required fields"""
        observation = {
            "timestamp": datetime.now(timezone.utc),
            # Missing z_score
            "target_price": 100.0
        }
        
        result = base_strategy.evaluate_state(observation)
        assert isinstance(result, Err)
        assert "z_score" in str(result.error).lower()
    
    # ========================================================================
    # ADAPTIVE & SENSITIVITY TESTS
    # ========================================================================
    
    def test_adaptive_threshold_calculation(self, adaptive_strategy: KalmanMRStrategy, sample_dataframe: pl.DataFrame):
        """Test that adaptive strategy calculates dynamic thresholds"""
        result = adaptive_strategy.generate_signals(sample_dataframe)
        assert result.is_ok()
        
        signals_df = result.unwrap()
        
        # Check that dynamic threshold columns exist
        assert "dyn_entry" in signals_df.columns
        assert "dyn_exit" in signals_df.columns
        assert "z_vol" in signals_df.columns
        
        # Check dynamic thresholds are calculated
        dyn_entries = signals_df["dyn_entry"].to_numpy()
        dyn_exits = signals_df["dyn_exit"].to_numpy()
        
        assert np.all(dyn_entries > 0)
        assert np.all(dyn_exits > 0)
        assert np.all(dyn_exits < dyn_entries)  # Exit should always be less than entry
    
    def test_sensitivity_impact_on_thresholds(self, sample_dataframe: pl.DataFrame):
        """Test that higher sensitivity lowers dynamic entry thresholds"""
        low_sens = KalmanMRStrategy(sensitivity=0.5, adaptive=True)
        high_sens = KalmanMRStrategy(sensitivity=2.0, adaptive=True)
        
        result_low = low_sens.generate_signals(sample_dataframe)
        result_high = high_sens.generate_signals(sample_dataframe)
        
        assert result_low.is_ok()
        assert result_high.is_ok()
        
        df_low = result_low.unwrap()
        df_high = result_high.unwrap()
        
        # Calculate average dynamic entry thresholds
        avg_entry_low = df_low["dyn_entry"].mean()
        avg_entry_high = df_high["dyn_entry"].mean()
        
        # Higher sensitivity should result in lower entry thresholds
        print("\n📊 Sensitivity Impact Test:")
        print(f"   Low sensitivity (0.5): avg dyn_entry = {avg_entry_low:.4f}")
        print(f"   High sensitivity (2.0): avg dyn_entry = {avg_entry_high:.4f}")
        
        assert avg_entry_high <= avg_entry_low, \
            "Higher sensitivity should lower entry thresholds"
    
    def test_sensitivity_impact_on_trades(self, sample_dataframe: pl.DataFrame):
        """Test that higher sensitivity produces more trades"""
        low_sens = KalmanMRStrategy(sensitivity=0.8, name="low_sens")
        high_sens = KalmanMRStrategy(sensitivity=2.5, name="high_sens")
        
        res_low = low_sens.generate_signals(sample_dataframe)
        res_high = high_sens.generate_signals(sample_dataframe)
        
        assert res_low.is_ok()
        assert res_high.is_ok()
        
        df_low = res_low.unwrap()
        df_high = res_high.unwrap()
        
        # Count directional signals (LONG or SHORT)
        count_low = df_low.filter(
            (pl.col("side") == SignalSide.LONG.value) | 
            (pl.col("side") == SignalSide.SHORT.value)
        ).height
        
        count_high = df_high.filter(
            (pl.col("side") == SignalSide.LONG.value) | 
            (pl.col("side") == SignalSide.SHORT.value)
        ).height
        
        print("\n🧪 Sensitivity Trade Count Test:")
        print(f"   📉 Low Sens (0.8): {count_low} directional trades")
        print(f"   📈 High Sens (2.5): {count_high} directional trades")
        
        # High sensitivity should generate equal or more trades
        assert count_high >= count_low, \
            "High sensitivity failed to generate more trades!"
        assert count_high > 0, "Even high sensitivity generated 0 trades!"
    
    # ========================================================================
    # PERFORMANCE & SCALABILITY TESTS
    # ========================================================================
    
    def test_batch_performance_large_dataset(self, base_strategy: KalmanMRStrategy):
        """Test performance with large dataset"""
        # Create large dataset
        n_rows = 10000
        timestamps = [datetime.now(timezone.utc) - timedelta(minutes=i) for i in range(n_rows)]
        
        df = pl.DataFrame({
            "timestamp": timestamps,
            "z_score": np.random.normal(0, 2, n_rows),
            "target_price": np.random.uniform(90.0, 110.0, n_rows)
        })
        
        # Benchmark performance
        import time
        start_time = time.perf_counter()
        
        result = base_strategy.generate_signals(df)
        
        end_time = time.perf_counter()
        execution_time_ms = (end_time - start_time) * 1000
        
        assert result.is_ok()
        assert execution_time_ms < 5000  # Should complete within 5 seconds
        
        signals_df = result.unwrap()
        assert len(signals_df) == n_rows
        
        print(f"\n⚡ Performance Test: {n_rows} rows processed in {execution_time_ms:.2f} ms")
        print(f"   Throughput: {n_rows / (execution_time_ms / 1000):.0f} rows/sec")
    
    def test_memory_efficiency(self, base_strategy: KalmanMRStrategy):
        """Test memory efficiency with large datasets"""
        import psutil
        import os
        
        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB
        
        # Create moderately large dataset
        n_rows = 5000
        df = pl.DataFrame({
            "timestamp": [datetime.now(timezone.utc)] * n_rows,
            "z_score": np.random.normal(0, 2, n_rows),
            "target_price": np.random.uniform(100.0, 100.0, n_rows)
        })
        
        result = base_strategy.generate_signals(df)
        assert result.is_ok()
        
        final_memory = process.memory_info().rss / 1024 / 1024
        memory_increase = final_memory - initial_memory
        
        # Memory increase should be reasonable
        assert memory_increase < 500  # Less than 500MB increase
        
        print(f"\n💾 Memory Test: {memory_increase:.2f} MB increase for {n_rows} rows")
    
    # ========================================================================
    # EDGE CASE & ROBUSTNESS TESTS
    # ========================================================================
    
    def test_invalid_threshold_configuration(self):
        """Test that invalid threshold configurations are caught"""
        # Entry <= Exit should be invalid
        with pytest.raises(ValueError, match="Invalid threshold configuration"):
            KalmanMRStrategy(entry_threshold=1.0, exit_threshold=1.5)
    
    def test_extreme_threshold_values(self):
        """Test strategy with extreme threshold values"""
        strategy = KalmanMRStrategy(
            entry_threshold=5.0,
            exit_threshold=2.0,
            adaptive=False
        )
        
        # With high thresholds, most signals should be neutral/hold
        df = pl.DataFrame({
            "timestamp": [datetime.now(timezone.utc)],
            "z_score": [3.0],  # Would trigger with normal thresholds (2.0)
            "target_price": [100.0]
        })
        
        result = strategy.generate_signals(df)
        assert result.is_ok()
        
        signals = result.unwrap()
        # With entry threshold at 5.0, z_score of 3.0 should not trigger
        assert signals["side"][0] is None or pl.is_null(signals["side"][0])
    
    def test_nan_and_inf_values(self, base_strategy: KalmanMRStrategy):
        """Test handling of NaN and infinite values"""
        df = pl.DataFrame({
            "timestamp": [datetime.now(timezone.utc), datetime.now(timezone.utc), datetime.now(timezone.utc)],
            "z_score": [np.nan, np.inf, -np.inf],
            "target_price": [100.0, 100.0, 100.0]
        })
        
        result = base_strategy.generate_signals(df)
        # Should handle gracefully
        assert isinstance(result, Ok)
        
        signals = result.unwrap()
        assert len(signals) == 3
        # Rows with NaN/Inf should have null side
        assert signals["side"][0] is None or pl.is_null(signals["side"][0])
    
    def test_very_small_dataset(self, base_strategy: KalmanMRStrategy):
        """Test with very small datasets (including empty)"""
        # Single row
        df_single = pl.DataFrame({
            "timestamp": [datetime.now(timezone.utc)],
            "z_score": [-2.5],
            "target_price": [100.0]
        })
        
        result_single = base_strategy.generate_signals(df_single)
        assert result_single.is_ok()
        assert len(result_single.unwrap()) == 1
        
        # Empty dataframe
        df_empty = pl.DataFrame({
            "timestamp": [],
            "z_score": [],
            "target_price": []
        })
        
        result_empty = base_strategy.generate_signals(df_empty)
        assert result_empty.is_ok()
        assert len(result_empty.unwrap()) == 0
    
    def test_zero_volatility_scenario(self):
        """Test scenario with zero volatility (z_vol = 0)"""
        strategy = KalmanMRStrategy(adaptive=True, sensitivity=1.0)
        
        # Create data with constant z_score (zero volatility)
        df = pl.DataFrame({
            "timestamp": [datetime.now(timezone.utc) - timedelta(minutes=i) for i in range(100)],
            "z_score": [2.5] * 100,  # Constant value
            "target_price": [100.0] * 100
        })
        
        result = strategy.generate_signals(df)
        assert result.is_ok()
        
        signals = result.unwrap()
        # Should handle zero volatility gracefully
        assert "dyn_entry" in signals.columns
        assert "dyn_exit" in signals.columns
    
    # ========================================================================
    # STRATEGY INTERFACE TESTS
    # ========================================================================
    
    def test_strategy_name_and_version(self, base_strategy: KalmanMRStrategy):
        """Test strategy name and version properties"""
        assert base_strategy.name == "test_kalman_mr"
        assert base_strategy.version == "11.0.0"
        assert base_strategy.STRATEGY_VERSION == "11.0.0"
    
    def test_strategy_data_validation(self, base_strategy: KalmanMRStrategy):
        """Test data validation method"""
        # Valid data
        valid_df = pl.DataFrame({
            "timestamp": [datetime.now(timezone.utc)],
            "z_score": [-2.5],
            "target_price": [100.0]
        })
        
        validation = base_strategy.validate_data(valid_df)
        assert validation.is_valid
        
        # Invalid data (missing column)
        invalid_df = pl.DataFrame({
            "timestamp": [datetime.now(timezone.utc)],
            "z_score": [-2.5]
            # Missing target_price
        })
        
        validation = base_strategy.validate_data(invalid_df)
        assert not validation.is_valid
        assert "target_price" in validation.error_summary
    
    # ========================================================================
    # REGRESSION TESTS (Ensure backward compatibility)
    # ========================================================================
    
    def test_original_trade_generation(self):
        """Original test case - ensure it still passes"""
        # Setup: Extreme data to guarantee trades
        df = pl.DataFrame({
            "timestamp": [datetime.now(timezone.utc)],
            "z_score": [-5.0],  # Very low, should trigger LONG
            "target_price": [100.0]
        })
        
        strategy = KalmanMRStrategy(entry_threshold=2.0)
        result = strategy.generate_signals(df)
        
        assert result.is_ok(), f"Expected Ok, got error: {result.error if isinstance(result, Err) else 'Unknown'}"
        
        signals = result.unwrap()
        
        # Check if 'side' column exists and has value 1 (LONG)
        assert "side" in signals.columns, f"Missing 'side' column. Columns: {signals.columns}"
        assert signals["side"][0] == SignalSide.LONG.value, \
            f"Expected LONG({SignalSide.LONG.value}) but got {signals['side'][0]}"
        
        # Check strength
        assert "strength" in signals.columns, "Missing 'strength' column"
        assert signals["strength"][0] >= 0.5, "Extreme signal should have strength >= 0.5"

# ============================================================================
# PARAMETERIZED TESTS
# ============================================================================

@pytest.mark.parametrize("z_score,expected_side", [
    (-3.0, SignalSide.LONG.value),      # Below -entry
    (-2.1, SignalSide.LONG.value),      # Below -entry
    (-1.9, None),                       # Between -exit and -entry (hold)
    (-0.4, SignalSide.NEUTRAL.value),   # Within exit threshold
    (0.0, SignalSide.NEUTRAL.value),    # Exactly zero
    (0.4, SignalSide.NEUTRAL.value),    # Within exit threshold
    (1.9, None),                        # Between exit and entry (hold)
    (2.1, SignalSide.SHORT.value),      # Above entry
    (3.0, SignalSide.SHORT.value)       # Above entry
])
def test_parameterized_z_scores(z_score: float, expected_side: int, base_strategy: KalmanMRStrategy):
    """Test various Z-Score values with parameterized tests"""
    df = pl.DataFrame({
        "timestamp": [datetime.now(timezone.utc)],
        "z_score": [z_score],
        "target_price": [100.0]
    })
    
    result = base_strategy.generate_signals(df)
    assert result.is_ok()
    
    signals = result.unwrap()
    actual_side = signals["side"][0]
    
    if expected_side is None:
        # None indicates HOLD (maintain current position)
        assert actual_side is None or pl.is_null(actual_side), \
            f"Expected None (HOLD) for z_score={z_score}, got {actual_side}"
    else:
        assert actual_side == expected_side, \
            f"For z_score={z_score}, expected side {expected_side}, got {actual_side}"

@pytest.mark.parametrize("entry_threshold,exit_threshold", [
    (1.5, 0.3),
    (2.0, 0.5),
    (2.5, 0.8),
    (3.0, 1.0)
])
def test_parameterized_thresholds(entry_threshold: float, exit_threshold: float):
    """Test strategy with various threshold combinations"""
    strategy = KalmanMRStrategy(
        entry_threshold=entry_threshold,
        exit_threshold=exit_threshold,
        adaptive=False
    )
    
    # Test with Z-Score that should trigger LONG
    df = pl.DataFrame({
        "timestamp": [datetime.now(timezone.utc)],
        "z_score": [-(entry_threshold + 0.5)],  # Clearly below entry threshold
        "target_price": [100.0]
    })
    
    result = strategy.generate_signals(df)
    assert result.is_ok()
    
    signals = result.unwrap()
    assert signals["side"][0] == SignalSide.LONG.value, \
        f"With entry_threshold={entry_threshold}, z_score={-(entry_threshold + 0.5)} should trigger LONG"

# ============================================================================
# TEST RUNNER
# ============================================================================

if __name__ == "__main__":
    # Run tests when script is executed directly
    pytest.main([__file__, "-v", "--tb=short"])
