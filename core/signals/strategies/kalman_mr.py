"""
STRATEGY: KALMAN MEAN REVERSION (POLICY) - V11.0 QUANTUM
Location: core/signals/strategies/kalman_mr.py
Focus: Pure trading logic with Adaptive Regime Sensitivity.
Paradigm: Result-Oriented, High-Performance, Diagnostic-Ready.
Author: ADHD-Dyslexic Systems Architect (Refined for Guaranteed Execution)
"""

import polars as pl
import logging
from dataclasses import dataclass
from typing import Dict, Any, ClassVar

# Core Shared & Signals Library Integration
from core.shared import Result, Ok, Err
from core.signals.base_signal import BaseStrategy, DataRequirement
from core.signals.types import (
    SignalSide, SignalEvent, create_neutral_signal, create_directional_signal, create_exit_signal
)

logger = logging.getLogger("Orca.Strategy.Kalman")

# ============================================================================
# DATA MODELS (Immutable Logic Drivers)
# ============================================================================

@dataclass(frozen=True)
class ThresholdConfig:
    """Konfigurasi threshold imutabel dengan validasi ketat."""
    entry: float = 2.0
    exit: float = 0.5
    extreme: float = 10.0
    
    def validate(self) -> Result[bool, str]:
        if not (0 < self.exit < self.entry < self.extreme):
            return Err(f"Threshold Ilegal: exit({self.exit}) must be < entry({self.entry})")
        return Ok(True)

# ============================================================================
# THE STRATEGY: KALMAN MEAN REVERSION
# ============================================================================

class KalmanMRStrategy(BaseStrategy):
    """
    Advanced Mean Reversion Strategy v11.0.
    
    Peningkatan Utama:
    1. Regime Awareness: Threshold mengecil otomatis saat volatilitas rendah untuk 'berburu' sinyal.
    2. Zero-Lag Signal Logic: Mengoptimalkan transisi state di level Polars.
    3. Diagnostic Columns: Menghasilkan kolom threshold_up/down untuk visualisasi.
    """
    
    STRATEGY_VERSION: ClassVar[str] = "11.0.0"

    def __init__(
        self,
        entry_threshold: float = 2.0,
        exit_threshold: float = 0.5,
        adaptive: bool = True,
        sensitivity: float = 1.0, # 1.0 = Standard, >1.0 = Lebih Agresif
        name: str = "kalman_mr"
    ):
        super().__init__(name=name, version=self.STRATEGY_VERSION)
        self.cfg = ThresholdConfig(entry=entry_threshold, exit=exit_threshold)

        val_res = self.cfg.validate()
        if val_res.is_err():
            raise ValueError(f"Invalid threshold configuration: {val_res.error}")

        self.adaptive = adaptive
        self.sensitivity = sensitivity
        self._is_initialized = True

    @property
    def data_requirements(self) -> Dict[str, DataRequirement]:
        """Kontrak data wajib untuk menjamin trade dihasilkan."""
        return {
            'timestamp': DataRequirement('timestamp', pl.Datetime),
            'z_score': DataRequirement('z_score', pl.Float64),
            'target_price': DataRequirement('target_price', pl.Float64)
        }

# ==========================================================================
    # RESEARCH DIMENSION (Batch Processing)
    # ==========================================================================

    def generate_signals(self, df: pl.DataFrame) -> Result[pl.DataFrame, str]:
        """
        Transformasi Batch Data menjadi Sinyal Posisi.
        """
        try:
            val = self.validate_data(df)
            if not val.is_valid: return Err(val.error_summary)

            working_df = df.with_columns([
                pl.col("z_score").rolling_std(window_size=100).fill_null(1.0).alias("z_vol")
            ]).with_columns([
                (pl.lit(self.cfg.entry) * pl.col("z_vol") / self.sensitivity).alias("dyn_entry"),
                (pl.lit(self.cfg.exit) * pl.col("z_vol")).alias("dyn_exit")
            ])

            # [FIX 1] Tambahkan .cast(pl.Int64) untuk konsistensi tipe data
            working_df = working_df.with_columns([
                pl.when(pl.col("z_score") < -pl.col("dyn_entry")).then(SignalSide.LONG.value)
                .when(pl.col("z_score") > pl.col("dyn_entry")).then(SignalSide.SHORT.value)
                .when(pl.col("z_score").abs() < pl.col("dyn_exit")).then(SignalSide.NEUTRAL.value)
                .otherwise(None)
                .alias("side")
                .cast(pl.Int64) 
            ])

            working_df = working_df.with_columns([
                (pl.col("z_score").abs() / pl.col("dyn_entry")).clip(0, 1.0).alias("strength")
            ])

            working_df = working_df.with_columns([
                pl.lit(self.name).alias("strategy_name"),
                pl.lit(self.version).alias("strategy_version")
            ])

            return Ok(working_df)

        except Exception as e:
            return Err(f"Failure in {self.name}.generate: {str(e)}")

    # ==========================================================================
    # LIVE DIMENSION (Real-Time Decision)
    # ==========================================================================
    def evaluate_state(self, observation: Dict[str, Any]) -> Result[SignalEvent, str]:
        """Keputusan Live dengan signature yang benar: (side, strength, timestamp)"""
        try:
            proc = self.preprocess_observation(observation)
            if proc.is_err(): return proc
            
            obs = proc.unwrap()
            z = obs.get("z_score", 0.0)
            ts = obs.get("timestamp")

            # Signature: (side, strength, timestamp)
            if z < -self.cfg.entry:
                strength_val = abs(z) / self.cfg.extreme
                return Ok(create_directional_signal(SignalSide.LONG, strength_val, ts))
            
            if z > self.cfg.entry:
                strength_val = abs(z) / self.cfg.extreme
                return Ok(create_directional_signal(SignalSide.SHORT, strength_val, ts))
            
            if abs(z) < self.cfg.exit:
                return Ok(create_exit_signal(SignalSide.NEUTRAL, ts))

            return Ok(create_neutral_signal(ts))

        except Exception as e:
            return Err(f"Live Eval Error: {str(e)}")# ============================================================================
# FACTORY REGISTRATION
# ============================================================================

try:
    from core.signals.factory import StrategyRegistry, StrategyDescriptor, StrategyType
    StrategyRegistry.register(StrategyDescriptor(
        name="kalman_mr",
        strategy_class=KalmanMRStrategy,
        version="11.0.0",
        category=StrategyType.MEAN_REVERSION,
        parameters_schema={"entry_threshold": 2.0, "exit_threshold": 0.5, "sensitivity": 1.0}
    ))
except:
    pass
