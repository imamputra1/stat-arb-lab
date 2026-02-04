"""
STRATEGY: KALMAN MEAN REVERSION (POLICY) - V11.0 QUANTUM (BULLETPROOF)
Location: core/signals/strategies/kalman_mr.py
Focus: Pure trading logic with Defensive Data Sanitization.
"""

import polars as pl
import logging
from dataclasses import dataclass
from typing import Dict, Any, ClassVar
import numpy as np

from core.shared import Result, Ok, Err
from core.signals.base_signal import BaseStrategy, DataRequirement
from core.signals.types import (
    SignalSide, SignalEvent, create_neutral_signal, create_directional_signal, create_exit_signal
)

logger = logging.getLogger("Orca.Strategy.Kalman")

@dataclass(frozen=True)
class ThresholdConfig:
    entry: float = 2.0
    exit: float = 0.5
    extreme: float = 10.0
    
    def validate(self) -> Result[bool, str]:
        if not (0 < self.exit < self.entry < self.extreme):
            return Err(f"Threshold Ilegal: exit({self.exit}) must be < entry({self.entry})")
        return Ok(True)

class KalmanMRStrategy(BaseStrategy):
    STRATEGY_VERSION: ClassVar[str] = "11.0.0"

    def __init__(self, entry_threshold: float = 2.0, exit_threshold: float = 0.5, adaptive: bool = True, sensitivity: float = 1.0, name: str = "kalman_mr"):
        super().__init__(name=name, version=self.STRATEGY_VERSION)
        self.cfg = ThresholdConfig(entry=entry_threshold, exit=exit_threshold)
        if self.cfg.validate().is_err(): raise ValueError("Invalid Config")
        self.adaptive = adaptive
        self.sensitivity = sensitivity
        self._is_initialized = True

    @property
    def data_requirements(self) -> Dict[str, DataRequirement]:
        return {
            'timestamp': DataRequirement('timestamp', pl.Datetime),
            'z_score': DataRequirement('z_score', pl.Float64),
            'target_price': DataRequirement('target_price', pl.Float64)
        }

    def generate_signals(self, df: pl.DataFrame) -> Result[pl.DataFrame, str]:
        """Transformasi Batch dengan SANITASI TOTAL."""
        try:
            # 1. Fail-Safe: Return empty jika input kosong
            if df.is_empty(): return Ok(df)

            # 2. Validasi Kolom
            val = self.validate_data(df)
            if not val.is_valid: return Err(val.error_summary)

            # 3. Defensive Copy & Sanitasi: Ubah Inf/NaN menjadi None (Null) agar tidak merusak logika
            #    Kita gunakan 'clean_z' untuk perhitungan
            working_df = df.with_columns([
                pl.when(pl.col("z_score").is_infinite() | pl.col("z_score").is_nan())
                .then(None)
                .otherwise(pl.col("z_score"))
                .alias("clean_z")
            ])

            # 4. Hitung Volatilitas pada 'clean_z' (fill_null 1.0 agar tidak division by zero)
            working_df = working_df.with_columns([
                pl.col("clean_z").rolling_std(window_size=100).fill_null(1.0).alias("z_vol")
            ]).with_columns([
                (pl.lit(self.cfg.entry) * pl.col("z_vol") / self.sensitivity).alias("dyn_entry"),
                (pl.lit(self.cfg.exit) * pl.col("z_vol")).alias("dyn_exit")
            ])

            # 5. Logic Inti (Menggunakan clean_z)
            #    Jika clean_z NULL -> Side otomatis NULL (Hold)
            working_df = working_df.with_columns([
                pl.when(pl.col("clean_z") < -pl.col("dyn_entry")).then(SignalSide.LONG.value)
                .when(pl.col("clean_z") > pl.col("dyn_entry")).then(SignalSide.SHORT.value)
                .when(pl.col("clean_z").abs() < pl.col("dyn_exit")).then(SignalSide.NEUTRAL.value)
                .otherwise(None)
                .alias("side")
                .cast(pl.Int64) # Wajib Int64
            ])

            # 6. Strength
            working_df = working_df.with_columns([
                (pl.col("clean_z").abs() / pl.col("dyn_entry")).clip(0, 1.0).fill_null(0.0).alias("strength")
            ])

            # 7. Metadata
            working_df = working_df.with_columns([
                pl.lit(self.name).alias("strategy_name"),
                pl.lit(self.version).alias("strategy_version")
            ])
            
            # Kembalikan dataframe (drop kolom temporary jika perlu, tapi biarkan untuk debug)
            return Ok(working_df)

        except Exception as e:
            return Err(f"Failure in {self.name}.generate: {str(e)}")

    def evaluate_state(self, observation: Dict[str, Any]) -> Result[SignalEvent, str]:
        """Keputusan Live dengan Validasi Kunci Manual."""
        try:
            # Pengecekan Kunci Manual (Dirty but effective)
            for k in ["z_score", "timestamp", "target_price"]:
                if k not in observation: return Err(f"Missing required field: {k}")

            proc = self.preprocess_observation(observation)
            if proc.is_err(): return proc
            
            obs = proc.unwrap()
            z = obs["z_score"] 
            ts = obs["timestamp"]

            # Filter Inf/NaN untuk Live (Python float check)
            if not np.isfinite(z):
                return Ok(create_neutral_signal(ts)) 

            if z < -self.cfg.entry:
                return Ok(create_directional_signal(SignalSide.LONG, abs(z)/self.cfg.extreme, ts))
            if z > self.cfg.entry:
                return Ok(create_directional_signal(SignalSide.SHORT, abs(z)/self.cfg.extreme, ts))
            if abs(z) < self.cfg.exit:
                return Ok(create_exit_signal(SignalSide.NEUTRAL, ts))

            return Ok(create_neutral_signal(ts))

        except Exception as e:
            return Err(f"Live Eval Error: {str(e)}")
