"""
SIGNAL & POSITION FILTERS (THE GUARDIANS) - V14.0 QUANTUM
Location: core/signals/filters.py
Focus: High-performance data integrity, safety gates, and risk enforcement.
Paradigm: Result-Oriented, Vectorized Logic, Structural Composition.
Author: ADHD-Dyslexic Systems Architect (Refined for Industrial Scale)
"""

import polars as pl
from abc import ABC, abstractmethod
from typing import List
from enum import Enum, auto
from datetime import datetime, timezone
import logging

# Core Shared & Signal Components Integration
from core.shared import Result, Ok, Err
from .types import SignalEvent

# ============================================================================
# LOGGING & TELEMETRY
# ============================================================================

logger = logging.getLogger("Orca.Guardians")

# ============================================================================
# ENUMERATIONS (The Control Logic)
# ============================================================================

class FilterType(Enum):
    DATA_INTEGRITY = auto()
    RISK_EXPOSURE = auto()
    STRATEGY_HEALTH = auto()
    COMPOSITE = auto()

class FilterSeverity(Enum):
    WARNING = auto()     # Log warning, allow execution
    REJECT = auto()      # Reject single signal
    CRITICAL = auto()    # Halt system (Panic mode)

class FilterScope(Enum):
    BATCH_ONLY = auto()  # Research/Backtest
    LIVE_ONLY = auto()   # Live Trading
    UNIVERSAL = auto()   # Both

# ============================================================================
# BASE INTERFACES (The Blueprint)
# ============================================================================

class BaseFilter(ABC):
    """
    Interface fundamental untuk semua filter sistem.
    Menerapkan pemisahan antara Batch (Vektor) dan Single (Event).
    """
    def __init__(self, name: str, severity: FilterSeverity = FilterSeverity.REJECT):
        self.name = name
        self.severity = severity
        self._is_enabled = True

    @abstractmethod
    def apply(self, df: pl.DataFrame) -> Result[pl.DataFrame, str]:
        """Aplikasi filter secara masif pada DataFrame (Node B/S)."""
        pass

    @abstractmethod
    def apply_single(self, event: SignalEvent) -> Result[bool, str]:
        """Validasi sinyal tunggal (Node L - Live)."""
        pass

# ============================================================================
# SIGNAL VALIDATOR (Data Integrity Guardian)
# ============================================================================

class SignalValidator(BaseFilter):
    """
    Memastikan sinyal yang keluar dari Generator memiliki struktur teknis yang benar.
    """
    def __init__(self, max_stale_seconds: int = 60):
        super().__init__("signal_validator", FilterSeverity.CRITICAL)
        self.max_stale = max_stale_seconds

    def apply(self, df: pl.DataFrame) -> Result[pl.DataFrame, str]:
        """Validasi batch menggunakan Polars Schema Verification."""
        required = ["timestamp", "position", "action"]
        
        # 1. Schema Check
        missing = [c for c in required if c not in df.columns]
        if missing: return Err(f"Missing Columns: {missing}")
        
        # 2. Value Integrity Check (Fast Path)
        # Mencari null atau nilai ilegal (-1, 0, 1) secara vectorized
        try:
            check = df.select([
                pl.col("position").is_null().any().alias("has_null"),
                pl.col("position").is_in([-1, 0, 1]).all().alias("valid_values")
            ]).row(0)
        
            if check[0] or not check[1]:
                return Err("Integrity Violation: Found NULLs or illegal position values")

            return Ok(df)


        except Exception as e:
            return Err(f"Validation Error: {str(e)}")
            

    def apply_single(self, event: SignalEvent) -> Result[bool, str]:
        """Validasi sinyal tunggal menggunakan kontrak internal SignalEvent."""
        # 1. Panggil validasi internal SignalEvent
        res = event.validate() if hasattr(event, 'validate') else Ok(True)
        if res.is_err(): return res
        
        # 2. Freshness Check
        now = datetime.now(timezone.utc)
        if event.timestamp.tzinfo is None:
            pass
        return Ok(True)

        if (now - event.timestamp).total_seconds() > self.max_stale:
            return Err(f"Stale Signal: {self.name} age exceeds {self.max_stale}s")
            
        return Ok(True)

# ============================================================================
# RISK FILTERS (Exposure Guardians)
# ============================================================================

class PositionFilter(BaseFilter):
    """
    Mengelola eksposur risiko dengan membatasi ukuran posisi (Clipping).
    """
    def __init__(self, max_exposure: float = 1.0, min_strength: float = 0.2):
        super().__init__("position_limit_filter")
        self.max_exp = max_exposure
        self.min_str = min_strength

    def apply(self, df: pl.DataFrame) -> Result[pl.DataFrame, str]:
        """Vectorized clipping dan filtering kekuatan sinyal."""
        # Gunakan Polars Expression untuk performa maksimal
        filtered = df.with_columns(
            pl.col("position").clip(-self.max_exp, self.max_exp)
        ).filter(
            pl.col("strength").abs() >= self.min_str
        )
        return Ok(filtered)

    def apply_single(self, event: SignalEvent) -> Result[bool, str]:
        """Evaluasi aturan bisnis pada event tunggal."""
        if not event.strength.is_above(self.min_str):
            return Err(f"Insufficient Strength: {event.strength.value} < {self.min_str}")
        return Ok(True)

class VolatilityFilter(BaseFilter):
    """Mencegah entri saat pasar sedang 'Chaos' (Extreme ATR/Vola)."""
    def __init__(self, threshold: float = 0.5):
        super().__init__("chaos_filter", FilterSeverity.REJECT)
        self.threshold = threshold

    def apply(self, df: pl.DataFrame) -> Result[pl.DataFrame, str]:
        if "volatility" not in df.columns: return Ok(df)
        return Ok(df.filter(pl.col("volatility") <= self.threshold))

    def apply_single(self, event: SignalEvent) -> Result[bool, str]:
        vola = event.metadata.get_typed("volatility", float, 0.0)
        if vola > self.threshold:
            return Err(f"Market Chaos: Volatility {vola} > {self.threshold}")
        return Ok(True)

# ============================================================================
# COMPOSITE FILTER (The Pipeline Orchestrator)
# ============================================================================

class CompositeFilter(BaseFilter):
    """
    Menggabungkan beberapa filter menjadi satu rangkaian (Pipeline).
    Mendukung 'Fast-Fail' logic untuk efisiensi CPU.
    """
    def __init__(self, filters: List[BaseFilter], name: str = "main_guardian_chain"):
        super().__init__(name)
        self.filters = filters

    def apply(self, df: pl.DataFrame) -> Result[pl.DataFrame, str]:
        """Menjalankan seluruh rangkaian filter pada batch data."""
        current_df = df
        for f in self.filters:
            res = f.apply(current_df)
            if res.is_err():
                if f.severity == FilterSeverity.CRITICAL: return res
                logger.warning(f"Filter {f.name} rejected batch: {res.error}")
                continue
            current_df = res.unwrap()
        return Ok(current_df)

    def apply_single(self, event: SignalEvent) -> Result[bool, str]:
        """Menjalankan validasi berantai pada sinyal tunggal."""
        for f in self.filters:
            res = f.apply_single(event)
            if res.is_err():
                return res # Fast Fail on single event
        return Ok(True)

# ============================================================================
# FACTORY & REGISTRY (The Entry Points)
# ============================================================================

class FilterFactory:
    """Factory untuk membangun pipeline filter standar."""
    @staticmethod
    def create_standard_guard() -> CompositeFilter:
        return CompositeFilter([
            SignalValidator(max_stale_seconds=300),
            PositionFilter(max_exposure=1.0, min_strength=0.15),
            VolatilityFilter(threshold=0.8)
        ])

def get_validator() -> SignalValidator:
    return SignalValidator()

def get_position_filter() -> PositionFilter:
    return PositionFilter()
