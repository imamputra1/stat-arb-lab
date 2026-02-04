"""
SIGNAL TYPES & DATA MODELS - V10.0 QUANTUM
Location: core/signals/types.py
Focus: Defining atomic signal types and state transition rules.
Paradigm: Type-Safe, Immutable, Performance Optimized.
"""

from enum import IntEnum, auto
from dataclasses import dataclass, field
from typing import Dict, Any, Optional, Tuple
from datetime import datetime

# ============================================================================
# ENUMERATIONS (The Logic States)
# ============================================================================

class SignalSide(IntEnum):
    """
    Arah posisi trading dengan properti aritmatika.
    Memungkinkan perhitungan PnL langsung: side * price_diff.
    """
    LONG = 1
    SHORT = -1
    NEUTRAL = 0
    
    @property
    def is_directional(self) -> bool:
        """Check jika memiliki arah (bukan netral)."""
        return self.value != 0

    def opposite(self) -> 'SignalSide':
        """Inversi arah untuk manajemen hedging atau pembalikan posisi."""
        return SignalSide(-self.value) if self.is_directional else self

    def __str__(self) -> str:
        return self.name

class SignalAction(IntEnum):
    """Aksi eksekusi dengan state transition matrix."""
    OPEN = auto()   # Memulai posisi baru
    CLOSE = auto()  # Menutup posisi yang ada
    HOLD = auto()   # Mempertahankan state saat ini (No-op)

    @property
    def requires_active_position(self) -> bool:
        """Aksi yang membutuhkan posisi terbuka sebelumnya."""
        return self in (SignalAction.CLOSE, SignalAction.HOLD)

# ============================================================================
# COMPATIBILITY MAPPINGS (Externalized to avoid Enum casting errors)
# ============================================================================

# Matriks Kompatibilitas Aksi terhadap Sisi
ACTION_SIDE_COMPATIBILITY: Dict[SignalAction, Tuple[SignalSide, ...]] = {
    SignalAction.OPEN: (SignalSide.LONG, SignalSide.SHORT),
    SignalAction.CLOSE: (SignalSide.LONG, SignalSide.SHORT, SignalSide.NEUTRAL),
    SignalAction.HOLD: (SignalSide.LONG, SignalSide.SHORT, SignalSide.NEUTRAL)
}

# ============================================================================
# DATA MODELS (The Value Objects)
# ============================================================================

@dataclass(frozen=True)
class SignalStrength:
    """Representasi kekuatan sinyal (0.0 hingga 1.0)."""
    value: float = 0.0

    def __post_init__(self):
        """Menjamin nilai kekuatan selalu dalam batas operasional."""
        # Clamp value antara 0.0 dan 1.0
        object.__setattr__(self, 'value', max(0.0, min(1.0, float(self.value))))

    def __repr__(self) -> str:
        return f"{self.value * 100:.1f}%"

@dataclass(frozen=True)
class SignalMetadata:
    """Kontainer informasi tambahan untuk audit trail."""
    strategy_name: str
    strategy_version: str
    reason: Optional[str] = None
    extra: Dict[str, Any] = field(default_factory=dict)

@dataclass(frozen=True)
class SignalEvent:
    """
    Atomic Signal Event - Objek tunggal yang dibawa melintasi pipeline.
    """
    side: SignalSide
    action: SignalAction
    strength: SignalStrength = field(default_factory=SignalStrength)
    timestamp: datetime = field(default_factory=datetime.now)
    metadata: Optional[SignalMetadata] = None

# ============================================================================
# FACTORY FUNCTIONS (The Interface Gates)
# ============================================================================

def create_directional_signal(
    side: SignalSide, 
    strength: float, 
    ts: datetime,
    strategy: str = "unknown",
    version: str = "0.0.0"
) -> SignalEvent:
    """Helper untuk membuat sinyal Entry (LONG/SHORT)."""
    return SignalEvent(
        side=side,
        action=SignalAction.OPEN,
        strength=SignalStrength(strength),
        timestamp=ts,
        metadata=SignalMetadata(strategy_name=strategy, strategy_version=version)
    )

def create_exit_signal(
    side: SignalSide, 
    ts: datetime,
    reason: str = "Target reached"
) -> SignalEvent:
    """Helper untuk membuat sinyal Exit (CLOSE)."""
    return SignalEvent(
        side=side,
        action=SignalAction.CLOSE,
        strength=SignalStrength(0.0),
        timestamp=ts,
        metadata=SignalMetadata(strategy_name="system", strategy_version="1.0", reason=reason)
    )

def create_neutral_signal(ts: datetime) -> SignalEvent:
    """Helper untuk membuat sinyal HOLD (No action)."""
    return SignalEvent(
        side=SignalSide.NEUTRAL,
        action=SignalAction.HOLD,
        strength=SignalStrength(0.0),
        timestamp=ts
    )
