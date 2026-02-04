"""
SIGNAL TYPES & DATA MODELS
Location: core/signals/types.py
Focus: Universal vocabulary for trading decisions - Industrial Grade
Paradigm: Structured Composition, Result-Oriented, Type-Safe
"""

from enum import IntEnum, auto
from dataclasses import dataclass, field
from typing import Dict, Any, Optional, Tuple, Protocol, runtime_checkable, ClassVar
from datetime import datetime, timezone

# Integrasi dengan Core Shared Result Pattern
from core.shared import Result, Ok, Err

# ============================================================================
# PROTOCOLS (Behavioral Contracts)
# ============================================================================

@runtime_checkable
class SignalValidatable(Protocol):
    """Kontrak struktural untuk validasi sinyal secara internal."""
    def validate(self) -> Result[bool, str]:
        """Memastikan integritas data sinyal."""
        ...

# ============================================================================
# ENUMERATIONS (Domain Constants)
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
    
    @property
    def multiplier(self) -> int:
        """Gunakan untuk kalkulasi: 1 untuk Long, -1 untuk Short, 0 untuk Neutral."""
        return int(self.value)
    
    def opposite(self) -> 'SignalSide':
        """Inversi arah (LONG ↔ SHORT)."""
        return SignalSide(-self.value) if self.is_directional else self
    
    def __str__(self) -> str:
        return self.name

class SignalAction(IntEnum):
    """Aksi eksekusi dengan state transition matrix."""
    OPEN = auto()   # Memulai posisi baru
    CLOSE = auto()  # Menutup posisi yang ada
    HOLD = auto()   # Mempertahankan state saat ini
    
    # Matriks Kompatibilitas Aksi terhadap Sisi
    COMPATIBLE_SIDES: ClassVar[Dict['SignalAction', Tuple[SignalSide, ...]]] = {
        OPEN: (SignalSide.LONG, SignalSide.SHORT),
        CLOSE: (SignalSide.LONG, SignalSide.SHORT, SignalSide.NEUTRAL),
        HOLD: (SignalSide.LONG, SignalSide.SHORT, SignalSide.NEUTRAL)
    }
    
    @property
    def requires_active_position(self) -> bool:
        """Aksi yang membutuhkan posisi terbuka sebelumnya."""
        return self in (CLOSE, HOLD)

# ============================================================================
# VALUE OBJECTS (Immutable Data Carriers)
# ============================================================================

@dataclass(frozen=True, order=True)
class SignalStrength:
    """
    Objek Nilai Conviction Sinyal (Normalized [-1.0, 1.0]).
    Mengenkapsulasi logika normalisasi dan thresholding.
    """
    value: float = 0.0
    MIN: ClassVar[float] = -1.0
    MAX: ClassVar[float] = 1.0
    
    def __post_init__(self) -> None:
        if not isinstance(self.value, (int, float)):
            raise TypeError(f"Strength harus numerik, got {type(self.value)}")
        
        # Clamping otomatis pada inisialisasi
        clamped = max(self.MIN, min(self.MAX, float(self.value)))
        object.__setattr__(self, 'value', round(clamped, 4))
    
    @property
    def absolute(self) -> float:
        """Magnitudo tanpa arah (0.0 ke 1.0)."""
        return abs(self.value)
    
    def is_above(self, threshold: float) -> bool:
        """Evaluasi kekuatan terhadap ambang batas tertentu."""
        return self.absolute >= threshold

@dataclass(frozen=True)
class SignalMetadata:
    """Kontainer Metadata Imutabel dengan Type-Safe Access."""
    _data: Dict[str, Any] = field(default_factory=dict)
    
    def get(self, key: str, default: Any = None) -> Any:
        return self._data.get(key, default)
    
    def get_typed(self, key: str, expected_type: type, default: Any = None) -> Any:
        value = self.get(key, default)
        if value is not None and not isinstance(value, expected_type):
            raise TypeError(f"Key '{key}' mengharapkan {expected_type}, got {type(value)}")
        return value
    
    def update(self, **updates: Any) -> 'SignalMetadata':
        """Immutable pattern: mengembalikan instance baru dengan data terupdate."""
        return SignalMetadata({**self._data, **updates})
    
    def to_dict(self) -> Dict[str, Any]:
        return self._data.copy()

# ============================================================================
# MAIN SIGNAL EVENT (The Core Decision Unit)
# ============================================================================

@dataclass(frozen=True)
class SignalEvent(SignalValidatable):
    """
    Kapsul Sinyal ORCA - Keputusan Strategi Murni.
    Menerapkan validasi ketat dan interoperabilitas sistem.
    """
    timestamp: datetime
    side: SignalSide
    action: SignalAction
    strength: SignalStrength = field(default_factory=SignalStrength)
    metadata: SignalMetadata = field(default_factory=SignalMetadata)
    
    def __post_init__(self) -> None:
        # Fail Fast: Validasi Zona Waktu
        if self.timestamp.tzinfo is None:
            raise ValueError("SignalEvent WAJIB menggunakan timezone-aware datetime (UTC).")
        
        # Fail Fast: Kompatibilitas Aksi
        if self.side not in SignalAction.COMPATIBLE_SIDES[self.action]:
            raise ValueError(f"Aksi {self.action.name} tidak kompatibel dengan Sisi {self.side.name}")

    def validate(self) -> Result[bool, str]:
        """Validasi formal mengembalikan monad Result."""
        try:
            self.__post_init__()
            return Ok(True)
        except Exception as e:
            return Err(str(e))

    @property
    def is_valid(self) -> bool:
        return self.validate().is_ok()

    # --- Domain Logic Helpers ---
    
    def is_entry(self) -> bool:
        return self.action == SignalAction.OPEN and self.side.is_directional
    
    def is_exit(self) -> bool:
        return self.action == SignalAction.CLOSE
        
    def to_dict(self) -> Dict[str, Any]:
        """Serialisasi untuk penyimpanan/log."""
        return {
            'timestamp': self.timestamp.isoformat(),
            'side': self.side.name,
            'side_value': self.side.value,
            'action': self.action.name,
            'strength': self.strength.value,
            'metadata': self.metadata.to_dict()
        }

# ============================================================================
# FACTORY FUNCTIONS (The Safe Entry Points)
# ============================================================================

def create_neutral_signal(ts: Optional[datetime] = None) -> SignalEvent:
    """Membuat sinyal netral standar."""
    return SignalEvent(
        timestamp=ts or datetime.now(timezone.utc),
        side=SignalSide.NEUTRAL,
        action=SignalAction.HOLD,
        metadata=SignalMetadata({'type': 'system_neutral'})
    )

def create_directional_signal(
    side: SignalSide, 
    strength: float, 
    ts: Optional[datetime] = None,
    **meta: Any
) -> SignalEvent:
    """Membuat sinyal pembukaan posisi (Long/Short)."""
    return SignalEvent(
        timestamp=ts or datetime.now(timezone.utc),
        side=side,
        action=SignalAction.OPEN,
        strength=SignalStrength(strength),
        metadata=SignalMetadata({'type': 'strategy_signal', **meta})
    )

def create_exit_signal(
    side: SignalSide, 
    ts: Optional[datetime] = None,
    reason: str = "target_reached"
) -> SignalEvent:
    """Membuat sinyal penutupan posisi."""
    return SignalEvent(
        timestamp=ts or datetime.now(timezone.utc),
        side=side,
        action=SignalAction.CLOSE,
        strength=SignalStrength(1.0), # Exit biasanya memiliki conviction penuh
        metadata=SignalMetadata({'type': 'exit_signal', 'reason': reason})
    )
