"""
Signal Types and Events - Protocol definitions for ORCA trading system
"""

from enum import Enum
from dataclasses import dataclass, field
from typing import Dict, Any, List, Type, TypeVar # Tambahkan Type, TypeVar
import pandas as pd
import time
import json
import uuid
from types import MappingProxyType

from ..shared.result import Result, Ok, Err
T = TypeVar("T")


# ========== SIGNAL ENUMS ==========

class SignalType(Enum):
    """Jenis Sinyal yang bisa dihasilkan Strategy"""
    BUY = "BUY"           # Entry Long / Buy Spread
    SELL = "SELL"         # Entry Short / Sell Spread
    EXIT = "EXIT"         # Keluar Posisi (TP/Normal Close)
    STOP = "STOP"         # Cut Loss (Emergency)
    NEUTRAL = "NEUTRAL"   # Wait & See
    
    @classmethod
    def from_str(cls, value: str) -> 'SignalType':
        """Convert string to SignalType"""
        mapping = {
            'neutral': cls.NEUTRAL,
            'buy': cls.BUY,
            'sell': cls.SELL,
            'exit': cls.EXIT,
            'stop': cls.STOP
        }
        return mapping.get(value.lower(), cls.NEUTRAL)

class SignalStrength(str, Enum):
    """
    Categorizes signal confidence levels.
    Inherits 'str' for automatic JSON serialization.
    Supports comparison operators (e.g., EXTREME > WEAK).
    """
    WEAK = "weak"         # 0.0 - 1.0
    MODERATE = "moderate" # 1.0 - 2.0
    STRONG = "strong"     # 2.0 - 3.0
    EXTREME = "extreme"   # > 3.0

    @classmethod
    def from_score(cls, score: float) -> 'SignalStrength':
        """
        Factory method: Convert raw Z-Score to Strength Category.
        Industrial Standard: Absolute value handling.
        """
        abs_score = abs(score)
        
        if abs_score >= 3.0:
            return cls.EXTREME
        elif abs_score >= 2.0:
            return cls.STRONG
        elif abs_score >= 1.0:
            return cls.MODERATE
        return cls.WEAK

    @property
    def level(self) -> int:
        """Internal integer level for comparison"""
        ordering = {
            "weak": 1,
            "moderate": 2,
            "strong": 3,
            "extreme": 4
        }
        return ordering[self.value]

    def __lt__(self, other):
        """Enables sorting: signal_a < signal_b"""
        if self.__class__ is other.__class__:
            return self.level < other.level
        return NotImplemented

    def __ge__(self, other):
        """Enables threshold check: signal >= STRONG"""
        if self.__class__ is other.__class__:
            return self.level >= other.level
        return NotImplemented


class MarketState(str, Enum):
    """
    Categorizes market regime based on volatility and trend.
    Inherits 'str' for automatic JSON serialization.
    """
    IDLE = "idle"               # Low vol, no activity
    ACCUMULATING = "accumulating" # Low vol, potential breakout
    TRENDING = "trending"       # Directed movement
    MEAN_REVERTING = "mean_reverting" # Normal oscillation
    VOLATILE = "volatile"       # High volatility, wide stops needed
    SHOCK = "shock"             # Extreme volatility, circuit breaker needed
    
    @classmethod
    def from_volatility(cls, volatility: float, threshold: float = 0.02) -> 'MarketState':
        """
        Determine market state dynamically from rolling volatility.
        
        Args:
            volatility: Current rolling std dev (percentage)
            threshold: Baseline volatility threshold (e.g., 0.02 for crypto)
        """
        if volatility < threshold * 0.2:
            return cls.IDLE
        elif volatility < threshold * 0.5:
            return cls.ACCUMULATING
        elif volatility < threshold * 1.5:
            return cls.MEAN_REVERTING # Sweet spot for Kalman
        elif volatility < threshold * 3.0:
            return cls.VOLATILE
        else:
            return cls.SHOCK

    @property
    def is_tradeable(self) -> bool:
        """Safety check: Is it safe to open positions?"""
        return self not in (self.IDLE, self.SHOCK)

    @property
    def requires_wide_stops(self) -> bool:
        """Risk management hint"""
        return self in (self.VOLATILE, self.SHOCK)


# ========== SIGNAL EVENTS ==========

@dataclass(frozen=True, slots=True)
class SignalEvent:
    """
    Industrial Grade Immutable Signal Event.
    Optimized for memory (slots), traceability (uuid), and safety (frozen).
    """
    # --- IDENTITY & TIME ---
    timestamp: int                         # Unix timestamp (ms) - Waktu Data Market
    signal_type: SignalType                # Keputusan Trading
    strength: float                        # Confidence/Z-Score
    
    # --- TRACEABILITY ---
    symbol: str = ""                       # Asset Pair (e.g., "DOGE/USDT")
    strategy_name: str = "unknown"         # Nama strategi pengirim
    event_id: str = field(default_factory=lambda: uuid.uuid4().hex) # Unique ID untuk tracing
    created_at: int = field(default_factory=lambda: int(time.time() * 1000)) # Waktu Sinyal Dibuat (Latency check)
    
    # --- METADATA (Immutable) ---
    # Menggunakan private dict + property agar benar-benar read-only
    _metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        """High-performance validation"""
        # Validasi ringan (Critical Checks only)
        if self.timestamp <= 0:
            object.__setattr__(self, 'timestamp', int(time.time() * 1000)) # Auto-repair timestamp
            
    @property
    def metadata(self) -> MappingProxyType:
        """Expose metadata as read-only proxy"""
        return MappingProxyType(self._metadata)

    # --- SERIALIZATION ---
    def to_dict(self) -> Dict[str, Any]:
        """Fast serialization for logging/database"""
        return {
            'event_id': self.event_id,
            'timestamp': self.timestamp,
            'created_at': self.created_at,
            'latency_ms': self.created_at - self.timestamp, # Metric Latency
            'symbol': self.symbol,
            'strategy': self.strategy_name,
            'signal_type': self.signal_type.value,
            'signal_name': self.signal_type.name,
            'strength': round(self.strength, 5), # Presisi numerik
            'metadata': self._metadata
        }
    
    def to_json(self) -> str:
        """Helper untuk log JSON"""
        # Handle Enum serialization secara otomatis
        return json.dumps(self.to_dict(), default=str)

    # --- LOGIC HELPERS ---
    def is_actionable(self, threshold: float = 1.5) -> bool:
        """Filter noise berdasarkan kekuatan sinyal"""
        # Neutral tidak pernah actionable
        if self.signal_type == SignalType.NEUTRAL:
            return False
        return abs(self.strength) >= threshold
    
    @property
    def is_entry(self) -> bool:
        return self.signal_type in (SignalType.BUY, SignalType.SELL)

    @property
    def is_exit(self) -> bool:
        return self.signal_type in (SignalType.EXIT, SignalType.STOP)
        
    # --- COMPARISON (Sorting Support) ---
    def __lt__(self, other):
        """Memungkinkan sorting list[SignalEvent] berdasarkan waktu"""
        if not isinstance(other, SignalEvent):
            return NotImplemented
        return self.timestamp < other.timestamp


@dataclass
class SignalBatch:
    """Batch of signal events for research path"""
    events: List[SignalEvent]
    start_timestamp: int
    end_timestamp: int
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dataframe(self) -> pd.DataFrame:
        """Convert batch to DataFrame"""
        data = [event.to_dict() for event in self.events]
        df = pd.DataFrame(data)
        df['datetime'] = pd.to_datetime(df['timestamp'], unit='ms')
        return df
    
    def filter_by_type(self, signal_type: SignalType) -> 'SignalBatch':
        """Filter events by signal type"""
        filtered = [e for e in self.events if e.signal_type == signal_type]
        return SignalBatch(
            events=filtered,
            start_timestamp=self.start_timestamp,
            end_timestamp=self.end_timestamp,
            metadata=self.metadata
        )
    
    def get_statistics(self) -> dict:
        """Calculate batch statistics"""
        if not self.events:
            return {}
        
        strengths = [e.strength for e in self.events]
        buy_signals = sum(1 for e in self.events if e.signal_type == SignalType.BUY)
        sell_signals = sum(1 for e in self.events if e.signal_type == SignalType.SELL)
        
        return {
            'total_events': len(self.events),
            'buy_signals': buy_signals,
            'sell_signals': sell_signals,
            'neutral_signals': len(self.events) - buy_signals - sell_signals,
            'mean_strength': sum(strengths) / len(strengths),
            'max_strength': max(strengths),
            'min_strength': min(strengths),
            'actionable_signals': sum(1 for e in self.events if e.is_actionable()),
            'duration_hours': (self.end_timestamp - self.start_timestamp) / (1000 * 3600)
        }

# ========== MARKET OBSERVATION ==========

@dataclass(frozen=True, slots=True)
class MarketObservation:
    """
    Live market observation for real-time processing.
    Agnostic data carrier (Spread/Price/RSI/OrderBook).
    Immutable, Memory Efficient (slots), & Latency Aware.
    """
    timestamp: int            # Waktu terjadinya data di Exchange (Event Time)
    data: Dict[str, Any]      # Raw Payload (Price, Volume, Orderbook, etc)
    symbol: str = "unknown"   # Context Symbol (e.g., "DOGE/USDT")
    source: str = "unknown"   # Context Source (e.g., "binance_spot", "parquet")
    
    # [LATENCY TRACKING]
    # Waktu data ini mendarat di Engine/Memory kita
    received_at: int = field(default_factory=lambda: int(time.time() * 1000))

    def __post_init__(self):
        """
        Auto-repair timestamp & Validation.
        Jika timestamp dari source 0 atau invalid, gunakan waktu terima.
        """
        # Bypass frozen check menggunakan object.__setattr__
        if not isinstance(self.timestamp, (int, float)) or self.timestamp <= 0:
             object.__setattr__(self, 'timestamp', self.received_at)

    @property
    def latency_ms(self) -> int:
        """
        Critical Metric: Seberapa 'basi' data ini saat diproses?
        High latency (>1000ms) bisa men-trigger Circuit Breaker.
        """
        return max(0, self.received_at - self.timestamp)

    def get_value(self, key: str, type_cast: Type[T] = float) -> Result[T, str]:
        """
        Safe Extraction with Result Pattern.
        Mencegah 'KeyError' atau 'TypeError' yang sering membunuh Engine Live.
        
        Usage:
            price = obs.get_value("close", float).unwrap_or(0.0)
        """
        if key not in self.data:
            return Err(f"Key '{key}' missing in observation for {self.symbol}")

        try:
            raw_val = self.data[key]
            
            # Handle None explicitly (Common data issue)
            if raw_val is None:
                return Err(f"Value for '{key}' is None")
                
            # Lakukan casting
            val = type_cast(raw_val)
            return Ok(val)
            
        except (ValueError, TypeError) as e:
            return Err(f"Cast failed for '{key}' to '{type_cast.__name__}': {str(e)}")
    
    def get_price(self, asset: str = "") -> Result[float, str]:
        """
        Shortcut cerdas untuk mengambil harga.
        Jika asset kosong, coba cari 'close' atau 'price' standar.
        Jika ada asset, cari 'close_DOGE' dst.
        """
        # 1. Coba Specific Asset (Multi-asset mode)
        if asset:
            keys_to_try = [f"close_{asset}", f"price_{asset}"]
        else:
            # 2. Coba Default Keys (Single-asset mode)
            keys_to_try = ["close", "price", "last", "value"]

        for key in keys_to_try:
            res = self.get_value(key, float)
            if res.is_ok():
                return res

        return Err(f"No price found for asset '{asset}'. Tried: {keys_to_try}")

    def get_volume(self, asset: str = "") -> Result[float, str]:
        """Shortcut cerdas untuk mengambil volume"""
        if asset:
            key = f"volume_{asset}"
        else:
            key = "volume"
            
        return self.get_value(key, float)

    def to_dict(self) -> Dict[str, Any]:
        """
        Serialization optimized untuk Structured Logging (JSON).
        Tidak me-log seluruh 'data' blob agar log tidak meledak.
        """
        return {
            'ts_event': self.timestamp,
            'ts_recv': self.received_at,
            'latency_ms': self.latency_ms,
            'symbol': self.symbol,
            'source': self.source,
            # Log keys-nya saja untuk debug, valuenya hidden
            'available_keys': list(self.data.keys()) 
        }

# ========== STRATEGY CONFIGURATION ==========
@dataclass(frozen=True, slots=True)
class SignalConfig:
    """
    Configuration SPECIFIC to Signal Generation Logic.
    Bertindak sebagai 'Gatekeeper' agar Engine tidak menjalankan strategi dengan parameter ngawur.
    
    Compatible with: live/config.py -> STRATEGY_CONFIG['signal_params']
    """
    name: str
    
    # --- LOGIC PARAMETERS (The Trigger) ---
    entry_z_score: float = 2.0    # Masuk saat deviasi tinggi
    exit_z_score: float = 0.0     # Keluar saat kembali ke mean
    stop_loss_z: float = 4.0      # Emergency break
    
    # --- RISK & SIZING ---
    max_position: float = 1.0     # Max exposure unit
    hedge_ratio: float = 1.0      # Beta awal
    
    # --- CALCULATION WINDOW ---
    # [CRITICAL] Parameter ini yang menghapus hardcode '50' di strategi
    volatility_window: int = 50   
    
    # --- META ---
    version: str = "1.0.0"

    @classmethod
    def from_dict(cls, config: Dict[str, Any]) -> Result['SignalConfig', str]:
        """
        Factory Method: Safe parsing dari Dictionary (misal dari YAML/JSON).
        Otomatis melakukan Type Casting dan Validasi.
        """
        try:
            # 1. Extract & Cast (Mencegah error string vs float)
            instance = cls(
                name=str(config.get("name", "Unknown_Strategy")),
                entry_z_score=float(config.get("entry_z_score", 2.0)),
                exit_z_score=float(config.get("exit_z_score", 0.0)),
                stop_loss_z=float(config.get("stop_loss_z", 4.0)),
                max_position=float(config.get("max_position", 1.0)),
                hedge_ratio=float(config.get("hedge_ratio", 1.0)),
                volatility_window=int(config.get("volatility_window", 50)),
                version=str(config.get("version", "1.0.0"))
            )
            
            # 2. Validate Logic
            return instance.validate().map(lambda _: instance)
            
        except (ValueError, TypeError) as e:
            return Err(f"Config Parsing Failed: {str(e)}")

    def validate(self) -> Result[None, str]:
        """
        Fail-fast validation.
        Mencegah strategi berjalan dengan logika terbalik.
        """
        # Rule 1: Entry harus lebih ekstrem dari Exit
        # Kita pakai abs() karena entry bisa negatif (Long) atau positif (Short)
        # Asumsi: Logic Mean Reversion standar (Entry di luar, Exit di tengah)
        if abs(self.entry_z_score) <= abs(self.exit_z_score):
            return Err(
                f"Entry Z ({self.entry_z_score}) must be further from zero than Exit Z ({self.exit_z_score})"
            )
            
        # Rule 2: Stop Loss harus lebih jauh dari Entry
        if abs(self.stop_loss_z) <= abs(self.entry_z_score):
            return Err(
                f"Stop Loss ({self.stop_loss_z}) must be wider than Entry ({self.entry_z_score})"
            )
            
        # Rule 3: Technical requirements
        if self.volatility_window < 5:
            return Err(f"Volatility Window too small ({self.volatility_window}). Min: 5")
            
        if self.max_position <= 0:
            return Err(f"Max Position must be positive. Got: {self.max_position}")

        return Ok(None)
