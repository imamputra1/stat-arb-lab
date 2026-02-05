"""
Signal Types and Events - Protocol definitions for ORCA trading system
"""

from enum import Enum, IntEnum
from dataclasses import dataclass, field
from typing import Dict, Any, List
import pandas as pd

from ..shared.result import Result, Ok, Err

# ========== SIGNAL ENUMS ==========

class SignalType(IntEnum):
    """
    Trading signal types with integer values for efficient storage.
    """
    NEUTRAL = 0      # No action
    BUY = 1          # Long position
    SELL = 2         # Short position
    EXIT = 3         # Exit current position
    STOP = 4         # Emergency stop
    
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

class SignalStrength(Enum):
    """Signal strength categories"""
    WEAK = "weak"        # 0.0 - 1.0
    MODERATE = "moderate" # 1.0 - 2.0
    STRONG = "strong"    # 2.0 - 3.0
    EXTREME = "extreme"  # > 3.0

class MarketState(Enum):
    """Market regime states"""
    IDLE = "idle"              # No data processing
    ACCUMULATING = "accumulating"  # Building position
    TRENDING = "trending"      # Strong trend detected
    MEAN_REVERTING = "mean_reverting"  # Mean reversion regime
    VOLATILE = "volatile"      # High volatility
    SHOCK = "shock"            # Market shock detected
    
    @classmethod
    def from_volatility(cls, volatility: float, threshold: float = 0.02) -> 'MarketState':
        """Determine market state from volatility"""
        if volatility < threshold * 0.5:
            return cls.IDLE
        elif volatility < threshold:
            return cls.MEAN_REVERTING
        elif volatility < threshold * 2:
            return cls.TRENDING
        else:
            return cls.VOLATILE

# ========== SIGNAL EVENTS ==========

@dataclass(frozen=True)
class SignalEvent:
    """
    Immutable signal event for trading decisions.
    Used in both research and live paths.
    """
    timestamp: int                     # Unix timestamp in milliseconds
    signal_type: SignalType            # Trading decision
    strength: float                    # Z-score or confidence score
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        """Validate signal event"""        
        if not isinstance(self.timestamp, int) or self.timestamp <= 0:
            raise ValueError("Timestamp must be positive integer")
    
    def to_dict(self) -> dict:
        """Convert to dictionary for serialization"""
        return {
            'timestamp': self.timestamp,
            'signal_type': self.signal_type.value,
            'signal_type_name': self.signal_type.name,
            'strength': self.strength,
            'metadata': self.metadata
        }
    
    def is_actionable(self, threshold: float = 1.5) -> bool:
        """Check if signal is actionable based on strength threshold"""
        return abs(self.strength) >= threshold
    
    @property
    def is_entry(self) -> bool:
        return self.signal_type in (SignalType.BUY, SignalType.SELL)

    @property
    def is_exit(self) -> bool:
        return self.signal_type in (SignalType.EXIT, SignalType.STOP)

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

@dataclass
class MarketObservation:
    """
    Live market observation for real-time processing.
    Agnostic for data content (Spread/Price/RSI/OrderBook).
    """
    timestamp: int
    data: Dict[str, Any]
    source: str = "unknown"
    
    def get_value(self, key: str, type_cast: type = float) -> Result[Any, str]:
        """
        Safe Extraction with type checking.
        Mencegah error 'KeyError' atau 'TypeError'
        """
        if key not in self.data:
            return Err(f"Key '{key}' not found in observation")

        try:
            raw_val = self.data[key]
            val = type_cast(raw_val)
            return Ok(val)
        except Exception:
            return Err(f"Failed to cast '{key}', to '{type_cast.__name__}'")
    
    def get_price(self, asset: str) -> Result[float, str]:
        """Shortcut ambil harga"""
        return self.get_value(f"close_{asset}", float)

    def get_volume(self, asset: str) -> Result[float, str]:
        """Shortcut ambil volume"""
        return self.get_value(f"volume_{asset}", float)


# ========== STRATEGY CONFIGURATION ==========

@dataclass(frozen=True)
class SignalConfig:
    """
    Configuration SPECIFIC to Signal Generation Logic (Thresholds & Risk).
    NOTE: Math parameters (R, Q) belong to KalmanConfig in core.math.
    """
    name: str
    
    # Signal Logic Parameters (Z-Score Thresholds)
    entry_z_score: float = 2.0    # Trigger Entry (Short > 2.0, Long < -2.0)
    exit_z_score: float = 0.5     # Trigger Exit (Back to Mean)
    stop_loss_z: float = 4.0      # Circuit Breaker (Too volatile)
    
    # Position Sizing & Risk
    max_position: float = 1.0     # Max allocation (1.0 = 100%)
    hedge_ratio: float = 1.0      # Default hedge ratio (bisa override nanti)
    
    # Metadata
    version: str = "1.0.0"
    
    def validate(self) -> Result[None, str]:
        """Fail-fast validation for logic errors"""
        if self.entry_z_score <= self.exit_z_score:
            return Err(f"Entry threshold ({self.entry_z_score}) must be higher than exit threshold ({self.exit_z_score})")
            
        if self.stop_loss_z <= self.entry_z_score:
            return Err(f"Stop loss ({self.stop_loss_z}) must be wider than entry threshold ({self.entry_z_score})")
            
        if self.max_position <= 0:
            return Err("Max position must be positive")
            
        return Ok(None)


