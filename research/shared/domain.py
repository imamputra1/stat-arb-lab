"""
DOMAIN MODULE - Type Disciplined, ADHD Optimized
Pure value objects with structural composition patterns
"""
from typing import Optional, Protocol, runtime_checkable, Any, List, Tuple
from dataclasses import dataclass
from datetime import datetime
from pydantic import BaseModel, ConfigDict, Field, field_validator, ValidationInfo

# ====================== STRUCTURAL PROTOCOLS ======================
@runtime_checkable
class OHLCVContract(Protocol):
    """Structural contract for OHLCV data - no inheritance, pure protocol"""
    timestamp: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    
    def to_dict(self) -> dict: ...
    def to_series(self) -> 'OHLCV': ...
    def validate_price_consistency(self) -> bool: ...

@runtime_checkable
class FetchJobContract(Protocol):
    """Structural contract for fetch jobs"""
    symbol: str
    source: str
    timeframe: str
    start_date: datetime
    end_date: Optional[datetime]
    
    def get_date_range(self) -> Tuple[int, Optional[int]]: ...
    def is_valid(self) -> bool: ...

# ====================== COMPOSITION-BASED IMPLEMENTATIONS ======================

class OHLCV(BaseModel):
    """Immutable OHLCV value object - optimized for batch processing"""
    model_config = ConfigDict(
        frozen=True,            # Immutable = Safe for concurrency
        strict=True,            # No type coercion = Predictable
        validate_assignment=True,  # Validate even on attribute access
        extra='forbid',         # No extra fields = Type discipline
        revalidate_instances='never'  # Performance: skip revalidation
    )
    
    timestamp: int = Field(
        ..., 
        gt=946684800000,       # Reject timestamps < year 2000 (approx)
        lt=10_000_000_000_000, # Reject future timestamps (>2286)
        description="Unix timestamp in milliseconds"
    )
    
    open: float = Field(..., gt=0, description="Opening price")
    high: float = Field(..., gt=0, description="Highest price")
    low: float = Field(..., gt=0, description="Lowest price")
    close: float = Field(..., gt=0, description="Closing price")
    volume: float = Field(..., ge=0, description="Trade volume")
    
    @field_validator('low', mode='after')
    @classmethod
    def validate_price_range(cls, v: float, info: ValidationInfo) -> float:
        """Fast price consistency check - O(1) validation"""
        data = info.data
        if 'high' in data and v > data['high']:
            raise ValueError(f"Low price ({v}) > High price ({data['high']})")
        return v
    
    # ========== COMPOSITION METHODS (No Inheritance) ==========
    
    def to_dict(self) -> dict:
        """Fast conversion to dict - optimized for serialization"""
        return self.model_dump()
    
    def to_series(self) -> 'OHLCV':
        """Return self (already a series) - for protocol compliance"""
        return self
    
    def validate_price_consistency(self) -> bool:
        """Pure function for price validation"""
        return (self.low <= self.high and 
                self.open > 0 and 
                self.high > 0 and 
                self.low > 0 and 
                self.close > 0)

@dataclass(frozen=True)
class FetchJob:
    """Immutable fetch job specification - pure value object"""
    symbol: str      # e.g., "BBCA.JK" or "BTC/USDT"
    source: str      # "yahoo" or "ccxt"
    timeframe: str   # "1d", "1h", "5m"
    start_date: datetime
    end_date: Optional[datetime] = None
    
    def __post_init__(self):
        """Fast validation at construction time"""
        # ADHD-friendly: Fail fast, fail loudly
        if self.start_date > datetime.now():
            raise ValueError("Start date cannot be in the future")
        
        if self.end_date and self.start_date > self.end_date:
            raise ValueError("Start date must be before end date")
        
        # Validate timeframe format (quick regex equivalent)
        if not any(self.timeframe.endswith(unit) for unit in ['m', 'h', 'd', 'w']):
            raise ValueError(f"Invalid timeframe: {self.timeframe}")
    
    # ========== COMPOSITION METHODS ==========
    
    def get_date_range(self) -> Tuple[int, Optional[int]]:
        """Return (start_timestamp_ms, end_timestamp_ms)"""
        start_ts = int(self.start_date.timestamp() * 1000)
        end_ts = int(self.end_date.timestamp() * 1000) if self.end_date else None
        return (start_ts, end_ts)
    
    def is_valid(self) -> bool:
        """Quick validity check without exceptions"""
        try:
            self.__post_init__()
            return True
        except ValueError:
            return False
    
    def with_end_date(self, end_date: datetime) -> 'FetchJob':
        """Immutable update - returns new instance"""
        return FetchJob(
            symbol=self.symbol,
            source=self.source,
            timeframe=self.timeframe,
            start_date=self.start_date,
            end_date=end_date
        )

# ====================== DOMAIN UTILITIES (Pure Functions) ======================

def create_ohlcv_bulk(
    timestamps: List[int],
    opens: List[float],
    highs: List[float],
    lows: List[float],
    closes: List[float],
    volumes: List[float]
) -> List[OHLCV]:
    """
    Batch creation of OHLCV objects - optimized for ADHD (no loops in user code)
    Uses composition: List[T] -> List[OHLCV] without mutation
    """
    if not all(len(lst) == len(timestamps) for lst in [opens, highs, lows, closes, volumes]):
        raise ValueError("All input lists must have same length")
    
    return [
        OHLCV(
            timestamp=ts,
            open=o,
            high=h,
            low=l,
            close=c,
            volume=v
        )
        for ts, o, h, l, c, v in zip(timestamps, opens, highs, lows, closes, volumes)
    ]

def validate_ohlcv_batch(data: List[OHLCV]) -> Tuple[List[OHLCV], List[str]]:
    """
    Fast batch validation using composition pattern
    Returns: (valid_items, error_messages)
    """
    valid = []
    errors = []
    
    for item in data:
        try:
            # Trigger validation by accessing a property
            if item.validate_price_consistency():
                valid.append(item)
        except Exception as e:
            errors.append(f"Validation failed: {e}")
    
    return valid, errors

# ====================== TYPE GUARDS (Structural Checking) ======================

def is_valid_ohlcv(obj: Any) -> bool:
    """Runtime structural type checking"""
    # Menggunakan Any (huruf besar) dari typing
    return isinstance(obj, OHLCVContract) and obj.validate_price_consistency()

def is_valid_fetch_job(obj: Any) -> bool:
    """Runtime structural type checking"""
    # Menggunakan Any (huruf besar) dari typing
    return isinstance(obj, FetchJobContract) and obj.is_valid()
