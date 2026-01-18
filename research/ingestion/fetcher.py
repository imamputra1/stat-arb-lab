from dataclasses import dataclass
from datetime import datetime
from typing import List, Protocol, runtime_checkable
from pydantic import BaseModel, ConfigDict, Field, validator, ValidationError

# Domain and type
class OHCLV(BaseModel):
    """DTO Strict yang divalidasi ketika runtime."""
    model_config = ConfigDict(frozen=True, validate_assignment=True)

    timestamp: int = Field(..., description="Unix timestamp in milliseconds")
    open: float = Field(..., gt=0)
    high: float = Field(..., gt=0)
    close: float = Field(..., gt=0)
    low: float = Field(..., gt=0)
    volume: float = Field(..., gt=0)

    @validator("low")
    def low_must_be_lower_or_equal(cls, v, value,):
        if 'high' in value and v > value['high']:
            raise ValidationError('low price cannot be higher than high price')
        return v

@dataclass(frozen=True)
class FetchJob:
    symbol: str
    timeframe: str
    start_date: datetime
    end_date: Optional[datetime] = None

# Interface/Protocol
@runtime_checkable
class ExchangeProvider(Protocol):
    """Kontrak: apapun yang bisa ambil candle"""
    def fetch_candles(self, symbol: str, timeframe: str, since: int) -> List[OHCLV]: ...

@runtime_checkable
class StorageProvider(Protocol):
    """Kontrak: Apapun yang bisa disimpan"""
    def save(self, data: List[OHCLV], symbol: str, timeframe: str, since: int) -> None: ...

# Implementasi 

