from typing import Optional
from dataclasses import dataclass
from datetime import datetime
from pydantic import BaseModel, ConfigDict, Field, field_validator, ValidationInfo
import logging


class OHLCV(BaseModel):
    model_config = ConfigDict(frozen=True)

    timestamp: float = Field(..., description="Unix timestamp ms")
    open: float = Field(..., gt=0)
    high: float = Field(..., gt=0)
    low: float = Field(..., gt=0)
    close: float = Field(..., gt=0)
    volume: float = Field(..., gt=0)

    @field_validator("low")
    @classmethod
    def validator_low(cls, v: float, info: ValidationInfo) -> float:
        values = info.data
        if 'high' in values and v > values['high']:
            logging.warning(f"Anomaly: low {v} > high {values['high']}")
        return v

@dataclass(frozen=True)
class FetchJob:
    symbol: str
    source: str
    timeFrame: str
    start_date: datetime
    end_date: Optional[datetime] = None


