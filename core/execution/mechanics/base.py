"""
MECHANICS BASE CLASSES
Location: core/execution/mechanics/base.py
Desc: Abstract Base Classes (ABC) yang mengikat Protocol dengan Configuration.
      Setiap implementasi (Models) wajib mewarisi class ini.
      Sinkron dengan update parameter 'volume' di Protocol.
"""

from abc import ABC, abstractmethod
from typing import Optional

# Import Utilities
from core.shared.utils import get_logger

# Import Protocols & Configs
from .protocols import (
    SlippageModel, LatencyModel, LiquidityModel, FeeModel, FundingModel
)
from .config import (
    SlippageConfig, LatencyConfig, LiquidityConfig, FeeConfig, FundingConfig
)
from core.execution.types import Order

# ====================== 1. BASE SLIPPAGE ======================

class BaseSlippageModel(ABC, SlippageModel):
    """
    Base class untuk semua model Slippage.
    Menyimpan config dan logger.
    """
    def __init__(self, config: Optional[SlippageConfig] = None):
        # Gunakan default config jika None
        self._config = config or SlippageConfig()
        self.logger = get_logger(f"mechanics.slippage.{self.config.model_type}")

    @property
    def config(self) -> SlippageConfig:
        return self._config

    @abstractmethod
    def calculate_execution_price(
        self, 
        order: Order, 
        market_price: float, 
        volatility: float, 
        volume: float  # [FIX] Disamakan dengan Protocol & Models (sebelumnya avg_volume)
    ) -> float:
        """Wajib diimplementasikan oleh child class"""
        pass

# ====================== 2. BASE LATENCY ======================

class BaseLatencyModel(ABC, LatencyModel):
    """
    Base class untuk semua model Latency.
    """
    def __init__(self, config: Optional[LatencyConfig] = None):
        self._config = config or LatencyConfig()
        self.logger = get_logger(f"mechanics.latency.{self.config.model_type}")

    @property
    def config(self) -> LatencyConfig:
        return self._config

    @abstractmethod
    def calculate_delay_ms(self, volatility: float) -> int:
        """Wajib diimplementasikan oleh child class"""
        pass

# ====================== 3. BASE LIQUIDITY ======================

class BaseLiquidityModel(ABC, LiquidityModel):
    """
    Base class untuk semua model Likuiditas (Ghost Liquidity).
    """
    def __init__(self, config: Optional[LiquidityConfig] = None):
        self._config = config or LiquidityConfig()
        self.logger = get_logger("mechanics.liquidity")

    @property
    def config(self) -> LatencyConfig:
        return self.config

    @abstractmethod
    def should_fill(
        self, 
        order: Order, 
        tick_volume: float, 
        bid_ask_spread: float = 0.0
    ) -> bool:
        """Wajib diimplementasikan oleh child class"""
        pass

# ====================== 4. BASE FEE ======================

class BaseFeeModel(ABC, FeeModel):
    """
    Base class untuk semua model Fee.
    """
    def __init__(self, config: Optional[FeeConfig] = None):
        self._config = config or FeeConfig()
        self.logger = get_logger(f"mechanics.fee.{self.config.model_type}")

    @property
    def config(self) -> FeeConfig:
        return self._config

    @abstractmethod
    def calculate_fee(
        self, 
        quantity: float, 
        price: float, 
        is_maker: bool,
        symbol: str
    ) -> float:
        """Wajib diimplementasikan oleh child class"""
        pass


# ====================== 5. BASE FUNDING RATE ======================
class BaseFundingModel(ABC, FundingModel):
    """
    Base class untuk semua model Funding.
    """
    def __init__(self, config: Optional[FundingConfig] = None):
        self._config = config or FundingConfig()
        self.logger = get_logger(f"mechanics.funding.{self.config.model_type}")

    @property
    def config(self) -> FundingConfig:
        return self._config

    @abstractmethod
    def calculate_funding_fee(
        self,
        position_size: float,
        mark_price: float,
        time_elapsed: float,
        current_timestamp: float
    ) -> float:
        pass

# ====================== EXPORTS ======================

__all__ = [
    'BaseSlippageModel', 
    'BaseLatencyModel', 
    'BaseLiquidityModel', 
    'BaseFeeModel',
    'BaseFundingModel'
]
