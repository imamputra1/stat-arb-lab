"""
MECHANICS MODULE
Location: core/execution/mechanics/__init__.py
Desc: Facade untuk Execution Mechanics. 
      Menggabungkan Protocols, Configs, Models, dan Factory menjadi satu paket import.
"""

# 1. Protocols (The Laws)
from .protocols import (
    SlippageModel, 
    LatencyModel, 
    LiquidityModel, 
    FeeModel,
    FundingModel
)

# 2. Configs (The Parameters)
from .config import (
    # Base Configs
    SlippageConfig, 
    LatencyConfig, 
    LiquidityConfig, 
    FeeConfig,
    FundingConfig,
    PerpetualFundingConfig,
    
    # Specific/Sadistic Configs
    VolatilitySlippageConfig,
    NetworkCongestionLatencyConfig,
    StochasticLiquidityConfig,
    StandardFeeConfig
)

# 3. Base Classes (The Foundation)
from .base import (
    BaseSlippageModel, 
    BaseLatencyModel, 
    BaseLiquidityModel, 
    BaseFeeModel,
    BaseFundingModel
)

# 4. Implementations (The Torture Tools)
from .models import (
    VolatilitySlippage,
    NetworkCongestionLatency,
    StochasticLiquidity,
    StandardFee
)

# 5. Factory (The Assembler)
from .factory import (
    MechanicsSuite,
    create_mechanics_suite,
    create_volatility_slippage,
    create_network_congestion_latency,
    create_stochastic_liquidity,
    create_standard_fee,
)

__all__ = [
    # Protocols
    'SlippageModel', 'LatencyModel', 'LiquidityModel', 'FeeModel', 'FundingModel',
    
    # Base Configs
    'SlippageConfig', 'LatencyConfig', 'LiquidityConfig', 'FeeConfig', 'FundingConfig',
    
    # Specific Configs
    'VolatilitySlippageConfig', 
    'NetworkCongestionLatencyConfig',
    'StochasticLiquidityConfig', 
    'StandardFeeConfig',
    'PerpetualFundingConfig',
    
    # Base Classes
    'BaseSlippageModel', 'BaseLatencyModel', 'BaseLiquidityModel', 'BaseFeeModel', 'BaseFundingModel',
    
    # Concrete Models
    'VolatilitySlippage', 
    'NetworkCongestionLatency', 
    'StochasticLiquidity', 
    'StandardFee',
    'PerpetualFunding',
    
    # Factory & Suite
    'MechanicsSuite',
    'create_mechanics_suite',
    'create_volatility_slippage',
    'create_network_congestion_latency',
    'create_stochastic_liquidity',
    'create_standard_fee',
    'create_perpetual_funding'
]
