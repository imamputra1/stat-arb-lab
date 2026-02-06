"""
MECHANICS FACTORY (THE ASSEMBLER)
Location: core/execution/mechanics/factory.py
Desc: Bertugas merakit komponen mechanics menjadi satu kesatuan (Suite).
      Memisahkan proses pembuatan object dari penggunaannya.
"""

from dataclasses import dataclass
from typing import Optional

# 1. Import Protocols (Interface)
from .protocols import (
    SlippageModel, LatencyModel, LiquidityModel, FeeModel
)

# 2. Import Configs (Blueprints)
from .config import (
    VolatilitySlippageConfig,
    NetworkCongestionLatencyConfig,
    StochasticLiquidityConfig,
    StandardFeeConfig
)

# 3. Import Models (Components)
from .models import (
    VolatilitySlippage,
    NetworkCongestionLatency,
    StochasticLiquidity,
    StandardFee
)

# ====================== THE SUITE (CONTAINER) ======================

@dataclass
class MechanicsSuite:
    """
    Wadah tunggal yang menyimpan semua 'alat penyiksa' pasar.
    Simulator akan menggunakan object ini untuk mengakses semua model.
    """
    slippage: SlippageModel
    latency: LatencyModel
    liquidity: LiquidityModel
    fee: FeeModel

# ====================== FACTORY FUNCTIONS ======================

def create_volatility_slippage(
    config: Optional[VolatilitySlippageConfig] = None
) -> VolatilitySlippage:
    """Merakit Slippage Model"""
    return VolatilitySlippage(config or VolatilitySlippageConfig())

def create_network_congestion_latency(
    config: Optional[NetworkCongestionLatencyConfig] = None
) -> NetworkCongestionLatency:
    """Merakit Latency Model"""
    return NetworkCongestionLatency(config or NetworkCongestionLatencyConfig())

def create_stochastic_liquidity(
    config: Optional[StochasticLiquidityConfig] = None
) -> StochasticLiquidity:
    """Merakit Liquidity Model"""
    return StochasticLiquidity(config or StochasticLiquidityConfig())

def create_standard_fee(
    config: Optional[StandardFeeConfig] = None
) -> StandardFee:
    """Merakit Fee Model"""
    return StandardFee(config or StandardFeeConfig())

# ====================== MASTER ASSEMBLER ======================

def create_mechanics_suite(
    slippage_cfg: Optional[VolatilitySlippageConfig] = None,
    latency_cfg: Optional[NetworkCongestionLatencyConfig] = None,
    liquidity_cfg: Optional[StochasticLiquidityConfig] = None,
    fee_cfg: Optional[StandardFeeConfig] = None
) -> MechanicsSuite:
    """
    [MAIN ENTRY POINT]
    Merakit seluruh komponen Mechanics menjadi satu Suite siap pakai.
    """
    return MechanicsSuite(
        slippage=create_volatility_slippage(slippage_cfg),
        latency=create_network_congestion_latency(latency_cfg),
        liquidity=create_stochastic_liquidity(liquidity_cfg),
        fee=create_standard_fee(fee_cfg)
    )

# ====================== EXPORTS ======================

__all__ = [
    'MechanicsSuite',
    'create_mechanics_suite',
    'create_volatility_slippage',
    'create_network_congestion_latency',
    'create_stochastic_liquidity',
    'create_standard_fee'
]
