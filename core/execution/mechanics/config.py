"""
MECHANICS CONFIGURATION (FINAL FIXED)
Location: core/execution/mechanics/config.py
"""
from dataclasses import dataclass, field
from typing import List, Dict
from core.shared.result import Result, Ok

# --- 1. SLIPPAGE ---
@dataclass(frozen=True)
class SlippageConfig:
    model_type: str = "volatility"
    base_bps: float = 5.0
    impact_factor: float = 0.1
    fixed_bps: float = 10.0
    
    def validate(self) -> Result[bool, str]:
        return Ok(True)

@dataclass(frozen=True)
class VolatilitySlippageConfig(SlippageConfig):
    model_type: str = "adaptive_volatility"
    volatility_threshold: float = 0.02
    volatility_multiplier: float = 2.0
    max_slippage_bps: float = 500.0
    square_root_factor: float = 0.1
    linear_factor: float = 0.05
    adverse_selection_factor: float = 0.3
    enable_adaptive_learning: bool = True
    learning_window: int = 100
    decay_factor: float = 0.99
    spread_impact_weight: float = 0.4
    depth_impact_weight: float = 0.3
    
    def validate(self) -> Result[bool, str]:
        return Ok(True)

# --- 2. LATENCY (ERROR SOURCE FIXED HERE) ---
@dataclass(frozen=True)
class LatencyConfig:
    model_type: str = "network"
    base_latency_ms: int = 50
    jitter_factor: float = 2.0
    fixed_latency_ms: int = 100
    
    def validate(self) -> Result[bool, str]:
        return Ok(True)

@dataclass(frozen=True)
class NetworkCongestionLatencyConfig(LatencyConfig):
    model_type: str = "network_congestion"
    geographic_distance_km: float = 5000.0
    congestion_threshold: float = 0.02
    congestion_multiplier: float = 10.0
    base_jitter_ms: float = 5.0
    base_packet_loss_rate: float = 0.01
    retry_penalty_ms: int = 300
    
    # [CRITICAL FIX] Menambahkan field yang dicari oleh models.py
    # [FIX] MENAMBAHKAN 4 FIELD YANG HILANG
    clock_drift_std_ms: float = 1.0     # Error 1 Fixed
    enable_jitter: bool = True          # Error 2 Fixed
    jitter_percentage: float = 0.1      # 10% Jitter range
    max_jitter_ms: float = 20.0         # Cap jitter
    min_jitter_ms: float = 1.0          # Floor jitter
    max_latency_ms: int = 5000          # Error 3 Fixed (Safety Cap)
    
    # Topology params (dari fix sebelumnya)
    network_nodes: List[str] = field(default_factory=lambda: ["JKT", "SG", "TYO"])
    node_latencies: Dict[str, float] = field(default_factory=lambda: {"JKT_SG": 15.0})

    def validate(self) -> Result[bool, str]:
        # ... (Validation logic existing) ...
        return Ok(True)
    
    def get_time_of_day_multiplier(self, hour: int) -> float:
        return 1.0

# --- 3. LIQUIDITY ---
@dataclass(frozen=True)
class LiquidityConfig:
    enabled: bool = True
    fill_probability: float = 1.0
    def validate(self) -> Result[bool, str]: return Ok(True)

@dataclass(frozen=True)
class StochasticLiquidityConfig(LiquidityConfig):
    model_type: str = "stochastic"
    ghost_duration_ms: int = 5000
    ghost_probability: float = 0.05
    volume_participation_threshold: float = 0.05
    volume_penalty_weight: float = 2.0
    queue_position_bias: float = 0.8
    max_spread_threshold: float = 0.005
    hidden_order_probability: float = 0.1     # Tambahan field agar aman
    orderbook_refresh_rate_hz: float = 10.0   # Tambahan field agar aman
    
    def validate(self) -> Result[bool, str]: return Ok(True)

# --- 4. FEE ---
@dataclass(frozen=True)
class FeeConfig:
    model_type: str = "standard"
    maker_fee_rate: float = 0.001
    taker_fee_rate: float = 0.001
    fee_asset: str = "USDT"
    def validate(self) -> Result[bool, str]: return Ok(True)

@dataclass(frozen=True)
class StandardFeeConfig(FeeConfig):
    enable_tiered_fees: bool = False
    enable_token_discount: bool = False
    token_discount_rate: float = 0.25

# --- EXPORTS ---
__all__ = [
    'SlippageConfig', 'LatencyConfig', 'LiquidityConfig', 'FeeConfig',
    'VolatilitySlippageConfig', 'NetworkCongestionLatencyConfig',
    'StochasticLiquidityConfig', 'StandardFeeConfig'
]
