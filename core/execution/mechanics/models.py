"""
INDUSTRIAL-GRADE MECHANICS MODELS (THE TORTURE TOOLS)
Location: core/execution/mechanics/models.py
Desc: Implementasi konkret dari simulasi friksi pasar.
"""

import random
import math
import time
from typing import Optional, Dict

from core.execution.types import (
    Order, OrderSide, Symbol
)

from .config import (
    VolatilitySlippageConfig,
    NetworkCongestionLatencyConfig,
    StandardFeeConfig,
    StochasticLiquidityConfig
)
from .base import (
    BaseSlippageModel,
    BaseLatencyModel,
    BaseLiquidityModel,
    BaseFeeModel
)

# ====================== 1. SLIPPAGE IMPLEMENTATIONS ======================
class VolatilitySlippage(BaseSlippageModel):
    """
    IMPLEMENTASI 1: VOLATILITY SLIPPAGE (THE PRICE CRUSHER)
    """

    @property
    def config(self) -> VolatilitySlippageConfig:
        return super().config

    def calculate_execution_price(self, order: Order, market_price: float, volatility: float, volume: float) -> float:
        if order.quantity <= 0 or market_price <= 0: return market_price

        slippage_bps = self._calculate_microstructure_impact(order.quantity, volatility, volume)
        slippage_factor = slippage_bps / 10000.0
        
        final_price = market_price
        if order.side == OrderSide.BUY:
            final_price = market_price * (1.0 + slippage_factor)
            if slippage_bps > 50:
                self.logger.info(f"High Slippage BUY: {slippage_bps:.1f} bps | Vol: {volatility:.2%}")
        elif order.side == OrderSide.SELL:
            final_price = market_price * (1.0 - slippage_factor)
            if slippage_bps > 50:
                self.logger.info(f"High Slippage SELL: {slippage_bps:.1f} bps | Vol: {volatility:.2%}")

        return final_price

    def _calculate_microstructure_impact(self, quantity: float, volatility: float, avg_volume: float) -> float:
        cfg = self.config 
        noise_bps = random.uniform(0, cfg.base_bps)

        vol_penalty_bps = 0.0
        if volatility > cfg.volatility_threshold:
            panic_ratio = volatility / cfg.volatility_threshold
            vol_penalty_bps = cfg.base_bps * (panic_ratio ** cfg.volatility_multiplier)

        liquidity_impact_bps = 0.0
        if avg_volume > 0:
            participation_rate = quantity / avg_volume
            sqrt_impact = cfg.square_root_factor * math.sqrt(participation_rate)
            linear_impact = cfg.linear_factor * participation_rate
            raw_impact = (sqrt_impact + linear_impact) * 100.0
            adverse_risk = cfg.adverse_selection_factor * (volatility * 100)
            liquidity_impact_bps = raw_impact + adverse_risk

        total_bps = noise_bps + vol_penalty_bps + liquidity_impact_bps
        return min(total_bps, cfg.max_slippage_bps)

# ====================== 2. LATENCY IMPLEMENTATIONS ======================
class NetworkCongestionLatency(BaseLatencyModel):
    """
    Model Latency dengan simulasi kemacetan jaringan.
    """
    
    def __init__(self, config: Optional[NetworkCongestionLatencyConfig] = None):
        super().__init__(config or NetworkCongestionLatencyConfig())
        self._network_state = {
            "congestion_level": 0.0,
            "packet_loss_count": 0
        }
    
    @property
    def config(self) -> NetworkCongestionLatencyConfig:
        return super().config

    def calculate_delay_ms(self, volatility: float) -> int:
        config = self.config
        
        if config.model_type == "fixed": return int(config.fixed_latency_ms)
        
        base_ms = config.base_latency_ms
        congestion_factor = 1.0
        if volatility > config.congestion_threshold:
            excess_vol = (volatility - config.congestion_threshold) / config.congestion_threshold
            congestion_factor = 1.0 + config.congestion_multiplier * excess_vol
        
        topology_delay = self._calculate_topology_delay()
        packet_loss_delay = self._simulate_packet_loss()
        clock_drift = random.gauss(0, config.clock_drift_std_ms)
        
        total_delay = (base_ms * congestion_factor + topology_delay + packet_loss_delay + clock_drift)
        
        if config.enable_jitter:
            jitter_range = total_delay * config.jitter_percentage
            jitter = random.uniform(-jitter_range, jitter_range)
            total_delay += jitter
        
        total_delay = max(0.0, min(total_delay, config.max_latency_ms))
        return max(5, int(round(total_delay)))
    
    def _calculate_topology_delay(self) -> float:
        # [FIX] Ganti self.get_config() -> self.config
        config = self.config
        if not config.network_nodes or len(config.network_nodes) < 2: return 0.0
        return 50.0 # Simplified topology delay
    
    def _simulate_packet_loss(self) -> float:
        # [FIX] Ganti self.get_config() -> self.config
        config = self.config

        if random.random() < config.base_packet_loss_rate:
            self._network_state["packet_loss_count"] += 1
            self.logger.debug(f"Packet loss simulated! Retrying (+{config.retry_penalty_ms}ms)")
            return float(config.retry_penalty_ms)
        return 0.0

# ====================== 3. LIQUIDITY IMPLEMENTATIONS ======================
class StochasticLiquidity(BaseLiquidityModel):
    """
    IMPLEMENTASI 3: STOCHASTIC LIQUIDITY (THE GHOST)
    """

    def __init__(self, config: Optional[StochasticLiquidityConfig] = None):
        super().__init__(config)
        self._ghost_liquidity_tracker: Dict[str, float] = {} 
        self._last_rejection_reason: Dict[str, str] = {}

    @property
    def config(self) -> StochasticLiquidityConfig:
        return super().config

    def should_fill(self, order: Order, tick_volume: float, bid_ask_spread: float = 0.0) -> bool:
        if not self.config.enabled: return True
        cfg = self.config
        
        # Ghost Mode Check
        if self._is_ghost_liquidity_active(order.symbol):
            self._last_rejection_reason[order.order_id] = "GHOST_MODE_ACTIVE"
            return False

        # Random Ghost Trigger
        if random.random() < cfg.ghost_probability:
            self._activate_ghost_liquidity(order.symbol)
            self._last_rejection_reason[order.order_id] = "GHOST_MODE_TRIGGERED"
            return False

        # Probabilitas Fill (Simplified)
        final_prob = cfg.fill_probability
        
        # Volume Penalty
        if tick_volume > 0:
            participation = order.quantity / tick_volume
            if participation > cfg.volume_participation_threshold:
                final_prob *= (1.0 - (participation * cfg.volume_penalty_weight))
        
        # Spread Penalty
        if bid_ask_spread > cfg.max_spread_threshold:
            final_prob *= 0.5
            
        return random.random() < final_prob

    def _is_ghost_liquidity_active(self, symbol: Symbol) -> bool:
        # [FIX] Ganti self.get_config() -> self.config
        if symbol not in self._ghost_liquidity_tracker: return False
        if time.time() * 1000 > self._ghost_liquidity_tracker[symbol]:
            del self._ghost_liquidity_tracker[symbol]
            return False
        return True
    
    def _activate_ghost_liquidity(self, symbol: Symbol) -> None:
        # [FIX] Ganti self.get_config() -> self.config
        expiry = (time.time() * 1000) + self.config.ghost_duration_ms
        self._ghost_liquidity_tracker[symbol] = expiry
        self.logger.debug(f"Ghost liquidity activated for {symbol}")

# ====================== 4. FEE IMPLEMENTATIONS ======================
class StandardFee(BaseFeeModel):
    
    def __init__(self, config: Optional[StandardFeeConfig] = None):
        super().__init__(config)
        self._negotiated_rates: Dict[str, Dict[str, float]] = {}

    def calculate_fee(self, quantity: float, price: float, is_maker: bool, symbol: str) -> float:
        # [FIX] Logic fee sederhana
        rate = self.config.maker_fee_rate if is_maker else self.config.taker_fee_rate
        return quantity * price * (rate / 10000.0)

    # Helper get_config boleh tetap ada di sini jika dibutuhkan caller luar
    def get_config(self) -> StandardFeeConfig:
        return self.config
