"""
MECHANICS CONFIGURATION
Location: core/execution/mechanics/config.py
Desc: Dataclasses immutable untuk mengatur parameter simulasi.
      Lengkap dengan validasi dan support untuk models 'sadis'.
"""

from dataclasses import dataclass
# [FIX] Import is_err agar tidak NameError
from core.shared.result import Result, Ok, Err, is_err

# ====================== 1. SLIPPAGE CONFIGS ======================

@dataclass(frozen=True)
class SlippageConfig:
    """Base Config untuk Slippage."""
    model_type: str = "volatility"  
    base_bps: float = 5.0           
    impact_factor: float = 0.1      
    fixed_bps: float = 10.0         
    
    def validate(self) -> Result[bool, str]:
        if self.base_bps < 0: return Err("base_bps cannot be negative")
        if self.impact_factor < 0: return Err("impact_factor cannot be negative")
        return Ok(True)

# FILE: core/execution/mechanics/config.py

@dataclass(frozen=True)
class VolatilitySlippageConfig(SlippageConfig):
    """
    [ADVANCED] Konfigurasi VolatilitySlippage dengan parameter Mikrostruktur.
    Dipindahkan dari models.py agar sesuai arsitektur.
    """
    # Override default model_type
    model_type: str = "adaptive_volatility"

    # 1. Volatility-based parameters
    volatility_threshold: float = 0.02   # 2% volatility threshold
    volatility_multiplier: float = 2.0   # 2x multiplier saat high volatility
    max_slippage_bps: float = 500.0      # Safety cap (5%)

    # 2. Market impact parameters (Square Root Law + Linear)
    square_root_factor: float = 0.1      # Impact dari akar kuadrat volume
    linear_factor: float = 0.05          # Impact linear (order sangat besar)
    adverse_selection_factor: float = 0.3 # Kemungkinan market bergerak melawan kita

    # 3. Adaptive learning (Untuk masa depan)
    enable_adaptive_learning: bool = True
    learning_window: int = 100
    decay_factor: float = 0.99

    # 4. Microstructure effects
    spread_impact_weight: float = 0.4
    depth_impact_weight: float = 0.3
    volume_imbalance_weight: float = 0.3
    
    def validate(self) -> Result[bool, str]:
        # Validasi Parent
        parent_res = super().validate()
        if is_err(parent_res): return parent_res
        
        # Validasi Tambahan
        if self.volatility_multiplier < 1.0:
            return Err("volatility_multiplier must be >= 1.0")
        if self.square_root_factor < 0 or self.linear_factor < 0:
            return Err("Impact factors cannot be negative")
        if not (0 <= self.decay_factor <= 1):
            return Err("decay_factor must be between 0 and 1")
            
        return Ok(True)

# ====================== 2. LATENCY CONFIGS ======================

@dataclass(frozen=True)
class LatencyConfig:
    """Base Config untuk Latency."""
    model_type: str = "network"     
    base_latency_ms: int = 50       
    jitter_factor: float = 2.0      
    fixed_latency_ms: int = 100     
    
    def validate(self) -> Result[bool, str]:
        if self.base_latency_ms < 0: return Err("base_latency_ms cannot be negative")
        return Ok(True)

@dataclass(frozen=True)
class NetworkCongestionLatencyConfig(LatencyConfig):
    """
    [ADVANCED] Konfigurasi Network Congestion.
    Mensimulasikan fisik jaringan, kemacetan, dan packet loss.
    """
    model_type: str = "network_congestion"

    # 1. Physical Topology Parameters
    geographic_distance_km: float = 5000.0  # Jarak server (misal Jakarta -> Tokyo)
    network_hops: int = 15                  # Jumlah router hops
    fiber_speed_km_ms: float = 200.0        # Kecepatan cahaya di fiber (~2/3 c)

    # 2. Congestion Parameters
    congestion_threshold: float = 0.02      # Volatility > 2% memicu kemacetan
    congestion_multiplier: float = 10.0     # Delay multiplier saat macet
    
    # 3. Jitter & Stability
    base_jitter_ms: float = 5.0             # Standar deviasi ping normal
    
    # 4. Packet Loss Simulation (The Killer)
    base_packet_loss_rate: float = 0.01     # 1% packet loss normal
    retry_penalty_ms: int = 300             # Biaya TCP Retransmission (mahal!)

    def validate(self) -> Result[bool, str]:
        parent = super().validate()
        if is_err(parent): return parent
        
        if self.geographic_distance_km < 0:
            return Err("geographic_distance_km cannot be negative")
        if not (0 <= self.base_packet_loss_rate <= 1.0):
            return Err("base_packet_loss_rate must be between 0.0 and 1.0")
            
        return Ok(True)
    
    # [HELPER] Method yang dipanggil models.py (TIDAK PERLU DIUBAH DARI SEBELUMNYA)
    def get_time_of_day_multiplier(self, hour: int) -> float:
        if 13 <= hour <= 16: return 1.5   # NY/London Overlap
        elif 0 <= hour <= 2: return 1.2   # Asian Open
        elif 20 <= hour <= 23: return 0.8 # Quiet
        return 1.0

# ====================== 3. FEE CONFIGS ======================

@dataclass(frozen=True)
class FeeConfig:
    """
    Base Configuration untuk Fee (Biaya Transaksi).
    Menentukan struktur dasar biaya exchange.
    """
    model_type: str = "standard"    # Options: "standard", "vip", "zero"
    
    # Base Rates (Default Binance Regular User)
    maker_fee_rate: float = 0.001   # 0.1% untuk Maker
    taker_fee_rate: float = 0.001   # 0.1% untuk Taker
    
    # Asset settings
    fee_asset: str = "USDT"         # Asset yang digunakan untuk bayar fee (Quote Asset)
    
    def validate(self) -> Result[bool, str]:
        if self.maker_fee_rate < 0 or self.taker_fee_rate < 0:
            return Err("Fee rates cannot be negative")
        return Ok(True)

@dataclass(frozen=True)
class StandardFeeConfig(FeeConfig):
    """
    [ADVANCED] Konfigurasi Fee Standar dengan fitur Tier & Diskon.
    Mensimulasikan struktur biaya exchange nyata (e.g., Binance, Bybit).
    """
    # 1. VIP / Volume Based Tiers
    # Jika True, fee rate bisa berubah berdasarkan volume bulanan (simulasi)
    enable_tiered_fees: bool = False
    
    # 2. Token Deduction Discount (e.g., Pay with BNB)
    # Diskon 25% jika bayar pakai native token exchange
    enable_token_discount: bool = False
    token_discount_rate: float = 0.25 
    
    # 3. Min/Max Fee Caps (Safety)
    min_fee_nominal: float = 0.0    # Minimal fee (e.g. 0.01 USDT)
    
    def validate(self) -> Result[bool, str]:
        # Validasi Parent
        parent_res = super().validate()
        if is_err(parent_res): return parent_res
        
        # Validasi Logic Diskon
        if self.enable_token_discount:
            if not (0.0 <= self.token_discount_rate <= 1.0):
                return Err("token_discount_rate must be between 0.0 and 1.0")
                
        return Ok(True)

# ====================== 4. LIQUIDITY CONFIGS ======================

@dataclass(frozen=True)
class LiquidityConfig:
    """
    Base Configuration untuk Liquidity.
    Mengatur probabilitas dasar eksekusi order limit.
    """
    enabled: bool = True            # Master switch: Aktifkan simulasi likuiditas?
    fill_probability: float = 1.0   # Default 1.0 (100% fill) untuk base model
    
    def validate(self) -> Result[bool, str]:
        if not (0.0 <= self.fill_probability <= 1.0):
            return Err("fill_probability must be between 0.0 and 1.0")
        return Ok(True)

@dataclass(frozen=True)
class StochasticLiquidityConfig(LiquidityConfig):
    """
    [ADVANCED] Konfigurasi Stochastic/Ghost Liquidity.
    Mensimulasikan:
    1. Ghost Liquidity (Order di layar tapi tidak bisa diambil).
    2. Queue Priority (Kita selalu ditaruh di antrean belakang).
    3. Volume Crunch (Order besar susah fill).
    """
    model_type: str = "stochastic"
    
    # 1. Ghost Liquidity Params
    ghost_duration_ms: int = 5000       # Durasi 'fase hantu' (ms)
    ghost_probability: float = 0.05     # 5% peluang masuk mode hantu tiba-tiba
    
    # 2. Volume Impact Params
    # Jika order size > 5% dari tick volume, fill probability turun drastis
    volume_participation_threshold: float = 0.05 
    volume_penalty_weight: float = 2.0  # Seberapa sadis penaltinya?
    
    # 3. Microstructure / Queue Params
    # HFT Bias: Simulasi bot lain lebih cepat dari kita.
    # 0.0 = Fair Queue, 1.0 = Kita selalu paling belakang
    queue_position_bias: float = 0.8    
    
    # 4. Spread Sensitivity
    # Jika spread melebar (market illiquid), probabilitas fill turun
    max_spread_threshold: float = 0.005 # 50 bps spread dianggap 'kering'

    def validate(self) -> Result[bool, str]:
        # Validasi Parent
        parent_res = super().validate()
        if is_err(parent_res): return parent_res
        
        # Validasi Logic Sadis
        if self.ghost_duration_ms < 0:
            return Err("ghost_duration_ms cannot be negative")
            
        if not (0.0 <= self.ghost_probability <= 1.0):
            return Err("ghost_probability must be between 0.0 and 1.0")
            
        if not (0.0 <= self.queue_position_bias <= 1.0):
            return Err("queue_position_bias must be between 0.0 and 1.0")
            
        if self.volume_participation_threshold <= 0:
            return Err("volume_participation_threshold must be positive")
            
        return Ok(True)

# ====================== EXPORTS ======================

__all__ = [
    # Base Configs
    'SlippageConfig',
    'LatencyConfig',
    'FeeConfig',
    'LiquidityConfig',
    
    # Concrete Configs (Matches models.py)
    'VolatilitySlippageConfig',
    'NetworkCongestionLatencyConfig',
    'StandardFeeConfig',
    'StochasticLiquidityConfig'
]
