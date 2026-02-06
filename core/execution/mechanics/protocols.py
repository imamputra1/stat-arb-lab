"""
MECHANICS PROTOCOLS (THE PHYSICS LAWS)
Location: core/execution/mechanics/protocols.py
Desc: Kontrak interface murni untuk simulasi friksi pasar.
      Sinkron dengan implementasi di core/execution/mechanics/models.py.
"""

from typing import Protocol, runtime_checkable
from core.execution.types import Order

# ====================== 1. SLIPPAGE PHYSICS ======================

@runtime_checkable
class SlippageModel(Protocol):
    """
    Protocol untuk menentukan harga eksekusi final.
    """
    def calculate_execution_price(
        self, 
        order: Order, 
        market_price: float, 
        volatility: float,      
        volume: float  # [FIX] Disamakan dengan models.py (sebelumnya avg_volume)
    ) -> float:
        """
        Menghitung harga final setelah memperhitungkan slippage.
        """
        ...

# ====================== 2. LATENCY PHYSICS ======================

@runtime_checkable
class LatencyModel(Protocol):
    """
    Protocol untuk menentukan delay eksekusi (Network Lag).
    """
    def calculate_delay_ms(
        self, 
        volatility: float
    ) -> int:
        """
        Menghitung delay (ms) berdasarkan volatilitas.
        """
        ...

# ====================== 3. LIQUIDITY PHYSICS (GHOST LIQUIDITY) ======================

@runtime_checkable
class LiquidityModel(Protocol):
    """
    Protocol untuk menentukan probabilitas fill (Ghost Liquidity).
    """
    def should_fill(
        self, 
        order: Order, 
        tick_volume: float,      
        bid_ask_spread: float = 0.0 
    ) -> bool:
        """
        Return True jika order berhasil mendapatkan likuiditas.
        """
        ...

# ====================== 4. FEE PHYSICS ======================

@runtime_checkable
class FeeModel(Protocol):
    """
    Protocol untuk menghitung biaya transaksi.
    """
    def calculate_fee(
        self, 
        quantity: float, 
        price: float, 
        is_maker: bool,      
        symbol: str
    ) -> float:
        """
        Menghitung nominal fee.
        """
        ...

# ====================== EXPORTS ======================

__all__ = [
    'SlippageModel', 
    'LatencyModel', 
    'LiquidityModel', 
    'FeeModel'
]
