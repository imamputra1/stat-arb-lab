"""
EXECUTION PROTOCOLS (INTERFACE ONLY)
Location: core/execution/protocols.py
Desc: Kontrak murni. Tidak ada logic, tidak ada config, tidak ada class ABC.
"""

from typing import Protocol, runtime_checkable, List, Optional, Any, Dict
from core.shared.result import Result
from .types import (
    OrderRequest, 
    ExecutionReport, 
    Order,
    OrderResult,
    Position
)

# ====================== CORE EXECUTION PROTOCOL ======================

@runtime_checkable
class ExecutionHandler(Protocol):
    """
    Kontrak Utama Execution Engine.
    Strategy hanya boleh berinteraksi dengan interface ini.
    """
    
    async def submit_order(self, request: OrderRequest) -> OrderResult:
        """Mengirim order baru ke market/simulator"""
        ...
        
    async def cancel_order(self, order_id: str) -> Result[bool, str]:
        """Membatalkan order aktif"""
        ...
        
    async def get_order(self, order_id: str) -> Result[Order, str]:
        """Mengambil status order terbaru"""
        ...
        
    async def get_open_orders(self) -> Result[List[Order], str]:
        """Mengambil semua order aktif"""
        ...

# ====================== RISK MANAGER PROTOCOL ======================

@runtime_checkable
class RiskManagerProtocol(Protocol):
    """
    Kontrak Risk Manager.
    Signature ini COCOK 100% dengan core/risk/manager.py.
    """
    
    def evaluate_trade(
        self, 
        request: Any,  # Any untuk hindari circular import TradeRequest
        account_state: Any, # Any untuk AccountState
        market_data: Optional[Dict[str, Any]] = None
    ) -> Result[Any, str]:
        """
        Main entry point: Mengevaluasi Trade Request.
        Returns: TradeVerdict (dibungkus Result).
        """
        ...
        
    def update_account_state(self, new_state: Any) -> None:
        """
        Update state akun internal Risk Manager.
        """
        ...

# ====================== MARKET DATA PROTOCOL ======================

@runtime_checkable
class MarketDataProvider(Protocol):
    """
    Kontrak untuk penyedia data harga (Live atau Sim).
    """
    
    async def get_price(self, symbol: str) -> float:
        """Get harga terakhir"""
        ...
        
    async def get_orderbook(self, symbol: str, depth: int = 10) -> Dict[str, Any]:
        """Get snapshot orderbook"""
        ...

# ====================== BROKER PROTOCOL ======================

@runtime_checkable
class BrokerProtocol(Protocol):
    """
    Kontrak Agnostic antara OMS dan Eksekutor (Broker/Simulator).
    """
    
    async def submit_order(self, request: OrderRequest) -> Result[ExecutionReport, str]:
        """
        Mengirim order baru.
        Return: ExecutionReport (Status: NEW/REJECTED)
        """
        ...
        
    async def cancel_order(self, order_id: str) -> Result[ExecutionReport, str]:
        """
        Membatalkan order.
        Return: ExecutionReport (Status: CANCELED/REJECTED)
        """
        ...
        
    async def get_order(self, order_id: str) -> Result[ExecutionReport, str]:
        """
        Cek status order spesifik.
        Return: ExecutionReport terbaru.
        """
        ...
        
    async def get_open_orders(self) -> Result[List[ExecutionReport], str]:
        """
        Ambil semua order aktif di sisi Broker.
        """
        ...

    async def get_all_positions(self) -> Result[List[Position], str]:
        """
        [UPDATED] Ambil snapshot posisi dari Broker.
        Broker WAJIB mengkonversi format raw mereka menjadi List[Position] kita.
        Ini memudahkan Rekonsiliasi OMS.
        """
        ...

    async def get_balance(self) -> Result[Dict[str, float], str]:
        """
        Ambil saldo cash (Free & Used).
        Return contoh: {'USDT': 1000.0, 'BTC': 0.5}
        """
        ...

# ====================== SUPPORTING PROTOCOLS ======================

@runtime_checkable
class MarketDataProtocol(Protocol):
    """
    Kontrak untuk melihat Harga Pasar (Mata OMS).
    """
    async def get_ticker(self, symbol: str) -> Result[float, str]: ...
    async def get_snapshot(self, symbol: str) -> Any: ...

@runtime_checkable
class RiskCheckProtocol(Protocol):
    """
    Kontrak untuk Polisi Militer (Risk).
    """
    async def validate_order(self, request: OrderRequest) -> Result[bool, str]: ...
