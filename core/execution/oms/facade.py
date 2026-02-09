"""
THE TRADING FACADE (THE CONTROLLER)
Location: core/execution/oms/facade.py
Desc: Layer kenyamanan (Convenience Layer) untuk Strategy Developer.
      Menyederhanakan pembuatan OrderRequest dan akses data Portfolio.
"""

from typing import Optional, Dict, Any

# Core Imports
from core.shared.result import Result, Ok, Err
from core.shared.utils import get_logger

# Internal System
from .system import OrderManagementSystem, create_oms, OMSMode
from .components import PortfolioSnapshot

# Protocols
from core.execution.protocols import (
    BrokerProtocol, 
    MarketDataProtocol, 
    RiskCheckProtocol
)

# Types (Kita bungkus agar user tidak perlu import manual)
from core.execution.types import (
    OrderRequest, 
    ExecutionReport, 
    OrderType, 
    OrderSide, 
    TimeInForce,
    Position
)

logger = get_logger("oms.facade")

class OMSFacade:
    """
    Pemandu Wisata untuk OMS.
    Menyediakan High-Level API untuk Trading.
    User tidak perlu menyentuh 'OrderRequest' manual di sini.
    """

    def __init__(self, oms: OrderManagementSystem):
        self._oms = oms

    @classmethod
    def create(
        cls, 
        broker: BrokerProtocol,
        market_data: Optional[MarketDataProtocol] = None,
        risk_check: Optional[RiskCheckProtocol] = None,
        mode: OMSMode = OMSMode.RESEARCH,
        **config_kwargs
    ) -> Result['OMSFacade', str]:
        """
        One-Stop Shop untuk inisialisasi.
        Membuat System, Config, dan Facade sekaligus.
        """
        # Panggil Factory System yang sudah kita buat sebelumnya
        oms_res = create_oms(broker, market_data, risk_check, mode, **config_kwargs)
        
        if oms_res.is_err():
            return Err(oms_res.unwrap_err())
            
        return Ok(cls(oms_res.unwrap()))

    # ================== LIFECYCLE ==================

    async def start(self):
        """Menyalakan mesin"""
        await self._oms.start()

    async def stop(self):
        """Mematikan mesin"""
        await self._oms.stop()

    # ================== TRADING SHORTCUTS ==================

    async def buy_market(self, symbol: str, quantity: float) -> Result[ExecutionReport, str]:
        """Shortcut: Beli di harga pasar sekarang juga"""
        request = OrderRequest.create(
            symbol=symbol,
            side=OrderSide.BUY,
            order_type=OrderType.MARKET,
            quantity=quantity
        )
        if request.is_err(): return Err(str(request.unwrap_err()))
        
        return await self._oms.submit_order(request.unwrap())

    async def sell_market(self, symbol: str, quantity: float) -> Result[ExecutionReport, str]:
        """Shortcut: Jual di harga pasar sekarang juga"""
        request = OrderRequest.create(
            symbol=symbol,
            side=OrderSide.SELL,
            order_type=OrderType.MARKET,
            quantity=quantity
        )
        if request.is_err(): return Err(str(request.unwrap_err()))
        
        return await self._oms.submit_order(request.unwrap())

    async def buy_limit(
        self, 
        symbol: str, 
        quantity: float, 
        price: float,
        tif: TimeInForce = TimeInForce.GTC
    ) -> Result[ExecutionReport, str]:
        """Shortcut: Pasang jaring beli (Limit Order)"""
        request = OrderRequest.create(
            symbol=symbol,
            side=OrderSide.BUY,
            order_type=OrderType.LIMIT,
            quantity=quantity,
            price=price,
            time_in_force=tif
        )
        if request.is_err(): return Err(str(request.unwrap_err()))
        
        return await self._oms.submit_order(request.unwrap())

    async def sell_limit(
        self, 
        symbol: str, 
        quantity: float, 
        price: float,
        tif: TimeInForce = TimeInForce.GTC
    ) -> Result[ExecutionReport, str]:
        """Shortcut: Pasang jaring jual (Limit Order)"""
        request = OrderRequest.create(
            symbol=symbol,
            side=OrderSide.SELL,
            order_type=OrderType.LIMIT,
            quantity=quantity,
            price=price,
            time_in_force=tif
        )
        if request.is_err(): return Err(str(request.unwrap_err()))
        
        return await self._oms.submit_order(request.unwrap())

    async def cancel(self, order_id: str) -> Result[ExecutionReport, str]:
        """Batalkan order"""
        return await self._oms.cancel_order(order_id)

    async def cancel_all(self, symbol: Optional[str] = None):
        """
        Panic Button: Cancel semua order aktif.
        (Nanti diimplementasikan bulk cancel di system, sekarang placeholder)
        """
        # TODO: Implement bulk cancel loop
        pass

    # ================== PORTFOLIO READING ==================

    def get_portfolio(self) -> PortfolioSnapshot:
        """Lihat isi dompet lengkap"""
        return self._oms.get_portfolio_snapshot()

    def get_position(self, symbol: str) -> Position:
        """Lihat posisi aset tertentu"""
        # Helper untuk akses cepat ke inventory dalam
        return self._oms.inventory.get_position(symbol)

    def get_pnl(self) -> float:
        """Lihat total keuntungan terealisasi"""
        return self._oms.accountant.get_total_realized_pnl()
    
    def get_summary_stats(self) -> Dict[str, Any]:
        """Summary cepat untuk Dashboard/UI"""
        snap = self.get_portfolio()
        # Ambil total fees dari accountant
        total_fees = self._oms.accountant.get_total_fees()
        return {
            "realized_pnl": snap.total_realized_pnl,
            "open_positions": len(snap.positions),
            "fees_paid": total_fees
        }
