"""
THE TRADING FACADE
Location: core/execution/oms/facade.py
"""
from core.shared.result import Ok, Err
from core.execution.types import OrderRequest, OrderSide, OrderType, TimeInForce

# Kita import System dan Factory dari sebelah
from .system import OrderManagementSystem, create_oms, OMSMode

class OMSFacade:  # <--- PASTIKAN NAMA CLASS INI BENAR
    def __init__(self, oms: OrderManagementSystem):
        self._oms = oms

    @classmethod
    def create(cls, broker, market_data=None, risk_check=None, mode=OMSMode.RESEARCH, **kwargs):
        # Panggil factory function 'create_oms' dari system.py
        res = create_oms(broker, market_data, risk_check, mode, **kwargs)
        if res.is_err(): return Err(res.unwrap_err())
        return Ok(cls(res.unwrap()))

    # --- Proxy Methods ---
    async def start(self): await self._oms.start()
    async def stop(self): await self._oms.stop()
    async def refresh_orders(self): await self._oms.poll_orders()
    
    async def buy_market(self, symbol, qty):
        return await self._oms.submit_order(OrderRequest.create(symbol, OrderSide.BUY, OrderType.MARKET, qty).unwrap())

    async def sell_limit(self, symbol, qty, price, tif=TimeInForce.GTC):
        return await self._oms.submit_order(OrderRequest.create(symbol, OrderSide.SELL, OrderType.LIMIT, qty, price, tif).unwrap())

    async def cancel(self, oid):
        return await self._oms.cancel_order(oid)

    def get_portfolio(self): return self._oms.get_portfolio_snapshot()
    def get_position(self, sym): return self._oms.inventory.get_position(sym)
    def get_pnl(self): return self._oms.accountant.get_total_realized_pnl()
    
    def get_summary_stats(self):
        s = self.get_portfolio()
        return {"realized_pnl": s.total_realized_pnl, "fees_paid": s.total_fees}
