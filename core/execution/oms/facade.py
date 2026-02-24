"""
THE TRADING FACADE
Location: core/execution/oms/facade.py
Desc: Simplified, safe, and complete interface for trading strategies.
      All Result types are properly propagated.
"""

from core.shared.result import Result, Ok, Err
from core.execution.types import OrderRequest, OrderSide, OrderType, TimeInForce
from .system import OrderManagementSystem, create_oms, OMSMode


class OMSFacade:
    """
    Facade for the Order Management System.
    Provides a simplified, high-level interface for trading strategies.
    """
    def __init__(self, oms: OrderManagementSystem):
        self._oms = oms

    @classmethod
    def create(cls, broker, market_data=None, risk_check=None, mode=OMSMode.RESEARCH, **kwargs) -> Result['OMSFacade', str]:
        """Factory method to create OMS and wrap with facade."""
        res = create_oms(broker, market_data, risk_check, mode, **kwargs)
        if res.is_err():
            return Err(res.unwrap_err())
        return Ok(cls(res.unwrap()))

    # ----------------------------------------------------------------------
    # LIFECYCLE
    # ----------------------------------------------------------------------
    async def start(self):
        await self._oms.start()

    async def stop(self):
        await self._oms.stop()

    async def refresh_orders(self):
        """Poll exchange for order status updates."""
        await self._oms.poll_orders()

    async def reconcile(self):
        """Reconcile local state with broker positions."""
        await self._oms.reconcile()

    # ----------------------------------------------------------------------
    # ORDER SUBMISSION
    # ----------------------------------------------------------------------
    async def buy_market(self, symbol: str, qty: float) -> Result:
        """Submit a market buy order."""
        req_res = OrderRequest.create(symbol, OrderSide.BUY, OrderType.MARKET, qty)
        if req_res.is_err():
            return Err(req_res.unwrap_err())
        return await self._oms.submit_order(req_res.unwrap())

    async def sell_market(self, symbol: str, qty: float) -> Result:
        """Submit a market sell order."""
        req_res = OrderRequest.create(symbol, OrderSide.SELL, OrderType.MARKET, qty)
        if req_res.is_err():
            return Err(req_res.unwrap_err())
        return await self._oms.submit_order(req_res.unwrap())

    async def buy_limit(self, symbol: str, qty: float, price: float, tif=TimeInForce.GTC) -> Result:
        """Submit a limit buy order."""
        req_res = OrderRequest.create(symbol, OrderSide.BUY, OrderType.LIMIT, qty, price, tif)
        if req_res.is_err():
            return Err(req_res.unwrap_err())
        return await self._oms.submit_order(req_res.unwrap())

    async def sell_limit(self, symbol: str, qty: float, price: float, tif=TimeInForce.GTC) -> Result:
        """Submit a limit sell order."""
        req_res = OrderRequest.create(symbol, OrderSide.SELL, OrderType.LIMIT, qty, price, tif)
        if req_res.is_err():
            return Err(req_res.unwrap_err())
        return await self._oms.submit_order(req_res.unwrap())

    async def cancel_order(self, order_id: str) -> Result:
        """Cancel an open order."""
        return await self._oms.cancel_order(order_id)

    # ----------------------------------------------------------------------
    # RISK MANAGEMENT
    # ----------------------------------------------------------------------
    def engage_kill_switch(self, reason: str = "Manual override"):
        """Immediately stop all trading."""
        self._oms.sentry.engage_kill_switch(reason)

    def disengage_kill_switch(self):
        """Re-enable trading after kill switch."""
        self._oms.sentry.disengage_kill_switch()

    def is_kill_switch_engaged(self) -> bool:
        """Check if kill switch is active."""
        return self._oms.sentry.is_kill_switch_engaged()

    def set_risk_limits(self, **kwargs):
        """
        Adjust risk limits.
        Supported kwargs: max_drawdown_pct, max_position_ratio, max_order_size_usdt.
        """
        self._oms.sentry.set_risk_limits(**kwargs)

    # ----------------------------------------------------------------------
    # PORTFOLIO & POSITIONS
    # ----------------------------------------------------------------------
    def get_portfolio_snapshot(self):
        """Get current portfolio snapshot (positions, PnL, fees)."""
        return self._oms.get_portfolio_snapshot()

    def get_position(self, symbol: str):
        """Get current position for a symbol."""
        return self._oms.inventory.get_position(symbol)

    def get_all_positions(self):
        """Get all positions."""
        return self._oms.inventory.get_all_positions()

    def get_cash_balance(self, currency: str = None):
        """Get cash balance for a currency (default base currency)."""
        return self._oms.inventory.get_cash_balance(currency)

    def get_equity(self, symbols=None) -> float:
        """
        Calculate total equity (cash + market value of positions).
        Uses current market prices. If symbols is provided, only those positions are valued.
        """
        return self._oms.get_equity(symbols)

    # ----------------------------------------------------------------------
    # PERFORMANCE
    # ----------------------------------------------------------------------
    def get_realized_pnl(self) -> float:
        """Get total realized PnL."""
        return self._oms.accountant.get_total_realized_pnl()

    def get_unrealized_pnl(self) -> float:
        """Get total unrealized PnL based on current market prices."""
        positions = self._oms.inventory.get_all_positions()
        market_prices = self._oms._get_market_prices({p.symbol for p in positions})
        return self._oms.accountant.compute_unrealized_pnl(positions, market_prices)

    def get_total_fees(self) -> dict:
        """Get total fees paid per currency."""
        return self._oms.accountant.get_total_fees()

    def get_performance_summary(self) -> dict:
        """
        Get trading performance summary.
        Includes: total_trades, win_rate, avg_win, avg_loss, profit_factor, etc.
        """
        return self._oms.accountant.get_performance_summary()

    def get_summary_stats(self) -> dict:
        """Legacy: simplified summary (PnL and fees only)."""
        s = self.get_portfolio_snapshot()
        return {
            "realized_pnl": s.total_realized_pnl,
            "fees_paid": s.total_fees
        }

    # ----------------------------------------------------------------------
    # STATE VALIDATION
    # ----------------------------------------------------------------------
    def validate_state(self) -> Result:
        """Validate internal consistency of inventory and accountant."""
        inv_res = self._oms.inventory.validate_state()
        if inv_res.is_err():
            return inv_res
        acc_res = self._oms.accountant.validate_state()
        if acc_res.is_err():
            return acc_res
        return Ok(True)

__all__ = ['OMSFacade']
