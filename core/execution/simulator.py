"""
THE EVIL SIMULATOR (THE ACTOR)
Location: core/execution/simulator.py
Desc: Broker simulasi yang mengimplementasikan BrokerProtocol.
      FIXED: Menangani Result wrapper pada transisi status Order (termasuk add_fill).
"""

import asyncio
import random
import uuid
from dataclasses import dataclass
from typing import Dict, List, Any

# Core Imports
from core.shared.result import Result, Ok, Err
from core.shared.utils import get_logger

# Protocols & Types
from core.execution.types import (
    OrderRequest, 
    ExecutionReport, 
    Order, 
    OrderType, 
    OrderSide, 
    TradeFill,
    Position,
    PositionFactory
)

logger = get_logger("execution.simulator")

@dataclass
class SimulatorConfig:
    """Konfigurasi Medan Perang Simulasi"""
    initial_cash: float = 100_000.0
    base_currency: str = "USDT"
    latency_mean_ms: float = 50.0
    latency_std_ms: float = 10.0
    slippage_std_bps: float = 2.0
    fee_rate_maker: float = 0.0002
    fee_rate_taker: float = 0.0004
    reject_probability: float = 0.001
    enable_market_impact: bool = False
    
@dataclass
class _OrderBookEntry:
    """Internal Simulator Order Book"""
    order: Order
    filled_qty: float
    remaining_qty: float

class ExecutionSimulator:
    """
    Broker Palsu yang Patuh Protokol (BrokerProtocol).
    Digunakan untuk Backtesting dan Paper Trading.
    """
    
    def __init__(self, config: SimulatorConfig = SimulatorConfig()):
        self.config = config
        
        self._cash_balances: Dict[str, float] = {
            config.base_currency: config.initial_cash
        }
        self._positions: Dict[str, Position] = {}
        self._active_orders: Dict[str, _OrderBookEntry] = {}
        self._order_history: List[ExecutionReport] = []
        self._last_prices: Dict[str, float] = {}
        
        logger.info(f"😈 Evil Simulator Initialized | Latency: ~{config.latency_mean_ms}ms")

    # =========================================================
    # IMPLEMENTASI BROKER PROTOCOL
    # =========================================================

    async def submit_order(self, request: OrderRequest) -> Result[ExecutionReport, str]:
        """[PROTOCOL] Menerima order dari OMS."""
        await self._simulate_latency()
        
        if random.random() < self.config.reject_probability:
            return self._create_rejection(request, "EXCHANGE_SYSTEM_ERROR")

        current_price = self._last_prices.get(request.symbol, request.price)
        if not current_price and request.order_type == OrderType.MARKET:
            return Err(f"Simulator has no price for {request.symbol}")

        # 1. Buat Order Object
        order = Order.from_request(request, exchange_order_id=f"SIM-{uuid.uuid4().hex[:8]}")
        
        # 2. Transisi Status: NEW -> ACKNOWLEDGED
        ack_res = order.mark_acknowledged(order.exchange_order_id)
        if ack_res.is_err():
            return Err(f"Failed to ack order: {ack_res.unwrap_err()}")
        order = ack_res.unwrap()
        
        # 3. Eksekusi
        if request.order_type == OrderType.MARKET:
            return self._execute_market_order(order, current_price)
        elif request.order_type == OrderType.LIMIT:
            return self._place_limit_order(order)
        else:
            return self._create_rejection(request, "UNSUPPORTED_ORDER_TYPE")

    async def cancel_order(self, order_id: str) -> Result[ExecutionReport, str]:
        """[PROTOCOL] Membatalkan order aktif."""
        await self._simulate_latency()
        
        if order_id not in self._active_orders:
            return Err("Order not found or already filled")
            
        entry = self._active_orders.pop(order_id)
        
        cancel_res = entry.order.mark_canceled()
        if cancel_res.is_err():
            return Err(f"Failed to cancel: {cancel_res.unwrap_err()}")
            
        order = cancel_res.unwrap()
        
        report = ExecutionReport(
            order=order,
            fills=(),
            is_complete=True,
            total_notional=order.filled_quantity * order.average_fill_price
        )
        return Ok(report)

    async def get_order(self, order_id: str) -> Result[ExecutionReport, str]:
        if order_id in self._active_orders:
            entry = self._active_orders[order_id]
            return Ok(ExecutionReport.from_order_and_fills(entry.order, []))
        
        for report in reversed(self._order_history):
            if report.order.order_id == order_id:
                return Ok(report)
                
        return Err("Order ID not found in Simulator")

    async def get_open_orders(self) -> Result[List[ExecutionReport], str]:
        reports = []
        for entry in self._active_orders.values():
            reports.append(ExecutionReport.from_order_and_fills(entry.order, []))
        return Ok(reports)

    async def get_all_positions(self) -> Result[List[Position], str]:
        return Ok(list(self._positions.values()))

    async def get_balance(self) -> Result[Dict[str, float], str]:
        return Ok(self._cash_balances.copy())

    # =========================================================
    # INTERNAL MATCHING ENGINE
    # =========================================================

    def process_tick(self, tick: Any):
        symbol = tick.symbol
        price = tick.price
        self._last_prices[symbol] = price
        
        active_ids = list(self._active_orders.keys())
        
        for order_id in active_ids:
            if order_id not in self._active_orders: continue
            
            entry = self._active_orders[order_id]
            order = entry.order
            
            if order.symbol != symbol: continue
            
            is_match = False
            if order.side == OrderSide.BUY and price <= order.price:
                is_match = True
            elif order.side == OrderSide.SELL and price >= order.price:
                is_match = True
                
            if is_match:
                self._fill_order(entry, price, is_taker=False)

    def _execute_market_order(self, order: Order, current_price: float) -> Result[ExecutionReport, str]:
        """Eksekusi instan Market Order dengan Slippage."""
        slippage_pct = random.gauss(0, self.config.slippage_std_bps / 10000.0)
        
        if order.side == OrderSide.BUY:
            exec_price = current_price * (1 + abs(slippage_pct))
        else:
            exec_price = current_price * (1 - abs(slippage_pct))
            
        entry = _OrderBookEntry(order, 0.0, order.quantity)
        return self._fill_order(entry, exec_price, is_taker=True)

    def _place_limit_order(self, order: Order) -> Result[ExecutionReport, str]:
        """Taruh Limit Order di buku (Queue)."""
        entry = _OrderBookEntry(order, 0.0, order.quantity)
        self._active_orders[order.order_id] = entry
        
        open_res = order.mark_open()
        if open_res.is_err():
            return Err(f"Failed to open order: {open_res.unwrap_err()}")
        
        report = ExecutionReport(
            order=open_res.unwrap(),
            fills=(),
            is_complete=False
        )
        return Ok(report)

    def _fill_order(self, entry: _OrderBookEntry, price: float, is_taker: bool) -> Result[ExecutionReport, str]:
        order = entry.order
        qty = entry.remaining_qty
        
        fee_rate = self.config.fee_rate_taker if is_taker else self.config.fee_rate_maker
        notional = qty * price
        fee_amt = notional * fee_rate
        
        self._update_exchange_balances(order.symbol, order.side, qty, price, fee_amt)
        
        # Create Fill
        fill_res = TradeFill.create(
            order_id=order.order_id,
            symbol=order.symbol,
            side=order.side,
            quantity=qty,
            price=price,
            fee=fee_amt,
            fee_currency=self.config.base_currency,
            latency_ms=random.gauss(self.config.latency_mean_ms, self.config.latency_std_ms),
            slippage_bps=abs((price - (order.price or price))/price * 10000)
        )
        
        if fill_res.is_err():
            return Err(f"Fill creation failed: {fill_res.unwrap_err()}")
        fill = fill_res.unwrap()
        
        # [FIX] Update Order State (Add Fill)
        # Unwrap result from add_fill since it returns Result['Order', ValidationError]
        updated_order_res = order.add_fill(qty, price)
        if updated_order_res.is_err():
             return Err(f"Failed to add fill: {updated_order_res.unwrap_err()}")
        updated_order = updated_order_res.unwrap()
        
        # Update entry
        if updated_order.is_terminal:
            self._active_orders.pop(order.order_id, None)
        else:
            entry.order = updated_order
            entry.filled_qty += qty
            entry.remaining_qty -= qty
            
        report = ExecutionReport.from_order_and_fills(updated_order, [fill])
        self._order_history.append(report)
        
        return Ok(report)

    def _update_exchange_balances(self, symbol: str, side: OrderSide, qty: float, price: float, fee: float):
        if symbol not in self._positions:
            self._positions[symbol] = PositionFactory.create_empty(symbol, self.config.base_currency)
            
        pos = self._positions[symbol]
        qty_signed = qty if side == OrderSide.BUY else -qty
        
        cost = qty * price
        if side == OrderSide.BUY:
            self._cash_balances[self.config.base_currency] -= cost
        else:
            self._cash_balances[self.config.base_currency] += cost
            
        self._cash_balances[self.config.base_currency] -= fee
        
        new_qty = pos.quantity + qty_signed
        self._positions[symbol] = pos.copy(quantity=new_qty)

    def _create_rejection(self, request: OrderRequest, reason: str) -> Result[ExecutionReport, str]:
        order = Order.from_request(request)
        rej_res = order.mark_rejected("REJECTED", reason)
        if rej_res.is_err():
            return Err(f"Failed to reject: {rej_res.unwrap_err()}")
            
        return Ok(ExecutionReport(
            order=rej_res.unwrap(),
            fills=(),
            is_complete=True,
            completion_reason=reason
        ))

    async def _simulate_latency(self):
        delay = random.gauss(self.config.latency_mean_ms, self.config.latency_std_ms)
        delay = max(1.0, delay)
        await asyncio.sleep(delay / 1000.0)
