"""
THE EVIL SIMULATOR (THE ACTOR)
Location: core/execution/simulator.py
Desc: Broker simulasi yang mengimplementasikan BrokerProtocol.
      [SURGERY UPDATE]: Added Regime Switching (Dynamic Latency & Rejection).
"""

import asyncio
import random
import uuid
from dataclasses import dataclass
from typing import Dict, List, Optional
from enum import Enum

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

class MarketRegime(Enum):
    """Kondisi Medan Perang"""
    NORMAL = "NORMAL"           # Low Latency, No Rejection
    VOLATILE = "VOLATILE"       # Med Latency, High Slippage
    CRISIS = "CRISIS"           # High Latency (Lag), Panic Selling
    ILLIQUID = "ILLIQUID"       # High Rejection (Ghost Liquidity)

@dataclass
class SimulatorConfig:
    """Konfigurasi Dasar (Baseline)"""
    initial_cash: float = 100_000.0
    base_currency: str = "USDT"
    # Baseline Metrics (Normal Mode)
    latency_mean_ms: float = 50.0
    latency_std_ms: float = 10.0
    slippage_std_bps: float = 2.0
    fee_rate_maker: float = 0.0002
    fee_rate_taker: float = 0.0004
    reject_probability: float = 0.001
    
@dataclass
class _OrderBookEntry:
    """Internal Simulator Order Book"""
    order: Order
    filled_qty: float
    remaining_qty: float

class ExecutionSimulator:
    """
    Broker Palsu yang bisa berubah menjadi 'Jahat' sesuai Regime.
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
        
        # --- DYNAMIC STATE (Override Config) ---
        self._current_regime = MarketRegime.NORMAL
        self._latency_multiplier = 1.0
        self._reject_prob_override: Optional[float] = None
        self._slippage_multiplier = 1.0
        
        logger.info(f"😈 Evil Simulator Initialized | Mode: {self._current_regime.value}")

    # =========================================================
    # THE TORMENTOR INTERFACE (REGIME SWITCHING)
    # =========================================================
    def set_regime(self, regime: MarketRegime):
        """
        Mengubah cuaca pasar secara instan.
        Dipanggil oleh War Room (Chaos Generator).
        [SURGERY FIX]: Crisis multiplier sekarang dinamis agar tetap LAG
        walaupun base latency config sangat kecil (misal saat unit test).
        """
        self._current_regime = regime
        
        if regime == MarketRegime.NORMAL:
            self._latency_multiplier = 1.0
            self._reject_prob_override = None 
            self._slippage_multiplier = 1.0
            
        elif regime == MarketRegime.VOLATILE:
            self._latency_multiplier = 2.0    
            self._reject_prob_override = 0.01 
            self._slippage_multiplier = 5.0   
            
        elif regime == MarketRegime.CRISIS:
            # [FIX] Force Minimum Crisis Lag (Target: 1000ms)
            # Jika base config 1ms, multiplier jadi 1000x.
            # Jika base config 50ms, multiplier jadi 20x.
            base_latency = max(0.1, self.config.latency_mean_ms)
            target_latency = 1000.0 # 1 detik lag minimal
            
            self._latency_multiplier = max(20.0, target_latency / base_latency)
            
            self._reject_prob_override = 0.05 
            self._slippage_multiplier = 10.0
            
        elif regime == MarketRegime.ILLIQUID:
            self._latency_multiplier = 1.0
            self._reject_prob_override = 0.30 
            self._slippage_multiplier = 20.0  
            
        logger.warning(f"⚠️ MARKET REGIME CHANGED TO: {regime.value}")


    # =========================================================
    # IMPLEMENTASI BROKER PROTOCOL
    # =========================================================
    async def submit_order(self, request: OrderRequest) -> Result[ExecutionReport, str]:
        """[PROTOCOL] Menerima order dari OMS."""
        await self._simulate_latency()
        
        current_reject_prob = self._reject_prob_override if self._reject_prob_override is not None else self.config.reject_probability
        
        if random.random() < current_reject_prob:
            logger.warning(f"🚫 Order REJECTED by Exchange (Simulated {self._current_regime.value})")
            return self._create_rejection(request, "EXCHANGE_SYSTEM_ERROR_GHOST")

        # [SURGERY FIX 1]: Pastikan current_price adalah Float Mutlak
        price_candidate = self._last_prices.get(request.symbol, request.price)
        if price_candidate is None:
            return Err(f"Simulator has no price for {request.symbol}")
        current_price: float = float(price_candidate)

        order = Order.from_request(request, exchange_order_id=f"SIM-{uuid.uuid4().hex[:8]}")
        
        ack_res = order.mark_acknowledged(order.exchange_order_id)
        if ack_res.is_err(): return Err(str(ack_res.unwrap_err()))
        order = ack_res.unwrap()
        
        if request.order_type == OrderType.MARKET:
            return self._execute_market_order(order, current_price)
        elif request.order_type == OrderType.LIMIT:
            return self._place_limit_order(order, current_price)
        else:
            return self._create_rejection(request, "UNSUPPORTED_ORDER_TYPE")

    async def cancel_order(self, order_id: str) -> Result[ExecutionReport, str]:
        await self._simulate_latency()
        
        if order_id not in self._active_orders:
            return Err("Order not found or already filled")
            
        entry = self._active_orders.pop(order_id)
        
        cancel_res = entry.order.mark_canceled()
        if cancel_res.is_err(): return Err(str(cancel_res.unwrap_err()))
        order = cancel_res.unwrap()
        
        # [SURGERY FIX 2]: Fallback ke 0.0 jika order belum pernah tereksekusi
        avg_price = order.average_fill_price if order.average_fill_price is not None else 0.0
        
        return Ok(ExecutionReport(
            order=order, fills=(), is_complete=True, 
            total_notional=order.filled_quantity * avg_price
        ))


    async def get_order(self, order_id: str) -> Result[ExecutionReport, str]:
        if order_id in self._active_orders:
            entry = self._active_orders[order_id]
            return Ok(ExecutionReport.from_order_and_fills(entry.order, []))
        
        for report in reversed(self._order_history):
            if report.order.order_id == order_id:
                return Ok(report)
        return Err("Order ID not found")

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
    # INTERNAL LOGIC
    # =========================================================

    async def _simulate_latency(self):
        """Sleep berdasarkan Regime saat ini"""
        base_mean = self.config.latency_mean_ms * self._latency_multiplier
        base_std = self.config.latency_std_ms * self._latency_multiplier
        
        delay_ms = random.gauss(base_mean, base_std)
        delay_ms = max(1.0, delay_ms) # Min 1ms
        
        # Convert to seconds for asyncio.sleep
        await asyncio.sleep(delay_ms / 1000.0)

    def _execute_market_order(self, order: Order, current_price: float) -> Result[ExecutionReport, str]:
        # Slippage dipengaruhi Regime
        slippage_bps = random.gauss(0, self.config.slippage_std_bps) * self._slippage_multiplier
        slippage_pct = slippage_bps / 10000.0
        
        if order.side == OrderSide.BUY:
            exec_price = current_price * (1 + abs(slippage_pct))
        else:
            exec_price = current_price * (1 - abs(slippage_pct))
            
        entry = _OrderBookEntry(order, 0.0, order.quantity)
        return self._fill_order(entry, exec_price, is_taker=True)

    def _place_limit_order(self, order: Order, current_price: float) -> Result[ExecutionReport, str]:
        """
        [SURGERY FIX]: Pseudo-Matching Engine untuk Limit Order.
        Karena kita menjalankan Fast-Forward Simulation, kita asumsikan order tereksekusi instan 
        untuk melihat PnL dan mendemonstrasikan struktur Taker/Maker Fee.
        """
        entry = _OrderBookEntry(order, 0.0, order.quantity)
        
        # Deteksi apakah order membelah spread (Aggressive Limit)
        is_marketable = False
        if order.side == OrderSide.BUY and order.price >= current_price:
            is_marketable = True
        elif order.side == OrderSide.SELL and order.price <= current_price:
            is_marketable = True
            
        if is_marketable:
            # [TAKER]: Eksekusi di harga market, kena biaya Taker Fee (+0.04%)
            return self._fill_order(entry, current_price, is_taker=True)
        else:
            # [MAKER]: Dalam simulasi MFT ini, kita asumsikan harga akhirnya terjemput (Filled)
            # Eksekusi di harga limit, mendapatkan Maker Rebate (-0.02%)
            return self._fill_order(entry, order.price, is_taker=False)

    def _fill_order(self, entry: _OrderBookEntry, price: float, is_taker: bool) -> Result[ExecutionReport, str]:
        order = entry.order
        qty = entry.remaining_qty
        
        fee_rate = self.config.fee_rate_taker if is_taker else self.config.fee_rate_maker
        fee_amt = (qty * price) * fee_rate
        
        self._update_exchange_balances(order.symbol, order.side, qty, price, fee_amt)
        
        fill_latency = random.gauss(self.config.latency_mean_ms, self.config.latency_std_ms) * self._latency_multiplier
        
        # Guard untuk slippage jika order.price None
        order_price_safe = order.price if order.price is not None else price
        slippage_bps = abs((price - order_price_safe) / price * 10000) if price > 0 else 0.0
        
        fill_res = TradeFill.create(
            order_id=order.order_id, symbol=order.symbol, side=order.side,
            quantity=qty, price=price, fee=fee_amt,
            fee_currency=self.config.base_currency, latency_ms=fill_latency,
            slippage_bps=slippage_bps
        )
        if fill_res.is_err(): return Err(str(fill_res.unwrap_err()))
        fill = fill_res.unwrap()
        
        updated_order_res = order.add_fill(qty, price)
        if updated_order_res.is_err(): return Err(str(updated_order_res.unwrap_err()))
        updated_order = updated_order_res.unwrap()
        
        # [SURGERY FIX 3]: Logika pasti! Jika barang habis (<= 0), berarti Terminal.
        entry.filled_qty += qty
        entry.remaining_qty -= qty
        
        if entry.remaining_qty <= 0.0:
            self._active_orders.pop(order.order_id, None)
        else:
            entry.order = updated_order
            
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
        if rej_res.is_err(): return Err(str(rej_res.unwrap_err()))
        
        return Ok(ExecutionReport(
            order=rej_res.unwrap(), fills=(), is_complete=True, completion_reason=reason
        ))
    # Inside class ExecutionSimulator, add this method:

    def _match_orders(self, symbol: str, current_price: float):
        """Mencocokkan harga pasar dengan Limit Order yang aktif."""
        # Gunakan list(values()) karena self._active_orders bisa berubah saat loop berjalan
        for entry in list(self._active_orders.values()):
            order = entry.order
            
            # Abaikan jika bukan tipe limit atau simbol beda
            if order.symbol != symbol or getattr(order, 'order_type', OrderType.LIMIT) != OrderType.LIMIT:
                continue
                
            # [SURGERY FIX 4]: Pelindung Type Float!
            if order.price is None:
                continue

            executable = False
            if order.side == OrderSide.BUY:
                if current_price <= order.price:
                    executable = True
            else:  # SELL
                if current_price >= order.price:
                    executable = True

            if executable:
                self._fill_order(entry, current_price, is_taker=True)


    def update_price(self, symbol: str, price: float):
        """
        Update the last known price for a symbol and trigger order matching.
        """
        self._last_prices[symbol] = price
        # Trigger matching engine for this symbol
        self._match_orders(symbol, price)
    # Utilitas untuk Mock Price Feed dari luar
