"""
THE EVIL SIMULATOR (THE ACTOR)
Location: core/execution/simulator.py
Desc: Broker simulasi yang mengimplementasikan BrokerProtocol.
      Mendukung Latency Injection, Slippage, dan Market Impact.
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
    # Modal Awal
    initial_cash: float = 100_000.0
    base_currency: str = "USDT"
    
    # Faktor Kesulitan (The Evil Parameters)
    latency_mean_ms: float = 50.0      # Rata-rata delay jaringan
    latency_std_ms: float = 10.0       # Jitter/Variasi delay
    slippage_std_bps: float = 2.0      # Volatilitas spread (Basis Points)
    fee_rate_maker: float = 0.0002     # 0.02%
    fee_rate_taker: float = 0.0004     # 0.04%
    reject_probability: float = 0.001  # 0.1% order random ditolak exchange
    
    # Market Impact (Opsional)
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
        
        # State Internal (Dompet & Order Book)
        self._cash_balances: Dict[str, float] = {
            config.base_currency: config.initial_cash
        }
        self._positions: Dict[str, Position] = {} # Symbol -> Position
        self._active_orders: Dict[str, _OrderBookEntry] = {}
        self._order_history: List[ExecutionReport] = []
        
        # Market State (Harga Terakhir yang diketahui Simulator)
        self._last_prices: Dict[str, float] = {}
        
        logger.info(f"😈 Evil Simulator Initialized | Latency: ~{config.latency_mean_ms}ms")

    # =========================================================
    # IMPLEMENTASI BROKER PROTOCOL (YANG DILIHAT OMS)
    # =========================================================

    async def submit_order(self, request: OrderRequest) -> Result[ExecutionReport, str]:
        """
        [PROTOCOL] Menerima order dari OMS.
        """
        # 1. Simulasi Latency (Network Delay)
        await self._simulate_latency()
        
        # 2. Random Rejection (System Busy Simulation)
        if random.random() < self.config.reject_probability:
            return self._create_rejection(request, "EXCHANGE_SYSTEM_ERROR")

        # 3. Validasi Saldo (Simple Check)
        # Note: Di real exchange, ini lebih kompleks (margin check).
        # Kita asumsikan OMS Sentry sudah melakukan basic check, 
        # tapi Exchange tetap cek final.
        current_price = self._last_prices.get(request.symbol, request.price)
        if not current_price and request.order_type == OrderType.MARKET:
            return Err(f"Simulator has no price for {request.symbol}")

        # 4. Buat Order Object Internal
        order = Order.from_request(request, exchange_order_id=f"SIM-{uuid.uuid4().hex[:8]}")
        order = order.mark_acknowledged(order.exchange_order_id)
        
        # 5. Eksekusi (Matching Engine Logic)
        if request.order_type == OrderType.MARKET:
            return self._execute_market_order(order, current_price)
        elif request.order_type == OrderType.LIMIT:
            return self._place_limit_order(order)
        else:
            return self._create_rejection(request, "UNSUPPORTED_ORDER_TYPE")

    async def cancel_order(self, order_id: str) -> Result[ExecutionReport, str]:
        """
        [PROTOCOL] Membatalkan order aktif.
        """
        await self._simulate_latency()
        
        if order_id not in self._active_orders:
            return Err("Order not found or already filled")
            
        entry = self._active_orders.pop(order_id)
        order = entry.order.mark_canceled()
        
        report = ExecutionReport(
            order=order,
            fills=(),
            is_complete=True,
            total_notional=order.filled_quantity * order.average_fill_price
        )
        return Ok(report)

    async def get_order(self, order_id: str) -> Result[ExecutionReport, str]:
        """[PROTOCOL] Get status order."""
        # Cek active
        if order_id in self._active_orders:
            entry = self._active_orders[order_id]
            # Return partial status
            return Ok(ExecutionReport.from_order_and_fills(entry.order, []))
        
        # Cek history (Scan linear - lambat tapi oke untuk sim)
        for report in reversed(self._order_history):
            if report.order.order_id == order_id:
                return Ok(report)
                
        return Err("Order ID not found in Simulator")

    async def get_open_orders(self) -> Result[List[ExecutionReport], str]:
        """[PROTOCOL] Get all active orders."""
        reports = []
        for entry in self._active_orders.values():
            reports.append(ExecutionReport.from_order_and_fills(entry.order, []))
        return Ok(reports)

    async def get_all_positions(self) -> Result[List[Position], str]:
        """[PROTOCOL] Snapshot posisi untuk rekonsiliasi OMS."""
        return Ok(list(self._positions.values()))

    async def get_balance(self) -> Result[Dict[str, float], str]:
        """[PROTOCOL] Snapshot saldo."""
        return Ok(self._cash_balances.copy())

    # =========================================================
    # INTERNAL MATCHING ENGINE (THE BRAIN OF EXCHANGE)
    # =========================================================

    def process_tick(self, tick: Any):
        """
        [PUBLIC API FOR BACKTEST ENGINE]
        Simulator perlu tahu 'Waktu' dan 'Harga' bergerak.
        Dipanggil oleh Event Loop setiap kali ada MarketTick baru.
        """
        symbol = tick.symbol
        price = tick.price
        self._last_prices[symbol] = price
        
        # Cek Limit Orders yang antri
        # Kita copy keys karena kita mungkin modify dictionary saat iterasi (fills)
        active_ids = list(self._active_orders.keys())
        
        for order_id in active_ids:
            if order_id not in self._active_orders: continue
            
            entry = self._active_orders[order_id]
            order = entry.order
            
            if order.symbol != symbol: continue
            
            # Match Logic:
            # BUY Limit: Execute jika Market Price <= Limit Price
            # SELL Limit: Execute jika Market Price >= Limit Price
            
            is_match = False
            if order.side == OrderSide.BUY and price <= order.price:
                is_match = True
            elif order.side == OrderSide.SELL and price >= order.price:
                is_match = True
                
            if is_match:
                # Execute Limit Order
                # Asumsi fill penuh untuk MVP (bisa dibuat partial tergantung volume)
                self._fill_order(entry, price, is_taker=False)

    def _execute_market_order(self, order: Order, current_price: float) -> Result[ExecutionReport, str]:
        """Eksekusi instan Market Order dengan Slippage."""
        # Hitung Slippage (Random Walk Gaussian)
        # Slippage = Harga sesungguhnya meleset dari harga terakhir dilihat
        slippage_pct = random.gauss(0, self.config.slippage_std_bps / 10000.0)
        
        if order.side == OrderSide.BUY:
            exec_price = current_price * (1 + abs(slippage_pct)) # Buy lebih mahal
        else:
            exec_price = current_price * (1 - abs(slippage_pct)) # Sell lebih murah
            
        # Market order always taker
        entry = _OrderBookEntry(order, 0.0, order.quantity)
        return self._fill_order(entry, exec_price, is_taker=True)

    def _place_limit_order(self, order: Order) -> Result[ExecutionReport, str]:
        """Taruh Limit Order di buku (Queue)."""
        entry = _OrderBookEntry(order, 0.0, order.quantity)
        self._active_orders[order.order_id] = entry
        
        # Return status NEW (belum fill)
        report = ExecutionReport(
            order=order.mark_open().unwrap(), # Status -> OPEN
            fills=(),
            is_complete=False
        )
        return Ok(report)

    def _fill_order(self, entry: _OrderBookEntry, price: float, is_taker: bool) -> Result[ExecutionReport, str]:
        """Helper: Menghasilkan Fill dan Update State Dompet"""
        order = entry.order
        qty = entry.remaining_qty # Full fill for now
        
        # 1. Hitung Fee
        fee_rate = self.config.fee_rate_taker if is_taker else self.config.fee_rate_maker
        notional = qty * price
        fee_amt = notional * fee_rate
        
        # 2. Update Internal Wallets (Exchange Side Accounting)
        self._update_exchange_balances(order.symbol, order.side, qty, price, fee_amt)
        
        # 3. Create Fill Record
        fill = TradeFill.create(
            order_id=order.order_id,
            symbol=order.symbol,
            side=order.side,
            quantity=qty,
            price=price,
            fee=fee_amt,
            fee_currency=self.config.base_currency, # Asumsi fee dalam quote
            latency_ms=random.gauss(self.config.latency_mean_ms, self.config.latency_std_ms),
            slippage_bps=abs((price - (order.price or price))/price * 10000)
        ).unwrap()
        
        # 4. Update Order State
        updated_order = order.add_fill(qty, price).unwrap()
        
        # 5. Clean up Order Book
        if updated_order.is_terminal:
            self._active_orders.pop(order.order_id, None)
        else:
            entry.order = updated_order
            entry.filled_qty += qty
            entry.remaining_qty -= qty
            
        # 6. Generate Report
        report = ExecutionReport.from_order_and_fills(updated_order, [fill])
        self._order_history.append(report)
        
        return Ok(report)

    def _update_exchange_balances(self, symbol: str, side: OrderSide, qty: float, price: float, fee: float):
        """
        Logika akuntansi internal Simulator.
        Memastikan Simulator tahu posisi user (untuk get_all_positions).
        """
        # Update Posisi
        if symbol not in self._positions:
            self._positions[symbol] = PositionFactory.create_empty(symbol, self.config.base_currency)
            
        pos = self._positions[symbol]
        
        # Logic sederhana: mark_to_market dilakukan oleh OMS, Simulator hanya catat Qty & Cost
        # Kita pakai logika sederhana: update qty saja
        qty_signed = qty if side == OrderSide.BUY else -qty
        
        # Update Cash
        cost = qty * price
        if side == OrderSide.BUY:
            self._cash_balances[self.config.base_currency] -= cost
        else:
            self._cash_balances[self.config.base_currency] += cost
            
        # Deduct Fee
        self._cash_balances[self.config.base_currency] -= fee
        
        # Update Position Object (Create new immutable)
        # Note: Simulator tidak perlu hitung AvgPrice se-akurat OMS, 
        # tapi untuk get_all_positions kita update seadanya.
        new_qty = pos.quantity + qty_signed
        self._positions[symbol] = pos.copy(quantity=new_qty)

    def _create_rejection(self, request: OrderRequest, reason: str) -> Result[ExecutionReport, str]:
        """Helper untuk membuat laporan penolakan"""
        order = Order.from_request(request).mark_rejected("REJECTED", reason).unwrap()
        return Ok(ExecutionReport(
            order=order,
            fills=(),
            is_complete=True,
            completion_reason=reason
        ))

    async def _simulate_latency(self):
        """Tidur sebentar untuk simulasi network lag"""
        delay = random.gauss(self.config.latency_mean_ms, self.config.latency_std_ms)
        delay = max(1.0, delay) # Minimal 1ms
        await asyncio.sleep(delay / 1000.0)
