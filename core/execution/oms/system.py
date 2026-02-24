"""
THE BRAIN - UNIVERSAL ORDER MANAGEMENT SYSTEM
Location: core/execution/oms/system.py
"""

import uuid
import asyncio
import math
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime, timezone
from typing import Dict, List, Set, Optional

from core.shared.result import Result, Ok, Err
from core.shared.utils import get_logger

from core.execution.types import OrderRequest, ExecutionReport, TimeInForce, Symbol
from .components import InventoryManager, Accountant, Sentry, PortfolioSnapshot

logger = get_logger("oms.system")

# --- CONFIG ---
class OMSMode(Enum):
    RESEARCH = "RESEARCH"
    PAPER = "PAPER"
    LIVE = "LIVE"

@dataclass(frozen=True)
class OMSConfig:
    mode: OMSMode = OMSMode.RESEARCH
    oms_id: str = field(default_factory=lambda: f"OMS-{uuid.uuid4().hex[:6]}")
    max_open_orders: int = 50
    max_notional_per_order: float = 100_000.0
    order_timeout_seconds: int = 30
    default_tif: TimeInForce = TimeInForce.GTC
    auto_reconcile: bool = True
    validate_risk: bool = True
    
    def validate(self) -> Result['OMSConfig', str]:
        if self.max_open_orders <= 0: return Err("max_open_orders must be positive")
        return Ok(self)

# --- SYSTEM CLASS ---
class OrderManagementSystem:
    def __init__(self, broker, market_data, risk_check, config):
        self.config = config
        self.broker = broker
        self.market_data = market_data
        self.risk_check = risk_check
        
        self._is_running = False
        self._orders: Dict[str, OrderRequest] = {}
        self._history: List[ExecutionReport] = []
        self._lock = asyncio.Lock()
        
        # SUPERIOR LOGIC: Deduplication Set
        self._processed_fills: Set[str] = set()
        
        self.inventory = InventoryManager()
        self.accountant = Accountant()
        self.sentry = Sentry()

    async def start(self):
        self._is_running = True
        if self.config.auto_reconcile: await self.reconcile()

    async def stop(self):
        self._is_running = False

    # 🔧 MODIFIED: Sekarang menggunakan comprehensive risk check dari Sentry
    async def submit_order(self, request: OrderRequest) -> Result[ExecutionReport, str]:
        if not self._is_running:
            return Err("OMS is not running")
        
        async with self._lock:
            # 1. BASIC SENTRY CHECK (duplicate, rate limit, fat finger)
            basic_check = self.sentry.validate_order(request)
            if basic_check.is_err():
                return Err(basic_check.unwrap_err())
            
            # 2. COMPREHENSIVE RISK CHECK (drawdown, exposure, solvency)
            #    Butuh equity & cash dari inventory
            market_prices = self._get_market_prices({request.symbol})
            equity = self.inventory.get_equity(market_prices)
            cash = self.inventory.get_cash_balance()
            
            risk_check = self.sentry.check_risk(request, equity, cash)
            if risk_check.is_err():
                logger.warning(f"🚫 Risk rejected: {risk_check.unwrap_err()}")
                return Err(risk_check.unwrap_err())
            
            # 3. Optional external risk callback (jika disediakan)
            if self.risk_check:
                ext_check = await self.risk_check(request)
                if ext_check.is_err():
                    return Err(ext_check.unwrap_err())
            
            # 4. Broker Submit
            res = await self.broker.submit_order(request)
            if res.is_ok():
                report = res.unwrap()
                if not report.is_complete:
                    self._orders[report.order_id] = request
                self._handle_execution_report(report)
                return Ok(report)
            return res

    async def cancel_order(self, order_id: str) -> Result[ExecutionReport, str]:
        res = await self.broker.cancel_order(order_id)
        if res.is_ok():
            self._handle_execution_report(res.unwrap())
        return res

    # 🔧 MODIFIED: Sekarang juga update peak equity dan bersihkan state
    async def poll_orders(self):
        """AGGRESSIVE POLLING: Jemput bola status order."""
        active_ids = list(self._orders.keys())
        for oid in active_ids:
            res = await self.broker.get_order(oid)
            if res.is_ok():
                self._handle_execution_report(res.unwrap())
        
        # 🔥 Update peak equity untuk drawdown protection
        await self._update_peak_equity()

    # 🔧 MODIFIED: Sekarang validasi state & rekonsiliasi accountant
    async def reconcile(self):
        res_pos = await self.broker.get_all_positions()

        res_bal = None
        if hasattr(self.broker, 'get_balance'):
            res_bal = await self.broker.get_balance()
        if res_pos.is_ok():
            async with self._lock:
                self.inventory.sync_positions(res_pos.unwrap())
                
                if res_bal and res_bal.is_ok():
                    balances = res_bal.unwrap()
                    for currency, amount in balances.items():
                        self.inventory._cash_balances[currency] = amount
                        logger.info(f"💰 Saldo tersinkronisasi: {amount:,.2f}{currency}")

                # 🔥 Validasi internal inventory
                inv_state = self.inventory.validate_state()
                if inv_state.is_err():
                    logger.error(f"🧨 Inventory state invalid: {inv_state.unwrap_err()}")
                
                # 🔥 Rekonsiliasi accountant vs inventory
                acc_rec = self.accountant.reconcile_with_inventory(self.inventory)
                if acc_rec.is_err():
                    logger.error(f"🧨 Accountant reconciliation failed: {acc_rec.unwrap_err()}")

    def get_portfolio_snapshot(self) -> PortfolioSnapshot:
        return PortfolioSnapshot(
            timestamp=datetime.now(timezone.utc).timestamp(),
            positions=self.inventory.get_all_positions(),
            total_realized_pnl=self.accountant.get_total_realized_pnl(),
            total_fees=self.accountant.get_total_fees()
        )

    # 🔥 PUBLIC METHOD: Mendapatkan equity terkini
    def get_equity(self, symbols: Optional[List[Symbol]] = None) -> float:
        """
        Menghitung total ekuiti (cash + nilai posisi) berdasarkan harga terkini.
        Jika symbols diberikan, hanya posisi tersebut yang dihitung (default semua).
        """
        if symbols:
            pos_set = set(symbols)
        else:
            pos_set = {pos.symbol for pos in self.inventory.get_all_positions()}
        market_prices = self._get_market_prices(pos_set)
        return self.inventory.get_equity(market_prices)

    # 🔥 PRIVATE: Ambil harga pasar terkini untuk symbol tertentu
    def _get_market_prices(self, symbols: Set[Symbol]) -> Dict[Symbol, float]:
        """Query market_data untuk mendapatkan harga terakhir."""
        prices = {}
        if not self.market_data:
            return prices
        
        # Asumsi market_data memiliki method get_last_price(symbol)
        # Jika tidak, fallback ke empty dict
        if not hasattr(self.market_data, 'get_last_price'):
            logger.warning("⚠️ market_data tidak memiliki get_last_price(), tidak bisa mendapat harga")
            return prices
        
        for sym in symbols:
            try:
                price = self.market_data.get_last_price(sym)
                if price is not None and isinstance(price, (int, float)) and not math.isnan(price) and not math.isinf(price):
                    prices[sym] = price
                else:
                    logger.debug(f"⚠️ Harga tidak valid untuk {sym}: {price}")
            except Exception as e:
                logger.debug(f"⚠️ Gagal mengambil harga {sym}: {e}")
        return prices

    # 🔥 PRIVATE: Update peak equity di sentry
    async def _update_peak_equity(self):
        """Hitung equity terkini dan beri tahu sentry untuk drawdown tracking."""
        all_symbols = {pos.symbol for pos in self.inventory.get_all_positions()}
        market_prices = self._get_market_prices(all_symbols)
        equity = self.inventory.get_equity(market_prices)
        self.sentry.update_peak_equity(equity)

    def _handle_execution_report(self, report: ExecutionReport):
        # 1. Update Tracking
        if report.is_complete and report.order_id in self._orders:
            self._orders.pop(report.order_id, None)
        
        # 2. Add to History
        self._history.append(report)
        
        # 3. Process Fills (CRITICAL FIX)
        if report.fills:
            for fill in report.fills:
                if fill.fill_id not in self._processed_fills:
                    self._processed_fills.add(fill.fill_id)
                    
                    # Update Components
                    self.inventory.on_fill(fill)
                    self.accountant.on_fill(fill)
                    
                    logger.info(f"💰 Processed Fill: {fill.side.value} {fill.quantity} {fill.symbol} @ {fill.price}")

# --- FACTORY FUNCTION (UNCHANGED) ---
def create_oms(broker, market_data=None, risk_check=None, mode=OMSMode.RESEARCH, **kwargs) -> Result[OrderManagementSystem, str]:
    try:
        cfg = OMSConfig(mode=mode, **kwargs)
        if cfg.validate().is_err():
            return Err("Invalid Config")
        return Ok(OrderManagementSystem(broker, market_data, risk_check, cfg))
    except Exception as e:
        return Err(str(e))


__all__ = ['OrderManagementSystem', 'OMSConfig', 'OMSMode', 'create_oms']
