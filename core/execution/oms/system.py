"""
THE BRAIN - UNIVERSAL ORDER MANAGEMENT SYSTEM
Location: core/execution/oms/system.py
"""

import uuid
import asyncio
from dataclasses import dataclass, field
from enum import Enum
from datetime import datetime, timezone
from typing import Dict, List, Set

from core.shared.result import Result, Ok, Err
from core.shared.utils import get_logger

from core.execution.types import OrderRequest, ExecutionReport, TimeInForce
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

    async def submit_order(self, request: OrderRequest) -> Result[ExecutionReport, str]:
        if not self._is_running: return Err("OMS is not running")
        async with self._lock:
            # Sentry Check
            if self.sentry.validate_order(request).is_err():
                return Err("Sentry Blocked")
            
            # Broker Submit
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
        if res.is_ok(): self._handle_execution_report(res.unwrap())
        return res

    async def poll_orders(self):
        """AGGRESSIVE POLLING: Jemput bola status order."""
        active_ids = list(self._orders.keys())
        for oid in active_ids:
            res = await self.broker.get_order(oid)
            if res.is_ok():
                self._handle_execution_report(res.unwrap())

    async def reconcile(self):
        res = await self.broker.get_all_positions()
        if res.is_ok():
            async with self._lock:
                self.inventory.sync_positions(res.unwrap())

    def get_portfolio_snapshot(self) -> PortfolioSnapshot:
        return PortfolioSnapshot(
            timestamp=datetime.now(timezone.utc).timestamp(),
            positions=self.inventory.get_all_positions(),
            total_realized_pnl=self.accountant.get_total_realized_pnl(),
            total_fees=self.accountant.get_total_fees()
        )

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
# --- FACTORY FUNCTION (YANG HILANG SEBELUMNYA) ---
def create_oms(broker, market_data=None, risk_check=None, mode=OMSMode.RESEARCH, **kwargs) -> Result[OrderManagementSystem, str]:
    try:
        cfg = OMSConfig(mode=mode, **kwargs)
        if cfg.validate().is_err(): return Err("Invalid Config")
        return Ok(OrderManagementSystem(broker, market_data, risk_check, cfg))
    except Exception as e:
        return Err(str(e))
