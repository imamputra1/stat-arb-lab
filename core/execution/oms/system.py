"""
THE BRAIN - UNIVERSAL ORDER MANAGEMENT SYSTEM
Location: core/execution/oms/system.py
Desc: Orchestrator utama (Facade) yang menghubungkan Strategy, Risk, dan Broker.
      Menerapkan prinsip 'The Glass Box' (Transparan & Agnostic).
"""

import uuid
import asyncio
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Dict, List

# Core Imports
from core.shared.result import Result, Ok, Err
from core.shared.utils import get_logger

# Execution Protocol & Types
from core.execution.protocols import (
    BrokerProtocol, 
    MarketDataProtocol, 
    RiskCheckProtocol
)
from core.execution.types import (
    OrderRequest, 
    ExecutionReport, 
    TimeInForce
)

# Internal Components
# Kita meng-import definisi komponen, tapi instansiasinya ada di dalam OMS (Composition)
from .components import (
    InventoryManager, 
    Accountant, 
    Sentry,
    PortfolioSnapshot
)

logger = get_logger("oms.system")

# ====================== CONFIGURATION ======================

class OMSMode(Enum):
    RESEARCH = "RESEARCH"      # Lab (Backtest)
    PAPER = "PAPER"            # Forward Test (Uang Mainan)
    LIVE = "LIVE"              # Uang Asli

@dataclass(frozen=True)
class OMSConfig:
    """
    Konfigurasi OMS yang Immutable dan Aman.
    """
    # Identitas
    mode: OMSMode = OMSMode.RESEARCH
    oms_id: str = field(default_factory=lambda: f"OMS-{uuid.uuid4().hex[:6]}")
    
    # Safety Limits (Hard Constraints)
    max_open_orders: int = 50
    max_notional_per_order: float = 100_000.0  # $100k cap
    
    # Timeouts & Latency Control
    order_timeout_seconds: int = 30
    default_tif: TimeInForce = TimeInForce.GTC
    
    # Feature Flags
    auto_reconcile: bool = True     # Cek sinkronisasi dgn broker tiap X detik
    validate_risk: bool = True      # Jalankan Pre-Trade Risk Check
    
    def validate(self) -> Result['OMSConfig', str]:
        """Self-Validation saat startup"""
        if self.max_open_orders <= 0:
            return Err("max_open_orders must be positive")
        if self.max_notional_per_order <= 0:
            return Err("max_notional_per_order must be positive")
        return Ok(self)

# ====================== HELPER UTILS ======================

class DummyLock:
    """
    Lock palsu untuk konteks synchronous atau single-threaded.
    Diperlukan agar code path 'async with lock:' tetap jalan tanpa error.
    """
    async def __aenter__(self): return self
    async def __aexit__(self, *args): pass

# ====================== THE FACADE SYSTEM ======================

class OrderManagementSystem:
    """
    [FACADE] Sistem Manajemen Order Universal.
    User (Strategy) hanya perlu berinteraksi dengan class ini, 
    tidak perlu tahu ada Inventory, Accountant, atau Sentry di dalamnya.
    """
    
    def __init__(
        self, 
        broker: BrokerProtocol,                 # Wajib: Tangan (Eksekutor)
        market_data: Optional[MarketDataProtocol] = None, # Opsional: Mata
        risk_check: Optional[RiskCheckProtocol] = None,   # Opsional: Polisi
        config: Optional[OMSConfig] = None
    ):
        # 1. Config Setup
        self.config = config if config else OMSConfig()
        if self.config.validate().is_err():
            raise ValueError(f"Invalid OMS Config: {self.config.validate().unwrap_err()}")
            
        # 2. Dependency Injection (The Tools)
        self.broker = broker
        self.market_data = market_data
        self.risk_check = risk_check
        
        # 3. Internal State (The Brain Memory)
        self._is_running = False
        self._orders: Dict[str, OrderRequest] = {}   # Active Orders
        self._history: List[ExecutionReport] = []    # Ledger
        self._lock = asyncio.Lock() # Thread-safety
        
        # 4. Components (The Organs - Composition Pattern)
        # OMS menyembunyikan komponen ini dari dunia luar
        self.inventory = InventoryManager()
        self.accountant = Accountant()
        self.sentry = Sentry()
        
        logger.info(f"🚀 OMS Initialized | Mode: {self.config.mode.value} | ID: {self.config.oms_id}")

    # --- PUBLIC API (FACADE METHODS) ---

    async def start(self):
        """Menyalakan sistem dan melakukan rekonsiliasi awal"""
        self._is_running = True
        logger.info("OMS Started.")
        if self.config.auto_reconcile:
            await self.reconcile()

    async def stop(self):
        """Mematikan sistem dengan anggun (Graceful Shutdown)"""
        self._is_running = False
        logger.info("OMS Stopped.")

    async def submit_order(self, request: OrderRequest) -> Result[ExecutionReport, str]:
        """
        [FACADE ENTRY] Pintu Masuk Utama Order.
        Strategi tidak perlu tahu bahwa di dalam sini ada Sentry, Inventory, dan Broker.
        """
        if not self._is_running:
            return Err("OMS is not running")

        async with self._lock:
            # 1. Pre-Flight Checks (Internal OMS Logic)
            if len(self._orders) >= self.config.max_open_orders:
                return Err(f"Max open orders limit reached ({self.config.max_open_orders})")

            # 2. Internal Risk Check (Sentry)
            sentry_check = self.sentry.validate_order(request)
            if sentry_check.is_err():
                return Err(f"Sentry Blocked: {sentry_check.unwrap_err()}")

            # 3. External Risk Check (Optional Protocol)
            if self.risk_check and self.config.validate_risk:
                risk_res = await self.risk_check.validate_order(request)
                if risk_res.is_err():
                    return Err(f"Risk Protocol Blocked: {risk_res.unwrap_err()}")
                if not risk_res.unwrap():
                     return Err("Risk Protocol Validation Returned False")

            # 4. Execution (Delegasi ke Broker)
            logger.info(f"📤 Sending Order: {request.symbol} {request.side.value} {request.quantity}")
            broker_res = await self.broker.submit_order(request)
            
            # 5. Post-Process
            if broker_res.is_ok():
                report = broker_res.unwrap()
                self._handle_execution_report(report)
                return Ok(report)
            else:
                logger.error(f"❌ Order Rejected by Broker: {broker_res.unwrap_err()}")
                return broker_res

    async def cancel_order(self, order_id: str) -> Result[ExecutionReport, str]:
        """Membatalkan order yang ada"""
        if order_id not in self._orders:
            return Err(f"Order ID {order_id} not found in OMS")
            
        return await self.broker.cancel_order(order_id)

    async def reconcile(self):
        """Mencocokkan catatan OMS dengan Broker (Reality Check)"""
        logger.info("🔄 Reconciling with Broker...")
        
        # Get Broker State
        broker_positions_res = await self.broker.get_all_positions()
        if broker_positions_res.is_err():
            logger.error(f"Reconcile Failed: {broker_positions_res.unwrap_err()}")
            return

        broker_positions = broker_positions_res.unwrap()
        
        # Force sync inventory to broker state (Broker is Truth)
        async with self._lock:
            self.inventory.sync_positions(broker_positions)
        
        logger.info("✅ Reconciliation Complete")

    def get_portfolio_snapshot(self) -> PortfolioSnapshot:
        """
        [FACADE READ] Mengambil snapshot lengkap portfolio.
        Menggabungkan data dari Inventory dan Accountant menjadi satu report rapi.
        """
        positions = self.inventory.get_all_positions()
        # Asumsi Accountant bisa menghitung total realized PnL
        realized_pnl = self.accountant.get_total_realized_pnl()
        
        return PortfolioSnapshot(
            timestamp=0.0, # Nanti diisi datetime.now()
            positions=positions,
            total_realized_pnl=realized_pnl,
            # Field lain bisa ditambahkan sesuai kebutuhan component PortfolioSnapshot
        )

    # --- PRIVATE METHODS ---

    def _handle_execution_report(self, report: ExecutionReport):
        """
        Central Event Handler for Order Updates.
        Updates Inventory, Accountant, and Internal State.
        """
        # 1. Update Internal State
        if report.order.is_terminal:
            self._orders.pop(report.order.order_id, None)

        self._history.append(report) # Catat di ledger

        # 2. Update Components (The Organs)
        if report.fills:
            for fill in report.fills:
                # Inventory hitung stok
                self.inventory.on_fill(fill)
                # Accountant hitung duit
                self.accountant.on_fill(fill)

# ====================== FACTORY FUNCTION ======================

def create_oms(
    broker: BrokerProtocol,
    market_data: Optional[MarketDataProtocol] = None,
    risk_check: Optional[RiskCheckProtocol] = None,
    mode: OMSMode = OMSMode.RESEARCH,
    **kwargs
) -> Result[OrderManagementSystem, str]:
    """
    [FACTORY] Cara standar membuat OMS.
    Menyembunyikan detail inisialisasi Config dan Injection.
    
    Args:
        broker: Implementasi Broker (Simulator/Binance)
        market_data: Sumber harga
        risk_check: Polisi risiko eksternal
        mode: Mode operasi (RESEARCH/PAPER/LIVE)
        **kwargs: Parameter config tambahan (max_open_orders, dll)
    """
    try:
        # 1. Build Config
        config = OMSConfig(mode=mode, **kwargs)
        
        # 2. Validate Config
        validation = config.validate()
        if validation.is_err():
            return Err(f"OMS Factory Error: {validation.unwrap_err()}")
            
        # 3. Instantiate System
        oms = OrderManagementSystem(
            broker=broker,
            market_data=market_data,
            risk_check=risk_check,
            config=config
        )
        
        return Ok(oms)
        
    except Exception as e:
        return Err(f"Failed to create OMS: {str(e)}")
