"""
THE GATEWAY - OMS MODULE ENTRY POINT
Location: core/execution/oms/__init__.py
Role: Public API. Mengutamakan Facade untuk penggunaan umum.
"""

# 1. The Tour Guide (Recommended Entry Point)
from .facade import OMSFacade

# 2. The Engine Room (For Advanced Usage)
from .system import (
    OrderManagementSystem,
    OMSConfig,
    OMSMode,
    DummyLock,
    create_oms
)

# 3. Component Details (For Types/Inspection)
from .components import (
    InventoryManager,
    Accountant,
    Sentry,
    PortfolioSnapshot,
    TradeRecord,
    RiskViolation,
    RiskViolationLevel
)

# 4. Shared Types (Re-export from core/execution/types for convenience)
# Agar user bisa akses Position/OrderSide via oms.Position tanpa import panjang
from core.execution.types import Position

__all__ = [
    # Facade (User Friendly)
    'OMSFacade',
    
    # System (Advanced)
    'OrderManagementSystem',
    'OMSConfig',
    'OMSMode',
    'DummyLock',
    'create_oms',
    
    # Components & Data
    'InventoryManager',
    'Accountant',
    'Sentry',
    'PortfolioSnapshot',
    'TradeRecord',
    'RiskViolation',
    'RiskViolationLevel',
    'Position'
]
