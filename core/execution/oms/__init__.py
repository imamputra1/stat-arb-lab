"""
OMS GATEWAY
Location: core/execution/oms/__init__.py
"""

# 1. Import Facade dari file facade.py
from .facade import OMSFacade

# 2. Import System dari file system.py
from .system import (
    OrderManagementSystem, 
    OMSConfig, 
    OMSMode, 
    create_oms
)

# 3. Import Components
from .components import (
    InventoryManager,
    Accountant,
    PortfolioSnapshot
)

__all__ = [
    'OMSFacade',              # <--- Pastikan ini ada
    'OrderManagementSystem',
    'OMSConfig',
    'OMSMode',
    'create_oms',
    'PortfolioSnapshot'
]
