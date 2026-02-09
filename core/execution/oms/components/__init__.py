"""
OMS COMPONENTS FACADE
Location: core/execution/oms/components/__init__.py
Desc: Mengekspos logic component dan internal struct mereka.
      TIDAK me-reexport tipe dasar dari core.execution.types (Clean Architecture).
"""

# 1. Import Component Logic
from .inventory import InventoryManager
from .accountant import Accountant, TradeRecord
from .sentry import Sentry, RiskViolation, RiskViolationLevel

# 2. Define Shared View Models (Data gabungan antar komponen)
# PortfolioSnapshot adalah hasil gabungan Inventory + Accountant,
# jadi dia layak hidup di level facade components ini.
from dataclasses import dataclass
from typing import List
from core.execution.types import Position 

@dataclass
class PortfolioSnapshot:
    """
    Potret lengkap kekayaan saat ini (View Model).
    Digunakan oleh UI/Dashboard/Logger.
    """
    timestamp: float
    positions: List[Position]
    total_realized_pnl: float
    total_fees: dict
    # cash_balance, dll bisa ditambahkan nanti

__all__ = [
    # Logic Components
    'InventoryManager',
    'Accountant', 
    'Sentry',
    
    # Component-Specific Structs
    'TradeRecord',
    'RiskViolation',
    'RiskViolationLevel',
    
    # Composite View Models
    'PortfolioSnapshot'
]
