"""
RISK MANAGEMENT MODULE - FACADE PATTERN
Location: core/risk/__init__.py
Exports all risk types and logic through a clean facade.
"""

# 1. DATA STRUCTURES & TYPES (From .types)
from .types import (
    # Protocols
    RiskValidatable,
    SizeCalculator,
    
    # Enums
    RiskLevel,
    TradeAction,
    RejectionCode,
    
    # Data Classes (RiskConfigModel removed)
    RiskConfig,
    AccountState,
    TradeRequest,
    TradeVerdict,
    RiskMetrics,
    
    # Composition
    RiskContext,
    
    # Type Aliases
    RiskResult,
    SizeCalculation
)

# 2. LOGIC & ENGINE (From .manager)
from .manager import RiskManager

# ==================== FACADE FUNCTIONS (Simplified Access) ====================

def get_risk_manager(config: RiskConfig = None) -> RiskManager:
    """
    [Factory] Get a ready-to-use Risk Manager instance.
    """
    return RiskManager(config=config)

def create_config(**kwargs) -> RiskConfig:
    """
    [Factory] Create valid risk configuration safely.
    Uses .from_dict() to handle parsing/defaults.
    """
    # Menggunakan factory method yang sudah kita buat di RiskConfig
    result = RiskConfig.from_dict(kwargs)
    return result.unwrap_or(RiskConfig()) # Fallback to default if invalid

def create_account_snapshot(
    balance: float, 
    equity: float, 
    **kwargs
) -> AccountState:
    """
    [Factory] Create account state snapshot from raw values.
    """
    # Bungkus dalam dict agar bisa pakai .from_snapshot() yang robust
    snapshot_data = {'balance': balance, 'equity': equity, **kwargs}
    return AccountState.from_snapshot(snapshot_data)

# ==================== EXPORTS ====================
__all__ = [
    # Core Logic
    'RiskManager',
    
    # Data Structures
    'RiskConfig',
    'AccountState',
    'TradeRequest',
    'TradeVerdict',
    'RiskMetrics',
    'RiskContext',
    
    # Enums
    'RiskLevel',
    'TradeAction',
    'RejectionCode',
    
    # Protocols & Types
    'RiskValidatable',
    'SizeCalculator',
    'RiskResult',
    'SizeCalculation',
    
    # Facade Helpers
    'get_risk_manager',
    'create_config',
    'create_account_snapshot'
]
