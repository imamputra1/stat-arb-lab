"""
RISK MANAGEMENT MODULE - FACADE PATTERN
Location: core/risk/__init__.py
Exports all risk types through clean facade
"""

# Re-export everything from types module
from .types import (
    # Protocols
    RiskValidatable,
    SizeCalculator,
    
    # Enums
    RiskLevel,
    TradeAction,
    RejectionCode,
    
    # Models
    RiskConfigModel,
    
    # Data Classes
    RiskConfig,
    AccountState,
    TradeRequest,
    TradeVerdict,
    RiskMetrics,
    
    # Composition
    RiskContext,
    
    # Factories
    RiskFactory,
    
    # Type Aliases
    RiskResult,
    SizeCalculation
)

# Facade functions for common operations
def create_default_config() -> RiskConfig:
    """Create default risk configuration"""
    return RiskFactory.create_config()

def create_account_snapshot(balance: float, equity: float, **kwargs) -> AccountState:
    """Create account state snapshot"""
    return RiskFactory.create_account_state(balance=balance, equity=equity, **kwargs)

def create_approval(size: float, request_id: str) -> TradeVerdict:
    """Create approval verdict"""
    return RiskFactory.create_verdict_approve(size, request_id)

def create_rejection(reason: str, code: RejectionCode, request_id: str) -> TradeVerdict:
    """Create rejection verdict"""
    return RiskFactory.create_verdict_reject(reason, code, request_id)

# Export everything
__all__ = [
    # From types
    'RiskValidatable',
    'SizeCalculator',
    'RiskLevel',
    'TradeAction',
    'RejectionCode',
    'RiskConfigModel',
    'RiskConfig',
    'AccountState',
    'TradeRequest',
    'TradeVerdict',
    'RiskMetrics',
    'RiskContext',
    'RiskFactory',
    'RiskResult',
    'SizeCalculation',
    
    # Facade functions
    'create_default_config',
    'create_account_snapshot',
    'create_approval',
    'create_rejection'
]
