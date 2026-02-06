"""
EXECUTION MODULE FACADE
Location: core/execution/__init__.py
Desc: Clean facade exposing only the essential interfaces.
"""

# Re-export core types
from .types import (
    # Enums
    OrderType,
    OrderSide,
    OrderStatus,
    TimeInForce,
    
    # Core Entities
    OrderRequest,
    Order,
    TradeFill,
    ExecutionReport,
    
    # Factory
    OrderFactory,
    
    # Metrics
    ExecutionMetrics,
    
    # Protocols
    Executable,
)

# Re-export simulator
from .simulator import ExecutionSimulator

# Version
__version__ = "2.0.0"
__author__ = "Trading Systems Engineering"
__description__ = "Industrial-grade execution simulation system"

# Shortcut functions
def create_market_order(
    symbol: str,
    side: OrderSide,
    quantity: float,
    **kwargs
) -> OrderRequest:
    """Convenience function for creating market orders"""
    return OrderFactory.market(symbol, side, quantity, **kwargs)

def create_limit_order(
    symbol: str,
    side: OrderSide,
    quantity: float,
    price: float,
    **kwargs
) -> OrderRequest:
    """Convenience function for creating limit orders"""
    return OrderFactory.limit(symbol, side, quantity, price, **kwargs)

# Export convenience functions
__all__ = [
    # Core Types
    'OrderType',
    'OrderSide',
    'OrderStatus',
    'TimeInForce',
    
    # Core Entities
    'OrderRequest',
    'Order',
    'TradeFill',
    'ExecutionReport',
    
    # Factory
    'OrderFactory',
    
    # Simulator
    'ExecutionSimulator',
    
    # Metrics
    'ExecutionMetrics',
    
    # Protocols
    'Executable',
    
    # Convenience Functions
    'create_market_order',
    'create_limit_order',
    
    # Version info
    '__version__',
    '__author__',
    '__description__',
]
