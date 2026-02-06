"""
EXECUTION MODULE - FACADE PATTERN
Location: core/execution/__init__.py
Desc: Clean interface to the execution subsystem.
      Black-box complexity, expose only what's needed.
"""

# Re-export core types
from .types import (
    # Domain Primitives
    Currency,
    Symbol,
    MonetaryAmount,
    
    # Enums
    OrderType,
    OrderSide,
    OrderStatus,
    
    # Strong IDs
    OrderId,
    StrategyId,
    
    # Core Types
    Order,
    TradeFill,
    ExecutionReport,
    
    # Type Aliases
    OrderResult,
    FillResult,
    ExecutionReportResult,
    
    # Factory Functions
    create_market_order,
    create_limit_order,
    create_stop_loss_order,
)

# Re-export protocols
from .protocols import (
    ExecutionHandler,
    MarketDataProvider,
)

# Re-export OMS
from .oms import (
    OrderManager,
    OrderBook,
)

# Re-export simulator (for research/backtesting)
from .simulator import (
    SimulationEngine,
    SlippageModel,
    FeeModel,
)

# ====================== CONVENIENCE FACTORY ======================

def create_execution_handler(
    handler_type: str = "simulator",
    **config
) -> ExecutionHandler:
    """
    Factory method for creating execution handlers.
    Black-box instantiation based on configuration.
    
    Args:
        handler_type: "simulator", "binance", "bybit", "mock"
        **config: Handler-specific configuration
    
    Returns:
        Configured execution handler instance
    """
    # Lazy imports for modularity
    if handler_type == "simulator":
        from .simulator import SimulationEngine
        return SimulationEngine(**config)
    
    # Future implementations
    # elif handler_type == "binance":
    #     from .binance_handler import BinanceHandler
    #     return BinanceHandler(**config)
    
    else:
        from .simulator import SimulationEngine
        return SimulationEngine(**config)

__all__ = [
    # Core Types
    'Currency',
    'Symbol',
    'MonetaryAmount',
    
    # Enums
    'OrderType',
    'OrderSide',
    'OrderStatus',
    
    # IDs
    'OrderId',
    'StrategyId',
    
    # Main Types
    'Order',
    'TradeFill',
    'ExecutionReport',
    
    # Type Aliases
    'OrderResult',
    'FillResult',
    'ExecutionReportResult',
    
    # Factories
    'create_market_order',
    'create_limit_order',
    'create_stop_loss_order',
    'create_execution_handler',
    
    # Protocols
    'ExecutionHandler',
    'MarketDataProvider',
    
    # OMS
    'OrderManager',
    'OrderBook',
    
    # Simulator
    'SimulationEngine',
    'SlippageModel',
    'FeeModel',
]
