"""
EXECUTION MODULE - FACADE
Location: core/execution/__init__.py
Desc: Expose core components cleanly.
"""

# 1. Types (Vocabulary)
from .types import (
    Order, 
    OrderRequest, 
    TradeFill, 
    ExecutionReport,
    OrderStatus, 
    OrderType, 
    OrderSide, 
    TimeInForce,
    OrderResult,
    FillResult,
    ExecutionReportResult,
    Symbol,
    Currency,
    OrderFactory
)

# 2. Protocols (Contracts)
from .protocols import (
    ExecutionHandler,
    RiskManagerProtocol,
    MarketDataProvider
)

# 3. Base Implementation (Foundation)
from .base import BaseExecutionHandler

# 4. Configuration & Exceptions
from .config import ExecutionConfig
from .exceptions import (
    ExecutionError,
    InsufficientFundsError,
    OrderNotFoundError,
    RateLimitError
)
# 5. Simulator
from .simulator import ExecutionSimulator

# 5. OMS (State Manager) - Kita akan buat ini setelah Mechanics
# from .oms import OrderManagementSystem

__all__ = [
    # Types
    'Order', 'OrderRequest', 'TradeFill', 'ExecutionReport',
    'OrderStatus', 'OrderType', 'OrderSide', 'TimeInForce',
    'OrderResult', 'FillResult', 'ExecutionReportResult',
    'Symbol', 'Currency', 'OrderFactory',
    
    # Protocols
    'ExecutionHandler', 'RiskManagerProtocol', 'MarketDataProvider',
    
    # Base
    'BaseExecutionHandler',
    
    # Config & Exceptions
    'ExecutionConfig',
    'ExecutionError', 'InsufficientFundsError', 
    'OrderNotFoundError', 'RateLimitError',

    # Simulator
    'ExecutionSimulator'
]
