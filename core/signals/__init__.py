"""
CORE SIGNALS MODULE
Location: core/signals/__init__.py
Role: Public Facade for the Signal Generation Ecosystem.
"""

# 1. Base Interfaces & Types
from .base_signal import BaseStrategy
from .types import (
    SignalConfig,
    SignalType,
    SignalEvent,
    MarketObservation,
    MarketState
)

# 2. Factory & Assembly Line
from .factory import (
    SignalFactory,
    FactoryManager,
    StrategyRegistry,
    ConfigParser,
    create_strategy,
    validate_config,
    get_factory
)

# 3. Facade (THE MISSING PIECE)
from .facade import SignalGeneratorFacade

# 4. Public Export List
__all__ = [
    # Base
    'BaseStrategy',
    
    # Data Structures
    'SignalConfig',
    'SignalType',
    'SignalEvent',
    'MarketObservation',
    'MarketState',
    
    # Factory Ecosystem
    'SignalFactory',
    'FactoryManager',
    'StrategyRegistry',
    'ConfigParser',
    
    # Quick Access Functions
    'create_strategy',
    'validate_config',
    'get_factory',

    # Facade
    'SignalGeneratorFacade'
]
