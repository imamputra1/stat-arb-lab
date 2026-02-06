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
    MarketObservation
)

# 2. Factory & Assembly Line (New)
# Kita expose class utama dan fungsi pintas (quick access)
from .factory import (
    SignalFactory,
    FactoryManager,
    StrategyRegistry,
    ConfigParser,
    create_strategy,   # <-- Helper instan
    validate_config,   # <-- Helper instan
    get_factory        # <-- Singleton Access
)

# 3. Public Export List
__all__ = [
    # Base
    'BaseStrategy',
    
    # Data Structures
    'SignalConfig',
    'SignalType',
    'SignalEvent',
    'MarketObservation',
    
    # Factory Ecosystem
    'SignalFactory',
    'FactoryManager',
    'StrategyRegistry',
    'ConfigParser',
    
    # Quick Access Functions
    'create_strategy',
    'validate_config',
    'get_factory'
]
