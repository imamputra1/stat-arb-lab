"""
ENGINES SECTOR GATE
Location: research/strategy/engines/__init__.py
"""
from .vectorized import HybridBacktestEngine, create_backtest_engine

__all__ = ["HybridBacktestEngine", "create_backtest_engine"]
