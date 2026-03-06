"""
STRATEGY MODULE (THE COMMAND CENTER) - V1.1
Location: research/strategy/__init__.py
Exports:
    - Data preparation pipeline (prepare_combat_data)
    - In-memory Kalman executor
"""

# Data preparation pipeline
from .pipeline import prepare_combat_data

# Kalman in-memory executor
from .executor import run_kalman_backtest

__all__ = [
    # Pipeline
    "prepare_combat_data",
    # Executor
    "run_kalman_backtest",
]
