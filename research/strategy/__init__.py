"""
STRATEGY MODULE (THE COMMAND CENTER) - V1.0
Location: research/strategy/__init__.py
Exports:
    - Data preparation pipeline (load, align, compute spread)
    - In-memory Kalman executor
    - Legacy compatibility class
"""

# Data preparation pipeline
from .pipeline import (
    load_silver_data,
    align_and_sanitize,
    calculate_raw_spread,
    prepare_combat_data,
    AdvancedStrategyPipeline,
)

# Kalman in-memory executor
from .executor import run_kalman_backtest

__all__ = [
    # Pipeline
    "load_silver_data",
    "align_and_sanitize",
    "calculate_raw_spread",
    "prepare_combat_data",
    "AdvancedStrategyPipeline",
    # Executor
    "run_kalman_backtest",
]
