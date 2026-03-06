# research/strategy/optimization/__init__.py
"""
OPTIMIZATION GATEWAY (THE CITY GATE) - V9.0
Location: research/strategy/optimization/__init__.py
"""
from .spaces import QuantumParameterSpace, get_parameter_space, ParameterSpace, SearchResult
from .shotgun import run_shotgun_test

__all__ = [
    "QuantumParameterSpace",
    "get_parameter_space",
    "ParameterSpace",
    "SearchResult",
    "run_shotgun_test",
]
