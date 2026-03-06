"""
OPTIMIZATION GATEWAY (THE CITY GATE) - V8.4 SYNC
Location: research/strategy/optimization/__init__.py
"""
from .spaces import QuantumParameterSpace, get_parameter_space, ParameterSpace, SearchResult
from .shotgun import HyperParallelEngine

__all__ = [
    "QuantumParameterSpace", 
    "get_parameter_space",
    "ParameterSpace",
    "SearchResult",
    "HyperParallelEngine"
]
