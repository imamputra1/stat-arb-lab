"""
OPTIMIZATION GATEWAY (THE CITY GATE) - V8.4 SYNC
Location: research/strategy/optimization/__init__.py
"""
from .spaces import QuantumParameterSpace, get_parameter_space, ParameterSpace, SearchResult
from .objective import QuantumScoreKeeper, ScoringStrategy, calculate_smart_score
from .shotgun import HyperParallelEngine
from .storage import OptimizationClerk, QuantumVault

__all__ = [
    "QuantumParameterSpace", 
    "get_parameter_space",
    "ParameterSpace",
    "SearchResult",
    "QuantumScoreKeeper", 
    "ScoringStrategy",
    "calculate_smart_score",
    "HyperParallelEngine",
    "OptimizationClerk",
    "QuantumVault"
]
