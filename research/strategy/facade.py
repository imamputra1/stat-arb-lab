"""
RESEARCH LAB FACADE
Location: research/strategy/facade.py
Desc: Unified interface for backtesting and optimization.
"""
from typing import Dict, Any, List, Tuple, Optional
from pathlib import Path

from core.shared import Result
from .pipeline import AdvancedStrategyPipeline
from .optimization.shotgun import HyperParallelEngine

class ResearchLabFacade:
    """
    Facade for research operations: backtest and parameter optimization.
    """
    
    def __init__(self, data_dir: Optional[Path] = None):
        self.data_dir = data_dir or Path("data/silver")
    
    def run_backtest(self, target: str, anchor: str, start: str, end: str, **params) -> Result[Dict[str, Any], str]:
        """
        Run a single backtest with given parameters.
        """
        pipeline = AdvancedStrategyPipeline(
            target=target,
            anchor=anchor,
            start=start,
            end=end,
            **params
        )
        return pipeline.execute_pair_arbitrage(target, anchor, start, end)
    
    def run_optimization(self, target_pairs: List[Tuple[str, str]], 
                         space_name: str = "shotgun",
                         start: str = "2024-01-01", 
                         end: str = "2024-12-31",
                         num_random_trials: Optional[int] = None,
                         max_combos: Optional[int] = None) -> Result[Path, str]:
        """
        Run hyper-parameter optimization using shotgun engine.
        """
        engine = HyperParallelEngine()
        return engine.fire(
            target_pairs=target_pairs,
            space_name=space_name,
            start_date=start,
            end_date=end,
            max_combos=max_combos,
            num_random_trials=num_random_trials
        )
