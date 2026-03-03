"""
INDUSTRIAL PARAMETER SPACES (THE EXTREME AMMO DEPOT) - V5.0 FINAL
Location: research/strategy/optimization/spaces.py
Focus: Absolute synchronization with SignalGenerator and Kamikaze diagnostics.
"""
import numpy as np
import itertools
import sys
import json
import hashlib
from typing import Dict, List, Any, Generator, cast
from dataclasses import dataclass
from enum import Enum, auto
from pathlib import Path
import math
import random




# --- PATH INJECTION ---
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent.absolute()
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.shared import Result, Ok, Err

# ====================== NEW DATACLASS ======================
@dataclass
class SearchSpace:
    """
    Defines a continuous parameter search space with bounds and scaling.
    Used for optimization algorithms that require ranges (e.g., Bayesian Optimization).
    """
    name: str          # Parameter name (e.g., "entry_threshold", "process_noise")
    min_val: float     # Lower bound
    max_val: float     # Upper bound
    is_log: bool = False  # If True, use logarithmic scaling (for parameters like Q, R)

class SpaceStrategy(Enum):
    BASE = auto()
    SURGICAL = auto()
    SHOTGUN = auto()
    KAMIKAZE = auto() # Tambahkan ini

class ParameterType(Enum):
    THRESHOLD = auto()
    NOISE = auto()
    TEMPORAL = auto()
    STRUCTURAL = auto()

@dataclass
class SearchResult:
    params: Dict[str, Any]
    label: str
    space_hash: str = ""

@dataclass
class ParameterDimension:
    name: str
    values: List[Any]
    param_type: ParameterType

class QuantumParameterSpace:
    """Industrial parameter space generator synchronized with SignalGenerator."""
    def __init__(self, name: str, dimensions: List[ParameterDimension], strategy: SpaceStrategy):
        self.name = name
        self.dimensions = dimensions
        self.strategy = strategy

    @classmethod
    def base_qr_hunt(cls) -> 'QuantumParameterSpace':
        dimensions = [
            ParameterDimension("entry_threshold", [0.3], ParameterType.THRESHOLD),
            ParameterDimension("exit_threshold", [0.0], ParameterType.THRESHOLD),
            ParameterDimension("stop_loss", [999999.0], ParameterType.THRESHOLD),
            ParameterDimension("volatility_window", [120], ParameterType.TEMPORAL),
            ParameterDimension("use_intercept", [True], ParameterType.STRUCTURAL),
            ParameterDimension("warmup_days", [1], ParameterType.TEMPORAL),

            ParameterDimension("process_noise", np.logspace(-7, -3, 10).tolist(), ParameterType.NOISE),
            ParameterDimension("observation_noise", np.logspace(-4, -1, 10).tolist(), ParameterType.NOISE)
            ]
        return cls("qr_hunt", dimensions, SpaceStrategy.SURGICAL)

    @classmethod
    def surgical_grid(cls) -> 'QuantumParameterSpace':
        dimensions = [
            ParameterDimension("entry_threshold", [1.8, 2.0, 2.2], ParameterType.THRESHOLD),
            ParameterDimension("exit_threshold", [0.5, 0.8], ParameterType.THRESHOLD),
            ParameterDimension("process_noise", [1e-6, 1e-5], ParameterType.NOISE),
            ParameterDimension("observation_noise", [1e-4, 1e-3], ParameterType.NOISE),
            ParameterDimension("use_intercept", [True], ParameterType.STRUCTURAL),
            ParameterDimension("warmup_days", [1], ParameterType.TEMPORAL)
        ]
        return cls("surgical", dimensions, SpaceStrategy.SURGICAL)

    @classmethod
    def dirty_shotgun(cls) -> 'QuantumParameterSpace':
        dimensions = [
            ParameterDimension("entry_threshold", np.round(np.arange(1.5, 3.5, 0.5), 2).tolist(), ParameterType.THRESHOLD),
            ParameterDimension("volatility_window", [1440], ParameterType.TEMPORAL),
            ParameterDimension("stop_loss", [10.0], ParameterType.THRESHOLD),
            ParameterDimension("exit_threshold", [0.0], ParameterType.THRESHOLD),
            ParameterDimension("process_noise", np.logspace(-12, -8, 5).tolist(), ParameterType.NOISE),
            ParameterDimension("observation_noise", np.logspace(-4, -1, 4).tolist(), ParameterType.NOISE),
            ParameterDimension("use_intercept", [True], ParameterType.STRUCTURAL),
            ParameterDimension("warmup_days", [7], ParameterType.TEMPORAL)
        ]
        return cls("shotgun", dimensions, SpaceStrategy.SHOTGUN)

    @classmethod
    def kamikaze_mode(cls) -> 'QuantumParameterSpace':
        """FORCED TRADE MODE: Memastikan pipeline dan dashboard menerima data."""
        dimensions = [
            ParameterDimension("entry_threshold", [0.5, 0.8], ParameterType.THRESHOLD),
            ParameterDimension("exit_threshold", [0.0], ParameterType.THRESHOLD),
            ParameterDimension("process_noise", [1e-9], ParameterType.NOISE),
            ParameterDimension("observation_noise", [1e-2], ParameterType.NOISE),
            ParameterDimension("use_intercept", [True], ParameterType.STRUCTURAL),
            ParameterDimension("warmup_days", [30], ParameterType.TEMPORAL)
        ]
        return cls("kamikaze", dimensions, SpaceStrategy.KAMIKAZE)

    def generate(self, num_trials: int = 500) -> Generator[Result[SearchResult, str], None, None]:
        space_hash = self._compute_hash()
        keys = [d.name for d in self.dimensions]
        if self.strategy in [SpaceStrategy.SURGICAL, SpaceStrategy.KAMIKAZE]:
            vals = [d.values for d in self.dimensions]
            for combo in itertools.product(*vals):
                params = dict(zip(keys, combo))
                if params.get("entry_threshold", 0) <= params.get("exit_threshold", 0):
                    continue
                label = f"Q{params.get('process_noise', 0):.0e}_R{params.get('observation_noise', 0):.0}_INT{params.get('use_intercept', True)}"
                yield Ok(SearchResult(params=params, label=label, space_hash=space_hash))

        elif self.strategy == SpaceStrategy.SHOTGUN:
            for i in range(num_trials):
                params = {}
                for d in self.dimensions:
                    min_val = min(d.values)
                    max_val = max(d.values)

                    if d.param_type == ParameterType.NOISE:
                        log_min = math.log(min_val)
                        log_max = math.log(max_val)
                        params[d.name] = math.exp(random.uniform(log_min, log_max))

                    elif d.param_type == ParameterType.THRESHOLD:
                        params[d.name] = random.uniform(min_val, max_val)

                    else:
                        params[d.name] = random.choice(d.values)

                if params.get("entry_threshold", 0) <= params.get("exit_threshold", 0):
                    continue
                label = f"SGN_{i}_Q{params['process_noise']:.0e}_R{params['observation_noise']:.0e}"
                yield Ok(SearchResult(params=params, label=label, space_hash=space_hash))


    def _compute_hash(self) -> str:
        data_str = json.dumps([(d.name, d.values) for d in self.dimensions], sort_keys=True)
        return hashlib.md5(data_str.encode()).hexdigest()[:12]

def get_parameter_space(name: str) -> Result[QuantumParameterSpace, str]:
    if name == "surgical":
        return Ok(QuantumParameterSpace.surgical_grid())
    if name == "shotgun": 
        return Ok(QuantumParameterSpace.dirty_shotgun())
    if name == "kamikaze": 
        return Ok(QuantumParameterSpace.kamikaze_mode())
    if name == "qr_hunt":
        return Ok(QuantumParameterSpace.base_qr_hunt())
    return Err(f"Space not found: {name}")


# --- LEGACY COMPATIBILITY ADAPTER (The Bridge) ---
class ParameterSpace:
    """
    Compatibility layer for legacy modules expecting 'ParameterSpace'.
    Redirects calls to the new QuantumParameterSpace engine.
    """
    @staticmethod
    def surgical_grid():
        for res in QuantumParameterSpace.surgical_grid().generate():
            if res.is_ok(): 
                search_result = cast(SearchResult, res.unwrap())
                yield search_result.params
    @staticmethod
    def dirty_shotgun():
        for res in QuantumParameterSpace.dirty_shotgun().generate():
            if res.is_ok():
                search_result = cast(SearchResult, res.unwrap())
                yield search_result.params
