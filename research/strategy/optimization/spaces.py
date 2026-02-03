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
from typing import Dict, List, Any, Generator
from dataclasses import dataclass
from enum import Enum, auto
from pathlib import Path

# --- PATH INJECTION ---
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent.absolute()
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from research.shared import Result, Ok, Err

class SpaceStrategy(Enum):
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
    def surgical_grid(cls) -> 'QuantumParameterSpace':
        dimensions = [
            ParameterDimension("entry_threshold", [1.8, 2.0, 2.2], ParameterType.THRESHOLD),
            ParameterDimension("exit_threshold", [0.5, 0.8], ParameterType.THRESHOLD),
            ParameterDimension("process_noise", [1e-6, 1e-5], ParameterType.NOISE),
            ParameterDimension("observation_noise", [1e-4, 1e-3], ParameterType.NOISE),
            ParameterDimension("use_intercept", [True], ParameterType.STRUCTURAL),
            ParameterDimension("warmup_days", [30], ParameterType.TEMPORAL)
        ]
        return cls("surgical", dimensions, SpaceStrategy.SURGICAL)

    @classmethod
    def dirty_shotgun(cls) -> 'QuantumParameterSpace':
        dimensions = [
            ParameterDimension("entry_threshold", np.round(np.arange(1.5, 3.5, 0.5), 2).tolist(), ParameterType.THRESHOLD),
            ParameterDimension("exit_threshold", [0.5], ParameterType.THRESHOLD),
            ParameterDimension("process_noise", np.logspace(-8, -4, 5).tolist(), ParameterType.NOISE),
            ParameterDimension("observation_noise", np.logspace(-4, -1, 4).tolist(), ParameterType.NOISE),
            ParameterDimension("use_intercept", [True, False], ParameterType.STRUCTURAL),
            ParameterDimension("warmup_days", [30], ParameterType.TEMPORAL)
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

    def generate(self) -> Generator[Result[SearchResult, str], None, None]:
        space_hash = self._compute_hash()
        keys = [d.name for d in self.dimensions]
        vals = [d.values for d in self.dimensions]
        for combo in itertools.product(*vals):
            params = dict(zip(keys, combo))
            if params.get("entry_threshold", 0) <= params.get("exit_threshold", 0): continue
            label = f"Q{params['process_noise']:.0e}_R{params['observation_noise']:.0e}_INT{params['use_intercept']}"
            yield Ok(SearchResult(params=params, label=label, space_hash=space_hash))

    def _compute_hash(self) -> str:
        data_str = json.dumps([(d.name, d.values) for d in self.dimensions], sort_keys=True)
        return hashlib.md5(data_str.encode()).hexdigest()[:12]

def get_parameter_space(name: str) -> Result[QuantumParameterSpace, str]:
    if name == "surgical": return Ok(QuantumParameterSpace.surgical_grid())
    if name == "shotgun": return Ok(QuantumParameterSpace.dirty_shotgun())
    if name == "kamikaze": return Ok(QuantumParameterSpace.kamikaze_mode())
    return Err(f"Space not found: {name}")
