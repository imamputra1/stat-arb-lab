"""
INDUSTRIAL PARAMETER SPACES (THE EXTREME AMMO DEPOT) - V4.6
Location: research/strategy/optimization/spaces.py
Focus: Extreme boundary testing for Kalman Filter Overfitting.
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

# --- PATH INJECTION & SHARED SYNC ---
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent.absolute()
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from research.shared import Result, Ok, Err

class SpaceStrategy(Enum):
    SURGICAL = auto()
    SHOTGUN = auto()
    ADAPTIVE = auto()
    CUSTOM = auto()

class ParameterType(Enum):
    THRESHOLD = auto()
    NOISE = auto()
    TEMPORAL = auto()
    STRUCTURAL = auto()

@dataclass
class ParameterDimension:
    name: str
    values: List[Any]
    param_type: ParameterType
    distribution: str = "linear"
    priority: int = 1

@dataclass
class SearchResult:
    params: Dict[str, Any]
    label: str
    space_hash: str = ""
    priority_score: float = 1.0

class QuantumParameterSpace:
    """
    Industrial-strength parameter space generator.
    Optimized for Extreme Boundary Testing (Noisy vs Frozen Beta).
    """
    def __init__(self, name: str, dimensions: List[ParameterDimension], strategy: SpaceStrategy):
        self.name = name
        self.dimensions = dimensions
        self.strategy = strategy

    @classmethod
    def surgical_grid(cls) -> 'QuantumParameterSpace':
        """Precision search around theoretical optimum."""
        dimensions = [
            ParameterDimension("entry_threshold", [1.8, 2.0, 2.2], ParameterType.THRESHOLD, priority=9),
            ParameterDimension("exit_threshold", [0.5, 0.8], ParameterType.THRESHOLD, priority=8),
            ParameterDimension("process_noise", [1e-6, 1e-5], ParameterType.NOISE, distribution="log"),
            ParameterDimension("observation_noise", [1e-4, 1e-3], ParameterType.NOISE, distribution="log"),
            ParameterDimension("use_intercept", [True], ParameterType.STRUCTURAL),
            ParameterDimension("warmup_days", [30], ParameterType.TEMPORAL)
        ]
        return cls("surgical", dimensions, SpaceStrategy.SURGICAL)

    @classmethod
    def dirty_shotgun(cls) -> 'QuantumParameterSpace':
        """
        EXTREME BOUNDARY TESTING: Menghancurkan Overfitting.
        Logic: Perluas Log-Range Q & R, Tambahkan Intercept Toggle, Hapus Warmup Variable.
        """
        dimensions = [
            # 1. Expand Entry Range: Menangkap ekor distribusi yang lebih gemuk
            ParameterDimension("entry_threshold", 
                              np.round(np.arange(1.0, 4.0, 0.2), 2).tolist(), 
                              ParameterType.THRESHOLD, priority=10),
            
            ParameterDimension("exit_threshold", [0.5], ParameterType.THRESHOLD),
            
            # 2. Extreme Q (Process Noise): Dari Frozen Beta (1e-9) ke Hyper-Adaptive (1e-3)
            ParameterDimension("process_noise", 
                              np.logspace(-9, -3, 7).tolist(), 
                              ParameterType.NOISE, distribution="log", priority=9),
            
            # 3. Extreme R (Obs Noise): Dari Strict (1e-5) ke Noisy Market (1.0)
            ParameterDimension("observation_noise", 
                              np.logspace(-5, 0, 6).tolist(), 
                              ParameterType.NOISE, distribution="log", priority=9),
            
            # 4. SENJATA RAHASIA: Intercept Switch (alpha=0 vs alpha adaptive)
            ParameterDimension("use_intercept", [True, False], ParameterType.STRUCTURAL, priority=10),
            
            # 5. Static Warmup: Menghemat komputasi untuk parameter yang berdampak
            ParameterDimension("warmup_days", [30], ParameterType.TEMPORAL)
        ]
        return cls("extreme_shotgun", dimensions, SpaceStrategy.SHOTGUN)

    def generate(self) -> Generator[Result[SearchResult, str], None, None]:
        """Quantum generation with Hard Constraint guards."""
        space_hash = self._compute_hash()
        dim_values = [dim.values for dim in self.dimensions]
        keys = [dim.name for dim in self.dimensions]
        
        for combo in itertools.product(*dim_values):
            params = dict(zip(keys, combo))
            
            # Constraint: Entry must be strictly > Exit
            if params.get("entry_threshold", 0) <= params.get("exit_threshold", 0):
                continue

            try:
                # Generate specific label for extreme diagnostics
                label = f"Q{params['process_noise']:.0e}_R{params['observation_noise']:.0e}_INT{params['use_intercept']}"
                yield Ok(SearchResult(params=params, label=label, space_hash=space_hash))
            except Exception as e:
                yield Err(f"Space Gen Failure: {str(e)}")

    def _compute_hash(self) -> str:
        data_str = json.dumps([(d.name, d.values) for d in self.dimensions], sort_keys=True)
        return hashlib.md5(data_str.encode()).hexdigest()[:12]

# --- LEGACY COMPATIBILITY ---
class ParameterSpace:
    @staticmethod
    def surgical_grid():
        for res in QuantumParameterSpace.surgical_grid().generate():
            if res.is_ok(): yield res.unwrap().params

    @staticmethod
    def dirty_shotgun():
        for res in QuantumParameterSpace.dirty_shotgun().generate():
            if res.is_ok(): yield res.unwrap().params

def get_parameter_space(name: str) -> Result[QuantumParameterSpace, str]:
    if name == "surgical": return Ok(QuantumParameterSpace.surgical_grid())
    if name == "shotgun": return Ok(QuantumParameterSpace.dirty_shotgun())
    return Err(f"Space not found: {name}")
