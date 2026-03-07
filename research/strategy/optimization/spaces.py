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
from enum import auto, Enum
from pathlib import Path
import math
import random


# --- Core Import ---
from core.shared import Result, Ok, Err
from core.signals.types import SignalConfig
from core.math.kalman import KalmanConfig

# --- PATH INJECTION ---
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent.absolute()
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


# ================================================================
# ====================== ENUM AND DATA ===========================
# ================================================================
class SpaceStrategy(Enum):
    """Strategy for exploring parameter spacec."""
    BASE = auto()      # Fixed set (e.g., QR Hunt)
    SURGICAL = auto()  # Small grid around known good values
    SHOTGUN = auto()   # Random sampling within bounds
    KAMIKAZE = auto()  # Forced trade mode (wide ranges)

class ParameterType(Enum):
    """Type of parameter for scaling and interpretation"""
    THRESHOLD = auto()
    NOISE = auto()
    TEMPORAL = auto()
    STRUCTURAL = auto()

@dataclass
class ParameterDimension:
    """
    Defines one dimension of the search space.
    name must match exactly a field in SignalConfig or KalmanConfig.
    values can be a list of discrete values or a (min, max) tuple for continuous.
    """
    name: str                     # Parameter name (e.g., "entry_threshold", "process_noise")
    values: List[Any]             # Lower bound
    param_type: ParameterType     # Upper bound
    description: str = ""         # If True, use logarithmic scaling (for parameters like Q, R)

@dataclass
class SearchResult:
    """
    A single parameter combination that passed validation.
    """
    params: Dict[str, Any]      # flat dict with for both configs
    label: str                  # human readable identifier
    space_hash: str = ""        # hash of the space definition


class QuantumParameterSpace:
    """
    Industrial parameter space generator.
    Pure functional composition: each classmethod builds a list of dimensions.
    generate() yields only validated parameter combinations.
    """
    def __init__(self, name: str, dimensions: List[ParameterDimension], strategy: SpaceStrategy):
        self.name = name
        self.dimensions = dimensions
        self.strategy = strategy
        self._space_hash = self._compute_hash()

    # ---------------------------------------------------------
    # -------------------- Factory Methods --------------------
    # ---------------------------------------------------------
    @classmethod
    def base_qr_hunt(cls) -> 'QuantumParameterSpace':
        """
        Focused search on Q and R only.
        All other parameter fixed to sensible default.
        """
        dimensions = [
            ParameterDimension("entry_z_score", [2.0], ParameterType.THRESHOLD,
                               description="Z-score to entry"),
            ParameterDimension("exit_z_score", [0.5], ParameterType.THRESHOLD,
                               description="Z-score to exit"),
            ParameterDimension("stop_loss_z", [4.0], ParameterType.THRESHOLD,
                               description="Z-score for stop loss"),
            ParameterDimension("volatility_window", [120], ParameterType.TEMPORAL,
                               description="Window for rolling volatility"),
            ParameterDimension("warmup_ticks", [100], ParameterType.TEMPORAL,
                               description="Minimum ticks before signals generation"),

            # KalmanConfig Parameter (log scale)
            ParameterDimension("Q", np.logspace(-9, -5, 10).tolist(), ParameterType.NOISE,
                               description="Process noise base"),
            ParameterDimension("R", np.logspace(-6, -2, 10).tolist(), ParameterType.NOISE,
                               description="Measurement noise"),
        ]
        return cls("qr_hunt", dimensions, SpaceStrategy.BASE)

    @classmethod
    def surgical_grid(cls) -> 'QuantumParameterSpace':
        """
        Small grid around proven parameter.
        Use for final tuning after shotgun phase.
        """
        dimensions = [
            ParameterDimension("entry_z_score", [1.8, 2.0, 2.2], ParameterType.THRESHOLD),
            ParameterDimension("exit_z_score", [0.5, 0.8], ParameterType.THRESHOLD),
            ParameterDimension("stop_loss_z", [4.0, 5.0], ParameterType.THRESHOLD),
            ParameterDimension("volatility_window", [4.0, 5.0], ParameterType.TEMPORAL),
            ParameterDimension("warmup_days", [100], ParameterType.TEMPORAL),
            ParameterDimension("Q", [1e-8, 1e-7, 1e-6], ParameterType.NOISE),
            ParameterDimension("R", [1e-8, 1e-4, 1e-3], ParameterType.NOISE),
        ]
        return cls("surgical", dimensions, SpaceStrategy.SURGICAL)

    @classmethod
    def dirty_shotgun(cls) -> 'QuantumParameterSpace':
        """
        Wide random sampling to discover promising regions.
        Updated with MFT-friendly parameter ranges:
        - entry_z_score: discrete [2.0, 2.5, 3.0]
        - exit_z_score: discrete [0.5, 1.0]
        - stop_loss_z: discrete [4.0, 5.0]
        - Q: discrete [1e-5, 1e-6, 1e-7]
        - R: discrete [1e-2, 1e-3, 1e-4]
        - volatility_window: discrete [60, 120, 240]
        - warmup_ticks: unchanged [100, 200]
        """
        dimensions = [
            # Thresholds (now discrete to focus on proven values)
            ParameterDimension("entry_z_score", [1.5, 2.0, 2.5], ParameterType.THRESHOLD),
            ParameterDimension("exit_z_score", [0.0, 0.5], ParameterType.THRESHOLD),
            ParameterDimension("stop_loss_z", [5.0, 6.0], ParameterType.THRESHOLD),

            # Windows (discrete steps)
            ParameterDimension("volatility_window", [120, 240, 480], ParameterType.TEMPORAL),
            ParameterDimension("warmup_ticks", [100, 200], ParameterType.TEMPORAL),

            # Noise (discrete values, no longer continuous)
            ParameterDimension("Q", [1e-10, 1e-11, 1e-12], ParameterType.NOISE),
            ParameterDimension("R", [5e-6, 1e-6, 5e-7], ParameterType.NOISE)
        ]
        return cls("shotgun", dimensions, SpaceStrategy.SHOTGUN)
    
    @classmethod
    def kamikaze_mode(cls) -> 'QuantumParameterSpace':
        """Extreme ranges – for stress testing and forcing pipeline to produce signals."""
        dimensions = [
            ParameterDimension("entry_z_score", [0.5, 0.8, 1.5], ParameterType.THRESHOLD),
            ParameterDimension("exit_z_score", [0.0], ParameterType.THRESHOLD),
            ParameterDimension("stop_loss_z", [10.0], ParameterType.THRESHOLD),
            ParameterDimension("volatility_window", [1440], ParameterType.THRESHOLD),
            ParameterDimension("observation_noise", [500], ParameterType.THRESHOLD),

            ParameterDimension("Q", [1e-12, 1e-8, 1e-5], ParameterType.NOISE),
            ParameterDimension("R", [1e-4, 1e-2, 1e0], ParameterType.NOISE)
        ]
        return cls("kamikaze", dimensions, SpaceStrategy.KAMIKAZE)

    # ---------------------------------------------------------
    # ----------- Core Generator with Guard Clauses -----------
    # ---------------------------------------------------------
    def generate(self, num_trials: int = 500) -> Generator[Result[SearchResult, str], None, None]:
        """
        Yield Validated parameter combination.
        for grid strategy, yield all combos.
        for shotgun, yield up to num_trials random samples
        """
        # Guard: dimensions must not be empty
        if not self.dimensions:
            yield Err("ParameterSpace has zero dimensions")
            return
        if self.strategy in (SpaceStrategy.BASE, SpaceStrategy.SURGICAL, SpaceStrategy.KAMIKAZE):
            yield from self._generate_grid()
        elif self.strategy == SpaceStrategy.SHOTGUN:
            yield from self._generate_random(num_trials)
        else:
            yield Err(f"Unknown Strategy: {self.strategy}")

    def _generate_grid(self) -> Generator[Result[SearchResult, str], None, None]:
        """Cartesian product of all discrete dimensions."""
        # Extract discrete value lists (all dimensions must be discrete for grid)
        keys = [dim.name for dim in self.dimensions]
        value_lists = [dim.values for dim in self.dimensions]
        
        for combo in itertools.product(*value_lists):
            params_candidate = dict(zip(keys, combo))

            validation = self._validate_parameters(params_candidate)
            if validation.is_err():
                # Skip invalid combos silently (or log if needed)
                continue

            label = self._create_label(params_candidate)

            yield Ok(SearchResult(
                params=params_candidate,
                label=label,
                space_hash=self._space_hash
            ))

    def _generate_random(self, num_trials: int) -> Generator[Result[SearchResult, str], None, None]:
        """Random sampling from continuous/discrete mixed space"""
        for i in range(num_trials):
            params_candidate = {}

            for dim in self.dimensions:
                if dim.param_type == ParameterType.NOISE:
                    # Log uniform sampling
                    min_val, max_val = dim.values[0], dim.values[1]
                    log_min = math.log(min_val)
                    log_max = math.log(max_val)
                    params_candidate[dim.name] = math.exp(random.uniform(log_min, log_max))

                elif dim.param_type in (ParameterType.THRESHOLD, ParameterType.TEMPORAL):
                    # Continuous uniform if values is [min, max], else discrete choice
                    if len(dim.values) == 2 and isinstance(dim.values[0], (int, float)):
                        low, high = dim.values
                        params_candidate[dim.name] = random.uniform(low, high)
                    else:
                        params_candidate[dim.name] = random.choice(dim.values)

                else: # STRUCTURAL (Boolean, etc)
                    params_candidate[dim.name] = random.choice(dim.values)

            # Guard clause: validate
            validation = self._validate_parameters(params_candidate)
            if validation.is_err():
                continue

            label = f"SHOT_{i}" + self._create_label(params_candidate, short=True)
            yield Ok(SearchResult(
                params=params_candidate,
                label=label,
                space_hash=self._space_hash
            ))


    # ---------------------------------------------------------
    # ---------------- Validation Helpers ---------------------
    # ---------------------------------------------------------
    def _validate_parameters(self, params: Dict[str, Any]) -> Result[bool, str]:
        """
        Pass parameters through SignalConfig.from_dict().
        If it return Ok, parameters satisfy business rules (entry > exit, etc.)
        """
        # 1. Ensure required keys have sane default if missing
        # (through our dimensions should provide them)

        if "name" not in params:
            params_with_name = params.copy()
            params_with_name["name"] = "Optimized"

        else:
            params_with_name = params

        # 2. Validate with SignalConfig
        config_result = SignalConfig.from_dict(params_with_name)
        if config_result.is_err():
            error_row = config_result.unwrap_err()
            error_msg = str(error_row) if error_row is not None else "Unknown Validate Error"
            return Err(error_msg)

        # 3.Optional: Also validate KalmanConfig if Q and R present
        # (KalmanConfig has its own __post_init__ validation)
        if "Q" in params and "R" in params:
            try:
                # We don't need the instance, just check if construction fails
                _ = KalmanConfig(
                    R=params['R'],
                    Q=params['Q'],
                    initial_value=0.0 # dummy
                )
            except Exception as e:
                return Err(f"KalmanConfig validation failed: {str(e)}")

        return Ok(True)

    def _create_label(self, params: Dict[str, Any], short: bool = False) -> str:
        """Create a readable label from key parameters."""
        if short:
            # Short label for shotgun
            parts = []
            if "Q" in params:
                parts.append(f"Q{params['Q']:.0e}")
            if "R" in params:
                parts.append(f"R{params['R']:.0e}")
            if "entry_z_score" in params:
                parts.append(f"E{params['entry_z_score']:.1f}")

            return "_".join(parts)

        # Full label
        label_parts = []
        for key in ["entry_z_score", "exit_z_score", "stop_loss_z", "Q", "R", "volatility_window"]:
            if key in params:
                val = params[key]
                if isinstance(val, float):
                    label_parts.append(f"{key}={val:.3g}")
                else:
                    label_parts.append(f"{key}={val}")

        return "_".join(label_parts)

    def _compute_hash(self) -> str:
        """Unique hash for this space definition"""
        data = [(dim.name, dim.values) for dim in self.dimensions]
        data_str = json.dumps(data, sort_keys=True, default=str)
        return hashlib.md5(data_str.encode()).hexdigest()[:12]

# ============================================================================
# ============================== PUBLIC API ==================================
# ============================================================================
def get_parameter_space(name: str) -> Result[QuantumParameterSpace, str]:
    """
    Factory functional to retrieve a predefined parameter space.
    Names: 'qr_hunt', 'surgical', 'shotgun', 'kamikaze'
    """
    spaces = {
        "qr_hunt": QuantumParameterSpace.base_qr_hunt,
        "surgical": QuantumParameterSpace.surgical_grid,
        "shotgun": QuantumParameterSpace.dirty_shotgun,
        "kamikaze": QuantumParameterSpace.kamikaze_mode
    }

    if name not in spaces:
        return Err(f"Parameter Space '{name}' not found. Available: {list(spaces.keys())}")

    try:
        return Ok(spaces[name]())
    except Exception as e:
        return Err(f"Failed to create space: '{name}': {str(e)}")

# ============================================================================
# ================= LEGACY COMPATIBILITY ADAPTER =============================
# ============================================================================
class ParameterSpace:
    """
    Compatibility layer for legacy modules expecting the old ParameterSpace interface.
    Yields raw dicts (not Result) for backward compatibility.
    """
    @staticmethod
    def surgical_grid():
        space = QuantumParameterSpace.surgical_grid()
        for res in space.generate():
            if res.is_ok():
                search_result = res.unwrap()
                # Type Guard to satisfy linter(linter_result is not None)
                assert search_result is not None, "SearchResult should not be None"
                yield search_result.params

    @staticmethod
    def dirty_shotgun(num_trials=500):
        space =  QuantumParameterSpace.dirty_shotgun()
        for res in space.generate(num_trials):
            if res.is_ok():
                search_result = res.unwrap()
                assert search_result is not None, "SearchResult should not be None"
                yield search_result.params
