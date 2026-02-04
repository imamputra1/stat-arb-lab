"""
BASE SIGNAL INTERFACE (THE CONTRACT) - V10.0 QUANTUM
Location: core/signals/base_signal.py
Focus: Defining structural contracts for all trading strategies.
Paradigm: Result-Oriented, Structural Composition, Performance Optimized.
Author: ADHD-Dyslexic Systems Architect (Refined for Industrial Scale)
"""

from abc import ABC, abstractmethod
from typing import (
    Dict, Any, Optional, Protocol, runtime_checkable, 
    TypeVar, Generic, Callable, final, List
)
from dataclasses import dataclass
import polars as pl
import logging

# Core Shared & Signal Types Integration
from core.shared import Result, Ok, Err
from .types import SignalEvent

# ============================================================================
# TYPE VARIABLES & GENERICS
# ============================================================================

T = TypeVar('T')
E = TypeVar('E')

# ============================================================================
# PROTOCOLS (Structural Architecture)
# ============================================================================

@runtime_checkable
class StrategyProtocol(Protocol):
    """Protokol struktural utama tanpa ketergantungan inheritance."""
    name: str
    version: str
    is_ready: bool
    
    def generate_signals(self, df: pl.DataFrame) -> Result[pl.DataFrame, str]: ...
    def evaluate_state(self, observation: Dict[str, Any], context: Optional[Dict[str, Any]] = None) -> Result[SignalEvent, str]: ...

@runtime_checkable
class LifecycleProtocol(Protocol):
    """Protokol untuk manajemen state model (Warm-up/Cool-down)."""
    def warm_up(self, historical_data: pl.DataFrame) -> Result[bool, str]: ...
    def reset_state(self) -> None: ...

# ============================================================================
# PERFORMANCE-OPTIMIZED DATA VALIDATION
# ============================================================================

@dataclass(frozen=True)
class DataRequirement:
    """Spesifikasi kolom dengan validasi berbasis Polars Expressions."""
    column_name: str
    data_type: Any
    required: bool = True
    min_value: Optional[float] = None
    max_value: Optional[float] = None

    def get_validation_expr(self) -> List[pl.Expr]:
        """Menghasilkan Polars expressions untuk validasi kilat."""
        exprs = []
        if self.min_value is not None:
            exprs.append(pl.col(self.column_name).min() >= self.min_value)
        if self.max_value is not None:
            exprs.append(pl.col(self.column_name).max() <= self.max_value)
        return exprs

# ============================================================================
# ABSTRACT BASE CLASS (The Master Contract)
# ============================================================================

class BaseStrategy(ABC, Generic[T]):
    """
    Pure Trading Policy Interface - THE IMMUTABLE CONTRACT V10.0
    
    Responsibilities:
    - Policy decision making (Logic over Math).
    - Data integrity enforcement.
    - Deterministic signal generation.
    """
    
    def __init__(
        self, 
        name: str = "generic_strategy",
        version: str = "1.0.0",
        **params: Any
    ):
        self._name = name
        self._version = version
        self._params = params
        self._is_initialized = False
        self._requirements: Dict[str, DataRequirement] = {}
        self.logger = logging.getLogger(f"Strategy.{name}")

    # ==========================================================================
    # CORE ABSTRACT METHODS (The Logic Gates)
    # ==========================================================================
    
    @abstractmethod
    def generate_signals(self, df: pl.DataFrame) -> Result[pl.DataFrame, str]:
        """
        [RESEARCH DIMENSION] - Batch Vectorized Decision making.
        Returns DataFrame with 'side' and 'strength' columns.
        """
        pass
    
    @abstractmethod
    def evaluate_state(
        self, 
        observation: Dict[str, Any], 
        context: Optional[Dict[str, Any]] = None
    ) -> Result[SignalEvent, str]:
        """
        [LIVE DIMENSION] - Real-time point-in-time decision.
        """
        pass

    # ==========================================================================
    # INDUSTRIAL INFRASTRUCTURE (Final Methods)
    # ==========================================================================

    @final
    def validate_input(self, df: pl.DataFrame) -> Result[bool, str]:
        """Validasi batch berperforma tinggi menggunakan Polars."""
        missing = [c for c in self.data_requirements if c not in df.columns]
        if missing:
            return Err(f"Missing critical columns: {missing}")
        
        # Eksekusi semua aturan validasi secara paralel di Polars
        validations = []
        for req in self.data_requirements.values():
            validations.extend(req.get_validation_expr())
            
        if validations:
            check = df.select(validations).collect().row(0)
            if not all(check):
                return Err("Data value constraints violation (min/max bounds)")
                
        return Ok(True)

    @property
    def name(self) -> str: return self._name

    @property
    def version(self) -> str: return self._version

    @property
    def is_ready(self) -> bool: return self._is_initialized

    @property
    def data_requirements(self) -> Dict[str, DataRequirement]:
        """Override untuk mendefinisikan kontrak data spesifik."""
        return {
            'timestamp': DataRequirement('timestamp', pl.Datetime),
            'close': DataRequirement('close', pl.Float64, min_value=0.0)
        }

    # ==========================================================================
    # COMPOSITION & CHAINING
    # ==========================================================================

    def __call__(self, data: Any) -> Result[Any, str]:
        """Routing otomatis berdasarkan tipe input."""
        if isinstance(data, pl.DataFrame):
            return self.generate_signals(data)
        elif isinstance(data, dict):
            return self.evaluate_state(data)
        return Err(f"Input type {type(data)} unsupported")

# ============================================================================
# COMPOSITION BUILDER (High-Order Strategy)
# ============================================================================

class StrategyOrchestrator:
    """
    Membangun strategi kompleks melalui komposisi fungsional (Decorator Pattern).
    Mencegah 'Deep Inheritance' yang membingungkan.
    """
    def __init__(self, base: BaseStrategy):
        self.base = base
        self._pre: List[Callable] = []
        self._post: List[Callable] = []

    def with_preprocessing(self, func: Callable) -> 'StrategyOrchestrator':
        self._pre.append(func)
        return self

    def with_postprocessing(self, func: Callable) -> 'StrategyOrchestrator':
        self._post.append(func)
        return self

    def execute_live(self, obs: Dict[str, Any]) -> Result[SignalEvent, str]:
        # 1. Pipeline Pre-processing
        current_obs = obs
        for p in self._pre:
            current_obs = p(current_obs)
            
        # 2. Base Logic Execution
        res = self.base.evaluate_state(current_obs)
        if res.is_err(): return res
        
        # 3. Pipeline Post-processing (Filtering/Sizing)
        signal = res.unwrap()
        for p in self._post:
            signal = p(signal)
            
        return Ok(signal)
