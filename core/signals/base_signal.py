"""
BASE SIGNAL INTERFACE (THE CONSTITUTION) - V12.2 QUANTUM
Location: core/signals/base_signal.py
Focus: Full structural protocols and industrial base class with Orchestrator.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Protocol, runtime_checkable, List, TypeVar, Generic, Callable
from dataclasses import dataclass
import polars as pl
import logging
from core.shared import Result, Ok, Err
from .types import SignalEvent

# ====================== TYPE VARIABLES ======================
T = TypeVar('T')

# ====================== DATA MODELS ======================

@dataclass(frozen=True)
class DataRequirement:
    """Spesifikasi kolom data wajib dengan validasi Polars."""
    column_name: str
    data_type: Any
    required: bool = True

@dataclass(frozen=True)
class ValidationResult:
    """Hasil validasi data yang dipahami oleh Strategy V11."""
    is_valid: bool
    error_summary: str = ""

# ====================== STRUCTURAL PROTOCOLS ======================

@runtime_checkable
class StrategyProtocol(Protocol):
    """Kontrak struktural utama untuk semua strategi."""
    name: str
    version: str
    
    def generate_signals(self, df: pl.DataFrame) -> Result[pl.DataFrame, str]: ...
    def evaluate_state(self, observation: Dict[str, Any]) -> Result[Any, str]: ...

@runtime_checkable
class LifecycleProtocol(Protocol):
    """Manajemen state untuk strategi yang memiliki memori."""
    def warm_up(self, historical_data: pl.DataFrame) -> Result[bool, str]: ...
    def reset_state(self) -> None: ...

# ====================== THE BASE CONTRACT ======================

class BaseStrategy(ABC, Generic[T]):
    """Immutable Contract for all Orca Strategies."""
    
    def __init__(self, name: str, version: str):
        self._name = name
        self._version = version
        self.logger = logging.getLogger(f"Orca.Strategy.{name}")

    @property
    @abstractmethod
    def data_requirements(self) -> Dict[str, DataRequirement]:
        """Mendefinisikan kolom wajib untuk strategi."""
        pass

    @abstractmethod
    def generate_signals(self, df: pl.DataFrame) -> Result[pl.DataFrame, str]:
        """Logika batch processing."""
        pass

    @abstractmethod
    def evaluate_state(self, observation: Dict[str, Any]) -> Result[Any, str]:
        """Logika real-time processing."""
        pass

    def validate_data(self, df: pl.DataFrame) -> ValidationResult:
        """Validasi integritas DataFrame."""
        missing = [k for k, v in self.data_requirements.items() if v.required and k not in df.columns]
        if missing:
            return ValidationResult(False, f"Missing required columns: {missing}")
        return ValidationResult(True)

    def preprocess_observation(self, observation: Dict[str, Any]) -> Result[Dict[str, Any], str]:
        """Standarisasi observasi sebelum diproses."""
        if not observation:
            return Err("Empty observation")
        return Ok(observation)

    @property
    def name(self) -> str: return self._name

    @property
    def version(self) -> str: return self._version

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}({self.name} v{self.version})>"

# ============================================================================
# COMPOSITION BUILDER (High-Order Strategy)
# ============================================================================

class StrategyOrchestrator:
    """
    Membangun strategi kompleks melalui komposisi fungsional.
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
