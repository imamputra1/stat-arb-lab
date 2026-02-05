"""
Abstract Base Strategy - Standard Socket Interface
Protocol for all trading strategies in ORCA system
"""

from abc import ABC, abstractmethod
from typing import Protocol, runtime_checkable, Generic, TypeVar, Any, List
import pandas as pd


from ..shared.result import Result, Ok, Err
from ..shared.performance import PerformanceMonitor
from .types import SignalEvent, MarketState

T = TypeVar('T', bound='BaseStrategy')

@runtime_checkable
class StrategyProtocol(Protocol):
    """Structural protocol for all strategies - no inheritance required"""
    
    def generate_signals(self, df: pd.DataFrame) -> Result[pd.DataFrame, str]:
        """Research path - batch processing"""
        ...
    
    def evaluate_state(self, obs: dict) -> Result[SignalEvent, str]:
        """Live path - real-time evaluation"""
        ...
    
    def get_state(self) -> MarketState:
        """Get current market state"""
        ...
    
    def reset(self) -> None:
        """Reset strategy state"""
        ...

class BaseStrategy(ABC, Generic[T]):
    """
    Abstract Base Strategy implementing Socket Standard.
    Dual-path architecture for Research (batch) and Live (real-time).
    """
    
    def __init__(self, name: str, version: str = "1.0.0"):
        self.name = name
        self.version = version
        self._state: MarketState = MarketState.IDLE
        self.monitor = PerformanceMonitor(history_size=1000)
        
    # ========== ABSTRACT INTERFACE ==========
    
    @abstractmethod
    def _initialize_filter(self, initial_value: float) -> Result[Any, str]:
        """Initialize the underlying mathematical filter"""
        pass
    
    @abstractmethod
    def _process_observation(self, spread: float, timestamp: int) -> Result[SignalEvent, str]:
        """Process single observation through mathematical kernel"""
        pass
    
    @abstractmethod
    def _extract_spread(self, data: dict | pd.Series) -> Result[float, str]:
        """Extract spread value from different data formats"""
        pass
    
    # ========== DUAL-PATH IMPLEMENTATIONS ==========
    
    def generate_signals(self, df: pd.DataFrame) -> Result[pd.DataFrame, str]:
        """
        Research Path: Batch processing of historical data.
        
        Args:
            df: Silver Lake DataFrame with required columns including spread
            
        Returns:
            DataFrame with original data + signal columns
        """
        pass
    
    def evaluate_state(self, obs: dict) -> Result[SignalEvent, str]:
        """
        Live Path: Real-time evaluation of single observation.
        
        Args:
            obs: Real-time observation dictionary with market data
            
        Returns:
            SignalEvent with trading decision
        """
        pass


    # ==========================================================================
    # STATE MANAGEMENT (GENERIC)
    # ==========================================================================
    
    def get_state(self) -> MarketState:
        """Get current market state"""
        return self._state

    def reset(self) -> Result[None, str]:
        """
        [GENERIC RESET]
        Hanya reset state umum. Logic spesifik (seperti filter) 
        harus di-handle oleh override method di child class.
        """
        try:
            self._state = MarketState.IDLE
            # Reset monitor, bukan dict manual
            self.monitor = PerformanceMonitor(history_size=1000) 
            return Ok(None)
        except Exception as e:
            return Err(f"Base reset failed: {str(e)}")

    def get_performance_metrics(self) -> dict[str, Any]:
        """Expose metrics dari PerformanceMonitor"""
        # Kita ambil summary statistik dari monitor
        return {
            "avg_latency": self.monitor.get_avg_latency("signal_generation"),
            "total_ops": self.monitor.total_operations
        }

    # ==========================================================================
    # UTILITY METHODS (REPLACEMENT)
    # ==========================================================================
    
    # HAPUS _validate_dataframe yang lama.
    # GANTIKAN dengan validate_data yang dinamis ini:
    
    def validate_data(self, df: pd.DataFrame, required_cols: List[str] = None) -> Result[bool, str]:
        """
        [GENERIC VALIDATION]
        Cek kelengkapan data tanpa hardcode nama kolom.
        """
        if df is None or df.empty:
            return Err(f"Strategy {self.name}: Input DataFrame is empty or None")
            
        # Selalu cek timestamp karena itu mandatory buat semua time-series strategy
        if 'timestamp' not in df.columns:
             return Err(f"Strategy {self.name}: Missing 'timestamp' column")
            
        # Cek kolom dinamis sesuai request anak (Child Class)
        if required_cols:
            missing = [col for col in required_cols if col not in df.columns]
            if missing:
                return Err(f"Strategy {self.name}: Missing required columns: {missing}")
                
        return Ok(True)
    
# ========== FACTORY PATTERN ==========

class StrategyFactory:
    """Factory for creating strategy instances"""
    
    _registry: dict[str, type[BaseStrategy]] = {}
    
    @classmethod
    def register(cls, name: str, strategy_class: type[BaseStrategy]) -> None:
        """Register a strategy class"""
        cls._registry[name] = strategy_class
    
    @classmethod
    def create(cls, name: str, **kwargs) -> Result[BaseStrategy, str]:
        """Create a strategy instance"""
        if name not in cls._registry:
            return Err(f"Strategy '{name}' not registered")
        
        try:
            strategy_class = cls._registry[name]
            instance = strategy_class(**kwargs)
            return Ok(instance)
        except Exception as e:
            return Err(f"Failed to create strategy '{name}': {str(e)}")
    
    @classmethod
    def list_available(cls) -> list[str]:
        """List all available strategies"""
        return list(cls._registry.keys())
