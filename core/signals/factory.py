"""
STRATEGY FACTORY (THE GATEWAY) - V12.0 QUANTUM
Location: core/signals/factory.py
Focus: Industrial-grade dynamic loading, lifecycle management, and composition.
Paradigm: Result-Oriented, Type-Safe, Plugin-Architecture.
Author: ADHD-Dyslexic Systems Architect (Refined for High-Performance Quant Lab)
"""

import inspect
import importlib.util
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum, auto
from pathlib import Path
from typing import Any, Dict, Optional, Type

# Core Shared & Signal Component Integration
from core.shared import Result, Ok, Err
from .base_signal import BaseStrategy, LifecycleProtocol, StrategyOrchestrator
from .strategies.kalman_mr import KalmanMRStrategy

# ============================================================================
# LOGGING CONFIGURATION
# ============================================================================

logger = logging.getLogger("Orca.Factory")

# ============================================================================
# ENUMERATIONS & METADATA
# ============================================================================

class StrategyType(Enum):
    """Klasifikasi perilaku strategi untuk pemfilteran di War Room."""
    MEAN_REVERSION = auto()
    TREND_FOLLOWING = auto()
    BREAKOUT = auto()
    ARBITRAGE = auto()
    ML_ENSEMBLE = auto()
    HYBRID = auto()

class StrategyLifecycle(Enum):
    """State mesin untuk manajemen instance."""
    REGISTERED = auto()
    INITIALIZING = auto()
    READY = auto()
    ACTIVE = auto()
    PAUSED = auto()
    ERROR = auto()

@dataclass(frozen=True)
class StrategyDescriptor:
    """Metadata imutabel untuk registrasi strategi."""
    name: str
    strategy_class: Type[BaseStrategy]
    version: str
    category: StrategyType = StrategyType.HYBRID
    parameters_schema: Dict[str, Any] = field(default_factory=dict)
    author: str = "orca.core"
    
    def get_default_params(self) -> Dict[str, Any]:
        """Ekstraksi parameter default via refleksi tanda tangan __init__."""
        sig = inspect.signature(self.strategy_class.__init__)
        return {
            k: v.default for k, v in sig.parameters.items() 
            if v.default is not inspect.Parameter.empty and k != 'self'
        }

@dataclass
class ManagedInstance:
    """Container untuk instance aktif dengan pelacakan vitalitas."""
    id: str
    descriptor: StrategyDescriptor
    instance: BaseStrategy
    state: StrategyLifecycle = StrategyLifecycle.REGISTERED
    last_call: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    metrics: Dict[str, Any] = field(default_factory=dict)

# ============================================================================
# STRATEGY REGISTRY (The Source of Truth)
# ============================================================================

class StrategyRegistry:
    """Registri pusat untuk penemuan (discovery) dan pendaftaran strategi."""
    _descriptors: Dict[str, StrategyDescriptor] = {}
    _instances: Dict[str, ManagedInstance] = {}
    
    @classmethod
    def register(cls, descriptor: StrategyDescriptor) -> Result[bool, str]:
        """Mendaftarkan strategi baru ke dalam sistem."""
        if descriptor.name in cls._descriptors:
            return Err(f"Strategy '{descriptor.name}' sudah terdaftar.")
        
        # Validasi kepatuhan Protokol
        if not issubclass(descriptor.strategy_class, BaseStrategy):
            return Err(f"Kelas {descriptor.strategy_class.__name__} tidak mematuhi BaseStrategy.")
            
        cls._descriptors[descriptor.name] = descriptor
        logger.info(f"Registered: {descriptor.name} v{descriptor.version}")
        return Ok(True)

    @classmethod
    def discover(cls, search_path: Path) -> Result[int, str]:
        """Menjelajahi direktori untuk mencari strategi (Plugin Support)."""
        if not search_path.exists():
            return Err(f"Discovery path tidak ditemukan: {search_path}")
            
        count = 0
        for py_file in search_path.glob("*.py"):
            if py_file.name.startswith("_"): continue
            
            try:
                # Dynamic Module Loading
                spec = importlib.util.spec_from_file_location(py_file.stem, py_file)
                if not spec or not spec.loader: continue
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                
                for name, obj in inspect.getmembers(module, inspect.isclass):
                    if issubclass(obj, BaseStrategy) and obj is not BaseStrategy:
                        cls.register(StrategyDescriptor(
                            name=name.lower().replace("strategy", ""),
                            strategy_class=obj,
                            version=getattr(obj, "__version__", "1.0.0")
                        ))
                        count += 1
            except Exception as e:
                logger.error(f"Gagal memuat plugin {py_file.name}: {str(e)}")
        
        return Ok(count)

# ============================================================================
# THE QUANTUM FACTORY (The Fabricator)
# ============================================================================

class StrategyFactory:
    """Pabrik cerdas untuk pembuatan, caching, dan orkestrasi strategi."""
    
    def __init__(self, use_cache: bool = True):
        self._cache: Dict[str, ManagedInstance] = {}
        self._use_cache = use_cache

    def create(self, name: str, **params: Any) -> Result[BaseStrategy, str]:
        """
        Membuat instance strategi dengan pemfilteran parameter otomatis 
        dan dukungan Lifecycle (Warm-up).
        """
        # 1. Resolve Descriptor
        desc = StrategyRegistry._descriptors.get(name)
        if not desc: return Err(f"Strategi '{name}' tidak ditemukan.")
        
        # 2. Parameter Integrity (Reflection)
        sig = inspect.signature(desc.strategy_class.__init__)
        valid_keys = sig.parameters.keys()
        filtered = {k: v for k, v in params.items() if k in valid_keys and k != 'self'}
        
        try:
            # 3. Cache Check
            instance_key = f"{name}_{hash(frozenset(filtered.items()))}"
            if self._use_cache and instance_key in self._cache:
                managed = self._cache[instance_key]
                managed.last_call = datetime.now(timezone.utc)
                return Ok(managed.instance)
            
            # 4. Instantiation
            instance = desc.strategy_class(**filtered)
            
            # 5. Lifecycle Management (Warm-up)
            if isinstance(instance, LifecycleProtocol):
                logger.info(f"Warming up strategy: {name}")
                # Logika warm_up bisa dipicu di sini jika data tersedia
                
            # 6. Managed Registry
            managed = ManagedInstance(id=instance_key, descriptor=desc, instance=instance)
            managed.state = StrategyLifecycle.READY
            
            if self._use_cache:
                self._cache[instance_key] = managed
                StrategyRegistry._instances[instance_key] = managed
                
            return Ok(instance)
            
        except Exception as e:
            logger.error(f"Critical Factory Failure: {str(e)}")
            return Err(f"Instansiasi gagal: {str(e)}")

# ============================================================================
# COMPOSITE & ORCHESTRATION (Advanced Logic)
# ============================================================================

    def orchestrate(self, base_name: str, **params: Any) -> Result[StrategyOrchestrator, str]:
        """
        Membangun Orkestrator untuk strategi dengan dukungan Pipeline 
        Pre/Post Processing.
        """
        res = self.create(base_name, **params)
        if res.is_err(): return Err(res.error)
        
        return Ok(StrategyOrchestrator(res.unwrap()))

# ============================================================================
# GLOBAL ACCESSOR & FALLBACK (The Blind Commander Link)
# ============================================================================

_FACTORY: Optional[StrategyFactory] = None

def get_signal_strategy(name: str, params: Dict[str, Any]) -> BaseStrategy:
    """
    Entry point utama untuk Pipeline. 
    Mengimplementasikan Fallback bertingkat agar sistem tidak pernah 'Buta'.
    """
    global _FACTORY
    if not _FACTORY:
        _FACTORY = StrategyFactory()
        # Initial Built-in Registration
        StrategyRegistry.register(StrategyDescriptor("kalman_mr", KalmanMRStrategy, "1.0.0", StrategyType.MEAN_REVERSION))

    # TINGKAT 1: Percobaan Utama
    result = _FACTORY.create(name, **params)
    if result.is_ok(): return result.unwrap()
    
    # TINGKAT 2: Fallback ke Default KalmanMR
    logger.warning(f"Strategy {name} failed: {result.error}. Falling back to KalmanMR.")
    fallback = _FACTORY.create("kalman_mr")
    
    return fallback.unwrap() if fallback.is_ok() else KalmanMRStrategy()
