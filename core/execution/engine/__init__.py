"""
EXECUTION ENGINE MODULE
Location: core/execution/engine/__init__.py
Desc: Entry point utama. Merakit Mechanics, Emulator, dan Wrapper menjadi satu sistem.
"""

from typing import Optional, Dict, Any
from dataclasses import dataclass
from .adapter import EmulatorAdapter
# 1. Import Core Components
from .types import (
    Trade,
    Rejection,
    RejectionCode,
    TradeStatus,
    ExecutionResult,
    is_trade,
    is_rejection
)

from .emulator import (
    ExchangeEmulator,
    MarketContext,
    MarketRegime,
    MarketRegimeDetector,
    CircuitBreaker,
    ExecutionEngine
)

# 2. Import Mechanics Factory
from core.execution.mechanics.factory import (
    create_mechanics_suite,
    MechanicsSuite
)

# ====================== CONFIGURATION ======================

@dataclass
class EngineConfig:
    """
    Konfigurasi Global untuk Execution Engine.
    """
    # Mechanics Config
    base_latency_ms: float = 10.0
    base_fee_bps: float = 5.0
    
    # Emulator Config
    enable_circuit_breaker: bool = True
    enable_regime_detection: bool = True
    
    # Limits
    max_orders_per_sec: int = 50
    max_rejections_per_sec: int = 10

# ====================== FACTORY FUNCTIONS ======================

def create_execution_engine(config: Optional[EngineConfig] = None) -> ExecutionEngine:
    """
    Factory Utama: Merakit Robot Eksekusi.
    """
    cfg = config or EngineConfig()
    
    # [FIX] Jangan passing Dict mentah ke factory. 
    # Biarkan factory menggunakan Default Config object miliknya sendiri
    # untuk menghindari AttributeError: 'dict' object has no attribute 'model_type'
    mechanics = create_mechanics_suite() 
    
    # NOTE: Jika ingin mengubah latency/fee secara dinamis, 
    # kita harus mengimport Config Class spesifik (VolatilitySlippageConfig, dll)
    # Tapi untuk tahap ini, Default Mechanics sudah cukup untuk menjalankan Test.

    # 2. Build Emulator (The Kill House)
    emulator = ExchangeEmulator(mechanics=mechanics)
    
    # 3. Apply Configuration to Emulator Components
    if cfg.enable_circuit_breaker:
        emulator.circuit_breaker.max_orders_per_sec = cfg.max_orders_per_sec
        emulator.circuit_breaker.max_rejections_per_sec = cfg.max_rejections_per_sec
    else:
        emulator.circuit_breaker.max_orders_per_sec = 999999
    
    # 4. Wrap & Return
    return ExecutionEngine(emulator=emulator, config=cfg)

# ====================== EXPORTS ======================

__all__ = [
    'Trade',
    'Rejection', 
    'RejectionCode',
    'TradeStatus',
    'ExecutionResult',
    'is_trade',
    'is_rejection',
    'ExchangeEmulator',
    'ExecutionEngine',
    'MarketContext',
    'MarketRegime',
    'EngineConfig',
    'create_execution_engine',
    'EmulatorAdapter'
]
