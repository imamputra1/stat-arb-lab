"""
SIGNAL GENERATOR (THE ENGINE) - V13.4 STABLE
Location: core/signals/generator.py
Focus: Industrial-grade state machine, high-speed batching, and live telemetry.
"""

import polars as pl
import numpy as np
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, replace
from enum import Enum, auto
from datetime import datetime, timezone
import logging

# Core Shared & Signal Components Integration
from core.shared import Result, Ok, Err, PerformanceMonitor
from .types import SignalSide, SignalAction, SignalEvent, SignalMetadata
from .base_signal import BaseStrategy, StrategyProtocol
from .filters import SignalValidator, PositionFilter

logger = logging.getLogger("Orca.Generator")

# ============================================================================
# DATA MODELS
# ============================================================================

class GeneratorState(Enum):
    """Status internal mesin generator."""
    IDLE = auto()
    INITIALIZING = auto()
    PROCESSING_BATCH = auto()
    PROCESSING_LIVE = auto()
    ERROR = auto()
    SHUTTING_DOWN = auto()

@dataclass(frozen=True)
class GeneratorConfig:
    """Konfigurasi imutabel untuk orkestrasi sinyal."""
    enable_validation: bool = True
    max_batch_size: int = 1_000_000
    enable_caching: bool = True
    cache_ttl_seconds: int = 300
    cooldown_minutes: int = 5 

@dataclass
class PositionState:
    """Pelacakan status posisi aktif dan P&L."""
    side: SignalSide = SignalSide.NEUTRAL
    entry_price: Optional[float] = None
    entry_time: Optional[datetime] = None
    realized_pnl: float = 0.0

    def update(self, side: SignalSide, price: float, ts: datetime) -> None:
        """Update status posisi secara atomik."""
        self.side = side
        self.entry_price = price
        self.entry_time = ts

# ============================================================================
# THE GENERATOR ENGINE
# ============================================================================

class SignalGenerator:
    
    def __init__(
        self, 
        strategy: BaseStrategy,
        config: Optional[GeneratorConfig] = None,
        validator: Optional[SignalValidator] = None,
        filters: Optional[List[PositionFilter]] = None
    ):
        self.strategy = strategy
        self.config = config or GeneratorConfig()
        self.validator = validator or SignalValidator()
        self.filters = filters or []
        
        # State & Telemetry
        self._pos_state = PositionState()
        self._gen_state = GeneratorState.IDLE
        self._monitor = PerformanceMonitor()
        self._batch_cache: Dict[str, pl.DataFrame] = {}

    # ==========================================================================
    # RESEARCH DIMENSION: BATCH PROCESSING
    # ==========================================================================

    def process_batch(self, df: pl.DataFrame) -> Result[pl.DataFrame, str]:
        """
        Memproses data masif dengan sinkronisasi State Machine.
        """
        self._gen_state = GeneratorState.PROCESSING_BATCH
        start_ts = datetime.now(timezone.utc)
        
        try:
            # 1. Delegasi Kebijakan ke Strategi
            res = self.strategy.generate_signals(df)
            if res.is_err(): return res
            
            raw_df = res.unwrap()
            
            # 2. State Machine Loop (Numpy Optimized)
            z_scores = raw_df["z_score"].to_numpy()
            n = len(z_scores)
            
            # Get Strategy Policy Thresholds (Fallback mechanism)
            # Menggunakan getattr berantai untuk kompatibilitas properti vs config object
            entry = getattr(self.strategy, 'entry_threshold', getattr(self.strategy.cfg, 'entry', 2.0))
            exit_val = getattr(self.strategy, 'exit_threshold', getattr(self.strategy.cfg, 'exit', 0.5))
            
            positions = np.zeros(n, dtype=np.int8)
            actions = np.zeros(n, dtype=np.int8)
            
            current_side = SignalSide.NEUTRAL
            
            for i in range(n):
                z = z_scores[i]
                action = SignalAction.HOLD
                
                if current_side == SignalSide.NEUTRAL:
                    if z < -entry:
                        current_side = SignalSide.LONG
                        action = SignalAction.OPEN
                    elif z > entry:
                        current_side = SignalSide.SHORT
                        action = SignalAction.OPEN
                
                elif current_side == SignalSide.LONG:
                    if z > -exit_val:
                        current_side = SignalSide.NEUTRAL
                        action = SignalAction.CLOSE
                
                elif current_side == SignalSide.SHORT:
                    if z < exit_val:
                        current_side = SignalSide.NEUTRAL
                        action = SignalAction.CLOSE
                
                positions[i] = current_side.value
                actions[i] = action.value

            # 3. Final Assembly
            final_df = raw_df.with_columns([
                pl.Series("position", positions).cast(pl.Int8),
                pl.Series("action", actions).cast(pl.Int8)
            ])
            
            duration = (datetime.now(timezone.utc) - start_ts).total_seconds() * 1000
            logger.info(f"Batch completed: {n} rows in {duration:.2f}ms")
            
            self._gen_state = GeneratorState.IDLE
            return Ok(final_df)
            
        except Exception as e:
            self._gen_state = GeneratorState.ERROR
            return Err(f"Generator Batch Failure: {str(e)}")

    # ==========================================================================
    # LIVE DIMENSION: STREAM PROCESSING
    # ==========================================================================

    def process_live(self, observation: Dict[str, Any]) -> Result[SignalEvent, str]:
        """
        Evaluasi real-time dengan proteksi transisi state.
        """
        self._gen_state = GeneratorState.PROCESSING_LIVE
        
        # 1. Evaluasi Strategi (Policy)
        res = self.strategy.evaluate_state(observation)
        if res.is_err(): return res
        
        proposed = res.unwrap()
        
        # 2. Logic Machine (Mechanism)
        actual_action = SignalAction.HOLD
        
        if self._pos_state.side == SignalSide.NEUTRAL:
            if proposed.action == SignalAction.OPEN:
                self._pos_state.update(proposed.side, observation.get('price', 0), proposed.timestamp)
                actual_action = SignalAction.OPEN
        
        elif self._pos_state.side.is_directional:
            if proposed.action == SignalAction.CLOSE:
                self._pos_state.side = SignalSide.NEUTRAL
                actual_action = SignalAction.CLOSE
        
        # 3. Metadata Handling (Immutable Update)
        # Ambil metadata dari strategi atau buat baru jika None
        base_meta = proposed.metadata or SignalMetadata(
            strategy_name=self.strategy.name,
            strategy_version=self.strategy.version
        )
        
        # Buat salinan dict extra dan update
        new_extra = base_meta.extra.copy() if base_meta.extra else {}
        new_extra.update({
            "engine_ver": "v13.0",
            "is_live": True
        })
        
        # Gunakan replace() untuk membuat objek SignalMetadata baru
        final_meta = replace(base_meta, extra=new_extra)

        # 4. Construct Signal Event
        event = SignalEvent(
            timestamp=proposed.timestamp,
            side=self._pos_state.side,
            action=actual_action,
            strength=proposed.strength,
            metadata=final_meta
        )
        
        self._gen_state = GeneratorState.IDLE
        return Ok(event)

# ============================================================================
# FACTORY & FALLBACKS
# ============================================================================

class GeneratorFactory:
    """Pabrik pembuatan generator dengan jaminan stabilitas."""
    
    @staticmethod
    def create(strategy: BaseStrategy, config: Optional[GeneratorConfig] = None) -> Result[SignalGenerator, str]:
        try:
            # Verifikasi Kontrak Protokol
            if not isinstance(strategy, StrategyProtocol):
                return Err("Strategi tidak mematuhi StrategyProtocol.")
                
            return Ok(SignalGenerator(strategy, config))
        except Exception as e:
            return Err(f"Factory Failure: {str(e)}")

def create_signal_generator(name: str, params: Dict[str, Any]) -> SignalGenerator:
    """Entry point utama dengan fallback ke KalmanMR."""
    from .factory import get_signal_strategy 
    strategy = get_signal_strategy(name, params)
    
    res = GeneratorFactory.create(strategy)
    return res.unwrap() if res.is_ok() else SignalGenerator(strategy)
