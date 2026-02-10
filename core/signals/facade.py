"""
SIGNAL GENERATOR FACADE
Location: core/signals/facade.py
Desc: Unified interface for Signal Generation. 
      Orchestrates Strategy creation (Factory) and Output Validation (Filters).
"""

from typing import Dict, Any, Optional, Union
import pandas as pd
import polars as pl

# Core Shared
from core.shared.result import Result, Ok, Err

# Signal Components
from .factory import create_strategy, validate_config
from .filters import FilterFactory, CompositeFilter
from .types import SignalEvent
from .base_signal import BaseStrategy

class SignalGeneratorFacade:
    """
    [FACADE] Pintu gerbang utama subsistem Sinyal.
    Menyembunyikan kompleksitas Factory dan Filter dari dunia luar.
    """
    
    def __init__(self):
        self._strategy: Optional[BaseStrategy] = None
        self._filters: Optional[CompositeFilter] = None
        self._is_initialized = False

    def initialize(self, config: Dict[str, Any]) -> Result[bool, str]:
        """
        Menyiapkan Strategy dan Filters berdasarkan konfigurasi.
        """
        # 1. Validasi Config (Fail Fast)
        val_res = validate_config(config)
        if val_res.is_err():
            return Err(f"Config Validation Failed: {val_res.unwrap_err()}")
        
        valid_config = val_res.unwrap()

        # 2. Spawn Strategy via Factory (The Worker)
        strat_res = create_strategy(valid_config)
        if strat_res.is_err():
            return Err(f"Strategy Creation Failed: {strat_res.unwrap_err()}")
        
        self._strategy = strat_res.unwrap()

        # 3. Setup Filters via Factory (The Guard)
        # Default menggunakan 'Standard Guard' (Stale check, Exposure check)
        # Di masa depan bisa inject config untuk custom filter chain
        self._filters = FilterFactory.create_standard_guard()
        
        self._is_initialized = True
        return Ok(True)

    def generate_signals(self, data: Any) -> Result[Union[pd.DataFrame, SignalEvent], str]:
        """
        Single Entry Point untuk menghasilkan sinyal.
        Otomatis mendeteksi mode: Batch (Backtest) atau Stream (Live).
        """
        if not self._is_initialized:
            return Err("SignalFacade not initialized. Call initialize() first.")

        # A. MODE BATCH (DataFrame) -> Research/Backtest
        if isinstance(data, (pd.DataFrame, pl.DataFrame)):
            return self._process_batch(data)
            
        # B. MODE STREAM (Dict/Observation) -> Live Trading
        elif isinstance(data, dict):
            return self._process_stream(data)
            
        else:
            return Err(f"Unsupported input data type: {type(data)}")

    # ================== INTERNAL PROCESSORS ==================

    def _process_batch(self, df: Union[pd.DataFrame, pl.DataFrame]) -> Result[pd.DataFrame, str]:
        """Alur Batch: Strategy -> Bulk Filter"""
        # 1. Strategy Generate
        # Strategy diharapkan mengembalikan DataFrame yang sudah ada kolom 'signal'
        raw_signals_res = self._strategy.generate_signals(df)
        if raw_signals_res.is_err():
            return raw_signals_res
        
        raw_signals = raw_signals_res.unwrap()

        # 2. Filter Apply (Batch)
        # Filter akan membuang/menandai sinyal yang tidak valid dalam batch
        filtered_res = self._filters.apply(raw_signals)
        
        return filtered_res

    def _process_stream(self, observation: Dict[str, Any]) -> Result[SignalEvent, str]:
        """Alur Stream: Strategy -> Single Filter"""
        # 1. Strategy Evaluate
        # Mengubah data pasar mentah menjadi SignalEvent kandidat
        signal_res = self._strategy.evaluate_state(observation)
        if signal_res.is_err():
            return signal_res
        
        signal = signal_res.unwrap()

        # 2. Filter Check (The Gatekeeper)
        # Cek integritas sinyal (misal: data terlalu tua, volatilitas ekstrem)
        check_res = self._filters.apply_single(signal)
        if check_res.is_err():
            # Signal DITOLAK oleh Filter
            return Err(f"Signal Rejected by Guard: {check_res.unwrap_err()}")

        # Signal VALID
        return Ok(signal)

    @property
    def strategy_name(self) -> str:
        return self._strategy.name if self._strategy else "Uninitialized"
    
    @property
    def strategy_state(self) -> str:
        return self._strategy.get_state().name if self._strategy else "UNKNOWN"
