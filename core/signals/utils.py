"""
SIGNAL UTILITIES (THE MECHANICS) - V16.0 QUANTUM
Location: core/signals/utils.py
Focus: Ultra-high performance vectorized mechanics with hardware-level optimization.
Paradigm: Result-Oriented, Numba-Accelerated, Lazy-Evaluation Friendly.
Author: ADHD-Dyslexic Systems Architect (Refined for Industrial Throughput)
"""

import polars as pl
import numpy as np
import numba as nb
import logging
from typing import Dict, Any, Union
from datetime import datetime
from contextlib import contextmanager

# Core Shared & Signal Types Integration
from core.shared import Result, Ok, Err

# ============================================================================
# PERFORMANCE KERNELS (Numba Accelerated)
# ============================================================================

@nb.njit(parallel=True, fastmath=True, cache=True)
def _kernel_calculate_drawdown(cum_pnl: np.ndarray) -> np.ndarray:
    """Numba kernel untuk menghitung drawdown dengan kompleksitas O(n)."""
    n = len(cum_pnl)
    drawdown = np.empty(n, dtype=np.float64)
    running_max = -np.inf
    
    for i in nb.prange(n):
        if cum_pnl[i] > running_max:
            running_max = cum_pnl[i]
        drawdown[i] = running_max - cum_pnl[i]
    return drawdown

@nb.njit(parallel=True, cache=True)
def _kernel_apply_signal_decay(signals: np.ndarray, halflife: float) -> np.ndarray:
    """Menerapkan peluruhan eksponensial pada kekuatan sinyal."""
    decay_constant = np.log(2) / halflife
    n = len(signals)
    output = np.empty(n, dtype=np.float64)
    for i in nb.prange(n):
        output[i] = signals[i] * np.exp(-decay_constant * i)
    return output

# ============================================================================
# COMPUTATIONAL ENGINE
# ============================================================================

class SignalMechanics:
    """
    Kumpulan operasi mekanik murni (stateless) untuk pemrosesan sinyal.
    Dioptimalkan untuk throughput Ryzen 5 melalui Polars Lazy & Numba.
    """

    @staticmethod
    def calculate_vectorized_pnl(
        df: Union[pl.DataFrame, pl.LazyFrame],
        price_col: str = "target_price",
        pos_col: str = "position",
        lag: int = 1
    ) -> Result[Union[pl.DataFrame, pl.LazyFrame], str]:
        """
        Menghitung PnL secara vektor dengan dukungan LazyFrame.
        Formula: Position[t-lag] * (Price[t] - Price[t-1])
        """
        try:
            # Menggunakan Polars Expression untuk efisiensi maksimal
            expr = [
                (pl.col(pos_col).shift(lag).fill_null(0) * pl.col(price_col).diff()).alias("pnl_step")
            ]
            
            res_df = df.with_columns(expr).with_columns([
                pl.col("pnl_step").cum_sum().alias("cumulative_pnl")
            ])
            
            return Ok(res_df)
        except Exception as e:
            return Err(f"PnL Calculation Failed: {str(e)}")

    @staticmethod
    def detect_transitions(
        df: Union[pl.DataFrame, pl.LazyFrame], 
        col: str = "position"
    ) -> Result[Union[pl.DataFrame, pl.LazyFrame], str]:
        """Mendeteksi perubahan status posisi secara vectorized."""
        try:
            return Ok(df.with_columns([
                (pl.col(col).diff().fill_null(0) != 0).alias("is_transition")
            ]))
        except Exception as e:
            return Err(f"Transition Detection Failed: {str(e)}")

    @staticmethod
    def calculate_risk_metrics(pnl_series: pl.Series) -> Result[Dict[str, float], str]:
        """Menghitung metrik risiko (Sharpe, MaxDD) menggunakan Kernel Numba."""
        try:
            if pnl_series.len() == 0: return Err("Series kosong")
            
            cum_pnl = pnl_series.cum_sum().to_numpy()
            dd_array = _kernel_calculate_drawdown(cum_pnl)
            
            returns = pnl_series.to_numpy()
            sharpe = (np.mean(returns) / np.std(returns)) * np.sqrt(252 * 1440) if np.std(returns) > 0 else 0
            
            return Ok({
                "max_drawdown": float(np.max(dd_array)),
                "sharpe_ratio": float(sharpe),
                "total_return": float(cum_pnl[-1])
            })
        except Exception as e:
            return Err(f"Risk Metrics Calculation Failed: {str(e)}")

    @staticmethod
    def validate_temporal_integrity(
        df: pl.DataFrame, 
        expected_freq_min: int = 1
    ) -> Result[Dict[str, Any], str]:
        """Memvalidasi kontinuitas waktu dan deteksi celah (gaps)."""
        if "timestamp" not in df.columns:
            return Err("Timestamp missing")
            
        diffs = df["timestamp"].diff().dt.total_minutes().drop_nulls()
        gaps = diffs.filter(diffs > expected_freq_min)
        
        return Ok({
            "is_valid": gaps.len() == 0,
            "gap_count": int(gaps.len()),
            "max_gap_min": float(gaps.max()) if gaps.len() > 0 else 0.0
        })

# ============================================================================
# PERFORMANCE UTILITIES (Hardware Specific)
# ============================================================================

@contextmanager
def performance_gate(op_name: str):
    """Context manager untuk monitoring latensi level mikro."""
    start = datetime.now()
    try:
        yield
    finally:
        duration = (datetime.now() - start).total_seconds() * 1000
        if duration > 100: # Log jika lebih dari 100ms
            logging.getLogger("Orca.Perf").warning(f"Slow Op: {op_name} took {duration:.2f}ms")

def optimize_numba_runtime():
    """Konfigurasi otomatis Numba untuk optimalisasi Ryzen 5."""
    try:
        nb.config.THREADING_LAYER = 'tbb'
        num_cores = 4 # Ryzen 5 sweet spot for parallel tasks
        nb.set_num_threads(num_cores)
        return Ok(f"Numba optimized for {num_cores} threads.")
    except Exception as e:
        return Err(str(e))

# ============================================================================
# INITIALIZATION
# ============================================================================

# Auto-configure pada saat load
optimize_numba_runtime()

# Global Mechanic Instance
mechanics = SignalMechanics()
