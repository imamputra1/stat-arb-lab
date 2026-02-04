"""
INDUSTRIAL PERFORMANCE EVALUATOR (THE NEURO-SCOREKEEPER) - V6.2 SYNC
Location: research/strategy/optimization/objective.py
Focus: Multi-dimensional adaptive scoring with Numba acceleration and Result Pattern.
"""
import numpy as np
import polars as pl
import sys
from pathlib import Path
from typing import Dict, Any
from dataclasses import dataclass
from enum import Enum, auto
import numba as nb

# --- PATH INJECTION & SHARED SYNC ---
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent.absolute()
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.shared import Result, Ok, Err

# --- QUANTUM ENUMS ---
class ScoringStrategy(Enum):
    CONSERVATIVE = auto()  # High penalty on Drawdown
    AGGRESSIVE = auto()    # Maximize Returns
    BALANCED = auto()      # Equal weight

@dataclass
class PerformanceMetrics:
    sharpe_ratio: float = 0.0
    sortino_ratio: float = 0.0
    calmar_ratio: float = 0.0
    total_return: float = 0.0
    max_drawdown: float = 0.0
    total_trades: int = 0
    win_rate: float = 0.0
    profit_factor: float = 0.0
    monotonicity: float = 0.0
    stability_ratio: float = 0.0
    composite_score: float = 0.0
    smart_score: float = 0.0 # Standard Sync for Node D
    calculation_hash: str = ""

# --- QUANTUM SCORING ENGINE ---
class QuantumScoreKeeper:
    """
    Industrial-strength evaluator optimized for Ryzen 5 multi-core execution.
    Synced with 'cumulative_returns' and 'position' columns from pipeline.py.
    """
    def __init__(self, strategy: ScoringStrategy = ScoringStrategy.BALANCED):
        self.strategy = strategy
        self.cache_dir = PROJECT_ROOT / "research" / "strategy" / "optimization" / ".score_cache"
        self.cache_dir.mkdir(exist_ok=True, parents=True)

    def evaluate(self, df: pl.DataFrame) -> Result[PerformanceMetrics, str]:
        """Comprehensive evaluation yielding Result pattern."""
        try:
            # 1. Validation Logic
            required = ["cumulative_returns", "position"]
            missing = [c for c in required if c not in df.columns]
            if missing: return Err(f"Missing required columns: {missing}")
            if df.height < 10: return Err("Data density too low for scoring")

            # 2. Extract & Convert (Aero-13 Memory Optimization)
            equity = df["cumulative_returns"].to_numpy().astype(np.float64)
            pos = df["position"].to_numpy().astype(np.int32)
            
            # 3. Core Metric Calculation
            risk_adj = self._calc_risk_adjusted(equity)
            trades = self._calc_trade_metrics(equity, pos)
            risk = self._calc_risk_metrics(equity)
            stability = self._calc_stability_metrics(equity)
            
            # 4. Neural-Inspired Composite Score
            metrics = PerformanceMetrics(
                sharpe_ratio=risk_adj["sharpe"],
                sortino_ratio=risk_adj["sortino"],
                calmar_ratio=risk_adj["calmar"],
                total_return=float(equity[-1]),
                max_drawdown=risk["max_dd"],
                total_trades=trades["count"],
                win_rate=trades["win_rate"],
                profit_factor=trades["pf"],
                monotonicity=stability["mono"],
                stability_ratio=stability["stab"],
                smart_score=self._calc_smart_score(risk_adj["sharpe"], trades["count"], trades["win_rate"])
            )
            
            metrics.composite_score = self._weight_score(metrics)
            return Ok(metrics)

        except Exception as e:
            return Err(f"Quantum Score failure: {str(e)}")

    def _calc_risk_adjusted(self, equity: np.ndarray) -> Dict[str, float]:
        returns = np.diff(equity)
        std = np.std(returns)
        if std < 1e-12: return {"sharpe": -1.0, "sortino": -1.0, "calmar": -1.0}
        
        sharpe = np.sqrt(365) * (np.mean(returns) / std)
        
        neg_ret = returns[returns < 0]
        dd_std = np.std(neg_ret) if len(neg_ret) > 0 else 1e-12
        sortino = np.sqrt(365) * (np.mean(returns) / dd_std)
        
        max_dd = self._fast_max_dd(equity)
        calmar = equity[-1] / abs(max_dd) if abs(max_dd) > 1e-12 else 0.0
        
        return {"sharpe": sharpe, "sortino": sortino, "calmar": calmar}

    def _calc_trade_metrics(self, equity: np.ndarray, pos: np.ndarray) -> Dict[str, Any]:
        changes = np.diff(pos) != 0
        if not np.any(changes): return {"count": 0, "win_rate": 0.0, "pf": 0.0}
        
        # Calculate returns per trade
        trade_points = np.where(changes)[0] + 1
        trade_pnl = np.diff(equity[trade_points]) if len(trade_points) > 1 else np.array([])
        
        wins = trade_pnl[trade_pnl > 0]
        losses = trade_pnl[trade_pnl < 0]
        
        win_rate = len(wins) / len(trade_pnl) if len(trade_pnl) > 0 else 0.0
        pf = np.sum(wins) / abs(np.sum(losses)) if np.sum(losses) != 0 else (np.inf if np.sum(wins) > 0 else 0.0)
        
        return {"count": len(trade_pnl), "win_rate": win_rate, "pf": pf}

    def _calc_risk_metrics(self, equity: np.ndarray) -> Dict[str, float]:
        return {"max_dd": self._fast_max_dd(equity)}

    def _calc_stability_metrics(self, equity: np.ndarray) -> Dict[str, float]:
        increasing = np.diff(equity) > 0
        mono = np.mean(increasing)
        
        n = len(equity)
        q_size = n // 4
        q_rets = [equity[min((i+1)*q_size, n-1)] - equity[i*q_size] for i in range(4)]
        std_q = np.std(q_rets)
        stab = 1.0 - (std_q / (abs(np.mean(q_rets)) + 1e-12)) if std_q > 0 else 1.0
        
        return {"mono": float(mono), "stab": float(max(0, min(1, stab)))}

    def _calc_smart_score(self, sharpe: float, trades: int, win_rate: float) -> float:
        """Standard Node D scoring sync."""
        if trades == 0: return -10.0
        return float(sharpe * np.log10(trades + 1) * win_rate)

    def _weight_score(self, m: PerformanceMetrics) -> float:
        """Dynamic weighting based on selected strategy."""
        if self.strategy == ScoringStrategy.CONSERVATIVE:
            return (m.sharpe_ratio * 0.4) + (m.win_rate * 0.2) - (abs(m.max_drawdown) * 0.4)
        return (m.sharpe_ratio * 0.5) + (m.win_rate * 0.3) + (m.stability_ratio * 0.2)

    @staticmethod
    @nb.jit(nopython=True)
    def _fast_max_dd(equity: np.ndarray) -> float:
        """Numba-accelerated MDD calculation for Ryzen 5 performance."""
        if len(equity) == 0: return 0.0
        peak = equity[0]
        mdd = 0.0
        for val in equity:
            if val > peak: peak = val
            dd = (val - peak) / peak if peak != 0 else 0.0
            if dd < mdd: mdd = dd
        return mdd

# --- CONVENIENCE HANDLERS ---
def calculate_smart_score(df: pl.DataFrame) -> Result[float, str]:
    keeper = QuantumScoreKeeper()
    res = keeper.evaluate(df)
    return Ok(res.unwrap().smart_score) if res.is_ok() else Err(res.error)

def get_performance_summary(df: pl.DataFrame) -> Result[Dict[str, Any], str]:
    keeper = QuantumScoreKeeper()
    res = keeper.evaluate(df)
    if res.is_err(): return Err(res.error)
    m = res.unwrap()
    return Ok({
        "smart_score": m.smart_score,
        "total_pnl": m.total_return,
        "max_drawdown": m.max_drawdown,
        "trades": m.total_trades,
        "win_rate": m.win_rate,
        "sharpe": m.sharpe_ratio
    })
