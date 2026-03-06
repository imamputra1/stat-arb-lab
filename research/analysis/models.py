# research/analysis/models.py
"""
SHARED DATA MODELS FOR ANALYSIS MODULE
Location: research/analysis/models.py
Focus: Centralized definitions of data classes used across analysis submodules.
"""

from dataclasses import dataclass, field
from typing import Dict, Any


@dataclass(frozen=True)
class PerformanceMetrics:
    """
    Immutable container for all calculated performance metrics.
    Used as the output of the analytics pipeline.
    """
    # Core metrics
    total_return: float
    annualized_return: float
    volatility: float
    sharpe_ratio: float
    sortino_ratio: float
    calmar_ratio: float
    max_drawdown: float
    max_drawdown_duration: int  # in days

    # Trade statistics
    total_trades: int
    winning_trades: int
    losing_trades: int
    win_rate: float
    avg_win: float
    avg_loss: float
    profit_factor: float
    expectancy: float

    # Risk metrics
    value_at_risk_95: float
    conditional_var_95: float
    ulcer_index: float

    # Time metrics
    start_date: str
    end_date: str
    total_days: int

    # Additional metadata
    config_snapshot: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to flat dictionary for serialization."""
        return {
            "total_return": self.total_return,
            "annualized_return": self.annualized_return,
            "volatility": self.volatility,
            "sharpe_ratio": self.sharpe_ratio,
            "sortino_ratio": self.sortino_ratio,
            "calmar_ratio": self.calmar_ratio,
            "max_drawdown": self.max_drawdown,
            "max_drawdown_duration": self.max_drawdown_duration,
            "total_trades": self.total_trades,
            "winning_trades": self.winning_trades,
            "losing_trades": self.losing_trades,
            "win_rate": self.win_rate,
            "avg_win": self.avg_win,
            "avg_loss": self.avg_loss,
            "profit_factor": self.profit_factor,
            "expectancy": self.expectancy,
            "value_at_risk_95": self.value_at_risk_95,
            "conditional_var_95": self.conditional_var_95,
            "ulcer_index": self.ulcer_index,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "total_days": self.total_days,
            **self.config_snapshot
        }
