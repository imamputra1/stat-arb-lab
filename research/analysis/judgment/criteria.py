"""
JUDGMENT CRITERIA (THE RULES OF WAR) - V1.0
Location: research/analysis/judgment/criteria.py
Focus: Define evaluation criteria for backtest results.
Each criterion evaluates a PerformanceMetrics object and returns a CriterionResult.
"""

from typing import Dict, Any, List, Union, Protocol
from dataclasses import dataclass, field
from enum import Enum

# Import from pipeline (will be available)
from research.analysis.models import PerformanceMetrics

# Core shared
from core.shared import Result, Ok, Err


# ============================================================================
# ENUMS & DATA CLASSES (THE BLUEPRINTS)
# ============================================================================

class CriterionSeverity(Enum):
    """How severe a criterion violation is."""
    INFO = "info"        # Just for information, does not affect acceptance
    WARNING = "warning"  # Should be reviewed, but may be acceptable
    CRITICAL = "critical"  # Must pass for acceptance


@dataclass(frozen=True)
class CriterionResult:
    """
    Result of evaluating a single criterion.
    passed: whether the criterion is satisfied.
    message: human-readable explanation.
    severity: how important this criterion is.
    metadata: additional details (e.g., actual value, threshold).
    """
    passed: bool
    message: str
    severity: CriterionSeverity = CriterionSeverity.CRITICAL
    metadata: Dict[str, Any] = field(default_factory=dict)


# ============================================================================
# CRITERION INTERFACE (PROTOCOL)
# ============================================================================

class Criterion(Protocol):
    """Protocol that all criteria must implement."""

    def evaluate(self, metrics: PerformanceMetrics) -> Result[CriterionResult, str]:
        """
        Evaluate the metrics against this criterion.
        Returns a CriterionResult or an error.
        """
        ...


# ============================================================================
# CONCRETE CRITERIA (IMPLEMENTATIONS)
# ============================================================================

@dataclass(frozen=True)
class SharpeRatioCriterion:
    """
    Criterion: Sharpe ratio must be above a minimum threshold.
    """
    min_sharpe: float = 1.0
    severity: CriterionSeverity = CriterionSeverity.CRITICAL

    def evaluate(self, metrics: PerformanceMetrics) -> Result[CriterionResult, str]:
        # Guard clause
        if metrics is None:
            return Err("Metrics object is None")

        sharpe = metrics.sharpe_ratio
        passed = sharpe >= self.min_sharpe

        message = (
            f"Sharpe ratio = {sharpe:.3f} (required >= {self.min_sharpe})"
        )
        metadata = {
            "actual": sharpe,
            "threshold": self.min_sharpe
        }

        return Ok(CriterionResult(
            passed=passed,
            message=message,
            severity=self.severity,
            metadata=metadata
        ))


@dataclass(frozen=True)
class MaxDrawdownCriterion:
    """
    Criterion: Maximum drawdown must not exceed a maximum allowed drawdown.
    Note: drawdown is a negative number, so max_allowed_drawdown is also negative (e.g., -0.2 for 20%).
    """
    max_allowed_drawdown: float = -0.25  # e.g., -25%
    severity: CriterionSeverity = CriterionSeverity.CRITICAL

    def evaluate(self, metrics: PerformanceMetrics) -> Result[CriterionResult, str]:
        if metrics is None:
            return Err("Metrics object is None")

        dd = metrics.max_drawdown
        # dd is negative, we want dd >= max_allowed_drawdown (less negative)
        passed = dd >= self.max_allowed_drawdown

        message = (
            f"Max drawdown = {dd:.2%} (allowed >= {self.max_allowed_drawdown:.2%})"
        )
        metadata = {
            "actual": dd,
            "threshold": self.max_allowed_drawdown
        }

        return Ok(CriterionResult(
            passed=passed,
            message=message,
            severity=self.severity,
            metadata=metadata
        ))


@dataclass(frozen=True)
class TotalReturnCriterion:
    """
    Criterion: Total return must be above a minimum.
    """
    min_return: float = 0.0  # e.g., 0% (positive)
    severity: CriterionSeverity = CriterionSeverity.CRITICAL

    def evaluate(self, metrics: PerformanceMetrics) -> Result[CriterionResult, str]:
        if metrics is None:
            return Err("Metrics object is None")

        ret = metrics.total_return
        passed = ret >= self.min_return

        message = (
            f"Total return = {ret:.2%} (required >= {self.min_return:.2%})"
        )
        metadata = {
            "actual": ret,
            "threshold": self.min_return
        }

        return Ok(CriterionResult(
            passed=passed,
            message=message,
            severity=self.severity,
            metadata=metadata
        ))


@dataclass(frozen=True)
class WinRateCriterion:
    """
    Criterion: Win rate must be above a minimum percentage.
    """
    min_win_rate: float = 0.5  # 50%
    severity: CriterionSeverity = CriterionSeverity.WARNING

    def evaluate(self, metrics: PerformanceMetrics) -> Result[CriterionResult, str]:
        if metrics is None:
            return Err("Metrics object is None")

        wr = metrics.win_rate
        passed = wr >= self.min_win_rate

        message = (
            f"Win rate = {wr:.2%} (required >= {self.min_win_rate:.2%})"
        )
        metadata = {
            "actual": wr,
            "threshold": self.min_win_rate
        }

        return Ok(CriterionResult(
            passed=passed,
            message=message,
            severity=self.severity,
            metadata=metadata
        ))


@dataclass(frozen=True)
class ProfitFactorCriterion:
    """
    Criterion: Profit factor must be above a minimum.
    """
    min_profit_factor: float = 1.5
    severity: CriterionSeverity = CriterionSeverity.CRITICAL

    def evaluate(self, metrics: PerformanceMetrics) -> Result[CriterionResult, str]:
        if metrics is None:
            return Err("Metrics object is None")

        pf = metrics.profit_factor
        # Handle infinite profit factor (when no losing trades)
        if pf == float('inf'):
            passed = True
        else:
            passed = pf >= self.min_profit_factor

        message = (
            f"Profit factor = {pf:.3f} (required >= {self.min_profit_factor})"
        )
        metadata = {
            "actual": pf,
            "threshold": self.min_profit_factor
        }

        return Ok(CriterionResult(
            passed=passed,
            message=message,
            severity=self.severity,
            metadata=metadata
        ))


@dataclass(frozen=True)
class TradeCountCriterion:
    """
    Criterion: Minimum number of trades to be statistically significant.
    """
    min_trades: int = 10
    severity: CriterionSeverity = CriterionSeverity.WARNING

    def evaluate(self, metrics: PerformanceMetrics) -> Result[CriterionResult, str]:
        if metrics is None:
            return Err("Metrics object is None")

        trades = metrics.total_trades
        passed = trades >= self.min_trades

        message = (
            f"Total trades = {trades} (required >= {self.min_trades})"
        )
        metadata = {
            "actual": trades,
            "threshold": self.min_trades
        }

        return Ok(CriterionResult(
            passed=passed,
            message=message,
            severity=self.severity,
            metadata=metadata
        ))


# ============================================================================
# COMPOSITE CRITERION (COMBINE MULTIPLE)
# ============================================================================

@dataclass
class CompositeCriterion:
    """
    Combines multiple criteria into one evaluation.
    The overall result is:
      - passed only if all constituent criteria pass.
      - message is concatenation of individual messages.
      - severity is the highest severity among failed criteria (or INFO if all pass).
    """
    criteria: List[Union[Criterion, 'CompositeCriterion']]
    name: str = "Composite"

    def evaluate(self, metrics: PerformanceMetrics) -> Result[CriterionResult, str]:
        if metrics is None:
            return Err("Metrics object is None")

        results: List[CriterionResult] = []
        for idx, crit in enumerate(self.criteria):
            res = crit.evaluate(metrics)
            if res.is_err():
                return Err(f"Sub-criterion {idx} evaluation failed: {res.unwrap_err()}")
            result = res.unwrap()
            # Guard against None
            if result is None:
                return Err(f"Sub-criterion {idx} returned None")
            results.append(result)

        # Determine overall passed (all must pass)
        overall_passed = all(r.passed for r in results)

        # Determine highest severity among failures (or INFO if none)
        if overall_passed:
            highest_severity = CriterionSeverity.INFO
        else:
            failed_severities = [r.severity for r in results if not r.passed]
            if any(s == CriterionSeverity.CRITICAL for s in failed_severities):
                highest_severity = CriterionSeverity.CRITICAL
            elif any(s == CriterionSeverity.WARNING for s in failed_severities):
                highest_severity = CriterionSeverity.WARNING
            else:
                highest_severity = CriterionSeverity.INFO

        # Build combined message
        lines = [f"Composite '{self.name}' evaluation:"]
        for i, res in enumerate(results):
            status = "✅" if res.passed else "❌"
            lines.append(f"  {status} {res.message}")
        message = "\n".join(lines)

        # Collect metadata from all
        metadata: Dict[str, Any] = {
            f"criterion_{i}": res.metadata for i, res in enumerate(results)
        }

        return Ok(CriterionResult(
            passed=overall_passed,
            message=message,
            severity=highest_severity,
            metadata=metadata
        ))


# ============================================================================
# FACTORY FOR STANDARD CRITERIA SETS
# ============================================================================

def create_default_acceptance_criteria() -> CompositeCriterion:
    """
    Create a standard set of criteria for accepting a strategy.
    """
    return CompositeCriterion(
        criteria=[
            SharpeRatioCriterion(min_sharpe=1.0),
            MaxDrawdownCriterion(max_allowed_drawdown=-0.25),
            TotalReturnCriterion(min_return=0.0),
            ProfitFactorCriterion(min_profit_factor=1.2),
            TradeCountCriterion(min_trades=20),
        ],
        name="DefaultAcceptance"
    )


def create_conservative_criteria() -> CompositeCriterion:
    """
    More strict criteria for high-confidence strategies.
    """
    return CompositeCriterion(
        criteria=[
            SharpeRatioCriterion(min_sharpe=1.5, severity=CriterionSeverity.CRITICAL),
            MaxDrawdownCriterion(max_allowed_drawdown=-0.15, severity=CriterionSeverity.CRITICAL),
            TotalReturnCriterion(min_return=0.10, severity=CriterionSeverity.CRITICAL),  # 10%
            ProfitFactorCriterion(min_profit_factor=2.0, severity=CriterionSeverity.CRITICAL),
            TradeCountCriterion(min_trades=50, severity=CriterionSeverity.WARNING),
            WinRateCriterion(min_win_rate=0.55, severity=CriterionSeverity.WARNING),
        ],
        name="Conservative"
    )


def create_exploratory_criteria() -> CompositeCriterion:
    """
    Loose criteria for initial exploration.
    """
    return CompositeCriterion(
        criteria=[
            SharpeRatioCriterion(min_sharpe=0.5, severity=CriterionSeverity.WARNING),
            MaxDrawdownCriterion(max_allowed_drawdown=-0.40, severity=CriterionSeverity.WARNING),
            TotalReturnCriterion(min_return=-0.10, severity=CriterionSeverity.INFO),  # allow small loss
            TradeCountCriterion(min_trades=5, severity=CriterionSeverity.INFO),
        ],
        name="Exploratory"
    )


# ============================================================================
# QUICK ACCESS
# ============================================================================

def get_criteria_by_name(name: str) -> Result[CompositeCriterion, str]:
    """
    Get a predefined criteria set by name.
    Names: 'default', 'conservative', 'exploratory'
    """
    criteria_map = {
        "default": create_default_acceptance_criteria,
        "conservative": create_conservative_criteria,
        "exploratory": create_exploratory_criteria,
    }
    if name not in criteria_map:
        return Err(f"Unknown criteria name: {name}. Available: {list(criteria_map.keys())}")
    return Ok(criteria_map[name]())
