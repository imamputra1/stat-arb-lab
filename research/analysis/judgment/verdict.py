"""
JUDGMENT VERDICT (THE FINAL WORD) - V1.0
Location: research/analysis/judgment/verdict.py
Focus: Aggregate criterion results into a final verdict (ACCEPT, REJECT, REVIEW, INCONCLUSIVE).
"""

from typing import List, Dict, Any
from dataclasses import dataclass, field
from enum import Enum

# Import from criteria (relative import)
from .criteria import CriterionResult, CriterionSeverity

# Core shared
from core.shared import Result, Ok, Err


# ============================================================================
# VERDICT ENUM
# ============================================================================

class Verdict(Enum):
    """Final decision on the strategy performance."""
    ACCEPT = "ACCEPT"          # All critical criteria passed, warnings may exist
    REJECT = "REJECT"          # One or more critical criteria failed
    REVIEW = "REVIEW"          # Mixed results, needs human review
    INCONCLUSIVE = "INCONCLUSIVE"  # Not enough data to judge (e.g., no trades)


# ============================================================================
# JUDGMENT DATA CLASS
# ============================================================================

@dataclass(frozen=True)
class Judgment:
    """
    Complete judgment including verdict, summary, and details.
    """
    verdict: Verdict
    summary: str
    details: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


# ============================================================================
# JUDGE CLASS (THE DECIDER)
# ============================================================================

class StrategyJudge:
    """
    Evaluates a list of criterion results and produces a final verdict.
    """

    def __init__(self, name: str = "DefaultJudge") -> None:
        self.name = name

    def judge(self, criterion_results: List[CriterionResult]) -> Result[Judgment, str]:
        """
        Given a list of CriterionResult, produce a Judgment.
        """
        # Guard clauses
        if criterion_results is None:
            return Err("Criterion results list is None")
        if not isinstance(criterion_results, list):
            return Err(f"Expected list, got {type(criterion_results)}")
        if len(criterion_results) == 0:
            return Err("No criterion results provided")

        # Separate by severity
        critical_results = [r for r in criterion_results if r.severity == CriterionSeverity.CRITICAL]
        warning_results = [r for r in criterion_results if r.severity == CriterionSeverity.WARNING]
        info_results = [r for r in criterion_results if r.severity == CriterionSeverity.INFO]

        # Determine failures
        critical_failures = [r for r in critical_results if not r.passed]
        warning_failures = [r for r in warning_results if not r.passed]
        info_failures = [r for r in info_results if not r.passed]

        # Special case: if there are no critical criteria at all, it's inconclusive
        if len(critical_results) == 0:
            verdict = Verdict.INCONCLUSIVE
            summary = "No critical criteria defined – cannot make a determination."
        # If any critical fails → REJECT
        elif critical_failures:
            verdict = Verdict.REJECT
            summary = f"Rejected due to {len(critical_failures)} critical failure(s)."
        # If no critical failures but there are warning failures → REVIEW
        elif warning_failures:
            verdict = Verdict.REVIEW
            summary = f"Review needed: {len(warning_failures)} warning(s) and no critical failures."
        # If only info failures or all pass → ACCEPT
        elif info_failures:
            verdict = Verdict.ACCEPT
            summary = f"Accepted with {len(info_failures)} informational notes."
        else:
            verdict = Verdict.ACCEPT
            summary = "All criteria passed."

        # Build details list
        details: List[str] = []
        for r in criterion_results:
            status = "✅ PASS" if r.passed else "❌ FAIL"
            details.append(f"{status} [{r.severity.value.upper()}] {r.message}")

        # Metadata
        metadata = {
            "judge_name": self.name,
            "total_criteria": len(criterion_results),
            "critical_failures": len(critical_failures),
            "warning_failures": len(warning_failures),
            "info_failures": len(info_failures),
        }

        return Ok(Judgment(
            verdict=verdict,
            summary=summary,
            details=details,
            metadata=metadata
        ))


# ============================================================================
# FACTORY / QUICK ACCESS
# ============================================================================

def create_default_judge() -> StrategyJudge:
    """Get the default judge instance."""
    return StrategyJudge("DefaultJudge")


def quick_judge(criterion_results: List[CriterionResult]) -> Result[Judgment, str]:
    """One-shot judgment using default judge."""
    judge = create_default_judge()
    return judge.judge(criterion_results)


# ============================================================================
# END
# ============================================================================
