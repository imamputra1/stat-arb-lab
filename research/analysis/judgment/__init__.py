"""
JUDGMENT MODULE (THE COURTROOM) - V1.0
Location: research/analysis/judgment/__init__.py
Focus: Unified exports for criteria and verdict subsystems.
"""

# ============================================================================
# Import from criteria
# ============================================================================

from .criteria import (
    # Base classes
    CriterionResult,
    CriterionSeverity,
    
    # Concrete criteria
    SharpeRatioCriterion,
    MaxDrawdownCriterion,
    TotalReturnCriterion,
    WinRateCriterion,
    ProfitFactorCriterion,
    TradeCountCriterion,
    
    # Composite
    CompositeCriterion,
    
    # Factory functions
    create_default_acceptance_criteria,
    create_conservative_criteria,
    create_exploratory_criteria,
    get_criteria_by_name,
)

# ============================================================================
# Import from verdict
# ============================================================================

from .verdict import (
    Verdict,
    Judgment,
    StrategyJudge,
    create_default_judge,
    quick_judge,
)

# ============================================================================
# Public exports
# ============================================================================

__all__ = [
    # Criteria
    "CriterionResult",
    "CriterionSeverity",
    "SharpeRatioCriterion",
    "MaxDrawdownCriterion",
    "TotalReturnCriterion",
    "WinRateCriterion",
    "ProfitFactorCriterion",
    "TradeCountCriterion",
    "CompositeCriterion",
    "create_default_acceptance_criteria",
    "create_conservative_criteria",
    "create_exploratory_criteria",
    "get_criteria_by_name",
    
    # Verdict
    "Verdict",
    "Judgment",
    "StrategyJudge",
    "create_default_judge",
    "quick_judge",
]

# ============================================================================
# END
# ============================================================================
