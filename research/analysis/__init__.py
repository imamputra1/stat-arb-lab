"""
ANALYSIS MODULE (THE MECHANIC) - V1.0
Location: research/analysis/__init__.py
Exposes all analysis tools: pipeline, inspector, visualizer, sanity, and judgment.
"""

# ============================================================================
# Core models (shared data classes)
# ============================================================================

from .models import PerformanceMetrics

# ============================================================================
# Main analytics orchestrator
# ============================================================================

from .pipeline import (
    PipelineAnalytics,
    AnalyticsConfig,
    AnalyticsPhase,
    create_analytics,
    quick_metrics,
)

# ============================================================================
# Inspector (forensic analysis)
# ============================================================================

from .inspector import ResultInspector, quick_inspect

# ============================================================================
# Visualizer (dashboard creation)
# ============================================================================

from .visualizer import StrategyVisualizer, quick_visualize

# ============================================================================
# Sanity checker (system validation)
# ============================================================================

from .sanity import SystemDoctor, quick_checkup

# ============================================================================
# Judgment system (criteria and verdict)
# ============================================================================

from .judgment import (
    # Criteria
    CriterionResult,
    CriterionSeverity,
    SharpeRatioCriterion,
    MaxDrawdownCriterion,
    TotalReturnCriterion,
    WinRateCriterion,
    ProfitFactorCriterion,
    TradeCountCriterion,
    CompositeCriterion,
    create_default_acceptance_criteria,
    create_conservative_criteria,
    create_exploratory_criteria,
    get_criteria_by_name,
    # Verdict
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
    # Models
    "PerformanceMetrics",
    
    # Pipeline
    "PipelineAnalytics",
    "AnalyticsConfig",
    "AnalyticsPhase",
    "create_analytics",
    "quick_metrics",
    
    # Inspector
    "ResultInspector",
    "quick_inspect",
    
    # Visualizer
    "StrategyVisualizer",
    "quick_visualize",
    
    # Sanity
    "SystemDoctor",
    "quick_checkup",
    
    # Judgment
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
    "Verdict",
    "Judgment",
    "StrategyJudge",
    "create_default_judge",
    "quick_judge",
]
