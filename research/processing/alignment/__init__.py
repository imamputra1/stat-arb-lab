"""
ALIGNMENT DISTRICT - Time Series Alignment Module
Public interface for aligning multiple time series data streams.

ADHD-friendly: Clean imports, single point of entry, no hidden complexity.
"""

# ====================== CORE FACTORY FUNCTIONS ======================
from .aligner import (
    get_aligner,
    get_default_aligner,
    create_loose_aligner,
    create_strict_aligner,
)

# ====================== CONVENIENCE FUNCTIONS ======================
from .aligner import (
    align_multiple_series,
    validate_alignment_input,
    list_available_methods,
)

# ====================== IMPLEMENTATION CLASSES ======================
from .strategies import (
    HybridAsofAligner,
    ExactTimeAligner,
)

# ====================== TYPE ALIASES (for IDE support) ======================
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from ..protocols import TimeSeriesAligner
    from ...shared import Result
    import polars as pl
    
    # Re-export type aliases from protocols for convenience
    LazyFrameResult = 'Result[pl.LazyFrame, str]'
    DataFrameResult = 'Result[pl.DataFrame, str]'

# ====================== PUBLIC API ======================
__all__ = [
    # Factory functions (primary interface)
    "get_aligner",
    "get_default_aligner",
    "create_loose_aligner",
    "create_strict_aligner",
    
    # Convenience functions (one-shot operations)
    "align_multiple_series",
    "validate_alignment_input",
    "list_available_methods",
    
    # Implementation classes (for advanced use/testing)
    "HybridAsofAligner",
    "ExactTimeAligner",
]

# ====================== MODULE METADATA ======================
__version__ = "1.0.0"
__description__ = "Time series alignment with asof-join and exact matching strategies"
__author__ = "Node B - The Refinery"

# ====================== RUNTIME VALIDATION ======================
try:
    # Test that we can create a default aligner
    test_result = get_default_aligner()
    if test_result.is_ok():
        from ..protocols import TimeSeriesAligner
        aligner = test_result.unwrap()
        if not isinstance(aligner, TimeSeriesAligner):
            import warnings
            warnings.warn("Default aligner does not comply with TimeSeriesAligner protocol")
except ImportError:
    # Silently fail during type checking or if dependencies missing
    pass
except Exception as e:
    import warnings
    warnings.warn(f"Alignment module validation failed: {e}")

# ====================== DOCSTRING FOR MODULE ======================
"""
USAGE EXAMPLES:

1. Quick alignment with defaults:
   >>> from research.processing.alignment import align_multiple_series
   >>> result = align_multiple_series(data_map)
   >>> if result.is_ok():
   >>>     aligned_data = result.unwrap()

2. Custom aligner configuration:
   >>> from research.processing.alignment import get_aligner
   >>> aligner_result = get_aligner("asof", "5m", join_strategy="forward")
   >>> if aligner_result.is_ok():
   >>>     aligner = aligner_result.unwrap()
   >>>     result = aligner.align(data_map, strict=False)

3. Validate input before alignment:
   >>> from research.processing.alignment import validate_alignment_input
   >>> validation = validate_alignment_input(data_map)
   >>> if validation.is_ok():
   >>>     # Proceed with alignment

4. Discover available methods:
   >>> from research.processing.alignment import list_available_methods
   >>> methods = list_available_methods()
   >>> for method in methods:
   >>>     print(f"{method['method']}: {method['description']}")

PROTOCOL COMPLIANCE:
All aligners implement the TimeSeriesAligner protocol from research.processing.protocols.
All functions return Result[T, str] from research.shared for error handling.
"""
