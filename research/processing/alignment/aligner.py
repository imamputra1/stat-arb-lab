"""
ALIGNMENT MODULE - Public Interface
Public Facade for Time Series Alignment operations.
Hides implementation details of strategies behind a simple factory interface.
"""
import logging
from typing import List, Dict, Any, TYPE_CHECKING

# Shared Imports
from ...shared import Result, Ok, Err

# Internal Factory Import
from .strategies import (
    create_aligner as _create_strategy_factory,
    HybridAsofAligner,
    ExactTimeAligner
)

# Type Checking Imports
if TYPE_CHECKING:
    from ..protocols import TimeSeriesAligner

logger = logging.getLogger("AlignmentFacade")

# ====================== PUBLIC INTERFACE ======================

def get_aligner(
    method: str = "asof", 
    tolerance: str = "1m",
    **kwargs: Any
) -> Result['TimeSeriesAligner', str]:
    """
    Factory function to create a TimeSeriesAligner instance.
    
    Args:
        method: Alignment strategy ("asof" or "exact").
        tolerance: Time tolerance string (e.g., "1m", "5s") for asof join.
        **kwargs: Strategy-specific parameters (e.g., strict=True).
    
    Returns:
        Result containing the configured Aligner or an Error message.
    """
    valid_methods = ["asof", "exact"]
    if method not in valid_methods:
        return Err(f"Invalid method: '{method}'. Must be one of {valid_methods}")
    
    try:
        # Delegate to internal strategy factory
        return _create_strategy_factory(
            strategy=method,
            tolerance=tolerance,
            join_strategy=kwargs.get("join_strategy", "backward")
        )
        
    except Exception as e:
        msg = f"Factory error: {str(e)}"
        logger.error(msg, exc_info=True)
        return Err(msg)

def get_default_aligner() -> Result['TimeSeriesAligner', str]:
    """Returns the default configuration: Asof Join with 1m tolerance."""
    return get_aligner(method="asof", tolerance="1m")

def create_loose_aligner(tolerance: str = "5m") -> Result['TimeSeriesAligner', str]:
    """Returns a configuration with looser tolerance (useful for illiquid assets)."""
    return get_aligner(method="asof", tolerance=tolerance)

def create_strict_aligner() -> Result['TimeSeriesAligner', str]:
    """Returns a strict Exact Match configuration (Inner Join)."""
    return get_aligner(method="exact")

# ====================== UTILITIES ======================

def validate_alignment_input(
    data_map: Dict[str, Any]
) -> Result[Dict[str, Any], str]:
    """
    Validates input data before processing.
    Ensures input is a Dictionary of Polars DataFrames/LazyFrames with a 'timestamp' column.
    """
    if not data_map:
        return Err("Input data_map cannot be empty")
    
    for symbol, data in data_map.items():
        # Check 1: Is it a Polars Object?
        type_name = type(data).__name__
        if "DataFrame" not in type_name and "LazyFrame" not in type_name:
            return Err(f"Data for '{symbol}' is not a Polars object (Got: {type_name})")
        
        # Check 2: Does it have 'timestamp'?
        # Handle both DataFrame (columns) and LazyFrame (collect_schema)
        try:
            if hasattr(data, "collect_schema"):
                cols = data.collect_schema().names()
            elif hasattr(data, "columns"):
                cols = data.columns
            else:
                return Err(f"Cannot inspect columns for '{symbol}'")
                
            if "timestamp" not in cols:
                return Err(f"Data for '{symbol}' is missing required column: 'timestamp'")
                
        except Exception as e:
            return Err(f"Validation crashed for '{symbol}': {str(e)}")
    
    return Ok(data_map)

def align_multiple_series(
    data_map: Dict[str, Any],
    method: str = "asof",
    tolerance: str = "1m",
    **kwargs: Any
) -> Result[Any, str]:
    """
    Convenience wrapper: Validates input -> Creates Aligner -> Executes Alignment.
    """
    # 1. Validate Input
    val_res = validate_alignment_input(data_map)
    if val_res.is_err():
        return Err(val_res.error)
        
    # 2. Create Aligner
    aligner_res = get_aligner(method, tolerance, **kwargs)
    
    # 3. Execute (using Result.match pattern manually for clarity)
    if aligner_res.is_err():
        return Err(aligner_res.error)
    
    aligner = aligner_res.unwrap()
    logger.info(f"Executing alignment using {aligner.method}")
    
    return aligner.align(data_map, **kwargs)

def list_available_methods() -> List[Dict[str, str]]:
    """Returns metadata about available alignment strategies."""
    return [
        {
            "method": "asof",
            "desc": "Hybrid Alignment (Nearest Neighbor within tolerance)",
            "params": ["tolerance", "join_strategy"],
            "default": "1m"
        },
        {
            "method": "exact",
            "desc": "Strict Alignment (Inner Join on Timestamp)",
            "params": [],
            "default": "N/A"
        }
    ]

# ====================== EXPORTS ======================

__all__ = [
    "get_aligner",
    "get_default_aligner",
    "create_loose_aligner",
    "create_strict_aligner",
    "align_multiple_series",
    "validate_alignment_input",
    "list_available_methods",
    "HybridAsofAligner",
    "ExactTimeAligner"
]

# ====================== INTERNAL SELF-CHECK ======================

def _internal_module_check():
    """Fail-fast check to ensure module is correctly wired."""
    try:
        res = get_default_aligner()
        if res.is_err():
            logger.warning(f"Self-check failed: {res.error}")
    except Exception as e:
        logger.error(f"Module definition broken: {e}")

_internal_module_check()
