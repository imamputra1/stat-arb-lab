"""
SHARED CORE MODULE
Location: core/shared/__init__.py
Desc: Exposes core utilities and domain types without polluting the namespace with TypeVars.
"""

# 1. RESULT PATTERN (Hanya Logic Utama, Tanpa T/E)
from .result import (
    Result, 
    Ok, 
    Err, 
    safe,           # [Added] Penting untuk fungsi sync
    safe_async,     # Penting untuk coroutines
    match_result,   # Pattern matching helper
    is_ok,          # [Optional] Type guard helper
    is_err          # [Optional] Type guard helper
)

# 2. DOMAIN TYPES (Data Contracts)
from .domain import (
    OHLCV, 
    FetchJob,
    
    OHLCVContract, 
    FetchJobContract,
    
    create_ohlcv_bulk, 
    validate_ohlcv_batch,

    is_valid_ohlcv, 
    is_valid_fetch_job,

    Status,
    AsyncResult
)

# 3. UTILITIES
from .performance import PerformanceMonitor
from .utils import get_logger

# ==============================================================================
# PUBLIC EXPORTS
# ==============================================================================
__all__ = [
    "Result", 
    "Ok", 
    "Err", 
    "safe", 
    "safe_async", 
    "match_result",
    "is_ok",
    "is_err",
    "OHLCV", 
    "FetchJob", 
    "OHLCVContract", 
    "FetchJobContract",
    "create_ohlcv_bulk", 
    "validate_ohlcv_batch", 
    "is_valid_ohlcv", 
    "is_valid_fetch_job",
    "Status",
    "AsyncResult",
    "PerformanceMonitor",
    "get_logger"
]
