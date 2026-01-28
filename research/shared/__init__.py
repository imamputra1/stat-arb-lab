from .result import (
    Result, 
    Ok, 
    Err, 
    safe_async, 
    match_result
)

from .domain import (
    OHLCV, 
    FetchJob,
    
    OHLCVContract, 
    FetchJobContract,
    
    create_ohlcv_bulk, 
    validate_ohlcv_batch,

    is_valid_ohlcv, 
    is_valid_fetch_job
)

__all__ = [
    "Result", 
    "Ok", 
    "Err", 
    "safe_async", 
    "match_result",

    # Domain Types
    "OHLCV", 
    "FetchJob", 
    "OHLCVContract", 
    "FetchJobContract",

    # Domain Utils
    "create_ohlcv_bulk", 
    "validate_ohlcv_batch", 
    "is_valid_ohlcv", 
    "is_valid_fetch_job",
]
