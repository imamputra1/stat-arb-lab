"""
Facade Pattern untuk Math Subsystem
"""

from .kalman import (
    # Core Types
    AdaptiveKalmanFilter,
    KalmanConfig,
    KalmanState,
    
    # Factories
    KalmanFactory,
    KalmanBatchProcessor,
    
    # Error Types
    KalmanError,
    SingularMatrixError,
    NumericalStabilityError,
    
    # Enums
    AdaptationMode,
    
    # Utilities
    compose_kalman_operations,
    with_retry,
    
    # Async
    async_kalman_update,
    async_batch_process,
)

# Alias untuk backward compatibility
KalmanFilter = AdaptiveKalmanFilter

# Re-export Result pattern untuk consistency
from core.shared.result import Result, Ok, Err, match_result, safe_async

__all__ = [
    # Core
    'AdaptiveKalmanFilter',
    'KalmanFilter',  # Alias
    'KalmanConfig',
    'KalmanState',
    
    # Factories
    'KalmanFactory',
    'KalmanBatchProcessor',
    
    # Errors
    'KalmanError',
    'SingularMatrixError',
    'NumericalStabilityError',
    
    # Enums
    'AdaptationMode',
    
    # Utilities
    'compose_kalman_operations',
    'with_retry',
    
    # Async
    'async_kalman_update',
    'async_batch_process',
    
    # Result Pattern
    'Result', 'Ok', 'Err', 'match_result', 'safe_async'
]
