"""
CORE STRATEGIES FACADE
Exposes concrete strategy implementations (The Motherboards).
"""

from .kalman_mr import KalmanMeanReversion, KalmanState

__all__ = [
    'KalmanMeanReversion',
    'KalmanState'
]
