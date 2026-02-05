"""
CORE STRATEGIES FACADE
Exposes concrete strategy implementations (The Motherboards).
"""

from .kalman_mr import KalmanMRStrategy

__all__ = [
    'KalmanMRStrategy'
]
