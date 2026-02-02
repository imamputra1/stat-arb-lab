"""
KALMAN FILTER FOR STATISTICAL ARBITRAGE (SUPERIOR ADAPTIVE ESTIMATION)
Location: research/strategy/models/library/kalman.py
Focus: Dynamic hedge ratio estimation with recursive Bayesian updates.
Paradigm: Online learning with numerical stability (Joseph Form).
"""
import logging
from typing import Dict, Any, List, Union
import numpy as np
import polars as pl

# Path correction: 5 dots to reach research/shared/ from strategy/models/library/
from .....shared import Result, Ok, Err

from ..base import StrategyModel, BatchStrategyModel

logger = logging.getLogger("KalmanArbitrage")

class KalmanFilter(StrategyModel, BatchStrategyModel):
    """
    Adaptive Kalman Filter for Time-Varying Linear Regression.
    Model: y = beta * x + alpha + noise
    """

    def __init__(
        self,
        process_noise: float = 1e-5,
        observation_noise: float = 1e-4,
        min_periods: int = 100,
        name: str = "KalmanFilter"
    ):
        """
        Args:
            process_noise: Q matrix diagonal (controls adaptation speed).
            observation_noise: R scalar (measurement uncertainty).
            min_periods: Threshold for OLS warm-up.
        """
        self.name = name
        self.Q = np.eye(2) * process_noise
        self.R = observation_noise
        self.min_periods = min_periods
        
        # State: [beta, alpha]
        self.theta = np.zeros(2)
        self.P = np.eye(2) * 1.0  # Initial uncertainty
        
        # Internal Registry
        self.target_col: str = ""
        self.feature_cols: List[str] = []
        self._is_trained = False
        
        # Adaptive Signal Cache
        self.last_z_score = 0.0
        self.last_spread = 0.0
        self.observation_count = 0

    def train(
        self, 
        historical_data: Union[pl.LazyFrame, pl.DataFrame], 
        target_col: str, 
        feature_cols: List[str]
    ) -> Result[bool, str]:
        """
        KOTOR bin SUPERIOR: Initialize state using OLS (The Warm-up).
        """
        try:
            self.target_col = target_col
            self.feature_cols = feature_cols
            
            df = historical_data.collect() if isinstance(historical_data, pl.LazyFrame) else historical_data
            
            if df.height < self.min_periods:
                return Err(f"Warm-up failed: Need {self.min_periods} rows, got {df.height}")

            # Prepare Matrices
            y = df.get_column(target_col).to_numpy()
            x = df.get_column(feature_cols[0]).to_numpy()
            A = np.column_stack([x, np.ones(len(x))])
            
            # OLS Initial Guess
            self.theta, residuals, _, _ = np.linalg.lstsq(A, y, rcond=None)
            
            # Initial Covariance (P) from residuals
            mse = residuals[0] / len(y) if len(residuals) > 0 else 1e-4
            self.P = np.linalg.inv(A.T @ A) * mse
            
            self._is_trained = True
            logger.info(f"Kalman Warm-up Complete | Initial Beta: {self.theta[0]:.6f}")
            return Ok(True)
        except Exception as e:
            return Err(f"Kalman training failed: {str(e)}")

    def predict(self, features: Dict[str, float]) -> Result[float, str]:
        """
        Generates Adaptive Z-Score based on normalized innovation.
        Note: Use the state from T-1 to predict T.
        """
        if not self._is_trained:
            return Err("Model not initialized")
            
        try:
            # y_hat = beta * x + alpha
            x_val = features[self.feature_cols[0]]
            y_hat = x_val * self.theta[0] + self.theta[1]
            
            # Kita kembalikan Z-Score terakhir yang dihitung saat update sebelumnya
            # Ini memastikan sinyal trading didasarkan pada informasi yang tersedia.
            return Ok(self.last_z_score)
        except Exception as e:
            return Err(f"Prediction crash: {str(e)}")

    def update(self, new_observation: Dict[str, float]) -> Result[bool, str]:
        """
        Recursive Bayesian Update (Joseph Form for Stability).
        """
        if not self._is_trained:
            return Err("Model not initialized")

        try:
            y = new_observation[self.target_col]
            x_val = new_observation[self.feature_cols[0]]
            h = np.array([x_val, 1.0]) # Observation Matrix
            
            # 1. Prediction (Time Update)
            # theta = theta (Random Walk model)
            self.P += self.Q
            
            # 2. Innovation
            y_hat = np.dot(h, self.theta)
            residual = y - y_hat
            self.last_spread = residual
            
            # 3. Innovation Covariance
            S = np.dot(h, np.dot(self.P, h.T)) + self.R
            
            # 4. Kalman Gain
            K = np.dot(self.P, h.T) / S
            
            # 5. Update State
            self.theta += K * residual
            
            # 6. Update Covariance (Joseph Form: Numeric Safety)
            # P = (I - KH)P(I - KH)' + KRK'
            I = np.eye(2)
            I_KH = I - np.outer(K, h)
            self.P = np.dot(I_KH, np.dot(self.P, I_KH.T)) + np.outer(K, K) * self.R
            
            # 7. Adaptive Z-Score Calculation
            self.last_z_score = residual / np.sqrt(S) if S > 0 else 0.0
            self.observation_count += 1
            
            return Ok(True)
        except Exception as e:
            return Err(f"Kalman update failed: {str(e)}")

    def batch_update(self, new_data: Union[pl.LazyFrame, pl.DataFrame]) -> Result[bool, str]:
        """Superior: Fast vectorized loop over batch data."""
        try:
            df = new_data.collect() if isinstance(new_data, pl.LazyFrame) else new_data
            for row in df.iter_rows(named=True):
                self.update(row)
            return Ok(True)
        except Exception as e:
            return Err(f"Batch update failed: {str(e)}")

    def get_state(self) -> Result[Dict[str, Any], str]:
        return Ok({
            "beta": float(self.theta[0]),
            "alpha": float(self.theta[1]),
            "z_score": self.last_z_score,
            "spread": self.last_spread,
            "p_diag": np.diag(self.P).tolist(),
            "count": self.observation_count
        })

    def reset(self) -> Result[bool, str]:
        self.theta = np.zeros(2)
        self.P = np.eye(2)
        self._is_trained = False
        return Ok(True)

    def get_hyperparameters(self) -> Dict[str, Any]:
        return {
            "process_noise": float(self.Q[0, 0]),
            "observation_noise": self.R,
            "min_periods": self.min_periods
        }
