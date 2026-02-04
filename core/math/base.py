"""
STRATEGY MODEL PROTOCOL (THE LAW)
Location: research/strategy/models/base.py
Focus: Abstract definition for state-space and regression models.
Paradigm: Online Learning (update state bar-by-bar or batch-by-batch).
"""
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Union
import polars as pl

# Path correction: 3 dots to reach research/shared/
from core.shared import Result, Ok, Err

class StrategyModel(ABC):
    """
    Abstract base class for all alpha models in Node S.
    Enforces a strict 'train-predict-update' lifecycle to prevent look-ahead bias.
    
    Key Principles:
    1. Online Learning: Models must support incremental updates.
    2. Result Pattern: All operations return Result for robust error handling.
    3. State Inspection: Models must expose internal state for monitoring.
    """

    @abstractmethod
    def train(
        self, 
        historical_data: Union[pl.LazyFrame, pl.DataFrame], 
        target_col: str, 
        feature_cols: List[str]
    ) -> Result[bool, str]:
        """
        Initial calibration of the model (Warm-up phase).
        Used to set initial state vector before the backtest loop begins.
        """
        pass

    @abstractmethod
    def update(self, new_observation: Dict[str, float]) -> Result[bool, str]:
        """
        Ingests a SINGLE new market tick to update internal state (Online Learning).
        This is the core of dynamic models like Kalman Filter.
        """
        pass

    @abstractmethod
    def predict(self, features: Dict[str, float]) -> Result[float, str]:
        """
        Generates the next-step prediction (e.g., Expected Spread or Alpha).
        Must be called BEFORE update() to simulate real-time trading.
        """
        pass

    @abstractmethod
    def get_state(self) -> Result[Dict[str, Any], str]:
        """
        Returns internal diagnostics (e.g., Current Hedge Ratio/Beta, Error Variance).
        Used for real-time monitoring and debug logs.
        """
        pass

    @abstractmethod
    def reset(self) -> Result[bool, str]:
        """Resets the model to initial state for walk-forward testing."""
        pass

    @abstractmethod
    def get_hyperparameters(self) -> Dict[str, Any]:
        """Returns the model's configuration for reproducibility."""
        pass

    def validate_features(self, features: Dict[str, float], expected_features: List[str]) -> Result[bool, str]:
        """
        KOTOR bin SUPERIOR: Fast validation for incoming tick data.
        Ensures the model doesn't process corrupted or incomplete observations.
        """
        try:
            missing = [f for f in expected_features if f not in features]
            if missing:
                return Err(f"Validation Failure: Missing features {missing}")
            
            # Optional: Strict check for unexpected features to prevent data leakage
            return Ok(True)
        except Exception as e:
            return Err(f"Feature Validation Crash: {str(e)}")


class BatchStrategyModel(ABC):
    """
    Extended protocol for models that support periodic batch retraining.
    Suitable for Static OLS or ML-based models.
    """
    
    @abstractmethod
    def batch_update(
        self, 
        new_data: Union[pl.LazyFrame, pl.DataFrame]
    ) -> Result[bool, str]:
        """Updates model with a batch of new observations."""
        pass

    @abstractmethod
    def should_retrain(self, metrics: Dict[str, float]) -> bool:
        """Logic to determine if the model's drift requires retraining."""
        pass


# --- Utility Handlers (Industrial Standard) ---

def validate_strategy_model(model: Any) -> Result[bool, str]:
    """Verifies if an object implements the StrategyModel contract."""
    if isinstance(model, StrategyModel):
        return Ok(True)
    return Err(f"Object does not implement StrategyModel. Got: {type(model)}")


def create_model_summary(model: StrategyModel) -> Result[Dict[str, Any], str]:
    """Generates a standardized diagnostic summary for monitoring."""
    try:
        state_res = model.get_state()
        if state_res.is_err():
            return Err(f"Model State Access Failure: {state_res.error}")
        
        return Ok({
            "model_class": type(model).__name__,
            "hyperparameters": model.get_hyperparameters(),
            "current_state_keys": list(state_res.unwrap().keys()),
            "supports_batch": isinstance(model, BatchStrategyModel)
        })
    except Exception as e:
        return Err(f"Summary Generation Failure: {str(e)}")
