"""
KALMAN EXECUTOR (THE FIRING SQUAD) - V1.5
Location: research/strategy/executor.py
Focus: Pure functional in-memory execution of Kalman strategy.
       Transforms raw signals into analytics‑ready format.
       Zero disk I/O, strict validation, monadic error handling.
       Added forensic logging to catch silent under‑trading.
       Fixed signal mapping using forward fill to preserve positions.
"""

import pandas as pd
from typing import Dict, Any
import logging

# Core shared monadic tools
from core.shared import Result, Ok, Err

# Signal and math configuration
from core.signals.types import SignalConfig
from core.math.kalman import KalmanConfig

# The strategy itself
from core.signals.strategies.kalman_mr import KalmanMeanReversion

# Setup logger for executor
logger = logging.getLogger("Executor")
logger.setLevel(logging.INFO)
if not logger.handlers:
    ch = logging.StreamHandler()
    ch.setFormatter(logging.Formatter('%(asctime)s | %(name)s | %(levelname)s | %(message)s'))
    logger.addHandler(ch)


def run_kalman_backtest(
    historical_dataframe: pd.DataFrame,
    candidate_parameters: Dict[str, Any]
) -> Result[pd.DataFrame, str]:
    """
    Execute Kalman mean‑reversion backtest entirely in memory.

    Args:
        historical_dataframe: Pandas DataFrame with at least 'timestamp',
                              'close_DOGE' and 'close_BTC' columns.
        candidate_parameters: Dictionary containing:
            - entry_z_score, exit_z_score, stop_loss_z, volatility_window, ...
            - Q, R, ...

    Returns:
        Result containing enriched DataFrame with 'price' and 'position'
        columns ready for analytics, or an error message.
    """
    # ========================================================================
    # PHASE 1: GUARD CLAUSES & VALIDATION
    # ========================================================================

    # Rule 1: Input dataframe must not be empty
    if historical_dataframe is None:
        return Err("Historical dataframe is None")
    if not isinstance(historical_dataframe, pd.DataFrame):
        return Err(f"Expected pd.DataFrame, got {type(historical_dataframe)}")
    if historical_dataframe.empty:
        return Err("Historical dataframe is empty")

    # Rule 2: Build and validate SignalConfig
    # Ensure a 'name' field exists (SignalConfig requires it)
    parameters_with_name = candidate_parameters.copy()
    if "name" not in parameters_with_name:
        parameters_with_name["name"] = "KalmanExecutor"

    signal_config_result = SignalConfig.from_dict(parameters_with_name)
    if signal_config_result.is_err():
        error_raw = signal_config_result.unwrap_err()
        error_message = str(error_raw) if error_raw is not None else "Unknown SignalConfig error"
        return Err(f"SignalConfig validation failed: {error_message}")

    valid_signal_config = signal_config_result.unwrap()
    if valid_signal_config is None:
        return Err("SignalConfig resolved to None despite Ok result")

    # Rule 3: Build KalmanConfig (requires Q and R)
    try:
        kalman_config = KalmanConfig(
            R=float(candidate_parameters["R"]),
            Q=float(candidate_parameters["Q"]),
            initial_value=0.0
        )
    except KeyError as missing_key:
        return Err(f"Missing Kalman parameter: {missing_key}")
    except Exception as e:
        return Err(f"KalmanConfig creation failed: {str(e)}")

    # ========================================================================
    # PHASE 2: ENGINE ASSEMBLY & EXECUTION
    # ========================================================================

    # Instantiate the strategy with both configs
    try:
        kalman_engine = KalmanMeanReversion(
            signal_config=valid_signal_config,
            math_config=kalman_config
        )
    except Exception as e:
        return Err(f"Strategy instantiation failed: {str(e)}")

    # Run the backtest
    execution_result = kalman_engine.generate_signals(df=historical_dataframe)
    if execution_result.is_err():
        error_raw = execution_result.unwrap_err()
        error_message = str(error_raw) if error_raw is not None else "Unknown execution error"
        return Err(f"Strategy execution failed: {error_message}")

    raw_signals_dataframe = execution_result.unwrap()
    if raw_signals_dataframe is None:
        return Err("Strategy returned None instead of DataFrame")

    # ========================================================================
    # 🔬 FORENSIC LOGGING – CHECK SIGNAL QUALITY BEFORE TRANSFORM
    # ========================================================================
    if "z_score" in raw_signals_dataframe.columns:
        max_z = raw_signals_dataframe["z_score"].abs().max()
        logger.info(f"Max |z‑score| from strategy: {max_z:.4f}")
    else:
        logger.warning("No 'z_score' column found in raw output.")

    if "signal_type" in raw_signals_dataframe.columns:
        unique_signals = raw_signals_dataframe["signal_type"].unique()
        logger.info(f"Raw signal_type values: {unique_signals}")
    else:
        logger.warning("No 'signal_type' column found in raw output.")

    # ========================================================================
    # PHASE 3: ANALYTICS ADAPTER – MEMORY INJECTION
    # ========================================================================
    # Transform raw signals into format expected by research.analysis.pipeline.
    # NEUTRAL is deliberately omitted from the mapping so it becomes NaN,
    # then forward fill preserves the previous position.

    enriched_dataframe = raw_signals_dataframe.copy()

    # Transformation 1: position from signal_type
    if "signal_type" not in enriched_dataframe.columns:
        return Err("Missing 'signal_type' column in strategy output")

    # Define mapping for all actionable signals.
    signal_map = {
        "BUY": 1, "Buy": 1, "buy": 1, "LONG": 1, "Long": 1, "long": 1,
        "SELL": -1, "Sell": -1, "sell": -1, "SHORT": -1, "Short": -1, "short": -1,
        "EXIT": 0, "Exit": 0, "exit": 0,
        "STOP": 0, "Stop": 0, "stop": 0,
        "FLAT": 0, "Flat": 0, "flat": 0,
        1: 1, 
        -1: -1, 
        0: 0
    }
    # Step 1: map signal to numeric, NaN for unmapped (NEUTRAL, unknown)
    enriched_dataframe["position_raw"] = enriched_dataframe["signal_type"].map(signal_map)

    # Step 2: forward fill to carry positions through NEUTRAL periods
    enriched_dataframe["position"] = enriched_dataframe["position_raw"].ffill()

    # Step 3: fill any remaining NaN (beginning of series) with 0
    enriched_dataframe["position"] = enriched_dataframe["position"].fillna(0).astype(int)

    # Drop temporary column
    enriched_dataframe.drop(columns=["position_raw"], inplace=True)

    # Transformation 2: price from spread_val
    if "spread_val" not in enriched_dataframe.columns:
        return Err("Missing 'spread_val' column in strategy output")
    enriched_dataframe["price"] = enriched_dataframe["spread_val"]

    # Optional but recommended: drop any rows where price is NaN
    rows_before = len(enriched_dataframe)
    enriched_dataframe = enriched_dataframe.dropna(subset=["price"])
    rows_after = len(enriched_dataframe)
    if rows_after < rows_before:
        logger.info(f"Dropped {rows_before - rows_after} rows with NaN price.")

    # Final forensic check on transformed data
    non_zero_positions = (enriched_dataframe["position"] != 0).sum()
    logger.info(f"Non‑zero positions after transform: {non_zero_positions} / {len(enriched_dataframe)}")

    # ========================================================================
    # PHASE 4: SUCCESS RETURN
    # ========================================================================
    return Ok(enriched_dataframe)
