"""
KALMAN EXECUTOR (THE FIRING SQUAD) - V1.1
Location: research/strategy/executor.py
Focus: Pure functional in-memory execution of Kalman strategy.
       Transforms raw signals into analytics‑ready format.
       Zero disk I/O, strict validation, monadic error handling.
"""

import pandas as pd
from typing import Dict, Any

# Core shared monadic tools
from core.shared import Result, Ok, Err

# Signal and math configuration
from core.signals.types import SignalConfig
from core.math.kalman import KalmanConfig

# The strategy itself
from core.signals.strategies.kalman_mr import KalmanMeanReversion


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
    # Guard against None (theoretically impossible, but linter demands)
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
    # PHASE 3: ANALYTICS ADAPTER
    # ========================================================================
    # Transform raw signals into format expected by research.analysis.pipeline

    enriched_dataframe = raw_signals_dataframe.copy()

    # Transformation 1: position from signal_type
    if "signal_type" not in enriched_dataframe.columns:
        return Err("Missing 'signal_type' column in strategy output")
    enriched_dataframe["position"] = enriched_dataframe["signal_type"]

    # Translasi Teks Sinyal ke Angka Vektor
    signal_map = {
        "LONG": 1, "Long": 1, "long": 1, "BUY": 1, "Buy": 1, "buy": 1, 1: 1, 1.0: 1,
        "SHORT": -1, "Short": -1, "short": -1, "SELL": -1, "Sell": -1, "sell": -1, -1: -1, -1.0: -1,
        "NEUTRAL": 0, "Neutral": 0, "neutral": 0, 0: 0, 0.0: 0,
        "FLAT": 0, "Flat": 0, "flat": 0,
        "EXIT": 0, "Exit": 0, "exit": 0
    }
    enriched_dataframe["position"] = enriched_dataframe["signal_type"].map(signal_map).fillna(0).astype(int)

    # Transformation 2: price from spread_val
    if "spread_val" not in enriched_dataframe.columns:
        return Err("Missing 'spread_val' column in strategy output")
    enriched_dataframe["price"] = enriched_dataframe["spread_val"]

    # Optional but recommended: drop any rows where position or price is NaN
    rows_before = len(enriched_dataframe)
    enriched_dataframe = enriched_dataframe.dropna(subset=["position", "price"])
    rows_after = len(enriched_dataframe)
    if rows_after < rows_before:
        # Just a warning, not a failure – we proceed
        pass  # can be logged later if needed

    # ========================================================================
    # 🔬 KACA PEMBESAR FORENSIK (DEBUG)
    # ========================================================================
    print("\n" + "🔍 [TKP FORENSIK] ".ljust(50, "="))
    if "z_score" in enriched_dataframe.columns:
        print(f"Max Abs Z-Score : {enriched_dataframe['z_score'].abs().max()}")
    
    if "signal_type" in enriched_dataframe.columns:
        mentah = enriched_dataframe['signal_type'].unique()
        print(f"Wujud Mentah signal_type : {mentah} (Tipe: {type(mentah[0]) if len(mentah) > 0 else 'Kosong'})")
        
    print(f"Hasil Terjemahan position: {enriched_dataframe['position'].unique()}")
    print("="*50)
    # ========================================================================

    return Ok(enriched_dataframe) # (Ini baris asli Anda, biarkan di bawah sini)

    # ========================================================================
    # PHASE 4: SUCCESS RETURN
    # ========================================================================
    return Ok(enriched_dataframe)
