"""
STRATEGY DATA PIPELINE (THE QUARTERMASTER) - V1.0
Location: research/strategy/pipeline.py
Focus: One-time data preparation for backtesting.
       Loads, aligns, and sanitizes silver data.
       Returns a clean DataFrame ready for Kalman execution.
"""

import polars as pl
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Tuple
import logging

# Core shared
from core.shared import Result, Ok, Err

# Setup logger
logger = logging.getLogger("StrategyPipeline")

# Base directory (assuming project root)
PROJECT_ROOT = Path(__file__).parent.parent.parent.absolute()
SILVER_DIR = PROJECT_ROOT / "data" / "silver"

class AdvancedStrategyPipeline:
    """
    Compatibility layer for legacy code expecting a pipeline class.
    New code should use the standalone functions directly:
        - load_silver_data
        - align_and_sanitize
        - calculate_raw_spread
        - prepare_combat_data
    """
    pass
def load_silver_data(
    target_coin: str,
    anchor_coin: str,
    start_date: str,
    end_date: str
) -> Result[Tuple[pl.LazyFrame, pl.LazyFrame], str]:
    """
    Load target and anchor coin data from silver lake for given date range.
    Assumes date format YYYY-MM-DD and that data is partitioned by coin, year, month.
    Returns LazyFrames for further processing.
    """
    try:
        # Extract year and month from start_date (simplified; assumes one month range)
        year = start_date.split("-")[0]
        month = start_date.split("-")[1]

        # Build glob pattern for all parquet files under silver
        base_path = str(SILVER_DIR / "**/*.parquet")

        # Target coin
        target_lf = pl.scan_parquet(base_path, hive_partitioning=True).filter(
            (pl.col("coin") == target_coin) &
            (pl.col("year") == year) &
            (pl.col("month") == month)
        )

        # Anchor coin
        anchor_lf = pl.scan_parquet(base_path, hive_partitioning=True).filter(
            (pl.col("coin") == anchor_coin) &
            (pl.col("year") == year) &
            (pl.col("month") == month)
        )

        return Ok((target_lf, anchor_lf))
    except Exception as e:
        return Err(f"Failed to load silver data: {str(e)}")


def align_and_sanitize(
    target_lf: pl.LazyFrame,
    anchor_lf: pl.LazyFrame
) -> Result[pd.DataFrame, str]:
    """
    Join target and anchor on timestamp, forward fill NaN, return pandas DataFrame.
    """
    try:
        # Select necessary columns (timestamp, close) and rename
        target = target_lf.select(["timestamp", "close"]).rename({"close": "close_target"})
        anchor = anchor_lf.select(["timestamp", "close"]).rename({"close": "close_anchor"})

        # Inner join on timestamp
        merged = target.join(anchor, on="timestamp", how="inner")

        # Collect to DataFrame (eager) and convert to pandas
        df = merged.collect().to_pandas()

        # Sort by timestamp
        df = df.sort_values("timestamp").reset_index(drop=True)

        # Forward fill any remaining NaNs (shouldn't be many after inner join)
        df = df.ffill().bfill()  # forward then backward fill

        return Ok(df)
    except Exception as e:
        return Err(f"Align and sanitize failed: {str(e)}")


def calculate_raw_spread(
    df: pd.DataFrame,
    hedge_ratio: float = 1.0
) -> pd.DataFrame:
    """
    Add spread_val column: log(close_target) - hedge_ratio * log(close_anchor)
    """
    # Guard: ensure no non-positive prices
    if (df["close_target"] <= 0).any() or (df["close_anchor"] <= 0).any():
        raise ValueError("Non-positive prices encountered; cannot compute log spread.")

    df = df.copy()
    df["spread_val"] = np.log(df["close_target"]) - hedge_ratio * np.log(df["close_anchor"])
    return df


def prepare_combat_data(
    target_coin: str,
    anchor_coin: str,
    start_date: str,
    end_date: str,
    hedge_ratio: float = 1.0
) -> Result[pd.DataFrame, str]:
    """
    Main orchestration: load, align, sanitize, compute spread.
    Returns a clean pandas DataFrame with columns:
        timestamp, close_target, close_anchor, spread_val
    """
    # Step 1: Load
    load_res = load_silver_data(target_coin, anchor_coin, start_date, end_date)
    if load_res.is_err():
        return Err(load_res.unwrap_err())
    target_lf, anchor_lf = load_res.unwrap()

    # Step 2: Align and sanitize
    align_res = align_and_sanitize(target_lf, anchor_lf)
    if align_res.is_err():
        return Err(align_res.unwrap_err())
    df = align_res.unwrap()

    # Step 3: Compute spread
    try:
        df = calculate_raw_spread(df, hedge_ratio)
    except Exception as e:
        return Err(f"Spread calculation failed: {str(e)}")

    return Ok(df)
