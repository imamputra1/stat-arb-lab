"""
SHOTGUN OPTIMIZATION ENGINE (THE COMMANDER) - V9.1
Location: research/strategy/optimization/shotgun.py
Focus:
  1. Load combat data ONCE (using pipeline.prepare_combat_data).
  2. Generate parameter candidates (using spaces.get_parameter_space).
  3. Parallel execution of Kalman backtest (using executor.run_kalman_backtest).
  4. Score with analysis.pipeline.quick_metrics.
  5. Display leaderboard (no disk I/O).
  6. Smoke test defaults: 10k rows, 5 candidates, 2 workers.
"""

import logging
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import List, Dict, Any
from pathlib import Path

import pandas as pd

# --- Path injection ---
import sys
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent.absolute()
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Core shared
from core.shared import Result, Ok, Err

# Strategy modules
from research.strategy.pipeline import prepare_combat_data
from research.strategy.executor import run_kalman_backtest
from research.strategy.optimization.spaces import get_parameter_space, SearchResult
from research.analysis.pipeline import quick_metrics

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
logger = logging.getLogger("Shotgun")


# ============================================================================
# Helper: evaluate a single parameter combination (must be top-level for pickling)
# ============================================================================
def _evaluate_single(
    candidate: SearchResult,
    combat_dataframe: pd.DataFrame
) -> Result[Dict[str, Any], str]:
    """
    Run backtest for one parameter candidate and return compact metrics.
    This function is executed in a separate process.
    """
    try:
        # 1. Execute Kalman backtest
        exec_result = run_kalman_backtest(
            historical_dataframe=combat_dataframe,
            candidate_parameters=candidate.params
        )
        if exec_result.is_err():
            return Err(f"Backtest failed: {exec_result.unwrap_err()}")

        result_df = exec_result.unwrap()
        # Guard against None (satisfies strict linter)
        if result_df is None:
            return Err("Backtest returned None DataFrame")

        # 2. Score the result using quick_metrics (expects DataFrame with price/position)
        metrics_result = quick_metrics(result_df)
        if metrics_result.is_err():
            return Err(f"Scoring failed: {metrics_result.unwrap_err()}")

        metrics = metrics_result.unwrap()
        # Guard against None (satisfies strict linter)
        if metrics is None:
            return Err("Scoring returned None metrics")

        # 3. Extract relevant metrics into a flat dictionary
        return Ok({
            "label": candidate.label,
            "params": candidate.params,
            "total_return": metrics.total_return,
            "sharpe_ratio": metrics.sharpe_ratio,
            "total_trades": metrics.total_trades,
            "win_rate": metrics.win_rate,
            "max_drawdown": metrics.max_drawdown,
            "profit_factor": metrics.profit_factor,
        })
    except Exception as e:
        return Err(f"Unexpected crash: {str(e)}")

# ============================================================================
# Main orchestration
# ============================================================================
def run_shotgun_test(
    target_coin: str = "DOGE",
    anchor_coin: str = "BTC",
    start_date: str = "2025-09-01",
    end_date: str = "2025-09-30",
    space_name: str = "shotgun",
    max_rows: int = 10000,
    max_candidates: int = 5,
    n_workers: int = 2
) -> Result[pd.DataFrame, str]:
    """
    Execute a small-scale shotgun optimization test entirely in RAM.
    Returns a DataFrame with sorted results (leaderboard).
    """
    logger.info("🔫 SHOTGUN OPTIMIZATION STARTING")
    logger.info(f"Target: {target_coin}/{anchor_coin} | Period: {start_date} to {end_date}")
    logger.info(f"Space: {space_name} | Max candidates: {max_candidates} | Max rows: {max_rows} | Workers: {n_workers}")

    # ------------------------------------------------------------------------
    # PHASE 1: PREPARE COMBAT DATA (once)
    # ------------------------------------------------------------------------
    data_result = prepare_combat_data(
        target_coin=target_coin,
        anchor_coin=anchor_coin,
        start_date=start_date,
        end_date=end_date,
        hedge_ratio=1.0
    )
    if data_result.is_err():
        return Err(f"Data preparation failed: {data_result.unwrap_err()}")

    full_dataframe = data_result.unwrap()
    # Guard against None
    if full_dataframe is None:
        return Err("Data preparation returned None")
    logger.info(f"✅ Full data loaded: {len(full_dataframe)} rows")

    # Apply the aero shield: take only first max_rows
    if len(full_dataframe) > max_rows:
        combat_dataframe = full_dataframe.head(max_rows).copy()
        logger.info(f"✂️ Data trimmed to {max_rows} rows")
    else:
        combat_dataframe = full_dataframe
        logger.info(f"ℹ️ Data has {len(combat_dataframe)} rows (≤ {max_rows})")

    # ------------------------------------------------------------------------
    # PHASE 2: GENERATE PARAMETER CANDIDATES
    # ------------------------------------------------------------------------
    space_result = get_parameter_space(space_name)
    if space_result.is_err():
        return Err(f"Parameter space error: {space_result.unwrap_err()}")

    space = space_result.unwrap()
    if space is None:
        return Err("Parameter space is None")

    candidates: List[SearchResult] = []
    for res in space.generate():
        if res.is_ok():
            candidate = res.unwrap()
            if candidate is not None:
                candidates.append(candidate)
        if len(candidates) >= max_candidates:
            break

    if not candidates:
        return Err("No valid parameter candidates generated")
    logger.info(f"🔫 Generated {len(candidates)} parameter candidates")

    # ------------------------------------------------------------------------
    # PHASE 3: PARALLEL EXECUTION
    # ------------------------------------------------------------------------
    results: List[Dict[str, Any]] = []
    start_time = time.perf_counter()

    with ProcessPoolExecutor(max_workers=n_workers) as executor:
        future_to_candidate = {
            executor.submit(_evaluate_single, cand, combat_dataframe): cand
            for cand in candidates
        }

        for future in as_completed(future_to_candidate):
            cand = future_to_candidate[future]
            try:
                eval_result = future.result(timeout=60)
                if eval_result.is_ok():
                    metric_dict = eval_result.unwrap()
                    if metric_dict is not None:
                        results.append(metric_dict)
                        logger.info(f"✅ {cand.label} succeeded")
                else:
                    logger.warning(f"❌ {cand.label} failed: {eval_result.unwrap_err()}")
            except Exception as e:
                logger.error(f"💥 {cand.label} crashed: {str(e)}")

    elapsed = time.perf_counter() - start_time
    logger.info(f"⏱️  All tasks completed in {elapsed:.2f} seconds")

    # ------------------------------------------------------------------------
    # PHASE 4: BUILD LEADERBOARD
    # ------------------------------------------------------------------------
    if not results:
        return Err("No successful runs to display")

    leaderboard_df = pd.DataFrame(results)
    leaderboard_df = leaderboard_df.sort_values("total_return", ascending=False).reset_index(drop=True)

    print("\n" + "="*80)
    print("🏆 SHOTGUN LEADERBOARD (top 5)".center(80))
    print("="*80)
    display_cols = ["label", "total_return", "sharpe_ratio", "total_trades", "win_rate", "max_drawdown"]
    print(leaderboard_df[display_cols].head(5).to_string(index=False))
    print("="*80 + "\n")

    return Ok(leaderboard_df)

# ============================================================================
# CLI / notebook entry point – SMOKE TEST CONFIGURATION
# ============================================================================
if __name__ == "__main__":
    # Smoke test: 10k rows, 5 candidates, 2 workers
    result = run_shotgun_test(
        target_coin="DOGE",
        anchor_coin="BTC",
        start_date="2025-09-01",
        end_date="2025-09-30",
        space_name="shotgun",
        max_rows=50000,
        max_candidates=20,
        n_workers=4
    )
    if result.is_err():
        logger.error(f"❌ Smoke test failed: {result.unwrap_err()}")
    else:
        logger.info("✅ Smoke test completed successfully.")
