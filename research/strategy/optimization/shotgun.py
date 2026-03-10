"""
SHOTGUN OPTIMIZATION ENGINE (THE COMMANDER) - V10.0 INDUSTRIAL GRADE
Location: research/strategy/optimization/shotgun.py
Focus: Parallel execution, explicit terminal telemetry, bulletproof error handling.
"""

import logging
import time
import warnings
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import List, Dict, Any
from pathlib import Path

import pandas as pd

# Matikan warning Pandas yang berisik
warnings.simplefilter(action='ignore', category=FutureWarning)

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
from research.analysis.pipeline import create_analytics, quick_metrics 
from research.analysis.judgment.criteria import get_criteria_by_name

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(message)s')
logger = logging.getLogger("Shotgun")


# ============================================================================
# Helper: evaluate a single parameter combination
# ============================================================================
def _evaluate_single(candidate: SearchResult, combat_dataframe: pd.DataFrame) -> Result[Dict[str, Any], str]:
    try:
        exec_result = run_kalman_backtest(combat_dataframe, candidate.params)
        if exec_result.is_err():
            return Err(f"Backtest failed: {exec_result.unwrap_err()}")

        result_df = exec_result.unwrap()
        if result_df is None:
            return Err("Backtest returned None DataFrame")

        metrics_result = quick_metrics(result_df)
        if metrics_result.is_err():
            return Err(f"Scoring failed: {metrics_result.unwrap_err()}")

        metrics = metrics_result.unwrap()
        if metrics is None:
            return Err("Scoring returned None metrics")

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
    
    print("\n" + "="*60)
    print(" [PHASE 1] Loading data from Silver Lake (wait for about 10-30 minutes)...")
    print("="*60)
    
    data_result = prepare_combat_data(target_coin, anchor_coin, start_date, end_date, 1.0)
    if data_result.is_err():
        return Err(f"Data preparation failed: {data_result.unwrap_err()}")

    full_dataframe = data_result.unwrap()
    if full_dataframe is None:
        return Err("Data preparation returned None")
    
    print(f"[DONE] Data loaded successfully: {len(full_dataframe)} rows.\n")

    if len(full_dataframe) > max_rows:
        combat_dataframe = full_dataframe.head(max_rows).copy()
    else:
        combat_dataframe = full_dataframe

    # ------------------------------------------------------------------------
    # PHASE 2: GENERATE PARAMETER CANDIDATES
    # ------------------------------------------------------------------------
    print("\n" + "="*60)
    print(f" [PHASE 2] The combination of R and Q to be tried (Parameter Space: {space_name})...")
    print("\n" + "="*60)
    space_result = get_parameter_space(space_name)
    if space_result.is_err():
        return Err(f"Parameter space error: {space_result.unwrap_err()}")

    space = space_result.unwrap()
    candidates: List[SearchResult] = []
    for res in space.generate():
        if res.is_ok():
            candidates.append(res.unwrap())
        if len(candidates) >= max_candidates:
            break

    if not candidates:
        return Err("No valid parameter candidates generated")
    print(f"[DONE] The combination was successfully created {len(candidates)} kombinasi parameter.\n")

    # ------------------------------------------------------------------------
    # PHASE 3: PARALLEL EXECUTION
    # ------------------------------------------------------------------------
    print("\n" + "="*60)
    print(" [PHASE 3] Running Combinations (Parallel Execution)...")
    print("\n" + "="*60)
    print("Calculating ETA... (Please wait while the first combination is being processed)")
    print("\n" + "-" * 60)
    
    results: List[Dict[str, Any]] = []
    start_time = time.perf_counter()
    total_candidates = len(candidates)
    completed_count = 0

    with ProcessPoolExecutor(max_workers=n_workers) as executor:
        future_to_candidate = {
            executor.submit(_evaluate_single, cand, combat_dataframe): cand
            for cand in candidates
        }

        for future in as_completed(future_to_candidate):
            cand = future_to_candidate[future]
            completed_count += 1

            # --- ⏱️ PERHITUNGAN ETA ---
            elapsed_time = time.perf_counter() - start_time
            avg_time_per_task = elapsed_time / completed_count
            remaining_tasks = total_candidates - completed_count
            eta_seconds = avg_time_per_task * remaining_tasks
            m, s = divmod(int(eta_seconds), 60)
            eta_str = f"{m:02d}:{s:02d}"

            try:
                # TIMEOUT DIPERBESAR KE 120 DETIK
                eval_result = future.result(timeout=120)
                if eval_result.is_ok():
                    metric_dict = eval_result.unwrap()
                    if metric_dict is not None:
                        results.append(metric_dict)
                        print(f"[DONE] [{completed_count}/{total_candidates}] {cand.label} | ETA: {eta_str}")
                else:
                    print(f"[FAILED] [{completed_count}/{total_candidates}] {cand.label} Failed | ETA: {eta_str}")
            except Exception as e:
                print(f"[CRASHED] [{completed_count}/{total_candidates}] {cand.label} Crashed: {str(e)} | ETA: {eta_str}")

    total_elapsed = time.perf_counter() - start_time
    m, s = divmod(int(total_elapsed), 60)
    print(f"⏱️ Done! {total_candidates} the test time of {len(candidates)} is {m:02d} menit {s:02d} detik.\n")

    # ------------------------------------------------------------------------
    # PHASE 4: BUILD LEADERBOARD
    # ------------------------------------------------------------------------
    if not results:
        return Err("No successful runs to display")

    leaderboard_df = pd.DataFrame(results)
    # URUTKAN BERDASARKAN SHARPE RATIO (TUGAS 3 HARI 3)
    leaderboard_df = leaderboard_df.sort_values("sharpe_ratio", ascending=False).reset_index(drop=True)

    print("="*80)
    print("LEADERBOARD Q & R COMBINATIONS (TOP 5)".center(80))
    print("="*80)
    display_cols = ["label", "total_return", "sharpe_ratio", "total_trades", "win_rate", "max_drawdown"]
    print(leaderboard_df[display_cols].head(5).to_string(index=False))
    print("="*80 + "\n")

    return Ok(leaderboard_df)

# ============================================================================
# CLI ENTRY POINT
# ============================================================================
if __name__ == "__main__":
    # EKSEKUSI SURGICAL GRID (HARI 3)
    result = run_shotgun_test(
        target_coin="DOGE", 
        anchor_coin="BTC",
        start_date="2025-09-01", 
        end_date="2025-09-30",
        space_name="surgical",    # Menggunakan Surgical Grid
        max_rows=50000, 
        max_candidates=150, 
        n_workers=4               # Jumlah CPU
    )

    if result.is_ok():
        leaderboard = result.unwrap()
        if leaderboard is not None and not leaderboard.empty:
            
            top_candidate = leaderboard.iloc[0]
            print(f"Extracting Champion 1 (Alpha): {top_candidate['label']} For further analysis...")
            
            combat_df = prepare_combat_data("DOGE", "BTC", "2025-09-01", "2025-09-30", 1.0).unwrap()
            
            if combat_df is not None:
                winning_params = top_candidate['params']
                exec_result = run_kalman_backtest(combat_df, winning_params).unwrap()
                
                if exec_result is not None:
                    analytics = create_analytics()
                    analytics.compute_metrics(exec_result)
                    
                    judge_criteria = get_criteria_by_name("default").unwrap()
                    if judge_criteria is not None:
                        judgment_res = analytics.judge(judge_criteria)
                        
                        report_text = analytics.generate_report(format="text").unwrap()
                        if report_text is not None:
                            print("\n" + report_text)
                        
                        if judgment_res.is_ok():
                            judgment = judgment_res.unwrap()
                            if judgment is not None:
                                print("=" * 60)
                                print(f"⚖️ FINAL VERDICT : {judgment.verdict.value}")
                                print(f"📝 SUMMARY       : {judgment.summary}")
                                print("=" * 60)
                        
                        output_folder = Path("research/results/plots")
                        output_folder.mkdir(parents=True, exist_ok=True)
                        plots_res = analytics.generate_plots(save=True, output_dir=output_folder)
                        if plots_res.is_ok():
                            print(f"📊 Visualisasi DISIMPAN di: {output_folder}")
