"""
INDUSTRIAL DISTRIBUTED EXECUTION ORCHESTRATOR (HYPER-PARALLEL ENGINE) - V8.2 SYNC
Location: research/strategy/optimization/shotgun.py
Focus: Quantum-grade distributed computing optimized for Ryzen 5 (12 Threads).
Architecture: Microservices Orchestration with Result Pattern & ACID Checkpoints.
"""

import os
import logging
import time
import json
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass, field, asdict
from typing import List, Dict, Any, Optional, Tuple, Union
from pathlib import Path
from datetime import datetime
from threading import Lock
import pandas as pd
import polars as pl
from dataclasses_json import dataclass_json

# --- PATH CONFIGURATION & SHARED SYNC ---
import sys
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent.absolute()
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from research.shared import Result, Ok, Err
from research.strategy.pipeline import AdvancedStrategyPipeline
from research.strategy.optimization.spaces import SearchResult
from research.strategy.optimization.objective import QuantumScoreKeeper

# --- INDUSTRIAL LOGGING ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d | %(name)-20s | %(levelname)-8s | %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger("QuantumOrchestrator")

# --- DATA MODELS ---
@dataclass_json
@dataclass(frozen=True)
class ExperimentConfig:
    target: str
    anchor: str
    start_date: str
    end_date: str
    space_name: str
    batch_id: str
    created_at: str = field(default_factory=lambda: datetime.utcnow().isoformat())

@dataclass_json
@dataclass
class ExperimentResult:
    label: str
    status: str
    exec_id: Optional[str] = None
    smart_score: float = 0.0
    composite_score: float = 0.0
    pnl: float = 0.0
    sharpe: float = 0.0
    trades: int = 0
    win_rate: float = 0.0
    max_dd: float = 0.0
    pf: float = 0.0
    stability: float = 0.0
    execution_time_ms: float = 0.0
    error_message: Optional[str] = None
    params: Dict[str, Any] = field(default_factory=dict)
    
    def to_compact_dict(self) -> Dict[str, Any]:
        return {k: v for k, v in asdict(self).items() if v is not None}

# --- ADAPTIVE LOAD BALANCER ---
class AdaptiveLoadBalancer:
    def __init__(self, max_workers: int = None):
        self.max_workers = max_workers or os.cpu_count()
        self.lock = Lock()
        self.worker_load = [0] * self.max_workers
        
    def get_optimal_worker(self) -> int:
        with self.lock: return self.worker_load.index(min(self.worker_load))
    
    def update_load(self, worker_id: int, load: float):
        with self.lock: self.worker_load[worker_id] += load

# --- CHECKPOINT MANAGER ---
class CheckpointManager:
    def __init__(self, checkpoint_dir: Path):
        self.checkpoint_dir = checkpoint_dir
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        self.lock = Lock()
        
    def create_checkpoint(self, batch_id: str, completed: List[str], total: int, config: Dict[str, Any]):
        data = {
            "batch_id": batch_id,
            "timestamp": datetime.utcnow().isoformat(),
            "completed": completed,
            "total": total,
            "config": config
        }
        path = self.checkpoint_dir / f"checkpoint_{batch_id}.json"
        with self.lock:
            with open(path.with_suffix('.tmp'), 'w') as f: json.dump(data, f)
            path.with_suffix('.tmp').rename(path)
            
    def load_checkpoint(self, batch_id: str) -> Optional[Dict[str, Any]]:
        path = self.checkpoint_dir / f"checkpoint_{batch_id}.json"
        if not path.exists(): return None
        with open(path, 'r') as f: return json.load(f)

# --- TELEMETRY COLLECTOR ---
class TelemetryCollector:
    def __init__(self):
        self.start_time = time.time()
        self.stats = {"success": 0, "failed": 0, "crashed": 0}
        self.resources = {"cpu": [], "ram": []}
        
    def log_result(self, status: str):
        if status == "SUCCESS": self.stats["success"] += 1
        elif "FAIL" in status: self.stats["failed"] += 1
        else: self.stats["crashed"] += 1

    def get_summary(self) -> Dict[str, Any]:
        duration = time.time() - self.start_time
        return {**self.stats, "duration": duration, "throughput": sum(self.stats.values())/duration if duration > 0 else 0}

# --- ATOMIC EXECUTOR ---
class QuantumExperimentExecutor:
    """Atomic unit of execution. Synced with Pipeline V4.5 & ScoreKeeper V6.5."""
    
    def execute(self, search_result: Union[SearchResult, Dict[str, Any]], config: ExperimentConfig) -> ExperimentResult:
        start_time = time.perf_counter()
        
        # Handle both SearchResult object and raw dict for backward compatibility
        if isinstance(search_result, SearchResult):
            params = search_result.params
            label = search_result.label
        else:
            params = search_result.get("params", search_result)
            label = search_result.get("label", "unlabeled")

        try:
            # 1. Pipeline Injection
            # Ensure keys match pipeline: observation_noise, warmup_days, etc.
            pipeline = AdvancedStrategyPipeline(**params)
            
            # 2. Backtest Execution
            sim_res = pipeline.execute_pair_arbitrage(
                target=config.target,
                anchor=config.anchor,
                start=config.start_date,
                end=config.end_date
            )
            
            if sim_res.is_err():
                return ExperimentResult(label=label, status="FAILED", error_message=sim_res.error, params=params)
            
            # 3. Artifact Forensic
            exec_id = sim_res.unwrap()["id"]
            result_path = PROJECT_ROOT / "research" / "results" / f"arb_{config.target}_{config.anchor}_{exec_id}.parquet"
            
            if not result_path.exists():
                return ExperimentResult(label=label, status="MISSING_FILE", error_message=f"File not found: {result_path.name}", params=params)
            
            # 4. Neural Scoring
            df = pl.read_parquet(result_path)
            keeper = QuantumScoreKeeper()
            eval_res = keeper.evaluate(df)
            
            if eval_res.is_err():
                return ExperimentResult(label=label, status="EVAL_ERROR", error_message=eval_res.error, params=params)
            
            m = eval_res.unwrap()
            return ExperimentResult(
                label=label, status="SUCCESS", exec_id=exec_id,
                smart_score=m.smart_score, composite_score=m.composite_score,
                pnl=m.total_return, sharpe=m.sharpe_ratio, trades=m.total_trades,
                win_rate=m.win_rate, max_dd=m.max_drawdown, pf=m.profit_factor,
                stability=m.stability_ratio, execution_time_ms=(time.perf_counter()-start_time)*1000,
                params=params
            )
            
        except Exception as e:
            return ExperimentResult(label=label, status="CRASHED", error_message=str(e), params=params)

# --- HYPER-PARALLEL ENGINE ---
class HyperParallelEngine:
    def __init__(self, n_jobs: int = -1):
        self.n_jobs = n_jobs if n_jobs > 0 else os.cpu_count()
        self.executor = QuantumExperimentExecutor()
        self.results_dir = PROJECT_ROOT / "research" / "optimization_results"
        self.results_dir.mkdir(parents=True, exist_ok=True)
        self.checkpoint_mgr = CheckpointManager(self.results_dir / "checkpoints")
        self.telemetry = TelemetryCollector()
        self._stop_signal = False

    def fire(self, target_pairs: List[Tuple[str, str]], space_name: str = "shotgun", 
             start_date: str = "2024-01-01", end_date: str = "2024-12-31", 
             max_combos: Optional[int] = None, batch_id: Optional[str] = None) -> Result[Path, str]:
        
        batch_id = batch_id or datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # 1. Parameter Space Generation
        from research.strategy.optimization.spaces import get_parameter_space
        space_res = get_parameter_space(space_name)
        if space_res.is_err(): return Err(space_res.error)
        
        space = space_res.unwrap()
        all_combos = []
        for res in space.generate():
            if res.is_ok(): 
                all_combos.append(res.unwrap())
                if max_combos and len(all_combos) >= max_combos: break
        
        # 2. Recovery Logic
        completed = []
        checkpoint = self.checkpoint_mgr.load_checkpoint(batch_id)
        if checkpoint:
            completed = checkpoint["completed"]
            all_combos = [c for c in all_combos if c.label not in completed]
            logger.info(f"🔄 Resuming {batch_id}: {len(completed)} experiments skipped.")

        total_tasks = len(target_pairs) * len(all_combos)
        logger.info(f"🔫 SHOTGUN ARMED | {total_tasks} Tasks | Workers: {self.n_jobs}")
        
        final_results = []
        try:
            with ProcessPoolExecutor(max_workers=self.n_jobs) as pool:
                futures = {}
                for target, anchor in target_pairs:
                    config = ExperimentConfig(target, anchor, start_date, end_date, space_name, batch_id)
                    for combo in all_combos:
                        f = pool.submit(self.executor.execute, combo, config)
                        futures[f] = combo.label
                
                for i, f in enumerate(as_completed(futures)):
                    res = f.result()
                    final_results.append(res)
                    self.telemetry.log_result(res.status)
                    completed.append(futures[f])
                    
                    if i % 20 == 0:
                        self.checkpoint_mgr.create_checkpoint(batch_id, completed, total_tasks, {"space": space_name})
                        logger.info(f"📈 Progress: {len(completed)}/{total_tasks} ({len(completed)/total_tasks:.1%})")

        except Exception as e:
            return Err(f"Orchestration failure: {str(e)}")

        # 3. Consolidation
        output_path = self.results_dir / f"quantum_batch_{batch_id}.parquet"
        df_final = pd.DataFrame([r.to_compact_dict() for r in final_results])
        df_final.to_parquet(output_path, compression="zstd")
        
        # 4. Analytics Summary
        self._display_leaderboard(df_final)
        
        return Ok(output_path)

    def _display_leaderboard(self, df: pd.DataFrame):
        if df.empty: return
        success = df[df["status"] == "SUCCESS"].sort_values("smart_score", ascending=False)
        print("\n" + "🏆 OPTIMIZATION LEADERBOARD".center(100))
        print("=" * 100)
        cols = ["label", "smart_score", "pnl", "sharpe", "trades", "win_rate"]
        print(success[cols].head(10).to_string(index=False))
        print("=" * 100 + "\n")

if __name__ == "__main__":
    engine = HyperParallelEngine(n_jobs=-1)
    engine.fire(target_pairs=[("DOGE", "BTC")], space_name="shotgun", max_combos=1000)
