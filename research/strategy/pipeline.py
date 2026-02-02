"""
ADVANCED STRATEGY PIPELINE (THE COMMANDER)
Location: research/strategy/pipeline.py
Focus: End-to-end execution with full state preservation for Node D diagnostics.
"""
import json
import sys
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional
import polars as pl

# --- PATH INJECTION ---
PROJECT_ROOT = Path(__file__).parent.parent.parent.absolute()
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

# ABSOLUTE IMPORTS: Stabil & Industrial Standard
from research.strategy.data.loader import create_silver_loader
from research.strategy.models.library.kalman import KalmanFilter
from research.strategy.engine.vectorized import create_backtest_engine
from research.shared import Ok, Err, Result

# --- SETUP LOGGING ---
def setup_advanced_logging() -> logging.Logger:
    log_dir = PROJECT_ROOT / "logs" / "strategy_pipeline"
    log_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    logger = logging.getLogger("StrategyPipeline")
    logger.setLevel(logging.INFO)
    
    formatter = logging.Formatter('%(asctime)s | %(levelname)-8s | %(message)s', datefmt='%H:%M:%S')
    
    ch = logging.StreamHandler()
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    
    fh = logging.FileHandler(log_dir / f"pipeline_{timestamp}.log")
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    
    return logger

logger = setup_advanced_logging()

class AdvancedStrategyPipeline:
    def __init__(
        self,
        silver_path: Optional[str] = None,
        warmup_days: int = 30,
        entry_threshold: float = 2.0,
        exit_threshold: float = 0.5,
        **model_params
    ):
        # FIX: Definisikan silver_path PERTAMA KALI
        self.silver_path = Path(silver_path) if silver_path else PROJECT_ROOT / "data" / "silver"
        self.warmup_days = warmup_days
        self.entry_threshold = entry_threshold
        self.exit_threshold = exit_threshold
        
        # 1. Initialize Logistics
        self.loader = create_silver_loader(str(self.silver_path))
        
        # 2. Initialize Brain (Kalman Filter)
        self.model = KalmanFilter(
            process_noise=model_params.get('process_noise', 1e-5),
            observation_noise=model_params.get('observation_noise', 1e-4),
            min_periods=self.warmup_days * 1440
        )
        
        # Debugging storage
        self.debug_dir = PROJECT_ROOT / "research" / "debug_data"
        self.debug_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"Pipeline Ignited | Path: {self.silver_path}")


    def execute_pair_arbitrage(self, target: str, anchor: str, start: str, end: str) -> Result[Dict[str, Any], str]:
        try:
            # Generate ID unik untuk sinkronisasi metadata
            exec_id = f"{target}_{anchor}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            # 1. Setup Engine
            engine = create_backtest_engine(
                loader=self.loader, model=self.model,
                entry_threshold=self.entry_threshold,
                exit_threshold=self.exit_threshold,
                warmup_days=self.warmup_days
            )
            
            # 2. Run
            res = engine.run(start_date=start, end_date=end, symbols=[target, anchor])
            if res.is_err(): return Err(res.error)
            
            # 3. Save Metadata (The Dynamic Key)
            meta = {
                "target_symbol": target, "anchor_symbol": anchor,
                "entry_threshold": self.entry_threshold, "exit_threshold": self.exit_threshold,
                "process_noise": self.model.get_hyperparameters()["process_noise"]
            }
            meta_path = PROJECT_ROOT / "research" / "results" / f"metadata_{exec_id}.json"
            with open(meta_path, 'w') as f: json.dump(meta, f)
            
            # 4. Enrich & Save Data
            results_dict = res.unwrap()
            final_df = self._assemble_and_enrich(results_dict, target, anchor)
            self._save_results(final_df, target, anchor)
            self._generate_cli_report(final_df, target)
            
            return Ok({"status": "completed", "id": exec_id})
        except Exception as e:
            return Err(str(e))


    def _assemble_and_enrich(self, raw_data: Dict[str, Any], target: str, anchor: str) -> pl.DataFrame:
        """Converts engine dictionary output into a professional debug-ready DataFrame."""
        # Ekstrak state Kalman (beta, alpha, z_score)
        states = raw_data["states"]
        df = pl.DataFrame({
            "timestamp": raw_data["timestamps"],
            f"log_{target}": raw_data["target_values"],
            f"log_{anchor}": raw_data["feature_values"][0],
            "z_score": raw_data["signals"],
            "beta": [s["beta"] for s in states],
            "position": raw_data.get("positions", [0] * len(states)) # Fallback if not provided
        })

        # Hitung PnL di level Pipeline (KOTOR bin SUPERIOR)
        df = df.with_columns([
            (pl.col("position").shift(1).fill_null(0) * pl.col(f"log_{target}").diff()).alias("pnl_raw")
        ]).with_columns([
            pl.col("pnl_raw").cum_sum().alias("cumulative_returns")
        ])
        
        return df

    def _save_results(self, df: pl.DataFrame, target: str, anchor: str):
        """Saves artifacts for forensic inspection by Node D."""
        # 1. Latest Run (Overwrite for fast inspection)
        latest_path = self.debug_dir / "latest_run.parquet"
        df.write_parquet(latest_path, compression="zstd")
        
        # 2. Archived Run (Timestamped)
        results_dir = PROJECT_ROOT / "research" / "results"
        results_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        archive_path = results_dir / f"arb_{target}_{anchor}_{ts}.parquet"
        df.write_parquet(archive_path, compression="zstd")
        
        logger.info(f"STORAGE | Results archived to {archive_path}")

    def _generate_cli_report(self, df: pl.DataFrame, target: str):
        """Final summary for the pit wall."""
        trades = df.filter(pl.col("position").diff() != 0).height
        pnl = df["cumulative_returns"].tail(1)[0]
        
        print("\n" + "="*50)
        print(f" STRATEGY REPORT: {target}-BTC")
        print("-" * 50)
        print(f" Total Trades : {trades}")
        print(f" Final PnL    : {pnl:.6f}")
        print("="*50 + "\n")

"""
ADVANCED STRATEGY PIPELINE (DYNAMIC COMMANDER)
Location: research/strategy/pipeline.py
Focus: Scalable research with CLI parameter injection.
"""
# ... (impor dan class AdvancedStrategyPipeline tetap sama) ...

def main():
    """Main execution function with FULL dynamic argument parsing."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Execute Dynamic Kalman Arbitrage Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    # Assets & Period
    parser.add_argument("--target", type=str, default="DOGE", help="Target symbol")
    parser.add_argument("--anchor", type=str, default="BTC", help="Anchor symbol")
    parser.add_argument("--start", type=str, default="2024-01-01", help="YYYY-MM-DD")
    parser.add_argument("--end", type=str, default="2024-12-31", help="YYYY-MM-DD")
    
    # LOGIC PARAMETERS (THE TUNING KNOBS)
    parser.add_argument("--entry-threshold", type=float, default=2.0, help="Entry Z-Score")
    parser.add_argument("--exit-threshold", type=float, default=0.5, help="Exit Z-Score")
    parser.add_argument("--warmup", type=int, default=30, help="Warmup days")
    
    # BRAIN PARAMETERS (KALMAN TUNING)
    parser.add_argument("--process-noise", type=float, default=1e-5, help="Q: Adaptation speed")
    parser.add_argument("--obs-noise", type=float, default=1e-4, help="R: Measurement confidence")
    
    args = parser.parse_args()
    
    try:
        # Injeksi seluruh argumen ke dalam Pipeline
        pipeline = AdvancedStrategyPipeline(
            warmup_days=args.warmup,
            entry_threshold=args.entry_threshold,
            exit_threshold=args.exit_threshold,
            process_noise=args.process_noise,
            observation_noise=args.obs_noise
        )
        
        result = pipeline.execute_pair_arbitrage(
            target=args.target,
            anchor=args.anchor,
            start=args.start,
            end=args.end
        )
        
        if result.is_ok():
            logger.info("✅ DYNAMIC EXECUTION SUCCESS")
        else:
            logger.error(f"❌ PIPELINE FAILED: {result.error}")
                
    except Exception as e:
        logger.error(f"💥 CRASH: {str(e)}", exc_info=True)

if __name__ == "__main__":
    main()
