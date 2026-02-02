import logging
from string import Formatter
import sys
import argparse
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
from pandas import DataFrame
import polars as pl
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.absolute()
sys.path.append(str(PROJECT_ROOT))

from research.strategy.data import SilverDataLoader, create_silver_loader
from research.strategy.models import KalmanFilter
from research.strategy.engine import create_backtest_engine
from research.shared import Ok, Err, Result

def setup_pipeline_logging() -> logging.Logger:
    log_dir = PROJECT_ROOT / "logs/" / "strategy_pipeline"
    log_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_dir = log_dir / f"pipeline_{timestamp}.log"

    logger = logging.getLogger("StrategyPipeline")
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        '%(asctime)s | %(lavelname)-8s | %(name)-15s | %(message)s',
        datefmt='%H:%M:%S'
    )

    ch = logging.StreamHandler
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    fh = logging.FileHandler(log_file)
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    return logger

logger = setup_pipeline_logging()

class StrategyPipeline:
    def __init__(
        self,
        silver_path: Optional[List[str]] = None,
        warmup_days: int = 30,
        entry_threshold: float = 2.0,
        exit_threshold: float = 0.5,
        process_noise: float = 1e-5,
        observation_noise: float = 1e-4
    ):
        self.silver_path = Path(silver_path) if silver_path else PROJECT_ROOT / "data" / "silver"
        self.warmup_days = warmup_days
        self.entry_threshold = entry_threshold
        self.exit_threshold = exit_threshold

        self.loader = create_silver_loader(str(self.silver_path))
        
        self.model = KalmanFilter(
            process_noise=process_noise,
            observation_noise=observation_noise,
            min_periods=self.warmup_days * 1440
        )
        self.engine = None
        logger.info(f"Pipeline Ignited | silver_path")

    def execute_pair_arbitrage(
        self,
        target_symbol: str,
        anchore_symbol: str,
        start_date: str,
        end_date: str,
        save_results: bool = True
    ) -> Result[Dict[str, Any], str]:
        try:
            logger.info(f"EXECUTE | Pair{target_symbol}-{anchore_symbol} | Periods: {start_date} to {end_date}")

            self.engine = create_backtest_engine(
                data_loader=self.loader,
                strategy_model=self.model,
                warmup_days=self.warmup_days,
                entry_threshold=self.entry_threshold,
                exit_threshold=self.exit_threshold
            )
            
            backtest_res = self.engine.run(
                start_date=start_date,
                end_date=end_date,
                target_asset=target_symbol,
                anchore_asset=anchore_symbol
            )

            if backtest_res.is_err():
                return Err(f"Simulation Failure: {backtest_res.error}")

            result_df = backtest_res.unwrap()

            self._generate_report(result_df, target_symbol)

            if save_results:
                save_res = self.save_results(result_df, target_symbol, anchore_symbol)
                if save_res.is_ok():
                    logger.info(f"STORAGE | Result saved to {save_res.unwrap()}")

            return Ok({"status": "completed", "rows": result_df.height})

        except Exception as e:
            logger.error(f"Pipeline Crash: {str(e)", exc_info=True})
            return Err(f"Pipeline Error: {str(e)}")

    def _generate_report(self, df: pl.DataFrame, target: str):
        trades = df.filter(pl.col("posisition").diff() !=0).height
        final_pnl = df["cum_pnl"].tail(1)[0] if "cum_pnl" in df.columns else 0.0


        print("\n" + "="*50)
        print(f" STRATEGY REPORT: {target}-BTC")
        print("-" * 50)
        print(f" Simulation Period: {df['timestamp'].min()} - {df['timestamp'].max()}")
        print(f" Total Samples   : {df.height:,}")
        print(f" Total Trades    : {trades:,}")
        print(f" Final Cum PnL   : {final_pnl:.6f}")
        print("="*50 + "\n")


