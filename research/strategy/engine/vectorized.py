"""
HYBRID BACKTEST ENGINE (THE SIMULATOR)
Location: research/strategy/engines/vectorized.py
Focus: Online learning simulation with strict no look-ahead bias.
Paradigm: Hybrid vectorization (Polars for I/O, Numpy for recursive updates).
"""
import logging
import time
from typing import Dict, List
import polars as pl
from datetime import datetime, timedelta

# Path correction: 3 dots for research/shared/
from ...shared import Ok, Err, Result
from ..data import SilverDataLoader
from ..models import StrategyModel, validate_strategy_model

logger = logging.getLogger("HybridEngine")

class HybridBacktestEngine:
    """
    High-performance simulation engine for dynamic strategies.
    Orchestrates Logistics (Data) and Brain (Model) with microsecond precision.
    """

    def __init__(
        self,
        data_loader: SilverDataLoader,
        strategy_model: StrategyModel,
        entry_threshold: float = 2.0,
        exit_threshold: float = 0.5,
        warmup_days: int = 30
    ):
        # Strict validation of protocol adherence
        if not isinstance(data_loader, SilverDataLoader):
            raise TypeError(f"data_loader must be SilverDataLoader, got {type(data_loader)}")
        
        val_res = validate_strategy_model(strategy_model)
        if val_res.is_err():
            raise ValueError(f"Model Protocol Violation: {val_res.error}")

        self.loader = data_loader
        self.model = strategy_model
        self.warmup_days = warmup_days
        self.entry_threshold = entry_threshold
        self.exit_threshold = exit_threshold

    def run(
        self,
        start_date: str,
        end_date: str,
        target_asset: str,
        anchor_asset: str
    ) -> Result[pl.DataFrame, str]:
        """
        Executes the full hybrid backtest cycle.
        """
        try:
            logger.info(f"ENGINE START | Target: {target_asset} | Anchor: {anchor_asset}")
            start_ts = time.time()

            # 1. LOAD & PREPARE (Partition Pruning Active)
            # Ambil data sedikit lebih awal untuk mengakomodasi warm-up
            dt_start = datetime.fromisoformat(start_date)
            adjusted_start = (dt_start - timedelta(days=self.warmup_days)).strftime("%Y-%m-%d")
            
            symbols = [target_asset, anchor_asset]
            load_res = self.loader.load(start_date=adjusted_start, end_date=end_date, symbols=symbols)
            if load_res.is_err(): return Err(load_res.error)
            
            # Eager collection for the simulation loop
            full_df = load_res.unwrap().sort("timestamp").collect()
            
            # 2. SEPARATE WARM-UP & TEST
            target_col = f"log_{target_asset}"
            feature_cols = [f"log_{anchor_asset}"]
            
            warmup_rows = self.warmup_days * 1440 # 1m frequency
            if full_df.height <= warmup_rows:
                return Err(f"Insufficient data: {full_df.height} rows <= {warmup_rows} warmup rows")
                
            warmup_df = full_df.head(warmup_rows)
            test_df = full_df.tail(-warmup_rows)

            # 3. WARM-UP MODEL (OLS Initialization)
            train_res = self.model.train(warmup_df, target_col, feature_cols)
            if train_res.is_err(): return Err(f"Warm-up Failure: {train_res.error}")

            # 4. HYBRID SIMULATION LOOP
            logger.info(f"SIMULATION | Iterating {test_df.height} bars...")
            sim_results = self._execute_simulation_loop(test_df, target_col, feature_cols)
            
            # 5. VECTORIZED PERFORMANCE ASSEMBLY
            final_df = test_df.with_columns([
                pl.Series("z_score", sim_results["z_scores"]),
                pl.Series("beta", sim_results["betas"]),
                pl.Series("position", sim_results["positions"]),
            ])
            
            # Calculate PnL (KOTOR bin SUPERIOR: Log-return based approximation)
            final_df = final_df.with_columns([
                (pl.col("position").shift(1).fill_null(0) * pl.col(target_col).diff()).alias("pnl_raw")
            ]).with_columns([
                pl.col("pnl_raw").cum_sum().alias("cum_pnl")
            ])

            duration = time.time() - start_ts
            logger.info(f"ENGINE COMPLETE | Speed: {test_df.height/duration:,.0f} bars/sec")
            
            return Ok(final_df)

        except Exception as e:
            logger.error(f"Engine Crash: {str(e)}", exc_info=True)
            return Err(f"Simulation Error: {str(e)}")

    def _execute_simulation_loop(self, df: pl.DataFrame, target: str, features: List[str]) -> Dict[str, List]:
        """
        Micro-optimized iterative loop.
        No look-ahead: Predict (T) -> Logic (T) -> Update (T).
        """
        z_scores, betas, positions = [], [], []
        current_pos = 0 
        
        # Fast access via numpy
        y_vec = df.get_column(target).to_numpy()
        x_vec = df.get_column(features[0]).to_numpy()
        
        feat_name = features[0]

        for i in range(len(y_vec)):
            obs = {feat_name: x_vec[i], target: y_vec[i]}
            
            # 1. PREDICT SIGNAL (Adaptive Z-Score dari T-1)
            pred_res = self.model.predict(obs)
            z = pred_res.unwrap() if pred_res.is_ok() else 0.0
            
            # 2. TRADING LOGIC (Mean Reversion)
            if current_pos == 0:
                if z < -self.entry_threshold: current_pos = 1  # Long Spread
                elif z > self.entry_threshold: current_pos = -1 # Short Spread
            elif current_pos == 1 and z > -self.exit_threshold:
                current_pos = 0 # Exit Long
            elif current_pos == -1 and z < self.exit_threshold:
                current_pos = 0 # Exit Short

            # 3. UPDATE STATE (Update model dengan data T terbaru)
            self.model.update(obs)
            
            # 4. STORE
            state = self.model.get_state().unwrap()
            z_scores.append(z)
            betas.append(state["beta"])
            positions.append(current_pos)
            
        return {"z_scores": z_scores, "betas": betas, "positions": positions}

# --- FACTORY ---
def create_backtest_engine(loader, model, **kwargs) -> HybridBacktestEngine:
    return HybridBacktestEngine(loader, model, **kwargs)
