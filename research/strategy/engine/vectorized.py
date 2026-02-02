"""
HYBRID BACKTEST ENGINE (THE SIMULATOR)
Location: research/strategy/engine/vectorized.py
Focus: Online learning simulation with strict no look-ahead bias.
Paradigm: Hybrid vectorization (Polars for I/O, Numpy for recursive updates).
"""
import logging
import time
from typing import Dict, List, Any
import numpy as np
from datetime import datetime, timedelta

from research.shared import Ok, Err, Result
from research.strategy.data.loader import SilverDataLoader
from research.strategy.models.base import StrategyModel

logger = logging.getLogger("HybridEngine")

class HybridBacktestEngine:
    """
    High-performance simulation engine for dynamic strategies.
    Orchestrates Logistics (Data) and Brain (Model) with microsecond precision.
    """

    def __init__(
        self,
        loader: SilverDataLoader,
        model: StrategyModel,
        entry_threshold: float = 2.0,
        exit_threshold: float = 0.5,
        warmup_days: int = 30,
        target_col: str = "log_DOGE",     # Default fallback
        feature_cols: List[str] = None    # Default fallback
    ):
        self.loader = loader
        self.model = model
        self.warmup_days = warmup_days
        self.entry_threshold = entry_threshold
        self.exit_threshold = exit_threshold
        
        # Handle defaults list mutable argument
        self.default_target = target_col
        self.default_features = feature_cols if feature_cols else ["log_BTC"]

    def run(
        self,
        start_date: str,
        end_date: str,
        symbols: List[str] = None, # Changed signature to match Pipeline call
        target_col: str = None,    # Optional override
        feature_cols: List[str] = None # Optional override
    ) -> Result[Dict[str, Any], str]:
        """
        Executes the full hybrid backtest cycle.
        Returns a raw Dictionary containing all simulation artifacts.
        """
        try:
            # 1. SETUP ASSETS
            target = symbols[0] if symbols else "DOGE"
            anchor = symbols[1] if symbols and len(symbols) > 1 else "BTC"
            
            # Use columns from args or defaults, construct standard names if needed
            t_col = target_col if target_col else f"log_{target}"
            f_cols = feature_cols if feature_cols else [f"log_{anchor}"]
            
            logger.info(f"ENGINE START | Target: {t_col} | Features: {f_cols}")
            start_ts = time.time()

            # 2. LOAD & PREPARE
            dt_start = datetime.fromisoformat(start_date.replace('Z', '+00:00'))
            adjusted_start = (dt_start - timedelta(days=self.warmup_days)).strftime("%Y-%m-%d")
            
            # Load explicit symbols for column selection
            load_symbols = [target, anchor] 
            load_res = self.loader.load(start_date=adjusted_start, end_date=end_date, symbols=load_symbols)
            
            if load_res.is_err(): return Err(load_res.error)
            
            # Eager collection
            full_df = load_res.unwrap().sort("timestamp").collect()
            
            # Validate Columns exist
            missing_cols = [c for c in [t_col] + f_cols if c not in full_df.columns]
            if missing_cols:
                return Err(f"Missing columns in data: {missing_cols}")

            # 3. WARM-UP & SPLIT
            warmup_rows = self.warmup_days * 1440
            if full_df.height <= warmup_rows:
                return Err(f"Insufficient data: {full_df.height} rows <= {warmup_rows} warmup rows")
                
            warmup_df = full_df.head(warmup_rows)
            test_df = full_df.tail(-warmup_rows)

            # Train Model
            train_res = self.model.train(warmup_df, t_col, f_cols)
            if train_res.is_err(): return Err(f"Warm-up Failure: {train_res.error}")

            # 4. EXECUTE SIMULATION
            logger.info(f"SIMULATION | Iterating {test_df.height} bars...")
            
            # Extract raw numpy arrays for speed
            timestamps = test_df.get_column("timestamp").to_list() # Keep as list/objects
            target_values = test_df.get_column(t_col).to_numpy()
            feature_values = test_df.get_column(f_cols[0]).to_numpy() # Support single feature for now
            
            sim_data = self._execute_simulation_loop(target_values, feature_values, t_col, f_cols[0])
            
            # 5. ASSEMBLE RESULT DICTIONARY (Contract Fixed)
            # Pipeline expects: timestamps, signals, target_values, feature_values, states
            result_payload = {
                "timestamps": timestamps,
                "target_values": target_values.tolist(),
                "feature_values": [feature_values.tolist()], # Pipeline expects list of lists for features
                "signals": sim_data["z_scores"],
                "states": sim_data["states"], # Full state dictionaries
                
                # Metadata for reporting
                "performance_metrics": {}, # Placeholder, calculated in pipeline
                "trade_analysis": {},      # Placeholder
                "model_metrics": {
                    "final_beta": sim_data["states"][-1]["beta"] if sim_data["states"] else 0.0
                },
                "simulation_summary": {
                    "start_date": start_date,
                    "end_date": end_date,
                    "duration_seconds": time.time() - start_ts,
                    "total_rows": len(target_values)
                }
            }

            return Ok(result_payload)

        except Exception as e:
            logger.error(f"Engine Crash: {str(e)}", exc_info=True)
            return Err(f"Simulation Error: {str(e)}")


    def _execute_simulation_loop(self, y_vec: np.ndarray, x_vec: np.ndarray, target_name: str, feat_name: str) -> Dict[str, List]:
        z_scores, states, positions = [], [], []
        current_pos = 0 # 0: Neutral, 1: Long Spread, -1: Short Spread
        
        for i in range(len(y_vec)):
            obs = {feat_name: x_vec[i], target_name: y_vec[i]}
            
            # 1. PREDICT SIGNAL
            pred_res = self.model.predict(obs)
            z = pred_res.unwrap() if pred_res.is_ok() else 0.0
            
            # 2. TRADING LOGIC (The Missing Link)
            if current_pos == 0:
                if z < -self.entry_threshold: current_pos = 1
                elif z > self.entry_threshold: current_pos = -1
            elif current_pos == 1 and z > -self.exit_threshold:
                current_pos = 0
            elif current_pos == -1 and z < self.exit_threshold:
                current_pos = 0
            
            # 3. UPDATE MODEL & STORE
            self.model.update(obs)
            state_snapshot = self.model.get_state().unwrap()
            
            z_scores.append(z)
            states.append(state_snapshot)
            positions.append(current_pos) # Pastikan posisi dicatat!
            
        return {
            "z_scores": z_scores,
            "states": states,
            "positions": positions
        }

# --- FACTORY ---
def create_backtest_engine(loader, model, **kwargs) -> HybridBacktestEngine:
    return HybridBacktestEngine(loader, model, **kwargs)
