"""
QUANTUM STRATEGY PIPELINE (THE BLIND COMMANDER) - V10.0 KALMAN INTEGRATION
Location: research/strategy/pipeline.py
Focus: Pure orchestration with KalmanMeanReversion strategy.
"""

import sys
import logging
import warnings
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field
from enum import Enum, auto

import polars as pl
import pandas as pd

warnings.filterwarnings('ignore')

# --- PATH CONFIGURATION ---
PROJECT_ROOT = Path(__file__).parent.parent.parent.absolute()
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# --- CORE IMPORTS ---
from core.shared import Ok, Err, Result
from research.ingestion.loader import create_silver_loader

# Import Kalman strategy directly
from core.signals.strategies.kalman_mr import KalmanMeanReversion
from core.signals.types import SignalConfig
from core.math import KalmanConfig, AdaptationMode
from core.risk.manager import RiskManager
from core.execution.simulator import ExecutionSimulator
from research.analysis.pipeline import PipelineAnalytics

# Fallback/Mock Imports for Safety


# --- ENUMERATIONS ---
class PipelinePhase(Enum):
    INITIALIZATION = auto()
    DATA_LOADING = auto()
    STRATEGY_EXECUTION = auto()
    RISK_MANAGEMENT = auto()
    EXECUTION_SIMULATION = auto()
    ANALYTICS_GENERATION = auto()
    PERSISTENCE = auto()
    COMPLETION = auto()
    FAILURE = auto()

class ExecutionMode(Enum):
    BACKTEST = auto()
    PAPER_TRADE = auto()
    LIVE = auto()
    OPTIMIZATION = auto()

# --- DATA MODELS ---
@dataclass
class PipelineConfig:
    execution_id: str
    target_symbol: str
    anchor_symbol: str
    start_date: str
    end_date: str
    execution_mode: ExecutionMode = ExecutionMode.BACKTEST
    strategy_name: str = "kalman_mr"
    warmup_days: int = 30
    # Parameters for KalmanMeanReversion
    entry_threshold: float = 2.0
    exit_threshold: float = 0.5
    stop_loss: float = 4.0
    max_position: float = 1.0
    hedge_ratio: float = 1.0
    volatility_window: int = 20
    process_noise: float = 1e-5
    observation_noise: float = 0.01
    use_intercept: bool = True  # not used in Kalman, but kept for compatibility
    # Additional
    risk_params: Dict[str, Any] = field(default_factory=dict)
    execution_params: Dict[str, Any] = field(default_factory=dict)
    analytics_config: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "execution_id": self.execution_id,
            "target": self.target_symbol,
            "anchor": self.anchor_symbol,
            "start": self.start_date,
            "end": self.end_date,
            "strategy": self.strategy_name,
            "params": {
                "entry_threshold": self.entry_threshold,
                "exit_threshold": self.exit_threshold,
                "process_noise": self.process_noise,
                "observation_noise": self.observation_noise,
                "use_intercept": self.use_intercept,
                "warmup_days": self.warmup_days
            }
        }

@dataclass
class PipelineState:
    config: PipelineConfig
    current_phase: PipelinePhase = PipelinePhase.INITIALIZATION
    start_time: datetime = field(default_factory=datetime.now)
    end_time: Optional[datetime] = None
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    artifacts: Dict[str, Any] = field(default_factory=dict)
    metrics: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def duration(self) -> Optional[timedelta]:
        return (self.end_time or datetime.now()) - self.start_time

# --- ORCHESTRATOR ---
class QuantumStrategyOrchestrator:
    def __init__(self, config: PipelineConfig):
        self.config = config
        self.state = PipelineState(config)
        self.logger = self._setup_logging()
        
        self.loader = create_silver_loader("data/silver")
        self.strategy = None
        self.risk_manager = None
        self.execution_sim = None
        self.analytics = None
        
        self.logger.info(f"🚀 Orchestrator initialized | ID: {config.execution_id}")

    def _setup_logging(self) -> logging.Logger:
        logger = logging.getLogger(f"Orchestrator.{self.config.execution_id}")
        logger.setLevel(logging.INFO)
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        return logger

    def execute(self) -> Result[PipelineState, str]:
        try:
            self.state.current_phase = PipelinePhase.INITIALIZATION
            self._initialize_components()

            self.state.current_phase = PipelinePhase.DATA_LOADING
            res_raw = self._load_market_data(
                target=self.config.target_symbol,
                anchor=self.config.anchor_symbol,
                start=self.config.start_date,
                end=self.config.end_date
            )
            if res_raw.is_err():
                return Err(str(res_raw.unwrap_err()))
            data = res_raw.unwrap()
            assert data is not None
            
            lf_data: pl.LazyFrame = res_raw.unwrap()
            assert lf_data is not None, "Dataframe dari Loader bernilai None!"

            self.state.current_phase = PipelinePhase.STRATEGY_EXECUTION
            df_pandas = lf_data.collect().to_pandas()
            strategy_res = self._run_strategy(data=df_pandas)

            if strategy_res.is_err():
                return Err(str(strategy_res.unwrap_err()))

            df_pl_result = strategy_res.unwrap()
            assert df_pl_result is not None
            self.state.artifacts["processed_df"] = df_pl_result

            self.state.current_phase = PipelinePhase.RISK_MANAGEMENT
            self._apply_risk_management()

            self.state.current_phase = PipelinePhase.EXECUTION_SIMULATION
            self._simulate_execution()

            self.state.current_phase = PipelinePhase.ANALYTICS_GENERATION
            self._generate_analytics()

            self.state.current_phase = PipelinePhase.PERSISTENCE
            self._persist_results()

            self.state.end_time = datetime.now()
            self.state.current_phase = PipelinePhase.COMPLETION
            return Ok(self.state)

        except Exception as e:
            self.logger.error(f"🚨 PIPELINE CRASH FATAL: {str(e)}", exc_info=True) # ALARM
            self.state.end_time = datetime.now()
            self.state.current_phase = PipelinePhase.FAILURE
            error_msg = f"Pipeline Crash: {str(e)}"
            self.state.errors.append(error_msg)
            return Err(error_msg)

    def _initialize_components(self):
        """Initialize loader and strategy from config."""
        self.loader = create_silver_loader(str(PROJECT_ROOT / "data" / "silver"))
        
        # Create SignalConfig and KalmanConfig
        signal_config = SignalConfig(
            name=f"{self.config.strategy_name}_{self.config.execution_id}",
            entry_z_score=self.config.entry_threshold,
            exit_z_score=self.config.exit_threshold,
            stop_loss_z=self.config.stop_loss,
            max_position=self.config.max_position,
            hedge_ratio=self.config.hedge_ratio,
            volatility_window=self.config.volatility_window,
            version="1.0"
        )
        math_config = KalmanConfig(
            R=self.config.observation_noise,
            Q=self.config.process_noise,
            initial_value=0.0,
            adaptation_mode=AdaptationMode.NIS_THRESHOLD,
            state_dim=2
        )
        self.strategy = KalmanMeanReversion(signal_config, math_config)
        # Set warmup based on days (convert to ticks assuming 1-min data)
        warmup_ticks = self.config.warmup_days * 1440
        setattr(self.strategy, '_warmup_required', warmup_ticks) # type: ignore # override instance variable
        self.logger.info(f"✓ Strategy initialized with warmup {warmup_ticks} ticks")

        self.risk_manager = RiskManager(**self.config.risk_params)
        self.execution_sim = ExecutionSimulator(**self.config.execution_params)
        self.analytics = PipelineAnalytics(**self.config.analytics_config)
        self.logger.info("✓ All components synced")

    def _load_market_data(self, target: str, anchor: str, start: str, end: str) -> Result[pl.LazyFrame, str]:
        """
        [DATA INGESTION] Load and merge target and anchor datasets.
        Terlindungi dari Null-Pointer dan sesuai standar Polars Modern.
        """
        try:
            # 1. Buka bungkus data_loader dan yakinkan linter bahwa ini tidak None
            loader = self.loader
            assert loader is not None, "Critical: Data loader is missing!"

            # 2. Muat Data Target (contoh: DOGE)
            res_target = loader.load(symbols=[target], start_date=start, end_date=end)
            if res_target.is_err():
                # Fix: Menggunakan unwrap_err() bukan .error
                return Err(str(res_target.unwrap_err()))
            
            df_target = res_target.unwrap()
            assert df_target is not None, "Critical: Target DataFrame is None!"

            # 3. Muat Data Anchor (contoh: BTC)
            res_anchor = loader.load(symbols=[anchor], start_date=start, end_date=end) # Sesuaikan argumen loader
            if res_anchor.is_err():
                return Err(str(res_anchor.unwrap_err()))
            
            df_anchor = res_anchor.unwrap()
            assert df_anchor is not None, "Critical: Anchor DataFrame is None!"

            # 4. Penggabungan Matriks (Modern Polars Join)
            # Fix: Menggunakan argumen `on=` bukan `columns=`, dan menambahkan suffix
            df_merged = df_target.join(
                df_anchor, 
                on="timestamp", 
                how="inner", 
                suffix="_anchor"
            )

            return Ok(df_merged)

        except Exception as e:
            return Err(f"Data merge crashed: {str(e)}")

    def _run_strategy(self, data: pd.DataFrame) -> Result[pl.DataFrame, str]:
        try:
            strat = self.strategy
            assert strat is not None, "Critical: Strategy is None"

            df_feed = data.copy()            
            # Langsung suapkan data! kalman_mr.py Anda sudah pintar mencari 'close_'
            sig_res = strat.generate_signals(df_feed)
            
            if sig_res.is_err():
                return Err(str(sig_res.unwrap_err()))
               
            df_signals = sig_res.unwrap()
            assert df_signals is not None, "Critical: Signal DataFrame is None"
            if 'close_DOGE' in data.columns:
                df_signals['close_DOGE'] = data['close_DOGE']
            if 'close_BTC' in data.columns:
                df_signals['close_BTC'] = data['close_BTC']

            
            # --- 🛡️ FILTER ANTI-RACUN POLARS START ---
            # Mengamankan kolom 'signal_metadata' (dict/mappingproxy) menjadi teks
            # agar mesin Polars (Arrow) tidak Crash.
            for col in df_signals.columns:
                if df_signals[col].dtype == 'object':
                    df_signals[col] = df_signals[col].astype(str)
            # --- 🛡️ FILTER ANTI-RACUN POLARS END ---

            df_pl = pl.from_pandas(df_signals)
            return Ok(df_pl)
            
        except Exception as e:
            print(f"\n[!!!] CRASH DI PIPELINE: {str(e)}")
            return Err(f"Strategy run crashed: {str(e)}")

    def _apply_risk_management(self):
        df = self.state.artifacts["processed_df"]
        self.state.artifacts["processed_df"] = df
        return Ok(None)

    def _simulate_execution(self):
        df = self.state.artifacts["processed_df"]
        target_col = f"close_{self.config.target_symbol}"
        
        if target_col not in df.columns:
            self.logger.error(f"No price column '{target_col}' found for PnL calculation")
            return
        signal_col = None
        if signal_col:
            # Bersihkan spasi dan jadikan huruf besar
            df = df.with_columns(pl.col(signal_col).cast(pl.Utf8).str.to_uppercase().str.strip_chars().alias("signal_str"))
            
            # STRICT TYPING: Gunakan pl.Float64 secara eksplisit agar Forward Fill tidak error
            df = df.with_columns(
                pl.when(pl.col("signal_str").is_in(["LONG", "1", "1.0", "BUY"]))
                  .then(pl.lit(1.0, dtype=pl.Float64))
                  .when(pl.col("signal_str").is_in(["SHORT", "-1", "-1.0", "SELL"]))
                  .then(pl.lit(-1.0, dtype=pl.Float64))
                  .when(pl.col("signal_str").is_in(["EXIT", "FLAT", "STOP", "0", "0.0"]))
                  .then(pl.lit(0.0, dtype=pl.Float64))
                  .otherwise(pl.lit(None, dtype=pl.Float64))
                  .alias("target_position")
            )
            df = df.with_columns(
                pl.col("target_position").forward_fill().fill_null(0.0).alias("position")
            )
            df = df.with_columms(
                pl.when(pl.col("timestamp") == pl.col("timestamp").max())
                .then(pl.lit(0.0, dtype=pl.Float64))
                .otherwise(pl.col("position"))
                .alias("position")
                    )

        else:
            self.logger.warning("🚨 TIDAK ADA KOLOM SINYAL DITEMUKAN! Posisi dipaksa 0.")
            df = df.with_columns(pl.lit(0.0, dtype=pl.Float64).alias("position"))

        # ------------------------------------------

        # Lakukan kalkulasi PnL
        df = df.with_columns([
            (pl.col("position").shift(1).fill_null(0.0) * pl.col(target_col).diff()).alias("pnl_step")
        ]).with_columns([
            pl.col("pnl_step").cum_sum().alias("cumulative_returns")
        ])
        
        self.state.artifacts["final_df"] = df

    def _generate_analytics(self):
        # Deferred import to avoid circular dependency
        try:
            from research.strategy.optimization.objective import QuantumScoreKeeper
            
            df = self.state.artifacts["final_df"]
            score_keeper = QuantumScoreKeeper()
            eval_res = score_keeper.evaluate(df)
            
            if eval_res.is_ok():
                metrics = eval_res.unwrap()
                assert metrics is not None
                self.state.metrics = {
                    "smart_score": metrics.smart_score,
                    "sharpe": metrics.sharpe_ratio,
                    "pnl": metrics.total_return,
                    "trades": metrics.total_trades,
                    "win_rate": metrics.win_rate,
                    "max_dd": metrics.max_drawdown
                }
                self.logger.info(f"🏁 Score: {metrics.smart_score:.4f}")
            else:
                err_msg = str(eval_res.unwrap_err())
                self.logger.error(f"🚨 SCORING GAGAL: {err_msg}")
                self.state.warnings.append(f"Scoring failed: {eval_res.unwrap_err()}")
                raise ValueError(f"ScoreKeeper menolak evaluasi: {err_msg}")
        except ImportError:
            self.logger.warning("QuantumScoreKeeper unavailable due to environment issues.")

    def _persist_results(self):
        df = self.state.artifacts["final_df"]
        res_dir = PROJECT_ROOT / "research" / "results"
        res_dir.mkdir(parents=True, exist_ok=True)
        path = res_dir / f"arb_{self.config.target_symbol}_{self.config.anchor_symbol}_{self.config.execution_id}.parquet"
        df.write_parquet(path, compression="zstd")
        self.state.artifacts["parquet_path"] = path 
        self.state.artifacts["result_paths"] = {"artifacts": str(path)}

# --- COMPATIBILITY ADAPTER ---
class AdvancedStrategyPipeline(QuantumStrategyOrchestrator):
    """Adapter for HyperParallelEngine (shotgun.py)."""
    def __init__(self, **kwargs):
        # Map kwargs to PipelineConfig fields
        config = PipelineConfig(
            execution_id=f"EXEC_{datetime.now().strftime('%H%M%S_%f')}",
            target_symbol=kwargs.get("target", "DOGE"),
            anchor_symbol=kwargs.get("anchor", "BTC"),
            start_date=kwargs.get("start", "2024-01-01"),
            end_date=kwargs.get("end", "2024-12-31"),
            warmup_days=kwargs.get("warmup_days", 30),
            entry_threshold=kwargs.get("entry_threshold", 2.0),
            exit_threshold=kwargs.get("exit_threshold", 0.5),
            stop_loss=kwargs.get("stop_loss", 4.0),
            max_position=kwargs.get("max_position", 1.0),
            hedge_ratio=kwargs.get("hedge_ratio", 1.0),
            volatility_window=kwargs.get("volatility_window", 20),
            process_noise=kwargs.get("process_noise", 1e-5),
            observation_noise=kwargs.get("observation_noise", 0.01),
            use_intercept=kwargs.get("use_intercept", True),
            risk_params=kwargs.get("risk_params", {}),
            execution_params=kwargs.get("execution_params", {}),
            analytics_config=kwargs.get("analytics_config", {})
        )
        super().__init__(config)

    def execute_pair_arbitrage(self, target: str, anchor: str, start: str, end: str) -> Result[Dict[str, Any], str]:
        self.config.target_symbol = target
        self.config.anchor_symbol = anchor
        self.config.start_date = start
        self.config.end_date = end
        
        res = self.execute()
        if res.is_ok():
            state = res.unwrap()
            assert state is not None
            return Ok({
                "id": state.config.execution_id,
                "metrics": state.metrics,
                "path": state.artifacts.get("parquet_path")
            })
        return Err(str(res.unwrap_err()))

def create_quantum_pipeline(**kwargs) -> Result[QuantumStrategyOrchestrator, str]:
    try:
        pipeline = AdvancedStrategyPipeline(**kwargs)
        return Ok(pipeline)
    except Exception as e:
        return Err(str(e))


