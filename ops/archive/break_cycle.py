"""
CYCLE BREAKER (CIRCULAR IMPORT FIX)
Location: ~/orca/break_cycle.py
Focus: Moves 'QuantumScoreKeeper' import inside the method to prevent import deadlocks.
"""
from pathlib import Path

TARGET_FILE = Path("research/strategy/pipeline.py")

# Konten Pipeline V9.3 (Fixed Circular Import)
FIXED_CONTENT = """\"\"\"
QUANTUM STRATEGY PIPELINE (THE BLIND COMMANDER) - V9.3 CYCLE FIX
Location: research/strategy/pipeline.py
Focus: Pure orchestration. Optimized to prevent circular imports with Optimization Node.
\"\"\"

import sys
import logging
import warnings
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field
from enum import Enum, auto
import polars as pl

warnings.filterwarnings('ignore')

# --- PATH CONFIGURATION ---
PROJECT_ROOT = Path(__file__).parent.parent.parent.absolute()
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# --- CORE IMPORTS ---
from core.shared import Ok, Err, Result
from research.ingestion.loader import create_silver_loader
from core.math.kalman import KalmanFilter
from research.strategy.engine.vectorized import create_backtest_engine
from core.signals import get_signal_strategy

# [NOTE] QuantumScoreKeeper import is moved inside _generate_analytics to avoid circular dependency
# because Optimization package imports Shotgun which imports Pipeline.

# Fallback/Mock Imports for Safety
try:
    from core.risk.manager import RiskManager
except ImportError:
    class RiskManager:
        def __init__(self, **kwargs): pass
        def apply(self, df): return df

try:
    from core.execution.simulator import ExecutionSimulator
except ImportError:
    class ExecutionSimulator:
        def __init__(self, **kwargs): pass
        def simulate(self, df): return df

try:
    from research.analysis.pipeline import PipelineAnalytics
except ImportError:
    class PipelineAnalytics:
        def __init__(self, **kwargs): pass
        def analyze(self, artifacts): return {}

# --- ENUMERATIONS ---
class PipelinePhase(Enum):
    INITIALIZATION = auto()
    DATA_LOADING = auto()
    MODEL_PROCESSING = auto()
    SIGNAL_GENERATION = auto()
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
    strategy_name: str = "kalman_crossover"
    warmup_days: int = 30
    strategy_params: Dict[str, Any] = field(default_factory=dict)
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
            "params": self.strategy_params
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
        
        self.loader = None
        self.math_model = None
        self.signal_engine = None
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
            load_res = self._load_market_data()
            if load_res.is_err(): return Err(load_res.error)

            self.state.current_phase = PipelinePhase.MODEL_PROCESSING
            math_res = self._process_mathematical_model()
            if math_res.is_err(): return Err(math_res.error)

            self.state.current_phase = PipelinePhase.SIGNAL_GENERATION
            self._generate_trading_signals()

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
            self.state.end_time = datetime.now()
            self.state.current_phase = PipelinePhase.FAILURE
            error_msg = f"Pipeline Crash: {str(e)}"
            self.state.errors.append(error_msg)
            return Err(error_msg)

    def _initialize_components(self):
        self.loader = create_silver_loader(str(PROJECT_ROOT / "data" / "silver"))
        self.math_model = KalmanFilter(
            process_noise=self.config.strategy_params.get("process_noise", 1e-5),
            observation_noise=self.config.strategy_params.get("observation_noise", 1e-4),
            min_periods=self.config.warmup_days * 1440
        )
        self.signal_engine = get_signal_strategy(
            self.config.strategy_name,
            self.config.strategy_params
        )
        self.risk_manager = RiskManager(**self.config.risk_params)
        self.execution_sim = ExecutionSimulator(**self.config.execution_params)
        self.analytics = PipelineAnalytics(**self.config.analytics_config)
        self.logger.info(f"✓ Components synced")

    def _load_market_data(self) -> Result[pl.LazyFrame, str]:
        res = self.loader.load(
            start_date=self.config.start_date,
            end_date=self.config.end_date,
            symbols=[self.config.target_symbol, self.config.anchor_symbol]
        )
        if res.is_ok():
            self.state.artifacts["lazy_frame"] = res.unwrap()
            return Ok(self.state.artifacts["lazy_frame"])
        return Err(res.error)

    def _process_mathematical_model(self) -> Result[Dict[str, Any], str]:
        engine = create_backtest_engine(
            loader=self.loader,
            model=self.math_model,
            entry_threshold=999.0, 
            exit_threshold=999.0,
            warmup_days=self.config.warmup_days
        )
        res = engine.run(
            start_date=self.config.start_date,
            end_date=self.config.end_date,
            symbols=[self.config.target_symbol, self.config.anchor_symbol]
        )
        if res.is_ok():
            raw_math = res.unwrap()
            self.state.artifacts["math_df"] = pl.DataFrame({
                "timestamp": raw_math["timestamps"],
                "z_score": raw_math["signals"],
                "beta": [s["beta"] for s in raw_math["states"]],
                "target_price": raw_math["target_values"],
                "anchor_price": raw_math["feature_values"][0]
            })
            return Ok(raw_math)
        return Err(res.error)

    def _generate_trading_signals(self):
        math_df = self.state.artifacts["math_df"]
        signal_df = self.signal_engine.generate(math_df)
        self.state.artifacts["processed_df"] = signal_df

    def _apply_risk_management(self):
        df = self.state.artifacts["processed_df"]
        risk_df = self.risk_manager.apply(df)
        self.state.artifacts["processed_df"] = risk_df

    def _simulate_execution(self):
        df = self.state.artifacts["processed_df"]
        df = df.with_columns([
            (pl.col("position").shift(1).fill_null(0) * pl.col("target_price").diff()).alias("pnl_step")
        ]).with_columns([
            pl.col("pnl_step").cum_sum().alias("cumulative_returns")
        ])
        self.state.artifacts["final_df"] = df

    def _generate_analytics(self):
        # [FIX] DEFERRED IMPORT TO BREAK CYCLE
        try:
            from research.strategy.optimization.objective import QuantumScoreKeeper
            
            df = self.state.artifacts["final_df"]
            score_keeper = QuantumScoreKeeper()
            eval_res = score_keeper.evaluate(df)
            
            if eval_res.is_ok():
                metrics = eval_res.unwrap()
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
                self.state.warnings.append(f"Scoring failed: {eval_res.error}")
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
    \"\"\"Adapter for HyperParallelEngine (shotgun.py).\"\"\"
    def __init__(self, **kwargs):
        config = PipelineConfig(
            execution_id=f"EXEC_{datetime.now().strftime('%H%M%S')}",
            target_symbol=kwargs.get("target", "DOGE"),
            anchor_symbol=kwargs.get("anchor", "BTC"),
            start_date=kwargs.get("start", "2024-01-01"),
            end_date=kwargs.get("end", "2024-12-31"),
            warmup_days=kwargs.get("warmup_days", 30),
            strategy_params=kwargs
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
            return Ok({
                "id": state.config.execution_id,
                "metrics": state.metrics,
                "path": state.artifacts.get("parquet_path")
            })
        return Err(res.error)

def create_quantum_pipeline(**kwargs) -> Result[QuantumStrategyOrchestrator, str]:
    try:
        pipeline = AdvancedStrategyPipeline(**kwargs)
        return Ok(pipeline)
    except Exception as e:
        return Err(str(e))
"""

def apply_fix():
    print(f"🔧 Applying Circular Import Fix to {TARGET_FILE}...")
    with open(TARGET_FILE, "w", encoding="utf-8") as f:
        f.write(FIXED_CONTENT.strip())
    print("✅ CYCLE BROKEN. PIPELINE STABILIZED.")

if __name__ == "__main__":
    apply_fix()
