"""
PIPELINE ANALYTICS (THE ORCHESTRATOR) - V3.0 INDUSTRIAL GRADE
Location: research/analysis/pipeline.py
Focus: Pure orchestration of performance metrics, visualization, and reporting.
Zero tolerance for silent failures. Strict type discipline.
"""

import sys
from pathlib import Path
from typing import Dict, Any, Optional, Tuple, Union, List
from dataclasses import dataclass, field
from enum import Enum, auto
import logging
import warnings
import json

import numpy as np
import pandas as pd
import polars as pl
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.figure import Figure

from research.analysis.judgment.verdict import StrategyJudge, Judgment
from research.analysis.judgment.criteria import CompositeCriterion, create_default_acceptance_criteria
from research.analysis.models import PerformanceMetrics
# --- CORE SHARED ---
# Asumsikan core.shared sudah ada dengan Result, Ok, Err
from core.shared import Result, Ok, Err

# --- PATH BOOTSTRAP ---
PROJECT_ROOT = Path(__file__).parent.parent.parent.absolute()
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# ============================================================================
# ENUMS & DATA CLASSES (THE BLUEPRINTS)
# ============================================================================

class AnalyticsPhase(Enum):
    """Tracking phase within analytics generation."""
    VALIDATION = auto()
    METRIC_CALCULATION = auto()
    VISUALIZATION = auto()
    REPORTING = auto()
    COMPLETED = auto()
    FAILED = auto()
    JUDGMENT = auto()


@dataclass(frozen=True)
class PerformanceMetrics:
    """
    Immutable container for all calculated performance metrics.
    Used as the output of the analytics pipeline.
    """
    # Core metrics
    total_return: float
    annualized_return: float
    volatility: float
    sharpe_ratio: float
    sortino_ratio: float
    calmar_ratio: float
    max_drawdown: float
    max_drawdown_duration: int  # in days

    # Trade statistics
    total_trades: int
    winning_trades: int
    losing_trades: int
    win_rate: float
    avg_win: float
    avg_loss: float
    profit_factor: float
    expectancy: float

    # Risk metrics
    value_at_risk_95: float
    conditional_var_95: float
    ulcer_index: float

    # Time metrics
    start_date: str
    end_date: str
    total_days: int

    # Additional metadata
    config_snapshot: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to flat dictionary for serialization."""
        return {
            "total_return": self.total_return,
            "annualized_return": self.annualized_return,
            "volatility": self.volatility,
            "sharpe_ratio": self.sharpe_ratio,
            "sortino_ratio": self.sortino_ratio,
            "calmar_ratio": self.calmar_ratio,
            "max_drawdown": self.max_drawdown,
            "max_drawdown_duration": self.max_drawdown_duration,
            "total_trades": self.total_trades,
            "winning_trades": self.winning_trades,
            "losing_trades": self.losing_trades,
            "win_rate": self.win_rate,
            "avg_win": self.avg_win,
            "avg_loss": self.avg_loss,
            "profit_factor": self.profit_factor,
            "expectancy": self.expectancy,
            "value_at_risk_95": self.value_at_risk_95,
            "conditional_var_95": self.conditional_var_95,
            "ulcer_index": self.ulcer_index,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "total_days": self.total_days,
            **self.config_snapshot
        }


@dataclass
class AnalyticsConfig:
    """
    Configuration for the analytics pipeline.
    Allows customization of risk‑free rate, plot style, etc.
    """
    risk_free_rate: float = 0.0
    annualization_factor: int = 365  # trading days per year
    value_at_risk_confidence: float = 0.95
    figsize: Tuple[int, int] = (15, 10)
    plot_style: str = "seaborn-v0_8-darkgrid"
    save_plots: bool = False
    plot_output_dir: Optional[Path] = None
    verbose: bool = True
    judgment_criteria: Optional[CompositeCriterion] = None
    judge_name: str = "PipelineJudge"


# ============================================================================
# METRIC CALCULATORS (PURE FUNCTIONS, RESULT‑ORIENTED)
# ============================================================================

def calculate_returns_series(
    price_series: pd.Series,
    position_series: Optional[pd.Series] = None
) -> Result[pd.Series, str]:
    """
    Calculate returns suitable for Spread Trading (difference, not percentage).
    """
    # Guard clauses
    if price_series is None or price_series.empty:
        return Err("Price series is empty")

    # ---> 💉 SURGERY FIX: SPREAD RETURNS (Anti -inf) <---
    # Gunakan selisih absolut (diff), bukan persentase!
    returns = price_series.diff().fillna(0.0)

    if position_series is not None:
        if not isinstance(position_series, pd.Series):
            return Err(f"Expected pd.Series for position, got {type(position_series)}")
        
        # Kalikan posisi kemarin dengan selisih harga hari ini
        shifted_pos = position_series.shift(1).fillna(0.0)
        returns = returns * shifted_pos

    # Sapu bersih semua sisa -inf atau inf jika ada
    returns = returns.replace([float('inf'), float('-inf')], 0.0).fillna(0.0)

    return Ok(returns)

def calculate_annualized_return(
    total_return: float,
    days: int,
    annualization_factor: int = 365
) -> float:
    """Convert total return over days to annualized return."""
    if days <= 0:
        return 0.0
    years = days / annualization_factor
    if years <= 0:
        return 0.0
    return (1 + total_return) ** (1 / years) - 1


def calculate_volatility(
    returns: pd.Series,
    annualization_factor: int = 365
) -> float:
    """Annualized volatility."""
    if returns.empty:
        return 0.0
    return float(returns.std() * np.sqrt(annualization_factor))


def calculate_sharpe_ratio(
    returns: pd.Series,
    risk_free_rate: float = 0.0,
    annualization_factor: int = 365
) -> float:
    """Annualized Sharpe ratio."""
    if returns.empty:
        return 0.0
    excess_returns = returns - (risk_free_rate / annualization_factor)
    if excess_returns.std() == 0:
        return 0.0
    return float(np.sqrt(annualization_factor) * excess_returns.mean() / excess_returns.std())


def calculate_sortino_ratio(
    returns: pd.Series,
    risk_free_rate: float = 0.0,
    annualization_factor: int = 365
) -> float:
    """Sortino ratio (uses downside deviation)."""
    if len(returns) == 0:
        return 0.0
    excess_returns = returns - (risk_free_rate / annualization_factor)
    downside_returns = returns[returns < 0]
    if len(downside_returns) == 0:
        return 0.0
    downside_std = downside_returns.std()
    if downside_std == 0 or np.isnan(downside_std):
        return 0.0
    return float(np.sqrt(annualization_factor) * excess_returns.mean() / downside_std)


def calculate_drawdown_series(returns: pd.Series) -> pd.Series:
    """Calculate drawdown series from returns."""
    cumulative = (1 + returns).cumprod()
    running_max = cumulative.cummax()
    drawdown = (cumulative - running_max) / running_max
    return drawdown


def calculate_max_drawdown(returns: pd.Series) -> float:
    """Maximum drawdown (negative number)."""
    if returns.empty:
        return 0.0
    drawdown = calculate_drawdown_series(returns)
    return float(drawdown.min())


def calculate_max_drawdown_duration(returns: pd.Series) -> int:
    """
    Calculate the longest drawdown duration in days.
    Returns number of days.
    """
    if returns.empty:
        return 0
    drawdown = calculate_drawdown_series(returns)
    in_drawdown = drawdown < 0
    if not in_drawdown.any():
        return 0
    # Identify transitions
    transitions = in_drawdown.ne(in_drawdown.shift())
    groups = in_drawdown.groupby(transitions.cumsum())
    durations = groups.apply(lambda x: len(x) if x.iloc[0] else 0)
    return int(durations.max())


def calculate_trade_statistics(
    trades_df: pd.DataFrame
) -> Result[Dict[str, Any], str]:
    """
    Calculate trade‑level metrics from a DataFrame of trades.
    Expected columns: entry_price, exit_price, optional size.
    Returns a dictionary with ints and floats.
    """
    # Guard clauses
    if trades_df is None:
        return Err("Trades DataFrame is None")
    if not isinstance(trades_df, pd.DataFrame):
        return Err(f"Expected pd.DataFrame, got {type(trades_df)}")
    if trades_df.empty:
        return Err("Trades DataFrame is empty")

    required = ["entry_price", "exit_price"]
    missing = [c for c in required if c not in trades_df.columns]
    if missing:
        return Err(f"Missing required columns: {missing}")

    # Work on a copy to avoid modifying original
    df = trades_df.copy()

    # Calculate profit per trade
    df["profit"] = df["exit_price"] - df["entry_price"]
    if "size" in df.columns:
        df["profit"] = df["profit"] * df["size"]

    TRANSACTION_COST = 0.002
    df["fee"] = df["entry_price"].abs() * TRANSACTION_COST
    df["profit"] = df["profit"] - df["fee"]

    # Separate winning and losing trades
    winning = df[df["profit"] > 0]
    losing = df[df["profit"] < 0]

    total_trades = len(df)
    winning_trades = len(winning)
    losing_trades = len(losing)

    win_rate = winning_trades / total_trades if total_trades > 0 else 0.0

    # Gunakan float() langsung, tanpa .item() karena mean() sudah mengembalikan float
    if winning_trades > 0:
        avg_win = float(winning["profit"].mean())
        total_win = float(winning["profit"].sum())
    else:
        avg_win = 0.0
        total_win = 0.0

    if losing_trades > 0:
        avg_loss = float(losing["profit"].mean())
        total_loss = float(losing["profit"].sum())
    else:
        avg_loss = 0.0
        total_loss = 0.0

    # Profit factor: handle case with no losing trades
    if losing_trades == 0 or total_loss == 0:
        profit_factor = float('inf')
    else:
        profit_factor = abs(total_win / total_loss)

    expectancy = float(df["profit"].mean()) if total_trades > 0 else 0.0

    return Ok({
        "total_trades": int(total_trades),
        "winning_trades": int(winning_trades),
        "losing_trades": int(losing_trades),
        "win_rate": float(win_rate),
        "avg_win": float(avg_win),
        "avg_loss": float(avg_loss),
        "profit_factor": float(profit_factor) if profit_factor != float('inf') else float('inf'),
        "expectancy": float(expectancy)
    })

def calculate_var_cvar(
    returns: pd.Series,
    confidence: float = 0.95
) -> Tuple[float, float]:
    """
    Calculate Value at Risk and Conditional VaR (Expected Shortfall).
    """
    if returns.empty:
        return 0.0, 0.0
    sorted_returns = returns.sort_values()
    n = len(sorted_returns)
    var_index = int((1 - confidence) * n)
    if var_index >= n:
        var_index = n - 1
    var = float(sorted_returns.iloc[var_index])
    if var_index > 0:
        cvar = float(sorted_returns.iloc[:var_index].mean())
    else:
        cvar = var
    return var, cvar


def calculate_ulcer_index(returns: pd.Series) -> float:
    """
    Ulcer Index: square root of the mean squared drawdown.
    """
    if returns.empty:
        return 0.0
    drawdown = calculate_drawdown_series(returns)
    dd_squared = (drawdown ** 2).mean()
    return float(np.sqrt(dd_squared))


# ============================================================================
# MAIN ANALYTICS CLASS (THE ORCHESTRATOR)
# ============================================================================

class PipelineAnalytics:
    """
    Industrial‑grade analytics generator for backtest results.
    Consumes a Polars DataFrame (or Pandas) with at least:
        - timestamp
        - cumulative_returns (or price + position to compute)
    Optionally can include trades table.
    """

    def __init__(self, config: Optional[AnalyticsConfig] = None) -> None:
        self.config = config or AnalyticsConfig()
        self.logger = self._setup_logging()
        self._phase: AnalyticsPhase = AnalyticsPhase.VALIDATION
        self._metrics: Optional[PerformanceMetrics] = None
        self._results_df: Optional[Union[pl.DataFrame, pd.DataFrame]] = None
        self._trades_df: Optional[pd.DataFrame] = None
        self._judgment: Optional[Judgment] = None

    def _setup_logging(self) -> logging.Logger:
        logger = logging.getLogger("PipelineAnalytics")
        logger.setLevel(logging.INFO if self.config.verbose else logging.WARNING)
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        return logger

    def compute_metrics(
        self,
        results: Union[pl.DataFrame, pd.DataFrame],
        trades: Optional[pd.DataFrame] = None
    ) -> Result[PerformanceMetrics, str]:
        """
        Main entry point: compute all performance metrics from results DataFrame.
        Expects 'timestamp' and either 'cumulative_returns' or 'price' + 'position'.
        """
        self._phase = AnalyticsPhase.VALIDATION

        # --- Guard Clauses ---
        if results is None:
            return Err("Input results DataFrame is None")
        if isinstance(results, pl.DataFrame):
            if results.height == 0:
                return Err("Input Polars DataFrame is empty")
        elif isinstance(results, pd.DataFrame):
            if results.empty:
                return Err("Input Pandas DataFrame is empty")
        else:
            return Err(f"Unsupported type: {type(results)}. Expected Polars or Pandas DataFrame.")

        # Convert to Pandas for easier processing
        if isinstance(results, pl.DataFrame):
            df: pd.DataFrame = results.to_pandas()
        else:
            df = results.copy()

# --- Validate required columns ---
        required_cols = ["timestamp"]
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            return Err(f"Missing required columns: {missing}")

        # ---> 🧹 KEMBALI KE STANDAR ELEGAN <---
        if not pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
            try:
                # Coba konversi dengan asumsi integer milidetik
                # Gunakan pd.to_numeric dulu untuk memastikan angka
                timestamp_numeric = pd.to_numeric(df["timestamp"], errors='coerce')
                if timestamp_numeric.isna().any():
                    # Jika ada NaN, mungkin format string ISO
                    df["timestamp"] = pd.to_datetime(df["timestamp"])
                else:
                    # Semua numerik, konversi dengan unit='ms'
                    df["timestamp"] = pd.to_datetime(timestamp_numeric, unit='ms')
            except Exception:
                # Fallback terakhir
                try:
                    df["timestamp"] = pd.to_datetime(df["timestamp"])
                except Exception as e2:
                    return Err(f"Cannot convert timestamp to datetime: {e2}")

        df = df.sort_values("timestamp").reset_index(drop=True)

        # --- Determine returns series ---
        returns: Optional[pd.Series] = None

        if "cumulative_returns" in df.columns:
            returns_series = df["cumulative_returns"].diff().dropna()
            if returns_series.empty:
                return Err("cumulative_returns column yields no returns after diff")
            returns = returns_series
        elif "price" in df.columns and "position" in df.columns:
            price_series = df["price"]
            position_series = df["position"]
            # Safeguard: if selection returns DataFrame, take first column
            if isinstance(price_series, pd.DataFrame):
                price_series = price_series.iloc[:, 0]
            if isinstance(position_series, pd.DataFrame):
                position_series = position_series.iloc[:, 0]
            # Ensure they are Series
            if not isinstance(price_series, pd.Series):
                return Err("price column is not a Series after extraction")
            if not isinstance(position_series, pd.Series):
                return Err("position column is not a Series after extraction")

            # ---> 💉 SURGERY FIX: RUMUS RETURN UNTUK SPREAD (MENGHINDARI INF) <---
            # Karena price kita adalah log-spread, return adalah SELISIH (.diff), bukan Persentase (.pct_change)
            # Kita kalikan posisi KEMARIN (shift) dengan selisih harga HARI INI (diff)
            
            raw_diff = price_series.diff().fillna(0)
            shifted_pos = position_series.shift(1).fillna(0)

            # 1. Hitung Gross Profit (Murni selisih pergerakan harga)
            gross_returns = shifted_pos * raw_diff

            # 2. Hitung Fee (Biaya Transaksi) HANYA saat posisi berubah!
            # Jika posisi berubah (0 ke 1, atau 1 ke 0), kita bayar fee 0.1% per leg (Total 0.2% per round trip)
            pos_changes = position_series.diff().fillna(0).abs()
            FEE_PER_LEG = 0.001
            fee_series = pos_changes * price_series.abs() * FEE_PER_LEG

            # 3. Hitung Net Profit (Gross dikurangi Fee)
            returns = gross_returns - fee_series

            # 4. Bersihkan sisa-sisa angka anomali
            returns = returns.replace([float('inf'), float('-inf')], 0.0).fillna(0.0)
        else:
            return Err("DataFrame must contain either 'cumulative_returns' or both 'price' and 'position' columns.")

        # Final guard: returns must be a non‑empty Series
        if returns is None:
            return Err("Failed to derive returns series")
        if not isinstance(returns, pd.Series):
            return Err(f"Expected returns to be pd.Series, got {type(returns)}")
        if returns.empty:
            return Err("Returns series is empty after derivation")

        # --- Compute basic time metrics ---
        start_date = df["timestamp"].iloc[0].strftime("%Y-%m-%d")
        end_date = df["timestamp"].iloc[-1].strftime("%Y-%m-%d")
        total_days = (df["timestamp"].iloc[-1] - df["timestamp"].iloc[0]).days

        self._phase = AnalyticsPhase.METRIC_CALCULATION

        # --- Core metrics ---
        abs_profit = float(returns.sum())
        # Cari harga awal untuk membagi profit agar menjadi persentase yang masuk akal
        initial_price = 1.0
        if "price" in df.columns:
            first_valid = df["price"].loc[df["price"] != 0].first_valid_index()
            if first_valid is not None:
                initial_price = abs(float(df["price"].loc[first_valid]))
        total_return = abs_profit / initial_price
        annualized_return = calculate_annualized_return(total_return, total_days, self.config.annualization_factor)
        volatility = calculate_volatility(returns, self.config.annualization_factor)
        sharpe = calculate_sharpe_ratio(returns, self.config.risk_free_rate, self.config.annualization_factor)
        sortino = calculate_sortino_ratio(returns, self.config.risk_free_rate, self.config.annualization_factor)
        max_dd = calculate_max_drawdown(returns)

        max_dd_duration_minutes = calculate_max_drawdown_duration(returns)
        max_dd_duration = int(max_dd_duration_minutes / 1440)
        calmar = abs(annualized_return / max_dd) if max_dd != 0 else 0.0

        # --- Trade metrics (if trades provided) ---
        trade_stats: Dict[str, Any] = {}
        
        if (trades is None or trades.empty) and "price" in df.columns and "position" in df.columns:
            trades_list = []
            entry_price = 0.0
            current_pos = 0

            pos_arr = df["position"].values
            price_arr = df["price"].values

            for i in range(len(pos_arr)):
                pos = pos_arr[i]
                price = price_arr[i] # ---> 💉 SURGERY FIX: HARUS price_arr, bukan pos_arr! <---

                if pos != current_pos:
                    if current_pos != 0:
                        trades_list.append({
                            "entry_price": entry_price,
                            "exit_price": price,
                            "size": float(current_pos)
                        })
                    if pos != 0:
                        entry_price = price

                current_pos = pos
            trades = pd.DataFrame(trades_list)

        if trades is not None and not trades.empty:
            trade_res = calculate_trade_statistics(trades)
            if trade_res.is_ok():
                trade_stats = trade_res.unwrap()
            else:
                # Log warning but continue with empty stats
                self.logger.warning(f"Trade statistics calculation failed: {trade_res.unwrap_err()}")

        # --- Risk metrics ---
        var_95, cvar_95 = calculate_var_cvar(returns, self.config.value_at_risk_confidence)
        ulcer = calculate_ulcer_index(returns)

        # --- Assemble metrics object ---
        metrics = PerformanceMetrics(
            total_return=total_return,
            annualized_return=annualized_return,
            volatility=volatility,
            sharpe_ratio=sharpe,
            sortino_ratio=sortino,
            calmar_ratio=calmar,
            max_drawdown=max_dd,
            max_drawdown_duration=max_dd_duration,
            total_trades=trade_stats.get("total_trades", 0),
            winning_trades=trade_stats.get("winning_trades", 0),
            losing_trades=trade_stats.get("losing_trades", 0),
            win_rate=trade_stats.get("win_rate", 0.0) * 100,
            avg_win=trade_stats.get("avg_win", 0.0),
            avg_loss=trade_stats.get("avg_loss", 0.0),
            profit_factor=trade_stats.get("profit_factor", 0.0),
            expectancy=trade_stats.get("expectancy", 0.0),
            value_at_risk_95=var_95,
            conditional_var_95=cvar_95,
            ulcer_index=ulcer,
            start_date=start_date,
            end_date=end_date,
            total_days=total_days,
            config_snapshot=self._get_config_snapshot()
        )

        self._metrics = metrics
        self._results_df = df
        self._trades_df = trades
        self._phase = AnalyticsPhase.COMPLETED
        return Ok(metrics)

    def _get_config_snapshot(self) -> Dict[str, Any]:
        """Return a snapshot of the config for metadata."""
        return {
            "risk_free_rate": self.config.risk_free_rate,
            "annualization_factor": self.config.annualization_factor,
            "var_confidence": self.config.value_at_risk_confidence,
        }


    def judge(self, criteria: Optional[CompositeCriterion] = None) -> Result[Judgment, str]:
        """
        Evaluate the computed metrics against a set of criteria.
        If no criteria provided, uses the default from config or creates default.
        """
        if self._metrics is None:
            return Err("No metrics computed. Call compute_metrics() first.")

        self._phase = AnalyticsPhase.JUDGMENT

        # Determine which criteria to use
        if criteria is None:
            criteria = self.config.judgment_criteria or create_default_acceptance_criteria()

        # Evaluate composite to get a single CriterionResult
        res = criteria.evaluate(self._metrics)
        if res.is_err():
            return Err(res.unwrap_err())
        criterion_result = res.unwrap()

        # Use judge to produce judgment (even with one result)
        judge = StrategyJudge(name=self.config.judge_name)
        judge_res = judge.judge([criterion_result])
        if judge_res.is_ok():
            self._judgment = judge_res.unwrap()
        return judge_res


    # ============================================================================
    # VISUALIZATION (THE DOPAMINE HIT)
    # ============================================================================

    def generate_plots(
        self,
        save: Optional[bool] = None,
        output_dir: Optional[Path] = None
    ) -> Result[Dict[str, Figure], str]:
        """
        Create a comprehensive set of performance plots.
        Returns a dictionary of figure names to matplotlib Figure objects.
        """
        if self._metrics is None or self._results_df is None:
            return Err("No metrics computed yet. Call compute_metrics() first.")

        self._phase = AnalyticsPhase.VISUALIZATION
        save_plots = save if save is not None else self.config.save_plots
        out_dir = output_dir or self.config.plot_output_dir
        if save_plots and out_dir is None:
            return Err("save_plots=True but no output directory provided.")

        figures: Dict[str, Figure] = {}

        # --- Set style ---
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            plt.style.use(self.config.plot_style)

        df = self._results_df  # Pandas DataFrame

        # Determine returns series for plots (same logic as compute_metrics)
        returns: Optional[pd.Series] = None
        if "cumulative_returns" in df.columns:
            returns = df["cumulative_returns"].diff().dropna()
        elif "price" in df.columns and "position" in df.columns:
            price_series = df["price"]
            position_series = df["position"]
            if isinstance(price_series, pd.DataFrame):
                price_series = price_series.iloc[:, 0]
            if isinstance(position_series, pd.DataFrame):
                position_series = position_series.iloc[:, 0]
            if isinstance(price_series, pd.Series) and isinstance(position_series, pd.Series):
                returns_res = calculate_returns_series(price_series, position_series)
                if returns_res.is_ok():
                    returns = returns_res.unwrap()
        if returns is not None and returns.empty:
            returns = None

                # 1. Equity curve with drawdown
        fig1, axes = plt.subplots(2, 1, figsize=self.config.figsize, sharex=True)
        ax1, ax2 = axes

        # Equity curve
        if "cumulative_returns" in df.columns:
            cumulative = (1 + df["cumulative_returns"]).cumprod()
            ax1.plot(df["timestamp"], cumulative, color='blue', linewidth=1.5, label='Equity')
        elif "price" in df.columns:
            price_series = df["price"]
            if isinstance(price_series, pd.DataFrame):
                price_series = price_series.iloc[:, 0]
            if isinstance(price_series, pd.Series):
                first_price = price_series.iloc[0]
                if first_price != 0:
                    cumulative = price_series / first_price
                    ax1.plot(df["timestamp"], cumulative, color='blue', linewidth=1.5, label='Equity')
        else:
            ax1.text(0.5, 0.5, "No equity data", ha='center', va='center', transform=ax1.transAxes)

        ax1.set_ylabel('Cumulative Return')
        ax1.set_title('Equity Curve')
        ax1.legend(loc='upper left')
        ax1.grid(True, alpha=0.3)

        # Drawdown
        if returns is not None and not returns.empty:
            drawdown = calculate_drawdown_series(returns)
            # Align timestamps
            if len(drawdown) == len(df) - 1:
                drawdown_index = df["timestamp"].iloc[1:]
            elif len(drawdown) == len(df):
                drawdown_index = df["timestamp"]
            else:
                drawdown_index = df["timestamp"].iloc[-len(drawdown):]
            ax2.fill_between(drawdown_index, 0, drawdown * 100, color='red', alpha=0.3, label='Drawdown %')
        else:
            ax2.text(0.5, 0.5, "No returns data", ha='center', va='center', transform=ax2.transAxes)

        ax2.set_ylabel('Drawdown (%)')
        ax2.set_xlabel('Date')
        ax2.set_title('Drawdown')
        ax2.legend(loc='lower left')
        ax2.grid(True, alpha=0.3)

        # Format x-axis
        for ax in axes:
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
            ax.xaxis.set_major_locator(mdates.AutoDateLocator())
            plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right')

        plt.tight_layout()
        figures["equity_drawdown"] = fig1

        # 2. Returns distribution
        if returns is not None and not returns.empty:
            fig2, ax = plt.subplots(figsize=(self.config.figsize[0], 5))
            ax.hist(returns * 100, bins=50, color='purple', alpha=0.7, edgecolor='black')
            ax.axvline(x=0, color='red', linestyle='--', linewidth=1)
            ax.set_xlabel('Daily Return (%)')
            ax.set_ylabel('Frequency')
            ax.set_title('Distribution of Daily Returns')
            ax.grid(True, alpha=0.3)
            plt.tight_layout()
            figures["returns_dist"] = fig2

        # 3. Rolling Sharpe (if enough data)
        if returns is not None and len(returns) > 252:
            fig3, ax = plt.subplots(figsize=(self.config.figsize[0], 5))
            rolling_sharpe = returns.rolling(252).apply(
                lambda x: calculate_sharpe_ratio(x, self.config.risk_free_rate, 252)
            )
            if len(rolling_sharpe) == len(df) - 1:
                time_index = df["timestamp"].iloc[1:]
            elif len(rolling_sharpe) == len(df):
                time_index = df["timestamp"]
            else:
                time_index = df["timestamp"].iloc[-len(rolling_sharpe):]
            ax.plot(time_index, rolling_sharpe, color='green', linewidth=1)
            ax.axhline(y=1.0, color='gray', linestyle='--', alpha=0.7)
            ax.set_ylabel('Sharpe Ratio (1y rolling)')
            ax.set_title('Rolling Sharpe Ratio')
            ax.grid(True, alpha=0.3)
            plt.tight_layout()
            figures["rolling_sharpe"] = fig3

        # 4. Trade markers (if trades available)
        if self._trades_df is not None and not self._trades_df.empty and "price" in df.columns:
            fig4, ax = plt.subplots(figsize=self.config.figsize)
            price_series = df["price"]
            if isinstance(price_series, pd.DataFrame):
                price_series = price_series.iloc[:, 0]
            if isinstance(price_series, pd.Series):
                ax.plot(df["timestamp"], price_series, color='black', alpha=0.5, linewidth=1)
                if "entry_time" in self._trades_df.columns and "entry_price" in self._trades_df.columns:
                    ax.scatter(self._trades_df["entry_time"], self._trades_df["entry_price"],
                               marker='^', color='green', s=100, label='Entry', zorder=5)
                if "exit_time" in self._trades_df.columns and "exit_price" in self._trades_df.columns:
                    ax.scatter(self._trades_df["exit_time"], self._trades_df["exit_price"],
                               marker='v', color='red', s=100, label='Exit', zorder=5)
                ax.set_xlabel('Date')
                ax.set_ylabel('Price')
                ax.set_title('Trades on Price Chart')
                ax.legend()
                ax.grid(True, alpha=0.3)
                plt.tight_layout()
                figures["trades"] = fig4

        # Save if requested
        if save_plots and out_dir:
            out_dir.mkdir(parents=True, exist_ok=True)
            for name, fig in figures.items():
                path = out_dir / f"{name}.png"
                fig.savefig(path, dpi=150, bbox_inches='tight')
                self.logger.info(f"Saved plot: {path}")

        self._phase = AnalyticsPhase.COMPLETED
        return Ok(figures)

    def generate_report(self, format: str = "text") -> Result[str, str]:
        """
        Generate a human‑readable performance report.
        Supported formats: 'text', 'markdown', 'json' (returns JSON string).
        """
        if self._metrics is None:
            return Err("No metrics computed. Call compute_metrics() first.")

        self._phase = AnalyticsPhase.REPORTING
        m = self._metrics

        if format == "json":
            return Ok(json.dumps(m.to_dict(), indent=2))

        # Text / Markdown report
        lines: List[str] = []
        lines.append("=" * 60)
        lines.append("PERFORMANCE REPORT")
        lines.append("=" * 60)
        lines.append(f"Period: {m.start_date}  to  {m.end_date}  ({m.total_days} days)")
        lines.append("")
        lines.append("RETURN METRICS:")
        lines.append(f"  Total Return:           {m.total_return:>10.2%}")
        lines.append(f"  Annualized Return:      {m.annualized_return:>10.2%}")
        lines.append(f"  Volatility (ann.):      {m.volatility:>10.2%}")
        lines.append(f"  Sharpe Ratio:           {m.sharpe_ratio:>10.3f}")
        lines.append(f"  Sortino Ratio:          {m.sortino_ratio:>10.3f}")
        lines.append(f"  Calmar Ratio:           {m.calmar_ratio:>10.3f}")
        lines.append(f"  Max Drawdown:           {m.max_drawdown:>10.2%}")
        lines.append(f"  Max Drawdown Duration:  {m.max_drawdown_duration:>10} days")
        lines.append("")
        lines.append("TRADE STATISTICS:")
        lines.append(f"  Total Trades:           {m.total_trades:>10}")
        lines.append(f"  Win Rate:               {m.win_rate:>10.2%}")
        lines.append(f"  Avg Win:                {m.avg_win:>10.4f}")
        lines.append(f"  Avg Loss:               {m.avg_loss:>10.4f}")
        lines.append(f"  Profit Factor:          {m.profit_factor:>10.3f}")
        lines.append(f"  Expectancy:             {m.expectancy:>10.4f}")
        lines.append("")
        lines.append("RISK METRICS:")
        lines.append(f"  VaR (95%):              {m.value_at_risk_95:>10.2%}")
        lines.append(f"  CVaR (95%):             {m.conditional_var_95:>10.2%}")
        lines.append(f"  Ulcer Index:            {m.ulcer_index:>10.4f}")
        lines.append("=" * 60)

        report = "\n".join(lines)
        if format == "markdown":
            report = report.replace("=" * 60, "---")

        self._phase = AnalyticsPhase.COMPLETED
        return Ok(report)

    # ============================================================================
    # UTILITY
    # ============================================================================

    @property
    def phase(self) -> str:
        return self._phase.name

    @property
    def metrics(self) -> Optional[PerformanceMetrics]:
        return self._metrics


# ============================================================================
# FACTORY / QUICK ACCESS
# ============================================================================

def create_analytics(config: Optional[Union[Dict[str, Any], AnalyticsConfig]] = None) -> PipelineAnalytics:
    """Convenience factory for PipelineAnalytics."""
    if config is None:
        return PipelineAnalytics()
    if isinstance(config, dict):
        cfg = AnalyticsConfig(**config)
    elif isinstance(config, AnalyticsConfig):
        cfg = config
    else:
        raise TypeError("config must be dict or AnalyticsConfig")
    return PipelineAnalytics(cfg)


def quick_metrics(
    df: Union[pl.DataFrame, pd.DataFrame],
    trades: Optional[pd.DataFrame] = None,
    **kwargs: Any
) -> Result[PerformanceMetrics, str]:
    """One‑shot function to compute metrics without instantiating the class."""
    analytics = PipelineAnalytics()
    return analytics.compute_metrics(df, trades)


# ============================================================================
# END
# ============================================================================
