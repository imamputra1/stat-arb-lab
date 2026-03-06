"""
PIPELINE ANALYTICS (THE DIAGNOSTIC CENTER) - V1.0 INDUSTRIAL GRADE
Location: research/analysis/pipeline.py
Focus: 
  - Comprehensive performance metrics for backtest results.
  - Visual accessibility with high‑contrast plots.
  - Result‑oriented design with strict validation.
  - Zero tolerance for silent failures.
"""

import sys
from pathlib import Path
from typing import Dict, Any, Optional, Tuple, Union
from dataclasses import dataclass, field
from enum import Enum, auto
import logging
import warnings

import numpy as np
import pandas as pd
import polars as pl
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.figure import Figure

# --- CORE SHARED ---
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


class RiskFreeRate(Enum):
    """Standard risk‑free rate assumptions."""
    ZERO = 0.0
    US_TBILL_3M = 0.05  # 5% annualized, example
    CUSTOM = auto()


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
    annualization_factor: int = 252  # trading days per year
    value_at_risk_confidence: float = 0.95
    figsize: Tuple[int, int] = (15, 10)
    plot_style: str = "seaborn-v0_8-darkgrid"
    save_plots: bool = False
    plot_output_dir: Optional[Path] = None
    verbose: bool = True


# ============================================================================
# METRIC CALCULATORS (PURE FUNCTIONS, RESULT‑ORIENTED)
# ============================================================================

def calculate_returns_series(
    price_series: pd.Series,
    position_series: Optional[pd.Series] = None
) -> Result[pd.Series, str]:
    """
    Calculate daily returns from price series, optionally adjusted for position.
    Expects price series to be sorted ascending by time.
    """
    # Guard clauses
    if price_series is None or price_series.empty:
        return Err("Price series is empty or None")
    if not isinstance(price_series, pd.Series):
        return Err(f"Expected pd.Series, got {type(price_series)}")

    # Ensure no NaN in price
    if price_series.isnull().any():
        return Err("Price series contains NaN values")

    # Simple returns: (p_t / p_{t-1}) - 1
    returns = price_series.pct_change().dropna()

    if position_series is not None:
        # Align positions (assume position_series has same index)
        if len(position_series) != len(price_series):
            return Err("Position series length differs from price series")
        # Strategy returns = position * asset returns (lag position by 1 to avoid lookahead)
        # We'll assume position_series already contains the position for the next period.
        # For simplicity, we multiply aligned returns by position.
        # In real backtest, positions should be shifted.
        # Here we assume the input dataframe already has the correct position column.
        returns = returns * position_series.loc[returns.index]

    return Ok(returns)


def calculate_annualized_return(
    total_return: float,
    days: int,
    annualization_factor: int = 252
) -> float:
    """Convert total return over days to annualized return."""
    if days <= 0:
        return 0.0
    years = days / annualization_factor
    return (1 + total_return) ** (1 / years) - 1 if years > 0 else 0.0


def calculate_volatility(
    returns: pd.Series,
    annualization_factor: int = 252
) -> float:
    """Annualized volatility."""
    if returns.empty:
        return 0.0
    return float(returns.std() * np.sqrt(annualization_factor))


def calculate_sharpe_ratio(
    returns: pd.Series,
    risk_free_rate: float = 0.0,
    annualization_factor: int = 252
) -> float:
    """Annualized Sharpe ratio."""
    if returns.empty:
        return 0.0
    excess_returns = returns - risk_free_rate / annualization_factor
    if excess_returns.std() == 0:
        return 0.0
    return float(np.sqrt(annualization_factor) * excess_returns.mean() / excess_returns.std())


def calculate_sortino_ratio(
    returns: pd.Series,
    risk_free_rate: float = 0.0,
    annualization_factor: int = 252
) -> float:
    """Sortino ratio (uses downside deviation)."""
    if returns.empty:
        return 0.0
    excess_returns = returns - risk_free_rate / annualization_factor
    downside = returns[returns < 0].std()
    if downside == 0 or np.isnan(downside):
        return 0.0
    return float(np.sqrt(annualization_factor) * excess_returns.mean() / downside)


def calculate_drawdown_series(returns: pd.Series) -> pd.Series:
    """Calculate drawdown series from returns."""
    cumulative = (1 + returns).cumprod()
    running_max = cumulative.cummax()
    drawdown = (cumulative - running_max) / running_max
    return drawdown


def calculate_max_drawdown(returns: pd.Series) -> float:
    """Maximum drawdown (negative number, e.g., -0.25 for 25% loss)."""
    drawdown = calculate_drawdown_series(returns)
    return float(drawdown.min())


def calculate_max_drawdown_duration(returns: pd.Series) -> int:
    """
    Calculate the longest drawdown duration in days.
    Returns number of days (assuming daily data).
    """
    drawdown = calculate_drawdown_series(returns)
    # Mark periods where drawdown != 0
    in_drawdown = drawdown < 0
    # Find lengths of consecutive True
    if not in_drawdown.any():
        return 0
    # Identify transitions
    transitions = in_drawdown.ne(in_drawdown.shift())
    groups = in_drawdown.groupby(transitions.cumsum())
    durations = groups.apply(lambda x: len(x) if x.iloc[0] else 0)
    return int(durations.max())


def calculate_trade_statistics(
    trades_df: pd.DataFrame,
    price_col: str = "close"
) -> Result[Dict[str, Any], str]:
    """
    Calculate trade‑level metrics from a DataFrame of trades.
    Expected columns: entry_time, exit_time, entry_price, exit_price, size (optional).
    Returns a dictionary with mixed types (int for counts, float for ratios).
    """
    # Guard clauses
    if trades_df is None or trades_df.empty:
        return Err("No trades to analyze")

    required = ["entry_price", "exit_price"]
    missing = [c for c in required if c not in trades_df.columns]
    if missing:
        return Err(f"Missing required columns: {missing}")

    # Work on a copy to avoid modifying original
    df = trades_df.copy()

    # Calculate profit per trade
    df["profit"] = df["exit_price"] - df["entry_price"]
    if "size" in df.columns:
        df["profit"] *= df["size"]

    # Separate winning and losing trades
    winning = df[df["profit"] > 0]
    losing = df[df["profit"] < 0]

    total_trades = len(df)
    winning_trades = len(winning)
    losing_trades = len(losing)

    # Guard against division by zero
    win_rate = winning_trades / total_trades if total_trades > 0 else 0.0

    # Use .item() to extract scalar values from pandas Series (appeases strict type checkers)
    if winning_trades > 0:
        avg_win = winning["profit"].mean().item()
        total_win = winning["profit"].sum().item()
    else:
        avg_win = 0.0
        total_win = 0.0

    if losing_trades > 0:
        avg_loss = losing["profit"].mean().item()
        total_loss = losing["profit"].sum().item()
    else:
        avg_loss = 0.0
        total_loss = 0.0

    # Profit factor: handle case with no losing trades (infinite)
    if losing_trades == 0 or total_loss == 0:
        profit_factor = float('inf')
    else:
        profit_factor = abs(total_win / total_loss)

    expectancy = df["profit"].mean().item() if total_trades > 0 else 0.0

    # Return dictionary with explicit type casting to ensure Python native types
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
    var_index = int((1 - confidence) * len(sorted_returns))
    var = float(sorted_returns.iloc[var_index])
    cvar = float(sorted_returns.iloc[:var_index].mean()) if var_index > 0 else var
    return var, cvar


def calculate_ulcer_index(returns: pd.Series) -> float:
    """
    Ulcer Index: square root of the mean squared drawdown.
    """
    drawdown = calculate_drawdown_series(returns)
    # Use percentage drawdown (negative values) – ulcer uses depth, not sign.
    dd_squared = (drawdown ** 2).mean()
    return float(np.sqrt(dd_squared))


# ============================================================================
# MAIN ANALYTICS CLASS (THE ORCHESTRATOR)
# ============================================================================

class pipelineanalytics:
    """
    Industrial‑grade analytics generator for backtest results.
    Consumes a Polars DataFrame (or Pandas) with at least:
        - timestamp
        - cumulative_returns (or price + position to compute)
    Optionally can include trades table.
    """

    def __init__(self, config: Optional[AnalyticsConfig] = None):
        self.config = config or AnalyticsConfig()
        self.logger = self._setup_logging()
        self._phase = AnalyticsPhase.VALIDATION
        self._metrics: Optional[PerformanceMetrics] = None
        self._results_df: Optional[Union[pl.DataFrame, pd.DataFrame]] = None
        self._trades_df: Optional[pd.DataFrame] = None

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

        # Convert to Pandas for easier processing (most metrics use pandas)
        if isinstance(results, pl.DataFrame):
            df = results.to_pandas()
        else:
            df = results.copy()

        # --- Validate required columns ---
        required_cols = ["timestamp"]
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            return Err(f"Missing required columns: {missing}")

        # Ensure timestamp is datetime
        if not pd.api.types.is_datetime64_any_dtype(df["timestamp"]):
            try:
                df["timestamp"] = pd.to_datetime(df["timestamp"])
            except Exception as e:
                return Err(f"Cannot convert timestamp to datetime: {e}")

        df = df.sort_values("timestamp").reset_index(drop=True)

        # --- Determine returns series ---
        returns: Optional[pd.Series] = None

        if "cumulative_returns" in df.columns:
            # Derive daily returns from cumulative
            returns_series = df["cumulative_returns"].diff().dropna()
            if returns_series.empty:
                return Err("cumulative_returns column yields no returns after diff")
        elif "price" in df.columns and "position" in df.columns:
            price_series = df["price"]
            position_series = df["position"]
            # Safeguard: if selection returns DataFrame, take first column
            if isinstance(price_series, pd.DataFrame):
                price_series = price_series.iloc[:, 0]
            if isinstance(position_series, pd.DataFrame):
                position_series = position_series.iloc[:, 0]

            returns_res = calculate_returns_series(price_series, position_series)
            if returns_res.is_err():
                err_raw = returns_res.unwrap_err()
                err_msg = str(err_raw) if err_raw is not None else "Unknow error in returns calculation"
                return Err(err_msg)
            returns = returns_res.unwrap()
        else:
            # Try to infer from close_* columns? For simplicity, require explicit.
            return Err("DataFrame must contain either 'cumulative_returns' or both 'price' and 'position' columns.")

        # Final guard: returns must be a non‑empty series
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
        total_return = float((1 + returns).prod() - 1)
        annualized_return = calculate_annualized_return(total_return, total_days, self.config.annualization_factor)
        volatility = calculate_volatility(returns, self.config.annualization_factor)
        sharpe = calculate_sharpe_ratio(returns, self.config.risk_free_rate, self.config.annualization_factor)
        sortino = calculate_sortino_ratio(returns, self.config.risk_free_rate, self.config.annualization_factor)
        max_dd = calculate_max_drawdown(returns)
        max_dd_duration = calculate_max_drawdown_duration(returns)
        calmar = abs(annualized_return / max_dd) if max_dd != 0 else 0.0

        # --- Trade metrics (if trades provided) ---
        trade_stats: Dict[str, Any] = {}
        if trades is not None and not trades.empty:
            trade_res = calculate_trade_statistics(trades)
            if trade_res.is_ok():
                trade_stats = trade_res.unwrap()
            else:
                err_raw = trade_res.unwrap_err()
                err_msg = str(err_raw) if err_raw is not None else "Unknow trade statistics error"
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
            win_rate=trade_stats.get("win_rate", 0.0),
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

         # --- Extract returns series safely ---
        df = self._results_df  # Pandas DataFrame

        # Determine returns series (same logic as compute_metrics)
        returns: Optional[pd.Series] = None
        if "cumulative_returns" in df.columns:
            returns = df["cumulative_returns"].diff().dropna()
            if returns.empty:
                self.logger.warning("No returns derived from cumulative_returns")
        elif "price" in df.columns and "position" in df.columns:
            price_series = df["price"]
            position_series = df["position"]
            # Safeguard: if selection returns DataFrame, take first columns
            if isinstance(price_series, pd.DataFrame):
                price_series = price_series.iloc[:, 0]
            if isinstance(position_series, pd.DataFrame):
                position_series = position_series.iloc[:, 0]
            returns_res = calculate_returns_series(price_series, position_series)
            if returns_res.is_ok():
                returns = returns_res.unwrap()
            else:
                self.logger.warning(f"Cannot derive returns for plots: {returns_res.unwrap_err()}")
        else:
            self.logger.warning("No returns data available for plots")


        # 1. Equity curve with drawdown
        fig1, axes = plt.subplots(2, 1, figsize=self.config.figsize, sharex=True)
        ax1, ax2 = axes

        # Equity curve
        if "cumulative_returns" in df.columns:
            cumulative = (1 + self._results_df["cumulative_returns"]).cumprod()
            ax1.plot(df["timestamp"], cumulative, color='blue', linewidth=1.5, label='Equity')
        elif "price" in df.columns:
            # Use price normalized to starting value
            price_series = df["price"]
            if isinstance(price_series, pd.DataFrame):
                price_series = price_series.iloc[:, 0]
            cumulative = price_series / price_series.iloc[0]
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
            # align with timestamp (drawdown index is returns index, need to align)
            # For simplicity, we plot drawdown on same x-axis but shifted.
            # We'll just plot using the same timestamp index as returns.
            if len(drawdown) == len(df) -1:
                drawdown_index = df["timestamp"].iloc[1:]
            else:
                drawdown_index = df["timestamp"].iloc[-len(drawdown):]
            ax2.fill_between(drawdown_index, 0, drawdown * 100, color='red', alpha=0.3, label='Drawdown %')
            ax2.set.text(0.5, 0.5, "No returns data", ha='center', va='center', transform=ax2.transAxes)
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
        fig2, ax = plt.subplots(figsize=(self.config.figsize[0], 5))
        if returns is not None:
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
            ax.plot(self._results_df["timestamp"].iloc[1:][rolling_sharpe.index], rolling_sharpe, color='green', linewidth=1)
            ax.axhline(y=1.0, color='gray', linestyle='--', alpha=0.7)
            ax.set_ylabel('Sharpe Ratio (1y rolling)')
            ax.set_title('Rolling Sharpe Ratio')
            ax.grid(True, alpha=0.3)
            plt.tight_layout()
            figures["rolling_sharpe"] = fig3

        # 4. Trade markers (if trades available)
        if self._trades_df is not None and not self._trades_df.empty and "price" in self._results_df.columns:
            fig4, ax = plt.subplots(figsize=self.config.figsize)
            ax.plot(self._results_df["timestamp"], self._results_df["price"], color='black', alpha=0.5, linewidth=1)
            # Mark entries and exits
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
            import json
            return Ok(json.dumps(m.to_dict(), indent=2))

        # Text / Markdown report
        lines = []
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
            # Convert to markdown (simple)
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

def create_analytics(config: Optional[Dict[str, Any]] = None) -> PipelineAnalytics:
    """Convenience factory for PipelineAnalytics."""
    if config is None:
        return PipelineAnalytics()
    # Convert dict to AnalyticsConfig if needed
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
    **kwargs
) -> Result[PerformanceMetrics, str]:
    """One‑shot function to compute metrics without instantiating the class."""
    analytics = PipelineAnalytics()
    return analytics.compute_metrics(df, trades)


# ============================================================================
# END
# ============================================================================
