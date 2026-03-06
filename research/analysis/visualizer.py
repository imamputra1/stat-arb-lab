"""
STRATEGY VISUALIZER (THE PIT WALL) - V4.0 INDUSTRIAL GRADE
Location: research/analysis/visualizer.py
Focus: Create comprehensive dashboards from backtest results.
       Supports both single file analysis and batch generation.
"""

import sys
from pathlib import Path
from typing import Optional, List
import logging

import polars as pl
import numpy as np
import matplotlib.pyplot as plt
from matplotlib import gridspec
from matplotlib.figure import Figure

# --- PATH INJECTION ---
PROJECT_ROOT = Path(__file__).parent.parent.parent.absolute()
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# --- CORE SHARED ---
from core.shared import Result, Ok, Err


# ============================================================================
# STYLING & CONSTANTS
# ============================================================================

# Color scheme optimized for dark background (standard in trading terminals)
COLORS = {
    'signal': '#00FFFF',          # Cyan
    'threshold_entry': '#FF4444',  # Red
    'threshold_exit': '#FFAA00',   # Orange
    'beta': '#FF00FF',            # Magenta
    'pnl': '#00FF00',             # Lime
    'position_long': '#00FF00',   # Green
    'position_short': '#FF0000',  # Red
    'grid': '#333333',            # Dark gray
    'text': '#FFFFFF',            # White
}

# Default figure settings
plt.style.use('dark_background')
plt.rcParams.update({
    'figure.figsize': (16, 10),
    'axes.grid': True,
    'grid.alpha': 0.2,
    'axes.edgecolor': 'gray',
    'lines.linewidth': 1.2,
    'font.family': 'monospace',
    'text.color': COLORS['text'],
    'axes.labelcolor': COLORS['text'],
    'xtick.color': COLORS['text'],
    'ytick.color': COLORS['text'],
})


# ============================================================================
# LOGGING SETUP
# ============================================================================

def _setup_logger() -> logging.Logger:
    logger = logging.getLogger("Visualizer")
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    logger.propagate = False
    return logger


# ============================================================================
# MAIN VISUALIZER CLASS
# ============================================================================

class StrategyVisualizer:
    """
    Professional dashboard creator for backtest results.
    Generates multi‑panel plots showing Z‑score, hedge ratio, positions, and PnL.
    """

    def __init__(self, results_dir: Optional[Path] = None, output_dir: Optional[Path] = None) -> None:
        """
        Initialize visualizer with directories.
        
        Args:
            results_dir: Where to look for parquet files (default: research/results)
            output_dir:  Where to save generated plots (default: same as results_dir)
        """
        self.results_dir = results_dir or (PROJECT_ROOT / "research" / "results")
        self.output_dir = output_dir or self.results_dir
        self.logger = _setup_logger()

    def get_latest_artifact(self) -> Result[Path, str]:
        """
        Find the most recent result artifact (arb_*.parquet) in results_dir.
        """
        files = list(self.results_dir.glob("arb_*.parquet"))
        if not files:
            return Err(f"No artifacts found in {self.results_dir}")
        latest = max(files, key=lambda f: f.stat().st_mtime)
        return Ok(latest)

    def visualize(
        self,
        file_path: Optional[Path] = None,
        entry_threshold: float = 2.0,
        exit_threshold: float = 0.5,
        save: bool = True,
        show: bool = False,
        output_filename: Optional[str] = None,
    ) -> Result[Path, str]:
        """
        Generate and optionally save a dashboard for a backtest result.
        
        Args:
            file_path: Path to parquet file. If None, uses latest.
            entry_threshold: Entry Z‑score threshold (for plotting).
            exit_threshold:  Exit Z‑score threshold (for plotting).
            save: Whether to save the plot to disk.
            show: Whether to display the plot (blocking).
            output_filename: Custom output filename (default: same stem with .png).
        
        Returns:
            Path to saved plot if save=True, otherwise Ok(None) but with path.
        """
        # 1. Determine target file
        if file_path is None:
            artifact_res = self.get_latest_artifact()
            if artifact_res.is_err():
                return Err(artifact_res.unwrap_err())
            target_path = artifact_res.unwrap()
        else:
            target_path = Path(file_path)
            if not target_path.exists():
                return Err(f"File not found: {target_path}")

        self.logger.info(f"🎨 Visualizing: {target_path.name}")

        # 2. Read data
        try:
            df = pl.read_parquet(target_path)
        except Exception as e:
            return Err(f"Failed to read Parquet: {e}")

        if df.height == 0:
            return Err("DataFrame is empty")

        # 3. Validate required columns
        required = {"timestamp", "z_score", "beta", "position"}
        missing = required - set(df.columns)
        if missing:
            return Err(f"Missing required columns: {missing}")

        # 4. Create dashboard
        fig = self._create_dashboard(
            df=df,
            entry=entry_threshold,
            exit_t=exit_threshold,
            title=target_path.stem,
        )

        # 5. Save if requested
        if save:
            if output_filename is None:
                output_filename = target_path.stem + ".png"
            output_path = self.output_dir / output_filename
            fig.savefig(output_path, dpi=150, bbox_inches='tight')
            self.logger.info(f"💾 Saved plot to: {output_path}")
        else:
            output_path = None

        # 6. Show if requested
        if show:
            plt.show()
        else:
            plt.close(fig)

        return Ok(output_path) if output_path else Ok(Path(""))

    def _create_dashboard(
        self,
        df: pl.DataFrame,
        entry: float,
        exit_t: float,
        title: str,
    ) -> Figure:
        """
        Internal method to create the multi‑panel plot.
        """
        # Convert to numpy for plotting (matplotlib works best with numpy)
        ts = df["timestamp"].to_numpy()
        z = df["z_score"].to_numpy()
        beta = df["beta"].to_numpy()
        pos = df["position"].to_numpy()

        # Determine PnL column
        pnl_col = None
        for col in ["cumulative_returns", "cum_pnl", "pnl"]:
            if col in df.columns:
                pnl_col = col
                break
        pnl = df[pnl_col].to_numpy() if pnl_col else np.zeros(len(df))

        # Create figure with GridSpec for custom layout
        fig = plt.figure(figsize=(18, 12))
        gs = gridspec.GridSpec(4, 1, height_ratios=[2, 1, 1, 1.2], hspace=0.15)

        # Panel 1: Z‑score (innovation)
        ax1 = fig.add_subplot(gs[0])
        ax1.plot(ts, z, color=COLORS['signal'], label='Innovation Z‑Score', alpha=0.9)
        ax1.axhline(entry, color=COLORS['threshold_entry'], linestyle='--', label=f'Entry (±{entry})')
        ax1.axhline(-entry, color=COLORS['threshold_entry'], linestyle='--')
        ax1.axhline(exit_t, color=COLORS['threshold_exit'], linestyle=':', alpha=0.5, label=f'Exit (±{exit_t})')
        ax1.axhline(-exit_t, color=COLORS['threshold_exit'], linestyle=':', alpha=0.5)
        ax1.set_title(f"TELEMETRY: {title}", loc='left', color='gold', fontweight='bold')
        ax1.legend(loc='upper left', ncol=3)
        ax1.set_ylabel("Z‑Score")
        ax1.grid(True, alpha=0.3)

        # Panel 2: Beta (hedge ratio)
        ax2 = fig.add_subplot(gs[1], sharex=ax1)
        ax2.plot(ts, beta, color=COLORS['beta'], label='Adaptive β (Hedge Ratio)')
        ax2.set_ylabel("β")
        ax2.legend(loc='upper left')
        ax2.grid(True, alpha=0.3)

        # Panel 3: Positions
        ax3 = fig.add_subplot(gs[2], sharex=ax1)
        ax3.fill_between(ts, 0, pos, where=(pos > 0), color=COLORS['position_long'], alpha=0.4, label='Long')
        ax3.fill_between(ts, 0, pos, where=(pos < 0), color=COLORS['position_short'], alpha=0.4, label='Short')
        ax3.set_yticks([-1, 0, 1])
        ax3.set_yticklabels(['Short', 'Neutral', 'Long'])
        ax3.set_ylabel("Position")
        ax3.legend(loc='upper left')
        ax3.grid(True, alpha=0.3)

        # Panel 4: Cumulative PnL
        ax4 = fig.add_subplot(gs[3], sharex=ax1)
        ax4.plot(ts, pnl, color=COLORS['pnl'], linewidth=2, label='Equity Curve')
        ax4.fill_between(ts, 0, pnl, color=COLORS['pnl'], alpha=0.1)
        ax4.set_ylabel("Cumulative PnL")
        ax4.set_xlabel("Timestamp")
        ax4.legend(loc='upper left')
        ax4.grid(True, alpha=0.3)

        # Rotate x‑axis labels for readability
        plt.setp(ax4.xaxis.get_majorticklabels(), rotation=45, ha='right')

        return fig

    def generate_insights(self, df: pl.DataFrame, entry_t: float) -> List[str]:
        """
        Generate textual insights from the data (similar to inspector but lightweight).
        Useful for adding to reports.
        """
        insights: List[str] = []
        if "z_score" not in df.columns or "position" not in df.columns:
            return insights

        z = df["z_score"].to_numpy()
        pos = df["position"].to_numpy()
        max_abs_z = float(np.max(np.abs(z)))
        trades = int(np.sum(np.diff(pos, prepend=pos[0]) != 0))

        if max_abs_z < entry_t:
            insights.append(f"🚨 SIGNAL DEATH: Max |Z| ({max_abs_z:.2f}) never reached entry threshold ({entry_t})")
            insights.append("   → Recommendation: Reduce R (observation noise) or increase Q (process noise).")
        elif trades == 0:
            insights.append("🚨 TRADE PARALYSIS: Signals crossed threshold but no trades executed.")
            insights.append("   → Recommendation: Check engine logic or position state preservation.")
        else:
            insights.append(f"✅ ACTIVE: Strategy executed {trades} trades with max |Z| = {max_abs_z:.2f}")

        return insights


# ============================================================================
# QUICK ACCESS FUNCTION
# ============================================================================

def quick_visualize(
    file_path: Optional[Path] = None,
    entry: float = 2.0,
    exit_t: float = 0.5,
    save: bool = True,
    show: bool = False,
) -> Result[Path, str]:
    """
    One-shot visualization without instantiating the class.
    """
    visualizer = StrategyVisualizer()
    return visualizer.visualize(
        file_path=file_path,
        entry_threshold=entry,
        exit_threshold=exit_t,
        save=save,
        show=show,
    )


# ============================================================================
# END
# ============================================================================
