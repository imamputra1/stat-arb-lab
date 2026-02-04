"""
STRATEGY VISUALIZER (THE PIT WALL) - SYNCHRONIZED V3.1
Location: research/analysis/visualizer.py
Focus: Surgical visualization for Kalman-based Arbitrage.
"""
import sys
from pathlib import Path
from typing import Optional, Dict, Any, List
import polars as pl
import numpy as np
import matplotlib.pyplot as plt
from matplotlib import gridspec

# --- PATH INJECTION ---
PROJECT_ROOT = Path(__file__).parent.parent.parent.absolute()
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from core.shared import Ok, Err, Result

# --- STYLING ---
plt.style.use('dark_background')
plt.rcParams.update({
    'figure.figsize': (16, 10),
    'axes.grid': True,
    'grid.alpha': 0.2,
    'axes.edgecolor': 'gray',
    'lines.linewidth': 1.2,
    'font.family': 'monospace',
})

COLORS = {
    'signal': '#00FFFF',          # Cyan
    'threshold_entry': '#FF4444',  # Red
    'threshold_exit': '#FFAA00',   # Orange
    'beta': '#FF00FF',            # Magenta
    'pnl': '#00FF00',             # Lime
    'position_long': '#00FF00',   # Green
    'position_short': '#FF0000',  # Red
}

class StrategyVisualizer:
    def __init__(self, results_dir: Optional[str] = None):
        self.results_dir = Path(results_dir) if results_dir else PROJECT_ROOT / "research" / "results"

    def get_latest_artifact(self) -> Path:
        files = list(self.results_dir.glob("arb_*.parquet"))
        if not files:
            raise FileNotFoundError(f"No artifacts found in {self.results_dir}")
        return max(files, key=lambda f: f.stat().st_mtime)

    def visualize(self, 
                  file_path: Optional[str] = None,
                  entry_threshold: float = 2.0,
                  exit_threshold: float = 0.5) -> Result[Dict[str, Any], str]:
        try:
            target_path = Path(file_path) if file_path else self.get_latest_artifact()
            
            # Load data dengan kolom dinamis agar kompatibel dengan Node S
            df = pl.read_parquet(target_path)
            
            # 1. Dashboard Creation
            plot_path = target_path.with_suffix('.png')
            self._create_dashboard(df, entry_threshold, exit_threshold, plot_path)
            
            # 2. Forensic Insights
            insights = self._generate_insights(df, entry_threshold)
            
            return Ok({
                "plot_path": str(plot_path),
                "insights": insights
            })
            
        except Exception as e:
            return Err(f"Visualization Crash: {str(e)}")

    def _create_dashboard(self, df: pl.DataFrame, entry: float, exit_t: float, save_path: Path):
        ts = df["timestamp"].to_numpy()
        z = df["z_score"].to_numpy()
        beta = df["beta"].to_numpy()
        pos = df["position"].to_numpy() if "position" in df.columns else np.zeros(len(df))
        
        # FIX: Sinkronisasi kolom PnL antara 'cumulative_returns' (Pipeline) dan 'cum_pnl' (Engine)
        pnl_col = "cumulative_returns" if "cumulative_returns" in df.columns else "cum_pnl"
        pnl = df[pnl_col].to_numpy() if pnl_col in df.columns else np.zeros(len(df))

        fig = plt.figure(figsize=(18, 12))
        gs = gridspec.GridSpec(4, 1, height_ratios=[2, 1, 1, 1.2], hspace=0.15)

        # PANEL 1: Z-SCORE (Innovation)
        ax1 = fig.add_subplot(gs[0])
        ax1.plot(ts, z, color=COLORS['signal'], label='Innovation Z-Score', alpha=0.9)
        ax1.axhline(entry, color=COLORS['threshold_entry'], ls='--', label='Entry Threshold')
        ax1.axhline(-entry, color=COLORS['threshold_entry'], ls='--')
        ax1.axhline(exit_t, color=COLORS['threshold_exit'], ls=':', alpha=0.5, label='Exit Zone')
        ax1.axhline(-exit_t, color=COLORS['threshold_exit'], ls=':', alpha=0.5)
        ax1.set_title(f"TELEMETRY: {save_path.name}", loc='left', color='gold', fontweight='bold')
        ax1.legend(loc='upper left', ncol=3)

        # PANEL 2: STATE (Beta / Hedge Ratio)
        ax2 = fig.add_subplot(gs[1], sharex=ax1)
        ax2.plot(ts, beta, color=COLORS['beta'], label='Adaptive Beta (Hedge Ratio)')
        ax2.set_ylabel("β")
        ax2.legend(loc='upper left')

        # PANEL 3: EXECUTION (Positions)
        ax3 = fig.add_subplot(gs[2], sharex=ax1)
        ax3.fill_between(ts, 0, pos, where=(pos > 0), color=COLORS['position_long'], alpha=0.4, label='Long')
        ax3.fill_between(ts, 0, pos, where=(pos < 0), color=COLORS['position_short'], alpha=0.4, label='Short')
        ax3.set_yticks([-1, 0, 1])
        ax3.set_yticklabels(['Short', 'Neutral', 'Long'])
        ax3.legend(loc='upper left')

        # PANEL 4: PERFORMANCE (Cumulative Returns)
        ax4 = fig.add_subplot(gs[3], sharex=ax1)
        ax4.plot(ts, pnl, color=COLORS['pnl'], lw=2, label='Strategy Equity Curve')
        ax4.fill_between(ts, 0, pnl, color=COLORS['pnl'], alpha=0.1)
        ax4.set_ylabel("Cum. Returns")
        ax4.legend(loc='upper left')

        plt.savefig(save_path, dpi=150)
        plt.close()

    def _generate_insights(self, df: pl.DataFrame, entry_t: float) -> List[str]:
        z = df["z_score"].to_numpy()
        pos = df["position"].to_numpy()
        max_z = np.max(np.abs(z))
        trades = np.sum(np.diff(pos) != 0)
        
        insights = []
        if max_z < entry_t:
            insights.append(f"🚨 SIGNAL DEATH: Max |Z| ({max_z:.2f}) never reached Entry Threshold ({entry_t})")
            insights.append("   → Recommendation: Reduce R (Observation Noise) or increase Process Noise.")
        elif trades == 0:
            insights.append("🚨 TRADE PARALYSIS: Signals crossed threshold but no trades executed.")
            insights.append("   → Recommendation: Check Engine Logic or Position state preservation.")
        else:
            insights.append(f"✅ ACTIVE: Strategy executed {trades} trades with Max |Z| of {max_z:.2f}")
            
        return insights

if __name__ == "__main__":
    viz = StrategyVisualizer()
    res = viz.visualize(entry_threshold=2.0)
    if res.is_ok():
        print("\n".join(res.unwrap()["insights"]))
