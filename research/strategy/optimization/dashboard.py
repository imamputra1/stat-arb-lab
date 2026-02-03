"""
QUANTUM VISUALIZATION ENGINE (THE MISSION CONTROL) - V9.1 FIX
Location: research/strategy/optimization/dashboard.py
Focus: Full synchronization with War Room V9.0 API.
"""
import sys
import warnings
import polars as pl
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
from datetime import datetime
from typing import Dict, Optional

# --- ABSOLUTE PATH INJECTION ---
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent.absolute()
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from research.shared import Result, Ok, Err
from research.strategy.optimization import OptimizationClerk

# Suppress warnings
warnings.filterwarnings('ignore')
plt.style.use('dark_background')

class QuantumDashboard:
    """Industrial visualization hub for HP Aero-13."""
    
    def __init__(self):
        self.clerk = OptimizationClerk()
        self.reports_dir = PROJECT_ROOT / "research" / "reports" / "dashboards"
        self.reports_dir.mkdir(parents=True, exist_ok=True)

    def generate_dashboard(self, 
                         layout_name: str = "full_analytics", 
                         batch_id: Optional[str] = None,
                         limit: int = 1000,
                         interactive: bool = False) -> Result[Dict[str, Path], str]:
        """
        API V9.0 Compliant Dashboard Generator.
        Menerima parameter 'interactive' untuk mencegah crash War Room.
        """
        try:
            # 1. Data Retrieval
            res = self.clerk.get_latest_results(limit=limit)
            if res.is_err(): return Err(res.error)
            
            data = res.unwrap()
            # Filter by batch_id if provided
            if batch_id and "batch_id" in data.columns:
                data = data.filter(pl.col("batch_id") == batch_id)
            
            if data.height == 0: 
                return Err("Zero data found for dashboard visualization.")

            # 2. Rendering (Static)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            static_path = self._render_static_panel(data, timestamp)
            
            paths = {"static": static_path}

            # 3. Interactive Placeholder (To satisfy War Room request)
            if interactive:
                # Di masa depan kita bisa implementasi Plotly di sini
                # Untuk sekarang, kita return path static agar tidak crash
                paths["interactive"] = static_path 

            return Ok(paths)

        except Exception as e:
            return Err(f"Dashboard failed: {str(e)}")

    def _render_static_panel(self, data: pl.DataFrame, timestamp: str) -> Path:
        """Render 4-panel static diagnostic."""
        fig, axes = plt.subplots(2, 2, figsize=(16, 10))
        fig.suptitle(f"🚀 MISSION CONTROL: {timestamp}", fontsize=16, color="#00ffc8")
        
        # Panel A: Leaderboard
        top = data.sort("smart_score", descending=True).head(10)
        if not top.is_empty():
            sns.barplot(data=top.to_pandas(), x="smart_score", y="label", ax=axes[0,0], palette="viridis")
            axes[0,0].set_title("🏆 TOP PARAMETERS")
        
        # Panel B: Risk/Reward
        if not data.is_empty():
            pdf = data.to_pandas()
            sns.scatterplot(data=pdf, x="sharpe", y="pnl", size="trades", hue="smart_score", ax=axes[0,1])
            axes[0,1].set_title("📈 RISK vs REWARD")

        # Panel C: Noise Heatmap
        if "process_noise" in data.columns and "observation_noise" in data.columns:
            pivot = data.pivot(index="process_noise", columns="observation_noise", values="smart_score", aggregate_function="max")
            sns.heatmap(pivot.to_pandas(), cmap="YlGnBu", ax=axes[1,0])
            axes[1,0].set_title("🔥 NOISE SENSITIVITY")

        # Panel D: Trades
        if not data.is_empty():
            sns.histplot(data=data.to_pandas(), x="trades", bins=20, ax=axes[1,1], color="#ff0055")
            axes[1,1].set_title("🎯 TRADE DISTRIBUTION")

        plt.tight_layout(rect=[0, 0.03, 1, 0.95])
        
        output_path = self.reports_dir / f"dashboard_{timestamp}.png"
        plt.savefig(output_path, dpi=120)
        plt.close()
        return output_path

if __name__ == "__main__":
    d = QuantumDashboard()
    d.generate_dashboard()
