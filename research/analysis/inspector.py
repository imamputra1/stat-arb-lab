"""
RESULT INSPECTOR (THE DETECTIVE) - DYNAMIC V3.0
Location: research/analysis/inspector.py
Focus: Context-aware forensic analysis using auto-discovered metadata.
Standard: Sinkronisasi total dengan Node S & Node D Commander.
"""
import sys
import json
import logging
from pathlib import Path
from typing import Optional, Dict, Any, Tuple
import polars as pl
import numpy as np

# --- PATH INJECTION ---
PROJECT_ROOT = Path(__file__).parent.parent.parent.absolute()
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from core.shared import Ok, Err, Result

# --- LOGGING SETUP ---
def setup_logging():
    logger = logging.getLogger("Inspector")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(message)s'))
    logger.addHandler(handler)
    logger.propagate = False
    return logger

logger = setup_logging()

class ResultInspector:
    """
    Surgical forensic tool with metadata-driven insights.
    Automatically aligns diagnostics with simulation parameters.
    """
    
    def __init__(self, results_dir: Optional[str] = None):
        self.results_dir = Path(results_dir) if results_dir else PROJECT_ROOT / "research" / "results"
        self.debug_dir = PROJECT_ROOT / "research" / "debug_data"

    def get_latest_artifact(self) -> Tuple[Path, Optional[Dict[str, Any]]]:
        """Menemukan data terbaru dan metadatanya."""
        # 1. Cek Debug Data (Prioritas Utama)
        target_path = self.debug_dir / "latest_run.parquet"
        
        if not target_path.exists():
            files = list(self.results_dir.glob("arb_*.parquet"))
            if not files: raise FileNotFoundError("No strategy artifacts found.")
            target_path = max(files, key=lambda f: f.stat().st_mtime)

        # 2. Cari Metadata JSON terbaru
        metadata = None
        meta_files = list(self.results_dir.glob("metadata_*.json"))
        if meta_files:
            latest_meta = max(meta_files, key=lambda f: f.stat().st_mtime)
            try:
                with open(latest_meta, 'r') as f:
                    metadata = json.load(f)
            except: pass
                
        return target_path, metadata

    def analyze(self, file_path: Optional[str] = None) -> Result[Dict[str, Any], str]:
        """Main forensic routine with contextual awareness."""
        try:
            target_path, metadata = self.get_latest_artifact()
            if file_path: target_path = Path(file_path)
            
            # Auto-Extract Parameters
            entry_t = metadata.get("entry_threshold", 2.0) if metadata else 2.0
            exit_t = metadata.get("exit_threshold", 0.5) if metadata else 0.5
            
            logger.info(f"\n🕵️  FORENSIC OTOPSY: {target_path.name}")
            logger.info(f"📍 Context: Entry={entry_t} | Exit={exit_t}")
            logger.info("=" * 70)

            df = pl.read_parquet(target_path)
            
            # Diagnostics Assembly
            diag = {
                "integrity": self._check_integrity(df),
                "signal": self._analyze_signal(df, entry_t),
                "state": self._analyze_state(df),
                "trading": self._analyze_trading(df, entry_t)
            }
            
            self._print_report(diag, entry_t)
            return Ok(diag)
            
        except Exception as e:
            logger.error(f"💥 Inspection Failed: {str(e)}")
            return Err(str(e))

    def _check_integrity(self, df: pl.DataFrame) -> Dict[str, Any]:
        """Fast integrity check."""
        critical_cols = {"timestamp", "z_score", "beta", "position"}
        missing = critical_cols - set(df.columns)
        return {
            "status": "HEALTHY" if not missing else "CORRUPT",
            "rows": df.height,
            "missing": list(missing),
            "range": [df["timestamp"].min(), df["timestamp"].max()] if "timestamp" in df.columns else []
        }

    def _analyze_signal(self, df: pl.DataFrame, threshold: float) -> Dict[str, Any]:
        """Analisis kekuatan sinyal vs parameter simulasi."""
        z = df["z_score"].to_numpy()
        max_abs_z = float(np.max(np.abs(z)))
        std_z = float(np.std(z))
        
        return {
            "range": [float(np.min(z)), float(np.max(z))],
            "max_abs": max_abs_z,
            "std": std_z,
            "diagnosis": "FLATLINE" if max_abs_z < 1e-6 else "WEAK" if max_abs_z < threshold else "HEALTHY"
        }

    def _analyze_state(self, df: pl.DataFrame) -> Dict[str, Any]:
        """Analisis adaptasi Kalman."""
        beta = df["beta"].to_numpy()
        std_b = float(np.std(beta))
        return {
            "mean": float(np.mean(beta)),
            "std": std_b,
            "diagnosis": "STATIC" if std_b < 1e-8 else "ADAPTING"
        }

    def _analyze_trading(self, df: pl.DataFrame, entry_t: float) -> Dict[str, Any]:
        """Analisis efektivitas trading."""
        pos = df["position"].to_numpy()
        trades = int(np.sum(np.diff(pos) != 0))
        
        # Sync PnL Column
        pnl_col = "cumulative_returns" if "cumulative_returns" in df.columns else "cum_pnl"
        pnl = float(df[pnl_col][-1]) if pnl_col in df.columns else 0.0
        
        return {
            "trades": trades,
            "final_pnl": pnl,
            "long_pct": float(np.mean(pos == 1) * 100),
            "short_pct": float(np.mean(pos == -1) * 100)
        }

    def _print_report(self, diag: Dict[str, Any], entry_t: float):
        """CLI report yang profesional."""
        s = diag["signal"]
        logger.info(f"📊 INTEGRITY: {diag['integrity']['status']} ({diag['integrity']['rows']:,} rows)")
        logger.info(f"📡 SIGNAL: {s['diagnosis']} (Max|Z|={s['max_abs']:.4f} vs Thresh={entry_t})")
        logger.info(f"🧠 STATE: {diag['state']['diagnosis']} (Beta σ={diag['state']['std']:.6f})")
        logger.info(f"💰 TRADING: {diag['trading']['trades']} Trades | PnL: {diag['trading']['final_pnl']:.6f}")
        
        if diag['trading']['trades'] == 0:
            logger.warning("\n🚨 ASSESSMENT: TRADING COMA DETECTED")
            if s['max_abs'] < entry_t:
                logger.warning(f"   → Sinyal tertinggi ({s['max_abs']:.2f}) tidak mampu menembus target ({entry_t}).")
                logger.warning("   → Resep: Naikkan 'process-noise' (Q) atau turunkan threshold.")
        logger.info("=" * 70 + "\n")

if __name__ == "__main__":
    inspector = ResultInspector()
    inspector.analyze()
