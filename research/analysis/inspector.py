"""
RESULT INSPECTOR (THE DETECTIVE) - V4.0 INDUSTRIAL GRADE
Location: research/analysis/inspector.py
Focus: Context-aware forensic analysis of backtest results.
       Reads Parquet artifacts and metadata, produces diagnostic insights.
"""

import sys
import json
import logging
from pathlib import Path
from typing import Optional, Dict, Any, Tuple
from datetime import datetime

import polars as pl
import numpy as np

# --- PATH INJECTION ---
PROJECT_ROOT = Path(__file__).parent.parent.parent.absolute()
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# --- CORE SHARED ---
from core.shared import Result, Ok, Err


# ============================================================================
# LOGGING SETUP
# ============================================================================

def _setup_logger() -> logging.Logger:
    """Create a dedicated logger for inspector with clean output."""
    logger = logging.getLogger("Inspector")
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    logger.propagate = False
    return logger


# ============================================================================
# MAIN INSPECTOR CLASS
# ============================================================================

class ResultInspector:
    """
    Surgical forensic tool for backtest result analysis.
    Automatically discovers the latest artifact and metadata.
    """

    def __init__(
        self,
        results_dir: Optional[Path] = None,
        debug_dir: Optional[Path] = None
    ) -> None:
        """
        Initialize inspector with directories for results and debug data.
        
        Args:
            results_dir: Directory containing arb_*.parquet files.
            debug_dir:   Directory containing latest_run.parquet (optional).
        """
        self.results_dir = results_dir or (PROJECT_ROOT / "research" / "results")
        self.debug_dir = debug_dir or (PROJECT_ROOT / "research" / "debug_data")
        self.logger = _setup_logger()
        self._diagnosis: Dict[str, Any] = {}

    def get_latest_artifact(self) -> Result[Tuple[Path, Optional[Dict[str, Any]]], str]:
        """
        Find the most recent result artifact and its corresponding metadata.
        Priority: debug_dir/latest_run.parquet > latest arb_*.parquet.
        Returns tuple (file_path, metadata_dict) or error.
        """
        # 1. Try debug directory first
        debug_path = self.debug_dir / "latest_run.parquet"
        if debug_path.exists():
            self.logger.debug(f"Using debug artifact: {debug_path}")
            meta = self._load_metadata_for(debug_path)
            return Ok((debug_path, meta))

        # 2. Fallback to results directory
        files = list(self.results_dir.glob("arb_*.parquet"))
        if not files:
            return Err(f"No artifacts found in {self.results_dir}")

        latest_file = max(files, key=lambda f: f.stat().st_mtime)
        self.logger.debug(f"Using latest result: {latest_file.name}")
        meta = self._load_metadata_for(latest_file)
        return Ok((latest_file, meta))

    def _load_metadata_for(self, artifact_path: Path) -> Optional[Dict[str, Any]]:
        """
        Attempt to load metadata JSON file corresponding to the artifact.
        Metadata file naming convention: metadata_<execution_id>.json.
        If not found, return None.
        """
        # Try to extract execution_id from filename
        # Example: arb_DOGE_BTC_12345.parquet -> ID = 12345
        stem = artifact_path.stem
        parts = stem.split('_')
        if len(parts) >= 3:
            # Assume format: arb_SYMBOL1_SYMBOL2_ID
            exec_id = parts[-1]
            meta_path = self.results_dir / f"metadata_{exec_id}.json"
            if meta_path.exists():
                try:
                    with open(meta_path, 'r') as f:
                        return json.load(f)
                except Exception as e:
                    self.logger.warning(f"Failed to load metadata {meta_path}: {e}")
        return None

    def analyze(
        self,
        file_path: Optional[Path] = None,
        entry_threshold: Optional[float] = None,
        exit_threshold: Optional[float] = None
    ) -> Result[Dict[str, Any], str]:
        """
        Main forensic analysis routine.
        
        Args:
            file_path: Specific file to analyze. If None, uses latest.
            entry_threshold: Override entry threshold (otherwise from metadata).
            exit_threshold:  Override exit threshold (otherwise from metadata).
        
        Returns:
            Dictionary containing integrity, signal, state, trading diagnostics.
        """
        # 1. Locate target file and metadata
        if file_path is None:
            artifact_res = self.get_latest_artifact()
            if artifact_res.is_err():
                return Err(artifact_res.unwrap_err())
            target_path, metadata = artifact_res.unwrap()
        else:
            target_path = Path(file_path)
            if not target_path.exists():
                return Err(f"File not found: {target_path}")
            metadata = self._load_metadata_for(target_path)

        # 2. Extract parameters (with override)
        entry_t = entry_threshold
        if entry_t is None:
            entry_t = metadata.get("entry_threshold", 2.0) if metadata else 2.0

        exit_t = exit_threshold
        if exit_t is None:
            exit_t = metadata.get("exit_threshold", 0.5) if metadata else 0.5

        self.logger.info(f"\n🕵️  FORENSIC AUTOPSY: {target_path.name}")
        self.logger.info(f"📍 Context: Entry={entry_t} | Exit={exit_t}")
        self.logger.info("=" * 70)

        # 3. Read data
        try:
            df = pl.read_parquet(target_path)
        except Exception as e:
            return Err(f"Failed to read Parquet: {e}")

        if df.height == 0:
            return Err("DataFrame is empty")

        # 4. Run diagnostics
        integrity = self._check_integrity(df)
        signal = self._analyze_signal(df, entry_t)
        state = self._analyze_state(df)
        trading = self._analyze_trading(df)

        diagnosis = {
            "integrity": integrity,
            "signal": signal,
            "state": state,
            "trading": trading,
            "metadata": metadata or {},
            "file": str(target_path),
        }

        # 5. Print report
        self._print_report(diagnosis, entry_t)

        self._diagnosis = diagnosis
        return Ok(diagnosis)

    def _check_integrity(self, df: pl.DataFrame) -> Dict[str, Any]:
        """
        Fast integrity check: required columns, row count, time range.
        """
        critical_cols = {"timestamp", "z_score", "beta", "position"}
        missing = critical_cols - set(df.columns)

        status = "HEALTHY" if not missing else "CORRUPT"

        result: Dict[str, Any] = {
            "status": status,
            "rows": df.height,
            "missing": list(missing),
        }

        if "timestamp" in df.columns:
            # Convert to datetime for min/max if needed
            ts_min = df["timestamp"].min()
            ts_max = df["timestamp"].max()
            # Try to interpret as datetime if numeric
            if isinstance(ts_min, (int, float)):
                # Assume milliseconds
                result["time_range"] = [
                    datetime.fromtimestamp(ts_min / 1000).isoformat(),
                    datetime.fromtimestamp(ts_max / 1000).isoformat(),
                ]
            else:
                result["time_range"] = [str(ts_min), str(ts_max)]

        return result

    def _analyze_signal(self, df: pl.DataFrame, threshold: float) -> Dict[str, Any]:
        """
        Analyze signal strength relative to entry threshold.
        """
        if "z_score" not in df.columns:
            return {"diagnosis": "NO_DATA", "max_abs": 0.0, "std": 0.0}

        z = df["z_score"].to_numpy()
        max_abs_z = float(np.max(np.abs(z)))
        std_z = float(np.std(z))

        if max_abs_z < 1e-6:
            diagnosis = "FLATLINE"
        elif max_abs_z < threshold:
            diagnosis = "WEAK"
        else:
            diagnosis = "HEALTHY"

        return {
            "range": [float(np.min(z)), float(np.max(z))],
            "max_abs": max_abs_z,
            "std": std_z,
            "diagnosis": diagnosis,
        }

    def _analyze_state(self, df: pl.DataFrame) -> Dict[str, Any]:
        """
        Analyze Kalman state adaptation (beta).
        """
        if "beta" not in df.columns:
            return {"diagnosis": "NO_DATA", "mean": 0.0, "std": 0.0}

        beta = df["beta"].to_numpy()
        mean_b = float(np.mean(beta))
        std_b = float(np.std(beta))

        if std_b < 1e-8:
            diagnosis = "STATIC"
        else:
            diagnosis = "ADAPTING"

        return {
            "mean": mean_b,
            "std": std_b,
            "diagnosis": diagnosis,
        }

    def _analyze_trading(self, df: pl.DataFrame) -> Dict[str, Any]:
        """
        Analyze trading activity and PnL.
        """
        if "position" not in df.columns:
            return {"trades": 0, "final_pnl": 0.0, "long_pct": 0.0, "short_pct": 0.0}

        pos = df["position"].to_numpy()
        # Count trades as changes in position
        position_changes = np.diff(pos, prepend=pos[0])
        trades = int(np.sum(np.abs(position_changes) > 0))

        # Determine PnL column
        pnl_col = None
        for col in ["cumulative_returns", "cum_pnl", "pnl"]:
            if col in df.columns:
                pnl_col = col
                break

        if pnl_col:
            final_pnl = float(df[pnl_col][-1])
        else:
            final_pnl = 0.0

        long_pct = float(np.mean(pos == 1) * 100) if len(pos) > 0 else 0.0
        short_pct = float(np.mean(pos == -1) * 100) if len(pos) > 0 else 0.0

        return {
            "trades": trades,
            "final_pnl": final_pnl,
            "long_pct": long_pct,
            "short_pct": short_pct,
        }

    def _print_report(self, diag: Dict[str, Any], entry_t: float) -> None:
        """
        Print a formatted report to console.
        """
        s = diag["signal"]
        st = diag["state"]
        t = diag["trading"]
        i = diag["integrity"]

        self.logger.info(f"📊 INTEGRITY: {i['status']} ({i['rows']:,} rows)")
        self.logger.info(f"📡 SIGNAL: {s['diagnosis']} (Max|Z|={s['max_abs']:.4f} vs Thresh={entry_t})")
        self.logger.info(f"🧠 STATE: {st['diagnosis']} (Beta σ={st['std']:.6f})")
        self.logger.info(f"💰 TRADING: {t['trades']} Trades | PnL: {t['final_pnl']:.6f}")

        if t['trades'] == 0:
            self.logger.warning("\n🚨 ASSESSMENT: TRADING COMA DETECTED")
            if s['max_abs'] < entry_t:
                self.logger.warning(f"   → Highest |Z| ({s['max_abs']:.2f}) never reached entry threshold ({entry_t}).")
                self.logger.warning("   → Recommendation: Increase Q (process noise) or lower entry threshold.")
        self.logger.info("=" * 70 + "\n")

    @property
    def last_diagnosis(self) -> Dict[str, Any]:
        """Return the most recent diagnosis."""
        return self._diagnosis


# ============================================================================
# QUICK ACCESS FUNCTION
# ============================================================================

def quick_inspect(
    file_path: Optional[Path] = None,
    entry_threshold: Optional[float] = None
) -> Result[Dict[str, Any], str]:
    """
    One-shot inspection without instantiating the class.
    """
    inspector = ResultInspector()
    return inspector.analyze(file_path, entry_threshold)


# ============================================================================
# END
# ============================================================================
