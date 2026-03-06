"""
SYSTEM DOCTOR (THE SANITY CHECKER) - V1.0
Location: research/analysis/sanity.py
Focus: Validate system structure and configuration integrity.
       Ensures all required directories exist and configuration logic is sound.
"""

import sys
import json
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional

# --- PATH INJECTION ---
PROJECT_ROOT = Path(__file__).parent.parent.parent.absolute()
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# --- CORE SHARED ---
from core.shared import Result, Ok


# ============================================================================
# LOGGING SETUP
# ============================================================================

def _setup_logger() -> logging.Logger:
    """Create a dedicated logger for doctor with clean output."""
    logger = logging.getLogger("SystemDoctor")
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    logger.propagate = False
    return logger


# ============================================================================
# MAIN DOCTOR CLASS
# ============================================================================

class SystemDoctor:
    """
    Performs system‑level sanity checks:
      - Checks existence of critical directories.
      - Validates configuration logic (entry > exit, etc.) using latest metadata.
    """

    def __init__(self, project_root: Optional[Path] = None) -> None:
        """
        Initialize doctor with project root directory.
        
        Args:
            project_root: Path to project root. Defaults to auto‑detected.
        """
        self.project_root = project_root or PROJECT_ROOT
        self.results_dir = self.project_root / "research" / "results"
        self.logger = _setup_logger()
        self.diagnosis: Dict[str, Any] = {}

    def _get_latest_metadata(self) -> Dict[str, Any]:
        """
        Find the most recent metadata_*.json file in results directory.
        Returns empty dict if none found.
        """
        meta_files = list(self.results_dir.glob("metadata_*.json"))
        if not meta_files:
            return {}
        latest = max(meta_files, key=lambda f: f.stat().st_mtime)
        try:
            with open(latest, 'r') as f:
                return json.load(f)
        except Exception:
            return {}

    def full_checkup(self) -> Result[Dict[str, Any], str]:
        """
        Run all checks and return diagnosis.
        Prints a formatted report to console.
        """
        self.logger.info("\n" + "=" * 70)
        self.logger.info("🔬 SYSTEM SANITY CHECK")
        self.logger.info("=" * 70)

        # 1. Structural check
        structure_ok = self._check_structure()
        self.diagnosis["structure"] = "HEALTHY" if structure_ok else "DEFECTIVE"

        # 2. Load latest configuration
        config = self._get_latest_metadata()
        if config:
            entry_t = config.get("entry_threshold", 2.0)
            exit_t = config.get("exit_threshold", 0.5)
            self.logger.info(f"\n📋 Latest config: Entry={entry_t}, Exit={exit_t}")
            logic_ok = self._check_logic(entry_t, exit_t)
            self.diagnosis["logic"] = "SOUND" if logic_ok else "CORRUPT"
            self.diagnosis["config"] = config
        else:
            self.logger.info("\n⚠️  No metadata found – skipping logic check.")
            self.diagnosis["logic"] = "UNKNOWN"

        self.logger.info("\n" + "=" * 70 + "\n")
        return Ok(self.diagnosis)

    def _check_structure(self) -> bool:
        """
        Verify that critical directories exist.
        Returns True if all are present.
        """
        required_paths = [
            "research/strategy/engine",
            "research/strategy/models/library",
            "data/silver",
        ]
        missing: List[str] = []
        for rel_path in required_paths:
            full_path = self.project_root / rel_path
            if not full_path.exists():
                missing.append(rel_path)

        if missing:
            self.logger.error(f"   ❌ Missing directories: {missing}")
            return False
        else:
            self.logger.info("   ✅ All critical directories exist.")
            return True

    def _check_logic(self, entry: float, exit_t: float) -> bool:
        """
        Validate that entry threshold is greater than exit threshold,
        and both are positive (entry absolute value).
        """
        # Entry threshold is typically positive for absolute Z‑score.
        # We compare the numeric values: entry should be > exit.
        if entry <= exit_t:
            self.logger.error(f"   ❌ INVALID: Entry ({entry}) must be > Exit ({exit_t})")
            return False
        if exit_t <= 0:
            self.logger.error(f"   ❌ INVALID: Exit ({exit_t}) must be positive")
            return False
        self.logger.info("   ✅ Logic: SOUND (entry > exit > 0)")
        return True


# ============================================================================
# QUICK ACCESS FUNCTION
# ============================================================================

def quick_checkup() -> Result[Dict[str, Any], str]:
    """One‑shot system check without instantiating the class."""
    doctor = SystemDoctor()
    return doctor.full_checkup()


# ============================================================================
# END
# ============================================================================
