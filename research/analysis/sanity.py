"""
SANITY CHECKER (THE DOCTOR) - DYNAMIC V4.2
Location: research/analysis/sanity.py
Focus: Context-aware system validation.
Standard: Synchronized with Pipeline Metadata & CLI.
"""
import sys
import json
import logging
from pathlib import Path
from typing import Dict, Any

# --- PATH INJECTION ---
PROJECT_ROOT = Path(__file__).parent.parent.parent.absolute()
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


logger = logging.getLogger("Doctor")
logger.setLevel(logging.INFO)

class SystemDoctor:
    def __init__(self, project_root: Path = PROJECT_ROOT):
        self.project_root = project_root
        # Metadata sensor
        self.results_dir = project_root / "research" / "results"
        self.diagnosis = {}

    def _get_latest_config(self) -> Dict[str, Any]:
        """Membaca parameter terakhir yang digunakan pipeline."""
        meta_files = list(self.results_dir.glob("metadata_*.json"))
        if not meta_files:
            return {}
        latest_meta = max(meta_files, key=lambda f: f.stat().st_mtime)
        try:
            with open(latest_meta, 'r') as f:
                return json.load(f)
        except:
            return {}

    def full_checkup(self):
        logger.info("\n" + "="*70)
        logger.info("🔬 DYNAMIC SYSTEM SANITY CHECK")
        logger.info("="*70)

        # 1. Load context
        config = self._get_latest_config()
        entry_t = config.get("entry_threshold", 2.0)
        exit_t = config.get("exit_threshold", 0.5)

        # 2. Structural Check
        self._check_structure()

        # 3. Dynamic Logic Check
        self._check_logic(entry_t, exit_t)

        return self.diagnosis

    def _check_structure(self):
        """Memastikan folder inti ada."""
        paths = ["research/strategy/engine", "research/strategy/models/library", "data/silver"]
        missing = [p for p in paths if not (self.project_root / p).exists()]
        
        if missing:
            logger.error(f"   ❌ Missing: {missing}")
            self.diagnosis["structure"] = "DEFECTIVE"
        else:
            logger.info("   ✅ Structure: HEALTHY")
            self.diagnosis["structure"] = "HEALTHY"

    def _check_logic(self, entry: float, exit_t: float):
        """Validasi logika berdasarkan parameter dinamis."""
        logger.info(f"\n🧠 LOGIC BIOPSY (Context: Entry={entry}, Exit={exit_t})")
        
        if entry <= exit_t:
            logger.error(f"   ❌ INVALID: Entry ({entry}) must be > Exit ({exit_t})")
            self.diagnosis["logic"] = "CORRUPT"
        elif exit_t <= 0:
            logger.error(f"   ❌ INVALID: Exit ({exit_t}) must be positive")
            self.diagnosis["logic"] = "CORRUPT"
        else:
            logger.info("   ✅ Logic: SOUND")
            self.diagnosis["logic"] = "SOUND"

if __name__ == "__main__":
    doctor = SystemDoctor()
    doctor.full_checkup()
