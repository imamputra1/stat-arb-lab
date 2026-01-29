"""
NODE B SURVIVAL CHECK
System validation script for Node B infrastructure.
Location: tests/check_node_b.py
"""
import sys
from pathlib import Path

# --- [CRITICAL FIX] ---
# Arahkan path ke Root Project agar bisa import 'research'
# Kita ambil path file ini, naik satu level (..)
PROJECT_ROOT = Path(__file__).parent.parent.absolute()
sys.path.append(str(PROJECT_ROOT))
# ----------------------

import logging
from typing import Dict
from datetime import datetime

# Setup Logger agar masuk ke root/logs, bukan tests/logs
def setup_logging():
    # Gunakan PROJECT_ROOT yang sudah kita definisikan
    log_dir = PROJECT_ROOT / "logs"
    log_dir.mkdir(exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_filename = log_dir / f"NodeB_{timestamp}.log"

    logging.basicConfig(
        format='%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        level=logging.INFO,
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(str(log_filename), mode='w')
        ]
    )

def check_polars(logger) -> bool:
    """Check Polars installation and basic lazy execution."""
    try:
        import polars as pl
        version = pl.__version__
        logger.info(f"Polars detected: v{version}")
        
        # Test basic functionality
        df = pl.DataFrame({"test": [1, 2, 3]})
        lazy = df.lazy()
        if lazy.collect().shape == (3, 1):
            logger.info("Polars lazy execution test: PASSED")
            return True
        else:
            logger.warning("Polars lazy execution test: FAILED (Shape mismatch)")
            return False
    except Exception as e:
        logger.error(f"Polars check error: {e}")
        return False

def check_protocols(logger) -> bool:
    """Check Node B Protocols definitions and imports."""
    try:
        from research.processing import TimeSeriesAligner
        logger.info("Node B Protocols import: PASSED")
        
        class DummyAligner:
            def align(self, data_map, **kwargs): return None
            @property
            def method(self): return "dummy"
        
        dummy = DummyAligner()
        if isinstance(dummy, TimeSeriesAligner):
            logger.info("Protocol runtime check (TimeSeriesAligner): PASSED")
        else:
            logger.warning("Protocol runtime check: FAILED")
        return True
    except Exception as e:
        logger.error(f"Protocols check error: {e}")
        return False

def check_result_pattern(logger) -> bool:
    """Check Shared Result Pattern integration."""
    try:
        from research.shared import Ok, match_result
        logger.info("Shared Result Pattern import: PASSED")
        
        ok_res = Ok("test")
        res = match_result(ok_res, lambda x: True, lambda e: False)
        
        if res:
            logger.info("Result Pattern logic test: PASSED")
            return True
        else:
            logger.warning("Result Pattern logic test: FAILED")
            return False
    except Exception as e:
        logger.error(f"Result Pattern check error: {e}")
        return False

def check_directory_structure(logger) -> bool:
    """Verify critical file paths."""
    # Path harus dicek relatif terhadap PROJECT_ROOT
    expected_paths = [
        PROJECT_ROOT / "research/processing/__init__.py",
        PROJECT_ROOT / "research/processing/protocols.py",
        PROJECT_ROOT / "research/shared/__init__.py",
        PROJECT_ROOT / "research/shared/result.py",
    ]
    
    missing = []
    for path in expected_paths:
        if not path.exists():
            missing.append(str(path))
    
    if not missing:
        logger.info("Directory structure check: PASSED")
        return True
    else:
        logger.error(f"Missing files: {missing}")
        return False

def check_duckdb_integration(logger) -> bool:
    """Check DuckDB Repository availability."""
    try:
        from research.repository import DuckDBRepository
        logger.info("DuckDB Repository import: PASSED")
        
        # Test path raw data
        raw_path = PROJECT_ROOT / "data" / "raw"
        repo = DuckDBRepository(":memory:", str(raw_path))
        
        if hasattr(repo, "query"):
            return True
        return False
    except ImportError:
        logger.warning("DuckDB Repository module not found.")
        return False
    except Exception as e:
        logger.error(f"DuckDB check error: {e}")
        return False

def run_all_checks(logger) -> Dict[str, bool]:
    logger.info("--- STARTING NODE B SYSTEM CHECK ---")
    checks = {
        "Directory Structure": check_directory_structure(logger),
        "Polars Engine      ": check_polars(logger),
        "Protocols Import   ": check_protocols(logger),
        "Result Pattern     ": check_result_pattern(logger),
        "DuckDB Integration ": check_duckdb_integration(logger),
    }
    return checks

def print_summary(checks: Dict[str, bool]) -> None:
    print("\n" + "-"*50)
    print("NODE B SYSTEM CHECK SUMMARY")
    print("-"*50)
    passed_count = 0
    for name, status in checks.items():
        result_str = "PASS" if status else "FAIL"
        if status: passed_count += 1
        print(f"{name} : {result_str}")
    print("-"*50)
    total = len(checks)
    if passed_count == total:
        print(f"OVERALL STATUS: READY ({passed_count}/{total})")
    else:
        print(f"OVERALL STATUS: NOT READY ({passed_count}/{total})")
    print("-"*50 + "\n")

def main() -> int:
    setup_logging()
    logger = logging.getLogger("NodeB_Check")
    
    try:
        checks = run_all_checks(logger)
        print_summary(checks)
        if all(checks.values()):
            return 0
        return 1
    except KeyboardInterrupt:
        print("\nProcess interrupted.")
        return 130
    except Exception as e:
        logger.critical(f"Fatal execution error: {e}", exc_info=True)
        return 1

if __name__ == "__main__":
    sys.exit(main())
