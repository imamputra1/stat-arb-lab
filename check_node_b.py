"""
NODE B SURVIVAL CHECK
System validation script for Node B infrastructure.
Format: Standard Text/Log (No Icons)
"""
import sys
import logging
import importlib
from pathlib import Path
from typing import Dict, Any

# Setup Logger: Format Standard Professional
logging.basicConfig(
    format='%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=logging.INFO,
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('nodeb_check.log', mode='w') # mode='w' untuk overwrite log lama
    ]
)
logger = logging.getLogger("NodeB_Check")

def check_import(module_name: str, item_name: str = None) -> Dict[str, Any]:
    try:
        module = importlib.import_module(module_name)
        if item_name:
            item = getattr(module, item_name)
            return {"status": True, "module": module, "item": item}
        return {"status": True, "module": module}
    except Exception as e:
        return {"status": False, "error": str(e)}

def check_polars() -> bool:
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

def check_protocols() -> bool:
    """Check Node B Protocols definitions and imports."""
    try:
        # Import semua komponen yang ada di __init__.py
        from research.processing import (
            TimeSeriesAligner
        )
        
        logger.info("Node B Protocols import: PASSED")
        
        # Test runtime_checkable
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

def check_result_pattern() -> bool:
    """Check Shared Result Pattern integration."""
    try:
        from research.shared import Ok, match_result
        
        logger.info("Shared Result Pattern import: PASSED")
        
        # Test logic
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

def check_directory_structure() -> bool:
    """Verify critical file paths."""
    expected_paths = [
        Path("research/processing/__init__.py"),
        Path("research/processing/protocols.py"),
        Path("research/shared/__init__.py"),
        Path("research/shared/result.py"),
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

def check_duckdb_integration() -> bool:
    """Check DuckDB Repository availability."""
    try:
        from research.repository import DuckDBRepository
        logger.info("DuckDB Repository import: PASSED")
        
        repo = DuckDBRepository(":memory:", "./data/raw")
        if hasattr(repo, "query"):
            return True
        return False
        
    except ImportError:
        logger.warning("DuckDB Repository module not found.")
        return False
    except Exception as e:
        logger.error(f"DuckDB check error: {e}")
        return False

def run_all_checks() -> Dict[str, bool]:
    logger.info("--- STARTING NODE B SYSTEM CHECK ---")
    
    checks = {
        "Directory Structure": check_directory_structure(),
        "Polars Engine      ": check_polars(),
        "Protocols Import   ": check_protocols(),
        "Result Pattern     ": check_result_pattern(),
        "DuckDB Integration ": check_duckdb_integration(),
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
    try:
        checks = run_all_checks()
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
