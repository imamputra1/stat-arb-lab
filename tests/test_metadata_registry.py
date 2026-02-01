"""
UNIT TEST: METADATA REGISTRY (THE NOTARY)
Focus: Hashing consistency, Schema enforcement, and Atomic persistence.
"""
import sys
import json
import shutil
import tempfile
from pathlib import Path
from datetime import datetime
import logging
from typing import Tuple

# --- PATH INJECTION ---
PROJECT_ROOT = Path(__file__).parent.parent.absolute()
sys.path.append(str(PROJECT_ROOT))

import polars as pl

# Import Target Module
from research.processing.storage.metadata_registry import create_metadata_registry

# --- SETUP LOGGING ---
def setup_logging():
    log_dir = PROJECT_ROOT / "logs"
    log_dir.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_filename = log_dir / f"TestMetadata_{timestamp}.log"

    logging.basicConfig(
        format='%(asctime)s | %(levelname)-8s | %(name)-15s | %(message)s',
        datefmt='%H:%M:%S',
        level=logging.INFO,
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(str(log_filename), mode='w')
        ]
    )
    return logging.getLogger("TestMetadata")

logger = setup_logging()

class TestMetadataRegistryLogic:
    
    def __init__(self):
        # Create a temporary directory for testing storage
        self.test_dir = tempfile.mkdtemp()
        logger.info(f"Test sandbox created at: {self.test_dir}")

    def cleanup(self):
        """Remove temporary directory after tests."""
        shutil.rmtree(self.test_dir)
        logger.info("Test sandbox cleaned up.")

    def run(self) -> bool:
        logger.info("=== STARTING UNIT TEST: METADATA REGISTRY ===")
        
        test_cases = [
            ("1. Hash Determinism    ", self.test_hash_determinism),
            ("2. Schema Enforcement  ", self.test_schema_enforcement),
            ("3. Atomic Update       ", self.test_atomic_update),
            ("4. Consistency Check   ", self.test_consistency_verification)
        ]
        
        results = []
        for name, func in test_cases:
            try:
                success, msg = func()
                results.append((name, success, msg))
                if success:
                    logger.info(f"{name}: PASS")
                else:
                    logger.error(f"{name}: FAIL ({msg})")
            except Exception as e:
                logger.error(f"{name}: ERROR ({str(e)})", exc_info=True)
                results.append((name, False, str(e)))
        
        self.cleanup()
        self.print_summary(results)
        return all(r[1] for r in results)

    # --- TEST CASES ---

    def test_hash_determinism(self) -> Tuple[bool, str]:
        """Ensure identical configs produce identical hashes (order-agnostic)."""
        registry = create_metadata_registry(self.test_dir)
        
        # Config A
        config_a = {"window": "1h", "assets": ["BTC", "ETH"]}
        # Config B (Same content, different key order)
        config_b = {"assets": ["BTC", "ETH"], "window": "1h"}
        # Config C (Different content)
        config_c = {"window": "2h", "assets": ["BTC", "ETH"]}
        
        hash_a = registry.generate_feature_hash(config_a)
        hash_b = registry.generate_feature_hash(config_b)
        hash_c = registry.generate_feature_hash(config_c)
        
        if hash_a != hash_b:
            return False, "Hash mismatch for identical configs (Order issue?)"
            
        if hash_a == hash_c:
            return False, "Hash collision for different configs"
            
        return True, f"Hash A: {hash_a[:8]} == Hash B: {hash_b[:8]}"

    def test_schema_enforcement(self) -> Tuple[bool, str]:
        """Ensure sensitive columns are strictly Float64."""
        registry = create_metadata_registry(self.test_dir)
        
        # Case A: Valid Schema
        valid_schema = {
            "timestamp": pl.Datetime,
            "log_BTC": pl.Float64,
            "z_score_BTC": pl.Float64
        }
        if registry.validate_schema_integrity(valid_schema).is_err():
            return False, "Failed to accept valid Float64 schema"
            
        # Case B: Invalid Schema (Float32 for sensitive data)
        invalid_schema = {
            "timestamp": pl.Datetime,
            "z_score_BTC": pl.Float32 # Violation!
        }
        res = registry.validate_schema_integrity(invalid_schema)
        
        if res.is_ok():
            return False, "Failed to reject Float32 for sensitive column"
            
        # Case C: Valid Schema (Non-sensitive columns can be anything)
        mixed_schema = {
            "z_score_BTC": pl.Float64,
            "category_type": pl.Utf8 # OK, not sensitive
        }
        if registry.validate_schema_integrity(mixed_schema).is_err():
            return False, "Incorrectly rejected non-sensitive column"

        return True, "Strict Float64 enforcement verified"

    def test_atomic_update(self) -> Tuple[bool, str]:
        """Verify metadata file creation and content."""
        registry = create_metadata_registry(self.test_dir)
        
        params = {"window": "1h"}
        cols = ["timestamp", "log_BTC"]
        
        # Update Registry
        registry.update_registry(
            row_count=100,
            columns=cols,
            feature_params=params
        )
        
        # Verify File Exists
        meta_path = Path(self.test_dir) / "metadata.json"
        if not meta_path.exists():
            return False, "metadata.json not created"
            
        # Verify Content
        with open(meta_path, 'r') as f:
            data = json.load(f)
            
        if data["row_count"] != 100:
            return False, f"Row count mismatch: {data['row_count']}"
            
        if "feature_hash" not in data:
            return False, "Feature hash missing in JSON"
            
        return True, "Metadata persisted correctly"

    def test_consistency_verification(self) -> Tuple[bool, str]:
        """Verify the check logic between current params and saved registry."""
        registry = create_metadata_registry(self.test_dir)
        params = {"window": "4h"}
        
        # Initialize Registry
        registry.update_registry(100, ["col1"], params)
        
        # Test Match
        match_res = registry.verify_consistency(params)
        if match_res.is_err() or not match_res.unwrap():
            return False, "Failed to verify matching params"
            
        # Test Mismatch
        mismatch_res = registry.verify_consistency({"window": "1h"}) # Different
        if mismatch_res.is_err() or mismatch_res.unwrap():
            return False, "Failed to detect param mismatch"
            
        return True, "Consistency logic operational"

    # --- CLI SUMMARY ---

    def print_summary(self, results):
        total = len(results)
        passed = sum(1 for r in results if r[1])
        print("\n" + "="*70)
        print("METADATA REGISTRY TEST REPORT")
        print("="*70)
        for name, success, msg in results:
            status = "✓ PASS" if success else "✗ FAIL"
            print(f"{status:<8} {name:<25} | {msg}")
        print("-"*70)
        print(f"TOTAL: {passed}/{total} Passed")
        if passed == total:
            print("THE NOTARY IS SECURE & READY.")
        else:
            print("REGISTRY INTEGRITY COMPROMISED.")
        print("="*70 + "\n")

if __name__ == "__main__":
    success = TestMetadataRegistryLogic().run()
    sys.exit(0 if success else 1)
