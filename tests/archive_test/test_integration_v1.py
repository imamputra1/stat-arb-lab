"""
INTEGRATION TEST V1: NODE B PIPELINE
Location: tests/test_integration_v1.py
Scope: Aligner -> Validator -> Output
"""
import sys
from pathlib import Path
import time

# --- PATH INJECTION (CRITICAL) ---
PROJECT_ROOT = Path(__file__).parent.parent.absolute()
sys.path.append(str(PROJECT_ROOT))

import polars as pl
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict

# Import Internal Modules
from research.processing.pipeline import StandardPipeline, create_standard_pipeline
from research.processing.alignment import get_aligner
from research.processing.validation import create_validator

# --- CONFIGURATION ---
TEST_LOG_DIR = PROJECT_ROOT / "logs"
TEST_NAME = "IntegrationV1"

class ConsoleUI:
    """Helper for Professional CLI Output"""
    GREEN = "\033[92m"
    RED = "\033[91m"
    RESET = "\033[0m"
    BOLD = "\033[1m"

    @staticmethod
    def header(text: str):
        print(f"\n{ConsoleUI.BOLD}=== {text} ==={ConsoleUI.RESET}")

    @staticmethod
    def step(text: str):
        print(f" >> {text}...", end=" ", flush=True)

    @staticmethod
    def ok(msg: str = "OK"):
        print(f"{ConsoleUI.GREEN}[{msg}]{ConsoleUI.RESET}")

    @staticmethod
    def fail(msg: str = "FAIL"):
        print(f"{ConsoleUI.RED}[{msg}]{ConsoleUI.RESET}")

def setup_logging():
    TEST_LOG_DIR.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_filename = TEST_LOG_DIR / f"{TEST_NAME}_{timestamp}.log"

    logging.basicConfig(
        format='%(asctime)s | %(levelname)-8s | %(name)s | %(message)s',
        datefmt='%H:%M:%S',
        level=logging.INFO,
        handlers=[
            logging.StreamHandler(), # Print to console
            logging.FileHandler(str(log_filename), mode='w') # Save to file
        ]
    )
    return logging.getLogger(TEST_NAME)

logger = setup_logging()

class IntegrationTestV1:
    """
    Orchestrates the Integration Test for Node B (V1).
    Scenario:
    1. Load 2 synchronous assets (BTC, DOGE).
    2. Align using Asof Join (1m tolerance).
    3. Validate with Strict Rules (but low row count threshold).
    4. Verify Output Shape and Content.
    """

    def __init__(self):
        self.pipeline: StandardPipeline = None
        self.data_map: Dict[str, pl.LazyFrame] = {}
        self.result_df: pl.DataFrame = None

    def setup_components(self) -> bool:
        """Initialize Aligner, Validator, and Pipeline."""
        ConsoleUI.step("Initializing Components")
        try:
            # 1. Aligner
            aligner = get_aligner("asof", "1m").unwrap()
            
            # 2. Validator (Custom Rules for Small Data)
            # Kita override min_rows=3 karena data dummy cuma 5 baris
            rules = {"min_rows": 3, "required_columns": ["timestamp", "close_BTC", "close_DOGE"]}
            validator = create_validator(rules).unwrap()

            # 3. Pipeline Factory
            pipe_res = create_standard_pipeline(aligner, validator)
            if pipe_res.is_err():
                raise RuntimeError(f"Pipeline Creation Failed: {pipe_res.error}")
            
            self.pipeline = pipe_res.unwrap()
            ConsoleUI.ok()
            logger.info(f"Pipeline assembled: {type(self.pipeline).__name__}")
            return True
            
        except Exception as e:
            ConsoleUI.fail()
            logger.error(f"Setup Failed: {e}", exc_info=True)
            return False

    def generate_data(self) -> bool:
        """Create high-fidelity dummy data."""
        ConsoleUI.step("Generating Synthetic Data (BTC & DOGE)")
        try:
            # Base Time (UTC)
            t0 = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
            
            # Generate 5 minutes of data
            timestamps = [int((t0 + timedelta(minutes=i)).timestamp() * 1000) for i in range(5)]
            
            # BTC: 100 -> 104
            self.data_map["BTC"] = pl.DataFrame({
                "timestamp": timestamps,
                "close": [100.0, 101.0, 102.0, 103.0, 104.0],
                "volume": [1000.0] * 5
            }).lazy()

            # DOGE: 0.50 -> 0.54
            self.data_map["DOGE"] = pl.DataFrame({
                "timestamp": timestamps,
                "close": [0.50, 0.51, 0.52, 0.53, 0.54],
                "volume": [5000.0] * 5
            }).lazy()

            ConsoleUI.ok(f"Generated {len(timestamps)} rows/asset")
            return True
        except Exception as e:
            ConsoleUI.fail()
            logger.error(f"Data Gen Failed: {e}")
            return False

    def execute_pipeline(self) -> bool:
        """Run the pipeline and capture result."""
        ConsoleUI.step("Executing Pipeline (Align -> Validate)")
        try:
            t_start = time.time()
            
            # EXECUTION
            res = self.pipeline.execute_multi_asset(
                self.data_map, 
                strict=True, # Drop nulls
                anchor="BTC" # BTC as base time
            )

            if res.is_err():
                raise RuntimeError(f"Pipeline Error: {res.error}")

            # Collect LazyFrame to DataFrame for Verification
            self.result_df = res.unwrap().collect()
            elapsed = time.time() - t_start
            
            ConsoleUI.ok(f"Success in {elapsed:.3f}s")
            return True

        except Exception as e:
            ConsoleUI.fail(str(e))
            logger.error(f"Execution Failed: {e}", exc_info=True)
            return False

    def verify_results(self) -> bool:
        """Strict assertions on the output."""
        ConsoleUI.header("VERIFICATION PHASE")
        all_passed = True

        def check(condition: bool, msg: str):
            nonlocal all_passed
            if condition:
                logger.info(f"CHECK PASS: {msg}")
                print(f"  [+] {msg}")
            else:
                logger.error(f"CHECK FAIL: {msg}")
                print(f"  [-] {msg}")
                all_passed = False

        # 1. Check Row Count
        check(self.result_df.height == 5, f"Row count must be 5 (Got {self.result_df.height})")

        # 2. Check Column Names (Alignment Suffixes)
        cols = self.result_df.columns
        check("close_BTC" in cols, "Column 'close_BTC' exists")
        check("close_DOGE" in cols, "Column 'close_DOGE' exists")
        
        # 3. Check Data Integrity
        # BTC row 0 should be 100.0
        btc_val = self.result_df["close_BTC"][0]
        check(btc_val == 100.0, f"BTC Row 0 value correct ({btc_val})")

        return all_passed

    def run(self):
        """Main Test Runner."""
        ConsoleUI.header(f"STARTING {TEST_NAME}")
        
        # Sequence
        if not self.setup_components(): sys.exit(1)
        if not self.generate_data(): sys.exit(1)
        if not self.execute_pipeline(): sys.exit(1)
        
        # Verification
        if self.verify_results():
            ConsoleUI.header("TEST SUMMARY")
            print(f"{ConsoleUI.GREEN}INTEGRATION TEST V1 PASSED.{ConsoleUI.RESET}\n")
            
            # Optional: Print Preview
            print("Output Preview:")
            print(self.result_df.head(3))
            sys.exit(0)
        else:
            ConsoleUI.header("TEST SUMMARY")
            print(f"{ConsoleUI.RED}INTEGRATION TEST V1 FAILED.{ConsoleUI.RESET}\n")
            sys.exit(1)

if __name__ == "__main__":
    test = IntegrationTestV1()
    test.run()
