"""
PIPELINE ORCHESTRATOR - The Boss of Node B
Implements ProcessingPipeline protocol with fail-fast execution.
"""
import logging
import polars as pl
import time
from typing import Dict, Any, List, Optional, TYPE_CHECKING

# Type-safe imports for Protocol Checks
if TYPE_CHECKING:
    from .protocols import (
        TimeSeriesAligner, 
        DataValidator, 
        FeatureTransformer,
        RefineryStorage
    )
    from ..shared import Result

logger = logging.getLogger("ProcessingPipeline")

class StandardPipeline:
    """
    Standard implementation of ProcessingPipeline.
    Acts as the Orchestrator that connects all processing stations.
    """
    
    def __init__(
        self, 
        aligner: 'TimeSeriesAligner',
        validator: Optional['DataValidator'] = None,
        transformers: Optional[List['FeatureTransformer']] = None,
        storage: Optional['RefineryStorage'] = None
    ):
        # Validation dilakukan oleh Factory, tapi double check di sini bagus
        self.aligner = aligner
        self.validator = validator
        self.transformers = transformers or []
        self.storage = storage
        
        # State tracking (Audit Trail)
        self._steps_log: List[Dict[str, Any]] = []
        self._execution_stats: Dict[str, Any] = {}
        self._last_result: Optional['Result[pl.LazyFrame, str]'] = None
        
        logger.debug(f"Pipeline initialized. Transformers: {len(self.transformers)}, Storage: {bool(self.storage)}")
    
    def add_step(self, name: str, processor: Any) -> None:
        """Add processing step dynamically (Protocol requirement)."""
        # Kita import di dalam method untuk menghindari circular import saat runtime
        from .protocols import FeatureTransformer
        
        if isinstance(processor, FeatureTransformer):
            self.transformers.append(processor)
            logger.info(f"Added transformer step: {name}")
        else:
            logger.warning(f"Processor {name} is not a FeatureTransformer. Ignored.")
    
    def execute_multi_asset(
        self,
        assets: Dict[str, pl.LazyFrame],
        **kwargs: Any
    ) -> 'Result[pl.LazyFrame, str]':
        """
        Execute full pipeline sequence.
        """
        from ..shared import Ok, Err
        
        # Reset State
        self._steps_log = []
        start_time = time.time()
        self._execution_stats = {"start_time": start_time}
        
        try:
            logger.info("Starting Pipeline Execution...")
            
            # --- STEP 1: ALIGNMENT ---
            # Dict[str, LazyFrame] -> LazyFrame
            align_res = self._execute_alignment(assets, kwargs)
            if align_res.is_err():
                return align_res # Fail Fast
            
            current_data = align_res.unwrap()
            
            # --- STEP 2: VALIDATION ---
            # LazyFrame -> LazyFrame
            if self.validator:
                valid_res = self._execute_validation(current_data, kwargs)
                if valid_res.is_err():
                    return valid_res # Fail Fast
                current_data = valid_res.unwrap()
            
            # --- STEP 3: TRANSFORMATIONS ---
            # LazyFrame -> LazyFrame
            if self.transformers:
                transform_res = self._execute_transformations(current_data, kwargs)
                if transform_res.is_err():
                    return transform_res
                current_data = transform_res.unwrap()
            
            # --- STEP 4: STORAGE (Side Effect) ---
            # LazyFrame -> Path (String)
            if self.storage:
                # Storage tidak menghentikan pipeline jika gagal (opsional, bisa diubah policy-nya)
                self._execute_storage(current_data, kwargs)
            
            # --- FINISH ---
            elapsed = time.time() - start_time
            self._execution_stats.update({
                "end_time": time.time(),
                "elapsed_seconds": elapsed,
                "status": "success"
            })
            
            logger.info(f"Pipeline Completed in {elapsed:.2f}s. Steps: {len(self._steps_log)}")
            
            self._last_result = Ok(current_data)
            return self._last_result
            
        except Exception as e:
            error_msg = f"Pipeline Critical Crash: {str(e)}"
            logger.critical(error_msg, exc_info=True)
            self._last_result = Err(error_msg)
            return self._last_result
    
    def execute_single_asset(
        self, 
        data: pl.LazyFrame, 
        **kwargs: Any
    ) -> 'Result[pl.LazyFrame, str]':
        """Execute pipeline for single asset (Bypassing Alignment logic)."""
        # Wrap single asset to dict to reuse logic, but instruct aligner to skip/passthrough if supported
        # Or implicitly handled by _execute_alignment logic below
        assets = {"single_asset": data}
        kwargs["skip_alignment"] = True
        return self.execute_multi_asset(assets, **kwargs)
    
    def get_step_names(self) -> List[str]:
        return [step["name"] for step in self._steps_log]

    # ====================== INTERNAL STEPS ======================
    
    def _execute_alignment(self, assets: Dict, kwargs: Dict) -> 'Result[pl.LazyFrame, str]':
        from ..shared import Ok, Err
        t0 = time.time()
        
        # Check Bypass
        if kwargs.get("skip_alignment"):
            logger.debug("Skipping alignment (Pass-through)")
            self._log_step("alignment", "skipped", 0)
            return Ok(next(iter(assets.values())))

        try:
            res = self.aligner.align(assets, **kwargs)
            if res.is_ok():
                self._log_step("alignment", "success", time.time() - t0, method=self.aligner.method)
                return res
            else:
                self._log_step("alignment", "failed", time.time() - t0, error=res.error)
                return res
        except Exception as e:
            return Err(f"Alignment Crash: {e}")

    def _execute_validation(self, data: pl.LazyFrame, kwargs: Dict) -> 'Result[pl.LazyFrame, str]':
        from ..shared import Ok, Err
        t0 = time.time()
        
        # Check Bypass
        if kwargs.get("skip_validation"):
            self._log_step("validation", "skipped", 0)
            return Ok(data)

        try:
            # Ambil rules spesifik dari kwargs jika ada
            rules_override = kwargs.get("validation_rules")
            
            res = self.validator.validate(data, rules_override)
            
            if res.is_ok():
                # Jika validator punya summary, kita log
                summary = getattr(self.validator, "get_validation_summary", lambda: {})()
                self._log_step("validation", "success", time.time() - t0, summary=summary)
                return res
            else:
                self._log_step("validation", "failed", time.time() - t0, error=res.error)
                return res
        except Exception as e:
            return Err(f"Validation Crash: {e}")

    def _execute_transformations(self, data: pl.LazyFrame, kwargs: Dict) -> 'Result[pl.LazyFrame, str]':
        from ..shared import Ok, Err
        t0 = time.time()
        current = data
        
        for idx, tf in enumerate(self.transformers):
            try:
                # Transform
                res = tf.transform(current, **kwargs)
                if res.is_err():
                    self._log_step("transformation", "failed", time.time() - t0, transformer=type(tf).__name__)
                    return Err(f"Transformer {type(tf).__name__} failed: {res.error}")
                
                current = res.unwrap()
                
            except Exception as e:
                return Err(f"Transformer Crash: {e}")

        self._log_step("transformation", "success", time.time() - t0, count=len(self.transformers))
        return Ok(current)

    def _execute_storage(self, data: pl.LazyFrame, kwargs: Dict) -> None:
        """
        Executes storage. 
        IMPORTANT: We pass LazyFrame directly. We do NOT collect() here.
        Let the Storage Implementation decide (Sink vs Collect).
        """
        t0 = time.time()
        destination = kwargs.get("storage_destination", "pipeline_output")
        
        try:
            # Storage save mengembalikan Result[str, str] (Path)
            res = self.storage.save(data, destination, **kwargs)
            
            if res.is_ok():
                self._log_step("storage", "success", time.time() - t0, path=res.unwrap())
            else:
                logger.warning(f"Storage failed: {res.error}")
                self._log_step("storage", "failed", time.time() - t0, error=res.error)
                
        except Exception as e:
            logger.error(f"Storage Crash: {e}")

    def _log_step(self, name: str, status: str, elapsed: float, **info):
        entry = {
            "name": name,
            "status": status,
            "elapsed": elapsed,
            **info
        }
        self._steps_log.append(entry)
        if status == "failed":
            logger.error(f"Step {name} Failed: {info.get('error')}")
        elif status == "success":
            logger.debug(f"Step {name} OK ({elapsed:.3f}s)")


# ====================== FACTORY ======================

def create_standard_pipeline(
    aligner: 'TimeSeriesAligner',
    validator: Optional['DataValidator'] = None,
    transformers: Optional[List['FeatureTransformer']] = None,
    storage: Optional['RefineryStorage'] = None
) -> 'Result[StandardPipeline, str]':
    """
    Safe Factory for Pipeline. Ensures all components comply with Protocols.
    """
    from ..shared import Ok, Err
    from .protocols import TimeSeriesAligner, DataValidator, FeatureTransformer, RefineryStorage

    try:
        # 1. Check Aligner (Mandatory)
        if not isinstance(aligner, TimeSeriesAligner):
            return Err(f"Aligner must implement TimeSeriesAligner, got {type(aligner)}")

        # 2. Check Validator
        if validator and not isinstance(validator, DataValidator):
            return Err(f"Validator must implement DataValidator, got {type(validator)}")

        # 3. Check Transformers
        if transformers:
            for tf in transformers:
                if not isinstance(tf, FeatureTransformer):
                    return Err(f"Invalid Transformer: {type(tf)}")

        # 4. Check Storage
        if storage and not isinstance(storage, RefineryStorage):
            return Err(f"Storage must implement RefineryStorage, got {type(storage)}")

        return Ok(StandardPipeline(aligner, validator, transformers, storage))

    except Exception as e:
        return Err(f"Pipeline Factory Error: {e}")


# ====================== EXPORTS ======================
__all__ = ["StandardPipeline", "create_standard_pipeline"]


# ====================== SELF CHECK ======================
def _test_pipeline_wiring():
    """Simple wiring check on import."""
    try:
        # Import Aligner & Validator Factories
        from .alignment import get_aligner
        from .validation import get_default_validator
        
        # Mock Init
        aligner = get_aligner("asof").unwrap()
        validator = get_default_validator().unwrap()
        
        # Test Factory
        pipe_res = create_standard_pipeline(aligner, validator)
        
        if pipe_res.is_ok():
            logger.debug("Pipeline Wiring Check: OK")
        else:
            logger.warning(f"Pipeline Wiring Check Failed: {pipe_res.error}")
            
    except Exception as e:
        # Jangan crash jika module lain belum siap, cuma log warning
        logger.debug(f"Pipeline Wiring Skipped (Dependencies not ready): {e}")

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    _test_pipeline_wiring()
