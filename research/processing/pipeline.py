"""
PIPELINE ORCHESTRATOR - The Boss of Node B
Implements ProcessingPipeline protocol with fail-fast execution.
ADHD-friendly: Clear steps, fast feedback, no hidden complexity.
"""
import logging
import polars as pl
from typing import Dict, Any, List, Optional, Union
import time

# Type-safe imports
from typing import TYPE_CHECKING
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
    Standard implementation of ProcessingPipeline protocol.
    
    Flow: Raw Dict -> [Aligner] -> Aligned Frame -> [Validator] -> Valid Frame -> [Transformers] -> [Storage]
    
    ADHD: Fail-fast, clear logging, predictable behavior.
    """
    
    def __init__(
        self, 
        aligner: 'TimeSeriesAligner',
        validator: Optional['DataValidator'] = None,
        transformers: Optional[List['FeatureTransformer']] = None,
        storage: Optional['RefineryStorage'] = None
    ):
        # Validate inputs
        if not isinstance(aligner, TimeSeriesAligner):
            raise TypeError("aligner must implement TimeSeriesAligner protocol")
        
        self.aligner = aligner
        self.validator = validator
        self.transformers = transformers or []
        self.storage = storage
        
        # State tracking
        self._steps_log: List[Dict[str, Any]] = []
        self._execution_stats: Dict[str, Any] = {}
        self._last_result: Optional['Result[pl.LazyFrame, str]'] = None
        
        logger.info(f"✅ StandardPipeline initialized with {len(self.transformers)} transformers")
    
    def add_step(self, name: str, processor: Any) -> None:
        """Add processing step - protocol requirement."""
        from .protocols import FeatureTransformer
        
        if isinstance(processor, FeatureTransformer):
            self.transformers.append(processor)
            logger.info(f"➕ Added transformer step: {name}")
        else:
            logger.warning(f"⚠️ Processor {name} is not a FeatureTransformer. Ignored.")
    
    def execute_multi_asset(
        self,
        assets: Dict[str, pl.LazyFrame],
        **kwargs: Any
    ) -> 'Result[pl.LazyFrame, str]':
        """
        Execute full pipeline for multiple assets.
        
        Args:
            assets: Dict symbol -> LazyFrame
            **kwargs: Pipeline configuration
            
        Returns:
            Result[pl.LazyFrame, str]: Processed data or error
        """
        from ..shared import Ok, Err
        
        # Reset state
        self._steps_log = []
        self._execution_stats = {"start_time": time.time()}
        start_time = time.time()
        
        try:
            logger.info("🚀 Starting StandardPipeline execution...")
            
            current_data: Union[pl.LazyFrame, None] = None
            
            # --- STEP 1: ALIGNMENT ---
            alignment_result = self._execute_alignment(assets, kwargs)
            if alignment_result.is_err():
                return alignment_result
            
            current_data = alignment_result.unwrap()
            
            # --- STEP 2: VALIDATION (Optional) ---
            if self.validator and current_data is not None:
                validation_result = self._execute_validation(current_data, kwargs)
                if validation_result.is_err():
                    return validation_result
                current_data = validation_result.unwrap()
            
            # --- STEP 3: TRANSFORMATIONS ---
            if self.transformers and current_data is not None:
                transformation_result = self._execute_transformations(current_data, kwargs)
                if transformation_result.is_err():
                    return transformation_result
                current_data = transformation_result.unwrap()
            
            # --- STEP 4: STORAGE (Optional) ---
            if self.storage and current_data is not None:
                storage_result = self._execute_storage(current_data, kwargs)
                if storage_result.is_err():
                    # Storage failure doesn't fail pipeline, just log
                    logger.warning(f"Storage failed (pipeline continues): {storage_result.error}")
            
            # Final success
            elapsed = time.time() - start_time
            self._execution_stats.update({
                "end_time": time.time(),
                "elapsed_seconds": elapsed,
                "status": "success",
                "step_count": len(self._steps_log)
            })
            
            logger.info(f"✅ Pipeline completed in {elapsed:.2f}s, {len(self._steps_log)} steps")
            
            self._last_result = Ok(current_data)
            return self._last_result
            
        except Exception as e:
            error_msg = f"Pipeline critical failure: {str(e)}"
            logger.error(error_msg, exc_info=True)
            
            self._execution_stats.update({
                "end_time": time.time(),
                "elapsed_seconds": time.time() - start_time,
                "status": "error",
                "error": str(e)
            })
            
            self._last_result = Err(error_msg)
            return self._last_result
    
    def execute_single_asset(
        self, 
        data: pl.LazyFrame, 
        **kwargs: Any
    ) -> 'Result[pl.LazyFrame, str]':
        """
        Execute pipeline for single asset (skip alignment).
        
        Args:
            data: Single LazyFrame to process
            **kwargs: Pipeline configuration
            
        Returns:
            Result[pl.LazyFrame, str]: Processed data or error
        """
        # Create mock assets dict for alignment bypass
        assets = {"single_asset": data}
        
        # Override to skip alignment
        kwargs_without_alignment = {**kwargs, "skip_alignment": True}
        
        return self.execute_multi_asset(assets, **kwargs_without_alignment)
    
    def get_step_names(self) -> List[str]:
        """Get names of executed steps - protocol requirement."""
        return [step["name"] for step in self._steps_log]
    
    def get_execution_stats(self) -> Dict[str, Any]:
        """Get detailed execution statistics."""
        return self._execution_stats.copy()
    
    def get_last_result(self) -> Optional['Result[pl.LazyFrame, str]']:
        """Get result of last execution."""
        return self._last_result
    
    # ====================== PRIVATE HELPER METHODS ======================
    
    def _execute_alignment(
        self, 
        assets: Dict[str, pl.LazyFrame], 
        kwargs: Dict[str, Any]
    ) -> 'Result[pl.LazyFrame, str]':
        """Execute alignment step."""
        from ..shared import Ok, Err
        
        step_start = time.time()
        
        try:
            # Skip alignment if requested
            if kwargs.get("skip_alignment", False):
                logger.info("⏭️  Skipping alignment (single asset mode)")
                
                # Return first asset (should be only one)
                first_key = next(iter(assets))
                result = Ok(assets[first_key])
                
                self._steps_log.append({
                    "name": "alignment",
                    "status": "skipped",
                    "elapsed": 0.0,
                    "asset_count": len(assets)
                })
                
                return result
            
            logger.debug(f"🔗 Starting alignment for {len(assets)} assets")
            
            # Execute alignment
            result = self.aligner.align(assets, **kwargs)
            
            step_elapsed = time.time() - step_start
            
            # Log step result
            step_info = {
                "name": "alignment",
                "status": "success" if result.is_ok() else "failed",
                "elapsed": step_elapsed,
                "asset_count": len(assets),
                "method": self.aligner.method
            }
            
            if result.is_err():
                step_info["error"] = result.error
                logger.error(f"❌ Alignment failed: {result.error}")
            else:
                logger.info(f"✅ Alignment completed in {step_elapsed:.2f}s")
            
            self._steps_log.append(step_info)
            return result
            
        except Exception as e:
            step_elapsed = time.time() - step_start
            error_msg = f"Alignment execution error: {str(e)}"
            
            self._steps_log.append({
                "name": "alignment",
                "status": "error",
                "elapsed": step_elapsed,
                "error": error_msg
            })
            
            logger.error(error_msg, exc_info=True)
            return Err(error_msg)
    
    def _execute_validation(
        self, 
        data: pl.LazyFrame, 
        kwargs: Dict[str, Any]
    ) -> 'Result[pl.LazyFrame, str]':
        """Execute validation step."""
        from ..shared import Ok, Err
        
        step_start = time.time()
        
        try:
            # Skip validation if requested
            if kwargs.get("skip_validation", False):
                logger.info("⏭️  Skipping validation")
                
                self._steps_log.append({
                    "name": "validation",
                    "status": "skipped",
                    "elapsed": 0.0
                })
                
                return Ok(data)
            
            logger.debug("🛡️ Starting validation")
            
            # Get validation rules from kwargs if provided
            validation_rules = kwargs.get("validation_rules")
            
            # Execute validation
            if validation_rules:
                result = self.validator.validate(data, validation_rules)
            else:
                result = self.validator.validate(data)
            
            step_elapsed = time.time() - step_start
            
            # Log step result
            step_info = {
                "name": "validation",
                "status": "success" if result.is_ok() else "failed",
                "elapsed": step_elapsed,
                "strict_mode": not kwargs.get("skip_validation", False)
            }
            
            if result.is_err():
                step_info["error"] = result.error
                logger.error(f"❌ Validation failed: {result.error}")
            else:
                logger.info(f"✅ Validation passed in {step_elapsed:.2f}s")
                
                # Log validation summary if available
                if hasattr(self.validator, 'get_validation_summary'):
                    summary = self.validator.get_validation_summary()
                    step_info["summary"] = summary
            
            self._steps_log.append(step_info)
            return result
            
        except Exception as e:
            step_elapsed = time.time() - step_start
            error_msg = f"Validation execution error: {str(e)}"
            
            self._steps_log.append({
                "name": "validation",
                "status": "error",
                "elapsed": step_elapsed,
                "error": error_msg
            })
            
            logger.error(error_msg, exc_info=True)
            return Err(error_msg)
    
    def _execute_transformations(
        self, 
        data: pl.LazyFrame, 
        kwargs: Dict[str, Any]
    ) -> 'Result[pl.LazyFrame, str]':
        """Execute transformation steps."""
        from ..shared import Ok, Err
        
        step_start = time.time()
        current_data = data
        
        try:
            if not self.transformers:
                self._steps_log.append({
                    "name": "transformation",
                    "status": "skipped",
                    "elapsed": 0.0,
                    "transformer_count": 0
                })
                return Ok(data)
            
            logger.debug(f"🔧 Starting {len(self.transformers)} transformations")
            
            transformers_executed = []
            
            for i, transformer in enumerate(self.transformers):
                transformer_start = time.time()
                transformer_name = transformer.__class__.__name__
                
                try:
                    # Execute transformation
                    result = transformer.transform(current_data, **kwargs)
                    
                    if result.is_err():
                        error_msg = f"Transformer {i+1} ({transformer_name}) failed: {result.error}"
                        logger.error(error_msg)
                        
                        self._steps_log.append({
                            "name": "transformation",
                            "status": "failed",
                            "elapsed": time.time() - step_start,
                            "transformer_count": len(self.transformers),
                            "executed_count": len(transformers_executed),
                            "failed_transformer": transformer_name,
                            "error": result.error
                        })
                        
                        return result
                    
                    # Update data for next transformer
                    current_data = result.unwrap()
                    transformers_executed.append(transformer_name)
                    
                    logger.debug(f"  ✅ Transformer {i+1}/{len(self.transformers)}: {transformer_name}")
                    
                except Exception as e:
                    error_msg = f"Transformer {i+1} ({transformer_name}) crashed: {str(e)}"
                    logger.error(error_msg, exc_info=True)
                    
                    self._steps_log.append({
                        "name": "transformation",
                        "status": "error",
                        "elapsed": time.time() - step_start,
                        "transformer_count": len(self.transformers),
                        "executed_count": len(transformers_executed),
                        "failed_transformer": transformer_name,
                        "error": error_msg
                    })
                    
                    return Err(error_msg)
            
            step_elapsed = time.time() - step_start
            
            self._steps_log.append({
                "name": "transformation",
                "status": "success",
                "elapsed": step_elapsed,
                "transformer_count": len(self.transformers),
                "executed_count": len(transformers_executed),
                "transformers": transformers_executed
            })
            
            logger.info(f"✅ Transformations completed in {step_elapsed:.2f}s")
            return Ok(current_data)
            
        except Exception as e:
            step_elapsed = time.time() - step_start
            error_msg = f"Transformations execution error: {str(e)}"
            
            self._steps_log.append({
                "name": "transformation",
                "status": "error",
                "elapsed": step_elapsed,
                "error": error_msg
            })
            
            logger.error(error_msg, exc_info=True)
            return Err(error_msg)
    
    def _execute_storage(
        self, 
        data: pl.LazyFrame, 
        kwargs: Dict[str, Any]
    ) -> 'Result[str, str]':
        """Execute storage step."""
        from ..shared import Ok, Err
        
        step_start = time.time()
        
        try:
            # Skip storage if requested
            if kwargs.get("skip_storage", False):
                logger.info("⏭️  Skipping storage")
                
                self._steps_log.append({
                    "name": "storage",
                    "status": "skipped",
                    "elapsed": 0.0
                })
                
                return Ok("storage_skipped")
            
            logger.debug("💾 Starting storage")
            
            # Collect data for storage (storage typically needs DataFrame)
            df = data.collect()
            
            # Get destination from kwargs or use default
            destination = kwargs.get("storage_destination", "pipeline_output")
            
            # Execute storage
            result = self.storage.save(df, destination, **kwargs)
            
            step_elapsed = time.time() - step_start
            
            # Log step result
            step_info = {
                "name": "storage",
                "status": "success" if result.is_ok() else "failed",
                "elapsed": step_elapsed,
                "destination": destination,
                "row_count": len(df)
            }
            
            if result.is_err():
                step_info["error"] = result.error
                logger.warning(f"⚠️ Storage failed: {result.error}")
            else:
                logger.info(f"✅ Storage completed in {step_elapsed:.2f}s to {result.unwrap()}")
            
            self._steps_log.append(step_info)
            return result
            
        except Exception as e:
            step_elapsed = time.time() - step_start
            error_msg = f"Storage execution error: {str(e)}"
            
            self._steps_log.append({
                "name": "storage",
                "status": "error",
                "elapsed": step_elapsed,
                "error": error_msg
            })
            
            logger.error(error_msg, exc_info=True)
            return Err(error_msg)

# ====================== FACTORY FUNCTION ======================

def create_standard_pipeline(
    aligner: 'TimeSeriesAligner',
    validator: Optional['DataValidator'] = None,
    transformers: Optional[List['FeatureTransformer']] = None,
    storage: Optional['RefineryStorage'] = None
) -> 'Result[StandardPipeline, str]':
    """
    Factory function for creating StandardPipeline.
    
    Args:
        aligner: TimeSeriesAligner instance
        validator: Optional DataValidator instance
        transformers: Optional list of FeatureTransformer instances
        storage: Optional RefineryStorage instance
        
    Returns:
        Result[StandardPipeline, str]: Pipeline instance or error
    """
    from ..shared import Ok, Err
    
    try:
        # Verify aligner protocol compliance
        from .protocols import TimeSeriesAligner
        if not isinstance(aligner, TimeSeriesAligner):
            return Err("aligner must implement TimeSeriesAligner protocol")
        
        # Verify validator protocol compliance if provided
        if validator:
            from .protocols import DataValidator
            if not isinstance(validator, DataValidator):
                return Err("validator must implement DataValidator protocol")
        
        # Verify transformers protocol compliance
        if transformers:
            from .protocols import FeatureTransformer
            for i, tf in enumerate(transformers):
                if not isinstance(tf, FeatureTransformer):
                    return Err(f"transformer at index {i} must implement FeatureTransformer protocol")
        
        # Verify storage protocol compliance if provided
        if storage:
            from .protocols import RefineryStorage
            if not isinstance(storage, RefineryStorage):
                return Err("storage must implement RefineryStorage protocol")
        
        # Create pipeline
        pipeline = StandardPipeline(aligner, validator, transformers, storage)
        
        return Ok(pipeline)
        
    except Exception as e:
        return Err(f"Failed to create pipeline: {e}")

# ====================== TYPE-SAFE EXPORTS ======================

__all__ = [
    "StandardPipeline",
    "create_standard_pipeline",
]

# ====================== RUNTIME VALIDATION ======================

def _test_pipeline_creation() -> None:
    """Test pipeline creation on import."""
    try:
        # Create mock aligner for testing
        from .alignment import get_default_aligner
        from ..shared import match
        
        aligner_result = get_default_aligner()
        
        def test_with_aligner(aligner):
            from .validation import get_default_validator
            
            # Get default validator
            validator_result = get_default_validator()
            
            def test_with_validator(validator):
                # Create pipeline with just aligner and validator
                pipeline_result = create_standard_pipeline(aligner, validator)
                
                if pipeline_result.is_ok():
                    logger.debug("✅ Pipeline creation test passed")
                else:
                    logger.warning(f"⚠️ Pipeline creation test failed: {pipeline_result.error}")
            
            def handle_validator_error(err):
                logger.warning(f"⚠️ Validator creation failed: {err}")
            
            match(validator_result, test_with_validator, handle_validator_error)
        
        def handle_aligner_error(err):
            logger.warning(f"⚠️ Aligner creation failed: {err}")
        
        match(aligner_result, test_with_aligner, handle_aligner_error)
        
    except Exception as e:
        logger.error(f"❌ Pipeline module test failed: {e}")

_test_pipeline_creation()
