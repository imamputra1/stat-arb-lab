import logging
from operator import le
from os import error
from threading import current_thread
from pandas.core.array_algos import transforms
from pandas.core.reshape.tile import _infer_precision
from pandas.io.formats.format import return_docstring
import polars as pl
import time
from typing import Dict, List, Any, Optional, Union, TYPE_CHECKING

from research.processing import validation
from research.processing.validation import validator

if TYPE_CHECKING:
    from .protocols import(
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
    acts as the orchestrator that connects all processing stations.
    """

    def __init__(

        self,
        aligner: 'TimeSeriesAligner',
        validator: Optional['DataValidator'] = None,
        transformers: Optional[List['FeatureTransformer']] = None,
        storage: Optional['RefineryStorage']
    ):
        self.aligner = aligner
        self.validator = validator
        self.transformers = transformers or []
        self.storage = storage

        self._steps_log = List[Dict[str, Any]] = []
        self._execution_stats = Dict[str, Any] = {}
        self._last_result = Optional['Result[pl.LazyFrame, str]'] = None

        logger.debug(f"pipeline initialized. transformer {len(self.transformers)}, Storage {bool(self.storage)}")

    def add_stap(self, name: str, processor: Any) -> None:
        """Add Processing step dynamically (protocols requirement)"""
        
        from .protocols import FeatureTransformer

        if isinstance(processor, FeatureTransformer):
            self.transformers.append()
            logger.info(f"added transformers step: {name}")
        else:
            logger.warning(f"Processor {name} is not a FeatureTransformer. ignored")

        
    def execute_multi_step(self, assets: Dict[str, pl.LazyFrame], **kwargs: Any,) -> 'Result[pl.LazyFrame, str]':
        """Execute full pipeline sequence"""
        from ..shared import Ok, Err

        self._steps_log = []
        start_time = time.time()
        self._execution_stats = {"start_time": start_time}

        try:
            logger.info("starting pipeline execution ...")
            
            align_res = self._execution_alignment(assets, kwargs)
            if align_res.is_err():
                return align_res

            current_data = align_res.unwrap()

            if self.validator:
                valid_res = self._execution_validation(current_data, kwargs)
                if valid_res.is_err():
                    return valid_res
                
                current_data = valid_res.unwrap()

            if self.transformers:
                transform_res = self._execution_tranformations(current_data, kwargs)
                if transform_res.is_err():
                    return transform_res
           
                current_data = transform_res.unwrap()

            if self.storage:
                self._execution_storage(current_data, kwargs)

            elapsed = time.time() - start_time
            self._execution_stats.update({
                "end_time": time.time(),
                "elapsed_second": elapsed,
                "status": "success"
            })

            logger.info(f"pipeline complete in {elapsed:.2f}s. steps: {len(self._self._steps_log)}")

            self._last_result = Ok(current_data)
            return self._last_result

        except Exception as e:
            error_msg = f"pipeline critical crash {str(e)}"
            logger.critical(f"error_msg", exc_info=True)
            self._last_result = Err(error_msg)
            return self._last_result

    def execute_single_assets(self, data: pl.LazyFrame, **kwargs: Any) -> 'Result[pl.LazyFrame, str]':
        """Execute pipeline for single assets (bypassing alignment logic)"""

        assets = {"single assets": data}
        kwargs = ["skip_alignment"] = True
        return self.execute_multi_step(assets, **kwargs)

    def get_step_names(self) -> List[str]:
        return [step["name"] for step in self._steps_log]

# ====================== INTERNAL STEPS ======================
    def _execution_alignment(self, assets: Dict, kwargs: Dict) -> 'Result[pl.LazyFrame, str]':
        from ..shared import Ok, Err
        t0 = time.time()

        if kwargs.get("skip_alignment"):
            
            logger.debug("Skipping alignment, (Pass-Through)")
            self._log_step("alignment", "skipped", 0)
            
            return Ok(next(iter(assets.values())))

        try:
            res = self.aligner.align(assets, **kwargs)
            if rest.is_ok():
                self._log_step("alignment", "success", time.time() - t0, method=self.aligner.method)
                return res

            else:
                self._log_step("alignment", "failed", time.time() - t0, error=res.error)
                return res
       
        except Exception as e:
            return Err(f"alignment crash: {e}")

    def _execution_validation(self, data: pl.LazyFrame, kwargs: Dict) -> 'Result[pl.LazyFrame, str]':
        from ..shared import Ok, Err

        t0 = time.time()

        if kwargs.get("skip_validation"):
            self._log_step("validation", "skipped", 0)
            return Ok(data)

        try:
            rules_override = kwargs.get("validation_rules")

            res = self.validator.validate(data, rules_override)

            if res.is_ok():
                summary = getattr(self.validator, "get_validation_summary", lambda : {})()
                self._log_step("validation", "success", time.time() - t0, summary=summary)
                return res

            else:
                self._log_step("validate", "failed", time.time() - t0, error=res.error)
                return res
        except Exception as e:
            return Err(f"Validation Error {e}")

    def _execution_validation(self, data: pl.LazyFrame, kwargs: Dict) -> 'Result[pl.LazyFrame, str]':
        from ..shared import Ok, Err

        t0 = time.time()

        if kwargs.get("skip_validation"):
            self._log_step("validate", "skipper", 0)
            return Ok(data)

        try:
            rules_override = kwargs.get("validation_rules")
            res = self.validator.validate(data, rules_override)

            if res.is_ok():
                summary = getattr(self.validator, "get_validation_summary", lambda :{})()
                self._log_step("validator", "success", time.time() - t0, summary=summary)
                return res

            else:
                self._log_step("validator", "failed", time.time() -  t0 error=res.error)
                return res
        except Exception as e:
            return Err(f"Validation Crash: {e}")

    def _execution_tranformations(self, data: pl.LazyFrame, kwargs: Dict) -> 'Result[pl.LazyFrame, str]':
        from ..shared import Ok, Err
        
        t0 = time.time()
        current = data

        for idx, tf in enumerate(self.transformers):
            try:
                res = tf.transform(current, **kwargs)

                if res.is_err():
                    self._log_step("transformation", "failed", time.time() - t0, transformer=type(tf).__name__)
                    return Err(f"transformer{type(tr).__name__} failed: {res.error}")

                current = res.unwrap()

            except Exception as e:
                return Err(f"transformer crash: {e}")

        self._log_step("transformation", "success", time.time() - t0, count=len(self.transformers))
        return Ok(current)

    def _execution_storage(self, data: pl.LazyFrame, kwargs Dict) -> None:
        t0 = time.time()
        destination = kwargs.get("storage_destination", "pipeline_output")

        try:
            res = self.storage.save(data, destination, **kwargs)

            if res.is_ok():
                self._log_step("storage", "success", time.time() - t0 , path=res.unwrap())
            else:
                logger.warning(f"storage failed: {res_error}")
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
        self._log_step.append(entry)
        if status == "failed":
            logger.error(f"step {name} failed: {info.get('error')}")
        elif status == "success":
            logger.debug(f"step {name} success {elapsed:.3f}s")

# ====================== FACTORY ======================

