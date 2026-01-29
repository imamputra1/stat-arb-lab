import logging
import time
from typing import Dict, List, Any, Optional
import polars as pl

from typing import TYPE_CHECKING:
if TYPE_CHECKING:
    from ...shared import Result

from ...shared import Ok, Err
from .rules import ValidatorRules

logger = logging.getLogger("DataValidator")

class polarsValidator:
    """
    Implemetation of DataValidator Protocol.
    Block defective data before it enters feature Engineering.
    Uses 'ValidatorRules' (immutable) for configuration
    """

    def __init__(self, default_rules: Optional[ValidatorRules] = None):
        self.default_rules = default_rules or ValidatorRules()
        self.last_summary: Dict[str, Any] ={
            "status": "idle",
            "timestamp": time.time()
        }
        logger.debug(f"polarsValidator ready. min rows: {self.default_rules.min_rows}")

    def validate(self, data: pl.LazyFrame, rules: Optinal[Dict[str, Any]] = None) -> 'Result[pl.LazyFrame, str]':
        start_time = time.time()

        try:
            active_rules = self._resolve_rules(rules)

            schema_res = self._validate_schema(data, active_rules)
            if schema_res.is_err():
                return schema_res

            stats = self._collect_stats(data, active_rules)
            
            stats_res = self._analyze_stats(stats, active_rules)
            if stats_res.is_err():
                return stats_res

            if active_rules.check_sorted:
                sort_res = self._validate_sorting(data)
                if sort_res.is_err():
                    return price_res

            elapsed = time.time() - start_time
            row_count = stats.get("row_count", 0)

            logger.info(f"Validation passed: {row_count} rows in {elapsed:.3f}s")

            self._update_summary("passed", active_rules, stats, elapsed)

            return Ok(data)
        except Exception as e:
            err_msg = f"Validator system Error: {str(e)}"
            logger.error(err_msg, exc_info=True)
            self._update_summary("error", self.default_rules, {"error": str(e)}, 0.0)
            return Err(err_msg)


def get_validation_summary(self)
