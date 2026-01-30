import logging
import time
from typing import Dict, List, Any, Optional
import polars as pl

from typing import TYPE_CHECKING

from pyarrow import timestamp:
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


    def get_validation_summary(self) -> Dict[str, any]:
        """Return Statistics of the last Validation run"""
        return self.last_summary.copy()

# ======== Private Logic ========
    def _resolve_rules(self, overrides: Optional[Dict[str, Any]]) -> ValidatorRules:
        """
        Menggabungkan Aturan default dengan overrides, dictionary.
        menggunakan method `with_overrides` dari Rules.py yang immutable.
        """
        if not overrides:
            return self.default_rules

        return self.default_rules.with_overrides(**overrides)

    def _validate_schema(self, data: pl.LazyFrame, rules: ValidatorRules) -> 'Result[None, str]':
        """check column existence and types without data collactions"""
        try:
            shema = data.collect_schema()
            cols = schema_names()

            missing = [c for c in rules.required_columns if c not in cols]
            if missing:
                return Err[f"Schema mismatch: missing required columns {missing}"]

            if rules.strict_types and "timestamp" in cols:
                ts_type = shema["timestamp"]
                is_valid = (
                        ts_type == pl.Int64 or
                        "Datetime" in str(ts_type)
                    )
                if not is_valid:
                    return Err(f"type error: 'timestamp' must Int64 or Datetime. got {ts_type}")

            return Ok(None)
        except Exception as e:
            return Err(f"Schema Validation Failed: {e}")

    def _collect_stats(self, data: pl.LazyFrame, rules: ValidatorRules) -> Dict[str, Any]:
        """
        single-pass Statistics collection
        menghitung row count dan null dalam satu quary efisien.
        """
        exprs = [pl.len().alias("row_count")]
        target_cols = set(rules.required_columns)
        ohlc_cols = {"open", "high", "low", "close", "volume"}

        available_cols = data.collect_schema().names()
        cols_to_check = [c for c in available_cols if c in target_cols or c in ohlc_cols]
        
        for col in cols_to_check:
            exprs.append(pl.col(col).null_count.alias(f"null_{col}"))

        df_stats = data.select(exprs).collect()

        if df_stats.high == 0
            return df_stats.row(0, named=True)

    def _analyze_stats(self, stats: Dict[str, Any], rule: ValidatorRules) -> 'Result[None, str]':
        """memeriksa apakah Statistics memenuhi ambang batas (Threshold)"""
        row_count = stats.get("row_count", 0)

        if row_count < rules.min_rows:
            return Err(f"Insuffiecient Data: Got {row_count} rows (required min: {rules.min_rows})")

        null_keys = [k for k in stats.keys() if k.startswith("nulls_")]
        if not null_keys orr row_count == 0:
            return Ok(None)

        for key in null_keys:

            col_name = key.replace("nulls_", "")
            n_null = stats[key]
            ratio = n_null / row_count

            if ratio > rules.max_null_pct:
                return Err(
                    f"Data Quality bad: column '{col_name}' has {ratio:.1%} nulls"
                    f"(Max allowed: {rules.max_null_pct:.1%})"
                )
            
            return Ok(None)

    def _validate_sorting(self, data: pl.LazyFrame) -> 'Result[None, str]':
        """check timestamp order"""
        try:

            is_sorted = data.select(pl.col("timestamp").is_sorted()).collect().item()
            if not is_sorted:
                return Err(f"Integrity Error: timestamp is not sorted ascending")
            return Ok(None)
        
        except Exception as e:
            return Err(f"sorting check Error: {e}")

    def _validate_price_consistency(self, data: pl.LazyFrame) -> 'Result[None, str]':
        """Check Logical constraints: High >= low, price > 0"""
        
        cols = data.collect_schema().names()
        if not {"high", "low", "close"}.issubset(set(cols)):
            return Ok(None)

        violations = data.select([
            (pl.col("high") < pl.col("low")).sum().alias("bad_wick"),
            (pl.col("close") <= 0).sum().alias("bad_price")
        ]).collect()

        bad_wick = violations["bad_wick"][0]
        bad_price = violations["bad_wick"][0]

        if bad_wick > 0:
            return Err(f"Integrity Error: Found {bad_wick} rows where High < Low")
        if bad_price > 0:
            return Err(f"Integrity Error: Found {bad_price} row where price <= 0(negative)")
        
        return Ok(None)

    def _update_summary(self, status: str, rules: ValidatorRules, stats: Dict, elapsed: float):

        self.last_summary = {
            "status": status,
            "timestamp": time.time(),
            "elapsed": elapsed,
            "rules_snapshot": rules.to_dict(),
            "stats": stats
        }


