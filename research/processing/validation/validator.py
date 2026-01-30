import logging
import time
from typing import Dict, Any, Optional
import polars as pl

# Type-safe imports
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from ...shared import Result

# Import Shared
from ...shared import Ok, Err
# Import Rules Definition
from .rules import ValidationRules

logger = logging.getLogger("DataValidator")

class PolarsValidator:
    """
    Implementation of DataValidator Protocol.
    Blocks defective data before it enters Feature Engineering.
    Uses 'ValidationRules' (Immutable) for configuration.
    """

    def __init__(self, default_rules: Optional[ValidationRules] = None):
        # 1. Load Default Rules
        self.default_rules = default_rules or ValidationRules()
        
        # 2. Init Status Summary
        self.last_summary: Dict[str, Any] = {
            "status": "idle", 
            "timestamp": time.time()
        }
        logger.debug(f"PolarsValidator ready. Min rows: {self.default_rules.min_rows}")

    def validate(
        self, 
        data: pl.LazyFrame, 
        rules: Optional[Dict[str, Any]] = None
    ) -> 'Result[pl.LazyFrame, str]':
        """
        Execute validation pipeline (Schema -> Stats -> Integrity).
        """
        start_time = time.time()
        
        try:
            # --- STEP 1: PREPARE RULES ---
            active_rules = self._resolve_rules(rules)
            
            # --- STEP 2: SCHEMA CHECK (Metadata only) ---
            schema_res = self._validate_schema(data, active_rules)
            if schema_res.is_err():
                return schema_res

            # --- STEP 3: STATISTICAL CHECK (Triggers Collect) ---
            stats = self._collect_stats(data, active_rules)
            
            # Analisa hasil statistik
            stats_res = self._analyze_stats(stats, active_rules)
            if stats_res.is_err():
                return stats_res

            # --- STEP 4: INTEGRITY CHECK ---
            # Sorting check
            if active_rules.check_sorted:
                sort_res = self._validate_sorting(data)
                if sort_res.is_err():
                    return sort_res

            # Price Consistency
            if active_rules.check_ohlc_consistency:
                price_res = self._validate_price_consistency(data)
                if price_res.is_err():
                    return price_res

            # --- FINISH ---
            elapsed = time.time() - start_time
            row_count = stats.get("row_count", 0)
            
            logger.info(f"Validation Passed: {row_count} rows in {elapsed:.3f}s")
            self._update_summary("passed", active_rules, stats, elapsed)
            
            return Ok(data)

        except Exception as e:
            err_msg = f"Validator System Error: {str(e)}"
            logger.error(err_msg, exc_info=True)
            self._update_summary("error", self.default_rules, {"error": str(e)}, 0.0)
            return Err(err_msg)

    def get_validation_summary(self) -> Dict[str, Any]:
        return self.last_summary.copy()

    # ====================== PRIVATE LOGIC ======================

    def _resolve_rules(self, overrides: Optional[Dict[str, Any]]) -> ValidationRules:
        if not overrides:
            return self.default_rules
        return self.default_rules.with_overrides(**overrides)

    def _validate_schema(self, data: pl.LazyFrame, rules: ValidationRules) -> 'Result[None, str]':
        try:
            schema = data.collect_schema()
            cols = schema.names()

            # Missing Columns
            missing = [c for c in rules.required_columns if c not in cols]
            if missing:
                return Err(f"Schema Mismatch: Missing required columns {missing}")

            # Strict Type Check
            if rules.strict_types and "timestamp" in cols:
                ts_type = schema["timestamp"]
                is_valid = (ts_type == pl.Int64 or "Datetime" in str(ts_type))
                if not is_valid:
                    return Err(f"Type Error: 'timestamp' must be Int64 or Datetime, got {ts_type}")

            return Ok(None)
        except Exception as e:
            return Err(f"Schema Validation Failed: {e}")

    def _collect_stats(self, data: pl.LazyFrame, rules: ValidationRules) -> Dict[str, Any]:
        exprs = [pl.len().alias("row_count")]
        
        target_cols = set(rules.required_columns)
        ohlc_cols = {"open", "high", "low", "close", "volume"}
        available_cols = data.collect_schema().names()
        cols_to_check = [c for c in available_cols if c in target_cols or c in ohlc_cols]

        for col in cols_to_check:
            exprs.append(pl.col(col).null_count().alias(f"nulls_{col}"))

        # BUG FIX: Handle empty dataframe safely
        try:
            df_stats = data.select(exprs).collect()
            if df_stats.height == 0:
                return {"row_count": 0}
            return df_stats.row(0, named=True)
        except Exception as e:
            logger.warning(f"Stats collection failed (possibly empty): {e}")
            return {"row_count": 0}

    def _analyze_stats(self, stats: Dict[str, Any], rules: ValidationRules) -> 'Result[None, str]':
        row_count = stats.get("row_count", 0)

        # Min Rows
        if row_count < rules.min_rows:
            return Err(f"Insufficient Data: Got {row_count} rows (Required min: {rules.min_rows})")

        # Null Checks
        null_keys = [k for k in stats.keys() if k.startswith("nulls_")]
        if not null_keys or row_count == 0:
            return Ok(None)

        for key in null_keys:
            col_name = key.replace("nulls_", "")
            n_null = stats[key]
            ratio = n_null / row_count
            
            if ratio > rules.max_null_pct:
                return Err(
                    f"Data Quality Bad: Column '{col_name}' has {ratio:.1%} nulls "
                    f"(Max Allowed: {rules.max_null_pct:.1%})"
                )
        
        return Ok(None)

    def _validate_sorting(self, data: pl.LazyFrame) -> 'Result[None, str]':
        """Check timestamp order (Robust implementation)."""
        try:
            # Logika: Jika diff minimal >= 0, berarti urut (ascending/flat).
            # cast ke Int64 dulu agar diff selalu angka (bukan Duration/Timedelta)
            check_expr = (
                pl.col("timestamp")
                .cast(pl.Int64)
                .diff()
                .fill_null(0)
                .min() 
                >= 0
            )
            
            is_sorted = data.select(check_expr.alias("sorted")).collect()["sorted"][0]
            
            if not is_sorted:
                return Err("Integrity Error: Timestamp is not sorted ascending")
            return Ok(None)
        except Exception as e:
            logger.warning(f"Sorting check warning: {e}")
            return Ok(None)

    def _validate_price_consistency(self, data: pl.LazyFrame) -> 'Result[None, str]':
        """Check logical constraints: High >= Low, Price > 0."""
        cols = data.collect_schema().names()
        if not {"high", "low", "close"}.issubset(set(cols)):
            return Ok(None) 

        violations = data.select([
            (pl.col("high") < pl.col("low")).sum().alias("bad_wick"),
            (pl.col("close") <= 0).sum().alias("bad_price")
        ]).collect()

        bad_wick = violations["bad_wick"][0]
        bad_price = violations["bad_price"][0]

        if bad_wick > 0:
            return Err(f"Integrity Error: Found {bad_wick} rows where High < Low")
        if bad_price > 0:
            return Err(f"Integrity Error: Found {bad_price} rows where Price <= 0")

        return Ok(None)

    def _update_summary(self, status: str, rules: ValidationRules, stats: Dict, elapsed: float):
        self.last_summary = {
            "status": status,
            "timestamp": time.time(),
            "elapsed": elapsed,
            "rules_snapshot": rules.to_dict(),
            "stats": stats
        }

# ====================== FACTORY ======================

def create_validator(rules: Optional[Dict[str, Any]] = None) -> 'Result[PolarsValidator, str]':
    try:
        rules_obj = ValidationRules.from_dict(rules) if rules else None
        validator = PolarsValidator(default_rules=rules_obj)
        return Ok(validator)
    except Exception as e:
        return Err(f"Validator Creation Failed: {e}")

def get_default_validator() -> 'Result[PolarsValidator, str]':
    return create_validator()

# ====================== EXPORTS ======================
__all__ = ["PolarsValidator", "create_validator", "get_default_validator"]
