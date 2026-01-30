"""
VALIDATION MODULE
The Gatekeeper implementation using Polars.
Ensures data quality standards (Schema, Stats, Integrity) before processing.
"""
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
        Lazy Evaluation preferred where possible.
        """
        start_time = time.time()
        
        try:
            # --- STEP 1: PREPARE RULES ---
            # Merge runtime overrides dengan default rules
            active_rules = self._resolve_rules(rules)
            
            # --- STEP 2: SCHEMA CHECK (Metadata only, Cheap) ---
            # Cek keberadaan kolom wajib dan tipe data timestamp
            schema_res = self._validate_schema(data, active_rules)
            if schema_res.is_err():
                return schema_res

            # --- STEP 3: STATISTICAL CHECK (Triggers Collect) ---
            # Kita lakukan satu kali 'collect' cerdas untuk mengambil semua statistik
            stats = self._collect_stats(data, active_rules)
            
            # Analisa hasil statistik (Row count, Nulls)
            stats_res = self._analyze_stats(stats, active_rules)
            if stats_res.is_err():
                return stats_res

            # --- STEP 4: INTEGRITY CHECK (Sorting, OHLC) ---
            # Sorting check (Opsional karena mahal)
            if active_rules.check_sorted:
                sort_res = self._validate_sorting(data)
                if sort_res.is_err():
                    return sort_res

            # Price Consistency (High >= Low, dll)
            if active_rules.check_ohlc_consistency:
                price_res = self._validate_price_consistency(data)
                if price_res.is_err():
                    return price_res

            # --- FINISH ---
            elapsed = time.time() - start_time
            row_count = stats.get("row_count", 0)
            
            logger.info(f"✅ Validation Passed: {row_count} rows in {elapsed:.3f}s")
            
            self._update_summary("passed", active_rules, stats, elapsed)
            
            # Return original LazyFrame (Zero-Copy)
            return Ok(data)

        except Exception as e:
            err_msg = f"Validator System Error: {str(e)}"
            logger.error(err_msg, exc_info=True)
            self._update_summary("error", self.default_rules, {"error": str(e)}, 0.0)
            return Err(err_msg)

    def get_validation_summary(self) -> Dict[str, Any]:
        """Return statistics of the last validation run."""
        return self.last_summary.copy()

    # ====================== PRIVATE LOGIC ======================

    def _resolve_rules(self, overrides: Optional[Dict[str, Any]]) -> ValidationRules:
        """
        Menggabungkan aturan default dengan override dictionary.
        Menggunakan method `with_overrides` dari Rules.py yang immutable.
        """
        if not overrides:
            return self.default_rules
        
        # Gunakan method aman dari Rules.py
        return self.default_rules.with_overrides(**overrides)

    def _validate_schema(self, data: pl.LazyFrame, rules: ValidationRules) -> 'Result[None, str]':
        """Check column existence and types without data collection."""
        try:
            # Gunakan collect_schema() yang lebih robust untuk LazyFrame
            schema = data.collect_schema()
            cols = schema.names()

            # 1. Missing Columns
            missing = [c for c in rules.required_columns if c not in cols]
            if missing:
                return Err(f"Schema Mismatch: Missing required columns {missing}")

            # 2. Strict Type Check (Timestamp)
            if rules.strict_types and "timestamp" in cols:
                ts_type = schema["timestamp"]
                # Valid: Int64 (Unix) atau Datetime (Any precision)
                # Polars type check bisa tricky, kita cek string repr atau class
                is_valid = (
                    ts_type == pl.Int64 or 
                    "Datetime" in str(ts_type)
                )
                if not is_valid:
                    return Err(f"Type Error: 'timestamp' must be Int64 or Datetime, got {ts_type}")

            return Ok(None)
        except Exception as e:
            return Err(f"Schema Validation Failed: {e}")

    def _collect_stats(self, data: pl.LazyFrame, rules: ValidationRules) -> Dict[str, Any]:
        """
        Single-pass statistics collection.
        Menghitung row count dan null counts dalam satu query efisien.
        """
        # Bangun ekspresi untuk query
        exprs = [pl.len().alias("row_count")]
        
        # Tambahkan null check hanya untuk kolom wajib (hemat resource)
        # dan kolom OHLCV jika ada (untuk mendeteksi bolong data market)
        target_cols = set(rules.required_columns)
        ohlc_cols = {"open", "high", "low", "close", "volume"}
        
        # Filter kolom yang benar-benar ada di data
        available_cols = data.collect_schema().names()
        cols_to_check = [c for c in available_cols if c in target_cols or c in ohlc_cols]

        for col in cols_to_check:
            exprs.append(pl.col(col).null_count().alias(f"nulls_{col}"))

        # Eksekusi Query
        # BUG FIX: Handle empty dataframe crash
        df_stats = data.select(exprs).collect()
        
        if df_stats.height == 0:
            return {"row_count": 0}

        return df_stats.row(0, named=True)

    def _analyze_stats(self, stats: Dict[str, Any], rules: ValidationRules) -> 'Result[None, str]':
        """Memeriksa apakah statistik memenuhi ambang batas (Threshold)."""
        row_count = stats.get("row_count", 0)

        # 1. Min Rows
        if row_count < rules.min_rows:
            return Err(f"Insufficient Data: Got {row_count} rows (Required min: {rules.min_rows})")

        # 2. Null Checks
        # Total Null Ratio secara kasar (rata-rata null di kolom penting)
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
        """Check timestamp order."""
        try:
            is_sorted = data.select(pl.col("timestamp").is_sorted()).collect().item()
            if not is_sorted:
                return Err("Integrity Error: Timestamp is not sorted ascending")
            return Ok(None)
        except Exception as e:
            return Err(f"Sorting check error: {e}")

    def _validate_price_consistency(self, data: pl.LazyFrame) -> 'Result[None, str]':
        """Check logical constraints: High >= Low, Price > 0."""
        # Pastikan kolom ada dulu
        cols = data.collect_schema().names()
        if not {"high", "low", "close"}.issubset(set(cols)):
            return Ok(None) # Skip jika bukan OHLC data

        # Cek High < Low (Mustahil dalam candle valid)
        # Kita count berapa baris yang melanggar
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
    """Factory to create validator safely."""
    try:
        # Konversi dict ke ValidationRules object via method factory yang aman
        rules_obj = ValidationRules.from_dict(rules) if rules else None
        
        validator = PolarsValidator(default_rules=rules_obj)
        return Ok(validator)
    except Exception as e:
        return Err(f"Validator Creation Failed: {e}")

def get_default_validator() -> 'Result[PolarsValidator, str]':
    return create_validator()

# ====================== EXPORTS ======================
__all__ = ["PolarsValidator", "create_validator", "get_default_validator"]

# ====================== SELF CHECK ======================
if __name__ == "__main__":
    # Quick sanity check logic
    logging.basicConfig(level=logging.DEBUG)
    res = get_default_validator()
    if res.is_ok():
        print("✅ Validator Module Loaded & Factory Verified")
    else:
        print(f"❌ Module Broken: {res.error}")
