"""
SILVER DATA LOADER (THE WORKER)
Location: research/strategy/data/loader.py
Focus: High-performance lazy loading with Result Pattern integration.
Optimization: Caching, efficient column selection, and Robust Date Parsing.
"""
import json
import logging
from pathlib import Path
from typing import Dict, Any, Optional, List, Set
import polars as pl
from functools import lru_cache

# Path correction: 3 dots for research/shared/ from research/strategy/data/
from ...shared import Ok, Err, Result

logger = logging.getLogger("SilverDataLoader")

class SilverDataLoader:
    """
    High-performance loader for Hive-partitioned Silver Lake data.
    Implements DataLoaderProtocol with optimized predicate pushdown.
    """

    def __init__(self, silver_path: str):
        self.path = Path(silver_path).resolve()
        self.metadata_path = self.path / "metadata.json"
        
        if not self.metadata_path.exists():
            raise FileNotFoundError(f"Critical Error: metadata.json missing at {self.path}")
        
        logger.info(f"SilverDataLoader initialized at: {self.path}")

    def load(
        self, 
        start_date: Optional[str] = None, 
        end_date: Optional[str] = None,
        symbols: Optional[List[str]] = None,
        **filters: Any
    ) -> Result[pl.LazyFrame, str]:
        """
        Loads data using Lazy Scanning with Hive Partition Pruning.
        """
        try:
            # 1. Scanning with Hive Partitioning enabled
            lf = pl.scan_parquet(self.path / "**/*.parquet", hive_partitioning=True)
            
            filter_exprs = []
            
            # Date range filtering (Robust Parsing Fix)
            if start_date or end_date:
                if start_date and end_date:
                    date_expr = pl.col("timestamp").is_between(
                        self._parse_to_datetime_expr(start_date),
                        self._parse_to_datetime_expr(end_date),
                        closed="both"
                    )
                elif start_date:
                    date_expr = pl.col("timestamp") >= self._parse_to_datetime_expr(start_date)
                else:
                    date_expr = pl.col("timestamp") <= self._parse_to_datetime_expr(end_date)
                filter_exprs.append(date_expr)
            
            # Hive partition pruning (year/month)
            for key in ["year", "month"]:
                val = filters.get(key)
                if val:
                    if isinstance(val, list):
                        filter_exprs.append(pl.col(key).is_in([str(v) for v in val]))
                    else:
                        filter_exprs.append(pl.col(key) == str(val))
            
            # Apply all filters for pushdown
            if filter_exprs:
                # Combine expressions using AND logic
                combined_expr = filter_exprs[0]
                for expr in filter_exprs[1:]:
                    combined_expr = combined_expr & expr
                lf = lf.filter(combined_expr)
            
            # 2. Wide Table Symbol Selection
            if symbols:
                selection_res = self._build_column_selection(symbols)
                if selection_res.is_err():
                    return Err(selection_res.error)
                lf = lf.select(selection_res.unwrap())
            
            return Ok(lf)
            
        except Exception as e:
            logger.error(f"Load Failure: {str(e)}", exc_info=True)
            return Err(f"DataLoader Error: {str(e)}")

    def _parse_to_datetime_expr(self, date_str: str) -> pl.Expr:
        """
        KOTOR bin SUPERIOR: Robust date parsing helper.
        Handles strict Polars casting by normalizing input strings.
        """
        # Jika input hanya "YYYY-MM-DD" (len 10), tambahkan jam nol
        # agar strict casting ke Datetime(ms) tidak error.
        clean_str = date_str.strip()
        if len(clean_str) == 10: 
            clean_str += " 00:00:00"
            
        # Gunakan str.to_datetime() yang lebih pintar dari cast() biasa
        return pl.lit(clean_str).str.to_datetime(time_unit="ms")

    def _build_column_selection(self, symbols: List[str]) -> Result[List[str], str]:
        """Filters columns based on requested symbols for Wide Table structure."""
        meta_res = self.get_metadata()
        if meta_res.is_err():
            return Err(f"Column selection failed: {meta_res.error}")
        
        all_columns = meta_res.unwrap().get("columns", [])
        
        # Core columns always included
        selected: Set[str] = {"timestamp", "year", "month"}
        
        # Node B naming convention: {prefix}_{SYMBOL} or {prefix}_{SYMBOL}_{window}
        for col in all_columns:
            for s in symbols:
                if f"_{s}" in col:
                    selected.add(col)
                    break
        
        return Ok(list(selected))

    @lru_cache(maxsize=1)
    def get_metadata(self) -> Result[Dict[str, Any], str]:
        """Loads and caches metadata.json registry records."""
        try:
            with open(self.metadata_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            return Ok(data)
        except Exception as e:
            return Err(f"Metadata Access Failure: {str(e)}")

    def get_available_partitions(self) -> Result[Dict[str, List[str]], str]:
        """Discovers available partitions via directory scanning."""
        try:
            years: Set[str] = set()
            months: Set[str] = set()
            
            # SUPERIOR: Glob directories instead of files for speed
            for year_dir in self.path.glob("year=*"):
                years.add(year_dir.name.split("=")[1])
                for month_dir in year_dir.glob("month=*"):
                    months.add(month_dir.name.split("=")[1])
            
            return Ok({
                "year": sorted(list(years)),
                "month": sorted(list(months))
            })
        except Exception as e:
            return Err(f"Partition discovery failed: {str(e)}")

    def estimate_size(
        self, 
        start_date: Optional[str] = None, 
        end_date: Optional[str] = None
    ) -> Result[Dict[str, Any], str]:
        """Estimates data footprint for resource planning."""
        try:
            meta_res = self.get_metadata()
            part_res = self.get_available_partitions()
            if meta_res.is_err(): return Err(meta_res.error)
            if part_res.is_err(): return Err(part_res.error)
            
            metadata = meta_res.unwrap()
            available_years = part_res.unwrap()["year"]
            
            # Determine years to scan
            y_start = start_date[:4] if start_date else min(available_years)
            y_end = end_date[:4] if end_date else max(available_years)
            
            scan_years = [y for y in available_years if y_start <= y <= y_end]
            
            # Proportional estimate based on year ratio
            total_rows = metadata.get("row_count", 0)
            ratio = len(scan_years) / len(available_years) if available_years else 1.0
            est_rows = int(total_rows * ratio)
            
            # Estimate MB: (rows * columns * 8 bytes/float64) / 1024^2
            col_count = len(metadata.get("columns", []))
            est_mb = (est_rows * col_count * 8) / (1024 * 1024)
            
            return Ok({
                "estimated_rows": est_rows,
                "estimated_mb": round(est_mb, 2),
                "partitions_to_scan": scan_years,
                "column_count": col_count
            })
        except Exception as e:
            return Err(f"Estimation failure: {str(e)}")

# --- FACTORY ---
def create_silver_loader(silver_path: str) -> SilverDataLoader:
    """Factory for SilverDataLoader."""
    return SilverDataLoader(silver_path)

# --- EXPORTS ---
__all__ = ["SilverDataLoader", "create_silver_loader"]
