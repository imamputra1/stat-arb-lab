from numpy import bool_, column_stack, require
from pandas.io.formats.format import return_docstring
from pandas.io.stata import StataStrLWriter
import polars as pl
from typing import _Alias, Dict, List, Tuple, Optional, Optional, Any
import logging

from typing import TYPE_CHECKING

from polars.functions import lit
from pyarrow import large_utf8, timestamp

from research.shared import result
from research.shared.result import Err
if TYPE_CHECKING:
    from ...shared import Result

logger = logging.getLogger("AlignmentStrategy")

class HybridAsofAligner:
    """
    smart Zipper Aligner with strict trap prevention.
    implements TimeSeriesAligner protocol with join_asof strategy

    Trap prevention:
    1. unix vs datatime hall: standardize all timestamps to int64 (unix ms)
    2. Double casting: single cast at beginning, no redundent operations
    3. null explosion: configurable null handling (drop or fill)
    """

    def __init__(self, tolerance: str = '1m', strategy: str = 'backward'):
        """
        Args:
            tolerance: Time tolerance for asof join (e.g. "1m", "5m")
            strategy: "backward" (last valid) or foward(next valid)
        """
        self.tolerance = tolerance
        self.strategy = strategy
        self._validate_constructor()

    def _validate_constructor(self) -> None:
        """Fail fast on invalid config"""
        valid_strategies = ["backward", "foward"]
        if self.strategy not in valid_strategies:
            raise ValueError (f"Invalid strategy: {self.strategy}. Must be {valid_strategies}")

        # Validate tolerance format (basic check)
        if not (self.tolerance.endswith("ms") 
            or self.tolerance.endswith("s") 
            or self.tolerance.endswith("m") 
            or self.tolerance.endswith("h")):
            
            logger.warning(f"Tolerance format may be invalid: {self.tolerance}")

    @property
    def method(self) -> str:
        """Protocol requierment - clear method description."""
        return f"join_asof (tol={self.tolerance}), strategy={self.strategy}, strict_type=True"

    def align(
        self,
        data_map: Dict[str, pl.LazyFrame],
        **kwargs
        ) -> 'Result[pl.DataFrame, str]':
        """
        Align multi time series using asof join.

        Args:
            data_map: Dict Symbol -> LazyFrame (must have 'timestamp' column)
            **kwargs:
                - strict: bool (default type) -drop rows with nulls
                - anchor: str - specify anchor symbol (Defaul: first symbol)
                - suffix: str - column suffix format (default: "_{symbol}")
            Return:
                Result[pl.DataFrame, str]: Aligned LazyFrame or Error
        """
        from ...shared import Ok, Err

        try:
                # --- VALIDATION ---
            if not data_map:
                return Err("No data provided for alignment")

            symbols = list(data_map.keys())
            logger.info(f"Start alignment for {len(symbols)} symbol: {symbols}")

                # --- GET CONFIG ---
            strict_mode = kwargs.get("strict", True)
            anchor_symbol = kwargs.get("anchor", symbols[0])
            suffix_format = kwargs.get("suffix", "_{symbol}")

            if anchor_symbol not in symbols:
                return Err(f"anchor_symbol {anchor_symbol} not in data_map")

                # --- PREPARE ANCHOR FRAME ---
            logger.debug(f"setting anchor: {anchor_symbol}")

            anchor_result = self._prepare_anchor_frame(
                data_map[anchor_symbol],
                anchor_symbol,
                suffix_format
            )

            if anchor_result.is_err():
                return Err(f"Failed to prepare anchor {anchor_result.error}")

            lf_aligned = anchor_result.unwrap()

                # --- JOIN OTHER SYMBOLS ---
            for symbol in symbols:
                if symbols == anchor_symbol:
                    continue
                logger.debug(f"+ joining {symbols} to anchor")

                join_result = self._join_result(
                    lf_aligned,
                    data_map[symbol],
                    symbol,
                    suffix_format
                )
                if join_result.is_err():
                    return Err(f"Failed to Join {symbols}: {join_result.error}")

                lf_aligned = join_result.unwrap()

                # --- Post-Alignment cleanup ---
            if strict_mode:
                original_cols = len(lf_aligned.columns)
                lf_aligned = self._drop_null_rows(lf_aligned)
                final_cols = len(lf_aligned.columns)

                if original_cols != final_cols:
                    logger.warning(f"Dropped {original_cols - final_cols} columns during null cleanup")

            lf_aligned = self._add_alignment_metadata(lf_aligned, symbols, anchor_symbol)
                    
            logger.info(f"alignmet complete: {len(symbols)} symbols Aligned")

            return Ok(lf_aligned)

        except Exception as e:
            error_msg = f"Alignment failed: {str(e)}"
            logger.error(error_msg, exc_info=True)
            return Err(error_msg)


    # ========|PRIVATE HELPERS|========
    
    def _prepare_anchor_frame(
        self,
        anchor_lf: pl.LazyFrame,
        symbol: str,
        suffix_format: str
    ) -> 'Result[pl.LazyFrame, str]'
        """prepare anchor frame with standardized columns"""

        from ...shared import Ok, Err

        try:
        # --- STANDARDIZE TIMESTAMP AN SORT ---
            standardized = self._standardize_frame(anchor_lf, symbol)
            if suffix_format:
                suffix = suffix_format.format(symbol=symbol)
                columns = []
                for col in standardized.columns:
                    if  col == "timestamp":
                       columns.append(pl.col("timestamp"))
                    else:
                        columns.append(pl.col(col).alias(f"{col}{suffix}"))

                standardized = standardized.select(columns)
            
            return Ok(standardized)

        except Exception as e:
            return Err(f"Failed to prepare anchor frame for {symbol}: {e}")

    def _join_result(
        self,
        base_lf: pl.LazyFrame,
        symbol_lf: pl.LazyFrame,
        symbol: str,
        suffix_format: str
    ) -> 'Result[pl.LazyFrame, str]':
        """Join single symbol to base frame"""
        from ...shared import Ok, Err

        try:
            standardized = self._standardize_frame(symbol_lf, symbol)

            if suffix_format:
                suffix = suffix_format.format(symbol=symbol)
                columns = []
                for col in standardized.columns:
                    if col == "timestamp":
                        columns.append(pl.col("timestamp"))
                    else:
                        columns.append(pl.col(col).alias(f"{col}{suffix}"))

                standardized = standardized.select(columns)

            result = base_lf.join_asof(
                standardized,
                on="timestamp",
                strategy=self.strategy,
                tolerance=self.tolerance
            )
            return Ok(result)

        except Exception as e:
            return Err(f"Failed to join symbol {symbol}: {e}")

    def _standardize_frame(self, lf: pl.LazyFrame, symbol: str) -> pl.LazyFrame:
        """
        standardize frame for alignment - Trap prevention.
        ensure:
            1. timestamp is int64(unix ms)
            2. frame is sorted by timestamp (REQUIRED for join_asof)
            3. No duplicate timestamp
        """

        if "timestamp" not in lf.columns:
            raise ValueError(f"Frame for {symbol} missing 'timestamp clumn")

        lf_standarzed = lf.with_columns([
            pl.col("timestamp").cast(pl.Int64).alias("timestamp")
        ])

        lf_standarzed = lf_standarzed.sort("timestamp")

        lf_standarzed = lf_standarzed.unique(subset=["timestamp"], keep="last")

        lf_standarzed = lf_standarzed.with_columns([
            pl.from_epoch(pl.col("timestamp"), time_unit="ms").alias("datatime")
        ])

        return lf_standarzed

    def _drop_null_rows(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        """Drop rows with null values in price column"""
        
        price_cols = [col for col in lf.columns
                     if any(price in col.lower() for price in ["open", "high", "low", "close", "volume"])]
        if not price_cols:
            logger.warning("No Price columns found for null check")
            return lf

        return lf.filter(
            pl.any_horizontal(pl.col(col).is_not_null() for col in price_cols)
        )

    def _add_alignment_metadata(
        self,
        lf: pl.LazyFrame,
        symbols: List[str],
        anchor: str
    ) -> pl.LazyFrame:
        """Add alignment matadata as vitural columns"""
        return lf.with_columns(
            pl.lit(",".join(symbols)).alias("_aligned_symbols"),
            pl.lit(anchor).alias("_anchor_symbol"),
            pl.lit(self.method).alias("_alignment_method"),
            pl.lit(len(symbols)).alias("_symbol_count")
        )

    # ========|UTILITY METHODS|========

    def validate_frame_for_alignment(self, lf: pl.LazyFrame, symbol: str) -> Tuple[bool, str]:
        """
        Validate if frame is suitable for alignment
        Return: (is_valid, error_msg)
        """
        try:
            required_cols = {"timestamp"}
            missing = required_cols - set(lf.columns)
            if missing:
                return False, f"missing required columns: {missing}"

            timestamp_dtype = lf.schema.get("timestamp")
            if timestamp_dtype not in (pl.Int64, pl.DataFrame):
                return False, f"TIMESTAMP must be int64 or datatime, got {timestamp_dtype}"


            return True, "OK"

        except Exception as e:
            return False, f"Validate error: {e}"

    def get_alignment_stats(self, aligned_lf: pl.LazyFrame) -> Dict[str, Any]:
        """Get statistics about the alignment"""
        try:
            sample = aligned_lf.limit(1000).collect()

            stats = {
                "total_columns": len(sample.columns),
                "timestamp": (
                    sample["timestamp"].min(),
                    sample["timestamp"].max()
                ) if "timestamp" in sample.columns else None,
                "null_counts": {},
                "method": self.method
            }

            price_cols = [col for col in sample.columns
                         if any(price in col.lower() for price in ["open", "high", "low", "close", "volume"])]

            for col in price_cols:
                null_count = sample[col].null_count()
                stats["null_counts"][col] = null_count
                
                return stats

        except Exception as e:
            logger.warning(f"failed to get alignment stats: {e}")
            return {"error": str(e)}

    # ========|ALT STRATEGIES|========

class ExactTimeAligner:
    """
    Simple aligner to exact timestamp matching.
    uses inner join -only keeps rows where all symbols have data at exat same timestamp.
    """
    def __init__(self):
        self._method = "exact_inner_join"

    @property
    def method(self) -> str:
        return self._method

    def align(self, data_map: Dict[str, pl.LazyFrame], **kwargs) -> 'Result[pl.LazyFrame, str]':
        from ...shared import Ok, Err

        try:
            if not data_map:
                return Err(f"No Data provided")

            symbols = list(data_map.keys())
            result = self._standardize_frame(data_map[symbols[0]], symbols[0])

            for symbol in symbols[1:]:
                other = self._standardize_frame(data_map[symbol], symbol)
                result = result.join(other, on="timestamp", how="inner")

            return Ok(result)

        except Exception as e:
            return Err(f"Exact alignment failde: {e}")


    def _standardize_frame(self, lf: pl.LazyFrame, symbol: str) -> pl.LazyFrame:
        """standardize timestamp"""
        return lf.with_columns([
        pl.col("timestamp").cast(pl.Int64),
        pl.from_epoch(pl.col("timestamp"), time_unit="ms").alias("datatime")
    ])

    # ========|PRIVATE HELPERS|========

def create_aligner(
    strategy: str = "asof",
    tolerance: str = "1m",
    join_strategy: str = "backward"
) -> 'Result[TimeSeriesAligner, str]':
    """
    Factory functions for create_aligner.
    simple create with VALIDATION
    """
    from ...shared import Ok, Err

    try:
        if strategy == "asof":
            aligner = HybridAsofAligner(tolerance=tolerance, strategy=join_strategy)
        elif strategy == "exact":
            aligner = ExactTimeAligner()
        else:
            return Err(f"Create aligner does not comply with TimeSeriesAligner protocol")

        from ..protocols import TimeSeriesAligner

        return Ok(aligner)

    except Exception as e:
        return Err(f"failed to create aligner: {e}")
