import polars as pl
from typing import Dict, List, Any
import logging

from ...shared import Result, Ok, Err
logger = logging.getLogger("AlignmentStrategy")

class HybridAsofAligner:
    """
    Smart Zipper Aligner with Strict Trap Prevention.
    Implements TimeSeriesAligner protocol using Polars 'join_asof'.
    
    Trap Prevention:
    1. Unix vs Datetime: We convert EVERYTHING to pl.Datetime("ms").
       Why? Because Polars join_asof requires Datetime dtype to use string tolerances like "1m".
    2. Double Casting: Single cast at beginning.
    3. Null Explosion: Configurable null handling (drop or fill).
    """

    def __init__(self, tolerance: str = '1m', strategy: str = 'backward'):
        self.tolerance = tolerance
        self.strategy = strategy
        self._validate_constructor()

    def _validate_constructor(self) -> None:
        valid_strategies = ["backward", "forward"]
        if self.strategy not in valid_strategies:
            raise ValueError(f"Invalid strategy: {self.strategy}. Must be {valid_strategies}")

        valid_suffixes = ("ms", "s", "m", "h", "d")
        if not self.tolerance.endswith(valid_suffixes):
            logger.warning(f"Tolerance format '{self.tolerance}' might be invalid for Polars.")

    @property
    def method(self) -> str:
        return f"HybridAsof(tol={self.tolerance}, strat={self.strategy})"

    def align(
        self,
        data_map: Dict[str, pl.LazyFrame],
        **kwargs: Any
    ) -> Result[pl.LazyFrame, str]:
        try:
            # --- 1. VALIDATION ---
            if not data_map:
                return Err("No data provided for alignment")

            symbols = list(data_map.keys())
            logger.info(f"Start alignment for {len(symbols)} symbols: {symbols}")

            # --- 2. CONFIGURATION ---
            strict_mode = kwargs.get("strict", True)
            anchor_symbol = kwargs.get("anchor", symbols[0])
            suffix_format = kwargs.get("suffix", "_{symbol}")

            if anchor_symbol not in data_map:
                return Err(f"Anchor symbol '{anchor_symbol}' not found in data map")

            # --- 3. PREPARE ANCHOR (BASE) ---
            logger.info(f"Setting Anchor: {anchor_symbol}")
            
            anchor_res = self._prepare_frame(
                data_map[anchor_symbol], 
                anchor_symbol, 
                suffix_format
            )
            
            if anchor_res.is_err():
                return Err(f"Anchor prep failed: {anchor_res.error}")
                
            lf_aligned = anchor_res.unwrap()

            # --- 4. JOIN FOLLOWERS ---
            for sym in symbols:
                if sym == anchor_symbol:
                    continue
                
                follower_res = self._prepare_frame(data_map[sym], sym, suffix_format)
                if follower_res.is_err():
                    logger.warning(f"Skipping {sym}: {follower_res.error}")
                    continue
                
                lf_follower = follower_res.unwrap()

                # Execute JOIN_ASOF (The Magic)
                # Requirement: Join Key MUST be sorted AND Datetime type (for string tolerance)
                lf_aligned = lf_aligned.join_asof(
                    lf_follower,
                    on="timestamp",
                    strategy=self.strategy,
                    tolerance=self.tolerance
                )

            # --- 5. CLEANUP ---
            if strict_mode:
                lf_aligned = lf_aligned.drop_nulls()

            lf_aligned = self._add_metadata(lf_aligned, symbols, anchor_symbol)
            
            return Ok(lf_aligned)

        except Exception as e:
            msg = f"Alignment Logic Crashed: {str(e)}"
            logger.error(msg, exc_info=True)
            return Err(msg)

    # ========| PRIVATE HELPERS |========

    def _prepare_frame(
        self, 
        lf: pl.LazyFrame, 
        symbol: str, 
        suffix_fmt: str
    ) -> Result[pl.LazyFrame, str]:
        """Standardize Timestamp & Rename Columns."""
        try:
            # 1. Standardize Timestamp (Datetime & Sorted)
            lf = self._standardize_timestamp(lf, symbol)
            
            # 2. Rename Columns (Suffixing)
            suffix = suffix_fmt.format(symbol=symbol)
            
            # Rename all except timestamp
            cols_to_rename = [c for c in lf.collect_schema().names() if c != "timestamp"]
            lf = lf.rename({c: f"{c}{suffix}" for c in cols_to_rename})
            
            return Ok(lf)
        except Exception as e:
            return Err(f"Frame prep error for {symbol}: {e}")

    def _standardize_timestamp(self, lf: pl.LazyFrame, symbol: str) -> pl.LazyFrame:
        """
        Trap Prevention Core:
        1. Cast 'timestamp' to Datetime[ms]. (FIXED from Int64)
        2. Sort by 'timestamp'.
        3. Deduplicate.
        """
        if "timestamp" not in lf.collect_schema().names():
             raise ValueError(f"Symbol {symbol} missing 'timestamp' column")

        return (
            lf
            .with_columns([
                # CRITICAL FIX: Cast Int64 -> Datetime[ms] agar kompatibel dengan tolerance="1m"
                pl.col("timestamp").cast(pl.Int64).cast(pl.Datetime("ms")).alias("timestamp")
            ])
            .sort("timestamp")
            .unique(subset=["timestamp"], keep="last")
        )

    def _add_metadata(self, lf: pl.LazyFrame, symbols: List[str], anchor: str) -> pl.LazyFrame:
        return lf.with_columns([
            pl.lit(",".join(symbols)).alias("_meta_symbols"),
            pl.lit(anchor).alias("_meta_anchor"),
            pl.lit(self.tolerance).alias("_meta_tolerance")
        ])

class ExactTimeAligner:
    """Simple aligner for exact timestamp matching (Inner Join)."""
    def __init__(self) -> None:
        self._method = "Exact(InnerJoin)"

    @property
    def method(self) -> str:
        return self._method

    def align(self, data_map: Dict[str, pl.LazyFrame], **kwargs: Any) -> Result[pl.LazyFrame, str]:
        try:
            if not data_map:
                return Err("No data provided")

            symbols = list(data_map.keys())
            anchor_sym = symbols[0]
            lf_result = self._standardize(data_map[anchor_sym], anchor_sym)

            for sym in symbols[1:]:
                lf_other = self._standardize(data_map[sym], sym)
                lf_result = lf_result.join(lf_other, on="timestamp", how="inner")

            return Ok(lf_result)

        except Exception as e:
            return Err(f"Exact alignment failed: {e}")

    def _standardize(self, lf: pl.LazyFrame, symbol: str) -> pl.LazyFrame:
        suffix = f"_{symbol}"
        cols = [c for c in lf.collect_schema().names() if c != "timestamp"]
        rename_map = {c: f"{c}{suffix}" for c in cols}
        
        return (
            lf
            .with_columns(pl.col("timestamp").cast(pl.Int64).cast(pl.Datetime("ms")))
            .sort("timestamp")
            .unique(subset=["timestamp"])
            .rename(rename_map)
        )

# ========| FACTORY FUNCTION |========

def create_aligner(
    strategy: str = "asof",
    tolerance: str = "1m",
    join_strategy: str = "backward"
) -> Result['TimeSeriesAligner', str]:
    try:
        if strategy == "asof":
            aligner = HybridAsofAligner(tolerance=tolerance, strategy=join_strategy)
            return Ok(aligner)
        elif strategy == "exact":
            aligner = ExactTimeAligner()
            return Ok(aligner)
        else:
            return Err(f"Unknown strategy: {strategy}")
    except Exception as e:
        return Err(f"Factory failed to create aligner: {e}")
