import polars as pl
from typing import Dict, List, Any
import logging

# Import Result Pattern dari Shared
# Structure: research/processing/alignment/strategies.py -> research/shared
from ...shared import Result, Ok, Err

# Setup Logger (Standard CLI Format)
logger = logging.getLogger("AlignmentStrategy")

class HybridAsofAligner:
    """
    Smart Zipper Aligner with Strict Trap Prevention.
    Implements TimeSeriesAligner protocol using Polars 'join_asof'.
    
    Trap Prevention:
    1. Unix vs Datetime Hell: Standardize all timestamps to Int64 (Unix MS).
    2. Double Casting: Single cast at beginning.
    3. Null Explosion: Configurable null handling (drop or fill).
    """

    def __init__(self, tolerance: str = '1m', strategy: str = 'backward'):
        """
        Args:
            tolerance: Time tolerance for asof join (e.g., "1m", "5m").
            strategy: "backward" (last known valid) or "forward".
        """
        self.tolerance = tolerance
        self.strategy = strategy
        self._validate_constructor()

    def _validate_constructor(self) -> None:
        """Fail fast on invalid config."""
        valid_strategies = ["backward", "forward"]
        if self.strategy not in valid_strategies:
            # Menggunakan ValueError agar object gagal dibuat saat init
            raise ValueError(f"Invalid strategy: {self.strategy}. Must be {valid_strategies}")

        # Basic tolerance format check
        valid_suffixes = ("ms", "s", "m", "h", "d")
        if not self.tolerance.endswith(valid_suffixes):
            logger.warning(f"Tolerance format '{self.tolerance}' might be invalid for Polars.")

    @property
    def method(self) -> str:
        """Protocol requirement - clear method description."""
        return f"HybridAsof(tol={self.tolerance}, strat={self.strategy})"

    def align(
        self,
        data_map: Dict[str, pl.LazyFrame],
        **kwargs: Any
    ) -> Result[pl.LazyFrame, str]:
        """
        Align multi-timeseries using Asof Join (Hybrid).
        
        Args:
            data_map: Symbol -> LazyFrame (must have 'timestamp' column).
            **kwargs:
                - strict: bool (default True) - Drop rows with nulls (Complete Data only).
                - anchor: str - Specify anchor symbol (Default: First symbol).
                - suffix: str - Column suffix format (Default: "_{symbol}").
        """
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
            
            # Prepare Anchor
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
                
                # Prepare Follower
                follower_res = self._prepare_frame(data_map[sym], sym, suffix_format)
                if follower_res.is_err():
                    logger.warning(f"Skipping {sym}: {follower_res.error}")
                    continue
                
                lf_follower = follower_res.unwrap()

                # Execute JOIN_ASOF (The Magic)
                # Left Table = Aligned so far (Anchor)
                # Right Table = Follower
                lf_aligned = lf_aligned.join_asof(
                    lf_follower,
                    on="timestamp",
                    strategy=self.strategy,
                    tolerance=self.tolerance
                )

            # --- 5. CLEANUP ---
            if strict_mode:
                # Drop rows where ANY column is null (Strict Alignment)
                # dalam toleransi waktu 1m.
                lf_aligned = lf_aligned.drop_nulls()

            # Add Metadata Columns (Optional, for debugging)
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
            # 1. Standardize Timestamp (Int64 & Sorted)
            lf = self._standardize_timestamp(lf, symbol)
            
            # 2. Rename Columns (Suffixing)
            suffix = suffix_fmt.format(symbol=symbol)
            
            cols_to_rename = [c for c in lf.collect_schema().names() if c != "timestamp"]
            
            lf = lf.rename({c: f"{c}{suffix}" for c in cols_to_rename})
            
            return Ok(lf)
        except Exception as e:
            return Err(f"Frame prep error for {symbol}: {e}")

    def _standardize_timestamp(self, lf: pl.LazyFrame, symbol: str) -> pl.LazyFrame:
        """
        Trap Prevention Core:
        1. Cast 'timestamp' to Int64 (Unix MS).
        2. Sort by 'timestamp' (Required by join_asof).
        3. Deduplicate 'timestamp'.
        """
        # Cek kolom timestamp ada atau tidak (via Schema check ringan)
        if "timestamp" not in lf.collect_schema().names():
             raise ValueError(f"Symbol {symbol} missing 'timestamp' column")

        return (
            lf
            .with_columns([
                # Force Cast ke Int64 (Unix MS)
                pl.col("timestamp").cast(pl.Int64)
            ])
            # WAJIB SORTED untuk join_asof
            .sort("timestamp")
            .unique(subset=["timestamp"], keep="last")
            .with_columns([
                 pl.from_epoch(pl.col("timestamp"), time_unit="ms").alias("datetime")
            ])
        )

    def _add_metadata(self, lf: pl.LazyFrame, symbols: List[str], anchor: str) -> pl.LazyFrame:
        """Inject metadata lineage."""
        return lf.with_columns([
            pl.lit(",".join(symbols)).alias("_meta_symbols"),
            pl.lit(anchor).alias("_meta_anchor"),
            pl.lit(self.tolerance).alias("_meta_tolerance")
        ])

class ExactTimeAligner:
    """
    Simple aligner for exact timestamp matching (Inner Join).
    Zero tolerance. Good for Daily candles, bad for High-Freq (M1).
    """
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
            
            # Ambil Anchor
            anchor_sym = symbols[0]
            lf_result = self._standardize(data_map[anchor_sym], anchor_sym)

            # Inner Join loop
            for sym in symbols[1:]:
                lf_other = self._standardize(data_map[sym], sym)
                lf_result = lf_result.join(
                    lf_other, 
                    on="timestamp", 
                    how="inner" # Strict Match
                )

            return Ok(lf_result)

        except Exception as e:
            return Err(f"Exact alignment failed: {e}")

    def _standardize(self, lf: pl.LazyFrame, symbol: str) -> pl.LazyFrame:
        # Helper simple untuk Exact Aligner
        suffix = f"_{symbol}"
        cols = [c for c in lf.collect_schema().names() if c != "timestamp"]
        rename_map = {c: f"{c}{suffix}" for c in cols}
        
        return (
            lf
            .with_columns(pl.col("timestamp").cast(pl.Int64))
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
    """
    Factory to create aligner instance.
    """
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
