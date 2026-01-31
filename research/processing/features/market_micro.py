"""
MICROSTRUCTURE FEATURES MODULE (TIER 2)
Focus: Market Risk Sensors (Rolling Volatility & Correlation).
Location: research/processing/features/market_micro.py
Compatibility: Polars 1.x+
"""
import logging
import polars as pl
from typing import List, Any, Optional

# Type-safe imports
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from ...shared import Result

from ...shared import Ok, Err

logger = logging.getLogger("MicrostructureTransformer")

class MicrostructureTransformer:
    """
    Tier 2 Transformer: Computes Rolling Volatility and Correlation.
    
    Logic:
    1. Volatility: Uses .rolling(index_column=...).std()
    2. Correlation: Uses .rolling(index_column=...).corr(other)
    
    Polars 1.x Compliance:
    - Removes 'by' argument (replaced by index_column).
    - Removes 'rolling_std' (replaced by expression chaining).
    - Enforces Datetime type for timestamp column.
    """

    def __init__(
        self,
        windows: Optional[List[str]] = None,
        anchor_symbol: str = "BTC",
        closed: str = "right"
    ):
        """
        Args:
            windows: Time windows (e.g., ["1h", "4h", "24h"]).
            anchor_symbol: Reference asset for correlation (default: BTC).
            closed: Window boundary ('left', 'right', 'both', 'none').
        """
        self.windows = windows or ["1h", "4h", "24h"]
        self.anchor_symbol = anchor_symbol
        self.closed = closed
        
        valid_closed = ["left", "right", "both", "none"]
        if closed not in valid_closed:
            raise ValueError(f"closed must be one of {valid_closed}, got {closed}")

    def transform(
        self, 
        data: pl.LazyFrame, 
        **kwargs: Any
    ) -> 'Result[pl.LazyFrame, str]':
        """
        Executes Tier 2 Transformation.
        """
        try:
            # 1. Schema Inspection
            schema = data.collect_schema()
            schema_cols = schema.names()
            
            # 2. Validate prerequisites
            if "timestamp" not in schema_cols:
                return Err("Missing required 'timestamp' column for rolling calculations")
            
            # 3. POLARS 1.x CRITICAL: Ensure Timestamp is Datetime
            # Dynamic rolling (period="1h") requires Datetime, not Int64.
            ts_type = schema["timestamp"]
            # Cek jika tipe datanya bukan Datetime (misal Int64)
            if not isinstance(ts_type, pl.Datetime):
                logger.debug(f"Casting timestamp from {ts_type} to Datetime for rolling ops")
                # Asumsi input Int64 adalah Unix Milliseconds (standar CCXT/Crypto)
                data = data.with_columns(
                    pl.col("timestamp").cast(pl.Datetime("ms")).alias("timestamp")
                )
            
            # 4. Find return columns
            ret_cols = [c for c in schema_cols if c.startswith("ret_")]
            if not ret_cols:
                return Err("Tier 2 Error: No 'ret_*' columns found. Run Tier 1 (Returns) first.")

            # 5. Check Anchor
            anchor_col = f"ret_{self.anchor_symbol}"
            has_anchor = anchor_col in ret_cols
            
            if not has_anchor:
                logger.warning(
                    f"Anchor '{self.anchor_symbol}' not found in returns. "
                    "Correlation features will be skipped."
                )

            logger.debug(f"Computing Microstructure Features. Windows: {self.windows}")

            # 6. Build Expressions
            expressions = self._build_expressions(ret_cols, anchor_col, has_anchor)

            # 7. Execute (Lazy)
            transformed_lf = data.with_columns(expressions)
            
            return Ok(transformed_lf)

        except Exception as e:
            logger.error(f"Microstructure Calculation Failed: {e}", exc_info=True)
            return Err(f"Tier 2 Error: {str(e)}")

    def _build_expressions(
        self, 
        ret_cols: List[str], 
        anchor_col: str, 
        has_anchor: bool
    ) -> List[pl.Expr]:
        """Build expressions using Polars 1.x Syntax."""
        expressions = []
        
        for window in self.windows:
            # A. ROLLING VOLATILITY
            for ret_col in ret_cols:
                asset_name = ret_col.replace("ret_", "")
                feat_name = f"vol_{asset_name}_{window}"
                
                # SYNTAX 1.x: .rolling(...).std()
                vol_expr = (
                    pl.col(ret_col)
                    .rolling(
                        period=window, 
                        index_column="timestamp", 
                        closed=self.closed
                    )
                    .std()
                    .fill_nan(0.0)
                    .fill_null(0.0)
                    .alias(feat_name)
                )
                expressions.append(vol_expr)
            
            # B. ROLLING CORRELATION
            if has_anchor:
                for ret_col in ret_cols:
                    if ret_col == anchor_col: continue 
                    
                    asset_name = ret_col.replace("ret_", "")
                    feat_name = f"corr_{asset_name}_{self.anchor_symbol}_{window}"
                    
                    # SYNTAX 1.x: .rolling(...).corr(other)
                    corr_expr = (
                        pl.col(ret_col)
                        .rolling(
                            period=window, 
                            index_column="timestamp", 
                            closed=self.closed
                        )
                        .corr(pl.col(anchor_col))
                        .fill_nan(0.0)
                        .fill_null(0.0)
                        .alias(feat_name)
                    )
                    expressions.append(corr_expr)
        
        return expressions

"""
MICROSTRUCTURE FEATURES MODULE (TIER 2)
Focus: Market Risk Sensors (Rolling Volatility & Correlation).
Location: research/processing/features/market_micro.py
Optimization: Row-based rolling (The 'KOTOR' way) for Polars 1.x stability.
"""
import logging
import polars as pl
from typing import List, Any, Optional

# Type-safe imports
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from ...shared import Result


logger = logging.getLogger("MicrostructureTransformer")

class MicrostructureTransformer:
    """
    Tier 2 Transformer: Computes Rolling Volatility and Correlation.
    
    ENGINEERING HACK:
    Uses row-count based rolling (window_size=int) instead of time-based.
    Assumes data is aligned to 1m intervals by the previous stage.
    """

    def __init__(
        self,
        windows: Optional[List[str]] = None,
        anchor_symbol: str = "BTC",
        min_periods: int = 1
    ):
        """
        Args:
            windows: Time windows (e.g., ["1h", "4h"]). 
            anchor_symbol: Reference asset for correlation.
            min_periods: Minimum observations for valid result.
        """
        self.windows = windows or ["1h", "4h", "24h"]
        self.anchor_symbol = anchor_symbol
        self.min_periods = min_periods

    def transform(
        self, 
        data: pl.LazyFrame, 
        **kwargs: Any
    ) -> 'Result[pl.LazyFrame, str]':
        try:
            # 1. Schema Inspection
            schema_cols = data.collect_schema().names()
            
            # 2. Find return columns (ret_*)
            ret_cols = [c for c in schema_cols if c.startswith("ret_")]
            if not ret_cols:
                return Err("Tier 2 Error: No 'ret_*' columns found.")

            # 3. Check Anchor
            anchor_col = f"ret_{self.anchor_symbol}"
            has_anchor = anchor_col in ret_cols
            
            if not has_anchor:
                logger.warning(f"Anchor '{self.anchor_symbol}' missing. Skipping correlation.")

            # 4. Build Expressions
            expressions = []
            
            for window_str in self.windows:
                # Convert "1h" -> 60, "1d" -> 1440
                n_rows = self._parse_window_to_rows(window_str)
                
                # A. ROLLING VOLATILITY
                for ret_col in ret_cols:
                    asset_name = ret_col.replace("ret_", "")
                    feat_name = f"vol_{asset_name}_{window_str}"
                    
                    # Row-based rolling std (Ultra Stable API)
                    vol_expr = (
                        pl.col(ret_col)
                        .rolling_std(window_size=n_rows, min_periods=self.min_periods)
                        .fill_nan(0.0)
                        .fill_null(0.0)
                        .alias(feat_name)
                    )
                    expressions.append(vol_expr)
                
                # B. ROLLING CORRELATION
                if has_anchor:
                    for ret_col in ret_cols:
                        if ret_col == anchor_col: continue 
                        
                        asset_name = ret_col.replace("ret_", "")
                        feat_name = f"corr_{asset_name}_{self.anchor_symbol}_{window_str}"
                        
                        # Row-based rolling corr (Top-level function for max stability)
                        corr_expr = (
                            pl.rolling_corr(
                                pl.col(ret_col),
                                pl.col(anchor_col),
                                window_size=n_rows,
                                min_periods=self.min_periods
                            )
                            .fill_nan(0.0)
                            .fill_null(0.0)
                            .alias(feat_name)
                        )
                        expressions.append(corr_expr)

            # 5. Execute
            return Ok(data.with_columns(expressions))

        except Exception as e:
            logger.error(f"Microstructure Calculation Failed: {e}", exc_info=True)
            return Err(f"Tier 2 Error: {str(e)}")

    def _parse_window_to_rows(self, window_str: str) -> int:
        """Dirty but effective: parses Polars duration to row count (1m base)."""
        try:
            val = int(''.join(filter(str.isdigit, window_str)))
            unit = ''.join(filter(str.isalpha, window_str)).lower()
            
            if unit == 'm' or not unit: return val
            if unit == 'h': return val * 60
            if unit == 'd': return val * 1440
            if unit == 'i': return val # raw index
            return val
        except:
            logger.warning(f"Failed to parse window '{window_str}', defaulting to 60 rows.")
            return 60

# ====================== FACTORY ======================
def create_microstructure_transformer(windows=None, anchor_symbol="BTC", **kwargs):
    return MicrostructureTransformer(windows=windows, anchor_symbol=anchor_symbol)

__all__ = ["MicrostructureTransformer", "create_microstructure_transformer"]
