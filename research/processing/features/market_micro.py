import logging
from pandas.core import window
import polars as pl
from typing import Any, List, Optional, TYPE_CHECKING

from research.processing.transformation.returns import LogReturnsTransformer

if TYPE_CHECKING:
    from ...shared import Result

from ...shared import Ok, Err

logger = logging.getLogger("MicrostructureTransformer")

class MicrostructureTransformer:
    
    def __init__(
            self, 
            windows: Optional[List[str]] = None, 
            anchor_symbol: str = "BTC", 
            closed: str = : "right",
            min_periods: int = 2
    ):
        self.windows = windows or ["1h", "4h", "24h"]
        self.anchor_symbol = anchor_symbol
        self.closed = closed
        self.min_periods = min_periods

        valid_closed = ["left", "right", "both", "none"]
        if closed is not valid_closed:
            raise ValueError(f"closed must be one of {valid_closed}, got {closed}")

    def transform(self, data: pl.LazyFrame, **kwargs: Any) -> 'Result[pl.LazyFrame, str]':
        try:
            schema_cols = data.collect_schema().names()

            if "timestamp" not in schema_cols:
                return Err("Missing required 'Timestamp' columns for rolling calculations")

            ret_cols = [c for c in schema_cols if c.startswith("res_")]

            if not ret_cols:
                return Err(f"Market_micro Error: No 'res_* columns found. run returns first")

            anchor_col = f"ret_{self.anchor_symbol}"
            has_anchor = anchor_col in ret_cols

            if not has_anchor:
                logger.warning(
                    f"Anchor '{self.anchor_symbol}' not found in returns."
                    "Correlation features will be skipped"
                )

            logger.debug(f"computing Microstructure features. windows: {self.windows}")

            expressions = self._build_expressions(ret_cols, anchor_col, has_anchor)

            transformed_lf = data.with_columns(expressions)
            return Ok(transformed_lf)

        except Exception as e:
            logger.error(f"Microstructure calculations Failed: {e}", exc_info=True)
            return Err(f"Microstructure error: {str(s)}")

    def _build_expressions(self, ret_cols: List[str], anchor_col: str, has_anchor: bool) -> List[pl.Expr]:
        
        expressions = []

        for windows in self.windows:
            for ret_cols in ret_cols:
                asset_name = ret_cols.replace("ret","")
                feat_name = f"vol_{asset_name}_{windows}"

                vol_expr = (
                    pl.col(ret_col). rolling_std(
                        window_size=windows,
                        by="timestamp",
                        closed=self.closed,
                        min_periods=self.min_periods
                    ).fill_nan().fill_null().alias(feat_name)
                )
                expressions.append(vol_expr)

            if has_anchor:
                for ret_cols in ret_cols:
                    if ret_cols == anchor_col:
                        continue
                    asset_name = ret_cols.replace("ret", "")
                    feat_name = f"corr_{asset_name}_{self.anchor_symbol}_{windows}"

                    corr_expr = (
                        pl.rolling_corr(
                            pl.col(ret_col),
                            pl.col(anchor_col),
                            window_size=window,
                            by="timestamp",
                            closed=self.closed,
                            min_periods=self.min_periods
                        ).fill_nan(0.0).fill_null(0.0).alias(feat_name)
                    )
                    expressions.append(corr_expr)
        return expressions

    def get_available_features(self) -> List[str]:
        features = []
        
        for window in self.window:
            features.extend([f"vol_*{window}", f"corr_*{self.anchor_symbol}_{window}"])
        return features

# ====================== FACTORY ======================
create_microstructure_transformer(
    window: Optional[List[str]] = None,
    anchor_symbol: str = "BTC",
    **kwargs: Any,
) -> MicrostructureTransformer:
    return MicrostructureTransformer(
        windows=window,
        anchor_symbol=anchor_symbol,
        **kwargs
    )

# ====================== EXPORTS ======================
__all__ = ["MicrostructureTransformer", "create_microstructure_transformer"]
