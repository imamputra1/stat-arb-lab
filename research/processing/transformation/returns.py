"""
ATOMIC TRANSFORMATION MODULE
Focus: Converting raw nominal prices into mathematical properties (Log-Space & Returns).
"""
import logging
import polars as pl
from typing import List, Any, Optional

# Type-safe imports
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from ...shared import Result

# Shared Imports
from ...shared import Ok, Err

logger = logging.getLogger("LogReturnsTransformer")

class LogReturnsTransformer:
    """
    Tier 1 Transformer: Efficiently converts 'close_X' to 'log_X' and 'ret_X'.
    
    Mathematical Logic:
    1. Log-Price: y_t = ln(P_t)
    2. Log-Return: r_t = y_t - y_{t-1} = ln(P_t / P_{t-1})
    
    Optimization:
    - Pure LazyFrame operations (No Eager Collection).
    - Single pass expression building.
    - Handles Log(0) and First-Row Nulls strictly.
    """

    def __init__(
        self, 
        target_columns: Optional[List[str]] = None,
        replace_zeros: bool = True,
        epsilon: float = 1e-9
    ):
        """
        Args:
            target_columns: List of price columns. If None, auto-detects 'close_*'.
            replace_zeros: If True, clips values <= 0 to epsilon to prevent -inf.
            epsilon: Small positive float to substitute for zero prices.
        """
        self.target_columns = target_columns
        self.replace_zeros = replace_zeros
        self.epsilon = epsilon

    def transform(
        self, 
        data: pl.LazyFrame, 
        **kwargs: Any
    ) -> 'Result[pl.LazyFrame, str]':
        """
        Executes the Log-Space transformation.
        """
        try:
            # 1. Schema Inspection (Metadata only, Cheap)
            schema_cols = data.collect_schema().names()
            
            # 2. Identify Targets
            targets = self._identify_targets(schema_cols)
            if not targets:
                return Err("LogReturns: No target columns found (expected 'close_*')")

            logger.debug(f"Computing Log-Returns for: {targets}")

            # 3. Build Expressions
            expressions = []
            for col in targets:
                # Naming: close_BTC -> log_BTC, ret_BTC
                base_name = col.replace("close_", "")
                name_log = f"log_{base_name}"
                name_ret = f"ret_{base_name}"

                # A. Safe Price Expression
                # Handle price <= 0 logic lazily
                price_expr = pl.col(col)
                if self.replace_zeros:
                    price_expr = (
                        pl.when(pl.col(col) <= 0)
                        .then(pl.lit(self.epsilon))
                        .otherwise(pl.col(col))
                    )

                # B. Log Price Expression
                # y_t = ln(P_t)
                log_expr = price_expr.log().alias(name_log)
                expressions.append(log_expr)

                # C. Log Return Expression
                # r_t = diff(y_t). Fill null at index 0 with 0.0 to maintain continuity.
                ret_expr = pl.col(name_log).diff().fill_null(0.0).alias(name_ret)
                expressions.append(ret_expr)

            # 4. Execute (Lazy)
            transformed_lf = data.with_columns(expressions)
            
            return Ok(transformed_lf)

        except Exception as e:
            logger.error(f"LogReturns Calculation Failed: {e}", exc_info=True)
            return Err(f"Transformation Error: {str(e)}")

    def _identify_targets(self, available_cols: List[str]) -> List[str]:
        """Resolve target columns against available schema."""
        if self.target_columns:
            # Validate user provided columns exist
            missing = [c for c in self.target_columns if c not in available_cols]
            if missing:
                logger.warning(f"Requested columns not found in data: {missing}")
            return [c for c in self.target_columns if c in available_cols]
        
        # Auto-detect defaults
        return [c for c in available_cols if c.startswith("close_")]

# ====================== FACTORY ======================

def create_log_returns_transformer(
    target_columns: Optional[List[str]] = None,
    **kwargs: Any
) -> LogReturnsTransformer:
    """Factory for LogReturnsTransformer."""
    return LogReturnsTransformer(target_columns=target_columns, **kwargs)

# ====================== EXPORTS ======================
__all__ = ["LogReturnsTransformer", "create_log_returns_transformer"]
