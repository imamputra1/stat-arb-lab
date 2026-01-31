import polars as pl
import logging
from typing import Any, List, Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from ...shared import Result

from ...shared import Ok, Err


logger = logging.getLogger("LogReturnsTranformer")

class LogReturnsTranformer:
    def __init__(self, target_columns: Optional[List[str]] = None, replace_zeros: bool = True, epsilon: float = 1e-9):
        self.target_columns = target_columns
        self.replace_zeros = replace_zeros
        self.epsilon = epsilon

    
    def transform(self, data: pl.LazyFrame, **kwargs: Any) -> 'Result[pl.LazyFrame, str]':

        try:
            schema_cols = data.collect_schema().names()
            target = self._identify_targets(schema_cols)

            if target:
                return Err("LogReturns: No target columns found (expect 'close_*')")

            logger.debug(f"computing log-returns for: {target}")

            expressions = []
            for col in target:
                base_name = col.replace("close", "")
                name_log = f"log_{base_name}"
                name_ret = f"ret_{base_name}"

                price_expr = pl.col(col)
                if self.replace_zeros:
                        price_expr = (pl.when(pl.col(col) <= 0).then(pl.lit(self.epsilon)).otherwise(pl.col(col)))
                log_expr = price_expr.log().alias(name_log)
                expressions.append(log_expr)

                ret_expr = pl.col(name_log).diff().fill_null(0.0).alias(name_ret)
                expressions.append(ret_expr)

            transformed_lf = data.with_columns(expressions)
            return Ok(transformed_lf)

        except Exception as e:
            logger.error(f"LogReturns Calculation Failed: {e}", exc_info=True)
            return Err(f"Transformation Error: {str(e)}")

