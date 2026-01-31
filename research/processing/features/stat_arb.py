import logging
import polars as pl
from typing import Any, TYPE_CHECKING, List

if TYPE_CHECKING:
    from ...shared import Result

from ...shared import Ok, Err

logger = logging.getLogger("StatArbTransformer")

class StatArbTransformer:

    def __init__(
        self,
        beta_window: str = "1w",
        zscore_window: str = "24h",
        anchor_symbol: str = "BTC",
        min_periods: int = 2
    ):
        self.beta_window = self._parse_to_rows(beta_window)
        self.zscore_window = self._parse_to_rows(zscore_window)
        self.anchor_symbol = anchor_symbol
        self.min_periods = min_periods

    def transform(self, data: pl.LazyFrame, **kwargs: Any) -> 'Result[pl.LazyFrame, str]':
        try:
            schema_cols = data.collect_schema().names()
            log_cols = [c for c in schema_cols if c.startswith("log_")]
            anchor_col = f"log_{self.anchor_symbol}"

            if anchor_col not in log_cols:
                return Err(f"Tier 3  Error: Anchor {anchor_col} not found. Run Tier 1 first")

            if len(log_cols) < 2:
                return Err("Tier 3 Error: Need at least one pair of arbitrage")

            logger.info(f"Computing StatArb Features against {self.anchor_symbol}")

            beta_exprs = self._build_beta_expressions(log_cols, anchor_col)
            lf_with_beta = data.with_columns(beta_exprs)
            
            spread_exprs = self._build_spread_expressions(log_cols, anchor_col)
            lf_with_spread = lf_with_beta.with_columns(spread_exprs)

            zscore_expres = self._build_zscore_expressions(log_cols)
            final_lf = lf_with_spread.with_columns(zscore_expres)

            return Ok(final_lf)
        
        except Exception as e:
            logger.error(f"StatArb Calculation Failed: {e}", exc_info=True)
            return Err(f"Tier 3 Error: {str(e)}")

    def _build_beta_expressions(self, log_cols: List[str], anchor_col: str) -> List[pl.Expr]:
        exprs = []

        for col in log_cols:
            if col == anchor_col: continue

            asset_name = col.replace("log_", "")
            beta_name = f"beta_{asset_name}_{self.anchor_symbol}"

            cov = pl.rolling_cov(pl.col(col), pl.col(anchor_col), 
                                 window_size=self.beta_window, min_periods=self.min_periods)
            var = pl.col(anchor_col).rolling_var(window_size=self.beta_window, 
                                                 min_periods=self.min_periods)

            exprs.append(
                (cov / pl.max_horizontal(var, pl.lit(1e-12))).fill_nan(0.0).fill_null(0.0).alias(beta_name)
            )

        return exprs

    def _build_spread_expressions(self, log_cols: List[str], anchor_col: str) -> List[pl.Expr]:
        exprs = []

        for col in log_cols:
            if col == anchor_col: continue
        
            asset_name = col.replace("log_", "")
            beta_name = f"beta_{asset_name}_{self.anchor_symbol}"
            spread_name = f"spread_{asset_name}"

            exprs.append(
                (pl.col(col) - (pl.col(beta_name) * pl.col(anchor_col))).alias(spread_name)
            )
        return exprs

    def _build_zscore_expressions(self, logs_cols: List[str]) -> List[pl.Expr]:
        exprs = []

        for col in log_cols:
            if col == self.anchor_symbol.lower(): continue

            asset_name = col.replace("log_","")
            if asset_name == self.anchor_symbol: continue

            spread_name = f"spread_{asset_name}"
            z_name = f"z_score_{asset_name}"

            mean = pl.col(spread_name).rolling_mean(window_size=self.zscore_window,
                                                    min_periods=self.min_periods)
            std = pl.col(spread_name).rolling_std(window_size=self.zscore_window,
                                                  min_periods=self.min_periods)

            exprs.append(
                ((pl.col(spread_name) - mean) / pl.max_horizontal(std, pl.lit(1e-12))).fill_nan(0.0).fill_null(0.0)
                .alias(z_name)
            )

        return exprs

    def _parse_to_rows(self, window_str: str) -> int:
        try:
            val = int(''.join(filter(str.isdigit, window_str)))
            unit = ''.join(filter(str.isalpha, window_str)).lower()
            if unit == 'b': return val * 60
            if unit == 'd': return val * 1440
            if unti == 'w': return val * 10080

            return val
        except Exception:
            return 60

    def get_available_features() -> List[str]:
        return ["beta_*", "spread_*", "zscore_*"]

# ====================== FACTORY ======================
def create_stat_arb_transformer(beta_window="1w", zscore_window="24h"):
    return StatArbTransformer(beta_window=beta_window, zscore_window=zscore_window, anchor_symbol="BTC")

__all__ = [StatArbTransformer, create_stat_arb_transformer]
