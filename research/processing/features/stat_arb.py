"""
STATIONARITY ENGINE (TIER 3)
Focus: Statistical Arbitrage Metrics (Rolling Beta, Spread, Z-Score).
Location: research/processing/features/stat_arb.py
Architecture: Tiered Chaining (Beta -> Spread -> Z-Score)
"""
import logging
import polars as pl
from typing import List, Any

# Type-safe imports
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from ...shared import Result

from ...shared import Ok, Err

logger = logging.getLogger("StatArbTransformer")

class StatArbTransformer:
    """
    Tier 3 Transformer: Statistical Arbitrage Engine.
    
    Mathematical Sequence:
    1. Beta (Hedge Ratio): Cov(Target, Anchor) / Var(Anchor)
    2. Spread (Residual): Target - (Beta * Anchor)
    3. Z-Score (Signal): Normalized Spread deviation.
    
    Compliance:
    - Polars 1.x Stable.
    - Integer-based rolling (KOTOR strategy).
    - Pure Lazy execution (No .collect()).
    """

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

    def transform(
        self, 
        data: pl.LazyFrame, 
        **kwargs: Any
    ) -> 'Result[pl.LazyFrame, str]':
        try:
            # 1. Schema Inspection
            schema_cols = data.collect_schema().names()
            
            # 2. Prerequisites Check (Log Prices from Tier 1)
            log_cols = [c for c in schema_cols if c.startswith("log_")]
            anchor_col = f"log_{self.anchor_symbol}"
            
            if anchor_col not in log_cols:
                return Err(f"Tier 3 Error: Anchor '{anchor_col}' not found. Run Tier 1 first.")
            
            if len(log_cols) < 2:
                return Err("Tier 3 Error: Need at least one pair for arbitrage.")

            logger.info(f"Computing StatArb Features against {self.anchor_symbol}")

            # 3. Execution via Chaining (Beta -> Spread -> Z-Score)
            # Stage A: Calculate Beta
            beta_exprs = self._build_beta_expressions(log_cols, anchor_col)
            lf_with_beta = data.with_columns(beta_exprs)
            
            # Stage B: Calculate Spread (Depends on Beta)
            spread_exprs = self._build_spread_expressions(log_cols, anchor_col)
            lf_with_spread = lf_with_beta.with_columns(spread_exprs)
            
            # Stage C: Calculate Z-Score (Depends on Spread)
            zscore_exprs = self._build_zscore_expressions(log_cols)
            final_lf = lf_with_spread.with_columns(zscore_exprs)

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
            
            # Beta = Cov(X,Y) / Var(X)
            # Use max_horizontal to prevent division by zero
            cov = pl.rolling_cov(pl.col(col), pl.col(anchor_col), 
                                 window_size=self.beta_window, min_periods=self.min_periods)
            var = pl.col(anchor_col).rolling_var(window_size=self.beta_window, 
                                                 min_periods=self.min_periods)
            
            exprs.append(
                (cov / pl.max_horizontal(var, pl.lit(1e-12)))
                .fill_nan(0.0).fill_null(0.0).alias(beta_name)
            )
        return exprs

    def _build_spread_expressions(self, log_cols: List[str], anchor_col: str) -> List[pl.Expr]:
        exprs = []
        for col in log_cols:
            if col == anchor_col: continue
            
            asset_name = col.replace("log_", "")
            beta_name = f"beta_{asset_name}_{self.anchor_symbol}"
            spread_name = f"spread_{asset_name}"
            
            # Spread = log_y - (beta * log_x)
            exprs.append(
                (pl.col(col) - (pl.col(beta_name) * pl.col(anchor_col))).alias(spread_name)
            )
        return exprs

    def _build_zscore_expressions(self, log_cols: List[str]) -> List[pl.Expr]:
        exprs = []
        for col in log_cols:
            if col == self.anchor_symbol.lower(): continue # Handle specific naming if needed
            
            asset_name = col.replace("log_", "")
            if asset_name == self.anchor_symbol: continue
            
            spread_name = f"spread_{asset_name}"
            z_name = f"z_score_{asset_name}"
            
            # Z = (Spread - Mean) / Std
            mean = pl.col(spread_name).rolling_mean(window_size=self.zscore_window, 
                                                   min_periods=self.min_periods)
            std = pl.col(spread_name).rolling_std(window_size=self.zscore_window, 
                                                 min_periods=self.min_periods)
"""
STATIONARITY ENGINE (TIER 3)
Focus: Statistical Arbitrage Metrics (Rolling Beta, Spread, Z-Score).
Location: research/processing/features/stat_arb.py
Architecture: Tiered Chaining (Beta -> Spread -> Z-Score)
Compatibility: Polars 1.x (Integer-based stability)
"""
import logging
import polars as pl
from typing import List, Any

# Type-safe imports
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from ...shared import Result


logger = logging.getLogger("StatArbTransformer")

class StatArbTransformer:
    """
    Tier 3 Transformer: Statistical Arbitrage Engine.
    
    Mathematical Sequence:
    1. Beta (Hedge Ratio): Cov(Target, Anchor) / Var(Anchor)
    2. Spread (Residual): Target - (Beta * Anchor)
    3. Z-Score (Signal): Normalized Spread deviation.
    
    Production Features:
    - Pure Lazy execution (No .collect() inside).
    - Numerical Guardrails against zero variance noise.
    - Explicit chaining to handle column dependencies.
    """

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

    def transform(
        self, 
        data: pl.LazyFrame, 
        **kwargs: Any
    ) -> 'Result[pl.LazyFrame, str]':
        """
        Execute StatArb transformations with chained dependencies.
        """
        try:
            # 1. Schema Inspection
            schema_cols = data.collect_schema().names()
            
            # 2. Prerequisites Check (Log Prices from Tier 1)
            log_cols = [c for c in schema_cols if c.startswith("log_")]
            anchor_col = f"log_{self.anchor_symbol}"
            
            if anchor_col not in log_cols:
                return Err(f"Tier 3 Error: Anchor '{anchor_col}' not found. Run Tier 1 first.")
            
            if len(log_cols) < 2:
                return Err("Tier 3 Error: Need at least one target asset for arbitrage.")

            logger.info(f"Computing StatArb Features against anchor: {self.anchor_symbol}")

            # 3. Execution via Chaining (Ensures Beta exists before Spread, etc.)
            
            # STAGE A: Calculate Rolling Beta
            beta_exprs = self._build_beta_expressions(log_cols, anchor_col)
            lf_stage_a = data.with_columns(beta_exprs)
            
            # STAGE B: Calculate Spread (Depends on Beta)
            spread_exprs = self._build_spread_expressions(log_cols, anchor_col)
            lf_stage_b = lf_stage_a.with_columns(spread_exprs)
            
            # STAGE C: Calculate Z-Score (Depends on Spread)
            zscore_exprs = self._build_zscore_expressions(log_cols)
            final_lf = lf_stage_b.with_columns(zscore_exprs)

            return Ok(final_lf)

        except Exception as e:
            logger.error(f"StatArb Calculation Failed: {e}", exc_info=True)
            return Err(f"Tier 3 Error: {str(e)}")

    def _build_beta_expressions(self, log_cols: List[str], anchor_col: str) -> List[pl.Expr]:
        """Build expressions for rolling OLS beta with numerical noise protection."""
        exprs = []
        for col in log_cols:
            if col == anchor_col: continue
            
            asset_name = col.replace("log_", "")
            beta_name = f"beta_{asset_name}_{self.anchor_symbol}"
            
            # Beta = Cov(X,Y) / Var(X)
            cov = pl.rolling_cov(
                pl.col(col), 
                pl.col(anchor_col), 
                window_size=self.beta_window, 
                min_periods=self.min_periods
            )
            var = pl.col(anchor_col).rolling_var(
                window_size=self.beta_window, 
                min_periods=self.min_periods
            )
            
            # PRODUCTION FIX: Strict zero-guard to prevent noise on flat markets
            # Jika varians di bawah 1e-9, paksa beta ke 0.0
            beta_expr = (
                pl.when(var < 1e-9)
                .then(pl.lit(0.0))
                .otherwise(cov / var)
                .fill_nan(0.0)
                .fill_null(0.0)
                .alias(beta_name)
            )
            exprs.append(beta_expr)
        return exprs

    def _build_spread_expressions(self, log_cols: List[str], anchor_col: str) -> List[pl.Expr]:
        """Build expressions for spread (Residual)."""
        exprs = []
        for col in log_cols:
            if col == anchor_col: continue
            
            asset_name = col.replace("log_", "")
            beta_name = f"beta_{asset_name}_{self.anchor_symbol}"
            spread_name = f"spread_{asset_name}"
            
            # Spread = log_target - (beta * log_anchor)
            exprs.append(
                (pl.col(col) - (pl.col(beta_name) * pl.col(anchor_col))).alias(spread_name)
            )
        return exprs

    def _build_zscore_expressions(self, log_cols: List[str]) -> List[pl.Expr]:
        """Build expressions for z-score normalization."""
        exprs = []
        for col in log_cols:
            asset_name = col.replace("log_", "")
            if asset_name == self.anchor_symbol: continue
            
            spread_name = f"spread_{asset_name}"
            z_name = f"z_score_{asset_name}"
            
            # Z = (Spread - RollingMean) / RollingStd
            mean = pl.col(spread_name).rolling_mean(
                window_size=self.zscore_window, 
                min_periods=self.min_periods
            )
            std = pl.col(spread_name).rolling_std(
                window_size=self.zscore_window, 
                min_periods=self.min_periods
            )
            
            # Guard against division by zero in Z-score calculation
            exprs.append(
                ((pl.col(spread_name) - mean) / pl.max_horizontal(std, pl.lit(1e-12)))
                .fill_nan(0.0)
                .fill_null(0.0)
                .alias(z_name)
            )
        return exprs

    def _parse_to_rows(self, window_str: str) -> int:
        """Helper to convert duration strings to row counts (1m base)."""
        try:
            val = int(''.join(filter(str.isdigit, window_str)))
            unit = ''.join(filter(str.isalpha, window_str)).lower()
            if unit in ('h', 'hr'): return val * 60
            if unit in ('d', 'day'): return val * 1440
            if unit in ('w', 'week'): return val * 10080
            return val
        except:
            return 60 # Default to 1 hour

    def get_available_features(self) -> List[str]:
        return ["beta_*", "spread_*", "z_score_*"]

# ====================== FACTORY ======================

def create_stat_arb_transformer(
    beta_window: str = "1w", 
    zscore_window: str = "24h", 
    anchor_symbol: str = "BTC",
    **kwargs: Any
) -> StatArbTransformer:
    """Factory to create a StatArbTransformer instance."""
    return StatArbTransformer(
        beta_window=beta_window, 
        zscore_window=zscore_window, 
        anchor_symbol=anchor_symbol,
        **kwargs
    )

__all__ = ["StatArbTransformer", "create_stat_arb_transformer"]
