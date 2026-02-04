"""
MICROSTRUCTURE FEATURES MODULE (TIER 2)
Focus: Market Risk Sensors (Rolling Volatility & Correlation).
Location: research/processing/features/market_micro.py
Paradigm: Row-based rolling for maximum stability in Polars 1.x
"""
import logging
import polars as pl
from typing import Any
from core.shared import Ok, Err

logger = logging.getLogger("MicrostructureTransformer")

class MicrostructureTransformer:
    def __init__(self, windows=None, anchor_symbol="BTC", min_periods=2):
        self.windows = windows or ["1h", "4h", "24h"]
        self.anchor_symbol = anchor_symbol
        self.min_periods = min_periods

    def transform(self, data: pl.LazyFrame, **kwargs: Any):
        try:
            schema_cols = data.collect_schema().names()
            ret_cols = [c for c in schema_cols if c.startswith("ret_")]
            
            if not ret_cols:
                return Err("Tier 2 Failure: Missing 'ret_*' columns. Check Tier 1 execution.")
            
            anchor_col = f"ret_{self.anchor_symbol}"
            has_anchor = anchor_col in ret_cols

            expressions = []
            
            for window_str in self.windows:
                n_rows = self._parse_window_to_rows(window_str)
                
                for ret_col in ret_cols:
                    asset_name = ret_col.replace("ret_", "")
                    # Volatility expression (Integer Window)
                    expressions.append(
                        pl.col(ret_col)
                        .rolling_std(window_size=n_rows, min_periods=self.min_periods)
                        .fill_nan(0.0)
                        .fill_null(0.0)
                        .alias(f"vol_{asset_name}_{window_str}")
                    )
                
                if has_anchor:
                    for ret_col in ret_cols:
                        if ret_col == anchor_col: continue
                        asset_name = ret_col.replace("ret_", "")
                        # Correlation expression (Integer Window)
                        expressions.append(
                            pl.rolling_corr(
                                pl.col(ret_col), 
                                pl.col(anchor_col), 
                                window_size=n_rows, 
                                min_periods=self.min_periods
                            )
                            .fill_nan(0.0)
                            .fill_null(0.0)
                            .alias(f"corr_{asset_name}_{self.anchor_symbol}_{window_str}")
                        )

            return Ok(data.with_columns(expressions))
        except Exception as e:
            return Err(f"Tier 2 Execution Error: {str(e)}")

    def _parse_window_to_rows(self, window_str: str) -> int:
        """Converts string window to row count based on 1m interval."""
        val = int(''.join(filter(str.isdigit, window_str)))
        unit = ''.join(filter(str.isalpha, window_str)).lower()
        if 'h' in unit: return val * 60
        if 'd' in unit: return val * 1440
        if 'w' in unit: return val * 10080
        return val

def create_microstructure_transformer(windows=None, anchor_symbol="BTC", **kwargs):
    return MicrostructureTransformer(windows=windows, anchor_symbol=anchor_symbol)
