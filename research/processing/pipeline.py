"""
NODE B PROCESSING PIPELINE (THE CONDUCTOR)
Focus: End-to-end orchestration from Brown Lake to Silver Lake.
Location: research/processing/pipeline.py
Paradigm: Facade Pattern
"""
import logging
import polars as pl
from typing import Any, List, Union

from core.shared import Result, Err

from .transformation.returns import create_log_returns_transformer
from .features.market_micro import create_microstructure_transformer
from .features.stat_arb import create_stat_arb_transformer
from .storage.metadata_registry import create_metadata_registry
from .storage.parquet_engine import create_parquet_engine
from .validation import create_validator, PolarsValidator


logger = logging.getLogger('ProcessingPipeline')

class NodeBPipeline:
    """
    The main orchestrator for Node B.
    Refines raw Brown Lake data into high-precision Silver Lake features.

    Sequence:
    0. Validation (PolarsValidator)
    1. Log Returns (Tier 1)
    2. Microstructure (Tier 2)
    3. Statistical Arbitrage (Tier 3)
    4. Storage (Parquet with Hive partitioning)
    """

    def __init__(self, 
                 silver_path: str, 
                 anchor_symbol: str='BTC', 
                 windows: List[str]=['1h', '4h', '24h'], 
                 beta_window: str='1w', 
                 zscore_window: str='24h') -> None:
        """
        Args:
            silver_path: Absolute path to Silver Lake directory.
            anchor_symbol: Reference asset for correlation and beta.
            windows: Rolling windows for Tier 2 features (default: ['1h','4h','24h']).
            beta_window: Window for OLS beta calculation.
            zscore_window: Window for z-score normalization.
        """
        self.silver_path = silver_path
        self.anchor_symbol = anchor_symbol

        # 1. Metadata Registry (The Notary)
        self.registry = create_metadata_registry(silver_path)

        # 2. Storage Engine (The Factory)
        self.storage = create_parquet_engine(silver_path, self.registry)

        # 3. Validator (The Gatekeeper) – Fail fast if not available.
        validator_result: Result[PolarsValidator, str] = create_validator(
            {"required_columns": ["timestamp"]}   # <-- RELAXED RULE
        )
        if validator_result.is_err():
            raise RuntimeError(
                f"Cannot initialize pipeline: Validator creation failed -> {validator_result.error}"
            )
        self.validator: PolarsValidator = validator_result.unwrap()

        # 4. Configuration (Immutable for this run)
        self.config: dict = {'anchor_symbol': anchor_symbol, 
                             'windows': windows if not None else ['1h', '4h', '24h'], 
                             'beta_window': beta_window, 
                             'zscore_window': zscore_window, 
                             'pipeline_version': '1.1.0'}

        logger.info(f"Pipeline Initialized | Anchor: {anchor_symbol} |"
                    f"Windows: {self.config['windows']} | Beta: {beta_window} | Z: {zscore_window}")

    def run_batch(self, 
                  raw_data: Union[pl.DataFrame, pl.LazyFrame]) -> Result[str, str]:
        """
        Execute the full transformation pipeline on a batch of aligned data.

        Args:
            raw_data: Polars DataFrame or LazyFrame (wide format, with timestamp and close_* columns).

        Returns:
            Result[str, str]: Path to stored Silver Lake data on success, error message on failure.
        """
        try:
            data_lf: pl.LazyFrame = (raw_data.lazy() 
                                     if isinstance(raw_data, pl.DataFrame) 
                                     else raw_data)
        except Exception as e:
            return Err(f"Input normalization failed: {str(e)}")

        logger.info(f'Initiating batch processing for anchor: {self.anchor_symbol}')

        # Step 1: VALIDATION (Guard against corrupted data)
        validation_result = self.validator.validate(data_lf)
        if validation_result.is_err():
            return Err(f"VALIDATION FAILED: {validation_result.error}")
        
        validated_lf: pl.LazyFrame = validation_result.unwrap()
        logger.debug("Validation passed – data integrity confirmed.")

        # Step 2: TIER 1 – Log Returns
        t1 = create_log_returns_transformer()
        t1_result = t1.transform(validated_lf)
        if t1_result.is_err():
            return Err(f"TIER 1 (LogReturns) failed: {t1_result.error}")
        lf_t1: pl.LazyFrame = t1_result.unwrap()
        logger.debug("Tier 1 complete (log/ret columns added).")

        # Step 3: TIER 2 – Microstructure (Volatility, Correlation)
        t2 = create_microstructure_transformer(windows=self.config['windows'], 
                                               anchor_symbol=self.anchor_symbol)
        t2_result = t2.transform(lf_t1)
        if t2_result.is_err():
            return Err(f"TIER 2 (Microstructure) failed: {t2_result.error}")
        lf_t2: pl.LazyFrame = t2_result.unwrap()
        logger.debug("Tier 2 complete (vol_*, corr_* columns added).")

        # Step 4: TIER 3 – Statistical Arbitrage (Beta, Spread, Z‑Score)
        t3 = create_stat_arb_transformer(beta_window=self.config['beta_window'], 
                                         zscore_window=self.config['zscore_window'], 
                                         anchor_symbol=self.anchor_symbol)
        t3_result = t3.transform(lf_t2)
        if t3_result.is_err():
            return Err(f"TIER 3 (StatArb) failed: {t3_result.error}")
        final_lf: pl.LazyFrame = t3_result.unwrap()
        logger.debug("Tier 3 complete (beta_*, spread_*, z_score_* columns added).")

        # Step 5: STORAGE – Write to Silver Lake with Hive partitioning
        logger.info("All transformations successful. Writing to Silver Lake...")
        save_result = self.storage.save(final_lf, feature_params=self.config)
        if save_result.is_err():
            return Err(f"STORAGE FAILED: {save_result.error}")

        stored_path: str = save_result.unwrap()
        logger.info(f"Batch processing completed successfully. Data stored at: {stored_path}")
        return save_result


# ====================== FACTORY (Public Interface) ======================

def create_processing_pipeline(
    silver_path: str,
    **kwargs: Any
) -> NodeBPipeline:
    """
    Factory function for NodeBPipeline.

    Args:
        silver_path: Absolute path to Silver Lake directory.
        **kwargs: Override default parameters (anchor_symbol, windows, etc.)

    Returns:
        Fully initialized NodeBPipeline instance.
    """
    return NodeBPipeline(silver_path=silver_path, **kwargs)


__all__ = ['NodeBPipeline', 'create_processing_pipeline']
