"""
NODE B PROCESSING PIPELINE (THE CONDUCTOR)
Focus: End-to-end orchestration from Brown Lake to Silver Lake.
Location: research/processing/pipeline.py
Paradigm: Facade Pattern
"""
import logging
import polars as pl
from typing import Any, List
from .transformation.returns import create_log_returns_transformer
from .features.market_micro import create_microstructure_transformer
from .features.stat_arb import create_stat_arb_transformer
from .storage.metadata_registry import create_metadata_registry
from .storage.parquet_engine import create_parquet_engine
from core.shared import Err, Result
logger = logging.getLogger('ProcessingPipeline')

class NodeBPipeline:
    """
    The main orchestrator for Node B.
    Refines raw Brown Lake data into high-precision Silver Lake features.
    
    Sequence:
    1. Returns (Tier 1) -> 2. Microstructure (Tier 2) -> 3. StatArb (Tier 3) -> 4. Storage
    """

    def __init__(self, silver_path: str, anchor_symbol: str='BTC', windows: List[str]=['1h', '4h', '24h'], beta_window: str='1w', zscore_window: str='24h'):
        """
        Args:
            silver_path: Target directory for Silver Lake.
            anchor_symbol: Reference asset for correlation and beta.
            windows: Rolling windows for Tier 2 features.
            beta_window: Window for Tier 3 OLS Beta.
            zscore_window: Window for Tier 3 normalization.
        """
        self.silver_path = silver_path
        self.anchor_symbol = anchor_symbol
        self.registry = create_metadata_registry(silver_path)
        self.storage = create_parquet_engine(silver_path, self.registry)
        self.config = {'anchor_symbol': anchor_symbol, 'windows': windows, 'beta_window': beta_window, 'zscore_window': zscore_window, 'pipeline_version': '1.0.0'}

    def run_batch(self, raw_data: Any) -> Result[str, str]:
        """
        Executes the full transformation sequence with strict error tracking.
        """
        try:
            data = raw_data.lazy() if isinstance(raw_data, pl.DataFrame) else raw_data
            logger.info(f'Initiating batch processing for anchor: {self.anchor_symbol}')
            t1 = create_log_returns_transformer()
            res_t1 = t1.transform(data)
            if res_t1.is_err():
                return Err(f'T1 Failed: {res_t1.error}')
            lf_t1 = res_t1.unwrap()
            t2 = create_microstructure_transformer(windows=self.config['windows'], anchor_symbol=self.anchor_symbol)
            res_t2 = t2.transform(lf_t1)
            if res_t2.is_err():
                return Err(f'T2 Failed: {res_t2.error}')
            lf_t2 = res_t2.unwrap()
            t3 = create_stat_arb_transformer(beta_window=self.config['beta_window'], zscore_window=self.config['zscore_window'], anchor_symbol=self.anchor_symbol)
            res_t3 = t3.transform(lf_t2)
            if res_t3.is_err():
                return Err(f'T3 Failed: {res_t3.error}')
            final_lf = res_t3.unwrap()
            logger.info('Refinement complete. Streaming to Silver Lake...')
            save_res = self.storage.save(final_lf, feature_params=self.config)
            if save_res.is_err():
                return Err(f'Storage Failed: {save_res.error}')
            logger.info('Batch processing sequence finalized successfully')
            return save_res
        except Exception as e:
            logger.error(f'Pipeline Execution Crash: {str(e)}', exc_info=True)
            return Err(f'Pipeline Fatal Error: {str(e)}')

def create_processing_pipeline(silver_path: str, **kwargs: Any) -> NodeBPipeline:
    """Factory function for NodeBPipeline."""
    return NodeBPipeline(silver_path=silver_path, **kwargs)
__all__ = ['NodeBPipeline', 'create_processing_pipeline']