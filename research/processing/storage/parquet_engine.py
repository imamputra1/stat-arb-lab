"""
PARQUET ENGINE MODULE (THE FACTORY)
Focus: Efficient Hive-style partitioning and ZSTD compression.
Location: research/processing/storage/parquet_engine.py
Paradigm: Optimized Eager Writing for Hive Partitioning
"""
import logging
import polars as pl
from pathlib import Path
from typing import Dict, Any

# Type-safe imports
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from ...shared import Result
    from .metadata_registry import MetadataRegistry

from ...shared import Ok, Err

logger = logging.getLogger("ParquetEngine")

class ParquetStorageEngine:
    """
    The Factory of Silver Lake.
    Handles physical writing of data into Hive-partitioned Parquet files.
    """

    def __init__(self, base_path: str, registry: 'MetadataRegistry'):
        self.base_path = Path(base_path).resolve()
        self.registry = registry
        
        # Ensure base directory exists
        self.base_path.mkdir(parents=True, exist_ok=True)
        logger.debug(f"ParquetEngine initialized at: {self.base_path}")

    def save(
        self, 
        data: pl.LazyFrame, 
        feature_params: Dict[str, Any]
    ) -> 'Result[str, str]':
        """
        Executes the storage sequence with KOTOR bin SUPERIOR stability.
        """
        try:
            logger.info("Starting Silver Lake storage sequence...")
            
            # 1. Schema Check (The Notary Gate)
            schema = data.collect_schema()
            validation = self.registry.validate_schema_integrity(schema)
            if validation.is_err():
                return Err(f"Storage rejected: {validation.error}")

            # 2. Partition Injection (Hive Keys)
            processed_lf = data.with_columns([
                pl.col("timestamp").dt.year().cast(pl.Utf8).alias("year"),
                pl.col("timestamp").dt.month().cast(pl.Utf8).str.zfill(2).alias("month")
            ])

            # 3. The Execution Strategy
            # Polars 1.x partition_by is only stable in eager write_parquet.
            # We collect once to avoid multiple lazy passes.
            logger.info(f"Collecting and partitioning data into: {self.base_path}")
            df = processed_lf.collect()
            
            # 4. Physical Write (Hive Style)
            # FIX: Removed 'maintain_order' which caused TypeError in eager writer.
            df.write_parquet(
                self.base_path,
                compression="zstd",
                compression_level=5,
                partition_by=["year", "month"],
                use_pyarrow=False
            )

            # 5. Metadata Registry Update
            self.registry.update_registry(
                row_count=df.height,
                columns=df.columns,
                feature_params=feature_params
            )

            logger.info(f"Storage complete | Partitions created at {self.base_path}")
            return Ok(str(self.base_path))

        except Exception as e:
            logger.error(f"Storage Engine Failure: {str(e)}", exc_info=True)
            return Err(f"Parquet Write Error: {str(e)}")

# ====================== FACTORY ======================

def create_parquet_engine(
    base_path: str, 
    registry: 'MetadataRegistry'
) -> ParquetStorageEngine:
    """Factory for ParquetStorageEngine."""
    return ParquetStorageEngine(base_path=base_path, registry=registry)

# ====================== EXPORTS ======================
__all__ = ["ParquetStorageEngine", "create_parquet_engine"]
