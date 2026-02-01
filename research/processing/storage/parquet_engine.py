import logging 
from pathlib import Path
from typing import TYPE_CHECKING, Dict, Any
import polars as pl

if TYPE_CHECKING:
    from .metadata_registry import MetadataRegistry
    from ...shared import Result

from ...shared import Ok, Err

logger = logging.getLogger("ParquetEngine")

class ParquetStorageEngine:
    def __init__(self, base_path: str, registry: 'MetadataRegistry'):
        self.base_path = Path(base_path).resolve()
        self.registry = registry
        
        self.base_path.mkdir(parents=True, exist_ok=True)
        logger.debug(f"ParquetEngine initialized at: {self.base_path}")

    def save(self, data: pl.LazyFrame, feature_params: Dict[str, Any]) -> 'Result[str, str]':
        try:
            logger.info("Starting Silver lake storage sequence ...")

            schema = data.collect_schema()
            validation = self.registry.validate_schema_integrity(schema)
            if validation.is_err():
                return Err(f"Storage Rejected: {validation.error}")

            processed_lf = data.with_columns([
                pl.col("timestamp").dt.year().cast(pl.Utf8).alias("year"),
                pl.col("timestamp").dt.month().cast(pl.Utf8).str.zfill(2).alias("month")
            ])

            logger.info(f"Collecting and partitioning data into: {self.base_path}")
            df = processed_lf.collect()

            df.write_parquet(
                self.base_path,
                compression="zstd",
                compression_level=5,
                partition_by=['year', 'month'],
                maintain_order=False
            )

            self.registry.update_registry(
                row_count=df.height,
                columns=df.columns,
                feature_params=feature_params
            )

            logger.info(f"Storage complete | partition created at: {self.base_path}")
            return Ok(str(self.base_path))

        except Exception as e:
            logger.error(f"Storage Engine Failure:{str(e)}", exc_info=True)
            return Err(f"Parquet Write Error: {str(e)}")

# ====================== FACTORY ======================
def created_parquet_engine(
    base_path: str,
    registry: 'MetadataRegistry'
) -> ParquetStorageEngine:
    return ParquetStorageEngine(base_path=base_path, registry=registry)

__all__ = ["ParquetStorageEngine", "created_parquet_engine"]
