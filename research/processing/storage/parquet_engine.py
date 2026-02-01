import logging 
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .metadata_registry import MetadataRegistry

logger = logging.getLogger("ParquetEngine")

class ParquetStorageEngine:
    def __init__(self, base_path: str, registry: 'MetadataRegistry'):
        self.base_path = Path(base_path).resolve()
        self.registry = registry
        
        self.base_path.mkdir(parents=True, exist_ok=True)
        logger.debug(f"ParquetEngine initialized at: {self.base_path}")


