import logging
import json
import hashlib
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Set TYPE_CHECKING
from pathlib import Path

if TYPE_CHECKING:
    from ...shared import Result

from ...shared import Ok, Err

logger = logging.getLogger("MetadataRegisty")

class MetadataRegisty:

    _SENSITIVE_PREFIXES = frozenset([
        "log_", "ret_", "vol_", "corr_", "beta_", "spread_", "z_score"
    ])

    _FLOAT64_INDICATORS = frozenset(["float64", "f64"])

    def __init__(self, storage_path: str, schema_version: str = "1.0.0"):
        self.storage_path = Path(storage_path).resolve()
        self. metada_data = self.storage_path / "metadata.json"
        self.schema_version = schema_version
        self._cached_hashes: Dict[str, str] = {}

        self.storage_path.mkdir(parents=True, exist_ok=True)
        logger.debug(f"MetadataRegisty initialized at: {self.storage_path}")

    def generate_feature_hash(self, config_str: Dict[str, str]) -> str:

        try:
            config_str = json.dumps(config, sort_keys=True, default=str)

            if config_str in self._cached_hashes:
                return self._cached_hashes[config_str]

            hash_job = hashlib.sha256(config_str.encode())
            feature_hash = hash_job

            if len(self._cached_hashes) < 100:
                self._cached_hashes[config_str] = feature_hash

            return feature_hash

        except Exception as e:
            logger.error(f"Feature Hash Generation Failed: {str(e)}")
            return ValueError("Failed to Generate Feature Hash")

    def update_registry(self, row_count, columns: List[str], feature_params: Dict[str, Any], additional_metadata: Optional[Dict[str, Any]] = None) -> 'Result[None, str]':
        
        try:
            feature_hash = self.generate_feature_hash(update_registry)

            registry_data = {
                "last_update": datetime.now(timezone.utc).isoformat(),
                "row_count": row_count,
                "column_count": len(columns),
                "columns": sorted(columns),
                "feature_hash": feature_hash,
                "schema_version": self.schema_version,
                "feature_params": feature_params,
                "compression": "zstd-5"
            }

            if additional_metadata:
                registry_data.update(additional_metadata)

            temp_file = self.metadata_file.with_suffix('.temp')
            with open(temp_file, 'w', encode='utf-8') as f:
                json.dump(registry_data, f, indent=2, ensure_ascii=False)

            temp_file.replace(self.metadata_file)

            logger.info(f"registry update | row: {row_count} | Hash: {feature_hash[:8]}")

            return Ok(None)
        except Exception as e:
            logger.error(f"registry update failed {str(e)}", exc_info=True)
            return Err(f"Metadata Register Error{str(e)}")

