import logging
import json
import hashlib
from typing import Dict, TYPE_CHECKING
from pathlib import Path

if TYPE_CHECKING:
    pass


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


