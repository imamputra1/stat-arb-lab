"""
METADATA REGISTRY MODULE (THE NOTARY)
Focus: Schema validation, Feature Hashing, and Storage bookkeeping.
Location: research/processing/storage/metadata_registry.py
Paradigm: Composition & Type Discipline
"""
import json
import logging
import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List, Optional

# Type-safe imports
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from ...shared import Result

from ...shared import Ok, Err

logger = logging.getLogger("MetadataRegistry")

class MetadataRegistry:
    """
    The Notary of Node B.
    Ensures data integrity and records the lineage of processed features.
    
    Responsibilities:
    1. Calculate SHA-256 Feature Hashes for Walk-Forward consistency.
    2. Maintain metadata.json for backtest validation.
    3. Enforce strict Schema constraints (Float64).
    """

    # Pre-compiled sets for O(1) lookups
    _SENSITIVE_PREFIXES = frozenset([
        "log_", "ret_", "vol_", "corr_", "beta_", "spread_", "z_score_"
    ])
    
    _FLOAT64_INDICATORS = frozenset(["float64", "f64"])

    def __init__(self, storage_path: str, schema_version: str = "1.0.0"):
        """
        Args:
            storage_path: Directory for metadata storage.
            schema_version: Version string for schema compatibility.
        """
        self.storage_path = Path(storage_path).resolve()
        self.metadata_file = self.storage_path / "metadata.json"
        self.schema_version = schema_version
        self._cached_hashes: Dict[str, str] = {}
        
        # Ensure storage path exists during initialization
        self.storage_path.mkdir(parents=True, exist_ok=True)
        logger.debug(f"MetadataRegistry initialized at: {self.storage_path}")

    def generate_feature_hash(self, config: Dict[str, Any]) -> str:
        """
        Generates a SHA-256 hash from a configuration dictionary.
        Prevents stale data bugs by tracking parameter changes across tiers.
        """
        try:
            # Create deterministic string representation
            # Sort keys is mandatory for hash consistency
            config_str = json.dumps(config, sort_keys=True, default=str)
            
            # Simple runtime cache check
            if config_str in self._cached_hashes:
                return self._cached_hashes[config_str]
            
            # Generate SHA-256 hash
            hash_obj = hashlib.sha256(config_str.encode())
            feature_hash = hash_obj.hexdigest()
            
            # Update cache (limited to 100 entries to prevent memory growth)
            if len(self._cached_hashes) < 100:
                self._cached_hashes[config_str] = feature_hash
            
            return feature_hash
            
        except Exception as e:
            logger.error(f"Feature hash generation failed: {str(e)}")
            raise ValueError(f"Failed to generate feature hash: {str(e)}")

    def update_registry(
        self, 
        row_count: int, 
        columns: List[str], 
        feature_params: Dict[str, Any],
        additional_metadata: Optional[Dict[str, Any]] = None
    ) -> 'Result[None, str]':
        """
        Updates the registry with the latest production run metadata.
        Uses atomic write pattern to prevent corruption.
        """
        try:
            feature_hash = self.generate_feature_hash(feature_params)
            
            # Prepare registry data
            registry_data = {
                "last_update": datetime.now(timezone.utc).isoformat(),
                "row_count": row_count,
                "column_count": len(columns),
                "columns": sorted(columns),
                "feature_hash": feature_hash,
                "schema_version": self.schema_version,
                "feature_params": feature_params,
                "format": "parquet",
                "compression": "zstd-5"
            }
            
            if additional_metadata:
                registry_data.update(additional_metadata)
            
            # Atomic Write Pattern: Write to .tmp then rename
            temp_file = self.metadata_file.with_suffix('.tmp')
            with open(temp_file, 'w', encoding='utf-8') as f:
                json.dump(registry_data, f, indent=2, ensure_ascii=False)
            
            temp_file.replace(self.metadata_file)
            
            logger.info(
                f"Registry updated | Rows: {row_count} | Hash: {feature_hash[:8]}"
            )
            return Ok(None)
            
        except Exception as e:
            logger.error(f"Registry update failed: {str(e)}", exc_info=True)
            return Err(f"Metadata Registry Error: {str(e)}")

    def validate_schema_integrity(self, schema_dict: Dict[str, Any]) -> 'Result[None, str]':
        """
        Enforces strict Float64 for sensitive financial features.
        Required for Kalman Filter matrix stability in Node S.
        """
        violations: List[str] = []
        
        for col, dtype in schema_dict.items():
            # Check if column starts with any sensitive prefix
            is_sensitive = any(col.startswith(prefix) for prefix in self._SENSITIVE_PREFIXES)
            
            if is_sensitive:
                dtype_str = str(dtype).lower()
                
                # Verify if dtype is Float64
                is_float64 = any(indicator in dtype_str for indicator in self._FLOAT64_INDICATORS)
                
                if not is_float64:
                    violations.append(f"{col} ({dtype_str})")
        
        if violations:
            error_msg = f"Precision violation: {', '.join(violations)} must be Float64"
            logger.error(f"Schema integrity check failed: {error_msg}")
            return Err(error_msg)
        
        logger.debug(f"Schema integrity validated for {len(schema_dict)} columns")
        return Ok(None)

    def load_registry(self) -> 'Result[Dict[str, Any], str]':
        """Loads existing metadata from registry file."""
        try:
            if not self.metadata_file.exists():
                return Err(f"Metadata not found: {self.metadata_file}")
            
            with open(self.metadata_file, 'r', encoding='utf-8') as f:
                metadata = json.load(f)
            
            return Ok(metadata)
            
        except Exception as e:
            logger.error(f"Failed to load registry: {str(e)}")
            return Err(f"Registry load failed: {str(e)}")

    def verify_consistency(
        self, 
        current_params: Dict[str, Any], 
        expected_params: Optional[Dict[str, Any]] = None
    ) -> 'Result[bool, str]':
        """Verifies if current parameters match the registry hash."""
        try:
            if expected_params is None:
                registry_res = self.load_registry()
                if registry_res.is_err():
                    return Err(f"Consistency check failed: {registry_res.error}")
                expected_params = registry_res.unwrap().get("feature_params", {})
            
            current_hash = self.generate_feature_hash(current_params)
            expected_hash = self.generate_feature_hash(expected_params)
            
            is_consistent = (current_hash == expected_hash)
            if not is_consistent:
                logger.warning(f"Hash mismatch: {current_hash[:8]} vs {expected_hash[:8]}")
                
            return Ok(is_consistent)
            
        except Exception as e:
            return Err(f"Consistency verification error: {str(e)}")

# ====================== FACTORY ======================

def create_metadata_registry(storage_path: str, **kwargs: Any) -> MetadataRegistry:
    """Factory for MetadataRegistry."""
    return MetadataRegistry(storage_path=storage_path, **kwargs)

# ====================== EXPORTS ======================
__all__ = ["MetadataRegistry", "create_metadata_registry"]
