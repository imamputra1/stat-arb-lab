"""
VALIDATION RULES (THE LAW)
Configuration dataclass for data quality validation.
Focus: Immutable, Type-Safe, Pure Python (No Polars dependencies).
"""
from dataclasses import dataclass, field, replace, asdict
from typing import List, Dict, Any
import logging

# Setup Logger khusus untuk module ini
logger = logging.getLogger("ValidationRules")

@dataclass(frozen=True)
class ValidationRules:
    """
    Immutable Configuration for Data Validation.
    
    Attributes:
        min_rows: Minimum rows required for statistical significance.
        max_null_pct: Max allowed null percentage (0.0 - 1.0).
        required_columns: List of columns that MUST exist.
        check_sorted: Ensure timestamp is strictly increasing.
        strict_types: Enforce specific data types (Int64/Datetime).
        
        # Financial Checks
        min_price: Minimum allowed price (prevents negative/zero prices).
        max_price_spread_pct: Max High-Low spread (detects wick anomalies).
        check_ohlc_consistency: Ensure High >= Low, High >= Close, etc.
        allow_zero_volume: Allow volume to be 0 (False for major pairs).
        
        # Timestamp Checks
        validate_timestamp_range: Enable min/max timestamp check.
        min_timestamp: Min Unix MS (default 0).
        max_timestamp: Max Unix MS (default Year 2286).
    """
    
    # Core Params
    min_rows: int = 100
    max_null_pct: float = 0.05
    required_columns: List[str] = field(default_factory=lambda: ["timestamp", "close"])
    
    # Structural Checks
    check_sorted: bool = True
    strict_types: bool = True
    
    # Financial Logic
    check_ohlc_consistency: bool = True
    min_price: float = 0.0
    max_price_spread_pct: float = 0.5  # 50% spread
    allow_zero_volume: bool = True
    
    # Range Logic
    validate_timestamp_range: bool = False
    min_timestamp: int = 0
    max_timestamp: int = 10_000_000_000_000

    def __post_init__(self) -> None:
        """
        Sanity Check immediately upon creation.
        Since frozen=True, we cannot modify self here, only validate.
        """
        # Validasi Logika Dasar
        if self.min_rows < 1:
            raise ValueError(f"min_rows must be >= 1, got {self.min_rows}")
        
        if not (0.0 <= self.max_null_pct <= 1.0):
            raise ValueError(f"max_null_pct must be 0.0-1.0, got {self.max_null_pct}")
            
        if self.min_price < 0:
            raise ValueError("min_price cannot be negative")
            
        if self.validate_timestamp_range and (self.min_timestamp >= self.max_timestamp):
            raise ValueError("min_timestamp must be < max_timestamp")

        # Pastikan required_columns tidak kosong
        if not self.required_columns:
            raise ValueError("required_columns cannot be empty")

    @classmethod
    def from_dict(cls, config: Dict[str, Any]) -> "ValidationRules":
        """
        Safe Factory: Creates rules from a dictionary, ignoring unknown keys.
        Auto-injects 'timestamp' into required_columns if missing.
        """
        if not isinstance(config, dict):
            logger.warning(f"Invalid config type: {type(config)}, using defaults.")
            return cls()

        # 1. Filter keys yang valid sesuai definisi dataclass
        valid_keys = cls.__annotations__.keys()
        filtered = {k: v for k, v in config.items() if k in valid_keys}
        
        # 2. Handle required_columns khusus
        # Kita pastikan 'timestamp' selalu ada di sini (sebelum object dibuat)
        if "required_columns" in filtered:
            cols = filtered["required_columns"]
            # Convert tuple ke list jika perlu
            if isinstance(cols, tuple):
                cols = list(cols)
            
            # Inject timestamp jika belum ada
            if "timestamp" not in cols:
                cols = ["timestamp"] + cols
            
            filtered["required_columns"] = cols

        return cls(**filtered)

    def to_dict(self) -> Dict[str, Any]:
        """Export configuration to dictionary."""
        return asdict(self)

    def with_overrides(self, **kwargs) -> "ValidationRules":
        """
        Create a NEW Rules object with updated values (Immutable Pattern).
        Replaces manual 'update' or 'copy' methods.
        
        Usage:
            new_rules = rules.with_overrides(min_rows=500)
        """
        try:
            # Cek apakah key override valid
            valid_keys = self.__annotations__.keys()
            safe_kwargs = {}
            
            for k, v in kwargs.items():
                if k in valid_keys:
                    safe_kwargs[k] = v
                else:
                    logger.warning(f"Ignoring unknown rule override: '{k}'")
            
            return replace(self, **safe_kwargs)
            
        except Exception as e:
            logger.error(f"Failed to override rules: {e}")
            return self

    # --- Factory Methods (Presets) ---

    @classmethod
    def create_strict_rules(cls) -> "ValidationRules":
        """Preset: Production-Grade Strict Rules."""
        return cls(
            min_rows=1000,
            max_null_pct=0.01, # Max 1% null
            required_columns=["timestamp", "open", "high", "low", "close", "volume"],
            check_sorted=True,
            strict_types=True,
            check_ohlc_consistency=True,
            min_price=0.00000001,
            max_price_spread_pct=0.2, # Max 20% spread
            allow_zero_volume=False,
            validate_timestamp_range=True
        )

    @classmethod
    def create_loose_rules(cls) -> "ValidationRules":
        """Preset: Debugging/Research Loose Rules."""
        return cls(
            min_rows=10,
            max_null_pct=0.50, # Max 50% null allowed
            required_columns=["timestamp", "close"],
            check_sorted=False,
            strict_types=False,
            check_ohlc_consistency=False,
            allow_zero_volume=True,
            max_price_spread_pct=10.0
        )

# ====================== RUNTIME CHECK ======================

if __name__ == "__main__":
    # Simple self-check when running this file directly
    try:
        print("--- Testing Rules Module ---")
        
        # 1. Default
        r1 = ValidationRules()
        print(f"Default Rules: OK (min_rows={r1.min_rows})")
        
        # 2. Strict Factory
        r2 = ValidationRules.create_strict_rules()
        print(f"Strict Rules: OK (strict={r2.strict_types})")
        
        # 3. Override Mechanism
        r3 = r1.with_overrides(min_rows=999, invalid_key=123)
        assert r3.min_rows == 999
        assert r1.min_rows == 100 # Original must not change
        print("Immutability Check: OK")
        
        # 4. From Dict Injection
        r4 = ValidationRules.from_dict({"required_columns": ["close"]})
        assert "timestamp" in r4.required_columns
        print(f"Auto-inject Timestamp: OK ({r4.required_columns})")
        
    except Exception as e:
        print(f"Rules Check Failed: {e}")
        raise
