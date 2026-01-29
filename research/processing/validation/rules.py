from typing import List, Dict, Any
from dataclasses import asdict, dataclass, field, replace
import logging



logger = logging.getLogger("ValidatorRules")

@dataclass(frozen=True)
class ValidatorRules:
    """
    Immutable configuration for data validation.

    Attributes:
        min_rows: Minimum rows required for statistical significance.
        max_null: Max allowed null percentage (0.0 - 1.0)
        required_columns: List of columns that must exist.
        check_sorted: Ensure timestamp is strictly increasing.
        strict_types: enforce specific data types(int64/Datetime)

        # financial Checks.
        min_price: minimum allowed price (prevets negative/zero price)
        max_price_speard_pct: Max High-low spread (Detects wick anomalies).
        check_ohlc_consistency: ensure High > Low, High >= close, etc.
        allow_zero_volume: allow volume to be 0 (false for major pairs).

        # timestamp checks.
        validate_timestamp_range: Enable min/max timestamp check.
        min_timestamp: min unix MS (default 0)
        max_timestamp: max unix Ms (default 2286)
    """

    # ==== CORE PARAMS ====
    min_rows: int = 100
    max_null_pct: float = 0.05
    required_columns: List[str] = field(default_factory=lambda: ["timestamp", "close"])

    # ==== STRUCTURE CHECKS ====
    check_sorted: bool = True
    strict_types: bool = True

    # ==== FINANCIAL LOGIC ====
    check_ohlc_consistency: bool = True
    min_price: float = 0.0
    max_price_spread_pct: float = 0.5
    allow_zero_volume: bool = True

    # ==== RANGE ROGIC ====
    validate_timestamp_range: bool = False
    min_timestamp: int = 0
    max_timestamp: int = 10_000_000_000_000

    def __post_init__(self) -> None:
        """
        sanity check immediatly upon creation.
        since frozen=True, we cannot modify self here, only validate.
        """

        if self.min_rows < 1:
            raise ValueError(f"min_rows must be >= 1, got {self.min_rows}")

        if not (0.0 <= self.max_null_pct <= 1.0):
            raise ValueError(f"max_null_pct must be 0.0 - 1.0, got {self.max_null_pct}")

        if self.min_price < 0:
            raise ValueError("min_price cannot negative")

        if self.validate_timestamp_range and (self.min_timestamp - self.max_timestamp):
            raise ValueError("min_timestamp must be < max_timestamp")

        if not self.required_columns:
            raise ValueError("required_columns cannot be empty")

    @classmethod
    def from_dict(cls, config: Dict[str, Any]) -> "ValidatorRules":
        """
        safe Factory: creates rules from a dictionary, ignoring unknwon keys.
        auto-injects 'timestamp' into required_columns if missing
        """
        if not isinstance(config, dict):
            logging.warning(f"Invalid config Type: {type(config)}, using defaults.")
            return cls()

        valid_keys = cls.__annotations__.keys()
        filtered = {k: v for k, v in config.items() if k in valid_keys}

        if "required_columns" in filtered:
            cols = filtered["required_columns"]
            if isinstance(cols, tuple):
                cols = list(cols)

            if "timestamp" not in cols:
                cols = ["timestamp"] + cols

            filtered["required_columns"] = cols

        return cls(**filtered)

    def to_dict(self) -> Dict[str, Any]:
        """Export configuration to dictionary"""
        return asdict(self)

    def with_overrides(self, **kwargs) -> "ValidatorRules":
        """
        Create a new rules obj with update values (Immutable pattern)
        Replaces manual 'update' or copy methods.

        usage:
            new_rules = rules.with_overrides(min_rows=500)
        """

        try:
            valid_keys = self.__annotations__.keys()
            safe_kwargs = {}

            for k, v in kwargs.items():
                if k in valid_keys:
                    safe_kwargs  = v
                else:
                    logger.warning(f"ignoring unknown rules with_overrides: '{k}'")

            return replace(self, **safe_kwargs)

        except Exception as e:
            logger.error(f"failed to overrides rules: {e}")

    @classmethod
    def create_strict_rules(cls) -> "ValidatorRules":
        """preset: production-grade strict Rules"""
        return cls(
            min_rows=1000,
            max_null_pct=0.01,
            required_columns=["timestamp", "open", "high", "low", "close", "volume"],
            check_sorted=True,
            strict_types=True,
            check_ohlc_consistency=True,
            min_price=0.00000001,
            max_price_spread_pct=0.2,
            allow_zero_volume=False,
            validate_timestamp_range=True
        )

    @classmethod
    def create_loose_rules(cls) ->  "ValidatorRules":
        """Preset: Debugging/Research loose rules"""
        return cls(
            min_rows=10,
            max_null_pct=0.50,
            required_columns=["timestamp", "close"],
            check_sorted=False,
            strict_types=False,
            check_ohlc_consistency=False,
            allow_zero_volume=True,
            max_price_spread_pct=10.0
        )

    # ==== RUNTIME CHECK ====

if __name__ == "__main__":
    try:
        print("*****|Testing Rules Module|*****")

        r1 = ValidatorRules()
        print(f"Default Rules: OK (main_rows={r1.min_rows})")

        r2 = ValidatorRules.create_strict_rules()
        print(f"Strict Rules: OK (strict]{r2.strict_types})")

        r3 = r1.with_overrides(min_rows=999, invalid_key=123)
        assert r3.min_rows == 999
        assert r1.min_rows == 100
        print("Immutable Check: Ok")

        r4 = ValidatorRules.from_dict({"required_columns": ["close"]})
        assert "timestamp" in r4.required_columns
        print(f"auto-injects timestamp: OK({r4.required_columns})")

    except Exception as e:
        print(f"Rules Check failed: {e}")
        raise
