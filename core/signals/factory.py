"""
SIGNAL FACTORY & REGISTRY - The Assembly Line
Location: core/signals/factory.py
Role: Dynamic strategy instantiation with monadic error handling and protocol-based assembly
"""

import inspect
import logging
from typing import (
    Dict, 
    Any, 
    Type, 
    Optional, 
    List, 
    Union, 
    Callable,
    get_type_hints
)
from functools import lru_cache
from enum import Enum
from dataclasses import dataclass, fields, is_dataclass, asdict
from core.shared.result import (
    Result, 
    Ok, 
    Err
)
# Import Protocol interfaces
from core.signals.base_signal import BaseStrategy 
from core.signals.types import SignalConfig

# Import available strategies
from core.signals.strategies.kalman_mr import KalmanMeanReversion

# Import math configurations
from core.math.kalman import KalmanConfig, AdaptationMode

# ====================== TYPES & INTERFACES ======================

class StrategyType(Enum):
    """Registered strategy types - extend as needed"""
    KALMAN_MEAN_REVERSION = "kalman_mr"
    KALMAN_TREND_FOLLOWING = "kalman_tf"
    RSI_MOMENTUM = "rsi_momentum"
    BOLLINGER_MEAN_REVERSION = "bollinger_mr"
    
    @classmethod
    def from_str(cls, value: str) -> 'StrategyType':
        """Convert string to StrategyType with fallback"""
        try:
            return cls(value.lower())
        except ValueError:
            # Try case-insensitive match
            for member in cls:
                if member.value.lower() == value.lower():
                    return member
            raise ValueError(f"Unknown strategy type: {value}")

@dataclass(frozen=True)
class AssemblySpec:
    """Specification for strategy assembly"""
    strategy_type: StrategyType
    signal_config: SignalConfig
    math_config: Optional[Any] = None
    metadata: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.metadata is None:
            object.__setattr__(self, 'metadata', {})

# ====================== REGISTRY SYSTEM ======================

class StrategyRegistry:
    """
    Dynamic Strategy Registry with Protocol-based validation.
    Acts as the 'Menu System' for available strategies.
    """
    
    _registry: Dict[StrategyType, Type[BaseStrategy]] = {}
    _config_types: Dict[StrategyType, Type] = {}
    _builder_functions: Dict[StrategyType, Callable[[Any, SignalConfig], BaseStrategy]] = {}
    
    # ========== REGISTRATION METHODS ==========
    
    @classmethod
    def register(
        cls, 
        strategy_type: Union[StrategyType, str],
        strategy_class: Type[BaseStrategy],
        math_config_type: Optional[Type] = None,
        builder: Optional[Callable[[Any, SignalConfig], BaseStrategy]] = None
    ) -> Result[None, str]:
        """
        Register a strategy class with its configuration types.
        
        Args:
            strategy_type: Strategy type enum or string
            strategy_class: The strategy class to register
            math_config_type: Type of math configuration (if any)
            builder: Optional custom builder function
        """
        try:
            # Convert string to StrategyType
            if isinstance(strategy_type, str):
                strategy_type = StrategyType.from_str(strategy_type)
            
            # Validate strategy class
            if not (inspect.isclass(strategy_class) and issubclass(strategy_class, BaseStrategy)):
                return Err(f"Invalid strategy class: {strategy_class}")
            
            # Register
            cls._registry[strategy_type] = strategy_class
            
            if math_config_type:
                cls._config_types[strategy_type] = math_config_type
            
            if builder:
                cls._builder_functions[strategy_type] = builder
            
            logging.info(f"Registered strategy: {strategy_type.value} -> {strategy_class.__name__}")
            return Ok(None)
            
        except Exception as e:
            return Err(f"Registration failed for {strategy_type}: {str(e)}")
    
    @classmethod
    def get_strategy_class(cls, strategy_type: Union[StrategyType, str]) -> Result[Type[BaseStrategy], str]:
        """Get strategy class by type"""
        try:
            if isinstance(strategy_type, str):
                strategy_type = StrategyType.from_str(strategy_type)
            
            if strategy_type not in cls._registry:
                return Err(f"Strategy type '{strategy_type.value}' not registered")
            
            return Ok(cls._registry[strategy_type])
        except ValueError as e:
            return Err(str(e))
    
    @classmethod
    def get_math_config_type(cls, strategy_type: StrategyType) -> Optional[Type]:
        """Get math config type for strategy"""
        return cls._config_types.get(strategy_type)
    
    @classmethod
    def has_builder(cls, strategy_type: StrategyType) -> bool:
        """Check if strategy has custom builder"""
        return strategy_type in cls._builder_functions
    
    @classmethod
    def get_builder(cls, strategy_type: StrategyType) -> Optional[Callable]:
        """Get custom builder function"""
        return cls._builder_functions.get(strategy_type)
    
    @classmethod
    def list_registered(cls) -> Dict[str, str]:
        """List all registered strategies"""
        return {
            stype.value: sclass.__name__
            for stype, sclass in cls._registry.items()
        }
    
    @classmethod
    def validate_protocol(cls, strategy_instance: BaseStrategy) -> Result[None, str]:
        """Validate strategy implements required protocol"""
        required_methods = ['generate_signals', 'evaluate_state', 'get_state', 'reset']

        for method in required_methods:
            if not hasattr(strategy_instance, method):
                return Err(f"Missing required method: {method}")
        
        return Ok(None)

# ====================== CONFIG PARSER ======================

class ConfigParser:
    """
    Intelligent configuration parser with schema validation.
    Converts raw dictionaries to typed configuration objects.
    """
    
    @staticmethod
    def parse_signal_config(raw_signal: Dict[str, Any]) -> Result[SignalConfig, str]:
        """
        Parse raw dictionary to SignalConfig with validation.
        Hanya mengurusi Parameter Sinyal (Alpha). 
        Urusan Risk Management (Drawdown, Fee) ada di module terpisah.
        """
        try:
            # 1. Extract Identity
            name = raw_signal.get('name', 'Unnamed_Strategy')
            
            # 2. Mapping Key: "Istilah Manusia" -> "Istilah Sistem (types.py)"
            # Ini memungkinkan config file pake istilah 'entry_threshold' 
            # tapi sistem tetap baca sebagai 'entry_z_score'.
            field_mapping = {
                # --- THRESHOLDS (Logic Alpha) ---
                'entry_z_score': 'entry_z_score',  
                'entry_threshold': 'entry_z_score', # Alias umum
                
                'exit_z_score': 'exit_z_score',
                'exit_threshold': 'exit_z_score',   # Alias umum
                
                'stop_loss_z': 'stop_loss_z',
                'stop_loss': 'stop_loss_z',         # Alias umum
                
                # --- POSITIONING (Sizing Dasar) ---
                'max_position': 'max_position',
                'hedge_ratio': 'hedge_ratio',       # Vital untuk Pair Trading
                
                # --- METADATA ---
                'version': 'version'
            }
            
            # 3. Build Config Dictionary
            config_data = {'name': name}
            
            for raw_key, cls_key in field_mapping.items():
                if raw_key in raw_signal:
                    try:
                        val = raw_signal[raw_key]
                        
                        # Type Casting (Safety)
                        # Kecuali version (string), sisanya float
                        if cls_key != 'version':
                            val = float(val)
                            
                        config_data[cls_key] = val
                        
                    except (ValueError, TypeError) as e:
                        return Err(f"Invalid value for parameter '{raw_key}': {e}")

            # 4. Create Object (Fail Fast jika ada field wajib yang hilang)
            # SignalConfig ada di core.signals.types
            try:
                # Filter hanya argumen yang dikenal oleh SignalConfig
                # (Mencegah error jika config file punya key sampah)
                valid_fields = {f.name for f in fields(SignalConfig)}
                filtered_data = {k: v for k, v in config_data.items() if k in valid_fields}
                
                signal_config = SignalConfig(**filtered_data)
                
            except TypeError as e:
                return Err(f"SignalConfig creation failed. Missing required fields? Error: {e}")
            
            # 5. Logic Validation (Entry harus > Exit, dll)
            validation_result = signal_config.validate()
            if validation_result.is_err():
                return validation_result
            
            return Ok(signal_config)
            
        except Exception as e:
            return Err(f"Parser crash: {str(e)}")

    @staticmethod
    def parse_kalman_config(raw_math: Dict[str, Any]) -> Result[KalmanConfig, str]:
        """
        Parse raw dictionary to KalmanConfig.
        
        Args:
            raw_math: Raw math parameters for Kalman filter
            
        Returns:
            Result[KalmanConfig, str]
        """
        try:
            # Required parameters with validation
            required_params = ['R', 'Q', 'initial_value']
            for param in required_params:
                if param not in raw_math:
                    return Err(f"Missing required parameter: {param}")
            
            # Prepare config data
            config_data = {
                'R': float(raw_math['R']),
                'Q': float(raw_math['Q']),
                'initial_value': float(raw_math['initial_value']),
            }
            
            # Optional parameters with defaults
            optional_params = {
                'state_dim': 2,
                'shock_threshold': 4.0,
                'max_boost_factor': 10.0,
                'min_lambda': 0.8,
                'max_lambda': 1.0,
            }
            
            for key, default in optional_params.items():
                config_data[key] = float(raw_math.get(key, default))
            
            # Parse adaptation mode
            adapt_mode_str = raw_math.get('adaptation_mode', 'nis')
            try:
                adapt_mode = AdaptationMode(adapt_mode_str.lower())
                config_data['adaptation_mode'] = adapt_mode
            except ValueError:
                return Err(f"Invalid adaptation mode: {adapt_mode_str}")
            
            # Create KalmanConfig
            kalman_config = KalmanConfig(**config_data)
            
            # Validate numeric ranges
            if kalman_config.R <= 0 or kalman_config.Q <= 0:
                return Err("R and Q must be positive")
            
            if not (0 < kalman_config.min_lambda <= kalman_config.max_lambda <= 1):
                return Err("Lambda values must be between 0 and 1")
            
            return Ok(kalman_config)
            
        except Exception as e:
            return Err(f"Failed to parse Kalman config: {str(e)}")
    
    @staticmethod
    def parse_generic_config(raw_config: Dict[str, Any], config_type: Type) -> Result[Any, str]:
        """
        Generic config parser using type hints and dataclass fields.
        
        Args:
            raw_config: Raw configuration dictionary
            config_type: Target configuration class type
            
        Returns:
            Result[config_type, str]
        """
        try:
            # Get type hints from config class
            type_hints = get_type_hints(config_type)
            
            # Get dataclass fields if applicable
            dataclass_fields = {}
            if is_dataclass(config_type):
                dataclass_fields = {f.name: f.type for f in fields(config_type)}
            
            # Merge type information
            field_types = {**type_hints, **dataclass_fields}
            
            # Convert and validate each field
            converted_data = {}
            
            for field_name, field_type in field_types.items():
                if field_name in raw_config:
                    raw_value = raw_config[field_name]
                    
                    try:
                        # Handle Enum types
                        if inspect.isclass(field_type) and issubclass(field_type, Enum):
                            if isinstance(raw_value, str):
                                # Try to get enum by name
                                enum_value = field_type[raw_value.upper()]
                            else:
                                # Try to get enum by value
                                enum_value = field_type(raw_value)
                            converted_data[field_name] = enum_value
                        
                        # Handle basic type conversion
                        elif field_type == float:
                            converted_data[field_name] = float(raw_value)
                        elif field_type == int:
                            converted_data[field_name] = int(raw_value)
                        elif field_type == str:
                            converted_data[field_name] = str(raw_value)
                        elif field_type == bool:
                            converted_data[field_name] = bool(raw_value)
                        else:
                            # For complex types, try direct assignment
                            converted_data[field_name] = raw_value
                    
                    except (ValueError, TypeError, KeyError) as e:
                        return Err(f"Failed to convert field '{field_name}': {str(e)}")
            
            # Create config instance
            config_instance = config_type(**converted_data)
            return Ok(config_instance)
            
        except Exception as e:
            return Err(f"Failed to parse generic config: {str(e)}")

# ====================== ASSEMBLY FACTORY ======================

class SignalFactory:
    """
    Strategy Assembly Factory with monadic error handling.
    Responsible for parsing configurations and instantiating strategies.
    """
    
    def __init__(self):
        self.parser = ConfigParser()
        self.performance_log: List[Dict[str, Any]] = []

    
    def create_from_spec(self, spec: AssemblySpec) -> Result[BaseStrategy, str]:
        """
        Create strategy from AssemblySpec (typed configuration).
        
        Args:
            spec: Assembly specification with typed configs
            
        Returns:
            Result[BaseStrategy, str]
        """
        try:
            # Get strategy class
            strategy_class_result = StrategyRegistry.get_strategy_class(spec.strategy_type)
            if strategy_class_result.is_err():
                return strategy_class_result.map(lambda _: None)
            
            strategy_class = strategy_class_result.unwrap()
            
            # Check for custom builder
            builder = StrategyRegistry.get_builder(spec.strategy_type)
            
            if builder:
                # Use custom builder
                strategy = builder(spec.math_config, spec.signal_config)
            else:
                # Standard instantiation
                if spec.math_config is not None:
                    strategy = strategy_class(spec.math_config, spec.signal_config)
                else:
                    strategy = strategy_class(spec.signal_config)
            
            # Validate protocol implementation
            protocol_result = StrategyRegistry.validate_protocol(strategy)
            if protocol_result.is_err():
                return protocol_result.map(lambda _: None)
            
            # Record assembly
            self._record_assembly(spec, strategy)
            
            return Ok(strategy)
            
        except Exception as e:
            return Err(f"Assembly failed: {str(e)}")
    
    def create_from_raw(self, raw_config: Dict[str, Any]) -> Result[BaseStrategy, str]:
        """
        Create strategy from raw configuration dictionary.
        
        Expected structure:
        {
            "type": "kalman_mr",
            "name": "StrategyName",
            "signal_params": { ... },
            "math_params": { ... },
            "metadata": { ... }
        }
        
        Args:
            raw_config: Raw configuration dictionary
            
        Returns:
            Result[BaseStrategy, str]
        """
        try:
            # Extract configuration parts
            strategy_type_str = raw_config.get('type')
            if not strategy_type_str:
                return Err("Missing 'type' in configuration")
            
            # Parse strategy type
            try:
                strategy_type = StrategyType.from_str(strategy_type_str)
            except ValueError as e:
                return Err(str(e))
            
            # Parse signal configuration
            raw_signal = raw_config.get('signal_params', {})
            signal_config_result = self.parser.parse_signal_config(raw_signal)
            if signal_config_result.is_err():
                return signal_config_result.map(lambda _: None)
            
            signal_config = signal_config_result.unwrap()
            
            # Parse math configuration based on strategy type
            raw_math = raw_config.get('math_params', {})
            math_config_result = self._parse_math_config(strategy_type, raw_math)
            if math_config_result.is_err():
                return math_config_result.map(lambda _: None)
            
            math_config = math_config_result.unwrap()
            
            # Create assembly spec
            spec = AssemblySpec(
                strategy_type=strategy_type,
                signal_config=signal_config,
                math_config=math_config,
                metadata=raw_config.get('metadata', {})
            )
            
            # Assemble strategy
            return self.create_from_spec(spec)
            
        except Exception as e:
            return Err(f"Failed to create strategy from raw config: {str(e)}")

    def validate_config(self, raw_config: Dict[str, Any]) -> Result[Dict[str, Any], str]:
        """
        Validate configuration by attempting to parse it without instantiation.
        Returns the valid raw_config if successful.
        """
        try:
            # 1. Check Type
            type_str = raw_config.get('type')
            if not type_str:
                return Err("Missing 'type'")
            
            try:
                strat_type = StrategyType.from_str(type_str)
            except ValueError as e:
                return Err(str(e))

            # 2. Dry Run Parsing
            # Parse Signal Params
            sig_res = self.parser.parse_signal_config(raw_config.get('signal_params', {}))
            if sig_res.is_err():
                return Err(f"Signal Param Error: {sig_res.unwrap_err()}")

            # Parse Math Params
            math_res = self._parse_math_config(strat_type, raw_config.get('math_params', {}))
            if math_res.is_err():
                return Err(f"Math Param Error: {math_res.unwrap_err()}")

            # Jika sampai sini berarti Config Valid
            return Ok(raw_config)

        except Exception as e:
            return Err(f"Validation exception: {str(e)}")
    
    def _parse_math_config(self, strategy_type: StrategyType, raw_math: Dict[str, Any]) -> Result[Any, str]:
        """Parse math configuration based on strategy type"""
        # Get math config type from registry
        math_config_type = StrategyRegistry.get_math_config_type(strategy_type)
        
        if math_config_type is None:
            # No math config required for this strategy type
            return Ok(None)
        
        # Use appropriate parser based on config type
        if math_config_type == KalmanConfig:
            return self.parser.parse_kalman_config(raw_math)
        else:
            # Generic parser for other config types
            return self.parser.parse_generic_config(raw_math, math_config_type)
    
    def _record_assembly(self, spec: AssemblySpec, strategy: BaseStrategy) -> None:
        """Record assembly event for monitoring"""
        record = {
            'timestamp': inspect.currentframe().f_back.f_lineno,  # Placeholder
            'strategy_type': spec.strategy_type.value,
            'strategy_name': strategy.name,
            'signal_config': asdict(spec.signal_config),
            'math_config': asdict(spec.math_config) if spec.math_config else None,
            'metadata': spec.metadata
        }
        self.performance_log.append(record)
    
    def get_assembly_log(self) -> List[Dict[str, Any]]:
        """Get assembly performance log"""
        return self.performance_log.copy()
    
    @staticmethod
    @lru_cache(maxsize=128)
    def get_default_config(strategy_type: Union[StrategyType, str]) -> Result[Dict[str, Any], str]:
        """
        Get default configuration for strategy type.
        Useful for UI/config generation.
        
        Args:
            strategy_type: Strategy type enum or string
            
        Returns:
            Result[Dict[str, Any], str]
        """
        try:
            if isinstance(strategy_type, str):
                try:
                    strategy_type = StrategyType.from_str(strategy_type)
                except ValueError:
                    # [FIX] Syntax Error Fixed (kurung kurawal masuk ke string)
                    return Err(f"Unknown strategy type string: {strategy_type}")        
            defaults = {
                StrategyType.KALMAN_MEAN_REVERSION: {
                    'type': 'kalman_mr',
                    'name': 'Kalman_MR_Default',
                    'signal_params': {
                        'entry_z_score': 2.0,
                        'exit_z_score': 0.5,
                        'max_position': 4.0,
                        'stop_loss_z': 0.05,
                        'hedge_ratio': 1.0,
                    },
                    'math_params': {
                        'R': 0.001,
                        'Q': 0.0001,
                        'initial_value': 0.0,
                        'adaptation_mode': 'nis',
                        'state_dim': 2,
                    }
                },
                # Add defaults for other strategy types
                # Placeholder untuk strategi masa depan (misal: RSI, MACD)
                # StrategyType.RSI_MOMENTUM: { ... }
            }
            
            if strategy_type not in defaults:
                return Err(f"No default configuration for {strategy_type.value}")
            
            return Ok(defaults[strategy_type].copy())
            
        except Exception as e:
            return Err(f"Failed to get default config: {str(e)}")

# ====================== BUILDER FUNCTIONS ======================

def build_kalman_mean_reversion(math_config: KalmanConfig, signal_config: SignalConfig) -> KalmanMeanReversion:
    """
    Custom builder for Kalman Mean Reversion strategy.
    Allows for custom initialization logic.
    """
    return KalmanMeanReversion(math_config, signal_config)

# ====================== FACTORY MANAGER (SINGLETON) ======================

class FactoryManager:
    """
    Singleton manager for strategy factory operations.
    Provides high-level interface for strategy lifecycle management.
    """
    
    _instance: Optional['FactoryManager'] = None
    _factory: Optional[SignalFactory] = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._factory = SignalFactory()
            cls._instance._initialize_registry()
        return cls._instance
    
    def _initialize_registry(self):
        """Initialize strategy registry with available strategies"""
        # Register Kalman Mean Reversion
        StrategyRegistry.register(
            strategy_type=StrategyType.KALMAN_MEAN_REVERSION,
            strategy_class=KalmanMeanReversion,
            math_config_type=KalmanConfig,
            builder=build_kalman_mean_reversion
        )
    
    @property
    def factory(self) -> SignalFactory:
        """Get factory instance"""
        return self._factory
    
    # [FIX] Method ini sekarang mendelagasikan tugas ke self.factory.create_from_raw
    # Tidak perlu tulis ulang logic parsing di sini (DRY Principle)
    @staticmethod
    def create_strategy(raw_config: Dict[str, Any]) -> Result[BaseStrategy, str]:
        """
        [THE ASSEMBLER]
        Merakit object Strategy dari Raw Dictionary.
        """
        try:
            # 1. Identifikasi Tipe Strategi
            strat_type = raw_config.get("type")
            if not strat_type:
                # [FIX 2] Samakan pesan error persis dengan ekspektasi Test
                return Err("Missing required field 'type' in configuration")
            
            # Cek di Registry
            cls_result = StrategyRegistry.get_strategy_class(strat_type)
            if cls_result.is_err():
                return Err(f"Unknown strategy type: '{strat_type}'. Did you register it?")
            
            strategy_cls = cls_result.unwrap()

            # 2. Rakit Signal Config (Generic)
            factory = FactoryManager().factory
            
            # [FIX 1] INJECT NAME LOGIC
            # Ambil 'signal_params', lalu suntikkan 'name' dari top-level config
            sig_params = raw_config.get("signal_params", {}).copy()
            
            # Prioritas: Name di signal_params > Name di top-level > Default
            if "name" not in sig_params:
                sig_params["name"] = raw_config.get("name", f"Unnamed_{strat_type}")
            
            # Kirim ke parser (sekarang sudah bawa nama)
            sig_result = factory.parser.parse_signal_config(sig_params)
            if sig_result.is_err():
                return Err(f"Signal Config Error: {sig_result.unwrap_err()}")
            
            sig_conf = sig_result.unwrap()

            # 3. Rakit Math Config & Final Assembly
            math_params = raw_config.get("math_params", {}).copy()
            
            try:
                strat_enum = StrategyType.from_str(strat_type)
            except ValueError:
                 return Err(f"Invalid strategy type string: {strat_type}")

            math_result = factory._parse_math_config(strat_enum, math_params)
            if math_result.is_err():
                return Err(f"Math Config Error: {math_result.unwrap_err()}")
                
            math_conf = math_result.unwrap()
            
            # 4. Final Instantiation
            if StrategyRegistry.has_builder(strat_enum):
                builder = StrategyRegistry.get_builder(strat_enum)
                strategy = builder(math_conf, sig_conf)
            else:
                strategy = strategy_cls(math_conf, sig_conf)
                
            return Ok(strategy)

        except Exception as e:
            return Err(f"Factory failed to assemble strategy: {str(e)}")
    # [FIX] Method ini yang dicari-cari oleh Linter!
    @staticmethod
    def validate_config(raw_config: Dict[str, Any]) -> Result[Dict[str, Any], str]:
        """Delegate validation to the singleton factory instance"""
        return FactoryManager().factory.validate_config(raw_config)


# ====================== QUICK ACCESS FUNCTIONS ======================

def get_factory() -> SignalFactory:
    """Get factory instance (singleton pattern)"""
    return FactoryManager().factory

def create_strategy(raw_config: Dict[str, Any]) -> Result[BaseStrategy, str]:
    """Quick function to create strategy from raw config"""
    return FactoryManager().create_strategy(raw_config)

def validate_config(raw_config: Dict[str, Any]) -> Result[Dict[str, Any], str]:
    """Quick function to validate configuration"""
    return FactoryManager().validate_config(raw_config)

def list_available_strategies() -> Dict[str, str]:
    """List all registered strategies"""
    return StrategyRegistry.list_registered()

def get_default_config(strategy_type: str) -> Result[Dict[str, Any], str]:
    """Get default configuration for strategy type"""
    return SignalFactory.get_default_config(strategy_type)

# ====================== EXPORTS ======================

__all__ = [
    # Core Types
    'StrategyType',
    'AssemblySpec',
    
    # Registry
    'StrategyRegistry',
    
    # Parser
    'ConfigParser',
    
    # Factory
    'SignalFactory',
    'FactoryManager',
    
    # Builders
    'build_kalman_mean_reversion',
    
    # Quick Access
    'get_factory',
    'create_strategy',
    'validate_config',
    'list_available_strategies',
    'get_default_config',
]
