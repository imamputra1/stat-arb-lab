import polars as pl
from typing import List, Protocol, runtime_checkable, Dict, Any, Optional, Union

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from ..shared import Result


# -------| CORE PROTOCOLS |--------
@runtime_checkable
class TimeSeriesAligner(Protocol):
    """
    Contract for aligning multi-timeseries:
    Single responsibility - just align timeseries, No validation
    """
    def align(
        self,
        data_map: Dict[str, pl.LazyFrame],
        **kwargs: Any
    ) -> 'Result[pl.LazyFrame, str]':
        """
        Align multi-timeseries to comman timestamps.

        Args:
            data_map: Dict with symbol -> LazyFrame (must have 'timestamps' column)
            ** kwargs: optional alignment parameters (method, tolerance, etc)

        Return:
            Result[pl.LazyFrame, str]: single alignment LazyFrame or Error
        """
        ...

    @property
    def method(self) -> str:
        """ Alignment method description (for debugging/Monitoring)"""
        ...

@runtime_checkable
class Data_Validator(Protocol):
    """
    Contract for data validation:
    Optional Layer - only validate if business logic requires it.
    """
    def validate(
        self,
        data: pl.LazyFrame,
        rules: Optional[Dict[str, Any]] = None
    ) -> 'Result[pl.LazyFrame, str]':
        """
        validate data quality and business rules.

        Args:
            data: LazyFrame
            rules: Optional Validate rules Dict

        Return:
            Result[pl.LazyFrame, str]: validated LazyFrame or Error
        """
        ...

    def get_validation_summary(self) -> Dict[str, Any]:
        """ Get Validate Statistics (passed/failed counts)"""
        ...

@runtime_checkable
class FeatureTransformer(Protocol):
    """
    Contract for Feature Engineering:
    Pure functions preferred, stateful only if necessary.
    """
    def transform(
        self,
        data: pl.LazyFrame,
        features: Optional[List[str]] = None
    ) -> 'Result[pl.LazyFrame, str]':
        """
        Transform data to create features.

        Args:
            data: Input LazyFrame
            features: Optional list of  spesific features

        Return:
            Result[pl.LazyFrame, str]: Transformed LazyFrame or Error
        """
        ...

    @property
    def available_features(self) -> List[str]:
        """ List of available transformations"""
        ...

@runtime_checkable
class RefineryStorage(Protocol):
    """
    Contract for saving processed data (Silver layer):
    Simple data interface, let implementation handle complexity
    """
    def save(
        self,
        data: Union[pl.LazyFrame, pl.DataFrame],
        destination: str,
        **kwargs: Any
    ) -> 'Result[str, str]':
        """
        Save Processed data to Storage
        Save Processed data. if LazyFrame, will collect() before saving.
        
        Args:
            data: DataFrame anda LazyFrame
            destination: Path or identifier for Storage
            kwargs: Storage-spesific optional(comporession, partitioning, etc)

        Return: 
           Result[str, str]: Storage Path/identifier or Error
        """
        ...

    def list_saved(
        self,
        pattern: Optional[str] = None
    ) -> 'Result[List[str], str]':
        """ List available saved datasets"""
        ...

# --------| Composition Protocols |-------

@runtime_checkable
class ProcessingPipeline(Protocol):
    """
    Protocol for Composing multiple procesing steps:
    Black-Box the complexity, expose simple interface
    """

    def add_step(
        self,
        name: str,
        processor: Any
    ) -> None:
        """ Add processing step to pipeline"""
        ...

    def execute_multi_asset(
        self,
        assets: Dict[str, pl.LazyFrame],
        **kwargs: Any
    ) -> 'Result[pl.LazyFrame, str]':
        """
        Execute pipeline for multi assets (pairs)

        Args:
            assets: input data multiple asset (raw or already aligned)
            **kwargs: pipeline-specific parameters

        Return:
            Result[pl.LazyFrame, str]: Processed data or Error
        """
        ...

    def execute_single_asset(
        self, 
        data: pl.LazyFrame,
        **kwargs: Any
    ) -> 'Result[pl.LazyFrame, str]':
        """
        Execute pipeline for single data assets

        Args:
            data: pl.LazyFrame already aligned/single symbol
            **kwargs: pipeline-specific parameters

        Return:
            Result[pl.LazyFrame, str]: Processed data or Error
        """
        ...

    def get_step_names(self) -> List[str]:
        """ Get list of step names in pipeline"""
        ...

@runtime_checkable
class BatchProcessor(Protocol):
    """
    Protocol for batch processor of multiple Symbols/timeframes:
    Handle complexity of multiple jobs.
    """
    def process_batch(
        self,
        jobs: List[Dict[str, Any]],
        **kwargs: Any
    ) -> 'Result[Dict[str, pl.DataFrame], str]':
        """
        Proces multiple jobs in batch.

        Args:
            jobs: List of  job spesification
            **kwargs: BatchProcessor parameters

        Return:
            Result[Dict[str, pl.DataFrame], str]: dict symbols -> processed DataFrame
        """
        ...

    def get_progress(self) -> Dict[str, Any]:
        """ Get current batch processing progress"""
        ...

# --------| QUALITY AND MONITORING PROTOCOLS|--------
@runtime_checkable
class QualityChecker(Protocol):
    """
    Protocol for data quality metrics:
    separate concerns: quality checking vs validation
    """

    def calculate_matrics(
        self,
        data: pl.LazyFrame,
        metrics: Optional[List[str]] = None
    ) -> 'Result[Dict[str, float], str]':
        """
        calculate data quality metrics.

        Args:
            data: Data to analyze
            metrics: Optional of spesific metrics

        Return:
            Result[Dict[str, float], str]: metric name -> value dict
        """
        ...

@runtime_checkable
class MetricsStorage(Protocol):
    """
    protocol for storing processing metrics:
    keep metrics separate from data
    """
    def store_matrics(
        self,
        metrics: Dict[str, Any],
        tags: Optional[Dict[str, str]] = None
    ) -> 'Result[str, str]':
        """ Store Processing metrics"""
        ...

    def query_metrics(
        self,
        query: Dict[str, Any]
    ) -> 'Result[pl.DataFrame, str]':
        """Query stored metrics"""
        ...

# ---------| Utility Protocol |---------
@runtime_checkable
class SchemaProveder(Protocol):
    """
    Protocol for providing expected schema:
    keep schemas separate from precessing logic
    """
    def get_schema(
        self,
        data_type: str
    ) -> 'Result[Dict[str, str], str]':
        """ Get expected Schema for data type"""
        ...

    def validate_schema(
        self,
        data: pl.LazyFrame,
        expected_schema: Dict[str, str]
    ) -> 'Result[bool, str]':
        """Validate data against expected schema"""
        ...

@runtime_checkable
class ConfigProvider(Protocol):
    """
    protocol for providing configuration:
    separate config from business logic
    """
    def get_config(
        self,
        key: str
    ) -> 'Result[Any, str]':
        """Get config value"""
        ...

# --------| TYPE GUARDS AN UTILITIES |--------

def is_valid_processor(
        obj: Any, 
        protocol: Protocol
) -> bool:
    """ 
    Runtime check for protocol complience:
    simple validation without conplex dependencies
    """
    return isinstance(obj, protocol)

def create_processing_result(
    data: pl.LazyFrame,
    metadata: Optional[Dict[str, Any]] = None
    ) -> 'Result[pl.LazyFrame, str]':
    """ Helper for creating successfull processing results"""
    from ..shared import Ok

    if metadata:
        setattr(data, '_metadata', metadata)

    return Ok(data)

def creating_processing_error(
    self,
    error_msg: str,
    context: Optional[Dict[str, Any]] = None
) ->'Result[Any, str]':
    """
    Helper for create processing errors.
    rich error context for debugging
    """
    from ..shared import Err

    if context:
        error_msg = f"{error_msg} | context: {context}"

    return Err(error_msg)

# --------| TYPE ALIASES (self documenting)|---------
# Data type
LazyFrameResult = 'Result[pl.LazyFrame, str]'
DataFrameResult = 'Result[pl.DataFrame, str]'
DictResult = 'Result[Dict[str, Any], str]'
ListResutl = 'Result[List[Any], str]'
BoolResult = 'Result[bool, Any]'
StrResult = 'Result [str, str]'

# jobs type
AlignmentJob = Dict[str, Any]
TransformJob = Dict[str, Any]
ValidationsJob = Dict[str, Any]
StorageJobe = Dict[str, Any]

# metadata types
ProcessingMetadata = Dict[str, Any]
QualityMatrics = Dict[str, float]

