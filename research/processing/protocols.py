import polars as pl
from typing import (
    List, Protocol, runtime_checkable, Dict, Any, Optional, Union
)

from typing import TYPE_CHECKING
if TYPE_CHECKING:
    from ..shared import Result

AlignmentJob = Dict[str, Any]
TransformJob = Dict[str, Any]
ValidationJob = Dict[str, Any]
StorageJob = Dict[str, Any]
ProcessingMetadata = Dict[str, Any]
QualityMetrics = Dict[str, float]
LazyFrameResult = 'Result[pl.LazyFrame, str]'
DataFrameResult = 'Result[pl.DataFrame, str]'
DictResult = 'Result[Dict[str, Any], str]'
ListResult = 'Result[List[Any], str]'
BoolResult = 'Result[bool, Any]'
StrResult = 'Result[str, str]'


# --- 2. CORE PROTOCOLS ---

@runtime_checkable
class TimeSeriesAligner(Protocol):
    """Contract for aligning multi-timeseries."""
    def align(self, data_map: Dict[str, pl.LazyFrame], **kwargs: Any) -> 'Result[pl.LazyFrame, str]':
        ...
    @property
    def method(self) -> str:
        ...

@runtime_checkable
class DataValidator(Protocol):
    """Contract for data validation."""
    def validate(self, data: pl.LazyFrame, rules: Optional[Dict[str, Any]] = None) -> 'Result[pl.LazyFrame, str]':
        ...
    def get_validation_summary(self) -> Dict[str, Any]:
        ...

@runtime_checkable
class FeatureTransformer(Protocol):
    """Contract for Feature Engineering."""
    def transform(self, data: pl.LazyFrame, features: Optional[List[str]] = None) -> 'Result[pl.LazyFrame, str]':
        ...
    @property
    def available_features(self) -> List[str]:
        ...

@runtime_checkable
class RefineryStorage(Protocol):
    """Contract for saving processed data."""
    def save(self, data: Union[pl.LazyFrame, pl.DataFrame], destination: str, **kwargs: Any) -> 'Result[str, str]':
        ...
    def list_saved(self, pattern: Optional[str] = None) -> 'Result[List[str], str]':
        ...

# --- 3. COMPOSITION PROTOCOLS ---

@runtime_checkable
class ProcessingPipeline(Protocol):
    def add_step(self, name: str, processor: Any) -> None: ...
    def execute_multi_asset(self, assets: Dict[str, pl.LazyFrame], **kwargs: Any) -> 'Result[pl.LazyFrame, str]': ...
    def execute_single_asset(self, data: pl.LazyFrame, **kwargs: Any) -> 'Result[pl.LazyFrame, str]': ...
    def get_step_names(self) -> List[str]: ...

@runtime_checkable
class BatchProcessor(Protocol):
    def process_batch(self, jobs: List[Dict[str, Any]], **kwargs: Any) -> 'Result[Dict[str, pl.DataFrame], str]': ...
    def get_progress(self) -> Dict[str, Any]: ...

# --- 4. QUALITY & MONITORING ---

@runtime_checkable
class QualityChecker(Protocol):
    def calculate_metrics(self, data: pl.LazyFrame, metrics: Optional[List[str]] = None) -> 'Result[Dict[str, float], str]': ...

@runtime_checkable
class MetricsStorage(Protocol):
    def store_metrics(self, metrics: Dict[str, Any], tags: Optional[Dict[str, str]] = None) -> 'Result[str, str]': ...
    def query_metrics(self, query: Dict[str, Any]) -> 'Result[pl.DataFrame, str]': ...

# --- 5. UTILITY PROTOCOLS ---

@runtime_checkable
class SchemaProvider(Protocol):
    def get_schema(self, data_type: str) -> 'Result[Dict[str, str], str]': ...
    def validate_schema(self, data: pl.LazyFrame, expected_schema: Dict[str, str]) -> 'Result[bool, str]': ...

@runtime_checkable
class ConfigProvider(Protocol):
    def get_config(self, key: str) -> 'Result[Any, str]': ...

# --- 6. UTILITY FUNCTIONS ---

def is_valid_processor(obj: Any, protocol: Protocol) -> bool:
    return isinstance(obj, protocol)
