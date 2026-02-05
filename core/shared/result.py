"""
Rust-style Monadic Result Pattern dengan Full Algebraic Data Types
Industrial-grade untuk trading system dengan strong error handling.
"""

import functools
import logging
import asyncio
from typing import (
    Generic, 
    TypeVar, 
    Union, 
    Callable, 
    Awaitable, 
    ParamSpec,
    Optional,
    Any,
    Type
)
from dataclasses import dataclass
from contextlib import contextmanager

# ====================== TYPE DEFINITIONS ======================

T = TypeVar("T")    # Success type
E = TypeVar("E")    # Error type (must be Exception or str)
U = TypeVar("U")    # Return type for transformations
F = TypeVar("F")    # New error type for error transformations
P = ParamSpec("P")  # Function parameters
R = TypeVar("R")    # Return type for async operations

# ====================== ALGEBRAIC RESULT TYPES ======================

@dataclass(frozen=True, slots=True)
class Ok(Generic[T]):
    """
    Success container - immutable monadic value.
    Implements Rust-like Result pattern with full monadic operations.
    """
    value: T
    
    # ========== MONADIC CORE OPERATIONS ==========
    
    def is_ok(self) -> bool:
        return True
    
    def is_err(self) -> bool:
        return False
    
    def unwrap(self) -> T:
        return self.value
    
    def unwrap_err(self) -> None:
        raise ValueError("Cannot unwrap_err on Ok")
    
    def unwrap_or(self, default: T) -> T:
        return self.value
    
    def unwrap_or_else(self, f: Callable[[], T]) -> T:
        return self.value
    
    def expect(self, msg: str) -> T:
        return self.value
    
    def expect_err(self, msg: str) -> None:
        raise ValueError(f"{msg}: Expected Err, got Ok")
    
    # ========== TRANSFORMATION OPERATIONS ==========
    
    def map(self, op: Callable[[T], U]) -> 'Ok[U]':
        """Map the contained value if Ok"""
        return Ok(op(self.value))
    
    def map_err(self, op: Callable[[E], F]) -> 'Ok[T]':
        """No-op for Ok (only applies to Err)"""
        return self
    
    def and_then(self, op: Callable[[T], 'Result[U, E]']) -> 'Result[U, E]':
        """Monadic bind/flatMap operation"""
        return op(self.value)
    
    def or_else(self, op: Callable[[E], 'Result[T, F]']) -> 'Ok[T]':
        """No-op for Ok (only applies to Err)"""
        return self
    
    # ========== COMBINATION OPERATIONS ==========
    
    def and_(self, res: 'Result[U, E]') -> 'Result[U, E]':
        """Returns res if both are Ok, otherwise first Err"""
        return res
    
    def or_(self, res: 'Result[T, F]') -> 'Ok[T]':
        """Returns self (Ok)"""
        return self
    
    # ========== INSPECTION OPERATIONS ==========
    
    def ok(self) -> Optional[T]:
        """Extract Ok value as Option"""
        return self.value
    
    def err(self) -> None:
        """Extract Err value as Option (None for Ok)"""
        return None
    
    def contains(self, value: Any) -> bool:
        """Check if Ok contains given value"""
        return self.value == value
    
    def contains_err(self, error: Any) -> bool:
        """Always False for Ok"""
        return False
    
    # ========== ITERATION ==========
    
    def __iter__(self):
        """Enable pattern matching in for loops"""
        yield self.value
    
    def iter(self):
        """Iterator over Ok value"""
        yield self.value

@dataclass(frozen=True, slots=True)
class Err(Generic[E]):
    """
    Error container - immutable monadic value.
    Implements Rust-like Result pattern with full error handling.
    """
    error: E
    
    # ========== MONADIC CORE OPERATIONS ==========
    
    def is_ok(self) -> bool:
        return False
    
    def is_err(self) -> bool:
        return True
    
    def unwrap(self) -> None:
        raise ValueError(f"Cannot unwrap Err: {self.error}")
    
    def unwrap_err(self) -> E:
        return self.error
    
    def unwrap_or(self, default: T) -> T:
        return default
    
    def unwrap_or_else(self, f: Callable[[], T]) -> T:
        return f()
    
    def expect(self, msg: str) -> None:
        raise ValueError(f"{msg}: {self.error}")
    
    def expect_err(self, msg: str) -> E:
        return self.error
    
    # ========== TRANSFORMATION OPERATIONS ==========
    
    def map(self, op: Callable[[T], U]) -> 'Err[E]':
        """No-op for Err (only applies to Ok)"""
        return self
    
    def map_err(self, op: Callable[[E], F]) -> 'Err[F]':
        """Transform the error value"""
        return Err(op(self.error))
    
    def and_then(self, op: Callable[[T], 'Result[U, E]']) -> 'Err[E]':
        """Short-circuit for Err"""
        return self
    
    def or_else(self, op: Callable[[E], 'Result[T, F]']) -> 'Result[T, F]':
        """Recover from error"""
        return op(self.error)
    
    # ========== COMBINATION OPERATIONS ==========
    
    def and_(self, res: 'Result[U, E]') -> 'Err[E]':
        """Returns self (first Err)"""
        return self
    
    def or_(self, res: 'Result[T, F]') -> 'Result[T, F]':
        """Returns res if self is Err"""
        return res
    
    # ========== INSPECTION OPERATIONS ==========
    
    def ok(self) -> None:
        """Extract Ok value as Option (None for Err)"""
        return None
    
    def err(self) -> Optional[E]:
        """Extract Err value as Option"""
        return self.error
    
    def contains(self, value: Any) -> bool:
        """Always False for Err"""
        return False
    
    def contains_err(self, error: Any) -> bool:
        """Check if Err contains given error"""
        return self.error == error
    
    # ========== ITERATION ==========
    
    def __iter__(self):
        """Empty iterator for Err"""
        return iter(())
    
    def iter(self):
        """Empty iterator"""
        return iter(())

# ====================== RESULT TYPE ALIAS & UTILITIES ======================

Result = Union[Ok[T], Err[E]]

def match_result(
    result: Result[T, E],
    on_ok: Callable[[T], U],
    on_err: Callable[[E], U]
) -> U:
    """
    Pattern matching dengan type safety.
    Modern replacement for if-else chains.
    """
    if isinstance(result, Ok):
        return on_ok(result.value)
    else:
        return on_err(result.error)

# ====================== DECORATORS FOR EXCEPTION HANDLING ======================

def safe(func: Callable[P, T]) -> Callable[P, Result[T, Exception]]:
    """
    Decorator untuk synchronous functions.
    Membungkus exception ke dalam Err monad.
    """
    @functools.wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> Result[T, Exception]:
        try:
            return Ok(func(*args, **kwargs))
        except Exception as e:
            logging.debug(f"Function {func.__name__} failed: {e}")
            return Err(e)
    return wrapper

def safe_async(func: Callable[P, Awaitable[T]]) -> Callable[P, Awaitable[Result[T, Exception]]]:
    """
    Decorator untuk async functions.
    Membungkus exception ke dalam Err monad.
    """
    @functools.wraps(func)
    async def wrapper(*args: P.args, **kwargs: P.kwargs) -> Result[T, Exception]:
        try:
            result = await func(*args, **kwargs)
            return Ok(result)
        except Exception as e:
            logging.debug(f"Async function {func.__name__} failed: {e}")
            return Err(e)
    return wrapper

# ====================== MONADIC COMPOSITION UTILITIES ======================

@dataclass(frozen=True)
class ResultBuilder(Generic[E]):
    """
    Monadic builder untuk chaining Result operations.
    Inspired by Haskell's do-notation.
    """
    
    @staticmethod
    def of(value: T) -> Result[T, E]:
        """Wrap value in Ok"""
        return Ok(value)
    
    @staticmethod
    def fail(error: E) -> Result[Any, E]:
        """Wrap error in Err"""
        return Err(error)
    
    @staticmethod
    def collect(results: list[Result[T, E]]) -> Result[list[T], E]:
        """
        Collect multiple Results.
        Fails fast pada first error.
        """
        collected = []
        for result in results:
            if isinstance(result, Err):
                return result
            collected.append(result.value)
        return Ok(collected)
    
    @staticmethod
    def collect_all(results: list[Result[T, E]]) -> Result[list[T], list[E]]:
        """
        Collect semua Results, kumpulkan semua errors.
        """
        values = []
        errors = []
        for result in results:
            match_result(
                result,
                on_ok=lambda v: values.append(v),
                on_err=lambda e: errors.append(e)
            )
        
        if errors:
            return Err(errors)
        return Ok(values)
    
    @staticmethod
    def sequence(*results: Result[T, E]) -> Result[tuple[T, ...], E]:
        """Sequence multiple Results menjadi tuple"""
        values = []
        for result in results:
            if isinstance(result, Err):
                return result
            values.append(result.value)
        return Ok(tuple(values))

# ====================== ERROR RECOVERY PATTERNS ======================

class RetryConfig:
    """Configuration untuk retry operations"""
    
    def __init__(
        self,
        max_retries: int = 3,
        backoff_factor: float = 1.5,
        max_delay: float = 10.0,
        retry_on: Optional[Type[Exception]] = None
    ):
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.max_delay = max_delay
        self.retry_on = retry_on or Exception

def with_retry(
    config: Optional[RetryConfig] = None
) -> Callable[[Callable[P, Result[T, E]]], Callable[P, Result[T, E]]]:
    """
    Decorator untuk menambahkan retry logic pada Result-returning functions.
    """
    config = config or RetryConfig()
    
    def decorator(func: Callable[P, Result[T, E]]) -> Callable[P, Result[T, E]]:
        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> Result[T, E]:
            last_error = None
            
            for attempt in range(config.max_retries + 1):
                result = func(*args, **kwargs)
                
                if isinstance(result, Ok):
                    return result
                
                last_error = result.error
                
                # Cek apakah error termasuk dalam retryable exceptions
                if not isinstance(last_error, config.retry_on):
                    return result
                
                # Calculate delay with exponential backoff
                if attempt < config.max_retries:
                    delay = min(
                        config.backoff_factor ** attempt,
                        config.max_delay
                    )
                    import time
                    time.sleep(delay)
            
            return Err(last_error)
        
        return wrapper
    
    return decorator

@safe_async
async def with_retry_async(
    func: Callable[P, Awaitable[Result[T, E]]],
    config: Optional[RetryConfig] = None
) -> Callable[P, Awaitable[Result[T, E]]]:
    """
    Async version of with_retry.
    """
    config = config or RetryConfig()
    
    @functools.wraps(func)
    async def wrapper(*args: P.args, **kwargs: P.kwargs) -> Result[T, E]:
        last_error = None
        
        for attempt in range(config.max_retries + 1):
            result = await func(*args, **kwargs)
            
            if isinstance(result, Ok):
                return result
            
            last_error = result.error
            
            # Cek apakah error termasuk dalam retryable exceptions
            if not isinstance(last_error, config.retry_on):
                return result
            
            # Calculate delay dengan exponential backoff
            if attempt < config.max_retries:
                delay = min(
                    config.backoff_factor ** attempt,
                    config.max_delay
                )
                await asyncio.sleep(delay)
        
        return Err(last_error)
    
    return wrapper

# ====================== OPTION TYPE (FOR COMPLETENESS) ======================

@dataclass(frozen=True, slots=True)
class Some(Generic[T]):
    """Some value container (Rust's Some)"""
    value: T
    
    def is_some(self) -> bool:
        return True
    
    def is_none(self) -> bool:
        return False
    
    def unwrap(self) -> T:
        return self.value
    
    def unwrap_or(self, default: T) -> T:
        return self.value
    
    def map(self, f: Callable[[T], U]) -> 'Some[U]':
        return Some(f(self.value))
    
    def and_then(self, f: Callable[[T], 'Option[U]']) -> 'Option[U]':
        return f(self.value)

@dataclass(frozen=True, slots=True)
class _None:
    """None value container (Rust's None)"""
    
    def is_some(self) -> bool:
        return False
    
    def is_none(self) -> bool:
        return True
    
    def unwrap(self) -> None:
        raise ValueError("Cannot unwrap None")
    
    def unwrap_or(self, default: T) -> T:
        return default
    
    def map(self, f: Callable[[T], U]) -> '_None':
        return self
    
    def and_then(self, f: Callable[[T], 'Option[U]']) -> '_None':
        return self

Option = Union[Some[T], _None]
NoneType = _None()

# ====================== ADVANCED MONADIC OPERATIONS ======================

def try_all(*operations: Callable[[], Result[T, E]]) -> Result[list[T], list[E]]:
    """
    Try semua operations, kumpulkan semua results/errors.
    """
    results = []
    errors = []
    
    for op in operations:
        result = op()
        match_result(
            result,
            on_ok=lambda v: results.append(v),
            on_err=lambda e: errors.append(e)
        )
    
    if errors:
        return Err(errors)
    return Ok(results)

def fallback(
    primary: Callable[[], Result[T, E]],
    *fallbacks: Callable[[], Result[T, E]]
) -> Result[T, list[E]]:
    """
    Try primary operation, jika gagal coba fallbacks secara berurutan.
    """
    errors = []
    
    # Try primary
    result = primary()
    if isinstance(result, Ok):
        return result
    errors.append(result.error)
    
    # Try fallbacks
    for fallback_op in fallbacks:
        result = fallback_op()
        if isinstance(result, Ok):
            return result
        errors.append(result.error)
    
    return Err(errors)

# ====================== CONTEXT MANAGER SUPPORT ======================

@contextmanager
def result_context() -> Any:
    """
    Context manager untuk Result operations.
    """
    try:
        yield
    except Exception as e:
        return Err(e)

class ResultContext:
    """Context manager dengan automatic Result wrapping"""
    
    def __enter__(self) -> 'ResultContext':
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        if exc_val:
            return isinstance(exc_val, Exception)
        return True
    
    @staticmethod
    def wrap(func: Callable[P, T]) -> Callable[P, Result[T, Exception]]:
        """Wrap function dalam context manager"""
        @functools.wraps(func)
        def wrapper(*args: P.args, **kwargs: P.kwargs) -> Result[T, Exception]:
            with ResultContext():
                try:
                    return Ok(func(*args, **kwargs))
                except Exception as e:
                    return Err(e)
        return wrapper

# ====================== PERFORMANCE MONITORING ======================

class MonadMetrics:
    """Metrics collection untuk monadic operations"""
    
    def __init__(self):
        self.success_count = 0
        self.error_count = 0
        self.total_operations = 0
    
    def record(self, result: Result[Any, Any]) -> None:
        """Record metrics dari Result"""
        self.total_operations += 1
        if isinstance(result, Ok):
            self.success_count += 1
        else:
            self.error_count += 1
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate"""
        if self.total_operations == 0:
            return 0.0
        return self.success_count / self.total_operations

# ====================== TYPE GUARDS & VALIDATORS ======================

def is_ok(result: Result[Any, Any]) -> bool:
    """Type guard untuk Ok"""
    return isinstance(result, Ok)

def is_err(result: Result[Any, Any]) -> bool:
    """Type guard untuk Err"""
    return isinstance(result, Err)

def as_optional(result: Result[T, E]) -> Optional[T]:
    """Convert Result ke Optional (None jika Err)"""
    return result.ok()

def from_optional(value: Optional[T], error: E) -> Result[T, E]:
    """Convert Optional ke Result"""
    if value is None:
        return Err(error)
    return Ok(value)

# ====================== EXPORTS ======================

__all__ = [
    # Core Monadic Types
    'Result',
    'Ok',
    'Err',
    
    # Option Types
    'Option',
    'Some',
    'NoneType',
    
    # Core Functions
    'match_result',
    'safe',
    'safe_async',
    
    # Builders
    'ResultBuilder',
    
    # Error Recovery
    'RetryConfig',
    'with_retry',
    'with_retry_async',
    
    # Advanced Operations
    'try_all',
    'fallback',
    
    # Context Managers
    'ResultContext',
    'result_context',
    
    # Performance
    'MonadMetrics',
    
    # Type Guards
    'is_ok',
    'is_err',
    'as_optional',
    'from_optional',
    
    # Retry Patterns
    'RetryConfig',
    'with_retry',
    'with_retry_async',
]
