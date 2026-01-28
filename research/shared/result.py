import functools
import logging
from typing import (
    Generic, 
    TypeVar, 
    Union, 
    Callable, 
    Awaitable, 
    ParamSpec
)
from dataclasses import dataclass
T = TypeVar("T")  # Tipe Sukses
E = TypeVar("E")  # Tipe Error
U = TypeVar("U")  # Tipe Return untuk Match
P = ParamSpec("P") # Tipe Parameter (Args/Kwargs) untuk Decorator


@dataclass(frozen=True)
class Ok(Generic[T]):
    value: T

    def is_ok(self) -> bool:
        return True
    def is_err(self) -> bool:
        return False
    def unwrap(self) -> T:
        return self.value

@dataclass(frozen=True)
class Err(Generic[E]):
    error: E

    def is_ok(self) -> bool:
        return False
    def is_err(self) -> bool:
        return True  
    def unwrap(self):
        raise ValueError(f"Panic! Mencoba unwrap Err: {self.error}")

# Type Alias untuk Result
Result = Union[Ok[T], Err[E]]

# ====================== DECORATORS & UTILS ======================

def safe_async(func: Callable[P, Awaitable[T]]) -> Callable[P, Awaitable[Result[T, str]]]:
    """
    Decorator untuk membungkus fungsi ASYNC agar return Result.
    Menangani Exception dan mengubahnya menjadi Err(str).
    """
    @functools.wraps(func)
    async def wrapper(*args: P.args, **kwargs: P.kwargs) -> Result[T, str]:
        try:
            result = await func(*args, **kwargs)
            return Ok(result)
        except Exception as e:
            logging.error(f"Async Function {func.__name__} failed: {e}")
            return Err(str(e))
    return wrapper


def match_result(  # FIX: Rename from 'match' to 'match_result'
    result: Result[T, E],
    on_ok: Callable[[T], U],
    on_err: Callable[[E], U]
) -> U:
    """
    Pattern matching yang Type-Safe.
    Renamed to match_result to avoid conflict with Python 3.10 keyword 'match'.
    """
    if isinstance(result, Ok):
        return on_ok(result.value)
    elif isinstance(result, Err):
        return on_err(result.error)
    else:
        raise TypeError(f"Unknown Result type: {type(result)}")

"""
def match(
    result: Result[T, E],
    on_ok: Callable[[T], U],
    on_err: Callable[[E], U]
) -> U:
    Pattern matching yang Type-Safe.
    Menggunakan isinstance agar Linter paham (Type Narrowing).
    
    if isinstance(result, Ok):
        return on_ok(result.value)
    elif isinstance(result, Err):
        return on_err(result.error)
    else:
        # Should be unreachable if types are correct
        raise TypeError(f"Unknown Result type: {type(result)}")
"""
