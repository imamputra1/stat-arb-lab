from typing import Generic, TypeVar, Union
from dataclasses import dataclass

# Generic Type Var

T = TypeVar("T")
E = TypeVar("E")

@dataclass(frozen=True)
class Ok(Generic[T]):
    value: T

    def is_ok(self) -> bool:
        return True

    def unwrap(self) -> T:
        return self.value

@dataclass(frozen=True)
class Err(Generic[E]):
    error: E

    def is_err(self) -> bool:
        return False

    def unwrap(self):
        raise ValueError(f"panic!, mencoba unwrap err: {self.error}")

Result = Union[Ok[T], Err[E]]
