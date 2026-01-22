from typing import Protocol, List, runtime_checkable
from ..shared import Result, OHLCV, FetchJob
@runtime_checkable
class ExchangeProvider(Protocol):
    async def fetch(self, job: FetchJob) -> Result[List[OHLCV], str]:
        ...

    async def close(self) -> None:
        ...
@runtime_checkable
class StorageProvider(Protocol):

    async def save(self, data: List[OHLCV], job: FetchJob) -> Result[bool, str]:
        ...
