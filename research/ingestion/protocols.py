from typing import List, Protocol
from ..shared import Result, OHLCV, FetchJob

class ExchangeProvider(Protocol):
    async def fetch(self, job: FetchJob) -> Result[List[OHLCV]]:
        ...
    async def close(self):
        ...

class StorageProvider(Protocol):
    async def save(self, data: List[OHLCV], job: FetchJob) -> Result[str, str]:
        ...

