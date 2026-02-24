"""
SYSTEM MONITOR (THE RADIOMAN)
Location: live/guardian/monitor.py
Desc: Monitors system health: WebSocket heartbeat, API rate limits, event loop lag.
      Sends alerts before catastrophic failure.
"""

import asyncio
import time
import logging
from typing import Optional, Callable, Awaitable

logger = logging.getLogger(__name__)


class SystemMonitor:
    """
    Background task that monitors:
    - WebSocket heartbeat (last message time)
    - Binance API weight usage (via response headers)
    - Event loop lag (execution time of main loop)
    """

    def __init__(
        self,
        check_interval: float = 1.0,           # how often to run checks
        ws_timeout: float = 10.0,               # max seconds without ws data
        api_weight_limit: int = 1200,            # per minute
        api_warning_threshold: float = 0.8,      # 80% of limit
        loop_lag_warning_ms: float = 5.0,        # warn if loop > 5ms
        # Callbacks for alerts (can be used to trigger circuit breaker, etc.)
        on_ws_dead: Optional[Callable[[], Awaitable[None]]] = None,
        on_api_weight_high: Optional[Callable[[int], Awaitable[None]]] = None,
        on_loop_lag: Optional[Callable[[float], Awaitable[None]]] = None,
    ):
        self.check_interval = check_interval
        self.ws_timeout = ws_timeout
        self.api_weight_limit = api_weight_limit
        self.api_warning_threshold = api_warning_threshold
        self.loop_lag_warning_ms = loop_lag_warning_ms

        self.on_ws_dead = on_ws_dead
        self.on_api_weight_high = on_api_weight_high
        self.on_loop_lag = on_loop_lag

        # Internal state
        self._last_ws_timestamp: Optional[float] = None
        self._api_weights: list[tuple[float, int]] = []  # (timestamp, weight)
        self._last_loop_check: float = time.perf_counter()
        self._loop_lag_history: list[float] = []  # store recent lag for averaging

        self._task: Optional[asyncio.Task] = None

    # --- Methods to be called by external components ---

    def record_ws_message(self) -> None:
        """Call this whenever a WebSocket message is received."""
        self._last_ws_timestamp = time.time()

    def record_api_call(self, weight: int) -> None:
        """
        Call this after each REST API call with the weight used.
        (From Binance response header 'X-MBX-USED-WEIGHT-1M')
        """
        now = time.time()
        self._api_weights.append((now, weight))
        # Clean up older than 60 seconds
        cutoff = now - 60
        self._api_weights = [(ts, w) for ts, w in self._api_weights if ts > cutoff]

    def record_loop_iteration(self) -> None:
        """Call at the beginning of each main loop iteration to measure lag."""
        now = time.perf_counter()
        if self._last_loop_check is not None:
            lag_ms = (now - self._last_loop_check) * 1000
            self._loop_lag_history.append(lag_ms)
            # keep last 10 samples
            if len(self._loop_lag_history) > 10:
                self._loop_lag_history.pop(0)
        self._last_loop_check = now

    # --- Background check loop ---

    async def start(self) -> None:
        if self._task is not None:
            return
        self._task = asyncio.create_task(self._run(), name="SystemMonitor")

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def _run(self) -> None:
        while True:
            try:
                await asyncio.sleep(self.check_interval)
                await self._check_ws_heartbeat()
                await self._check_api_weight()
                await self._check_loop_lag()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception(f"Monitor error: {e}")

    async def _check_ws_heartbeat(self) -> None:
        if self._last_ws_timestamp is None:
            return  # not yet connected
        elapsed = time.time() - self._last_ws_timestamp
        if elapsed > self.ws_timeout:
            msg = f"WebSocket dead: no data for {elapsed:.1f}s (>{self.ws_timeout}s)"
            logger.critical(msg)
            if self.on_ws_dead:
                await self.on_ws_dead()

    async def _check_api_weight(self) -> None:
        if not self._api_weights:
            return
        now = time.time()
        total_weight = sum(w for ts, w in self._api_weights if ts > now - 60)
        if total_weight >= self.api_weight_limit * self.api_warning_threshold:
            percentage = (total_weight / self.api_weight_limit) * 100
            msg = f"API weight high: {total_weight}/{self.api_weight_limit} ({percentage:.1f}%)"
            logger.warning(msg)
            if self.on_api_weight_high:
                await self.on_api_weight_high(total_weight)

    async def _check_loop_lag(self) -> None:
        if not self._loop_lag_history:
            return
        avg_lag = sum(self._loop_lag_history) / len(self._loop_lag_history)
        if avg_lag > self.loop_lag_warning_ms:
            msg = f"Event loop lag: avg {avg_lag:.2f}ms (>{self.loop_lag_warning_ms}ms)"
            logger.warning(msg)
            if self.on_loop_lag:
                await self.on_loop_lag(avg_lag)


__all__ = ['SystemMonitor']
