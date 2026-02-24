"""
CIRCUIT BREAKER (THE SNIPER)
Location: live/guardian/circuit_breaker.py
Desc: Autonomous patrol that monitors drawdown from storage and executes
      emergency market orders directly via adapter (bypassing OMS/Sentry).
"""

import asyncio
import logging
from typing import Optional

from core.data.storage.interface import StorageEngine
from core.execution.protocols import BrokerProtocol  # ExecutionAdapter
from core.execution.types import OrderSide, OrderType, OrderRequest, Position

logger = logging.getLogger(__name__)


class CircuitBreaker:
    """
    Asynchronous circuit breaker that polls equity metrics from storage
    and triggers a kill switch if drawdown exceeds max_drawdown.
    """

    def __init__(
        self,
        storage: StorageEngine,
        adapter: BrokerProtocol,
        max_drawdown: float = -0.05,      # -5% default
        poll_interval: float = 0.1,       # 100 ms
        equity_key: str = "account:equity",
        peak_equity_key: str = "account:peak_equity",
        halted_key: str = "system:status",
    ) -> None:
        self.storage = storage
        self.adapter = adapter
        self.max_drawdown = max_drawdown
        self.poll_interval = poll_interval
        self.equity_key = equity_key
        self.peak_equity_key = peak_equity_key
        self.halted_key = halted_key

        self._task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        """Launch the background patrol task."""
        if self._task is not None:
            logger.warning("CircuitBreaker already running")
            return
        self._task = asyncio.create_task(self._patrol(), name="CircuitBreakerPatrol")
        logger.info("CircuitBreaker started")

    async def stop(self) -> None:
        """Cancel the patrol task."""
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
            logger.info("CircuitBreaker stopped")

    async def _patrol(self) -> None:
        """Main polling loop – reads equity and triggers kill if needed."""
        while True:
            try:
                await asyncio.sleep(self.poll_interval)

                # 1. Check if already halted
                status_res = await self.storage.get(self.halted_key)
                if status_res.is_ok() and status_res.unwrap() == "HALTED":
                    continue

                # 2. Fetch current and peak equity
                equity_res = await self.storage.get(self.equity_key)
                peak_res = await self.storage.get(self.peak_equity_key)

                if equity_res.is_err() or peak_res.is_err():
                    # storage errors are not fatal – just skip this cycle
                    continue

                equity = equity_res.unwrap()
                peak = peak_res.unwrap()

                if equity is None or peak is None:
                    # not yet initialised
                    continue

                # 3. Calculate drawdown
                #   drawdown = (current - peak) / peak
                drawdown = (equity - peak) / peak

                if drawdown <= self.max_drawdown:
                    logger.critical(
                        f"Drawdown {drawdown:.2%} <= {self.max_drawdown:.2%} – KILL SEQUENCE ACTIVATED"
                    )
                    await self._kill()

            except asyncio.CancelledError:
                logger.debug("CircuitBreaker patrol cancelled")
                break
            except Exception as e:
                # Isolate any unexpected error – patrol must survive
                logger.exception(f"CircuitBreaker patrol error: {e}")

    async def _kill(self) -> None:
        """Execute the kill sequence: halt flag + reverse all positions."""
        # 1. Set global halted flag
        await self.storage.set(self.halted_key, "HALTED")

        # 2. Get current positions from adapter (broker)
        positions_res = await self.adapter.get_all_positions()
        if positions_res.is_err():
            logger.error(f"Cannot fetch positions for kill: {positions_res.unwrap_err()}")
            return

        positions: list[Position] = positions_res.unwrap()

        # 3. For each non‑zero position, send a market order in the opposite direction
        for pos in positions:
            if abs(pos.quantity) < 1e-9:
                continue

            # Determine side and absolute quantity
            if pos.quantity > 0:          # long
                side = OrderSide.SELL
                qty = pos.quantity
            else:                           # short
                side = OrderSide.BUY
                qty = -pos.quantity

            # Build market order request
            req_res = OrderRequest.create(
                symbol=pos.symbol,
                side=side,
                order_type=OrderType.MARKET,
                quantity=qty,
                # market orders do not require a price
            )
            if req_res.is_err():
                logger.error(f"Invalid reverse order for {pos.symbol}: {req_res.unwrap_err()}")
                continue

            order_req = req_res.unwrap()
            logger.info(f"Kill: submitting {side.value} {qty} {pos.symbol} (market)")

            # Fire and forget – we do not wait for the result to avoid blocking
            # the patrol. In production you may want to handle failures minimally.
            asyncio.create_task(self._send_kill_order(order_req))

    async def _send_kill_order(self, order_req: OrderRequest) -> None:
        """Async task that actually submits the order and logs the result."""
        res = await self.adapter.submit_order(order_req)
        if res.is_ok():
            logger.info(f"Kill order filled: {res.unwrap().order_id}")
        else:
            logger.error(f"Kill order failed: {res.unwrap_err()}")

__all__ = ['CircuitBreaker']
