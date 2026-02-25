"""
SANITIZER (THE CLEANER)
Location: live/guardian/sanitizer.py
Desc: Periodic state reconciliation – removes ghost positions and cancels stale orders.
      Runs every 60 seconds.
"""

import asyncio
import time
import logging
from typing import Dict, Optional

from core.execution.types import Order, Position
from core.execution.protocols import BrokerProtocol
from core.execution.oms.components import InventoryManager

logger = logging.getLogger(__name__)


class Sanitizer:
    """
    Background task that periodically cleans up inconsistent state:
    - Ghost positions: inventory has a non‑zero position but broker reports zero.
    - Stale orders: orders that have been active for more than 10 minutes.
    """

    def __init__(
        self,
        inventory: InventoryManager,
        adapter: BrokerProtocol,
        active_orders: Dict[str, Order],   # e.g. OMS._orders
        interval_seconds: float = 60.0,
        stale_threshold_seconds: float = 600.0,  # 10 minutes
    ) -> None:
        self.inventory = inventory
        self.adapter = adapter
        self.active_orders = active_orders
        self.interval = interval_seconds
        self.stale_threshold = stale_threshold_seconds

        self._task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        """Launch the background cleanup task."""
        if self._task is not None:
            logger.warning("Sanitizer already running")
            return
        self._task = asyncio.create_task(self._run(), name="Sanitizer")
        logger.info("Sanitizer started (interval=%.1fs)", self.interval)

    async def stop(self) -> None:
        """Cancel the cleanup task."""
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
            logger.info("Sanitizer stopped")

    async def _run(self) -> None:
        """Main loop – runs every interval seconds."""
        while True:
            try:
                await asyncio.sleep(self.interval)
                await self._clean_ghost_positions()
                await self._clean_stale_orders()
            except asyncio.CancelledError:
                logger.debug("Sanitizer cancelled")
                break
            except Exception as e:
                # Isolate errors so a single failure doesn't stop the loop
                logger.exception("Sanitizer error: %s", e)

    async def _clean_ghost_positions(self) -> None:
        """
        Compare inventory positions with broker positions.
        If a symbol exists in inventory but not in broker (or broker shows zero),
        remove the inventory position entirely.
        """
        broker_pos_res = await self.adapter.get_all_positions()
        if broker_pos_res.is_err():
            logger.error("Cannot fetch broker positions: %s", broker_pos_res.unwrap_err())
            return

        broker_positions: list[Position] = broker_pos_res.unwrap()
        broker_map = {p.symbol: p for p in broker_positions}

        # Collect symbols to remove (do not modify dict while iterating)
        to_remove = []
        for symbol, pos in self.inventory._positions.items():
            # Even zero‑quantity positions are considered garbage; remove them.
            if abs(pos.quantity) < 1e-9:
                to_remove.append(symbol)
                continue

            broker_pos = broker_map.get(symbol)
            if broker_pos is None or abs(broker_pos.quantity) < 1e-9:
                logger.warning(
                    "Ghost position detected: %s (inv qty=%f) – removing",
                    symbol, pos.quantity
                )
                to_remove.append(symbol)

        # Perform deletion after iteration
        for symbol in to_remove:
            if symbol in self.inventory._positions:
                del self.inventory._positions[symbol]
                logger.info("Removed ghost position for %s", symbol)


    async def _clean_stale_orders(self) -> None:
        """
        Cancel orders that have been active longer than the stale threshold.
        """
        now = time.time()
        to_cancel = []

        for order_id, order in list(self.active_orders.items()):
            # order.created_at is a datetime; convert to timestamp
            if order.created_at:
                age = now - order.created_at.timestamp()
                if age > self.stale_threshold:
                    to_cancel.append(order_id)

        for order_id in to_cancel:
            logger.warning("Stale order detected (ID=%s) – cancelling", order_id)
            res = await self.adapter.cancel_order(order_id)
            if res.is_ok():
                logger.info("Cancelled stale order %s", order_id)
            else:
                logger.error("Failed to cancel stale order %s: %s",
                             order_id, res.unwrap_err())

__all__ = ['Sanitizer']
