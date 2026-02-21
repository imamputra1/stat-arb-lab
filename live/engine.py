# live/engine.py
"""
LIVE TRADING ENGINE
Location: live/engine.py
Desc: Central orchestration engine. Loads config, initializes components,
      replays market data, runs strategy, submits orders via OMS.
      Supports PAPER mode using ExecutionSimulator.
"""

import asyncio
import time
from typing import Optional, Dict, Any
from core.signals import MarketObservation
import pandas as pd

from core.shared.result import match_result
from core.shared.utils import get_logger

from core.execution.types import OrderSide
from core.execution.oms import OMSFacade, OMSMode
from core.execution.simulator import ExecutionSimulator, SimulatorConfig

from core.signals.factory import create_strategy
from core.signals.types import SignalType

from .config import DATA_CONFIG, STRATEGY_CONFIG, EXECUTION_CONFIG, RISK_CONFIG

logger = get_logger("live.engine")


class LiveEngine:
    """
    Main orchestration engine for live/paper trading.
    """

    def __init__(
        self,
        data_config: Dict[str, Any],
        strategy_config: Dict[str, Any],
        execution_config: Dict[str, Any],
        risk_config: Dict[str, Any],
    ):
        self.data_config = data_config
        self.strategy_config = strategy_config
        self.execution_config = execution_config
        self.risk_config = risk_config

        # State
        self._running = False
        self._strategy = None
        self._oms: Optional[OMSFacade] = None
        self._simulator: Optional[ExecutionSimulator] = None
        self._data: Optional[pd.DataFrame] = None
        self._current_index = 0
        self._peak_equity = 0.0
        self._warmup_ticks = data_config.get("warmup_ticks", 10)
        self._replay_speed = data_config.get("replay_speed", 0.001)

    async def start(self):
        """Load data, initialize components, and run the main loop."""
        logger.info("🚀 Starting Live Engine...")
        self._running = True

        # 1. Load market data
        if not self._load_data():
            logger.error("❌ Data loading failed. Aborting.")
            return

        # 2. Initialize execution environment (simulator + OMS)
        if not await self._init_execution():
            logger.error("❌ Execution initialization failed. Aborting.")
            return

        # 3. Initialize strategy via factory
        if not self._init_strategy():
            logger.error("❌ Strategy initialization failed. Aborting.")
            return

        # 4. Warmup: feed initial ticks without trading
        logger.info(f"🔥 Warmup for {self._warmup_ticks} ticks...")
        for i in range(min(self._warmup_ticks, len(self._data))):
            tick = self._data.iloc[i]
            self._feed_tick(tick)
            self._current_index = i + 1

        # 5. Main loop
        logger.info("⚙️ Entering main trading loop...")
        await self._main_loop()

        # 6. Cleanup
        await self._shutdown()

    # ----------------------------------------------------------------------
    # DATA LOADING
    # ----------------------------------------------------------------------
    def _load_data(self) -> bool:
        """Load target and reference parquet files, align them."""
        try:
            path_target = self.data_config["path_target"]
            path_ref = self.data_config["path_ref"]

            logger.info(f"Loading target data from {path_target}")
            df_target = pd.read_parquet(path_target)
            logger.info(f"Loading reference data from {path_ref}")
            df_ref = pd.read_parquet(path_ref)

            # Ensure timestamp column exists and sort
            if "timestamp" not in df_target.columns:
                # Try to infer index as timestamp
                if isinstance(df_target.index, pd.DatetimeIndex):
                    df_target = df_target.reset_index()
                else:
                    logger.error("Target data missing 'timestamp' column")
                    return False

            if "timestamp" not in df_ref.columns:
                if isinstance(df_ref.index, pd.DatetimeIndex):
                    df_ref = df_ref.reset_index()
                else:
                    logger.error("Reference data missing 'timestamp' column")
                    return False

            # Rename close columns to avoid conflict
            df_target = df_target[["timestamp", "close"]].rename(columns={"close": "close_target"})
            df_ref = df_ref[["timestamp", "close"]].rename(columns={"close": "close_ref"})

            # Merge on timestamp (inner join to ensure alignment)
            df = pd.merge(df_target, df_ref, on="timestamp", how="inner")
            df.sort_values("timestamp", inplace=True)
            df.reset_index(drop=True, inplace=True)

            self._data = df
            logger.info(f"✅ Data loaded: {len(df)} ticks")
            return True

        except Exception as e:
            logger.error(f"Data load failed: {e}")
            return False

    # ----------------------------------------------------------------------
    # EXECUTION INITIALIZATION
    # ----------------------------------------------------------------------
    async def _init_execution(self) -> bool:
        """Initialize simulator and OMS facade."""
        try:
            # Create simulator config from execution config
            sim_config = SimulatorConfig(
                initial_cash=self.execution_config.get("initial_cash", 100_000.0),
                base_currency="USDT",
                fee_rate_maker=self.execution_config.get("maker_fee", 0.0002),
                fee_rate_taker=self.execution_config.get("taker_fee", 0.0004),
                slippage_std_bps=self.execution_config.get("slippage", 0.0001) * 10000,  # convert to bps
            )
            self._simulator = ExecutionSimulator(config=sim_config)
            logger.info("✅ Simulator created")

            # Parameter yang valid untuk OMSConfig (lihat core/execution/oms/system.py)
            allowed_oms_params = {
                'max_open_orders',
                'max_notional_per_order',
                'order_timeout_seconds',
                'default_tif',
                'auto_reconcile',
                'validate_risk',
                'oms_id'
            }

            mode = OMSMode.PAPER if self.execution_config.get("mode") == "PAPER" else OMSMode.LIVE

            # Filter hanya parameter yang diizinkan
            oms_kwargs = {k: v for k, v in self.execution_config.items() 
                          if k in allowed_oms_params}

            oms_result = OMSFacade.create(
                broker=self._simulator,
                market_data=None,
                risk_check=None,
                mode=mode,
                **oms_kwargs
            )

            if oms_result.is_err():
                logger.error(f"OMS creation failed: {oms_result.unwrap_err()}")
                return False

            self._oms = oms_result.unwrap()
            await self._oms.start()
            logger.info("✅ Execution environment ready.")
            return True

        except Exception as e:
            logger.error(f"Execution init error: {e}", exc_info=True)
            return False

    # ----------------------------------------------------------------------
    # STRATEGY INITIALIZATION
    # ----------------------------------------------------------------------
    def _init_strategy(self) -> bool:
        """Instantiate the strategy using the factory with config."""
        try:
            strat_result = create_strategy(self.strategy_config)
            if strat_result.is_err():
                logger.error(f"Strategy creation failed: {strat_result.unwrap_err()}")
                return False

            self._strategy = strat_result.unwrap()
            logger.info(f"✅ Strategy '{self.strategy_config.get('name')}' initialized.")
            return True

        except Exception as e:
            logger.error(f"Strategy init error: {e}")
            return False

    # ----------------------------------------------------------------------
    # MAIN LOOP
    # ----------------------------------------------------------------------
    async def _main_loop(self):
        """Iterate through data ticks, update strategy, submit orders."""
        while self._running and self._current_index < len(self._data):
            tick_start = time.monotonic()

            # Get current tick
            row = self._data.iloc[self._current_index]
            timestamp = row["timestamp"]
            # Convert timestamp to milliseconds if needed (assuming it's in seconds or datetime)
            if isinstance(timestamp, pd.Timestamp):
                ts_ms = int(timestamp.timestamp() * 1000)
            else:
                # assume already ms or seconds? we'll convert to ms
                ts_ms = int(timestamp) if timestamp > 1e11 else int(timestamp * 1000)

            # Feed tick to strategy
            signal = self._feed_tick(row)

            # Update simulator's price (so that orders execute at correct price)
            # Simulator uses _last_prices dict; we set price for target symbol
            symbol_traded = self.data_config["symbol_traded"]
            self._simulator.update_price(symbol_traded, row["close_target"])

            # Check risk limits before acting on signal
            if not await self._check_risk():
                logger.warning("⛔ Risk limit breached. Stopping trading.")
                break

            # If signal is actionable, submit order
            if signal and signal.signal_type in (SignalType.BUY, SignalType.SELL):
                await self._submit_order(signal, row["close_target"])

            # Poll for order updates (optional, but good for simulation)
            if self._current_index % 10 == 0:
                await self._oms.refresh_orders()

            # Update peak equity for drawdown tracking
            await self._update_peak_equity()

            # Control replay speed
            elapsed = time.monotonic() - tick_start
            sleep_time = max(0, self._replay_speed - elapsed)
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)

            self._current_index += 1

        logger.info("🏁 Main loop finished.")

    def _feed_tick(self, row: pd.Series):
        """Feed a single tick to the strategy and return the signal."""
        symbol_traded = self.data_config["symbol_traded"]
        base = symbol_traded.split('/')[0]  # e.g., DOGE
        timestamp = row["timestamp"]
        if isinstance(timestamp, pd.Timestamp):
           ts_ms = int(timestamp.timestamp() * 1000)
        else:
        # if already ms (> year 2000 in ms) or seconds
            ts_ms = int(timestamp) if timestamp > 1e11 else int(timestamp * 1000)

        data = {
            f"close_{base}": float(row["close_target"]),
            "close_BTC": float(row["close_ref"]),
        }

        obs = MarketObservation(
            timestamp=ts_ms,
            data=data,
            symbol=symbol_traded,
            source="parquet"
        )
        signal_result = self._strategy.evaluate_state(obs)
        if signal_result.is_err():
            logger.debug(f"Strategy error: {signal_result.unwrap_err()}")
            return None
        return signal_result.unwrap()

    async def _submit_order(self, signal, current_price: float):
        """Convert signal to order and submit via OMS."""
        # Determine side and quantity
        if signal.signal_type == SignalType.BUY:
            side = OrderSide.BUY
        elif signal.signal_type == SignalType.SELL:
            side = OrderSide.SELL
        else:
            return  # not entry

        # Use fixed quantity for now; could be dynamic from strategy's position sizing
        # For simplicity, use a fixed fraction of max position from config
        max_pos = self.strategy_config.get("signal_params", {}).get("max_position", 1000.0)
        quantity = max_pos * 0.001  # 10% of max as default size

        # Use market orders
        symbol = self.data_config["symbol_traded"]
        if side == OrderSide.BUY:
            result = await self._oms.buy_market(symbol, quantity)
        else:
            result = await self._oms.sell_market(symbol, quantity)

        match_result(
            result,
            on_ok=lambda report: logger.info(
                f"✅ Order {report.order.order_id} executed: fills={len(report.fills)}"
            ),
            on_err=lambda err: logger.error(f"❌ Order failed: {err}"),
        )

    async def _check_risk(self) -> bool:
        """Check global risk limits (kill switch, drawdown)."""
        if self._oms.is_kill_switch_engaged():
            logger.warning(f"Kill switch engaged: {self._oms._oms.sentry._kill_reason}")
            return False

        # Drawdown check
        equity = self._oms.get_equity()
        if equity > self._peak_equity:
            self._peak_equity = equity
        if self._peak_equity > 0:
            drawdown = (self._peak_equity - equity) / self._peak_equity
            if drawdown > self.risk_config.get("max_drawdown", 0.15):
                logger.error(f"Max drawdown exceeded: {drawdown:.2%}")
                self._oms.engage_kill_switch("Max drawdown exceeded")
                return False
        return True

    async def _update_peak_equity(self):
        """Update peak equity in OMS sentry."""
        equity = self._oms.get_equity()
        self._oms._oms.sentry.update_peak_equity(equity)

    async def _shutdown(self):
        """Graceful shutdown."""
        logger.info("🛑 Shutting down engine...")
        if self._oms:
            await self._oms.stop()
        logger.info("✅ Shutdown complete.")


# ==============================================================================
# Entry point
# ==============================================================================
async def main():
    """Load config and run engine."""
    engine = LiveEngine(
        data_config=DATA_CONFIG,
        strategy_config=STRATEGY_CONFIG,
        execution_config=EXECUTION_CONFIG,
        risk_config=RISK_CONFIG,
    )
    await engine.start()


if __name__ == "__main__":
    asyncio.run(main())
