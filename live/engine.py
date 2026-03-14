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
import pandas as pd

from core.signals.types import MarketObservation, SignalType
from core.shared.result import match_result
from core.shared.utils import get_logger

from core.execution.types import OrderSide
from core.execution.oms.facade import OMSFacade
from core.execution.oms.system import OMSMode
from core.execution.simulator import ExecutionSimulator, SimulatorConfig

from core.signals.factory import create_strategy
from live.reporter import MetricsReporter
from .config import DATA_CONFIG, STRATEGY_CONFIG, EXECUTION_CONFIG, RISK_CONFIG


logger = get_logger("live.engine")

class SimulatorMarketData:
    def __init__(self):
        self._prices = {}

    def update_price(self, symbol:str, price: float):
        self._prices[symbol] = price

    def get_last_price(self, symbol: str) -> Optional[float]:
        return self._prices.get(symbol)

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
        tick_modifier: Optional[Any] = None
    ):
        self.data_config = data_config
        self.strategy_config = strategy_config
        self.execution_config = execution_config
        self.risk_config = risk_config
        self._market_data_adapter = None
        self._tick_modifier = tick_modifier

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
        logger.info("[Starting Live Engine...")
        self._running = True

        if not self._load_data():
            logger.error("[FAILED] Data loading failed. Aborting.")
            return

        if not await self._init_execution():
            logger.error("[FAILED] Execution initialization failed. Aborting.")
            return

        if not self._init_strategy():
            logger.error("[FAILED] Strategy initialization failed. Aborting.")
            return

        # =========================================================
        # [SURGERY FIX 1]: Menggunakan Assert agar Pyright yakin 100%
        # =========================================================
        assert self._data is not None, "Data is missing after load"

        logger.info(f"[PROCESSING] Warmup for {self._warmup_ticks} ticks...")
        limit = min(self._warmup_ticks, len(self._data)) 
        for i in range(limit):
            tick = self._data.iloc[i]
            self._feed_tick(tick)
            self._current_index = i + 1

        try:
            logger.info("[PROCESSING] Entering main trading loop...")
            await self._main_loop()

        except asyncio.CancelledError:
            logger.warning("[FORCE] Dihentikan paksa oleh user (Ctrl+C)! Mempersiapkan laporan parsial...")

        finally:
            if hasattr(self, '_print_final_report'):
                await self._print_final_report()
            await self._shutdown()


    def _load_data(self) -> bool:
        """Load target and reference parquet files, align them."""
        try:
            path_target = self.data_config["path_target"]
            path_ref = self.data_config["path_ref"]

            logger.info(f"Loading target data from {path_target}")
            df_target = pd.read_parquet(path_target)
            logger.info(f"Loading reference data from {path_ref}")
            df_ref = pd.read_parquet(path_ref)

            if "timestamp" not in df_target.columns:
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

            df_target = df_target[["timestamp", "close"]].rename(columns={"close": "close_target"})
            df_ref = df_ref[["timestamp", "close"]].rename(columns={"close": "close_ref"})

            df = pd.merge(df_target, df_ref, on="timestamp", how="inner")
            df.sort_values("timestamp", inplace=True)
            df.reset_index(drop=True, inplace=True)

            self._data = df
            logger.info(f"[DONE] Data loaded: {len(df)} ticks")
            return True

        except Exception as e:
            logger.error(f"Data load failed: {e}")
            return False

    async def _init_execution(self) -> bool:
        """Initialize simulator and OMS facade."""
        try:
            sim_config = SimulatorConfig(
                initial_cash=self.execution_config.get("initial_cash", 500.0),
                base_currency="USDT",
                fee_rate_maker=self.execution_config.get("maker_fee", -0.0002),
                fee_rate_taker=self.execution_config.get("taker_fee", 0.0004),
                slippage_std_bps=self.execution_config.get("slippage", 0.0001) * 10000,
            )
            self._simulator = ExecutionSimulator(config=sim_config)
            self._market_data_adapter =SimulatorMarketData()
            logger.info("[DONE] Simulator created")

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

            oms_kwargs = {k: v for k, v in self.execution_config.items() if k in allowed_oms_params}

            oms_result = OMSFacade.create(
                broker=self._simulator,
                market_data=self._market_data_adapter,
                risk_check=None,
                mode=mode,
                **oms_kwargs
            )

            if oms_result.is_err():
                logger.error(f"OMS creation failed: {oms_result.unwrap_err()}")
                return False

            self._oms = oms_result.unwrap()
            
            assert self._oms is not None
            await self._oms.start()
            
            logger.info("[DONE] Execution environment ready.")
            return True

        except Exception as e:
            logger.error(f"Execution init error: {e}", exc_info=True)
            return False

    def _init_strategy(self) -> bool:
        """Instantiate the strategy using the factory with config."""
        try:
            strat_result = create_strategy(self.strategy_config)
            if strat_result.is_err():
                logger.error(f"Strategy creation failed: {strat_result.unwrap_err()}")
                return False

            self._strategy = strat_result.unwrap()
            logger.info(f"[DONE] Strategy '{self.strategy_config.get('name')}' initialized.")
            return True

        except Exception as e:
            logger.error(f"Strategy init error: {e}")
            return False

    async def _main_loop(self):
        """Iterate through data ticks, update strategy, submit orders."""
        
        # =========================================================
        # [SURGERY FIX 2]: Assert beruntun untuk mematikan linter
        # =========================================================
        assert self._data is not None
        assert self._simulator is not None
        assert self._oms is not None
        assert self._strategy is not None

        while self._running and self._current_index < len(self._data):
            tick_start = time.monotonic()

            if self._current_index % 100 == 0:
                logger.info(f"[PROCESSING] Processing tick {self._current_index}/{len(self._data)}...")

            row = self._data.iloc[self._current_index]

            if self._tick_modifier is not None:
                row = self._tick_modifier(row)
            
            # [SURGERY FIX 3]: Bypass Pandas Type Stubs dengan type: ignore
            timestamp = row["timestamp"]  # type: ignore
            close_target = float(row["close_target"])  # type: ignore
            
            if isinstance(timestamp, pd.Timestamp):
                ts_ms = int(timestamp.timestamp() * 1000)
            else:
                ts_ms = int(timestamp) if timestamp > 1e11 else int(timestamp * 1000)

            signal = self._feed_tick(row)

            symbol_traded = self.data_config["symbol_traded"]
            self._simulator.update_price(symbol_traded, close_target)

            if self._market_data_adapter:
                self._market_data_adapter.update_price(symbol_traded, close_target)

            if not await self._check_risk():
                logger.warning("[STOP] Risk limit breached. Stopping trading.")
                break

            if signal and signal.signal_type in (SignalType.BUY, SignalType.SELL):
                await self._submit_order(signal, close_target)

            if self._current_index % 10 == 0:
                await self._oms.refresh_orders()

            await self._update_peak_equity()

            elapsed = time.monotonic() - tick_start
            sleep_time = max(0, self._replay_speed - elapsed)
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)

            self._current_index += 1

        logger.info("[FINISH] Main loop finished.")
        await self._print_final_report()

    def _feed_tick(self, row: pd.Series):
        """Feed a single tick to the strategy and return the signal."""
        assert self._strategy is not None

        symbol_traded = self.data_config["symbol_traded"]
        base = symbol_traded.split('/')[0] 
        
        # [SURGERY FIX 4]: Pandas ignore
        target_val: Any = row["close_target"]
        ref_val: Any = row["close_ref"]
        timestamp_val: Any = row["timestamp"]


        if isinstance(timestamp_val, pd.Timestamp):
           ts_ms = int(timestamp_val.timestamp() * 1000)
        else:
            ts_ms = int(timestamp_val) if timestamp_val > 1e11 else int(timestamp_val * 1000)

        data = {
            f"close_{base}": float(target_val),
            "close_BTC": float(ref_val),
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
        assert self._oms is not None

        try:
            if signal.signal_type not in (SignalType.BUY, SignalType.SELL):
                return

            equity = self._oms.get_equity()
            trade_value_usdt = equity * 0.10  
            quantity = trade_value_usdt / current_price
            
            if quantity <= 0:
                return

            symbol = self.data_config["symbol_traded"]
            side = OrderSide.BUY if signal.signal_type == SignalType.BUY else OrderSide.SELL
            
            metadata = getattr(signal, 'metadata', getattr(signal, '_metadata', {}))
            zscore = metadata.get("zscore", 0.0)
            is_urgent = abs(zscore) > 1.5

            if side == OrderSide.BUY:
                if is_urgent:
                    limit_price = current_price * 1.001 
                    logger.info(f"URGENT BUY (Z={zscore:.2f}) -> TAKER @ {limit_price:.4f}")
                else:
                    limit_price = current_price * 1.0005 
                    logger.info(f"PASSIVE BUY (Z={zscore:.2f}) -> MAKER(Simulated) @ {limit_price:.4f}")
                    
                result = await self._oms.buy_limit(symbol, quantity, limit_price)
            else: 
                if is_urgent:
                    limit_price = current_price * 0.999
                    logger.info(f"URGENT SELL (Z={zscore:.2f}) -> TAKER @ {limit_price:.4f}")
                else:
                    limit_price = current_price * 0.9995
                    logger.info(f"PASSIVE SELL (Z={zscore:.2f}) -> MAKER(Simulated) @ {limit_price:.4f}")
                    
                result = await self._oms.sell_limit(symbol, quantity, limit_price)

            match_result(
                result,
                on_ok=lambda report: logger.info(
                    f"[DONE] Order {report.order.order_id} routed | Side: {side.name} | Qty: {quantity:,.2f} {symbol}"
                ),
                on_err=lambda err: logger.error(f"[FAILED] Order failed: {err}"),
            )
            
        except Exception as e:
            logger.error(f"[CRASHED] _submit_order CRASHED: {e}", exc_info=True)

    async def _check_risk(self) -> bool:
        """Check global risk limits (kill switch, drawdown)."""
        assert self._oms is not None

        if self._oms.is_kill_switch_engaged():
            sentry = getattr(self._oms._oms, 'sentry', None)
            reason = getattr(sentry, '_kill_reason', 'Unknown Override') if sentry else 'Unknown'
            logger.warning(f"Kill switch engaged: {reason}")
            return False

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
        assert self._oms is not None
            
        equity = self._oms.get_equity()
        sentry = getattr(self._oms._oms, 'sentry', None)
        if sentry and hasattr(sentry, 'update_peak_equity'):
            sentry.update_peak_equity(equity)

    async def _shutdown(self):
        """Graceful shutdown."""
        logger.info("[STOP] Shutting down engine...")
        if self._oms:
            await self._oms.stop()
        logger.info("[DONE] Shutdown complete.")


    async def _print_final_report(self):
        """Cetak laporan performa setelah trading selesai."""
        if self._oms is None:
            logger.warning("[WARNING] OMS tidak tersedia, tidak bisa mencetak laporan.")
            return
        initial_cash =self.execution_config.get("initial_cash", 500.0)
        reporter = MetricsReporter(self._oms, initial_cash)
        reporter.print_report()
        


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
    import logging
    # [SURGERY FIX]: Paksa terminal untuk menampilkan pesan INFO dan kesuksesan
    logging.basicConfig(
        level=logging.INFO, 
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        force=True
    )
    
    asyncio.run(main())
    asyncio.run(main())
