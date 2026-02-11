"""
PURE REPLAY STREAMER
Location: live/stream/replay.py
Desc: Pemutar data historis murni yang patuh pada Protocol Aggregator.
      Pipeline: Parquet -> Raw Tick -> Aggregator -> Candle -> MarketObservation.
"""

import time
import logging
from typing import Generator, Dict, Any

# Core Imports (Facade)
from core.data import (
    ParquetLoader, 
    create_market_tick, 
    create_candle_aggregator,
    DataSource
)
from core.signals.types import MarketObservation

# Setup Logger
logger = logging.getLogger("Orca.Stream.Replay")

class ReplayStreamer:
    """
    Simulasi Exchange + Aggregator.
    Membaca data mentah, mengubahnya menjadi tick, lalu mengagregasikannya menjadi candle
    sebelum dikirim ke Engine.
    """
    
    def __init__(self, scenario_config: Dict[str, Any], data_path: str = "data/raw"):
        self.conf = scenario_config
        self.target_pair = "DOGE/USDT"
        self.ref_pair = "BTC/USDT"
        
        # Suffix kolom dari Loader (misal: close_DOGE-USDT)
        self.tgt_suf = self.target_pair.replace('/', '-')
        self.ref_suf = self.ref_pair.replace('/', '-')
        
        logger.info(f"🎞️  INIT PURE REPLAY: {self.conf.get('name', 'Custom Scenario')}")
        
        # 1. LOAD RAW DATA
        try:
            self.df = ParquetLoader.load_pair(
                base_path=data_path,
                target_symbol=self.target_pair, 
                ref_symbol=self.ref_pair,
                start_date=self.conf['start'],
                end_date=self.conf['end']
            )
        except Exception as e:
            logger.critical(f"❌ FATAL: Gagal memuat data replay. {e}")
            self.df = None
            
        # 2. INIT AGGREGATORS (60s Interval)
        self.agg_target = create_candle_aggregator(interval_seconds=60)
        self.agg_ref = create_candle_aggregator(interval_seconds=60)

    def stream(self, delay_sec: float = 0.0) -> Generator[MarketObservation, None, None]:
        """
        Yields MarketObservation (Candle Data) ke Engine.
        """
        if self.df is None or self.df.empty:
            logger.error("🛑 Stream Aborted: No Data.")
            return

        total_ticks = len(self.df)
        logger.info(f"▶️  START STREAM: {total_ticks} raw ticks queued.")

        records = self.df.to_dict('records')

        log_interval = max(1, total_ticks//10)

        
        for i, row in enumerate(records):
            ts = int(row['timestamp'])
            
            # Gunakan Source Futures agar Engine menganggap ini data Live
            src = DataSource.BINANCE_FUTURES

            # --- A. CREATE TICKS (Result Wrapper) ---
            res_target = create_market_tick(
                timestamp=ts,
                symbol=self.target_pair,
                price=float(row[f'close_{self.tgt_suf}']),
                volume=float(row[f'vol_{self.tgt_suf}']),
                source=src 
            )
            
            res_ref = create_market_tick(
                timestamp=ts,
                symbol=self.ref_pair,
                price=float(row[f'close_{self.ref_suf}']),
                volume=float(row[f'vol_{self.ref_suf}']),
                source=src
            )

            # Validasi & Unwrap (Skip jika data cacat)
            if res_target.is_err() or res_ref.is_err():
                continue

            tick_target = res_target.unwrap()
            tick_ref = res_ref.unwrap()

            # --- B. FEED AGGREGATOR ---
            # Menggunakan method .add_tick() sesuai Protocol
            candle_target = self.agg_target.add_tick(tick_target)
            candle_ref = self.agg_ref.add_tick(tick_ref)

            # --- C. CHECK OUTPUT & YIELD ---
            # Hanya yield jika Aggregator selesai membentuk candle penuh
            if candle_target and candle_ref:
                obs = MarketObservation(
                    timestamp=ts,
                    symbol=self.target_pair,
                    source="AGGREGATED_FUTURES", 
                    data={
                        "close_DOGE": candle_target.close,
                        "close_BTC": candle_ref.close,
                        "volume": candle_target.volume,
                        "is_complete": True
                    }
                )
                yield obs
            
            # Speed Control
            if delay_sec > 0:
                time.sleep(delay_sec)
                
            # Log Progress
            if total_ticks > 10 and i % log_interval == 0:
                logger.info(f"⏳ Progress: {(i/total_ticks)*100:.0f}%")

        logger.info("🏁 REPLAY FINISHED.")

__all__ = ['ReplayStreamer']
