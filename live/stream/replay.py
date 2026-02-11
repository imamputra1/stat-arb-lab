"""
REPLAY STREAMER (With Industrial Aggregator)
Location: live/stream/replay.py
Desc: Menyuapkan data Loader ke Aggregator untuk menghasilkan Candle valid.
      Pipeline: DataFrame -> MarketTick -> CandleAggregator -> Candle -> MarketObservation
"""

import time
import logging
from typing import Generator, Dict, Any

# Core Imports
from core.data import ParquetLoader, ChaosMonkey, create_market_tick, create_candle_aggregator
from core.signals.types import MarketObservation

# Setup Logger
logger = logging.getLogger("Orca.Stream.Replay")

class ReplayStreamer:
    """
    Virtual Exchange yang menggunakan Aggregator Asli (core/data/aggregators.py).
    Ini menjamin logika simulasi 100% sama dengan logika Live Trading.
    """
    
    def __init__(self, scenario_config: Dict[str, Any], data_path: str = "data/raw"):
        self.conf = scenario_config
        self.target_pair = "DOGE/USDT"
        self.ref_pair = "BTC/USDT"
        
        # Suffix kolom dari Loader
        self.tgt_suf = self.target_pair.replace('/', '-')
        self.ref_suf = self.ref_pair.replace('/', '-')
        
        logger.info(f"🏭 INIT REPLAY AGGREGATOR: {self.conf.get('name', 'Custom')}")
        
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
            logger.critical(f"❌ FATAL: Gagal memuat data. {e}")
            self.df = None
            
        # 2. INIT AGGREGATORS (Satu untuk setiap pair)
        # Interval 60 detik (1m) sesuai data parquet
        self.agg_target = create_candle_aggregator(interval_seconds=60)
        self.agg_ref = create_candle_aggregator(interval_seconds=60)

        # 3. INIT CHAOS
        chaos_cfg = self.conf.get('chaos', {})
        self.chaos = ChaosMonkey(chaos_cfg) if chaos_cfg else None

    def stream(self, delay_sec: float = 0.0) -> Generator[MarketObservation, None, None]:
        if self.df is None or self.df.empty:
            return

        total_ticks = len(self.df)
        logger.info(f"▶️  START PIPELINE: {total_ticks} ticks -> Aggregator -> Engine")

        records = self.df.to_dict('records')
        
        for i, row in enumerate(records):
            
            # A. CHAOS INJECTION (Pada data mentah sebelum masuk Aggregator)
            if self.chaos:
                # row = self.chaos.apply(row) # Implementasi chaos nanti
                pass

            ts = int(row['timestamp'])
            
            # B. CREATE TICKS (Seolah-olah dari WebSocket)
            # Ambil Close price sebagai 'Tick' harga saat ini
            tick_target = create_market_tick(
                timestamp=ts,
                symbol=self.target_pair,
                price=float(row[f'close_{self.tgt_suf}']),
                volume=float(row[f'vol_{self.tgt_suf}']),
                source="SIM_FEED"
            )
            
            tick_ref = create_market_tick(
                timestamp=ts,
                symbol=self.ref_pair,
                price=float(row[f'close_{self.ref_suf}']),
                volume=float(row[f'vol_{self.ref_suf}']),
                source="SIM_FEED"
            )

            # C. FEED AGGREGATOR (The Real Processing)
            # Masukkan tick ke mesin pengolah candle
            candle_target = self.agg_target.on_tick(tick_target)
            candle_ref = self.agg_ref.on_tick(tick_ref)

            # D. CHECK OUTPUT
            # Aggregator hanya me-return Candle jika interval sudah selesai.
            # Karena data kita 1m dan interval 1m, ini akan output candle (dengan 1 tick delay).
            
            if candle_target and candle_ref:
                # E. CONSTRUCT OBSERVATION (Hanya jika kedua candle matang)
                obs = MarketObservation(
                    timestamp=ts,
                    symbol=self.target_pair,
                    source="AGGREGATED_SIM",
                    data={
                        "close_DOGE": candle_target.close,
                        "close_BTC": candle_ref.close,
                        "volume": candle_target.volume,
                        # Metadata untuk debug
                        "is_complete": True
                    }
                )
                yield obs
            
            # Speed Control
            if delay_sec > 0:
                time.sleep(delay_sec)
                
            if i > 0 and i % (total_ticks // 10) == 0:
                logger.info(f"⏳ Progress: {(i/total_ticks)*100:.0f}%")

        logger.info("🏁 REPLAY FINISHED.")
