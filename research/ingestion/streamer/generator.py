"""
CHAOS STREAMER ENGINE (INDUSTRIAL GRADE)
Location: research/ingestion/streamer/generator.py
Desc: High-throughput Data Generator.
      Mampu streaming Parquet (Brown Lake) dan WideTable (Silver Lake)
      dengan efisiensi memori (Batch Processing) dan Chaos Injection.
"""

import time
import random
import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path
from typing import Generator

# Core Imports
from core.data.types import MarketTick, create_market_tick, DataSource
from core.shared.utils import get_logger
from core.shared.performance import PerformanceMonitor

# Local Imports
from .types import StreamerConfig, StreamMode
from .mechanics import StreamMechanic

logger = get_logger("streamer.generator")

class ChaosStreamer:
    """
    Engine utama untuk streaming data.
    Berperan sebagai 'Iterator' yang menghasilkan MarketTick satu per satu.
    """
    
    def __init__(self, config: StreamerConfig):
        # 1. Validasi Config
        validation = config.validate()
        if validation.is_err():
            raise ValueError(f"Streamer Config Error: {validation.error}")
        self.config = config

        # 2. Init Mechanics (Sutradara Chaos)
        self.mechanic = StreamMechanic(
            enable_chaos=config.enable_chaos,
            probability=config.chaos_probability
        )
        
        # 3. Telemetri
        self.metrics = PerformanceMonitor(history_size=5000)
        self._tick_counter = 0
        self._is_running = False

    def stream(self) -> Generator[MarketTick, None, None]:
        """
        [PUBLIC API] Main Generator Loop.
        Yields: MarketTick (Clean or Chaos-injected)
        """
        self._is_running = True
        logger.info(f"🚀 Streamer STARTED | Mode: {self.config.mode.value} | Chaos: {self.config.enable_chaos}")

        try:
            # Dispatcher berdasarkan Mode
            source_generator = None
            
            if self.config.mode == StreamMode.SYNTHETIC_GENERATOR:
                source_generator = self._stream_synthetic()
            elif self.config.mode in (StreamMode.LAKE_BROWN_PARQUET, StreamMode.LAKE_SILVER_WIDETABLE):
                source_generator = self._stream_parquet_lake()
            else:
                raise NotImplementedError(f"Mode {self.config.mode} belum diimplementasikan.")

            # Pipeline Loop
            for tick in source_generator:
                if not self._is_running: 
                    break
                
                # A. Apply Mechanics (Chaos Injection)
                # Method ini mengembalikan Result, kita harus unwrap dengan aman
                mech_result = self.mechanic.apply_mechanics(tick)
                
                if mech_result.is_ok():
                    final_tick = mech_result.unwrap()
                    
                    # B. Update Metrics
                    self._tick_counter += 1
                    # (Opsional) Rekam latency processing di sini via self.metrics
                    
                    # C. Yield ke Consumer (Strategy/Aggregator)
                    yield final_tick
                else:
                    # Skip tick jika mechanic gagal (Fail-safe)
                    pass

                # D. Max Ticks Guard (Safety)
                if self.config.max_ticks and self._tick_counter >= self.config.max_ticks:
                    logger.info("🛑 Max ticks limit reached.")
                    break

        except Exception as e:
            logger.error(f"🔥 Streamer CRASHED: {str(e)}")
            raise e
        finally:
            self.stop()

    def _stream_synthetic(self) -> Generator[MarketTick, None, None]:
        """Generator data dummy untuk sanity check tanpa file."""
        price = 50000.0
        timestamp = time.time()
        
        while self._is_running:
            # Random Walk Simulation
            change = (random.random() - 0.5) * 20 
            price += change
            timestamp += 1.0 # 1 detik per tick (simulasi)
            
            # Simulasi Speed Control
            self._apply_speed_control(1.0) 

            res = create_market_tick(
                symbol=self.config.symbol,
                price=abs(price),
                volume=random.uniform(0.01, 2.0),
                timestamp=timestamp,
                is_buyer_maker=random.choice([True, False]),
                source=DataSource.SYNTHETIC
            )
            
            if res.is_ok():
                yield res.unwrap()

    def _stream_parquet_lake(self) -> Generator[MarketTick, None, None]:
        """
        Membaca Brown/Silver Lake (Parquet) secara efisien (Batch Streaming).
        Menggunakan PyArrow untuk memory-mapped I/O dan Pandas untuk processing.
        """
        path = Path(self.config.source_path)
        if not path.exists():
            raise FileNotFoundError(f"Source path not found: {path}")

        logger.info(f"📂 Reading Lake: {path} | Batch: {self.config.batch_size}")

        # Menggunakan PyArrow Parquet File untuk iterasi batch (Hemat RAM)
        parquet_file = pq.ParquetFile(path)
        
        prev_timestamp = None

        # Loop per Row Group / Batch
        for batch in parquet_file.iter_batches(batch_size=self.config.batch_size):
            if not self._is_running: break
            
            # Convert ke Pandas DataFrame untuk manipulasi mudah
            df = batch.to_pandas()
            
            # --- COLUMN MAPPING & NORMALIZATION ---
            # Menangani variasi nama kolom dari Brown (Raw) vs Silver (Wide)
            df = self._normalize_columns(df)
            
            # Sort by time (Wajib untuk streaming yang benar)
            if 'timestamp' in df.columns:
                df = df.sort_values('timestamp')

            # Loop per Row (Vectorization sulit disini karena kita butuh yield object)
            for row in df.itertuples(index=False):
                # Speed Control (Replay)
                current_ts = getattr(row, 'timestamp', time.time())
                
                if prev_timestamp is not None and self.config.replay_speed_factor > 0:
                    time_diff = current_ts - prev_timestamp
                    if time_diff > 0:
                        self._apply_speed_control(time_diff)
                
                prev_timestamp = current_ts

                # Construct MarketTick via Factory
                # Menggunakan getattr dengan default untuk keamanan
                tick_res = create_market_tick(
                    symbol=self.config.symbol,
                    timestamp=current_ts,
                    price=getattr(row, 'price', 0.0),
                    volume=getattr(row, 'volume', 0.0),
                    # Handle boolean 'is_buyer_maker' or string 'side'
                    is_buyer_maker=self._parse_side(row),
                    source=DataSource.CSV_FILE # Mark as historical file
                )

                if tick_res.is_ok():
                    yield tick_res.unwrap()
                else:
                    # Log error tapi jangan stop stream (robustness)
                    pass

    def _normalize_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standarisasi nama kolom dari berbagai format.
        Target: timestamp, price, volume, is_buyer_maker
        """
        # Mapping kamus (Varian -> Standar)
        col_map = {
            # Waktu
            'time': 'timestamp', 'ts': 'timestamp', 'open_time': 'timestamp', 'transact_time': 'timestamp',
            # Harga
            'p': 'price', 'close': 'price', # Silver Lake (Candle) pakai Close sbg Price
            # Volume
            'q': 'volume', 'v': 'volume', 'qty': 'volume', 'quantity': 'volume',
            # Side
            'm': 'is_buyer_maker', 'side': 'side', 'is_buyer_maker': 'is_buyer_maker'
        }
        
        # Rename case-insensitive
        df.columns = [c.lower() for c in df.columns]
        df = df.rename(columns=col_map)
        
        # Ensure timestamp is float (Unix Seconds)
        if 'timestamp' in df.columns:
            # Heuristik: Jika timestamp > 3000000000, kemungkinan milisecond (13 digit)
            # Unix epoch sekarang sekitar 1.7 milyar (10 digit).
            if df['timestamp'].iloc[0] > 3000000000: 
                df['timestamp'] = df['timestamp'] / 1000.0
                
        return df

    def _parse_side(self, row) -> bool:
        """Helper cerdas untuk menentukan sisi trade (Maker/Taker)"""
        # Cek jika ada kolom 'is_buyer_maker' langsung (Standard Binance)
        if hasattr(row, 'is_buyer_maker'):
            return bool(row.is_buyer_maker)
        
        # Cek jika kolom 'side' (buy/sell)
        if hasattr(row, 'side'):
            s = str(row.side).lower()
            # Di Binance Tick: is_buyer_maker=True artinya MARKET MAKER adalah BUYER.
            # Ini berarti Taker adalah SELLER.
            # Jadi: Side 'sell' -> is_buyer_maker=True
            return s == 'sell'
        
        return False # Default assume Taker Buy

    def _apply_speed_control(self, time_delta: float):
        """Mengatur tempo replay untuk simulasi realistis"""
        if self.config.replay_speed_factor <= 0:
            return # Full Speed (Backtest mode)
            
        # Hitung delay nyata
        real_delay = time_delta / self.config.replay_speed_factor
        
        # Sleep hanya jika delay signifikan (>1ms) untuk menjaga presisi OS
        if real_delay > 0.001: 
            time.sleep(real_delay)

    def stop(self):
        """Graceful Shutdown"""
        self._is_running = False
        logger.info("🛑 Streamer STOPPED")
