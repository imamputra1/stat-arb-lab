"""
STREAMER CONFIGURATION (INDUSTRIAL GRADE)
Location: research/ingestion/streamer/types.py
Desc: Konfigurasi Streaming Data untuk High-Frequency Simulation.
      Mendukung integrasi Data Lake (Parquet/WideTable) dan Chaos Engineering.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Dict, Any

from core.shared.result import Result, Ok, Err
from core.shared.domain import FetchJob

class StreamMode(str, Enum):
    # --- LIVE MODES ---
    LIVE_SOCKET = "LIVE_SOCKET"             # Real-time WebSocket Feed
    
    # --- SIMULATION / REPLAY MODES ---
    SYNTHETIC_GENERATOR = "SYNTHETIC"       # Random Walk (Sanity Check)
    HISTORICAL_REPLAY = "HISTORICAL_REPLAY" # Generic Replay
    
    # --- DATA LAKE MODES (Target Utama) ---
    LAKE_BROWN_PARQUET = "LAKE_BROWN_PARQUET"     # Raw Compressed Data (zstd)
    LAKE_SILVER_WIDETABLE = "LAKE_SILVER_WIDETABLE" # Cleaned/Aligned Data

@dataclass(frozen=True, slots=True)
class StreamerConfig:
    """
    Konfigurasi Streamer yang Immutable dan Optimized.
    Dirancang untuk throughput tinggi dan integritas data.
    """
    # -- Identity --
    symbol: str = "BTC/USDT"
    mode: StreamMode = StreamMode.SYNTHETIC_GENERATOR
    
    # -- Data Lake Sources --
    source_path: Optional[str] = None       # Path ke file Parquet/Folder
    storage_options: Dict[str, Any] = field(default_factory=dict) # Config untuk S3/GCS/Local FS
    
    # -- Performance Control --
    replay_speed_factor: float = 0.0        # 0.0 = Maximum Speed (Backtest Mode)
    batch_size: int = 10_000                # Ukuran chunk pembacaan Parquet (Vectorized Read)
    buffer_size: int = 1_000                # Ukuran Ring Buffer internal
    max_ticks: Optional[int] = None         # Limit eksekusi (safety break)
    
    # -- Chaos Engineering (War Room) --
    enable_chaos: bool = False
    chaos_probability: float = 0.01         # Probabilitas injeksi per tick
    
    # -- Job Integration --
    fetch_job: Optional[FetchJob] = None

    def validate(self) -> Result['StreamerConfig', str]:
        """
        Validasi ketat sebelum engine dinyalakan.
        """
        from core.shared.domain import is_valid_fetch_job
        
        # 1. Cek Source Path untuk Mode Data Lake
        if self.mode in (StreamMode.LAKE_BROWN_PARQUET, StreamMode.LAKE_SILVER_WIDETABLE):
            if not self.source_path:
                return Err(f"Mode {self.mode.value} membutuhkan 'source_path' yang valid.")
            
        # 2. Validasi Kecepatan Replay
        if self.replay_speed_factor < 0:
            return Err("Replay speed factor tidak boleh negatif.")
        
        # 3. Validasi Chaos
        if not 0.0 <= self.chaos_probability <= 1.0:
            return Err("Chaos probability harus antara 0.0 dan 1.0.")
        
        # 4. Validasi Job (jika ada)
        if self.fetch_job and not is_valid_fetch_job(self.fetch_job):
            return Err("Konfigurasi FetchJob tidak valid/korup.")
            
        return Ok(self)

    def with_fetch_job(self, job: FetchJob) -> 'StreamerConfig':
        """
        Pattern Immutable Update: Membuat config baru dengan Job yang disuntikkan.
        """
        # Karena frozen=True, kita tidak bisa ubah self.fetch_job langsung.
        # Kita return instance baru.
        return StreamerConfig(
            symbol=job.symbol,
            mode=self.mode,
            source_path=self.source_path,
            storage_options=self.storage_options,
            replay_speed_factor=self.replay_speed_factor,
            batch_size=self.batch_size,
            buffer_size=self.buffer_size,
            max_ticks=self.max_ticks,
            enable_chaos=self.enable_chaos,
            chaos_probability=self.chaos_probability,
            fetch_job=job
        )
