"""
SILVER LAKE FACADE - The Single Point of Entry
Menyembunyikan kompleksitas pipeline di balik API yang sederhana.
"""

from typing import List, Optional
import polars as pl

# Import factories dari subsistem Anda yang rapi
from .alignment import get_default_aligner
from .validation import get_default_validator
from .feature import create_stat_arb_transformer
from .feature.returns import create_log_returns_transformer
from .storage import create_parquet_engine

class SilverLakeQuant:
    def __init__(self, data_path: str):
        """Inisialisasi semua mesin pabrik saat Facade dipanggil."""
        self.storage = create_parquet_engine(data_path)
        self.validator = get_default_validator()
        self.aligner = get_default_aligner()
        self.returns_engine = create_log_returns_transformer()
        self.stat_arb_engine = create_stat_arb_transformer()

    def get_market_baseline(self, coin: str) -> pl.DataFrame:
        """
        Skenario 1: Ambil data log_price mentah (L1) dari Storage.
        Ini operasi I/O dari Disk.
        """
        # 1. Baca Parquet
        # 2. Validasi
        # 3. Return DataFrame
        pass

    def build_gladiator_arena(self, coins: List[str], anchor: Optional[str] = None) -> pl.DataFrame:
        """
        Skenario 2: On-The-Fly Alpha Generation (L2 & L3).
        TIDAK MENYENTUH DISK UNTUK PENYIMPANAN. Murni komputasi RAM.
        """
        # 1. Ambil data mentah (L1) untuk semua `coins` via self.storage
        # 2. Gunakan self.aligner untuk meratakan timestamp semua koin
        # 3. Hitung Log Return via self.returns_engine
        
        # 4. THE MAGIC: Jika anchor None, adu semua vs semua (Matrix)
        #    Jika anchor ada, adu semua vs anchor.
        #    Gunakan self.stat_arb_engine untuk menghitung OLS Spread & Z-Score
        
        # 5. Kembalikan Wide Table dinamis langsung ke Jupyter Notebook
        pass
