"""
STRATEGY DATA PIPELINE (THE QUARTERMASTER) - V2.0
Location: research/strategy/pipeline.py
Focus: Load pre-merged silver data directly and compute spread_val.
"""

import polars as pl
import pandas as pd
import numpy as np
from pathlib import Path

# Core shared
from core.shared import Result, Ok, Err

# Base directory (assuming project root)
PROJECT_ROOT = Path(__file__).parent.parent.parent.absolute()
SILVER_DIR = PROJECT_ROOT / "data" / "silver"


def prepare_combat_data(
    target_coin: str,
    anchor_coin: str,
    start_date: str,
    end_date: str,
    hedge_ratio: float = 1.0
) -> Result[pd.DataFrame, str]:
    """
    Main orchestration: loads pre-merged silver data directly.
    Returns a clean pandas DataFrame ready for executor.
    """
    try:
        # 1. Ekstrak Tahun dan Bulan
        year = start_date.split("-")[0]
        month = start_date.split("-")[1]
        base_path = str(SILVER_DIR / "**/*.parquet")

        # 2. Load langsung (Karena data silver sudah digabungkan dari awal)
        lf = pl.scan_parquet(base_path, hive_partitioning=True).filter(
            (pl.col("year") == year) &
            (pl.col("month") == month)
        )

        # 3. Konversi ke Pandas & Bersihkan
        df = lf.collect().to_pandas()
        df = df.sort_values("timestamp").reset_index(drop=True)
        df = df.ffill().bfill()

        df["timestamp"] = pd.to_datetime(df["timestamp"]).astype("int64") // 10**6

        # 4. Validasi kolom harga ada
        col_target = f"close_{target_coin}"
        col_anchor = f"close_{anchor_coin}"
        if col_target not in df.columns or col_anchor not in df.columns:
            return Err(f"Kolom harga tidak ditemukan: {col_target} atau {col_anchor}")

        # 5. Hitung spread_val wajib untuk Executor
        df["spread_val"] = np.log(df[col_target]) - hedge_ratio * np.log(df[col_anchor])

        return Ok(df)

    except Exception as e:
        return Err(f"Data pipeline meledak: {str(e)}")
