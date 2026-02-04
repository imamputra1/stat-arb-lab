import pytest
import polars as pl
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
"""
ORCA QUANTUM TESTING KERNEL - V10.0
Location: tests/conftest.py
Paradigm: Result-Oriented, Structural Typed, High-Performance Polars Generation.
"""

from typing import List
from hypothesis import strategies as st

from core.shared import Result, Ok, Err, OHLCV, OHLCVContract

# ====================== QUANTUM DATA GENERATORS ======================

class QuantumDataFactory:
    """Composition-based factory for industrial-grade test data."""
    
    @staticmethod
    def create_mock_ohlcv(
        symbol: str, 
        n_rows: int = 1000, 
        freq: str = "5m"
    ) -> Result[pl.DataFrame, str]:
        """Generates high-fidelity synthetic market data wrapped in Result."""
        try:
            start_dt = datetime(2024, 1, 1)
            
            # Vectorized generation via Polars/Numpy
            df = pl.datetime_range(
                start_dt, 
                start_dt + timedelta(minutes=5 * (n_rows - 1)), 
                interval=freq, 
                eager=True
            ).to_frame("timestamp")
            
            # Stochastic Price Generation (Random Walk)
            returns = np.random.normal(0.0001, 0.01, n_rows)
            price = 100 * np.exp(np.cumsum(returns))
            
            df = df.with_columns([
                pl.lit(symbol).alias("symbol"),
                pl.Series("close", price),
                (pl.Series("close", price) * (1 + np.abs(np.random.normal(0, 0.002, n_rows)))).alias("high"),
                (pl.Series("close", price) * (1 - np.abs(np.random.normal(0, 0.002, n_rows)))).alias("low"),
                pl.Series("open", price).shift(1).fill_null(price[0]),
                pl.Series("volume", np.random.uniform(100, 1000, n_rows))
            ])
            
            # Hive Partitioning Columns
            df = df.with_columns([
                pl.col("timestamp").dt.year().cast(pl.Utf8).alias("year"),
                pl.col("timestamp").dt.month().format("%m").alias("month")
            ])
            
            return Ok(df)
        except Exception as e:
            return Err(f"Factory failure: {str(e)}")

# ====================== PYTEST FIXTURES ======================

@pytest.fixture(scope="session")
def data_factory() -> type[QuantumDataFactory]:
    return QuantumDataFactory

@pytest.fixture(scope="session")
def simulated_lake(tmp_path_factory, data_factory) -> Path:
    """
    Creates a temporary Hive-partitioned Silver Lake.
    Compliant with SilverDataLoader requirements.
    """
    temp_dir = tmp_path_factory.mktemp("quantum_lake")
    silver_path = temp_dir / "silver"
    silver_path.mkdir()
    
    # Generate Multi-Asset Data
    assets = ["BTC", "DOGE", "ETH"]
    all_columns = set()
    
    for asset in assets:
        res = data_factory.create_mock_ohlcv(asset)
        if res.is_ok():
            df = res.unwrap()
            all_columns.update(df.columns)
            
            # Hive Sink: year=YYYY/month=MM/asset.parquet
            # Kita simulasi partisi 2024/01
            part_path = silver_path / "year=2024" / "month=01"
            part_path.mkdir(parents=True, exist_ok=True)
            df.write_parquet(part_path / f"{asset.lower()}.parquet")

    # Generate Metadata Registry
    import json
    metadata = {
        "columns": sorted(list(all_columns)),
        "row_count": 3000,
        "assets": assets,
        "last_updated": datetime.now().isoformat()
    }
    (silver_path / "metadata.json").write_text(json.dumps(metadata))
    
    return silver_path

@pytest.fixture
def mock_ohlcv_batch(data_factory) -> List[OHLCVContract]:
    """Generates a list of structural-typed OHLCV objects for Unit Testing."""
    res = data_factory.create_mock_ohlcv("TEST", n_rows=10)
    df = res.unwrap()
    
    # Map to domain models using composition
    return [
        OHLCV(**row) for row in df.to_dicts()
    ]

# ====================== HYPOTHESIS STRATEGIES ======================

@st.composite
def ohlcv_strategy(draw):
    """Property-based testing strategy for OHLCV data."""
    return OHLCV(
        timestamp=draw(st.integers(min_value=1000000000000, max_value=2000000000000)),
        open=draw(st.floats(min_value=0.01, max_value=100000.0)),
        high=draw(st.floats(min_value=0.01, max_value=100000.0)),
        low=draw(st.floats(min_value=0.01, max_value=100000.0)),
        close=draw(st.floats(min_value=0.01, max_value=100000.0)),
        volume=draw(st.floats(min_value=0, max_value=1000000.0))
    )
