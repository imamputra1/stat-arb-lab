"""
ORCA QUANTUM TESTING FRAMEWORK - Master Configuration
Location: tests/conftest.py
Focus: Polars-Native Fixtures, Async Support, and Synthetic Market Physics.
"""

import pytest
import polars as pl
import numpy as np
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys
import asyncio
from typing import Dict, List, Generator
from unittest.mock import MagicMock, patch

# ============================================================================
# ENVIRONMENT SETUP
# ============================================================================

# Inject Project Root to Sys Path
PROJECT_ROOT = Path(__file__).parent.parent.absolute()
sys.path.insert(0, str(PROJECT_ROOT))

# Core Imports (After Sys Path Injection)
from core.shared import Ok

# ============================================================================
# CONFIGURATION
# ============================================================================

class TestConfig:
    """Konfigurasi Global untuk Testing Suite."""
    START_DATE = datetime(2024, 1, 1, tzinfo=timezone.utc)
    N_CANDLES = 1000  # Cukup untuk rolling window stats
    TIMEFRAME_M = 1   # 1 Minute
    ASSETS = ["BTC/USDT", "ETH/USDT"]
    SEED = 42

# ============================================================================
# SYNTHETIC DATA ENGINE (PHYSICS SIMULATION)
# ============================================================================

class MarketPhysics:
    """
    Generator data sintetis cerdas.
    Mampu membuat pola Trending, Mean Reverting, dan Chaos.
    """
    def __init__(self, seed: int = 42):
        self.rng = np.random.default_rng(seed)

    def generate_timestamps(self, n: int) -> List[datetime]:
        base = TestConfig.START_DATE
        return [base + timedelta(minutes=i) for i in range(n)]

    def geometric_brownian_motion(self, n: int, s0: float, mu: float, sigma: float) -> np.ndarray:
        """Simulasi pergerakan harga standar (Trending/Random Walk)."""
        dt = 1/n
        dW = self.rng.normal(0, np.sqrt(dt), n)
        W = np.cumsum(dW)
        t = np.linspace(0, 1, n)
        return s0 * np.exp((mu - 0.5 * sigma**2) * t + sigma * W)

    def mean_reverting_process(self, n: int, mu: float, theta: float, sigma: float) -> np.ndarray:
        """Simulasi Ornstein-Uhlenbeck (Khusus untuk test KalmanMR)."""
        x = np.zeros(n)
        x[0] = mu
        for i in range(1, n):
            dx = theta * (mu - x[i-1]) + sigma * self.rng.normal()
            x[i] = x[i-1] + dx
        return x

    def generate_ohlcv(self, mode: str = "random_walk") -> pl.DataFrame:
        """
        Membuat DataFrame Polars lengkap dengan OHLCV, Z-Score, dan Target Price.
        Mode: 'random_walk' (Trending) atau 'mean_reversion' (Ranging).
        """
        n = TestConfig.N_CANDLES
        timestamps = self.generate_timestamps(n)
        
        # 1. Generate Harga Close (Target Price)
        if mode == "mean_reversion":
            # Harga berosilasi di sekitar 100
            closes = self.mean_reverting_process(n, mu=100.0, theta=0.1, sigma=2.0)
        else:
            # Harga trending dari 50000
            closes = self.geometric_brownian_motion(n, s0=50000.0, mu=0.1, sigma=0.3)

        # 2. Generate OHLC dari Close
        volatility = self.rng.uniform(0.001, 0.005, n)
        opens = closes * (1 + self.rng.normal(0, volatility))
        highs = np.maximum(opens, closes) * (1 + np.abs(self.rng.normal(0, volatility)))
        lows = np.minimum(opens, closes) * (1 - np.abs(self.rng.normal(0, volatility)))
        volumes = self.rng.lognormal(10, 1, n)

        # 3. Generate Z-Score (Pre-calculated for Signal Generator testing)
        # Hitung rolling mean/std secara manual atau pakai numpy
        window = 20
        series = pl.Series(closes)
        rolling_mean = series.rolling_mean(window).fill_null(closes[0])
        rolling_std = series.rolling_std(window).fill_null(1.0)
        z_scores = (series - rolling_mean) / rolling_std

        # 4. Inject Anomalies (Opsional - untuk test Filter)
        # Buat outlier ekstrim di tengah data
        mid = n // 2
        z_scores[mid] = 10.0 # Extreme spike

        return pl.DataFrame({
            "timestamp": timestamps,
            "open": opens,
            "high": highs,
            "low": lows,
            "close": closes,
            "volume": volumes,
            "target_price": closes, # Alias untuk close
            "z_score": z_scores,    # Critical for KalmanMR
            "volatility": volatility, # Critical for VolatilityFilter
            "atr": volatility * closes # Mock ATR
        }).with_columns([
            pl.col("timestamp").cast(pl.Datetime).dt.replace_time_zone("UTC")
        ])

# ============================================================================
# SESSION FIXTURES (DATA)
# ============================================================================

@pytest.fixture(scope="session")
def market_physics():
    return MarketPhysics(seed=TestConfig.SEED)

@pytest.fixture(scope="session")
def mock_market_data(market_physics) -> Dict[str, pl.DataFrame]:
    """
    Menyediakan data pasar Native Polars yang siap pakai.
    Returns: Dict {'BTC/USDT': df_random, 'ETH/USDT': df_mean_revert}
    """
    return {
        "BTC/USDT": market_physics.generate_ohlcv(mode="random_walk"),
        "ETH/USDT": market_physics.generate_ohlcv(mode="mean_reversion") # Cocok untuk test Kalman
    }

@pytest.fixture(scope="session")
def mock_lazy_data(mock_market_data) -> pl.LazyFrame:
    """Versi LazyFrame untuk test pipeline utils."""
    return mock_market_data["BTC/USDT"].lazy()

# ============================================================================
# FUNCTION FIXTURES (COMPONENTS)
# ============================================================================

@pytest.fixture
def mock_strategy_factory():
    """Mock Strategy Factory yang mengembalikan strategi dummy."""
    with patch("core.signals.factory.StrategyFactory") as MockFactory:
        factory = MockFactory.return_value
        factory.create.return_value = Ok(MagicMock(name="MockStrategy"))
        yield factory

@pytest.fixture
def mock_execution_engine():
    """Mock Execution Engine untuk test integrasi."""
    mock = MagicMock()
    mock.submit_order.return_value = "ord_123"
    return mock

# ============================================================================
# ASYNC SUPPORT
# ============================================================================

@pytest.fixture(scope="session")
def event_loop() -> Generator:
    """
    Membuat instance event loop default untuk setiap test case.
    Diperlukan oleh pytest-asyncio.
    """
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()

# ============================================================================
# PYTEST CONFIGURATION
# ============================================================================

def pytest_configure(config):
    """Mendaftarkan marker kustom."""
    markers = [
        "slow: marks tests as slow (deselect with '-m \"not slow\"')",
        "integration: marks system integration tests",
        "unit: marks unit tests",
        "performance: marks latency/throughput tests"
    ]
    for marker in markers:
        config.addinivalue_line("markers", marker)

def pytest_collection_modifyitems(config, items):
    """Otomatis menambahkan marker 'unit' jika tidak ada marker lain."""
    for item in items:
        if "tests/unit" in str(item.fspath):
            item.add_marker(pytest.mark.unit)
        elif "tests/integration" in str(item.fspath):
            item.add_marker(pytest.mark.integration)
