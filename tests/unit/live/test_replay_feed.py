"""
UNIT TEST: REPLAY STREAMER
Tujuan: Memverifikasi logika pipeline (Load -> Tick -> Aggregate -> Yield)
        tanpa bergantung pada file fisik (Mocking ParquetLoader).
"""

import pytest
import pandas as pd
from unittest.mock import patch
from live.stream.replay import ReplayStreamer
from core.signals.types import MarketObservation

# Data Dummy untuk Test
# Skenario: 3 Tick. 
# Tick 1 & 2 ada di menit pertama (ts 60000 & 90000).
# Tick 3 ada di menit kedua (ts 120001). 
# Harapannya: Masuknya Tick 3 akan memicu Aggregator menutup candle menit pertama.
MOCK_DATA = {
    'timestamp': [60000, 90000, 120001], # ms
    'close_DOGE-USDT': [0.10, 0.15, 0.20],
    'vol_DOGE-USDT':   [100.0, 200.0, 100.0],
    'close_BTC-USDT':  [30000.0, 30005.0, 30010.0],
    'vol_BTC-USDT':    [1.0, 1.5, 0.5]
}

@pytest.fixture
def mock_config():
    return {
        "name": "UNIT_TEST_SCENARIO",
        "start": "2023-01-01",
        "end": "2023-01-02",
        "chaos": {}
    }

@patch('live.stream.replay.ParquetLoader') # Mock class ParquetLoader di module replay
def test_replay_initialization(mock_loader, mock_config):
    """
    Test 1: Memastikan Streamer bisa init dan memanggil Loader dengan parameter benar.
    """
    # Setup Mock return value
    mock_loader.load_pair.return_value = pd.DataFrame(MOCK_DATA)

    # Init Streamer
    streamer = ReplayStreamer(mock_config)

    # Assertions
    assert streamer.df is not None
    assert len(streamer.df) == 3
    
    # Pastikan Loader dipanggil dengan simbol yang benar
    mock_loader.load_pair.assert_called_once()
    call_args = mock_loader.load_pair.call_args[1]
    assert call_args['target_symbol'] == "DOGE/USDT"
    assert call_args['ref_symbol'] == "BTC/USDT"

@patch('live.stream.replay.ParquetLoader')
def test_replay_stream_flow(mock_loader, mock_config):
    """
    Test 2: Memastikan logika Stream menghasilkan MarketObservation yang valid.
    """
    # Setup Data Mock
    mock_loader.load_pair.return_value = pd.DataFrame(MOCK_DATA)
    streamer = ReplayStreamer(mock_config)

    # Jalankan Stream
    # Kita collect hasilnya ke dalam list
    results = list(streamer.stream(delay_sec=0.0))

    # Assertions Logika Aggregasi
    # Dari 3 tick data mock di atas, Tick ke-3 memicu close candle Tick 1&2.
    # Jadi kita minimal harus dapat 1 Observation yang matang.
    assert len(results) >= 1
    
    obs = results[0]
    
    # 1. Cek Tipe Data
    assert isinstance(obs, MarketObservation)
    assert obs.source == "AGGREGATED_FUTURES"
    assert obs.symbol == "DOGE/USDT"

    # 2. Cek Kebenaran Data (Close price candle adalah tick terakhir di menit itu)
    # Menit 1 (60000-119999): Tick terakhir adalah row ke-2 (0.15 dan 30005.0)
    assert obs.data['close_DOGE'] == 0.15
    assert obs.data['close_BTC'] == 30005.0
    
    # 3. Cek Volume (Harus diakumulasi)
    # Vol DOGE row 1 (100) + row 2 (200) = 300
    assert obs.data['volume'] == 300.0

@patch('live.stream.replay.ParquetLoader')
def test_replay_empty_data(mock_loader, mock_config):
    """
    Test 3: Memastikan tidak crash jika data kosong.
    """
    # Setup Mock return empty DF
    mock_loader.load_pair.return_value = pd.DataFrame()
    
    streamer = ReplayStreamer(mock_config)
    
    # Jalankan Stream
    results = list(streamer.stream())
    
    # Assert
    assert len(results) == 0  # Tidak boleh yield apapun, tapi tidak boleh error

@patch('live.stream.replay.ParquetLoader')
def test_replay_loader_error(mock_loader, mock_config):
    """
    Test 4: Memastikan graceful handling jika Loader error (misal file missing).
    """
    # Setup Mock raise Exception
    mock_loader.load_pair.side_effect = Exception("File Corrupt")
    
    # Ini tidak boleh raise exception keluar, tapi harus log error dan set df=None
    streamer = ReplayStreamer(mock_config)
    
    assert streamer.df is None
    
    # Stream juga harus aman (return langsung)
    results = list(streamer.stream())
    assert len(results) == 0
