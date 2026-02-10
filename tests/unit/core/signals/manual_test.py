"""
INTEGRATION TEST: KALMAN STRATEGY
Location: tests/unit/core/signals/test_kalman_integration.py
Desc: Memastikan Factory, Config, dan Strategy Logic terintegrasi dengan benar.
      Menggunakan 'assert' untuk validasi otomatis.
"""

import pytest
import pandas as pd
import numpy as np
import sys
import os

# [PATH HACK] Agar bisa import core
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../../')))

from core.signals.factory import FactoryManager
from core.signals.types import SignalType

# --- FIXTURES (Data Persiapan) ---

@pytest.fixture
def raw_config():
    """Config dummy untuk testing"""
    return {
        "type": "kalman_mr",
        "name": "Kalman_Pytest_Unit",
        "signal_params": {
            "entry_z_score": 1.5,
            "exit_z_score": 0.5,
            "stop_loss_z": 3.0,
            "volatility_window": 20,
            "max_position": 1.0,
            "version": "1.0.TEST"
        },
        "math_params": {
            "R": 0.1,
            "Q": 0.01,
            "adaptation_mode": "nis"
        }
    }

@pytest.fixture
def dummy_market_data():
    """Data sinusoidal yang pasti memicu sinyal"""
    x = np.linspace(0, 4*np.pi, 200)
    prices_y = 100 + 10 * np.sin(x) # DOGE naik turun
    prices_x = [100] * 200          # BTC diam
    
    return pd.DataFrame({
        'timestamp': pd.date_range(start='2024-01-01', periods=200, freq='1min'),
        'close_DOGE': prices_y,
        'close_BTC': prices_x
    })

# --- TESTS ---

def test_factory_creation(raw_config):
    """Test 1: Apakah Factory berhasil merakit strategi?"""
    factory = FactoryManager().factory
    strategy_res = factory.create_from_raw(raw_config)
    
    # Assert Result is OK
    assert strategy_res.is_ok(), f"Factory gagal: {strategy_res.unwrap_err()}"
    
    strategy = strategy_res.unwrap()
    
    # Assert Parameter Ter-inject dengan benar
    assert strategy.name == "Kalman_Pytest_Unit"
    assert strategy.sig_config.volatility_window == 20
    assert strategy.math_config.R == 0.1

def test_signal_generation_logic(raw_config, dummy_market_data):
    """Test 2: Apakah Strategi menghasilkan sinyal BUY/SELL yang valid?"""
    factory = FactoryManager().factory
    strategy = factory.create_from_raw(raw_config).unwrap()
    
    # Run Batch Processing
    result_res = strategy.generate_signals(dummy_market_data)
    assert result_res.is_ok()
    
    result_df = result_res.unwrap()
    
    # 1. Pastikan kolom sinyal ada
    assert 'signal_type' in result_df.columns
    assert 'z_score' in result_df.columns
    
    # 2. Filter Sinyal Aktif
    signals = result_df[result_df['signal_type'] != SignalType.NEUTRAL.value]
    
    # Harusnya ada sinyal karena data kita sinusoidal ekstrem
    assert len(signals) > 0, "Strategi mandul! Tidak ada sinyal padahal data volatil."
    
    # 3. Sanity Check Logic
    # Ambil sampel sinyal SELL
    sell_signals = result_df[result_df['signal_type'] == SignalType.SELL.value]
    if not sell_signals.empty:
        sample = sell_signals.iloc[0]
        # SELL harusnya terjadi saat Z-Score Positif (Harga ketinggian)
        assert sample['z_score'] > 0, f"Logic Terbalik! SELL di Z-Score negatif: {sample['z_score']}"

    # Ambil sampel sinyal BUY
    buy_signals = result_df[result_df['signal_type'] == SignalType.BUY.value]
    if not buy_signals.empty:
        sample = buy_signals.iloc[0]
        # BUY harusnya terjadi saat Z-Score Negatif (Harga kedalaman)
        assert sample['z_score'] < 0, f"Logic Terbalik! BUY di Z-Score positif: {sample['z_score']}"

    print(f"\n✅ Test Passed! Generated {len(signals)} signals.")
