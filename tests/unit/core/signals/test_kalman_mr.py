"""
UNIT TEST: THE BRAIN (Kalman Resilience Check)
Tujuan: Memastikan Strategy tidak crash saat menerima data 'Racun'.
Updated: Menggunakan SignalConfig object, bukan raw dict.
"""

import pytest
import numpy as np
from core.signals.strategies.kalman_mr import KalmanMeanReversion
from core.signals.types import MarketObservation, SignalType, SignalConfig # <--- Import SignalConfig

# Config Dummy (Raw Dictionary)
DUMMY_CONFIG_DICT = {
    "name": "TEST_STRAT",
    "volatility_window": 20, # Rename dari lookback
    "entry_z_score": 2.0,
    "exit_z_score": 0.5,
    "stop_loss_z": 4.0,
    "max_position": 1.0,
    "hedge_ratio": 1.0
}

@pytest.fixture
def brain():
    """Inisialisasi Strategy baru untuk setiap test"""
    # [FIX] Convert Dict -> SignalConfig Object
    config_res = SignalConfig.from_dict(DUMMY_CONFIG_DICT)
    
    if config_res.is_err():
        pytest.fail(f"Config setup failed: {config_res.unwrap_err()}")
        
    # Masukkan Object Config ke Strategy
    return KalmanMeanReversion(signal_config=config_res.unwrap())

def create_obs(ts, doge, btc):
    """Helper membuat MarketObservation cepat"""
    return MarketObservation(
        timestamp=ts,
        symbol="DOGE/USDT",
        source="TEST",
        data={
            "close_DOGE": doge,
            "close_BTC": btc,
            "volume": 1000.0
        }
    )

def test_brain_initialization(brain):
    """Cek apakah otak sehat saat lahir"""
    assert brain.name == "TEST_STRAT"
    # Cek internal state
    assert brain._internal_state is not None

def test_brain_warmup(brain):
    """Test 1: Cold Start"""
    for i in range(5):
        obs = create_obs(1000 + i*60000, 0.1, 30000.0)
        result = brain.evaluate_state(obs)
        assert result.is_ok()
        signal = result.unwrap()
        # Selama warmup, harus NEUTRAL
        assert signal.signal_type == SignalType.NEUTRAL

def test_poison_nan(brain):
    """Test 2: Serangan Racun (NaN / None)"""
    # 1. Init
    brain.evaluate_state(create_obs(1000, 0.1, 30000.0))
    
    # 2. Poison: None
    obs_poison = create_obs(2000, None, 30005.0)
    result = brain.evaluate_state(obs_poison)
    assert result.is_err() # Harusnya error gracefully

    # 3. Poison: NaN
    obs_nan = create_obs(3000, np.nan, 30010.0)
    result2 = brain.evaluate_state(obs_nan)
    assert result2.is_err()

def test_poison_zero(brain):
    """Test 3: Serangan Black Hole (Price = 0.0)"""
    brain.evaluate_state(create_obs(1000, 0.1, 30000.0))
    
    # Poison: Zero
    obs_zero = create_obs(2000, 0.0, 30000.0)
    result = brain.evaluate_state(obs_zero)
    assert result.is_err()

def test_sanity_extreme_value(brain):
    """Test 4: Stop Loss / Extreme Signal"""
    # 1. Warmup (isi history errors)
    for i in range(30):
        brain.evaluate_state(create_obs(1000+i, 0.1, 30000.0))
        
    # 2. Moon Event (Doge naik gila-gilaan)
    obs_moon = create_obs(50000, 0.5, 30000.0)
    
    result = brain.evaluate_state(obs_moon)
    assert result.is_ok()
    
    signal = result.unwrap()
    print(f"Moon Signal: {signal.signal_type} | Z: {signal.strength}")
    
    # Expect: SELL atau STOP
    valid_signals = [SignalType.SELL, SignalType.STOP]
    assert signal.signal_type in valid_signals
