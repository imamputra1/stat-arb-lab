"""
UNIT TEST: SIGNAL FACTORY (OPTIMIZED)
Location: tests/unit/test_factory.py
Focus: Parsing, Validation, Instantiation, & Error Handling
"""

import pytest
from typing import Dict, Any

# Import Core Components
from core.signals import (
    create_strategy,
    validate_config,
    SignalFactory,
    get_factory
)
from core.signals.strategies.kalman_mr import KalmanMeanReversion
from core.math.kalman import AdaptationMode

# --- SHARED FIXTURES ---

@pytest.fixture
def base_config() -> Dict[str, Any]:
    """Config valid standar untuk testing"""
    return {
        "type": "kalman_mr",
        "name": "BTC_TEST_V1",
        "signal_params": {
            "entry_z_score": 2.0,
            "exit_z_score": 0.5,
            "stop_loss_z": 4.0,
            "hedge_ratio": 1.0,
            "max_position": 1.0
        },
        "math_params": {
            "R": 0.001,
            "Q": 0.0001,
            "adaptation_mode": "robust",  # Test konversi String -> Enum
            "initial_value": 100.0
        }
    }

# --- HAPPY PATH TESTS ---

def test_create_strategy_success(base_config):
    """Test instansiasi strategi yang sukses dengan parameter lengkap"""
    result = create_strategy(base_config)
    
    assert result.is_ok(), f"Creation failed: {result.unwrap_err()}"
    strategy = result.unwrap()
    
    # 1. Validasi Tipe Object
    assert isinstance(strategy, KalmanMeanReversion)
    assert strategy.name == "BTC_TEST_V1"
    
    # 2. Validasi Konversi Parameter (String -> Enum)
    assert strategy.math_config.adaptation_mode == AdaptationMode.ROBUST
    
    # 3. Validasi Logic Params
    assert strategy.sig_config.entry_z_score == 2.0

@pytest.mark.parametrize("alias, target_attr, val", [
    ("entry_threshold", "entry_z_score", 3.5),
    ("exit_threshold", "exit_z_score", 0.8),
    ("stop_loss", "stop_loss_z", 5.0),
])
def test_alias_parameter_parsing(base_config, alias, target_attr, val):
    """Test apakah parser mengenali nama alias (backward compatibility)"""
    # Modifikasi config untuk pakai alias
    config = base_config.copy()
    config["signal_params"] = {alias: val, "hedge_ratio": 1.0, "max_position": 1.0}
    
    result = create_strategy(config)
    assert result.is_ok()
    
    strategy = result.unwrap()
    # Pastikan value masuk ke atribut yang benar
    assert getattr(strategy.sig_config, target_attr) == val

# --- ERROR HANDLING TESTS (PARAMETRIZED) ---

@pytest.mark.parametrize("invalid_conf, expected_error", [
    # Case 1: Tidak ada field 'type'
    ({"name": "NoType"}, "Missing required field 'type'"),
    
    # Case 2: Tipe strategi gaib
    ({"type": "ghost_strategy"}, "Unknown strategy type"),
    
    # Case 3: Parameter sinyal salah tipe (String bukan Float)
    ({
        "type": "kalman_mr", 
        "signal_params": {"entry_z_score": "bukan_angka"}
    }, "Invalid value"),
])
def test_factory_error_handling(invalid_conf, expected_error):
    """Test berbagai skenario input sampah"""
    result = create_strategy(invalid_conf)
    assert result.is_err()
    assert expected_error in result.unwrap_err()

# --- UTILITY & INFRASTRUCTURE TESTS ---

def test_validate_config_dry_run(base_config):
    """Test validasi tanpa instansiasi object (lebih cepat)"""
    # 1. Config Valid
    assert validate_config(base_config).is_ok()
    
    # 2. Config Invalid
    assert validate_config({"type": "unknown"}).is_err()

def test_get_default_config():
    """Test pengambilan blueprint default"""
    res = SignalFactory.get_default_config("kalman_mr")
    assert res.is_ok()
    
    conf = res.unwrap()
    assert conf["type"] == "kalman_mr"
    # Pastikan default adaptation mode adalah string 'nis' (sesuai factory.py)
    assert conf["math_params"]["adaptation_mode"] == "nis"

def test_factory_singleton():
    """Test pola Singleton (Hemat Memori)"""
    f1 = get_factory()
    f2 = get_factory()
    assert f1 is f2
