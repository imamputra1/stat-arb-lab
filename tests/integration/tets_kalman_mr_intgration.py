"""
INTEGRATION TEST: KALMAN MEAN REVERSION STRATEGY
Location: tests/integration/test_kalman_mr_integration.py
Focus: End-to-End verification (Data -> Motherboard -> Math Kernel -> Signal)
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# Import via Facade (Sesuai Request)
from core.signals.strategies.kalman_mr import KalmanMeanReversion
from core.signals.types import SignalConfig, SignalType, SignalEvent, MarketObservation
from core.math.kalman import KalmanConfig, AdaptationMode  # <--- INI KUNCINYA

# --- FIXTURES ---
@pytest.fixture
def strategy_setup():
    """Rakit Motherboard dengan Config Standard"""
    math_conf = KalmanConfig(
        R=1e-4, 
        Q=1e-5, 
        adaptation_mode=AdaptationMode.NIS_THRESHOLD,
        initial_value=0.0
    )
    
    # Entry di 2.0 sigma, Exit di 0.5 sigma
    sig_conf = SignalConfig(
        name="KalmanMR_Test_v1",
        entry_z_score=2.0,
        exit_z_score=0.5,
        stop_loss_z=5.0,
        hedge_ratio=1.0,
        max_position=1.0
    )
    
    strategy = KalmanMeanReversion(math_conf, sig_conf)
    return strategy

@pytest.fixture
def market_data_scenario():
    """
    Bikin Skenario Pasar Buatan:
    1. Warmup (0-20): Noise
    2. Locked (20-50): Perfect Correlation (Spread ~ 0)
    3. Divergence (50-60): Asset Y Naik Gila (Spread Melebar Positif) -> Harusnya SELL
    """
    n_points = 60
    base_price = 100.0
    
    # Time index
    base_time = datetime(2024, 1, 1, 10, 0, 0)
    timestamps = [base_time + timedelta(minutes=i) for i in range(n_points)]
    
    # Asset X (Independen) - Jalan santai
    x_prices = np.random.normal(base_price, 0.1, n_points)
    
    # Asset Y (Dependen)
    y_prices = x_prices.copy() # Awalnya sinkron
    
    # Inject Divergence di 10 candle terakhir
    # Y naik drastis (Spread jadi Mahal)
    y_prices[50:] = y_prices[50:] * 1.05  # Naik 5%
    
    df = pd.DataFrame({
        'timestamp': pd.to_datetime(timestamps), # Keep as datetime obj for DataFrame
        'close_Y': y_prices,
        'close_X': x_prices
    })
    
    return df

# --- INTEGRATION TESTS ---

def test_initialization_compliance(strategy_setup):
    """Cek apakah Motherboard nyala dan Config terpasang benar"""
    strat = strategy_setup
    assert strat.name == "KalmanMR_Test_v1"
    assert strat.math_config.R == 1e-4
    assert strat._warmup_count == 0
    # Pastikan monitor aktif
    assert hasattr(strat, 'monitor')

def test_batch_processing_flow(strategy_setup, market_data_scenario):
    """
    Test Jalur A (Research/Backtest): generate_signals(df)
    """
    strat = strategy_setup
    df = market_data_scenario
    
    # Execute Batch
    result = strat.generate_signals(df)
    
    # 1. Cek Result Wrapper
    assert result.is_ok(), f"Batch failed: {result.unwrap_err()}"
    res_df = result.unwrap()
    
    # 2. Cek Struktur Output
    assert 'signal_type' in res_df.columns
    assert 'z_score' in res_df.columns
    assert len(res_df) == len(df)


    
    # Ambil data sejak divergence dimulai (index 50)
    divergence_window = res_df.iloc[50:]
    
    # Cari keberadaan sinyal SELL (value = 2)
    sell_signals = divergence_window[divergence_window['signal_type'] == SignalType.SELL.value]
    
    print("\n[Batch] Divergence Window Z-Scores Sample:\n", divergence_window['z_score'].head(5).values)
    print("[Batch] Signals Found in Window:", divergence_window['signal_type_name'].unique())
    
    # Assert 1: Strategi harus sempat mendeteksi peluang SELL saat harga loncat
    assert not sell_signals.empty, "Strategy failed to trigger SELL during divergence event (Adaptation logic check)"
    
    # Assert 2: Z-Score harus sempat melonjak tinggi (> 2.0)
    max_z = divergence_window['z_score'].max()
    assert max_z > 2.0, f"Z-Score did not spike high enough. Max: {max_z}"
def test_live_processing_flow(strategy_setup, market_data_scenario):
    """
    Test Jalur B (Live Trading): evaluate_state(obs)
    Mensimulasikan data streaming tick-by-tick.
    """
    strat = strategy_setup
    df = market_data_scenario
    
    signals = []

    triggered_sell = False
    
    print("\n[Live Stream Simulation]")
    
    for idx, row in df.iterrows():
        # Simulasi Payload dari WebSocket
        obs_dict = {
            'timestamp': int(row['timestamp'].timestamp() * 1000),
            'close_Y': row['close_Y'],
            'close_X': row['close_X']
        }
        
        # Bungkus ke MarketObservation (atau biarkan dict, strategy handle keduanya)
        obs = MarketObservation(
            timestamp=obs_dict['timestamp'],
            data=obs_dict,
            source="pytest_sim"
        )
        
        # Execute Live Logic
        res = strat.evaluate_state(obs)
        assert res.is_ok()
        
        evt: SignalEvent = res.unwrap()
        signals.append(evt)
        
        # Debug print saat divergence mulai
        if idx >= 49:
            print(f"Tick {idx}: Z={evt.strength:.2f} -> {evt.signal_type.name}")

        # Check trigger: Apakah SELL pernah muncul?
        if evt.signal_type == SignalType.SELL:
            triggered_sell = True

    # 4. Validasi Akhir
    # Pastikan strategi sempat berteriak SELL saat harga naik
    assert triggered_sell is True, "Live strategy failed to detect SELL signal"
    
    # Validasi Metadata (ambil sample terakhir)
    last_evt = signals[-1]
    assert 'uncertainty' in last_evt.metadata
    assert last_evt.metadata['warmup_complete'] is True
def test_performance_telemetry(strategy_setup, market_data_scenario):
    """Cek apakah Monitor merekam detak jantung strategi"""
    strat = strategy_setup
    
    # Run batch
    strat.generate_signals(market_data_scenario)
    
    # Ambil report
    report = strat.get_performance_summary()
    
    print("\n[Telemetry Report]", report)
    
    assert report['total_observations'] == 60
    assert report['avg_batch_latency'] > 0
    assert 'sharpe_ratio' in report
    
    # Pastikan tidak ada error internal
    assert report.get('filter_errors', 0) == 0
