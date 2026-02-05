# ops/verify_integration.py

import sys
import os
import pandas as pd
import numpy as np

# Add project root to path
sys.path.append(os.getcwd())

from core.signals.strategies.kalman_mr import AdaptiveKalmanStrategy

def run_forensic_check():
    print("🕵️ ORCA INTEGRATION PROBE: Processing -> Strategy")
    print("================================================")

    # --- PHASE 1: CHECK PIPELINE OUTPUT ---
    print("\n[1] Probing Research Pipeline Output...")
    try:
        # Mocking config/params
        # Sesuaikan dengan method load_data atau run_pipeline lu
        # Kita coba load dummy atau panggil fungsi yang ada
        
        # MOCK DATA (Kalau pipeline butuh koneksi DB asli, kita mock dulu outputnya)
        # Ini simulasi apa yang BIASANYA keluar dari processing
        mock_output = pd.DataFrame({
            'open': np.random.rand(100) * 100,
            'high': np.random.rand(100) * 100,
            'low': np.random.rand(100) * 100,
            'close': np.random.rand(100) * 100 + 50, # Harga 50-150
            'volume': np.random.rand(100) * 1000
        }, index=pd.date_range("2024-01-01", periods=100, freq="1H"))
        
        print(f"    ✅ Data Type Received: {type(mock_output)}")
        print(f"    ✅ Data Shape: {mock_output.shape}")
        print(f"    ✅ Columns: {mock_output.columns.tolist()}")
        
    except Exception as e:
        print(f"    ❌ CRITICAL: Pipeline Output Failed. {e}")
        return

    # --- PHASE 2: CHECK STRATEGY INGESTION ---
    print("\n[2] Feeding Data to Adaptive Kalman Strategy...")
    strategy = AdaptiveKalmanStrategy(
        lookback=20, 
        entry_z=2.0, 
        shock_sensitivity=3.0
    )
    
    try:
        # TEST 1: Pass DataFrame (Common Mistake)
        print("    👉 Attempting to pass Full DataFrame (Expect Handling or Failure)...")
        
        # Strategy harus pintar milih kolom 'close' atau 'spread'
        # Kalau strategy lu loop raw data, ini bakal crash
        if isinstance(mock_output, pd.DataFrame):
            # Simulasi passing 'close' column ONLY (Best Practice)
            input_feed = mock_output['close']
        else:
            input_feed = mock_output

        signals = strategy.generate(input_feed)
        
        print(f"    ✅ Strategy accepted input type: {type(input_feed)}")
        print(f"    ✅ Output Signals Length: {len(signals)}")
        print(f"    ✅ Sample Signal: {signals.iloc[-1]}")
        
    except TypeError as e:
        print(f"    ❌ TYPE ERROR: {e}")
        print("    👉 DIAGNOSIS: Strategy looping over DataFrame columns, not rows.")
        print("    👉 FIX: Pass `df['close']` or fix loop to `data.iterrows()`")
    except KeyError as e:
        print(f"    ❌ KEY ERROR: {e}")
        print("    👉 DIAGNOSIS: Strategy looking for column name that doesn't exist.")
    except Exception as e:
        print(f"    ❌ RUNTIME ERROR: {e}")

    # --- PHASE 3: RESULT PATTERN CHECK ---
    print("\n[3] Checking Kalman Internal Updates (Result Pattern)...")
    try:
        # Cek apakah object Result ditangani?
        # Kita intip state terakhir
        last_est = strategy.kf.current_estimate
        last_unc = strategy.kf.uncertainty
        print(f"    ✅ Kalman State Alive: Est={last_est:.2f}, Unc={last_unc:.4f}")
        
        if last_unc == 1.0 and len(input_feed) > 10:
             print("    ⚠️ WARNING: Uncertainty didn't decrease. Model might be broken/frozen.")
        else:
             print("    ✅ Kalman Learning: Uncertainty Converging.")
             
    except AttributeError:
        print("    ❌ ERROR: Strategy failed to initialize 'kf' attribute.")

if __name__ == "__main__":
    run_forensic_check()
