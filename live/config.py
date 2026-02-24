"""
CENTRAL COMMAND CONFIGURATION
Location: live/config.py
Desc: Single Source of Truth. Mengatur Amunisi (Data), Otak (Strategy), dan Otot (Execution).
      Gunakan file ini untuk Tuning & Optimization.
"""

import os

# ==============================================================================
# 1. DATA CONFIGURATION (AMUNISI)
# ==============================================================================
DATA_CONFIG = {
    # [ACTION REQUIRED] Sesuaikan path ini dengan lokasi data Parquet di laptop Anda
    "path_target": "data/raw/symbol=DOGE-USDT/interval=1m/year=2023/month=10/data.parquet",
    "path_ref":    "data/raw/symbol=BTC-USDT/interval=1m/year=2023/month=10/data.parquet",
    
    "symbol_traded": "DOGE/USDT", # Aset yang dieksekusi OMS
    "replay_speed": 0.0,        # Delay antar tick (0.001s = Fast Replay)
    "warmup_ticks": 5         # Jumlah data awal untuk pemanasan Kalman Filter
}

# ==============================================================================
# 2. STRATEGY CONFIGURATION (OTAK)
# ==============================================================================
STRATEGY_CONFIG = {
    "type": "kalman_mr",      # Key untuk Factory (JANGAN UBAH, mapping ke KalmanMeanReversion)
    "name": "DOGE_Sniper_V1", # Nama unik strategi untuk logging
    
    # --- A. PARAMETER SINYAL (The Trigger / Logic) ---
    # Mengatur KAPAN kita masuk/keluar pasar berdasarkan output matematis.
    "signal_params": {
        "name": "DOGE_Sniper_V1",
        
        # SENSITIVITAS (Agresif vs Konservatif)
        "entry_z_score": 0.1,     # Masuk saat harga menyimpang 2 Standar Deviasi (95% confidence)
        "exit_z_score": 0.0,      # Keluar saat harga kembali ke Mean (Wajar)
        
        # RISK MANAGEMENT (Safety Net)
        "stop_loss_z": 10.0,       # Cut loss jika spread melebar ekstrem (>4 sigma)
        
        # SIZING & HEDGING
        "max_position": 10.0,   # Max Holding (Unit Asset)
        "hedge_ratio": 1.0,       # Estimasi Beta awal (DOGE vs BTC)
        
        # DYNAMIC WINDOW
        "volatility_window": 30   # Rolling window untuk menghitung volatilitas Z-Score
    },
    
    # --- B. PARAMETER MATEMATIKA (The Engine / Kernel) ---
    # Mengatur BAGAIMANA strategi "melihat" pasar. Disuntikkan ke core/math/kalman.py.
    "math_params": {
        # 1. MEASUREMENT NOISE (R): "Seberapa percaya kita pada harga pasar saat ini?"
        #    - R Besar (> 0.5)  = Anggap pasar banyak noise -> Filter jadi lambat/smooth.
        #    - R Kecil (< 0.01) = Anggap pasar sangat akurat -> Filter jadi reaktif/gerigi.
        "R": 0.05, 

        # 2. PROCESS NOISE (Q): "Seberapa cepat tren/korelasi aset berubah?"
        #    - Q Besar (> 1e-3)   = Menganggap tren labil -> Cepat adaptasi perubahan.
        #    - Q Kecil (< 1e-5)   = Menganggap tren stabil -> Filter kaku (Bagus untuk Mean Reversion).
        "Q": 1e-1,
        
        "initial_value": 0.0,     # Tebakan awal spread
        
        # 3. ADAPTIVE LOGIC (Fitur Canggih)
        #    - 'nis' (Normalized Innovation Squared): Otomatis mendeteksi market shock.
        #    - 'none': Kalman filter standar (statis).
        "adaptation_mode": "nis", 
        
        "shock_threshold": 4.0,   # Jika error > 4 sigma, anggap ada Shock
        "max_boost_factor": 10.0,  # Naikkan Q max 10x saat shock terjadi
        "state_dim": 2
    }
}

# ==============================================================================
# 3. EXECUTION CONFIGURATION (OTOT)
# ==============================================================================
EXECUTION_CONFIG = {
    "mode": "PAPER",           # PAPER / LIVE
    "initial_cash": 500.0, # Modal Awal (USD)
    
    # Fee Structure (Simulasi Binance VIP 0)
    "maker_fee": -0.0002,      # Rebate (jika ada, negatif berarti dapat duit)
    "taker_fee": 0.0004,       # Fee standar 0.04%
    
    "slippage": 0.0001         # Estimasi slip harga (0.01%)
}

# ==============================================================================
# 4. RISK CONFIGURATION (PERISAI)
# ==============================================================================
RISK_CONFIG = {
    "max_drawdown": 0.15,          # Stop trading jika equity turun 10%
    "max_exposure_per_asset": 0.5, # Maksimal 50% modal di satu aset
    "circuit_breaker_ms": 1000     # Stop jika latency > 1000ms
}

if not os.path.exists(DATA_CONFIG["path_target"]): pass

# ==============================================================================
# VALIDASI PATH OTOMATIS (Safety Check)
# ==============================================================================
# Script ini akan memberi peringatan dini jika file data tidak ditemukan
if not os.path.exists(DATA_CONFIG["path_target"]):
    print(f"\n[CONFIG] ⚠️ WARNING: File Target tidak ditemukan: {DATA_CONFIG['path_target']}")
    print("         Mohon cek path di DATA_CONFIG['path_target']")

if not os.path.exists(DATA_CONFIG["path_ref"]):
    print(f"\n[CONFIG] ⚠️ WARNING: File Reference tidak ditemukan: {DATA_CONFIG['path_ref']}")
    print("         Mohon cek path di DATA_CONFIG['path_ref']")
