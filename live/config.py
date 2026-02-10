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
    # [ACTION REQUIRED] Pastikan path ini valid di laptop Anda
    # Gunakan 'find data/raw ...' di terminal untuk memastikan
    "path_target": "data/raw/symbol=DOGE-USDT/interval=1m/year=2023/month=10/data.parquet",
    "path_ref":    "data/raw/symbol=BTC-USDT/interval=1m/year=2023/month=10/data.parquet",
    
    "symbol_traded": "DOGE/USDT", # Aset yang dieksekusi OMS
    "replay_speed": 0.001,        # Delay antar tick (0.001s = Fast Replay)
    "warmup_ticks": 200           # Jumlah data awal untuk pemanasan Kalman
}

# ==============================================================================
# 2. STRATEGY CONFIGURATION (OTAK)
# ==============================================================================
STRATEGY_CONFIG = {
    "type": "kalman_mr",      # Key untuk Factory (JANGAN UBAH)
    "name": "DOGE_Sniper_V1", # Nama unik strategi
    
    # --- PARAMETER SINYAL (The Trigger) ---
    "signal_params": {
        "name": "DOGE_Sniper_V1",
        
        # SENSITIVITAS (Agresif vs Konservatif)
        "entry_z_score": 1.25,    # Masuk saat spread menyimpang 1.25 deviasi (Agresif)
        "exit_z_score": 0.0,      # Keluar saat spread kembali ke 0 (Mean)
        
        # RISK MANAGEMENT
        "stop_loss_z": 5.0,       # Cut loss jika spread melebar gila-gilaan
        
        # SIZING
        "max_position": 10000.0,  # Max Holding (Unit DOGE)
        "hedge_ratio": 1.0,       # Estimasi Beta awal (DOGE vs BTC)
        
        # MATH LOGIC (Parameter Baru untuk menghapus hardcode di strategi)
        "volatility_window": 30   # Rolling window untuk hitung Z-Score (Dulu hardcoded 50)
    },
    
    # --- PARAMETER MATEMATIKA (The Engine) ---
    "math_params": {
        # MEASUREMENT NOISE (R): Seberapa percaya kita pada harga pasar?
        # R Besar (>0.1) = Anggap pasar berisik -> Filter jadi lambat/smooth
        # R Kecil (<0.01) = Anggap pasar akurat -> Filter jadi reaktif/gerigi
        "R": 0.5, 

        # PROCESS NOISE (Q): Seberapa cepat tren/korelasi berubah?
        # Q Besar (>0.01) = Korelasi labil -> Filter cepat adaptasi
        # Q Kecil (<0.0001) = Korelasi stabil -> Filter kaku (Bagus untuk Mean Reversion)
        "Q": 1e-5,
        
        "initial_value": 0.0,
        "adaptation_mode": "nis"
    }
}

# ==============================================================================
# 3. EXECUTION CONFIGURATION (OTOT)
# ==============================================================================
EXECUTION_CONFIG = {
    "initial_cash": 100_000.0, # Modal Awal (USD)
    "maker_fee": -0.0002,      # Rebate (jika ada)
    "taker_fee": 0.0004,       # Standard Fee Binance VIP 0 (0.04% taker approx)
    "slippage": 0.0001         # Estimasi slip harga eksekusi
}

# ==============================================================================
# VALIDASI PATH OTOMATIS (Safety Check)
# ==============================================================================
if not os.path.exists(DATA_CONFIG["path_target"]):
    print(f"⚠️ WARNING: File Target tidak ditemukan: {DATA_CONFIG['path_target']}")

if not os.path.exists(DATA_CONFIG["path_ref"]):
    print(f"⚠️ WARNING: File Reference tidak ditemukan: {DATA_CONFIG['path_ref']}")
