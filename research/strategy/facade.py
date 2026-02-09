"""
Alasan: Di sini ada engine (Vectorized?), optimization (Shotgun/Spaces), dan pipeline. User (Researcher) biasanya hanya ingin: "Jalankan Backtest" atau "Optimasi Parameter". Mereka tidak mau merakit ShotgunOptimizer secara manual.

    Lokasi: research/strategy/facade.py

    Nama Class: ResearchLabFacade

    Tugas: run_backtest() dan run_optimization(). Facade ini mengatur wiring antara engine backtest dan algoritma optimasi.
"""
