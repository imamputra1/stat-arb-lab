# Stat-Arb-Lab (Statistical Arbitrage Laboratory)

**Stat-Arb-Lab** adalah sebuah *framework trading* algoritmik berbasis Python yang dirancang khusus untuk penelitian, pengembangan, dan eksekusi strategi *Statistical Arbitrage*. 

Sistem ini dibangun dengan arsitektur yang modular, memisahkan secara jelas antara lingkungan riset (*backtesting*, *data ingestion*, eksplorasi) dan lingkungan eksekusi produksi (*live trading*, *risk management*, OMS).

---
## Fitur Utama

- ** Core Trading Logic**: Dilengkapi dengan modul matematika dan sinyal *built-in*, termasuk implementasi strategi *Kalman Filter Mean Reversion* (`kalman_mr.py`).
- ** Research Environment**: Ekosistem riset yang tangguh menggunakan Jupyter Notebooks, *data ingestion* (mendukung Yahoo Finance), dan penyimpanan berbasis *Parquet/DuckDB*.
- ** Live Execution Engine**: Mesin eksekusi *real-time* dengan *Order Management System* (OMS) dan pelaporan (*Reporter*).
- ** Guardian & Risk Management**: Sistem manajemen risiko yang ketat untuk sesi *live*, dilengkapi dengan fitur *Circuit Breaker*, *Monitor*, dan *Sanitizer* untuk melindungi modal.
- ** Modern Tooling**: Dikelola menggunakan `uv` untuk resolusi *dependency* Python yang sangat cepat.

---
## Struktur Direktori

Proyek ini terorganisir dalam beberapa modul utama:

- `/core/`: Jantung dari aplikasi. Berisi logika perhitungan matematika (`/math`), manajemen risiko (`/risk`), pembuatan sinyal (`/signals`), dan Sistem Manajemen Pesanan/OMS (`/execution/oms`).
- `/live/`: Modul untuk eksekusi perdagangan secara langsung. Berisi *Engine* utama (`engine.py`), *Stream* data, dan sistem pelindung (`/guardian`).
- `/research/`: Laboratorium untuk *Quantitative Researcher*. Berisi *pipeline* analisis, *data ingestion*, *Jupyter Notebooks* untuk validasi ide, dan skrip *backtesting* (`/strategy`).
- `/ops/`: Kumpulan skrip operasional dan *maintenance* untuk menjaga kesehatan sistem (misal: *fix imports*, *check environment*).
- `main.py`: Titik masuk (*entry point*) utama untuk menjalankan aplikasi.
- `config.yaml`: File konfigurasi utama untuk mengatur parameter strategi dan sistem.

---
##  Prasyarat (Prerequisites)

Sebelum memulai, pastikan sistem kamu memiliki perangkat lunak berikut:

1. **Python**: Pastikan versi Python yang sesuai sudah terinstal (sesuai dengan `.python-version`).
2. **uv**: Proyek ini menggunakan `uv` sebagai *package manager* (terlihat dari `uv.lock` dan `pyproject.toml`).
   - Install `uv` (jika belum): `curl -LsSf https://astral.sh/uv/install.sh | sh` (MacOS/Linux) atau baca dokumentasi resmi Astral.

---
## 🚀 Instalasi & Persiapan

1. **Clone repositori ini:**
   ```bash
   git clone <url-repositori-kamu>
   cd stat-arb-lab
   ```

2. **Sinkronisasi Dependency:**
Gunakan uv untuk membuat virtual environment dan menginstal semua paket yang dibutuhkan secara otomatis:
   ```bash
   uv sync
   ```

3. Konfigurasi:
Sesuaikan parameter API, batas risiko, dan pengaturan database pada file config.yaml sebelum menjalankan aplikasi.

---
## Cara Penggunaan (Usage)

1. Mode Riset & Analisis
Untuk melakukan riset, kamu bisa menelusuri folder /research/notebooks/ dan menjalankan Jupyter Notebook (seperti Lake_Inspector.ipynb atau Signal_Sandbox.ipynb) untuk melihat visualisasi dan memvalidasi model Statistical Arbitrage (ADF Test, Half-life, dll).

2. Mode Live Execution
Untuk menjalankan bot dalam mode eksekusi langsung (dengan guardian pelindung aktif), jalankan titik masuk utamanya:
```python
uv run main.py
```

---
Disklaimer

Aplikasi ini dibuat murni untuk tujuan riset, edukasi, dan eksperimen kuantitatif. Segala bentuk kerugian finansial yang terjadi akibat penggunaan software ini dalam perdagangan (trading) di pasar nyata adalah tanggung jawab pengguna sepenuhnya. Pastikan untuk selalu melakukan forward-testing dan paper trading sebelum mempertaruhkan modal asli.
