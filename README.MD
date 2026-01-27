# 🏛️ Arbitrage Lab: Statistical Arbitrage Engine

High-performance Statistical Arbitrage System menggunakan arsitektur modular ("City Method").
Sistem ini dirancang untuk menangani data High-Frequency (M1) dengan pipeline ingestion asinkronus dan penyimpanan berbasis Hive Partitioning.

## 🏗️ Architecture Overview

Sistem dibagi menjadi beberapa Node/Distrik:

1.  **Node A (Ingestion):** `research.ingestion`
    * **Adaptor:** CCXT (Async + Pagination) & Yahoo Finance.
    * **Storage:** Parquet dengan Hive Partitioning (`year=YYYY/month=MM`).
    * **Reliability:** Auto-retry, Rate Limiting, dan Error Handling (Result Pattern).

2.  **Node H (Repository):** `research.repository`
    * **Engine:** DuckDB (In-Memory / Zero-Copy).
    * **Fungsi:** Mengindeks jutaan baris data Parquet secara instan tanpa memuat ke RAM.
    * **Interface:** Menyediakan akses SQL yang Type-Safe ke layer aplikasi.

3.  **Node B (Processing) [WIP]:** `research.processing`
    * **Engine:** Polars.
    * **Fungsi:** Data Alignment, Cleaning, dan Feature Engineering (Log Returns).

## 📂 Project Structure

```text
arb-lab/
├── main.py                     # Orchestrator untuk Ingestion (Download Data)
├── check_db.py                 # Utility untuk inspeksi kesehatan Database
├── data/                       # Local Data Lake (Ignored by Git)
│   └── raw/                    # Hive Partitioned Parquet
├── logs/                       # Audit Trail & Error Logs
└── research/
    ├── shared/                 # Domain Objects (OHLCV) & Protocols
    ├── ingestion/              # Data Acquisition Modules
    └── repository/             # Data Access Layer (DuckDB)


## 🚀 How to Run

### 1. Setup Environment

```bash
# Clone repository
git clone <repository-url>
cd arb-lab

# Install dependencies
pip install -r requirements.txt
python main.py
python check_db.py

### Data Lake Stats (Current)
Timeframe: 1m (1 Menit)
Range: Jan 2023 - Jan 2026
Total Rows: ~3.200.000+
Format: Parquet ZSTD (Hive Partitioned)

### Tech Stack
Language: Python 3.10+
Database: DuckDB
Processing: Polars
Concurrency: Asyncio
Architecture: Hexagonal / Domain-Driven Design (Lite)
