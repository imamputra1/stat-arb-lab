#!/usr/bin/env fish

echo "🐳 INITIATING PROJECT ORCA MIGRATION (FISH SHELL EDITION)..."

# --- 1. MEMBUAT SKELETON FOLDER BARU (Blueprint V9.0) ---
echo "🏗️  Constructing Directory Skeleton..."

mkdir -p config
mkdir -p core/math
mkdir -p core/signals
mkdir -p core/risk
mkdir -p core/data
mkdir -p research/ingestion
mkdir -p research/processing/alignment
mkdir -p research/processing/validation
mkdir -p research/processing/transformation
mkdir -p research/processing/features
mkdir -p research/strategy/engine
mkdir -p research/strategy/optimization
mkdir -p research/analysis/judgment
mkdir -p live/stream
mkdir -p live/guardian
mkdir -p live/execution
mkdir -p storage/logs
mkdir -p storage/silver_lake
mkdir -p storage/brown_lake
mkdir -p storage/quantum_vault
mkdir -p storage/duckdb
mkdir -p tests/unit
mkdir -p tests/integration
mkdir -p utils

# --- 2. MEMINDAHKAN FILE LAMA KE RUMAH BARU (The Great Migration) ---
echo "🚚 Moving Assets to New Coordinates..."

# [NODE O] Optimization Tools
# Pindahkan dari research/strategy/optimization/ (jika ada)
if test -f research/strategy/optimization/shotgun.py
    # Sudah di tempat yang benar, tapi kita pastikan strukturnya rapi
    echo "   - Shotgun Engine checked."
end
# Catatan: File optimization (shotgun, war_room, spaces, objective, storage, dashboard)
# sudah berada di research/strategy/optimization/ pada struktur lama, jadi tidak perlu dipindah.

# [NODE S] Pipeline Orchestrator
# Lokasi lama & baru sama: research/strategy/pipeline.py
if test -f research/strategy/pipeline.py
    echo "   - Strategy Pipeline checked."
end

# [NODE S] Vectorized Engine
# Lokasi lama & baru sama: research/strategy/engine/vectorized.py
if test -f research/strategy/engine/vectorized.py
    echo "   - Vectorized Engine checked."
end

# [NODE A] Data Loader (Migration: Strategy/Data -> Research/Ingestion)
if test -f research/strategy/data/loader.py
    mv research/strategy/data/loader.py research/ingestion/loader.py
    echo "   -> Loader moved to Ingestion Node."
end

# [CORE] Kalman Model (Migration: Models/Library -> Core/Math)
if test -f research/strategy/models/library/kalman.py
    mv research/strategy/models/library/kalman.py core/math/kalman.py
    echo "   -> Kalman Model moved to Core Math."
end

# [CORE] Signals (Migration: Strategy/Signals -> Core/Signals)
if test -d research/strategy/signals
    # Pindahkan isi folder, bukan foldernya
    cp -r research/strategy/signals/* core/signals/
    # Hapus folder lama agar tidak bingung (opsional, tapi bersih)
    rm -rf research/strategy/signals
    echo "   -> Signal Logic moved to Core Brain."
end

# [SHARED] Result Pattern
if test -f research/shared.py
    cp research/shared.py core/shared.py
    echo "   -> Shared Utilities moved to Core Kernel."
end

# --- 3. MEMBUAT FILE KOSONG & KONFIGURASI (Missing Pieces) ---
echo "✨ Generating Missing Protocols & Configs..."

# Root Files
touch .env
touch .gitignore
touch pyproject.toml
touch config.yaml
touch cli.py
touch orca_cycle.fish

# Config Module
touch config/__init__.py
touch config/loader.py
touch config/logging.py

# Core Module
touch core/__init__.py
touch core/math/__init__.py
touch core/math/statistics.py
touch core/math/returns.py
touch core/signals/__init__.py
touch core/risk/__init__.py
touch core/risk/sizing.py
touch core/data/__init__.py
touch core/data/protocols.py

# Research Module
touch research/__init__.py
touch research/ingestion/__init__.py
touch research/processing/__init__.py
touch research/processing/pipeline.py
touch research/processing/alignment/__init__.py
touch research/processing/validation/__init__.py
touch research/processing/transformation/__init__.py
touch research/processing/features/__init__.py
touch research/analysis/__init__.py
touch research/analysis/sanity.py
touch research/analysis/inspector.py
touch research/analysis/visualizer.py
touch research/analysis/judgment/__init__.py
touch research/analysis/judgment/criteria.py
touch research/analysis/judgment/verdict.py

# Live Module
touch live/__init__.py
touch live/engine.py
touch live/state.py
touch live/stream/__init__.py
touch live/stream/websocket.py
touch live/guardian/__init__.py
touch live/guardian/circuit_breaker.py
touch live/guardian/sanitizer.py
touch live/guardian/monitor.py
touch live/execution/__init__.py
touch live/execution/orders.py

# Storage Module
touch storage/__init__.py
touch storage/duckdb/repository.py

# Tests Module
touch tests/__init__.py
touch tests/conftest.py
touch tests/unit/test_core_math.py
touch tests/unit/test_core_signals.py
touch tests/unit/test_risk_logic.py
touch tests/integration/test_research_pipeline.py
touch tests/integration/test_live_guardian.py

# Utils Module
touch utils/__init__.py
touch utils/rclone_sync.py
touch utils/db_migrate.py
touch utils/notify.py

# --- 4. DATA LOADER ADAPTER (Backward Compatibility) ---
# Membuat file adapter agar kode lama yang mengimport 'research.strategy.data.loader' tidak crash
echo "🔌 Creating Compatibility Adapters..."
mkdir -p research/strategy/data
echo "from research.ingestion.loader import *" > research/strategy/data/loader.py

echo "✅ MIGRATION COMPLETE. Project ORCA is online."
echo "📂 Struktur folder telah diperbarui. Git history aman."
