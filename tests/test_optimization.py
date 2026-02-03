"""
INDUSTRIAL UNIT TESTS: OPTIMIZATION MODULE
Location: tests/test_optimization.py
Focus: Validating HyperParallelEngine and QuantumScoreKeeper integration.
"""
import sys
import pytest
import polars as pl
import numpy as np
from pathlib import Path

# --- INDUSTRIAL PATH INJECTION ---
# Menghitung root dari tests/ ke ~/arb-lab/
PROJECT_ROOT = Path(__file__).parent.parent.absolute()
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# ABSOLUTE IMPORTS: Menjamin koherensi antar Node
from research.strategy.optimization import (
    HyperParallelEngine, 
    QuantumScoreKeeper, 
    ScoringStrategy,
    QuantumParameterSpace
)

def test_space_logic():
    """Memastikan amunisi parameter tidak melanggar hukum ekonomi."""
    space = QuantumParameterSpace.surgical_grid()
    for res in space.generate():
        assert res.is_ok(), f"Space generation failed: {res.error}"
        params = res.unwrap().params
        # Entry threshold harus selalu lebih besar dari exit untuk profit margin
        assert params["entry_threshold"] > params["exit_threshold"]

def test_scorekeeper_penalties():
    """Memastikan hakim memberikan hukuman pada strategi 'mati'."""
    keeper = QuantumScoreKeeper(strategy=ScoringStrategy.BALANCED)
    
    # Mock Data: 0 Trades (Equity diam)
    df_dead = pl.DataFrame({
        "cumulative_returns": np.zeros(100),
        "position": np.zeros(100)
    })
    
    res = keeper.evaluate(df_dead)
    assert res.is_ok()
    # Harus memberikan skor negatif yang berat untuk 0 trades
    assert res.unwrap().smart_score <= 0

def test_shotgun_initialization():
    """Memastikan mesin paralel siap tempur di Ryzen 5."""
    try:
        engine = HyperParallelEngine(n_jobs=2) # Smoke test core
        assert engine.n_jobs == 2
        assert engine.results_dir.exists()
    except Exception as e:
        pytest.fail(f"Shotgun Engine failed to initialize: {e}")

if __name__ == "__main__":
    # Eksekusi Manual untuk Smoke Test
    print("🔥 INITIATING SMOKE TEST FOR OPTIMIZATION NODE")
    print("-" * 50)
    
    try:
        test_space_logic()
        print("✅ Space Logic: PASSED")
        
        test_scorekeeper_penalties()
        print("✅ ScoreKeeper Penalties: PASSED")
        
        test_shotgun_initialization()
        print("✅ Shotgun Init: PASSED")
        
        print("\n🚀 INTEGRATION SMOKE TEST SUCCESSFUL. SYSTEM READY FOR WAR ROOM.")
    except AssertionError as e:
        print(f"❌ TEST FAILED: {str(e)}")
        sys.exit(1)
    except Exception as e:
        print(f"💥 SYSTEM CRASH: {str(e)}")
        sys.exit(1)
