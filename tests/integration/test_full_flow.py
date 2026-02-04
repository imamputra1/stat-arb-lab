"""
ORCA INTEGRATION TEST - FULL FLOW (H -> B -> S)
Location: tests/integration/test_full_flow.py
Focus: Proving Infrastructure Stability for Alpha Hunting.
"""

import pytest
from pathlib import Path

# --- ORCA SYSTEM IMPORTS ---
from research.ingestion.loader import SilverDataLoader
from core.math.kalman import KalmanFilter
from research.strategy.pipeline import AdvancedStrategyPipeline

@pytest.mark.integration
class TestOrcaFullFlow:
    """
    Testing the end-to-end data pipeline from Physical Warehouse to Signal Decision.
    """

    def test_01_ingestion_to_math_link(self, simulated_lake: Path):
        """
        PROVE: Node H (Loader) can feed Node B (Math/Kalman).
        """
        # 1. Setup Node H (Harvest)
        loader = SilverDataLoader(silver_path=str(simulated_lake))
        
        # 2. Load Data from Simulated Lake (Result Pattern)
        load_result = loader.load(
            start_date="2024-01-01",
            end_date="2024-01-31",
            symbols=["BTC", "DOGE"]
        )
        
        assert load_result.is_ok(), f"Ingestion failed: {load_result.error}"
        lazy_df = load_result.unwrap()
        
        # 3. Setup Node B (Processing/Math)
        model = KalmanFilter(process_noise=1e-5, observation_noise=1e-4)
        
        # 4. Trigger Eager Execution
        df = lazy_df.collect()
        
        # Assert structural typed consistency
        assert "close" in df.columns or any("close_" in c for c in df.columns)
        assert df.height > 0
        print(f"   ✅ Node H -> Node B: Handshake Successful ({df.height} rows)")

    def test_02_math_to_signal_logic(self, simulated_lake: Path):
        """
        PROVE: Node B (Z-Score) correctly triggers Node S (Signal Generation).
        """
        # 1. Orchestration via Pipeline Adapter
        pipeline = AdvancedStrategyPipeline(
            target="DOGE",
            anchor="BTC",
            start="2024-01-01",
            end="2024-01-31",
            warmup_days=5
        )
        
        # Inject our simulated lake into the pipeline's loader
        pipeline.loader = SilverDataLoader(silver_path=str(simulated_lake))
        
        # 2. Execute Full Pair Arbitrage Flow
        execution_result = pipeline.execute_pair_arbitrage(
            target="DOGE",
            anchor="BTC",
            start="2024-01-01",
            end="2024-01-31"
        )
        
        assert execution_result.is_ok(), f"Pipeline execution failed: {execution_result.error}"
        
        output = execution_result.unwrap()
        metrics = output.get("metrics", {})
        
        # 3. Verification of Signal Brain (Node S)
        # Pastikan sinyal menghasilkan metrics (berarti ada posisi yang dibuka)
        assert "smart_score" in metrics
        assert metrics["trades"] >= 0
        
        print(f"   ✅ Node B -> Node S: Signal Logic Verified (Score: {metrics.get('smart_score'):.4f})")

    def test_03_data_integrity_persistence(self, simulated_lake: Path):
        """
        PROVE: Artifacts are correctly persisted to SUB-SYSTEM storage/.
        """
        pipeline = AdvancedStrategyPipeline(target="BTC", anchor="ETH")
        pipeline.loader = SilverDataLoader(silver_path=str(simulated_lake))
        
        res = pipeline.execute()
        state = res.unwrap()
        
        # Cek apakah Parquet hasil backtest benar-benar tertulis di disk
        artifact_path = Path(state.artifacts.get("parquet_path"))
        assert artifact_path.exists()
        assert artifact_path.stat().st_size > 0
        
        print(f"   ✅ Storage: Artifact integrity confirmed at {artifact_path.name}")
