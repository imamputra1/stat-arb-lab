"""
PIPELINE BRIDGE RESTORATION (The Compatibility Patch)
Location: ~/orca/restore_pipeline_bridge.py
Focus: Injects 'AdvancedStrategyPipeline' adapter into pipeline.py to fix ImportError.
"""
from pathlib import Path

# Lokasi Target
TARGET_FILE = Path("research/strategy/pipeline.py")

# Kode Adapter yang Hilang (The Bridge)
# Ini menerjemahkan panggilan lama dari Shotgun ke Orchestrator baru
ADAPTER_CODE = """

# --- COMPATIBILITY LAYER (THE BRIDGE) ---
class AdvancedStrategyPipeline(QuantumStrategyOrchestrator):
    \"\"\"
    Adapter class to maintain compatibility with HyperParallelEngine (shotgun.py).
    Maps legacy API calls to the new Gate-City Orchestrator.
    \"\"\"
    def __init__(self, **kwargs):
        # 1. Extract Core Config
        target = kwargs.get("target", "DOGE")
        anchor = kwargs.get("anchor", "BTC")
        start = kwargs.get("start", "2024-01-01")
        end = kwargs.get("end", "2024-12-31")
        strategy = kwargs.get("strategy_name", "kalman_crossover")
        warmup = kwargs.get("warmup_days", 30)
        
        # 2. Construct New Config
        config = PipelineConfig(
            execution_id=f"EXEC_{datetime.now().strftime('%H%M%S')}",
            target_symbol=target,
            anchor_symbol=anchor,
            start_date=start,
            end_date=end,
            strategy_name=strategy,
            warmup_days=warmup,
            strategy_params=kwargs, # Pass rest as params
            execution_mode=ExecutionMode.BACKTEST
        )
        
        # 3. Init Parent
        super().__init__(config)

    def execute_pair_arbitrage(self, target: str, anchor: str, start: str, end: str) -> Result[Dict[str, Any], str]:
        \"\"\"
        Legacy entry point used by shotgun.py.
        Updates config and runs execution.
        \"\"\"
        try:
            # Update targets if changed
            self.config.target_symbol = target
            self.config.anchor_symbol = anchor
            self.config.start_date = start
            self.config.end_date = end
            
            # Execute Pipeline
            res = self.execute()
            
            if res.is_ok():
                state = res.unwrap()
                # Return format expected by Shotgun
                return Ok({
                    "id": state.config.execution_id,
                    "metrics": state.metrics,
                    "path": state.artifacts.get("result_paths", {}).get("artifacts")
                })
            return Err(res.error)
            
        except Exception as e:
            return Err(f"Adapter Execution Failed: {str(e)}")
"""

def apply_patch():
    print(f"🔧 Analyzing {TARGET_FILE}...")
    
    if not TARGET_FILE.exists():
        print("❌ Target file not found! Are you in the project root?")
        return

    content = TARGET_FILE.read_text(encoding="utf-8")
    
    # Cek apakah sudah ada
    if "class AdvancedStrategyPipeline" in content:
        print("✅ Adapter already exists. No action needed.")
        return

    # Append Patch di akhir file (sebelum __name__ == "__main__" jika ada, atau di paling bawah)
    # Kita taruh sebelum blok 'if __name__' agar rapi, atau append saja jika mudah.
    # Untuk keamanan parsing, kita append di level module.
    
    # Cari posisi insert yang aman (sebelum blok main execution atau di akhir)
    if 'if __name__ == "__main__":' in content:
        parts = content.split('if __name__ == "__main__":')
        new_content = parts[0] + ADAPTER_CODE + '\nif __name__ == "__main__":' + parts[1]
    else:
        new_content = content + ADAPTER_CODE

    with open(TARGET_FILE, "w", encoding="utf-8") as f:
        f.write(new_content)
    
    print("✨ Surgery Successful: 'AdvancedStrategyPipeline' restored.")
    print("🚀 Shotgun Engine should now be able to link with Pipeline.")

if __name__ == "__main__":
    apply_patch()
