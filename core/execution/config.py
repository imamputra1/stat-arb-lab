"""
EXECUTION CONFIGURATION
Location: core/execution/config.py
Desc: Configuration dataclasses for executors.
"""

from dataclasses import dataclass, field
from typing import Dict, Any, Optional

@dataclass
class ExecutionConfig:
    """Konfigurasi Global Eksekusi"""
    mode: str = "simulation"  # simulation / live / paper
    exchange_id: str = "binance"
    api_key: Optional[str] = None
    api_secret: Optional[str] = None
    
    # Retry Logic
    max_retries: int = 3
    retry_delay_ms: int = 500
    
    # Safety
    max_slippage_bps: int = 50
    dry_run: bool = False
    
    # Extra parameters (catch-all)
    extra_params: Dict[str, Any] = field(default_factory=dict)
