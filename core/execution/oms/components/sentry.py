"""
THE SENTRY (THE GUARD)
Location: core/execution/oms/components/sentry.py
Desc: Internal Risk Gatekeeper. Melakukan sanity check cepat sebelum order keluar.
"""

from typing import Dict, List, Any
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum

from core.shared.result import Result, Ok, Err
from core.shared.utils import get_logger
from core.execution.types import OrderRequest

logger = get_logger("oms.sentry")

class RiskViolationLevel(Enum):
    WARNING = "WARNING"
    REJECT = "REJECT"
    CRITICAL = "CRITICAL" # Trigger Circuit Breaker

@dataclass(frozen=True)
class RiskViolation:
    rule: str
    level: RiskViolationLevel
    details: str
    timestamp: float = field(default_factory=lambda: datetime.now(timezone.utc).timestamp())

class Sentry:
    """
    Penjaga Gerbang OMS.
    Menolak order yang terlihat 'bodoh' atau 'berbahaya' secara struktural.
    """
    
    def __init__(self):
        # Config (Hardcoded for MVP safety, later injectable)
        self.max_order_size_usdt = 100_000.0
        self.max_order_rate = 10 # orders per second
        self.duplicate_window_sec = 1.0
        
        # State
        self._violations: List[RiskViolation] = []
        self._last_orders: List[float] = [] # timestamps
        self._recent_signatures: Dict[str, float] = {} # hash -> timestamp

    def validate_order(self, request: OrderRequest) -> Result[bool, str]:
        """
        [MAIN CHECK] Melakukan serangkaian inspeksi cepat.
        """
        # 1. Fat Finger Check (Size)
        # Asumsi harga BTC ~100k untuk safety check kasar jika price None
        est_price = request.price if request.price else 50000.0 
        est_notional = request.quantity * est_price
        
        if est_notional > self.max_order_size_usdt:
            self._record_violation("FAT_FINGER", f"Notional {est_notional} > {self.max_order_size_usdt}")
            return Err(f"Order too large: ${est_notional:,.2f}")

        # 2. Duplicate Order Check (Mencegah double-click)
        # Kita buat 'signature' sederhana dari simbol, side, qty, price
        sig = f"{request.symbol}|{request.side}|{request.quantity}|{request.price}"
        now = datetime.now(timezone.utc).timestamp()
        
        if sig in self._recent_signatures:
            last_time = self._recent_signatures[sig]
            if now - last_time < self.duplicate_window_sec:
                self._record_violation("DUPLICATE_ORDER", f"Signature {sig} repeated in {now-last_time:.2f}s")
                return Err("Duplicate order detected (Debounce protection)")
        
        # Update signature time
        self._recent_signatures[sig] = now
        
        # 3. Rate Limit Check (Spam Protection)
        self._clean_old_timestamps(now)
        if len(self._last_orders) >= self.max_order_rate:
            self._record_violation("RATE_LIMIT", f"Rate {len(self._last_orders)} > {self.max_order_rate}/s")
            return Err("Internal Rate Limit Exceeded")
            
        self._last_orders.append(now)

        return Ok(True)

    def get_stats(self) -> Dict[str, Any]:
        return {
            "violations_count": len(self._violations),
            "recent_rate": len(self._last_orders)
        }

    def _record_violation(self, rule: str, details: str):
        v = RiskViolation(rule, RiskViolationLevel.REJECT, details)
        self._violations.append(v)
        logger.warning(f"🛡️ SENTRY REJECT: {rule} - {details}")

    def _clean_old_timestamps(self, now: float):
        # Hapus timestamp > 1 detik lalu
        self._last_orders = [t for t in self._last_orders if now - t < 1.0]
        
        # Bersihkan signature map juga (biar ga memory leak)
        to_del = [k for k, v in self._recent_signatures.items() if now - v > 5.0]
        for k in to_del:
            del self._recent_signatures[k]
