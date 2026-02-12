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
from core.execution.types import OrderRequest, OrderSide

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

        # --- 🔥 NEW: Advanced risk controls ---
        self._max_drawdown_pct = 0.20          # 20% max drawdown
        self._max_position_ratio = 0.25        # 25% of equity per order
        self._kill_switch_engaged = False
        self._kill_reason = ""
        self._peak_equity = 0.0               # untuk hitung drawdown

    # ========== EXISTING METHODS (UNCHANGED) ==========
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

    # ========== 🔥 NEW RISK METHODS ==========
    
    def engage_kill_switch(self, reason: str = "Manual override"):
        """Aktifkan kill switch – semua order akan ditolak."""
        self._kill_switch_engaged = True
        self._kill_reason = reason
        self._record_violation("KILL_SWITCH", reason, RiskViolationLevel.CRITICAL)

    def disengage_kill_switch(self):
        """Nonaktifkan kill switch."""
        self._kill_switch_engaged = False
        self._kill_reason = ""

    def is_kill_switch_engaged(self) -> bool:
        return self._kill_switch_engaged

    def update_peak_equity(self, equity: float):
        """Update puncak equity untuk perhitungan drawdown."""
        if equity > self._peak_equity:
            self._peak_equity = equity

    def set_risk_limits(self,
                        max_drawdown_pct: float = None,
                        max_position_ratio: float = None,
                        max_order_size_usdt: float = None):
        """Sesuaikan batasan risiko secara runtime."""
        if max_drawdown_pct is not None:
            self._max_drawdown_pct = max_drawdown_pct
        if max_position_ratio is not None:
            self._max_position_ratio = max_position_ratio
        if max_order_size_usdt is not None:
            self.max_order_size_usdt = max_order_size_usdt

    def check_risk(self,
                   request: OrderRequest,
                   equity: float,
                   cash: float) -> Result[bool, str]:
        """
        Comprehensive risk check sebelum order dikirim ke broker.
        Membutuhkan equity (total aset) dan cash balance.
        """
        # 1. Kill Switch
        if self._kill_switch_engaged:
            return Err(f"Kill switch engaged: {self._kill_reason}")

        # 2. Drawdown Check
        if self._peak_equity > 0:
            current_drawdown = (self._peak_equity - equity) / self._peak_equity
            if current_drawdown > self._max_drawdown_pct:
                self._record_violation("DRAWDOWN_LIMIT",
                                       f"Drawdown {current_drawdown:.2%} > {self._max_drawdown_pct:.2%}",
                                       RiskViolationLevel.REJECT)
                return Err(f"Max drawdown exceeded: {current_drawdown:.2%}")

        # 3. Exposure Check (position size relative to equity)
        est_price = request.price if request.price else 50000.0  # fallback harga
        est_notional = request.quantity * est_price
        max_notional = equity * self._max_position_ratio
        if est_notional > max_notional:
            self._record_violation("POSITION_SIZE",
                                   f"Notional {est_notional:.2f} > {max_notional:.2f} ({self._max_position_ratio:.0%} of equity)",
                                   RiskViolationLevel.REJECT)
            return Err(f"Order too large relative to equity: {est_notional:.2f} > {max_notional:.2f}")

        # 4. Solvency Check (cukup cash untuk beli)
        if request.side == OrderSide.BUY:
            # buffer fee 0.1% (bisa disesuaikan)
            required_cash = est_notional * 1.001
            if cash < required_cash:
                self._record_violation("INSUFFICIENT_CASH",
                                       f"Need {required_cash:.2f}, have {cash:.2f}",
                                       RiskViolationLevel.REJECT)
                return Err(f"Insufficient cash: {cash:.2f} < {required_cash:.2f}")

        return Ok(True)

    # ========== INTERNAL HELPERS (MODIFIED) ==========
    def _record_violation(self, rule: str, details: str, level: RiskViolationLevel = RiskViolationLevel.REJECT):
        """Catat pelanggaran dengan level yang bisa ditentukan."""
        v = RiskViolation(rule, level, details)
        self._violations.append(v)
        logger.warning(f"🛡️ SENTRY {level.value}: {rule} - {details}")

    def _clean_old_timestamps(self, now: float):
        # Hapus timestamp > 1 detik lalu
        self._last_orders = [t for t in self._last_orders if now - t < 1.0]
        
        # Bersihkan signature map juga (biar ga memory leak)
        to_del = [k for k, v in self._recent_signatures.items() if now - v > 5.0]
        for k in to_del:
            del self._recent_signatures[k]

    def get_stats(self) -> Dict[str, Any]:
        """Statistik lengkap termasuk status kill switch dan drawdown."""
        return {
            "violations_count": len(self._violations),
            "recent_rate": len(self._last_orders),
            "kill_switch": self._kill_switch_engaged,
            "kill_reason": self._kill_reason,
            "peak_equity": self._peak_equity,
            "max_drawdown_pct": self._max_drawdown_pct,
            "max_position_ratio": self._max_position_ratio
        }
