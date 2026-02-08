"""
INDUSTRIAL EXECUTION ENGINE TYPES
Location: core/execution/engine/types.py
Desc: Data structures dengan monadic validation, 
      perf telemetry, dan forensic tracking.
"""

import uuid
from dataclasses import dataclass, field, replace
from typing import Optional, Dict, Any, Union
from datetime import datetime, timezone
from enum import Enum

from core.execution.types import OrderSide, Symbol
from core.shared.result import Result, Ok, Err

# ====================== ENUMS ======================

class TradeStatus(str, Enum):
    """Status eksekusi trade dengan state machine"""
    PENDING = "PENDING"
    EXECUTED = "EXECUTED"
    SETTLED = "SETTLED"
    FAILED = "FAILED"
    REVERSED = "REVERSED"  # Untuk error correction

class RejectionCode(str, Enum):
    """Standardized rejection codes (ISO 15022 style)"""
    INVALID_QTY = "INVA"
    PRICE_NOT_AVAILABLE = "PNAV"
    INSUFFICIENT_LIQUIDITY = "ILIQ"
    MARKET_CLOSED = "MCLO"
    RATE_LIMIT = "RLIM"
    RISK_REJECTED = "RISK"
    SELF_TRADE = "SELF"
    GHOST_LIQUIDITY = "GHST"
    NETWORK_TIMEOUT = "TIME"
    VALIDATION_ERROR = "VALD"
    UNKNOWN = "UNKN"

# ====================== CORE DATA STRUCTURES ======================

@dataclass(frozen=True, slots=True)
class ExecutionReceipt:
    """Immutable receipt untuk setiap eksekusi attempt"""
    attempt_id: str = field(default_factory=lambda: f"att_{uuid.uuid4().hex[:8]}")
    timestamp: float = field(default_factory=lambda: datetime.now(timezone.utc).timestamp())
    latency_ms: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @classmethod
    def create(cls, **kwargs) -> 'ExecutionReceipt':
        """Factory dengan default timestamp jika tidak disediakan"""
        if 'timestamp' not in kwargs:
            kwargs['timestamp'] = datetime.now(timezone.utc).timestamp()
        return cls(**kwargs)

@dataclass(frozen=True, slots=True)
class Trade:
    """
    Industrial-grade trade execution record dengan forensic tracking.
    Immutable & hashable untuk audit trail.
    """
    # 1. Core Identifiers
    trade_id: str = field(default_factory=lambda: f"trd_{uuid.uuid4().hex[:12]}")
    order_id: str = ""
    client_order_id: Optional[str] = None  # [UPGRADE] Link ke ID strategi asli
    execution_id: str = field(default_factory=lambda: f"exe_{uuid.uuid4().hex[:8]}")
    
    # 2. Market Data
    symbol: Symbol = ""
    side: OrderSide = OrderSide.BUY
    
    # 3. Execution Metrics (The Pain)
    quantity: float = 0.0
    price: float = 0.0
    fee: float = 0.0
    fee_currency: str = "USDT"
    
    # 4. Quality Metrics [UPGRADE AREA]
    latency_ms: float = 0.0        # [UPGRADE] int -> float (Microsecond precision)
    market_price_snapshot: float = 0.0 # [UPGRADE] Harga pasar saat trigger (Forensik Slippage)
    slippage_bps: float = 0.0
    market_impact_bps: float = 0.0
    is_maker: bool = False
    
    # 5. State & Timing
    status: TradeStatus = TradeStatus.EXECUTED
    executed_at: float = field(default_factory=lambda: datetime.now(timezone.utc).timestamp())
    settled_at: Optional[float] = None
    receipt: ExecutionReceipt = field(default_factory=ExecutionReceipt)
    
    # 6. Metadata
    tags: Dict[str, str] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    # ========== FACTORY METHOD (UPGRADED) ==========
    
    @classmethod
    def create(
        cls,
        order_id: str,
        symbol: Symbol,
        side: OrderSide,
        quantity: float,
        price: float,
        # [UPGRADE] Parameter eksplisit (jangan sembunyikan di **kwargs)
        fee: float = 0.0,
        fee_currency: str = "USDT",
        latency_ms: float = 0.0,
        market_price_snapshot: float = 0.0,
        is_maker: bool = False,
        client_order_id: Optional[str] = None,
        **kwargs
    ) -> Result['Trade', str]:
        """
        Factory dengan validasi ketat untuk integritas data finansial.
        """
        # 1. Validasi Fisika Pasar
        if quantity <= 0:
            return Err(f"Invalid quantity: {quantity} (must be > 0)")
        if price <= 0:
            return Err(f"Invalid price: {price} (must be > 0)")
        if latency_ms < 0:
            return Err(f"Invalid latency: {latency_ms} (cannot be negative)")
        if not order_id:
            return Err("Order ID is required for reconciliation")
            
        # 2. Hitung Slippage Otomatis (Jika snapshot tersedia)
        slippage_bps = 0.0
        if market_price_snapshot > 0:
            # Formula: |ExecPrice - MarketPrice| / MarketPrice * 10000
            diff = abs(price - market_price_snapshot)
            slippage_bps = (diff / market_price_snapshot) * 10000.0
            
        # 3. Construct Object
        return Ok(cls(
            order_id=order_id,
            client_order_id=client_order_id,
            symbol=symbol,
            side=side,
            quantity=quantity,
            price=price,
            fee=fee,
            fee_currency=fee_currency,
            latency_ms=latency_ms,
            market_price_snapshot=market_price_snapshot,
            slippage_bps=kwargs.get('slippage_bps', slippage_bps), # Allow override
            is_maker=is_maker,
            **kwargs
        ))

    # ========== COMPUTED PROPERTIES ==========

    @property
    def notional(self) -> float:
        """
        Gross Value dalam Quote Currency (tanpa memperhitungkan fee).
        Rumus: Price * Quantity
        """
        return self.price * self.quantity

    @property
    def net_notional(self) -> float:
        """
        Net Cash Flow Impact (Uang riil yang berpindah tangan).
        - BUY:  Keluar uang lebih banyak (Notional + Fee)
        - SELL: Terima uang lebih sedikit (Notional - Fee)
        
        Note: Asumsi Fee dibayar dalam Quote Asset (misal USDT). 
        Jika Fee pakai BNB, logic ini perlu konversi rate (Future Improvement).
        """
        # Safety check: Asumsi fee asset sama dengan quote asset (USDT)
        # Jika beda, idealnya fee diabaikan di calc ini atau dikonversi.
        # Untuk MVP Simulator kita, kita anggap fee selalu memotong PnL/Cost.
        
        if self.side == OrderSide.BUY:
            return self.notional + self.fee  # Total Biaya
        else:
            return self.notional - self.fee  # Total Pendapatan Bersih

    @property
    def effective_price(self) -> float:
        """
        Harga asli per unit setelah semua biaya.
        Ini adalah titik Break-Even (BEP) instan untuk trade ini.
        """
        if self.quantity == 0:
            return 0.0
        return self.net_notional / self.quantity

    @property
    def fee_rate_bps(self) -> float:
        """
        Realized Fee Rate dalam Basis Points.
        Berguna untuk audit: 'Apakah saya benar kena 5bps atau dipalak exchange?'
        """
        if self.notional == 0:
            return 0.0
        return (self.fee / self.notional) * 10000.0
    
# ========== MUTATION METHODS (IMMUTABLE STATE EVOLUTION) ==========

    def copy(self, **changes) -> 'Trade':
        """
        Low-level mutation: Membuat clone dengan perubahan specific.
        Menggunakan dataclasses.replace untuk efisiensi.
        """
        return replace(self, **changes)

    def with_status(self, new_status: TradeStatus) -> 'Trade':
        """
        Semantic State Transition.
        Mengubah status trade (misal: EXECUTED -> SETTLED).
        """
        return self.copy(status=new_status)

    def with_metadata(self, **updates) -> 'Trade':
        """
        Smart Metadata Update.
        Menggabungkan metadata lama dengan yang baru tanpa merusak immutability.
        """
        # 1. Copy dictionary lama (PENTING: agar tidak merujuk ke memori yg sama)
        new_meta = self.metadata.copy()
        
        # 2. Update dengan data baru
        new_meta.update(updates)
        
        # 3. Return object baru
        return self.copy(metadata=new_meta)

    def with_settlement(self, timestamp: Optional[float] = None) -> 'Trade':
        """
        Atomic Settlement Lifecycle.
        Otomatis set status ke SETTLED dan isi timestamp.
        """
        ts = timestamp or datetime.now(timezone.utc).timestamp()
        
        return self.copy(
            status=TradeStatus.SETTLED,
            settled_at=ts
        )
    
    # ========== SERIALIZATION ==========
    
    def to_dict(self) -> Dict[str, Any]:
        """Serialize untuk logging/persistence"""
        return {
            'trade_id': self.trade_id,
            'order_id': self.order_id,
            'symbol': self.symbol,
            'side': self.side.value,
            'quantity': self.quantity,
            'price': self.price,
            'fee': self.fee,
            'fee_currency': self.fee_currency,
            'latency_ms': self.latency_ms,
            'slippage_bps': self.slippage_bps,
            'status': self.status.value,
            'executed_at': self.executed_at,
            'is_maker': self.is_maker,
            'tags': self.tags,
        }
    
    def fingerprint(self) -> str:
        """Deterministic hash untuk deduplication"""
        import hashlib
        components = [
            self.order_id,
            self.symbol,
            self.side.value,
            f"{self.quantity:.10f}",
            f"{self.price:.10f}",
            str(self.executed_at),
        ]
        return hashlib.md5("|".join(components).encode()).hexdigest()



@dataclass(frozen=True, slots=True)
class Rejection:
    """
    [UPGRADED] Immutable rejection record dengan forensic capabilities.
    Mencatat BUKAN HANYA alasan, tapi juga KONTEKS pasar saat penolakan terjadi.
    """
    # 1. Identity (Traceability)
    rejection_id: str = field(default_factory=lambda: f"rej_{uuid.uuid4().hex[:12]}")
    order_id: str = ""
    client_order_id: Optional[str] = None  # Link balik ke Strategi (Critical)
    
    # 2. Classification (The 'Why')
    code: RejectionCode = RejectionCode.UNKNOWN
    reason: str = ""
    
    # 3. Context (The 'When' & 'Where')
    timestamp: float = field(default_factory=lambda: datetime.now(timezone.utc).timestamp())
    market_price_snapshot: float = 0.0      # Harga pasar saat reject (Forensik)
    
    # 4. Request Details (Snapshot dari Order yang gagal)
    requested_price: float = 0.0
    requested_qty: float = 0.0
    
    # 5. Recovery Hints (Actionable Intel)
    retryable: bool = False
    retry_after_ms: int = 0                 # Backoff guidance (0 = no retry)
    attempt_count: int = 1
    previous_rejection_id: Optional[str] = None # Chain of failures
    
    # 6. Metadata (Extensibility)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    # ====================== FACTORY METHOD ======================
    
    @classmethod
    def create(
        cls,
        order_id: str,
        code: RejectionCode,
        reason: str = "",
        # Explicit Context Params (Jangan sembunyikan di kwargs)
        client_order_id: Optional[str] = None,
        market_price_snapshot: float = 0.0,
        requested_price: float = 0.0,
        requested_qty: float = 0.0,
        retryable: bool = False,
        retry_after_ms: int = 0,
        **kwargs
    ) -> 'Rejection':
        """
        Factory cerdas untuk membuat Rejection event.
        Otomatis mengisi reason standar jika kosong.
        """
        # 1. Auto-Reasoning (Standardized Messages)
        if not reason:
            reason_map = {
                RejectionCode.INVALID_QTY: "Quantity invalid or <= 0",
                RejectionCode.INSUFFICIENT_LIQUIDITY: "Not enough liquidity to fill",
                RejectionCode.GHOST_LIQUIDITY: "Liquidity vanished (Ghost/Spoof)",
                RejectionCode.RISK_REJECTED: "RMS Check Failed",
                RejectionCode.RATE_LIMIT: "API Rate Limit Breached",
                RejectionCode.PRICE_NOT_AVAILABLE: "Market Price unavailable",
            }
            reason = reason_map.get(code, f"Rejected with code: {code.value}")

        # 2. Construct Object
        return cls(
            order_id=order_id,
            client_order_id=client_order_id,
            code=code,
            reason=reason,
            market_price_snapshot=market_price_snapshot,
            requested_price=requested_price,
            requested_qty=requested_qty,
            retryable=retryable,
            retry_after_ms=retry_after_ms,
            **kwargs
        )

    # ========== COMPUTED PROPERTIES (INTELLIGENCE) ==========

    @property
    def is_transient(self) -> bool:
        """
        Apakah error ini sementara? (Boleh di-retry).
        Logic: Cek flag 'retryable' ATAU cek kode error spesifik.
        """
        if self.retryable:
            return True
            
        # Daftar dosa yang bisa dimaafkan (Retryable Codes)
        transient_codes = {
            RejectionCode.GHOST_LIQUIDITY,   # Hantu lewat, coba lagi
            RejectionCode.NETWORK_TIMEOUT,   # Kabel kesandung
            RejectionCode.RATE_LIMIT,        # Terlalu ngebut, rem dikit
            RejectionCode.PRICE_NOT_AVAILABLE, # Data feed macet
            RejectionCode.MARKET_CLOSED      # (Tergantung jam, tapi technically transient)
        }
        return self.code in transient_codes

    @property
    def is_fatal(self) -> bool:
        """
        Apakah error ini permanen? (Stop strategy / Kill switch).
        Kebalikan dari is_transient.
        """
        return not self.is_transient

    # ========== MUTATION METHODS (RECOVERY STRATEGY) ==========

    def with_retry_guidance(self, retry_after_ms: int = 1000) -> 'Rejection':
        """
        [SEMANTIC] Menambahkan instruksi 'Backoff' pada penolakan.
        Otomatis menandai rejection ini sebagai 'retryable'.
        """
        return self.copy(
            retryable=True,
            retry_after_ms=retry_after_ms
        )

    def increment_attempt(self) -> 'Rejection':
        """
        [TRACKING] Naikkan counter percobaan.
        Penting untuk mencegah Infinite Loop saat retry (Max Retries).
        """
        return self.copy(
            attempt_count=self.attempt_count + 1
        )

    def copy(self, **changes) -> 'Rejection':
        """
        [CORE] Immutable state evolution.
        Sama seperti di class Trade.
        """
        from dataclasses import replace
        return replace(self, **changes)

    # ========== SERIALIZATION (AUDIT) ==========

    def to_dict(self) -> Dict[str, Any]:
        """
        Full forensic report untuk debugging.
        """
        return {
            'type': 'REJECTION', # Marker agar mudah di-filter di log
            'rejection_id': self.rejection_id,
            'order_id': self.order_id,
            'client_order_id': self.client_order_id,
            'code': self.code.value,
            'reason': self.reason,
            'timestamp': self.timestamp,
            'market_snapshot': self.market_price_snapshot,
            'retry_logic': {
                'is_transient': self.is_transient, # Computed value masuk log
                'retry_after_ms': self.retry_after_ms,
                'attempt': self.attempt_count
            },
            'metadata': self.metadata
        }



# ====================== EXECUTION RESULT TYPE ======================

ExecutionResult = Union[Trade, Rejection]

def is_trade(result: ExecutionResult) -> bool:
    """Type guard untuk Trade"""
    return isinstance(result, Trade)

def is_rejection(result: ExecutionResult) -> bool:
    """Type guard untuk Rejection"""
    return isinstance(result, Rejection)

# ====================== EXPORTS ======================

__all__ = [
    'TradeStatus',
    'RejectionCode',
    'ExecutionReceipt',
    'Trade',
    'Rejection',
    'ExecutionResult',
    'is_trade',
    'is_rejection',
]
