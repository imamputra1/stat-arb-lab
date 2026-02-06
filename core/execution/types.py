"""
INDUSTRIAL-GRADE EXECUTION TYPES
Location: core/execution/types.py
Desc: Immutable, monadic-friendly data structures with validation, 
      performance tracking, and full algebraic type support.
"""

import uuid
import hashlib
from dataclasses import dataclass, field, asdict, replace
from datetime import datetime, timezone
from enum import Enum
from typing import (
    Optional, Dict, Any, List, Tuple, 
    FrozenSet, Self, Protocol, runtime_checkable
)
from decimal import Decimal
from functools import cached_property
import orjson as json

# Import shared utilities
from ..shared.result import (
    Result, Ok, Err, ResultBuilder, is_err
)
from ..shared.performance import PerformanceMonitor

# ====================== TYPE ALIASES & CONSTANTS ======================

NanoTimestamp = int  # nanoseconds since epoch
MicroTimestamp = int  # microseconds since epoch
Symbol = str
Currency = str
StrategyID = str
ClientOrderID = str
ExchangeOrderID = str

# ====================== VALIDATION UTILITIES ======================

class ValidationError(ValueError):
    """Domain-specific validation error"""
    pass

def validate_positive(value: float, field_name: str) -> Result[float, ValidationError]:
    """Validates that a value is positive"""
    if value > 0:
        return Ok(value)
    return Err(ValidationError(f"{field_name} must be positive, got {value}"))

def validate_non_negative(value: float, field_name: str) -> Result[float, ValidationError]:
    """Validates that a value is non-negative"""
    if value >= 0:
        return Ok(value)
    return Err(ValidationError(f"{field_name} must be non-negative, got {value}"))

def validate_price(price: Optional[float], order_type: 'OrderType') -> Result[Optional[float], ValidationError]:
    """Validates price based on order type"""
    if order_type == OrderType.MARKET:
        return Ok(price)  # Market orders can have None price
    
    if price is None or price <= 0:
        return Err(ValidationError(f"{order_type.value} orders must have positive price"))
    
    return Ok(price)

# ====================== ENUMERATIONS ======================

class OrderType(str, Enum):
    """Types of orders with additional metadata"""
    MARKET = "MARKET"
    LIMIT = "LIMIT"
    STOP_LOSS = "STOP_LOSS"
    TAKE_PROFIT = "TAKE_PROFIT"
    
    @property
    def requires_price(self) -> bool:
        """Whether this order type requires a price"""
        return self != OrderType.MARKET
    
    @property
    def is_conditional(self) -> bool:
        """Whether this is a conditional order"""
        return self in {OrderType.STOP_LOSS, OrderType.TAKE_PROFIT}

class OrderSide(str, Enum):
    """Order side with utility methods"""
    BUY = "BUY"
    SELL = "SELL"
    
    def opposite(self) -> 'OrderSide':
        """Returns the opposite side"""
        return OrderSide.SELL if self == OrderSide.BUY else OrderSide.BUY
    
    def multiplier(self) -> int:
        """Returns +1 for BUY, -1 for SELL (useful for P&L calculations)"""
        return 1 if self == OrderSide.BUY else -1

# ====================== ORDER STATUS (FIXED) ======================

class OrderStatus(str, Enum):
    """
    Order lifecycle states with state machine logic.
    """
    PENDING = "PENDING"           # Sent but not yet acknowledged
    ACKNOWLEDGED = "ACKNOWLEDGED" # Exchange acknowledged
    OPEN = "OPEN"                 # In order book
    PARTIALLY_FILLED = "PARTIALLY_FILLED"
    FILLED = "FILLED"
    CANCELED = "CANCELED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"           # For time-in-force orders
    
    def can_transition_to(self, new_status: 'OrderStatus') -> bool:
        """Check if transition is valid using external mapping"""
        # [FIX] Ambil mapping dari variabel global di bawah class ini
        allowed = _ORDER_STATUS_TRANSITIONS.get(self, frozenset())
        return new_status in allowed
    
    def is_terminal(self) -> bool:
        """Check if status is terminal"""
        return self in {
            OrderStatus.FILLED, 
            OrderStatus.CANCELED, 
            OrderStatus.REJECTED, 
            OrderStatus.EXPIRED
        }
    
    def is_active(self) -> bool:
        """Check if order is still active"""
        return self in {
            OrderStatus.PENDING, 
            OrderStatus.ACKNOWLEDGED, 
            OrderStatus.OPEN, 
            OrderStatus.PARTIALLY_FILLED
        }

# [FIX] Definisikan Mapping Transisi DI LUAR Class
# Agar tidak terkena NameError saat runtime.
_ORDER_STATUS_TRANSITIONS: Dict[OrderStatus, FrozenSet[OrderStatus]] = {
    OrderStatus.PENDING: frozenset([
        OrderStatus.ACKNOWLEDGED, 
        OrderStatus.REJECTED,
        OrderStatus.CANCELED  # Kadang bisa cancel sebelum ack
    ]),
    OrderStatus.ACKNOWLEDGED: frozenset([
        OrderStatus.OPEN, 
        OrderStatus.REJECTED, 
        OrderStatus.CANCELED,
        OrderStatus.FILLED  # Instant fill (Market Order)
    ]),
    OrderStatus.OPEN: frozenset([
        OrderStatus.PARTIALLY_FILLED, 
        OrderStatus.FILLED, 
        OrderStatus.CANCELED, 
        OrderStatus.EXPIRED,
        OrderStatus.REJECTED # Rare case
    ]),
    OrderStatus.PARTIALLY_FILLED: frozenset([
        OrderStatus.FILLED, 
        OrderStatus.CANCELED, 
        OrderStatus.EXPIRED
    ]),
    # Terminal states have no transitions
    OrderStatus.FILLED: frozenset(),
    OrderStatus.CANCELED: frozenset(),
    OrderStatus.REJECTED: frozenset(),
    OrderStatus.EXPIRED: frozenset(),
}
class TimeInForce(str, Enum):
    """Order time-in-force policies"""
    GTC = "GTC"       # Good Till Canceled
    IOC = "IOC"       # Immediate Or Cancel
    FOK = "FOK"       # Fill Or Kill
    DAY = "DAY"       # Day order
    GTD = "GTD"       # Good Till Date

# ====================== IMMUTABLE BASE CLASS ======================

@dataclass(frozen=True, slots=True)
class ImmutableBase:
    """Base class for all immutable execution entities"""
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary, handling Enums and datetimes"""
        def serialize(obj: Any) -> Any:
            if isinstance(obj, Enum):
                return obj.value
            elif isinstance(obj, datetime):
                return obj.isoformat()
            elif isinstance(obj, Decimal):
                return str(obj)
            elif hasattr(obj, 'to_dict'):
                return obj.to_dict()
            return obj
        
        return {k: serialize(v) for k, v in asdict(self).items()}
    
    def to_json(self) -> bytes:
        """Fast JSON serialization using orjson"""
        return json.dumps(self.to_dict())
    
    @classmethod
    def from_json(cls, data: bytes) -> Result[Self, ValidationError]:
        """Deserialize from JSON with validation"""
        try:
            parsed = json.loads(data)
            return cls.create(**parsed)
        except Exception as e:
            return Err(ValidationError(f"Failed to deserialize {cls.__name__}: {e}"))
    
    def copy(self, **changes) -> Self:
        """Create a new instance with specified changes"""
        return replace(self, **changes)
    
    @classmethod
    def create(cls, **kwargs) -> Result[Self, ValidationError]:
        """Factory method with validation"""
        # Subclasses should override with their own validation logic
        try:
            return Ok(cls(**kwargs))
        except Exception as e:
            return Err(ValidationError(f"Invalid {cls.__name__}: {e}"))

# ====================== CORE ENTITIES ======================

@dataclass(frozen=True, slots=True)
class OrderRequest(ImmutableBase):
    """
    Immutable order request with validation.
    This is what gets sent to the exchange.
    """
    symbol: Symbol
    side: OrderSide
    order_type: OrderType
    quantity: float
    price: Optional[float] = None
    time_in_force: TimeInForce = TimeInForce.GTC
    reduce_only: bool = False
    post_only: bool = False
    
    # Identifiers
    client_order_id: ClientOrderID = field(
        default_factory=lambda: f"clord_{uuid.uuid4().hex[:12]}"
    )
    strategy_id: StrategyID = "default"
    correlation_id: str = field(
        default_factory=lambda: f"corr_{uuid.uuid4().hex[:8]}"
    )
    
    # Metadata
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @classmethod
    def create(
        cls,
        symbol: str,
        side: OrderSide,
        order_type: OrderType,
        quantity: float,
        price: Optional[float] = None,
        **kwargs
    ) -> Result['OrderRequest', ValidationError]: # [FIX] Gunakan string forward reference
        """Factory method with comprehensive validation"""
        
        # Validate quantity
        quantity_result = validate_positive(quantity, "quantity")
        if is_err(quantity_result):
            return quantity_result
        
        # Validate price based on order type
        price_result = validate_price(price, order_type)
        if is_err(price_result):
            return price_result
        
        # Create the order
        return Ok(cls(
            symbol=symbol.upper(),
            side=side,
            order_type=order_type,
            quantity=quantity_result.unwrap(),
            price=price_result.unwrap(),
            **kwargs
        ))    
    @cached_property
    def notional(self) -> Optional[float]:
        """Calculate order notional value"""
        if self.price:
            return self.quantity * self.price
        return None
    
    @cached_property
    def fingerprint(self) -> str:
        """Create a deterministic fingerprint for order matching"""
        components = [
            self.symbol,
            self.side.value,
            self.order_type.value,
            f"{self.quantity:.8f}",
            f"{self.price:.8f}" if self.price else "MARKET",
            self.time_in_force.value,
            str(self.reduce_only),
            str(self.post_only),
            self.strategy_id,
        ]
        return hashlib.sha256("|".join(components).encode()).hexdigest()[:16]
    
    def with_metadata(self, **new_metadata) -> Self:
        """Add or update metadata"""
        merged = {**self.metadata, **new_metadata}
        return self.copy(metadata=merged)

@dataclass(frozen=True, slots=True)
class Order(ImmutableBase):
    """
    Immutable order with state tracking and monadic operations.
    This represents an order in our system.
    """
    # Core fields (same as OrderRequest)
    symbol: Symbol
    side: OrderSide
    order_type: OrderType
    quantity: float
    price: Optional[float] = None
    time_in_force: TimeInForce = TimeInForce.GTC
    reduce_only: bool = False
    post_only: bool = False
    
    # Identifiers
    order_id: str = field(
        default_factory=lambda: f"ord_{uuid.uuid4().hex[:12]}"
    )
    client_order_id: ClientOrderID = ""
    exchange_order_id: ExchangeOrderID = ""
    strategy_id: StrategyID = "default"
    correlation_id: str = ""
    
    # State tracking
    status: OrderStatus = OrderStatus.PENDING
    filled_quantity: float = 0.0
    average_fill_price: float = 0.0
    remaining_quantity: float = 0.0
    
    # Performance tracking
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    sent_to_exchange_at: Optional[datetime] = None
    acknowledged_at: Optional[datetime] = None
    first_fill_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    
    # Error handling
    error_code: Optional[str] = None
    error_message: Optional[str] = None
    warnings: List[str] = field(default_factory=list)
    
    # Metadata
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        """Ensure remaining_quantity is consistent"""
        if self.remaining_quantity != self.quantity - self.filled_quantity:
            object.__setattr__(self, 'remaining_quantity', 
                             self.quantity - self.filled_quantity)
    
    @classmethod
    def from_request(
        cls, 
        request: OrderRequest,
        exchange_order_id: str = ""
    ) -> Self:
        """Create an Order from an OrderRequest"""
        return cls(
            symbol=request.symbol,
            side=request.side,
            order_type=request.order_type,
            quantity=request.quantity,
            price=request.price,
            time_in_force=request.time_in_force,
            reduce_only=request.reduce_only,
            post_only=request.post_only,
            client_order_id=request.client_order_id,
            strategy_id=request.strategy_id,
            correlation_id=request.correlation_id,
            exchange_order_id=exchange_order_id,
            metadata=request.metadata,
            created_at=request.created_at,
        )
    
    # ========== STATE TRANSITION METHODS ==========
    
    def transition_to(self, new_status: OrderStatus) -> Result[Self, ValidationError]:
        """Attempt to transition to a new status"""
        if not self.status.can_transition_to(new_status):
            return Err(ValidationError(
                f"Invalid transition from {self.status} to {new_status}"
            ))
        
        return Ok(self.copy(
            status=new_status,
            updated_at=datetime.now(timezone.utc)
        ))
    
    def mark_sent(self, timestamp: Optional[datetime] = None) -> Self:
        """Mark order as sent to exchange"""
        return self.copy(
            sent_to_exchange_at=timestamp or datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc)
        )
    
    def mark_acknowledged(
        self, 
        exchange_order_id: str,
        timestamp: Optional[datetime] = None
    ) -> Result[Self, ValidationError]:
        """Mark order as acknowledged by exchange"""
        return self.transition_to(OrderStatus.ACKNOWLEDGED).map(
            lambda order: order.copy(
                exchange_order_id=exchange_order_id,
                acknowledged_at=timestamp or datetime.now(timezone.utc)
            )
        )
    
    def mark_open(self) -> Result[Self, ValidationError]:
        """Mark order as open in order book"""
        return self.transition_to(OrderStatus.OPEN)
    
    def add_fill(
        self, 
        fill_quantity: float, 
        fill_price: float,
        timestamp: Optional[datetime] = None
    ) -> Result[Self, ValidationError]:
        """Add a fill to the order"""
        # Validate fill
        if fill_quantity <= 0:
            return Err(ValidationError(f"Fill quantity must be positive: {fill_quantity}"))
        
        if fill_price <= 0:
            return Err(ValidationError(f"Fill price must be positive: {fill_price}"))
        
        if fill_quantity > self.remaining_quantity:
            return Err(ValidationError(
                f"Fill quantity {fill_quantity} exceeds remaining {self.remaining_quantity}"
            ))
        
        # Calculate new values
        new_filled = self.filled_quantity + fill_quantity
        new_remaining = self.remaining_quantity - fill_quantity
        
        # Calculate weighted average price
        total_value = (self.average_fill_price * self.filled_quantity + 
                      fill_price * fill_quantity)
        new_avg_price = total_value / new_filled if new_filled > 0 else 0.0
        
        # Determine new status
        if new_remaining == 0:
            new_status = OrderStatus.FILLED
            completed_at = timestamp or datetime.now(timezone.utc)
        elif self.status == OrderStatus.OPEN:
            new_status = OrderStatus.PARTIALLY_FILLED
            completed_at = None
        else:
            new_status = self.status
            completed_at = None
        
        # First fill tracking
        first_fill_at = self.first_fill_at or (timestamp or datetime.now(timezone.utc))
        
        return Ok(self.copy(
            status=new_status,
            filled_quantity=new_filled,
            average_fill_price=new_avg_price,
            remaining_quantity=new_remaining,
            first_fill_at=first_fill_at,
            completed_at=completed_at,
            updated_at=datetime.now(timezone.utc)
        ))
    
    def mark_canceled(self) -> Result[Self, ValidationError]:
        """Mark order as canceled"""
        return self.transition_to(OrderStatus.CANCELED)
    
    def mark_rejected(self, error_code: str, error_message: str) -> Result[Self, ValidationError]:
        """Mark order as rejected"""
        return self.transition_to(OrderStatus.REJECTED).map(
            lambda order: order.copy(
                error_code=error_code,
                error_message=error_message
            )
        )
    
    # ========== QUERY METHODS ==========
    
    @cached_property
    def is_active(self) -> bool:
        """Check if order is still active"""
        return self.status.is_active()
    
    @cached_property
    def is_terminal(self) -> bool:
        """Check if order is in terminal state"""
        return self.status.is_terminal()
    
    @cached_property
    def fill_percentage(self) -> float:
        """Percentage of order filled (0-100)"""
        return (self.filled_quantity / self.quantity * 100) if self.quantity > 0 else 0
    
    @cached_property
    def notional_filled(self) -> float:
        """Total notional value of fills"""
        return self.filled_quantity * self.average_fill_price
    
    @cached_property
    def notional_remaining(self) -> Optional[float]:
        """Notional value remaining"""
        if self.price:
            return self.remaining_quantity * self.price
        return None
    
    @cached_property
    def latency_sent_to_ack(self) -> Optional[float]:
        """Latency from sent to acknowledged in milliseconds"""
        if self.sent_to_exchange_at and self.acknowledged_at:
            delta = self.acknowledged_at - self.sent_to_exchange_at
            return delta.total_seconds() * 1000
        return None
    
    @cached_property
    def latency_ack_to_first_fill(self) -> Optional[float]:
        """Latency from acknowledged to first fill in milliseconds"""
        if self.acknowledged_at and self.first_fill_at:
            delta = self.first_fill_at - self.acknowledged_at
            return delta.total_seconds() * 1000
        return None
    
    @cached_property
    def total_latency(self) -> Optional[float]:
        """Total latency from creation to completion"""
        if self.created_at and self.completed_at:
            delta = self.completed_at - self.created_at
            return delta.total_seconds() * 1000
        return None

@dataclass(frozen=True, slots=True)
class TradeFill(ImmutableBase):
    """
    Immutable trade fill report with validation.
    Represents a single execution/trade.
    """
    fill_id: str = field(
        default_factory=lambda: f"fill_{uuid.uuid4().hex[:12]}"
    )
    order_id: str = ""
    client_order_id: ClientOrderID = ""
    exchange_order_id: ExchangeOrderID = ""
    exchange_trade_id: str = ""
    
    # Trade details
    symbol: Symbol = ""
    side: OrderSide = OrderSide.BUY
    quantity: float = 0.0
    price: float = 0.0
    
    # Fees (REVISI: Support Multi-Currency)
    fee: float = 0.0
    fee_currency: Currency = "USD"
    fee_rate: Optional[float] = None
    
    # Liquidity info
    is_maker: bool = False
    liquidity: str = "TAKER"
    
    # Timestamps
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    exchange_timestamp: Optional[datetime] = None
    local_timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    
    # Performance metrics (REVISI: Field untuk Simulator "Jahat")
    latency_ms: Optional[float] = None  # Waktu tunda eksekusi
    slippage_bps: Optional[float] = None  # Pergeseran harga (basis points)
    market_impact_bps: Optional[float] = None
    
    # Metadata
    metadata: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def create(
        cls,
        order_id: str,
        symbol: str,
        side: OrderSide,
        quantity: float,
        price: float,
        # [BARU] Parameter tambahan untuk Simulator
        fee: float = 0.0,
        fee_currency: str = "USD",
        latency_ms: Optional[float] = None,
        slippage_bps: Optional[float] = None,
        **kwargs
    ) -> Result[Self, ValidationError]:
        """Factory method with torture metrics support"""
        
        # Validate basics
        quantity_result = validate_positive(quantity, "quantity")
        price_result = validate_positive(price, "price")
        
        # Combine validations using ResultBuilder
        return ResultBuilder.sequence(quantity_result, price_result).map(
            lambda results: cls(
                order_id=order_id,
                symbol=symbol.upper(),
                side=side,
                quantity=results[0],
                price=results[1],
                # [BARU] Inject data ke dalam object
                fee=fee,
                fee_currency=fee_currency,
                latency_ms=latency_ms,
                slippage_bps=slippage_bps,
                **kwargs
            )
        )    

    @cached_property
    def notional(self) -> float:
        """Total notional value of the fill"""
        return self.quantity * self.price
    
    @cached_property
    def signed_quantity(self) -> float:
        """Quantity with sign based on side (+ for BUY, - for SELL)"""
        return self.quantity * self.side.multiplier()
    
    @cached_property
    def signed_notional(self) -> float:
        """Notional with sign based on side (+ for BUY, - for SELL)"""
        return self.notional * self.side.multiplier()

    @cached_property
    def fee_cost(self) -> float:
        """
        Alias untuk self.fee agar konsisten penamaan.
        WARNING: Ini adalah RAW value dalam 'fee_currency', bukan USD.
        """
        return self.fee

# ====================== EXECUTION REPORT ======================

@dataclass(frozen=True, slots=True)
class ExecutionReport(ImmutableBase):
    """
    Comprehensive execution report with aggregated metrics.
    """
    # Order reference
    order: Order
    fills: Tuple[TradeFill, ...] = ()
    
    # Performance metrics
    total_notional: float = 0.0
    
    # [FIX] Total fees harus Dictionary karena bisa Multi-Currency (misal: USDT & BNB)
    total_fees: Dict[str, float] = field(default_factory=dict)
    
    vwap_execution: float = 0.0
    avg_slippage_bps: Optional[float] = None
    avg_latency_ms: Optional[float] = None
    
    # Completion status
    is_complete: bool = False
    completion_reason: Optional[str] = None
    
    # Timestamps
    report_timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    
    @classmethod
    def from_order_and_fills(
        cls, 
        order: Order, 
        fills: List[TradeFill]
    ) -> Self:
        """Create execution report from order and its fills"""
        fills_tuple = tuple(fills)
        
        # Calculate aggregated metrics
        total_notional = sum(fill.notional for fill in fills_tuple)
        
        # [FIX] Calculate Total Fees per Currency
        # Jangan pakai fee_notional (sudah dihapus), pakai fill.fee & fill.fee_currency
        fees_map = {}
        for fill in fills_tuple:
            curr = fill.fee_currency
            fees_map[curr] = fees_map.get(curr, 0.0) + fill.fee
        
        # Calculate VWAP
        total_quantity = sum(fill.quantity for fill in fills_tuple)
        vwap = total_notional / total_quantity if total_quantity > 0 else 0.0
        
        # Calculate average slippage
        slippages = [f.slippage_bps for f in fills_tuple if f.slippage_bps is not None]
        avg_slippage = sum(slippages) / len(slippages) if slippages else None
        
        # Calculate average latency
        latencies = [f.latency_ms for f in fills_tuple if f.latency_ms is not None]
        avg_latency = sum(latencies) / len(latencies) if latencies else None
        
        return cls(
            order=order,
            fills=fills_tuple,
            total_notional=total_notional,
            total_fees=fees_map, # [FIX] Inject dictionary
            vwap_execution=vwap,
            avg_slippage_bps=avg_slippage,
            avg_latency_ms=avg_latency,
            is_complete=order.is_terminal,
            completion_reason=order.status.value if order.is_terminal else None
        )
    
    @cached_property
    def net_notional(self) -> float:
        """
        [UPGRADED] Total notional after fees.
        Hanya mengurangi fee jika mata uangnya match dengan Quote Currency.
        (Safe for Multi-Currency)
        """
        # 1. Deteksi Quote Currency (misal BTC/USDT -> USDT)
        try:
            # Asumsi format standar "BASE/QUOTE"
            quote_currency = self.order.symbol.split('/')[1]
        except IndexError:
            # Fallback jika format symbol aneh
            quote_currency = "USD" 
            
        # 2. Ambil fee yang relevan saja (misal fee USDT)
        # Fee dalam BNB/ETH diabaikan di sini karena tidak bisa dikurang langsung
        relevant_fee = self.total_fees.get(quote_currency, 0.0)
        
        return self.total_notional - relevant_fee

    @cached_property
    def effective_price(self) -> float:
        """
        [UPGRADED] Effective price paid/received (including relevant fees).
        """
        # Hitung total quantity dari fills
        total_quantity = sum(fill.quantity for fill in self.fills)
        
        if total_quantity > 0:
            # Gunakan net_notional yang sudah aman di atas
            return self.net_notional / total_quantity
            
        return 0.0

    @cached_property
    def fee_rate_bps(self) -> float:
        """
        [UPGRADED] Average fee rate in basis points.
        Hanya menghitung rate berdasarkan fee mata uang utama.
        """
        if self.total_notional <= 0:
            return 0.0
            
        # Gunakan logika deteksi currency yang sama
        try:
            quote_currency = self.order.symbol.split('/')[1]
        except IndexError:
            quote_currency = "USD"
            
        relevant_fee = self.total_fees.get(quote_currency, 0.0)
        
        # Rumus: (Fee / Notional) * 10000
        return (relevant_fee / self.total_notional) * 10000
# ====================== FACTORY FUNCTIONS ======================

class OrderFactory:
    """Factory for creating orders with different configurations"""
    
    @staticmethod
    def market(
        symbol: str,
        side: OrderSide,
        quantity: float,
        **kwargs
    ) -> Result[OrderRequest, ValidationError]:
        """Create a market order request"""
        return OrderRequest.create(
            symbol=symbol,
            side=side,
            order_type=OrderType.MARKET,
            quantity=quantity,
            price=None,
            **kwargs
        )
    
    @staticmethod
    def limit(
        symbol: str,
        side: OrderSide,
        quantity: float,
        price: float,
        post_only: bool = True,
        **kwargs
    ) -> Result[OrderRequest, ValidationError]:
        """Create a limit order request"""
        return OrderRequest.create(
            symbol=symbol,
            side=side,
            order_type=OrderType.LIMIT,
            quantity=quantity,
            price=price,
            post_only=post_only,
            **kwargs
        )
    
    @staticmethod
    def stop_loss(
        symbol: str,
        side: OrderSide,
        quantity: float,
        stop_price: float,
        **kwargs
    ) -> Result[OrderRequest, ValidationError]:
        """Create a stop loss order request"""
        return OrderRequest.create(
            symbol=symbol,
            side=side,
            order_type=OrderType.STOP_LOSS,
            quantity=quantity,
            price=stop_price,
            **kwargs
        )
    
    @staticmethod
    def take_profit(
        symbol: str,
        side: OrderSide,
        quantity: float,
        limit_price: float,
        **kwargs
    ) -> Result[OrderRequest, ValidationError]:
        """Create a take profit order request"""
        return OrderRequest.create(
            symbol=symbol,
            side=side,
            order_type=OrderType.TAKE_PROFIT,
            quantity=quantity,
            price=limit_price,
            **kwargs
        )

# ====================== PROTOCOLS ======================

@runtime_checkable
class Executable(Protocol):
    """Protocol for objects that can be executed"""
    symbol: Symbol
    side: OrderSide
    quantity: float
    price: Optional[float]
    
    def to_order_request(self) -> Result[OrderRequest, ValidationError]:
        """Convert to OrderRequest"""
        ...

# ====================== METRICS COLLECTOR ======================

class ExecutionMetrics:
    """Collects and aggregates execution metrics"""
    
    def __init__(self):
        self._monitor = PerformanceMonitor()
        self._orders: Dict[str, Order] = {}
        self._fills: Dict[str, List[TradeFill]] = {}
        
    def record_order(self, order: Order) -> None:
        """Record an order"""
        self._orders[order.order_id] = order
        self._monitor.record_metric(
            label=f"order_{order.status.value}",
            duration_ms=0.0,
            metadata={"order_id": order.order_id, "symbol": order.symbol}
        )
    
    def record_fill(self, fill: TradeFill) -> None:
        """Record a fill"""
        if fill.order_id not in self._fills:
            self._fills[fill.order_id] = []
        self._fills[fill.order_id].append(fill)
        
        self._monitor.record_metric(
            label="fill",
            duration_ms=fill.latency_ms or 0.0,
            metadata={
                "order_id": fill.order_id,
                "symbol": fill.symbol,
                "slippage_bps": fill.slippage_bps or 0.0
            }
        )
    
    def get_order_report(self, order_id: str) -> Optional[ExecutionReport]:
        """Get execution report for an order"""
        order = self._orders.get(order_id)
        fills = self._fills.get(order_id, [])
        
        if order and fills:
            return ExecutionReport.from_order_and_fills(order, fills)
        return None
    
    def get_summary(self) -> Dict[str, Any]:
        """Get summary statistics"""
        return {
            "total_orders": len(self._orders),
            "total_fills": sum(len(f) for f in self._fills.values()),
            "performance_summary": self._monitor.get_summary(),
            "active_orders": sum(1 for o in self._orders.values() if o.is_active),
            "completed_orders": sum(1 for o in self._orders.values() if o.is_terminal),
        }

# ====================== TYPE ALIASES ======================

# Hasil operasi Order: Berhasil (Order) atau Gagal (String Error)
OrderResult = Result[Order, str] 

# Hasil operasi Fill: Berhasil (TradeFill) atau Gagal (String Error)
FillResult = Result[TradeFill, str]

# Hasil operasi Report
ExecutionReportResult = Result[ExecutionReport, str]

# ====================== EXPORTS ======================

__all__ = [
    # Core Types
    'OrderType',
    'OrderSide',
    'OrderStatus',
    'TimeInForce',
    
    # Core Entities
    'OrderRequest',
    'Order',
    'TradeFill',
    'ExecutionReport',
    
    # Factory
    'OrderFactory',
    
    # Protocols
    'Executable',
    
    # [TAMBAHKAN INI] Type Aliases
    'OrderResult',
    'FillResult',
    'ExecutionReportResult' 
]
