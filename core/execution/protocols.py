"""
INDUSTRIAL-GRADE EXECUTION PROTOCOLS
Location: core/execution/protocols.py
Desc: Protocol interfaces (ABCs) dengan full monadic support, 
      async/await pattern, dan comprehensive error handling.
"""

import asyncio
from abc import ABC, abstractmethod
from typing import (
    Protocol, runtime_checkable, 
    TypeVar, AsyncIterator, 
    Optional, List, Dict, Any, Tuple
)
from dataclasses import dataclass
from datetime import datetime, timedelta
import inspect

# Import shared utilities
from ..shared.result import (
    Result, Ok, Err, safe_async, 
    match_result, MonadMetrics, is_ok, is_err
)
from ..shared.performance import PerformanceMonitor
from ..shared.utils import get_logger

# Import local types
from .types import (
    Order, OrderRequest, TradeFill, ExecutionReport,
    OrderSide, TimeInForce,
    Symbol, ValidationError
)

# ====================== TYPE VARIABLES ======================

T = TypeVar("T")
E = TypeVar("E", bound=Exception)
R = TypeVar("R", covariant=True)  # Return type for protocols

# ====================== CUSTOM EXCEPTIONS ======================

class ExecutionError(Exception):
    """Base exception untuk semua execution errors"""
    def __init__(self, message: str, order_id: Optional[str] = None, 
                 code: Optional[str] = None):
        self.message = message
        self.order_id = order_id
        self.code = code or "EXECUTION_ERROR"
        super().__init__(self.message)

class ConnectionError(ExecutionError):
    """Error koneksi ke exchange/simulator"""
    pass

class RateLimitError(ExecutionError):
    """Rate limiting error"""
    pass

class InsufficientFundsError(ExecutionError):
    """Insufficient margin/balance"""
    pass

class OrderNotFoundError(ExecutionError):
    """Order tidak ditemukan"""
    pass

class CancelFailedError(ExecutionError):
    """Gagal cancel order"""
    pass

# ====================== CONFIGURATION STRUCTURES ======================

@dataclass(frozen=True)
class ExecutionConfig:
    """
    Immutable configuration untuk execution engine.
    Supports dependency injection dan runtime configuration.
    """
    # Connection settings
    api_key: Optional[str] = None
    api_secret: Optional[str] = None
    base_url: str = "https://api.exchange.com"
    timeout_seconds: float = 30.0
    max_connections: int = 10
    
    # Order settings
    default_time_in_force: TimeInForce = TimeInForce.GTC
    enable_post_only: bool = True
    enable_reduce_only: bool = True
    max_order_size: float = 1000.0
    min_order_size: float = 0.001
    
    # Retry settings
    max_retries: int = 3
    retry_delay_ms: float = 100.0
    exponential_backoff: bool = True
    
    # Risk limits
    max_open_orders: int = 100
    max_notional_per_order: Optional[float] = None
    max_notional_per_symbol: Optional[float] = None
    max_notional_total: Optional[float] = None
    
    # Simulation settings
    simulation_mode: bool = False
    simulated_latency_ms: Tuple[float, float] = (5.0, 50.0)  # min, max
    fill_probability: float = 1.0
    enable_slippage: bool = True
    slippage_model: str = "proportional"  # "proportional", "constant", "none"
    
    # Monitoring
    enable_metrics: bool = True
    enable_telemetry: bool = True
    log_level: str = "INFO"
    
    # Metadata
    client_name: str = "orca-execution"
    version: str = "2.0.0"
    tags: Dict[str, Any] = None
    
    def __post_init__(self):
        # Validate configuration
        if self.min_order_size <= 0:
            raise ValueError("min_order_size must be positive")
        if self.max_order_size <= self.min_order_size:
            raise ValueError("max_order_size must be greater than min_order_size")
        if self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        
        # Initialize tags if None
        if self.tags is None:
            object.__setattr__(self, 'tags', {})
    
    @classmethod
    def create_default(cls) -> 'ExecutionConfig':
        """Factory method untuk default configuration"""
        return cls()
    
    def with_updates(self, **kwargs) -> 'ExecutionConfig':
        """Create new config with updated values (immutable update)"""
        new_dict = self.__dict__.copy()
        new_dict.update(kwargs)
        
        # Handle nested structures
        if 'tags' in kwargs and isinstance(kwargs['tags'], dict):
            new_dict['tags'] = {**self.tags, **kwargs['tags']}
        
        return cls(**new_dict)
    
    def validate_for_symbol(self, symbol: Symbol) -> Result[None, ValidationError]:
        """Validate config for specific symbol"""
        # Placeholder untuk symbol-specific validation
        # Contoh: cek jika symbol sesuai dengan aturan exchange
        return Ok(None)

# ====================== PROTOCOL INTERFACES ======================

@runtime_checkable
class ExecutionClient(Protocol):
    """
    Core protocol untuk execution client.
    Protocol-based untuk runtime checking dan flexibility.
    """
    
    async def submit_order(self, request: OrderRequest) -> Result[Order, ExecutionError]:
        """Submit order ke exchange/simulator"""
        ...
    
    async def cancel_order(self, order_id: str, symbol: Symbol) -> Result[bool, ExecutionError]:
        """Cancel order"""
        ...
    
    async def get_order(self, order_id: str, symbol: Symbol) -> Result[Order, ExecutionError]:
        """Get order status"""
        ...
    
    async def get_open_orders(self, symbol: Optional[Symbol] = None) -> Result[List[Order], ExecutionError]:
        """Get semua open orders"""
        ...
    
    async def cancel_all_orders(self, symbol: Optional[Symbol] = None) -> Result[int, ExecutionError]:
        """Cancel semua orders untuk symbol tertentu (atau semua)"""
        ...
    
    async def get_fills(
        self, 
        order_id: Optional[str] = None,
        symbol: Optional[Symbol] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> Result[List[TradeFill], ExecutionError]:
        """Get trade fills dengan filter"""
        ...
    
    async def get_execution_report(self, order_id: str) -> Result[ExecutionReport, ExecutionError]:
        """Get comprehensive execution report untuk order"""
        ...

class IExecutionClient(ABC):
    """
    Abstract Base Class untuk execution client.
    Lebih formal daripada Protocol, mendukung dependency injection dan testing.
    """
    
    def __init__(self, config: ExecutionConfig):
        self.config = config
        self.logger = get_logger(f"execution.{self.__class__.__name__}")
        self.metrics = MonadMetrics() if config.enable_metrics else None
        self.performance_monitor = PerformanceMonitor() if config.enable_telemetry else None
        self._connected = False
        
    @abstractmethod
    async def connect(self) -> Result[None, ConnectionError]:
        """Connect ke exchange/simulator"""
        pass
    
    @abstractmethod
    async def disconnect(self) -> Result[None, ConnectionError]:
        """Disconnect dari exchange/simulator"""
        pass
    
    @abstractmethod
    async def submit_order(self, request: OrderRequest) -> Result[Order, ExecutionError]:
        """Submit order ke exchange/simulator"""
        pass
    
    @abstractmethod
    async def cancel_order(self, order_id: str, symbol: Symbol) -> Result[bool, ExecutionError]:
        """Cancel order"""
        pass
    
    @abstractmethod
    async def get_order(self, order_id: str, symbol: Symbol) -> Result[Order, ExecutionError]:
        """Get order status"""
        pass
    
    @abstractmethod
    async def get_open_orders(self, symbol: Optional[Symbol] = None) -> Result[List[Order], ExecutionError]:
        """Get semua open orders"""
        pass
    
    @abstractmethod
    async def get_fills(
        self, 
        order_id: Optional[str] = None,
        symbol: Optional[Symbol] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> Result[List[TradeFill], ExecutionError]:
        """Get trade fills dengan filter"""
        pass
    
    # ========== CONVENIENCE METHODS ==========
    
    async def submit_and_track(
        self, 
        request: OrderRequest,
        timeout_seconds: float = 30.0,
        poll_interval_ms: float = 100.0
    ) -> Result[ExecutionReport, ExecutionError]:
        """
        Submit order dan track sampai complete (filled, canceled, atau rejected).
        Menggunakan exponential backoff polling.
        """
        # Submit order
        submit_result = await self.submit_order(request)
        
        def _track_order(order: Order) -> Result[ExecutionReport, ExecutionError]:
            """Inner function untuk tracking"""
            import time
            
            start_time = time.time()
            last_poll_time = start_time
            poll_count = 0
            
            while time.time() - start_time < timeout_seconds:
                if order.is_terminal:
                    # Get fills untuk order ini
                    fills_result = await self.get_fills(order_id=order.order_id)
                    
                    return match_result(
                        fills_result,
                        on_ok=lambda fills: Ok(ExecutionReport.from_order_and_fills(order, fills)),
                        on_err=lambda e: Err(e)
                    )
                
                # Exponential backoff polling
                current_time = time.time()
                if current_time - last_poll_time >= (poll_interval_ms / 1000) * (1.5 ** poll_count):
                    # Poll untuk update order status
                    order_result = await self.get_order(order.order_id, order.symbol)
                    
                    match_result(
                        order_result,
                        on_ok=lambda updated_order: setattr(order, 'status', updated_order.status),
                        on_err=lambda e: self.logger.warning(f"Failed to poll order {order.order_id}: {e}")
                    )
                    
                    last_poll_time = current_time
                    poll_count += 1
                
                await asyncio.sleep(0.001)  # Small sleep untuk cooperative multitasking
            
            return Err(ExecutionError(f"Order tracking timeout setelah {timeout_seconds}s", order_id=order.order_id))
        
        # Chain operations
        return await submit_result.and_then(_track_order)
    
    async def batch_submit(
        self, 
        requests: List[OrderRequest],
        max_concurrent: int = 5
    ) -> Result[List[Order], ExecutionError]:
        """
        Submit multiple orders secara concurrent dengan rate limiting.
        """
        import asyncio
        from typing import List as TList
        
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def submit_with_semaphore(request: OrderRequest) -> Result[Order, ExecutionError]:
            async with semaphore:
                return await self.submit_order(request)
        
        # Run semua tasks concurrent dengan semaphore
        tasks = [submit_with_semaphore(req) for req in requests]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        orders: TList[Order] = []
        errors: TList[ExecutionError] = []
        
        for result in results:
            if isinstance(result, Exception):
                errors.append(ExecutionError(f"Batch submit error: {result}"))
            elif is_err(result):
                errors.append(result.error)
            else:
                orders.append(result.value)
        
        if errors:
            return Err(ExecutionError(
                f"Batch submit completed dengan {len(errors)} errors",
                code="BATCH_PARTIAL_FAILURE"
            ))
        
        return Ok(orders)
    
    async def replace_order(
        self, 
        order_id: str, 
        symbol: Symbol,
        new_quantity: Optional[float] = None,
        new_price: Optional[float] = None
    ) -> Result[Order, ExecutionError]:
        """
        Replace existing order dengan parameter baru.
        Implementasi default: cancel + resubmit.
        """
        # Get existing order
        order_result = await self.get_order(order_id, symbol)
        if is_err(order_result):
            return order_result
        
        existing_order = order_result.value
        
        # Cancel existing order
        cancel_result = await self.cancel_order(order_id, symbol)
        if is_err(cancel_result):
            return Err(CancelFailedError(f"Failed to cancel order {order_id}"))
        
        # Create new order request
        from .types import OrderFactory
        
        new_quantity = new_quantity or existing_order.quantity
        new_price = new_price or existing_order.price
        
        request_result = OrderFactory.create(
            symbol=symbol,
            side=existing_order.side,
            order_type=existing_order.order_type,
            quantity=new_quantity,
            price=new_price,
            time_in_force=existing_order.time_in_force,
            reduce_only=existing_order.reduce_only,
            post_only=existing_order.post_only,
            client_order_id=f"replace_{existing_order.client_order_id}",
            strategy_id=existing_order.strategy_id
        )
        
        if is_err(request_result):
            return Err(ExecutionError(f"Invalid replacement order: {request_result.error}"))
        
        # Submit new order
        return await self.submit_order(request_result.value)
    
    # ========== HEALTH CHECKS ==========
    
    async def health_check(self) -> Result[Dict[str, Any], ExecutionError]:
        """
        Comprehensive health check untuk execution client.
        """
        health_data = {
            "connected": self._connected,
            "config": self.config.__dict__,
            "timestamp": datetime.now().isoformat(),
            "metrics": self.metrics.__dict__ if self.metrics else None,
            "performance": self.performance_monitor.get_summary() if self.performance_monitor else None,
        }
        
        # Coba simple operation untuk test connectivity
        try:
            # Get open orders dengan limit 1 sebagai connectivity test
            test_result = await self.get_open_orders()
            health_data["connectivity_test"] = is_ok(test_result)
            health_data["open_orders_count"] = len(test_result.value) if is_ok(test_result) else 0
        except Exception as e:
            health_data["connectivity_test"] = False
            health_data["connectivity_error"] = str(e)
        
        return Ok(health_data)
    
    # ========== CONTEXT MANAGER SUPPORT ==========
    
    async def __aenter__(self):
        """Async context manager entry"""
        connect_result = await self.connect()
        if is_err(connect_result):
            raise ConnectionError(f"Failed to connect: {connect_result.error}")
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        disconnect_result = await self.disconnect()
        if is_err(disconnect_result):
            self.logger.warning(f"Failed to disconnect cleanly: {disconnect_result.error}")
        
        # Log metrics sebelum exit
        if self.metrics:
            self.logger.info(f"Execution metrics: {self.metrics.success_rate:.2%} success rate")
        
        return False  # Don't suppress exceptions

# ====================== MARKET DATA PROTOCOLS ======================

@dataclass(frozen=True)
class MarketDataConfig:
    """Configuration untuk market data client"""
    
    # Subscription settings
    symbols: List[Symbol]
    update_frequency_ms: float = 100.0  # Untuk polling-based clients
    enable_websocket: bool = True
    enable_historical: bool = True
    
    # Data granularity
    tick_level: bool = True  # Trade-by-trade data
    orderbook_levels: int = 10  # Depth of orderbook
    enable_ohlcv: bool = True
    ohlcv_interval: str = "1m"  # 1m, 5m, 15m, 1h, etc.
    
    # Caching
    cache_size: int = 1000  # Number of ticks/candles to cache
    enable_compression: bool = True
    
    # Backtesting support
    replay_speed: float = 1.0  # 1.0 = realtime, 2.0 = 2x speed, etc.
    loop_mode: bool = False  # Loop historical data
    
    def validate(self) -> Result[None, ValidationError]:
        """Validate configuration"""
        if not self.symbols:
            return Err(ValidationError("At least one symbol must be specified"))
        
        if self.update_frequency_ms <= 0:
            return Err(ValidationError("update_frequency_ms must be positive"))
        
        return Ok(None)

class IMarketDataClient(ABC):
    """
    Abstract Base Class untuk market data client.
    Mendukung real-time streaming dan historical data.
    """
    
    def __init__(self, config: MarketDataConfig):
        self.config = config
        self.logger = get_logger(f"marketdata.{self.__class__.__name__}")
        self._subscribers: Dict[str, List[Any]] = {}
        
    @abstractmethod
    async def connect(self) -> Result[None, ConnectionError]:
        """Connect ke data source"""
        pass
    
    @abstractmethod
    async def disconnect(self) -> Result[None, ConnectionError]:
        """Disconnect dari data source"""
        pass
    
    @abstractmethod
    async def subscribe(self, symbols: List[Symbol]) -> Result[None, ConnectionError]:
        """Subscribe ke symbols tertentu"""
        pass
    
    @abstractmethod
    async def unsubscribe(self, symbols: List[Symbol]) -> Result[None, ConnectionError]:
        """Unsubscribe dari symbols"""
        pass
    
    @abstractmethod
    def get_latest_price(self, symbol: Symbol) -> Result[float, ConnectionError]:
        """Get latest price untuk symbol"""
        pass
    
    @abstractmethod
    def get_orderbook(self, symbol: Symbol, depth: int = 10) -> Result[Dict[str, Any], ConnectionError]:
        """Get orderbook snapshot"""
        pass
    
    @abstractmethod
    def get_ohlcv(
        self, 
        symbol: Symbol, 
        interval: str = "1m", 
        limit: int = 100
    ) -> Result[List[Dict[str, Any]], ConnectionError]:
        """Get OHLCV candles"""
        pass
    
    # ========== STREAMING INTERFACE ==========
    
    @abstractmethod
    async def stream_ticks(self, symbol: Symbol) -> AsyncIterator[Dict[str, Any]]:
        """Stream real-time ticks"""
        yield {}
    
    @abstractmethod
    async def stream_orderbook(self, symbol: Symbol) -> AsyncIterator[Dict[str, Any]]:
        """Stream orderbook updates"""
        yield {}
    
    # ========== EVENT PUB/SUB ==========
    
    def subscribe_to_event(self, event_type: str, callback: Any) -> None:
        """Subscribe to specific event type"""
        if event_type not in self._subscribers:
            self._subscribers[event_type] = []
        self._subscribers[event_type].append(callback)
    
    def unsubscribe_from_event(self, event_type: str, callback: Any) -> None:
        """Unsubscribe from event type"""
        if event_type in self._subscribers and callback in self._subscribers[event_type]:
            self._subscribers[event_type].remove(callback)
    
    def _publish_event(self, event_type: str, data: Any) -> None:
        """Publish event ke semua subscribers"""
        if event_type in self._subscribers:
            for callback in self._subscribers[event_type]:
                try:
                    # Handle both sync dan async callbacks
                    if inspect.iscoroutinefunction(callback):
                        asyncio.create_task(callback(data))
                    else:
                        callback(data)
                except Exception as e:
                    self.logger.error(f"Error in event callback: {e}")

# ====================== RISK MANAGER PROTOCOLS ======================

@dataclass(frozen=True)
class RiskConfig:
    """Configuration untuk risk management"""
    
    # Position limits
    max_position_size: Dict[Symbol, float] = None  # Per symbol
    max_total_position: Optional[float] = None  # Cross-symbol
    
    # Order limits
    max_order_size: Dict[Symbol, float] = None  # Per symbol
    max_total_orders: int = 100
    max_open_orders_per_symbol: int = 20
    
    # Loss limits
    max_daily_loss: Optional[float] = None  # Absolute loss limit
    max_drawdown: Optional[float] = None  # Percentage drawdown limit
    stop_loss_threshold: Optional[float] = None  # Auto-stop threshold
    
    # Rate limiting
    max_orders_per_minute: int = 60
    max_notional_per_minute: Optional[float] = None
    
    # Circuit breakers
    enable_circuit_breakers: bool = True
    circuit_breaker_threshold: float = 0.05  # 5% price movement
    circuit_breaker_window_ms: float = 1000.0
    
    def __post_init__(self):
        if self.max_position_size is None:
            object.__setattr__(self, 'max_position_size', {})
        if self.max_order_size is None:
            object.__setattr__(self, 'max_order_size', {})

class IRiskManager(ABC):
    """
    Risk management interface dengan pre-trade dan post-trade checks.
    """
    
    def __init__(self, config: RiskConfig):
        self.config = config
        self.logger = get_logger(f"risk.{self.__class__.__name__}")
        self.violations: List[Dict[str, Any]] = []
        
        # State tracking
        self._positions: Dict[Symbol, float] = {}
        self._orders_today: List[Dict[str, Any]] = []
        self._pnl_today: float = 0.0
        
    @abstractmethod
    async def pre_trade_check(self, request: OrderRequest) -> Result[OrderRequest, ValidationError]:
        """
        Pre-trade validation. Mengembalikan modified request jika approved,
        atau error jika rejected.
        """
        pass
    
    @abstractmethod
    async def post_trade_check(self, fill: TradeFill) -> Result[None, ValidationError]:
        """
        Post-trade validation dan state update.
        """
        pass
    
    @abstractmethod
    async def can_submit_order(self, request: OrderRequest) -> Result[bool, ValidationError]:
        """
        Quick check apakah order bisa di-submit.
        """
        pass
    
    @abstractmethod
    async def get_risk_summary(self) -> Dict[str, Any]:
        """
        Get current risk summary.
        """
        pass
    
    # ========== DEFAULT IMPLEMENTATIONS ==========
    
    async def check_position_limit(self, symbol: Symbol, new_position: float) -> Result[None, ValidationError]:
        """Check position limit untuk symbol"""
        max_size = self.config.max_position_size.get(symbol)
        
        if max_size is not None and abs(new_position) > max_size:
            return Err(ValidationError(
                f"Position limit exceeded for {symbol}: "
                f"{new_position} > {max_size}"
            ))
        
        return Ok(None)
    
    async def check_order_rate_limit(self) -> Result[None, ValidationError]:
        """Check order rate limiting"""
        from datetime import datetime
        
        now = datetime.now()
        one_minute_ago = now - timedelta(minutes=1)
        
        recent_orders = [
            o for o in self._orders_today 
            if o.get('timestamp', now) > one_minute_ago
        ]
        
        if len(recent_orders) >= self.config.max_orders_per_minute:
            return Err(ValidationError(
                f"Rate limit exceeded: {len(recent_orders)} orders in last minute"
            ))
        
        return Ok(None)
    
    async def check_daily_loss_limit(self) -> Result[None, ValidationError]:
        """Check daily loss limit"""
        if self.config.max_daily_loss is not None and self._pnl_today < -self.config.max_daily_loss:
            return Err(ValidationError(
                f"Daily loss limit exceeded: {self._pnl_today} < {-self.config.max_daily_loss}"
            ))
        
        return Ok(None)
    
    def record_violation(self, check_name: str, message: str, level: str = "WARNING") -> None:
        """Record risk violation"""
        violation = {
            "timestamp": datetime.now().isoformat(),
            "check": check_name,
            "message": message,
            "level": level,
            "context": {
                "positions": self._positions.copy(),
                "pnl_today": self._pnl_today,
                "orders_today": len(self._orders_today)
            }
        }
        self.violations.append(violation)
        self.logger.warning(f"Risk violation [{check_name}]: {message}")

# ====================== SIMULATOR PROTOCOL ======================

class ISimulator(ABC):
    """
    Interface untuk execution simulator.
    Menggabungkan execution, market data, dan risk management.
    """
    
    def __init__(
        self,
        execution_client: IExecutionClient,
        market_data_client: IMarketDataClient,
        risk_manager: IRiskManager,
        config: ExecutionConfig
    ):
        self.execution = execution_client
        self.market_data = market_data_client
        self.risk = risk_manager
        self.config = config
        self.logger = get_logger(f"simulator.{self.__class__.__name__}")
        
        # State
        self._is_running = False
        self._simulation_time = datetime.now()
        self._simulation_speed = config.simulation_mode if isinstance(config.simulation_mode, float) else 1.0
        
    @abstractmethod
    async def start(self) -> Result[None, ExecutionError]:
        """Start simulator"""
        pass
    
    @abstractmethod
    async def stop(self) -> Result[None, ExecutionError]:
        """Stop simulator"""
        pass
    
    @abstractmethod
    async def reset(self) -> Result[None, ExecutionError]:
        """Reset simulator state"""
        pass
    
    @abstractmethod
    async def simulate_order(self, request: OrderRequest) -> Result[ExecutionReport, ExecutionError]:
        """
        Simulate order execution dengan semua mekanisme:
        - Latency simulation
        - Fill probability
        - Slippage modeling
        - Market impact
        """
        pass
    
    @abstractmethod
    async def simulate_market_order(
        self,
        symbol: Symbol,
        side: OrderSide,
        quantity: float,
        **kwargs
    ) -> Result[ExecutionReport, ExecutionError]:
        """Convenience method untuk market order simulation"""
        pass
    
    @abstractmethod
    async def simulate_limit_order(
        self,
        symbol: Symbol,
        side: OrderSide,
        quantity: float,
        price: float,
        **kwargs
    ) -> Result[ExecutionReport, ExecutionError]:
        """Convenience method untuk limit order simulation"""
        pass
    
    @abstractmethod
    async def get_simulation_state(self) -> Dict[str, Any]:
        """Get current simulation state"""
        pass
    
    @abstractmethod
    async def advance_time(self, delta_ms: float) -> Result[None, ExecutionError]:
        """Advance simulation time (untuk backtesting)"""
        pass
    
    # ========== BATCH SIMULATION ==========
    
    async def simulate_batch(
        self, 
        requests: List[OrderRequest],
        execution_strategy: str = "sequential"  # "sequential", "concurrent", "vwap"
    ) -> Result[List[ExecutionReport], ExecutionError]:
        """
        Simulate batch of orders dengan berbagai execution strategies.
        """
        if execution_strategy == "sequential":
            results = []
            for req in requests:
                result = await self.simulate_order(req)
                if is_err(result):
                    return result.map_err(lambda e: ExecutionError(
                        f"Batch simulation failed: {e}",
                        code="BATCH_SIMULATION_ERROR"
                    ))
                results.append(result.value)
            return Ok(results)
        
        elif execution_strategy == "concurrent":
            # Run semua simulations concurrent
            tasks = [self.simulate_order(req) for req in requests]
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Process results
            reports = []
            errors = []
            
            for i, result in enumerate(batch_results):
                if isinstance(result, Exception):
                    errors.append(f"Order {i}: {result}")
                elif is_err(result):
                    errors.append(f"Order {i}: {result.error}")
                else:
                    reports.append(result.value)
            
            if errors:
                return Err(ExecutionError(
                    f"Batch simulation completed dengan errors: {errors}",
                    code="BATCH_PARTIAL_FAILURE"
                ))
            
            return Ok(reports)
        
        else:
            return Err(ExecutionError(
                f"Unknown execution strategy: {execution_strategy}",
                code="INVALID_STRATEGY"
            ))
    
    # ========== PERFORMANCE ANALYSIS ==========
    
    async def analyze_performance(
        self,
        reports: List[ExecutionReport],
        benchmark_price: Optional[float] = None
    ) -> Dict[str, Any]:
        """
        Analyze execution performance metrics.
        """
        if not reports:
            return {"error": "No reports to analyze"}
        
        # Calculate basic metrics
        total_notional = sum(r.net_notional for r in reports)
        total_fees = sum(r.total_fees for r in reports)
        total_quantity = sum(sum(f.quantity for f in r.fills) for r in reports)
        
        # Calculate slippage if benchmark price provided
        slippage_metrics = []
        if benchmark_price is not None:
            for report in reports:
                if report.fills:
                    fill_prices = [f.price for f in report.fills]
                    avg_price = sum(fill_prices) / len(fill_prices)
                    slippage_bps = ((avg_price - benchmark_price) / benchmark_price) * 10000
                    slippage_metrics.append({
                        "order_id": report.order.order_id,
                        "slippage_bps": slippage_bps,
                        "avg_price": avg_price,
                        "benchmark_price": benchmark_price
                    })
        
        # Calculate latency metrics
        latencies = []
        for report in reports:
            if report.order.total_latency is not None:
                latencies.append(report.order.total_latency)
        
        return {
            "summary": {
                "total_orders": len(reports),
                "total_notional": total_notional,
                "total_fees": total_fees,
                "total_quantity": total_quantity,
                "avg_fee_rate_bps": (total_fees / total_notional * 10000) if total_notional > 0 else 0,
            },
            "slippage": {
                "total_trades": len(slippage_metrics),
                "avg_slippage_bps": sum(s['slippage_bps'] for s in slippage_metrics) / len(slippage_metrics) if slippage_metrics else 0,
                "details": slippage_metrics
            } if slippage_metrics else None,
            "latency": {
                "avg_ms": sum(latencies) / len(latencies) if latencies else 0,
                "max_ms": max(latencies) if latencies else 0,
                "min_ms": min(latencies) if latencies else 0,
                "count": len(latencies)
            } if latencies else None,
            "timestamp": datetime.now().isoformat()
        }

# ====================== FACTORY PROTOCOLS ======================

class IExecutionFactory(ABC):
    """
    Factory untuk membuat execution components.
    Mendukung dependency injection dan configuration management.
    """
    
    @abstractmethod
    def create_execution_client(self, config: ExecutionConfig) -> IExecutionClient:
        """Create execution client instance"""
        pass
    
    @abstractmethod
    def create_market_data_client(self, config: MarketDataConfig) -> IMarketDataClient:
        """Create market data client instance"""
        pass
    
    @abstractmethod
    def create_risk_manager(self, config: RiskConfig) -> IRiskManager:
        """Create risk manager instance"""
        pass
    
    @abstractmethod
    def create_simulator(
        self,
        execution_config: ExecutionConfig,
        market_data_config: MarketDataConfig,
        risk_config: RiskConfig
    ) -> ISimulator:
        """Create simulator instance dengan semua dependencies"""
        pass
    
    # ========== BUILDER PATTERN ==========
    
    @classmethod
    def builder(cls) -> 'ExecutionFactoryBuilder':
        """Create builder untuk factory configuration"""
        return ExecutionFactoryBuilder()

class ExecutionFactoryBuilder:
    """
    Builder pattern untuk factory configuration.
    """
    
    def __init__(self):
        self._execution_config = ExecutionConfig()
        self._market_data_config = MarketDataConfig(symbols=[])
        self._risk_config = RiskConfig()
        self._client_type = "simulator"  # "simulator", "live", "mock"
        
    def with_execution_config(self, config: ExecutionConfig) -> 'ExecutionFactoryBuilder':
        self._execution_config = config
        return self
    
    def with_market_data_config(self, config: MarketDataConfig) -> 'ExecutionFactoryBuilder':
        self._market_data_config = config
        return self
    
    def with_risk_config(self, config: RiskConfig) -> 'ExecutionFactoryBuilder':
        self._risk_config = config
        return self
    
    def with_client_type(self, client_type: str) -> 'ExecutionFactoryBuilder':
        self._client_type = client_type
        return self
    
    def build(self) -> IExecutionFactory:
        """Build factory instance berdasarkan configuration"""
        # Ini akan diimplementasikan di concrete factory
        # Untuk sekarang return abstract factory
        
        class ConcreteFactory(IExecutionFactory):
            def create_execution_client(self, config: ExecutionConfig) -> IExecutionClient:
                from .simulator import ExecutionSimulator
                return ExecutionSimulator(config)
            
            def create_market_data_client(self, config: MarketDataConfig) -> IMarketDataClient:
                # Import concrete implementation
                from .mechanics import MarketDataSimulator
                return MarketDataSimulator(config)
            
            def create_risk_manager(self, config: RiskConfig) -> IRiskManager:
                from .mechanics import DefaultRiskManager
                return DefaultRiskManager(config)
            
            def create_simulator(
                self,
                execution_config: ExecutionConfig,
                market_data_config: MarketDataConfig,
                risk_config: RiskConfig
            ) -> ISimulator:
                # Create all components
                execution = self.create_execution_client(execution_config)
                market_data = self.create_market_data_client(market_data_config)
                risk = self.create_risk_manager(risk_config)
                
                # Create simulator
                from .simulator import ExecutionSimulator
                return ExecutionSimulator(execution, market_data, risk, execution_config)
        
        return ConcreteFactory()

# ====================== DECORATORS ======================

def retry_on_failure(max_retries: int = 3, delay_ms: float = 100.0):
    """
    Decorator untuk retry operations pada execution failures.
    """
    def decorator(func):
        @safe_async
        async def wrapper(*args, **kwargs):
            last_error = None
            
            for attempt in range(max_retries + 1):
                try:
                    result = await func(*args, **kwargs)
                    
                    # Check jika result adalah Result type
                    if isinstance(result, (Ok, Err)):
                        if isinstance(result, Ok):
                            return result
                        else:
                            last_error = result.error
                    else:
                        # Assume success jika bukan Result type
                        return Ok(result)
                
                except Exception as e:
                    last_error = e
                
                # Exponential backoff
                if attempt < max_retries:
                    await asyncio.sleep((delay_ms / 1000) * (2 ** attempt))
            
            return Err(ExecutionError(
                f"Operation failed after {max_retries} retries: {last_error}",
                code="MAX_RETRIES_EXCEEDED"
            ))
        
        return wrapper
    
    return decorator

def validate_order_request(func):
    """
    Decorator untuk validate order request sebelum processing.
    """
    @safe_async
    async def wrapper(self, request: OrderRequest, *args, **kwargs):
        # Basic validation
        if not request.symbol:
            return Err(ValidationError("Symbol is required"))
        
        if request.quantity <= 0:
            return Err(ValidationError("Quantity must be positive"))
        
        if request.order_type.requires_price and request.price is None:
            return Err(ValidationError(f"{request.order_type.value} order requires price"))
        
        if request.price is not None and request.price <= 0:
            return Err(ValidationError("Price must be positive if provided"))
        
        # Call original function
        return await func(self, request, *args, **kwargs)
    
    return wrapper

# ====================== EVENT BUS PROTOCOL ======================

class IEventBus(ABC):
    """
    Event bus untuk decoupled communication antara components.
    """
    
    @abstractmethod
    async def publish(self, event_type: str, data: Any) -> Result[None, ConnectionError]:
        """Publish event ke bus"""
        pass
    
    @abstractmethod
    async def subscribe(self, event_type: str, callback: Any) -> Result[str, ConnectionError]:
        """Subscribe ke event type, returns subscription ID"""
        pass
    
    @abstractmethod
    async def unsubscribe(self, subscription_id: str) -> Result[None, ConnectionError]:
        """Unsubscribe menggunakan subscription ID"""
        pass
    
    @abstractmethod
    async def get_subscription_count(self, event_type: str) -> int:
        """Get jumlah subscribers untuk event type"""
        pass

# ====================== EXPORTS ======================

__all__ = [
    # Core Protocols
    'ExecutionClient',
    'IExecutionClient',
    'IMarketDataClient',
    'IRiskManager',
    'ISimulator',
    'IExecutionFactory',
    'IEventBus',
    
    # Configurations
    'ExecutionConfig',
    'MarketDataConfig',
    'RiskConfig',
    
    # Exceptions
    'ExecutionError',
    'ConnectionError',
    'RateLimitError',
    'InsufficientFundsError',
    'OrderNotFoundError',
    'CancelFailedError',
    
    # Builders
    'ExecutionFactoryBuilder',
    
    # Decorators
    'retry_on_failure',
    'validate_order_request',
]
