"""
INDUSTRIAL EXCHANGE EMULATOR
Location: core/execution/engine/emulator.py
Desc: Stateful emulator dengan adaptive behavior, 
      circuit breakers, dan market regime detection.
"""

import time
import random
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List, Deque, Union  
from collections import defaultdict, deque
from enum import Enum

# Import Core Types
from core.execution.types import (
    Order, 
    OrderSide, 
    OrderType
)
from .types import Trade, Rejection, RejectionCode
from core.execution.mechanics.factory import MechanicsSuite
from core.shared.result import Result, Ok, Err
from core.shared.utils import get_logger

# Pastikan Dict, Any, List, Deque, defaultdict ada
# ====================== MARKET REGIME DETECTION ======================

class MarketRegime(str, Enum):
    """Market regime classification"""
    NORMAL = "NORMAL"           # Low volatility, high liquidity
    VOLATILE = "VOLATILE"       # High volatility, normal liquidity
    CRISIS = "CRISIS"           # Extreme volatility, low liquidity
    ILLIQUID = "ILLIQUID"       # Low volatility, low liquidity
    MANIPULATED = "MANIPULATED" # Abnormal orderbook patterns

@dataclass
class MarketRegimeDetector:
    """
    [UPGRADED] Detektor kondisi pasar menggunakan Rolling Statistics.
    Mendeteksi anomali volatilitas, likuiditas (spread), dan manipulasi.
    """
    # Thresholds (Sensitivity)
    volatility_threshold: float = 0.005   # 0.5% stddev per window
    crisis_threshold: float = 0.02        # 2% stddev per window (Crash)
    spread_threshold: float = 0.001       # 10 bps spread dianggap illiquid
    
    # State Memory (Rolling Windows)
    # Kita simpan returns, bukan raw price, untuk volatilitas akurat.
    returns_window: Deque[float] = field(default_factory=lambda: deque(maxlen=50))
    volume_window: Deque[float] = field(default_factory=lambda: deque(maxlen=50))
    spread_window: Deque[float] = field(default_factory=lambda: deque(maxlen=50))
    
    _last_price: float = 0.0
    _current_regime: MarketRegime = MarketRegime.NORMAL
    
    # -------------------------------------------------------------
    # METHOD 1: UPDATE (THE SENSOR)
    # -------------------------------------------------------------
    def update(self, price: float, volume: float, bid_ask_spread: float) -> None:
        """
        [UPGRADED] Ingest data tick baru dan update internal state.
        Menghitung Returns % untuk volatilitas yang scale-invariant.
        """
        # 1. Hitung Log Return / Percentage Return
        if self._last_price > 0:
            ret = (price - self._last_price) / self._last_price
            self.returns_window.append(ret)
        
        self._last_price = price
        self.volume_window.append(volume)
        self.spread_window.append(bid_ask_spread)
        
        # 2. Re-evaluate Regime (Setiap tick atau setiap N tick)
        if len(self.returns_window) > 10:
            self._recalculate_regime()

    # -------------------------------------------------------------
    # METHOD 2: CURRENT REGIME (THE GETTER)
    # -------------------------------------------------------------
    @property
    def current_regime(self) -> MarketRegime:
        return self._current_regime

    # -------------------------------------------------------------
    # INTERNAL LOGIC (THE BRAIN)
    # -------------------------------------------------------------
    def _recalculate_regime(self) -> None:
        import statistics
        
        # Hitung Volatilitas (Std Dev dari Returns)
        current_vol = statistics.stdev(self.returns_window)
        avg_spread = statistics.mean(self.spread_window)
        
        # Prioritas Deteksi:
        
        # 1. Cek Manipulasi dulu (Paling Bahaya)
        if self._detect_manipulation():
            self._current_regime = MarketRegime.MANIPULATED
            return

        # 2. Cek Crisis (Volatilitas Ekstrim OR Spread Meledak)
        if current_vol > self.crisis_threshold or avg_spread > (self.spread_threshold * 5):
            self._current_regime = MarketRegime.CRISIS
            return
            
        # 3. Cek Volatile (Volatilitas Tinggi tapi Spread Wajar)
        if current_vol > self.volatility_threshold:
            self._current_regime = MarketRegime.VOLATILE
            return
            
        # 4. Cek Illiquid (Spread Lebar tapi Harga Diam)
        if avg_spread > self.spread_threshold:
            self._current_regime = MarketRegime.ILLIQUID
            return
            
        # 5. Default
        self._current_regime = MarketRegime.NORMAL

    # -------------------------------------------------------------
    # METHOD 3: DETECT MANIPULATION (THE POLICE)
    # -------------------------------------------------------------
    def _detect_manipulation(self) -> bool:
        """
        [UPGRADED] Mendeteksi pola aneh.
        Contoh: Harga bergerak tajam (Volatile) tapi Volume NOL/Kecil (Spoofing/Paint the tape).
        """
        if len(self.returns_window) < 10:
            return False
            
        import statistics
        
        current_vol = statistics.stdev(self.returns_window)
        avg_volume = statistics.mean(self.volume_window)
        
        # Logic: High Volatility + Low Volume = Suspicious
        # (Harga gerak-gerak sendiri tanpa transaksi riil)
        if current_vol > self.volatility_threshold and avg_volume < 1.0: # Threshold volume sesuaikan aset
            return True
            
        return False
# ====================== CIRCUIT BREAKER ======================

@dataclass
class CircuitBreaker:
    """
    [UPGRADED] Pelindung sistem dengan Rolling Window Rate Limiting.
    Mendeteksi 'Algo Runaway' (spam order/rejection) dalam hitungan mikrodetik.
    """
    # Configuration
    max_orders_per_sec: int = 10         # Rate Limit Order
    max_rejections_per_sec: int = 5      # Kill Switch jika algoritma error terus
    cooldown_duration: float = 60.0      # Hukuman waktu (detik) jika tripped
    
    # State (Rolling Windows)
    # Menyimpan timestamp setiap event untuk hitungan presisi
    _order_timestamps: Deque[float] = field(default_factory=deque)
    _rejection_timestamps: Deque[float] = field(default_factory=deque)
    
    # Breaker Status
    _is_tripped: bool = False
    _tripped_until: float = 0.0
    _trip_reason: str = ""
    
    #-------------------------------------------------------------
    # 1. CHECK ORDER RATE (ROLLING WINDOW CHECK)
    # -------------------------------------------------------------
    def check_order_rate(self) -> bool:
        """
        [UPGRADED] Cek apakah rate limit terlampaui dalam 1 detik terakhir.
        True = Aman, False = Rate Limit Breached.
        """
        now = time.time()
        
        # 1. Cek apakah sedang dihukum (Tripped)
        if self._is_tripped:
            if now > self._tripped_until:
                self._reset_breaker() # Hukuman selesai
            else:
                return False # Masih dihukum

        # 2. Bersihkan timestamp kadaluwarsa (> 1 detik lalu)
        # Sliding window technique
        while self._order_timestamps and self._order_timestamps[0] < (now - 1.0):
            self._order_timestamps.popleft()
            
        # 3. Cek Burst Rate
        if len(self._order_timestamps) >= self.max_orders_per_sec:
            # Trip breaker sementara (Soft Ban)
            self._trip("RATE_LIMIT_EXCEEDED", duration=5.0) 
            return False
            
        return True

    # -------------------------------------------------------------
    # 2. RECORD ORDER (UPDATE STATE)
    # -------------------------------------------------------------
    def record_order(self) -> None:
        """Catat order baru ke dalam window"""
        self._order_timestamps.append(time.time())

    # -------------------------------------------------------------
    # 3. RECORD REJECTION (KILL SWITCH TRIGGER)
    # -------------------------------------------------------------
    def record_rejection(self) -> None:
        """
        [UPGRADED] Mencatat penolakan. 
        Jika terlalu banyak reject dalam waktu singkat -> AUTO KILL.
        """
        now = time.time()
        self._rejection_timestamps.append(now)
        
        # Bersihkan window reject lama
        while self._rejection_timestamps and self._rejection_timestamps[0] < (now - 1.0):
            self._rejection_timestamps.popleft()
            
        # Logic Sadis: Jika reject rate tinggi, matikan bot lama (Hard Ban)
        if len(self._rejection_timestamps) >= self.max_rejections_per_sec:
            self._trip("RUNAWAY_ALGO_DETECTED", duration=self.cooldown_duration)
    
    # -------------------------------------------------------------
    # 4. TRIP (MANUAL / AUTO ACTIVATION)
    # -------------------------------------------------------------
    def trip(self, reason: str = "MANUAL_TRIP", duration: Optional[float] = None) -> None:
        """
        [UPGRADED] Memutuskan sirkuit (Stop Trading).
        
        Args:
            reason: Mengapa breaker putus? (Audit Trail)
            duration: Berapa lama hukuman? (Default: self.cooldown_duration)
        """
        if self._is_tripped:
            return # Sudah putus, ignore
            
        now = time.time()
        actual_duration = duration if duration is not None else self.cooldown_duration
        
        # Set State
        self._is_tripped = True
        self._tripped_until = now + actual_duration
        self._trip_reason = reason
        
        # [CRITICAL] Log event ini. Di production, ini harus kirim Alert ke Telegram/Slack!
        # Kita print dulu untuk simulasi.
        print(f"⚠️ CIRCUIT BREAKER TRIPPED! Reason: {reason}. Halted for {actual_duration}s.")

    # -------------------------------------------------------------
    # 5. RESET (RECOVERY)
    # -------------------------------------------------------------
    def reset(self) -> None:
        """
        [UPGRADED] Menyambungkan kembali sirkuit (Resume Trading).
        Membersihkan status hukuman, tapi history (sliding window) tetap disimpan
        agar tidak langsung trip lagi jika rate masih tinggi.
        """
        self._is_tripped = False
        self._tripped_until = 0.0
        self._trip_reason = ""
        print("✅ CIRCUIT BREAKER RESET. Trading Resumed.")

    # -------------------------------------------------------------
    # 6. IS TRIPPED (SMART STATUS CHECK)
    # -------------------------------------------------------------
    def is_tripped(self) -> bool:
        """
        [UPGRADED] Cek status breaker dengan Auto-Recovery.
        
        Logic:
        1. Jika tidak tripped -> False
        2. Jika tripped TAPI durasi hukuman sudah habis -> Auto Reset -> False
        3. Jika tripped DAN masih dihukum -> True
        """
        if not self._is_tripped:
            return False
            
        # Cek apakah hukuman sudah selesai?
        if time.time() > self._tripped_until:
            self.reset() # Auto-healing
            return False
            
        return True
    
    # Alias untuk internal method yang mungkin dipanggil di snapshot sebelumnya
    def _trip(self, reason: str, duration: Optional[float] = None):
        self.trip(reason, duration)

    def _reset_breaker(self):
        self.reset()


# ====================== MARKET CONTEXT (THE SNAPSHOT) ======================

@dataclass
class MarketContext:
    """
    [FIXED] Snapshot data pasar saat order dieksekusi.
    Mendukung L1 (Top of Book) dan L2 (Orderbook Depth) simulation.
    """
    # 1. Basic Market Data
    price: float                # Mid Price / Last Price
    timestamp: float            # Unix Timestamp
    
    # 2. Liquidity Metrics
    volume: float = 0.0         # Tick Volume (Proxy likuiditas)
    volatility: float = 0.0     # Rolling Volatility (untuk Slippage)
    spread: float = 0.0         # Bid-Ask Spread
    liquidity_ratio: float = 0.1 # % Volume yang dianggap tersedia di Top Book (default 10%)
    
    # 3. L2 Depth Data (Optional, untuk simulasi Paus)
    # Format: {Price: Quantity}
    bid_depth: Dict[float, float] = field(default_factory=dict)
    ask_depth: Dict[float, float] = field(default_factory=dict)

    # -------------------------------------------------------------
    # 1. TOP OF BOOK ESTIMATION (L1)
    # -------------------------------------------------------------
    
    def mid_price(self) -> float:
        """Safe getter untuk Mid Price"""
        return self.price

    def best_bid(self) -> float:
        """Estimasi Best Bid dari Mid - Spread/2"""
        if self.spread <= 0: return self.price
        return self.price - (self.spread / 2.0)

    def best_ask(self) -> float:
        """Estimasi Best Ask dari Mid + Spread/2"""
        if self.spread <= 0: return self.price
        return self.price + (self.spread / 2.0)

    def available_liquidity(self, side: OrderSide, price_limit: Optional[float] = None) -> float:
        """
        [FIXED] Menghitung likuiditas yang bisa diambil SEKETIKA (Taker Liquidity).
        """
        # A. Base Liquidity Estimation (Top of Book)
        # Asumsi: Hanya sebagian kecil dari volume tick yang tersedia di harga terbaik
        approx_qty = self.volume * self.liquidity_ratio
        
        # B. Limit Order Constraint Check (The Filter)
        if price_limit is not None:
            if side == OrderSide.BUY:
                # Mau BELI. Lawan transaksi adalah PENJUAL (Ask).
                # Jika Limit Price SAYA < Harga Jual TERMURAH (Best Ask) -> Tidak Match.
                best_ask = self.best_ask()
                if price_limit < best_ask:
                    return 0.0 
                    
            elif side == OrderSide.SELL:
                # Mau JUAL. Lawan transaksi adalah PEMBELI (Bid).
                # Jika Limit Price SAYA > Harga Beli TERTINGGI (Best Bid) -> Tidak Match.
                best_bid = self.best_bid()
                if price_limit > best_bid:
                    return 0.0

        return approx_qty

    # -------------------------------------------------------------
    # 2. ORDER BOOK WALKING (L2 SIMULATION)
    # -------------------------------------------------------------
    
    def simulate_price_impact(self, side: OrderSide, quantity: float) -> float:
        """
        Menghitung harga rata-rata eksekusi dengan 'memakan' likuiditas L2.
        Fallback ke Mid Price jika L2 data kosong.
        """
        if quantity <= 0: return self.mid_price()
        
        # BUY memakan ASK, SELL memakan BID
        depth = self.ask_depth if side == OrderSide.BUY else self.bid_depth
        
        # Fallback ke Mid Price jika data L2 kosong
        if not depth:
            return self.mid_price()
        
        # Sorting Order Book
        # ASK: Murah ke Mahal (Ascending) | BID: Mahal ke Murah (Descending)
        is_buy = (side == OrderSide.BUY)
        prices = sorted(depth.keys(), reverse=not is_buy)
        
        remaining = quantity
        total_cost = 0.0
        filled_total = 0.0
        
        for p in prices:
            qty_at_level = depth[p]
            fill = min(remaining, qty_at_level)
            
            total_cost += (p * fill)
            remaining -= fill
            filled_total += fill
            
            if remaining <= 0:
                break
                
        # Calculate VWAP (Volume Weighted Average Price)
        if filled_total > 0:
            return total_cost / filled_total
            
        return self.mid_price()


# ====================== EXCHANGE EMULATOR ======================

class ExchangeEmulator:
    """
    Industrial-grade exchange emulator dengan:
    - Adaptive market regime detection
    - Circuit breaker protection
    - Smart order routing simulation
    - Partial fill support
    - Market impact modeling
    """
    
    def __init__(self, mechanics: MechanicsSuite):
        self.mechanics = mechanics
        self.logger = get_logger("emulator")
        
        # Components
        self.regime_detector = MarketRegimeDetector() 
        self.circuit_breaker = CircuitBreaker()
        
        # State
        self.orders: Dict[str, Order] = {}          
        self.order_history: Dict[str, Order] = {}   
        
        self.trades: List[Trade] = []               # Flat list untuk reporting cepat
        self.trade_history: Dict[str, Trade] = {}   # Dict map by ID untuk lookup
        
        # [FIX] Gunakan defaultdict untuk menampung history penolakan per Order ID
        self.rejection_history: Dict[str, List[Rejection]] = defaultdict(list)
        self.rejections: List[Rejection] = []       # Flat list untuk reporting cepat
        
        # Metrics
        self.metrics = {
            'total_orders': 0,
            'total_trades': 0,
            'total_rejections': 0,
            'total_volume': 0.0,
            'total_fees': 0.0,
            'avg_latency_ms': 0.0,
            'avg_slippage_bps': 0.0,
        }

        # Adaptive Params ... (lanjutan kode sebelumnya)        
        # Adaptive parameters berdasarkan regime
        self.adaptive_params = {
            MarketRegime.NORMAL: {
                'latency_multiplier': 1.0,
                'slippage_multiplier': 1.0,
                'fill_probability': 1.0
            },
            MarketRegime.VOLATILE: {
                'latency_multiplier': 1.5,   # Lag naik 50%
                'slippage_multiplier': 2.0,  # Slippage 2x
                'fill_probability': 0.9      # 10% reject chance
            },
            MarketRegime.CRISIS: {
                'latency_multiplier': 5.0,   # Lag parah
                'slippage_multiplier': 5.0,  # Slippage menggila
                'fill_probability': 0.5      # 50% order gagal (Ghost Liquidity)
            },
            MarketRegime.ILLIQUID: {
                'latency_multiplier': 1.2,
                'slippage_multiplier': 3.0,
                'fill_probability': 0.7
            },
            MarketRegime.MANIPULATED: {
                'latency_multiplier': 2.0,
                'slippage_multiplier': 4.0,
                'fill_probability': 0.8
            }
        }    
    
# ====================== PUBLIC API (DISPATCHER) ======================
    
    def process_order(
        self, 
        order: Order, 
        context: MarketContext
    ) -> Result[Trade, Rejection]:
        """
        EKSEKUSI UTAMA (THE KILL ZONE)
        Alur: Circuit Breaker -> Regime Detect -> Precheck -> Dispatch -> Metrics
        """
        start_time = time.time()

        # 1. GATEKEEPING (Circuit Breaker)
        if not self.circuit_breaker.check_order_rate():
            rejection = Rejection.create(
                order_id=order.order_id,
                code=RejectionCode.RATE_LIMIT,
                reason=f"Circuit Breaker Triggered: {self.circuit_breaker._trip_reason}",
                retryable=True,
                retry_after_ms=5000
            )
            self.circuit_breaker.record_rejection()
            self._update_metrics(order, rejection, 0.0)
            return Err(rejection)

        self.circuit_breaker.record_order()

        # 2. SENSING (Regime Detection)
        self.regime_detector.update(
            price=context.price,
            volume=context.volume,
            bid_ask_spread=context.spread
        )
        
        # 3. PRECHECK (Validasi Dasar)
        if rejection_err := self._precheck_order(order, context):
            self._update_metrics(order, rejection_err, time.time() - start_time)
            return Err(rejection_err)

        # 4. DISPATCHER (Arahkan ke Handler yang Tepat)
        try:
            if order.type == OrderType.MARKET:
                # Market order return Result[Trade, Rejection]
                result = self._process_market_order(order, context)
            
            elif order.type == OrderType.LIMIT:
                # Limit order return Result[Trade, Rejection]
                result = self._process_limit_order(order, context)
                
            else:
                # Order type lain (STOP, etc)
                result = self._process_conditional_order(order, context)

            # 5. METRICS & RETURN
            duration = time.time() - start_time
            if result.is_ok():
                self._update_metrics(order, result.unwrap(), duration)
            else:
                self._update_metrics(order, result.unwrap_err(), duration)
                
            return result

        except Exception as e:
            # Safety Net untuk crash tak terduga
            self.logger.error(f"Emulator Crash: {str(e)}")
            rejection = Rejection.create(
                order_id=order.order_id,
                code=RejectionCode.UNKNOWN,
                reason=f"Emulator System Error: {str(e)}"
            )
            return Err(rejection)

        # ====================== ORDER TYPE HANDLERS ======================
    
# ====================== ORDER TYPE HANDLERS ======================
    
    def _process_market_order(
        self, 
        order: Order, 
        context: MarketContext
    ) -> Result[Trade, Rejection]:
        """Process market order dengan logic chaotic"""
        
        # [FIX] Property access tanpa ()
        regime = self.regime_detector.current_regime 
        params = self.adaptive_params[regime]
        
        # Chaos Injection: Perbesar volatilitas jika krisis
        adjusted_volatility = context.volatility
        if regime == MarketRegime.CRISIS:
            adjusted_volatility *= 3.0
        
        # A. Latency Calculation
        base_latency = self.mechanics.latency.calculate_delay_ms(adjusted_volatility)
        # [FIX] Keep float for precision
        latency_ms = float(base_latency * params['latency_multiplier']) 
        
        # B. Price Impact & Slippage
        # Jika order besar (>10% volume), gunakan simulasi impact orderbook
        is_whale = order.quantity > (context.volume * 0.1)
        
        if is_whale:
            executed_price = context.simulate_price_impact(
                side=order.side,
                quantity=order.quantity
            )
        else:
            # Retail order: Gunakan model statistik slippage standard
            base_price = self.mechanics.slippage.calculate_execution_price(
                order=order,
                market_price=context.mid_price(),
                volatility=adjusted_volatility,
                volume=context.volume
            )
            # Apply additional regime penalty
            price_diff = base_price - context.mid_price()
            executed_price = context.mid_price() + (price_diff * params['slippage_multiplier'])
        
        # C. Fee Calculation
        fee = self.mechanics.fee.calculate_fee(
            quantity=order.quantity,
            price=executed_price,
            is_maker=False, # Market order is always Taker
            symbol=order.symbol
        )
        
        # D. Construction
        trade_result = Trade.create(
            order_id=order.order_id,
            symbol=order.symbol,
            side=order.side,
            quantity=order.quantity,
            price=executed_price,
            fee=fee,
            fee_currency="USDT",
            latency_ms=latency_ms,
            market_price_snapshot=context.price,
            is_maker=False,
            metadata={
                'regime': regime.value,
                'model': 'whale_impact' if is_whale else 'retail_slippage'
            }
        )
        
        # Return sebagai Result
        return trade_result # Trade.create sudah mengembalikan Result[Trade, str] sebenarnya, tapi kita assume Ok
    
    # ====================== INTERNAL HELPERS ======================

    def _precheck_order(self, order: Order, context: MarketContext) -> Optional[Rejection]:
        """Validasi cepat sebelum logic berat"""
        if order.quantity <= 0:
            return Rejection.create(order.order_id, RejectionCode.INVALID_QTY, "Qty <= 0")
        return None
        
    def _process_conditional_order(self, order: Order, context: MarketContext) -> Result[Trade, Rejection]:
        return Err(Rejection.create(order.order_id, RejectionCode.VALIDATION_ERROR, "Conditional Order Not Supported"))

    def _update_metrics(self, order: Order, result: Union[Trade, Rejection], duration: float):
        """Update statistik internal"""
        self.metrics['total_orders'] += 1
        
        if isinstance(result, Trade):
            self.metrics['total_trades'] += 1
            self.metrics['total_volume'] += result.quantity * result.price
            self.metrics['total_fees'] += result.fee
            
            # Running Average
            n = self.metrics['total_trades']
            self.metrics['avg_latency_ms'] = (self.metrics['avg_latency_ms'] * (n-1) + result.latency_ms) / n
            self.metrics['avg_slippage_bps'] = (self.metrics['avg_slippage_bps'] * (n-1) + result.slippage_bps) / n
            
            self.trades.append(result) # Simpan history
            
        elif isinstance(result, Rejection):
            self.metrics['total_rejections'] += 1
            self.rejections.append(result)



    # ====================== LIMIT ORDER LOGIC ======================
    def _process_limit_order(self, order: Order, context: MarketContext) -> Result[Trade, Rejection]:
        """
        Logika Limit Order:
        1. Cek Aggressive (Marketable) -> Fill Instan
        2. Cek Passive -> Reject (di V1 kita belum simpan order book)
        """
        regime = self.regime_detector.current_regime # [FIX] No brackets
        
        # 1. Cek Marketability
        is_marketable = False
        if order.side == OrderSide.BUY:
            if order.price >= context.best_ask(): # Buy Limit >= Ask -> Taker
                is_marketable = True
        elif order.side == OrderSide.SELL:
            if order.price <= context.best_bid(): # Sell Limit <= Bid -> Taker
                is_marketable = True
        
        # 2. Handle Passive (Non-Marketable)
        if not is_marketable:
            return Err(self._handle_non_executable_limit(order, context, regime))

        # 3. Handle Aggressive (Marketable)
        # Simulasi 'Ghost Liquidity' (Order gagal fill walau harga masuk)
        params = self.adaptive_params.get(regime, self.adaptive_params[MarketRegime.NORMAL])
        if random.random() > params['fill_probability']:
            rejection = Rejection.create(
                order_id=order.order_id,
                code=RejectionCode.GHOST_LIQUIDITY,
                reason=f"Ghost Liquidity: Fill failed in {regime.value} regime",
                retryable=True
            )
            return Err(rejection)

        return Ok(self._execute_limit_order(order, context, regime))

    def _handle_non_executable_limit(self, order: Order, context: MarketContext, regime: MarketRegime) -> Rejection:
        """Reject limit order yang tidak match instan"""
        best_price = context.best_ask() if order.side == OrderSide.BUY else context.best_bid()
        return Rejection.create(
            order_id=order.order_id,
            code=RejectionCode.INSUFFICIENT_LIQUIDITY,
            reason=f"Passive Limit Order Rejected (No Orderbook in V1). Limit: {order.price}, BestMkt: {best_price}",
            # [FIX] Rename parameter: market_price -> market_price_snapshot
            market_price_snapshot=context.price, 
            retryable=True
        )

    def _execute_limit_order(self, order: Order, context: MarketContext, regime: MarketRegime) -> Trade:
        """Eksekusi limit order yang marketable"""
        # Price Improvement: Fill di harga Market, bukan harga Limit (jika market lebih baik)
        market_price = context.best_ask() if order.side == OrderSide.BUY else context.best_bid()
        
        # Safety: Jangan fill lebih buruk dari limit
        executed_price = market_price
        if order.side == OrderSide.BUY:
            executed_price = min(executed_price, order.price)
        else:
            executed_price = max(executed_price, order.price)
            
        # Calc Fee & Latency
        params = self.adaptive_params[regime]
        latency_ms = float(self.mechanics.latency.calculate_delay_ms(context.volatility) * params['latency_multiplier'])
        
        fee = self.mechanics.fee.calculate_fee(
            quantity=order.quantity,
            price=executed_price,
            is_maker=False, # Marketable Limit is Taker
            symbol=order.symbol
        )
        
        # Create Trade
        trade_res = Trade.create(
            order_id=order.order_id,
            symbol=order.symbol,
            side=order.side,
            quantity=order.quantity,
            price=executed_price,
            fee=fee,
            latency_ms=latency_ms,
            market_price_snapshot=context.price,
            is_maker=False,
            metadata={'type': 'LIMIT_TAKER'}
        )
        return trade_res.unwrap()

# ====================== DIAGNOSTICS & MONITORING ======================

    def get_stats(self) -> Dict[str, Any]:
        """
        [UPGRADED] Mengambil ringkasan statistik kinerja real-time.
        Output diformat bersih untuk keperluan Dashboard / Log.
        """
        # Hitung Metrics Turunan
        total = self.metrics.get('total_orders', 0)
        filled = self.metrics.get('total_trades', 0)
        rejected = self.metrics.get('total_rejections', 0)
        
        fill_rate = (filled / total * 100.0) if total > 0 else 0.0
        
        return {
            'performance': {
                'fill_rate_pct': round(fill_rate, 2),
                'total_volume': round(self.metrics.get('total_volume', 0.0), 2),
                'total_fees': round(self.metrics.get('total_fees', 0.0), 4),
            },
            'quality_of_service': {
                'avg_latency_ms': round(self.metrics.get('avg_latency_ms', 0.0), 2),
                'avg_slippage_bps': round(self.metrics.get('avg_slippage_bps', 0.0), 2),
            },
            'counters': {
                'orders': total,
                'trades': filled,
                'rejections': rejected,
            }
        }

    def get_diagnostics(self) -> Dict[str, Any]:
        """
        [NEW] Deep Health Check (Inspeksi Organ Dalam).
        Menampilkan status vital: Regime Pasar, Breaker Status, dan Beban Memori.
        """
        # 1. Cek Kesehatan Circuit Breaker
        is_tripped = self.circuit_breaker.is_tripped()
        breaker_status = "HALTED 🛑" if is_tripped else "RUNNING 🟢"
        
        breaker_detail = "OK"
        if is_tripped:
            # Hitung sisa waktu hukuman
            remaining = max(0, self.circuit_breaker._tripped_until - time.time())
            breaker_detail = f"TRIPPED! Reason: {self.circuit_breaker._trip_reason} (Resumes in {remaining:.1f}s)"

        # 2. Cek Beban Memori (Penting untuk simulasi panjang)
        # Menghitung jumlah object yang tersimpan
        memory_stats = {
            'active_orders': len(self.orders) if hasattr(self, 'orders') else 0,
            'history_trades': len(self.trade_history) if hasattr(self, 'trade_history') else 0,
            'history_rejections': sum(len(v) for v in self.rejection_history.values()) if hasattr(self, 'rejection_history') else 0
        }

        return {
            'timestamp': time.time(),
            'system_health': breaker_status,
            'market_environment': self.regime_detector.current_regime.value,
            'circuit_breaker': breaker_detail,
            'memory_load': memory_stats
        }

    def health_check(self) -> bool:
        """
        Simple boolean check.
        Return True jika sistem siap menerima order.
        """
        return not self.circuit_breaker.is_tripped()

# ====================== EXECUTION ENGINE WRAPPER ======================

class ExecutionEngine:
    """
    [NEW] High-Level Interface yang membungkus Emulator.
    Ini adalah objek utama yang akan dipanggil oleh Orchestrator/Backtester.
    """
    def __init__(self, emulator: ExchangeEmulator, config: Optional[Dict[str, Any]] = None):
        self.emulator = emulator
        self.config = config or {}

    def submit_order(self, order: Order, context: MarketContext) -> Result[Trade, Rejection]:
        """Single entry point untuk mengirim order ke pasar"""
        return self.emulator.process_order(order, context)

    def get_trades(self) -> List[Trade]:
        """Retrieve history trades"""
        return self.emulator.trades

    def get_rejections(self) -> List[Rejection]:
        """Retrieve history rejections"""
        return self.emulator.rejections

    def reset(self):
        """Hard reset state simulasi"""
        self.emulator.trades.clear()
        self.emulator.rejections.clear()
        self.emulator.circuit_breaker.reset()

# ====================== EXPORTS ======================

__all__ = [
    'MarketRegime',
    'MarketRegimeDetector',
    'CircuitBreaker',
    'MarketContext',
    'ExchangeEmulator',
]
