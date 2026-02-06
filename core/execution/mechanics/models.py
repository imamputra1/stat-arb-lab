"""
INDUSTRIAL-GRADE MECHANICS MODELS (THE TORTURE TOOLS)
Location: core/execution/mechanics/models.py
Desc: Implementasi konkret dari simulasi friksi pasar.
      Mengandung logika matematika untuk Slippage, Latency, Liquidity, dan Fee.
"""

import random
import math
import time
from typing import Optional, Dict, Any
from datetime import datetime, timezone


from ..types import (
    Order, OrderType, OrderSide, Symbol
)

from .config import (
    # Base Configs
    VolatilitySlippageConfig,
    NetworkCongestionLatencyConfig,
    StandardFeeConfig,
    StochasticLiquidityConfig
)
from .base import (
    BaseSlippageModel,
    BaseLatencyModel,
    BaseLiquidityModel,
    BaseFeeModel
)
# ====================== 1. SLIPPAGE IMPLEMENTATIONS ======================
class VolatilitySlippage(BaseSlippageModel):
    """
    IMPLEMENTASI 1: VOLATILITY SLIPPAGE (THE PRICE CRUSHER)
    Model ini menghancurkan harga eksekusi berdasarkan 3 faktor:
    1. Base Noise: Ketidakpastian pasar wajar.
    2. Volatility Penalty: Saat market crash, spread melebar gila-gilaan.
    3. Microstructure Impact: Hukum Akar Kuadrat (Square Root Law) untuk order besar.
    """

    @property
    def config(self) -> VolatilitySlippageConfig:
        """Type-safe access ke konfigurasi sadis kita"""
        # Casting super().config ke tipe spesifik agar autocomplete jalan
        return super().config

    def calculate_execution_price(
        self, 
        order: Order, 
        market_price: float, 
        volatility: float, 
        volume: float
    ) -> float:
        """
        [PROTOCOL COMPLIANT] Menghitung harga final eksekusi.
        """
        # 0. Safety Check
        if order.quantity <= 0 or market_price <= 0:
            return market_price

        # 1. Hitung "Penderitaan" dalam Basis Points (bps)
        slippage_bps = self._calculate_microstructure_impact(
            quantity=order.quantity,
            volatility=volatility,
            avg_volume=volume
        )

        # 2. Terapkan ke Harga (Jual Murah, Beli Mahal)
        # Convert bps ke decimal (1 bps = 0.0001)
        slippage_factor = slippage_bps / 10000.0
        
        final_price = market_price
        if order.side == OrderSide.BUY:
            # Beli dapet harga lebih mahal (Ask side slippage)
            final_price = market_price * (1.0 + slippage_factor)
            # Log jika slippage parah (> 50 bps)
            if slippage_bps > 50:
                self.logger.info(f"High Slippage BUY: {slippage_bps:.1f} bps | Vol: {volatility:.2%}")
                
        elif order.side == OrderSide.SELL:
            # Jual dapet harga lebih murah (Bid side slippage)
            final_price = market_price * (1.0 - slippage_factor)
            if slippage_bps > 50:
                self.logger.info(f"High Slippage SELL: {slippage_bps:.1f} bps | Vol: {volatility:.2%}")

        return final_price

    def _calculate_microstructure_impact(
        self, 
        quantity: float, 
        volatility: float, 
        avg_volume: float
    ) -> float:
        """
        [THE CORE MATH - ADVANCED] 
        Menghitung impact harga menggunakan model Hybrid (Square Root + Linear).
        """
        cfg = self.config # Sekarang tipe VolatilitySlippageConfig yang lengkap
        
        # A. Base Noise
        noise_bps = random.uniform(0, cfg.base_bps)

        # B. Volatility Penalty (Crisis Simulation)
        vol_penalty_bps = 0.0
        if volatility > cfg.volatility_threshold:
            panic_ratio = volatility / cfg.volatility_threshold
            vol_penalty_bps = cfg.base_bps * (panic_ratio ** cfg.volatility_multiplier)

        # C. Market Impact (Advanced Liquidity Logic)
        liquidity_impact_bps = 0.0
        if avg_volume > 0:
            participation_rate = quantity / avg_volume
            
            # 1. Square Root Component (Standard Almgren-Chriss model)
            sqrt_impact = cfg.square_root_factor * math.sqrt(participation_rate)
            
            # 2. Linear Component (Untuk order 'Paus' yang sangat besar)
            linear_impact = cfg.linear_factor * participation_rate
            
            # Gabungkan impact (scaled ke basis points, misal * 100)
            raw_impact = (sqrt_impact + linear_impact) * 100.0
            
            # 3. Adverse Selection (Market bergerak melawan order kita sebelum fill)
            # Semakin volatile, semakin tinggi kemungkinan adverse selection
            adverse_risk = cfg.adverse_selection_factor * (volatility * 100)
            
            liquidity_impact_bps = raw_impact + adverse_risk

        # D. Total Torture
        total_bps = noise_bps + vol_penalty_bps + liquidity_impact_bps
        
        # Cap di Max Slippage
        return min(total_bps, cfg.max_slippage_bps)

    def __init__(self, config: Optional[VolatilitySlippageConfig] = None):
        """
        Override init untuk setup state 'Adaptive Learning'.
        Config tetap immutable, tapi kita butuh variabel yang bisa berubah (belajar).
        """
        super().__init__(config)
        
        # [STATE] Mutable Weights untuk Adaptive Learning
        # Kita ambil nilai awal dari Config
        self._current_spread_weight = self.config.spread_impact_weight
        self._current_depth_weight = self.config.depth_impact_weight
        
        # [METRICS] Counter untuk learning window
        self._trade_counter = 0

    # ================= 1. PUBLIC CALCULATION API =================

    def calculate_slippage_bps(
        self,
        order: Order,
        market_price: float,
        volatility: float,
        volume: float
    ) -> float:
        """
        Menghitung Total Slippage dalam Basis Points (bps).
        Method ini mengorkestrasi logic microstructure impact + noise.
        """
        # 1. Hitung Microstructure Impact (Math Logic)
        # Menggunakan method yang sudah kita buat sebelumnya
        impact_bps = self._calculate_microstructure_impact(
            quantity=order.quantity,
            volatility=volatility,
            avg_volume=volume
        )
        
        # 2. Tambahkan Base Noise (Randomness Pasar)
        noise_bps = random.uniform(0, self.config.base_bps)
        
        # 3. Total Calculation
        total_bps = impact_bps + noise_bps
        
        # 4. Adaptive Update Trigger
        # Setiap kali kita hitung, kita anggap ini observasi baru untuk learning
        if self.config.enable_adaptive_learning:
            self._update_adaptive_weights()
            
        # 5. Cap Result (Safety)
        return min(total_bps, self.config.max_slippage_bps)

    # ================= 2. RECORDING & TELEMETRY =================

    def _record_slippage_calculation(
        self,
        order: Order,
        market_price: float,
        executed_price: float,
        volatility: float,
        volume: float,
        total_slippage: float
    ) -> None:
        """
        Mencatat telemetri detail untuk analisis pasca-trade.
        Sangat berguna untuk men-debug kenapa order tertentu kena slippage parah.
        """
        # Hitung Impact Cost (Uang yang hilang karena slippage)
        price_diff = abs(executed_price - market_price)
        impact_cost = price_diff * order.quantity
        
        slippage_bps = (price_diff / market_price) * 10000
        
        log_data = {
            "order_id": order.order_id,
            "side": order.side,
            "qty": order.quantity,
            "volatility": f"{volatility:.2%}",
            "market_vol": volume,
            "slippage_bps": f"{slippage_bps:.2f}",
            "impact_cost": f"{impact_cost:.4f} {order.symbol.split('/')[1] if '/' in order.symbol else ''}",
            "adaptive_weights": {
                "spread": f"{self._current_spread_weight:.2f}",
                "depth": f"{self._current_depth_weight:.2f}"
            }
        }
        
        # Log level tergantung keparahan
        if slippage_bps > 50: # > 50 bps itu parah
            self.logger.warning(f"HIGH SLIPPAGE DETECTED: {log_data}")
        else:
            self.logger.debug(f"Slippage Record: {log_data}")

    # ================= 3. ADAPTIVE LEARNING ENGINE =================

    def _update_adaptive_weights(self) -> None:
        """
        Simulasi 'Adaptive Learning'.
        Dalam real market, kita akan adjust weight berdasarkan selisih 
        antara 'Predicted Slippage' vs 'Actual Realized Slippage'.
        
        Di sini, kita simulasikan 'Model Drift' agar simulasi tidak statis.
        """
        self._trade_counter += 1
        
        # Hanya update setelah window tertentu tercapai
        if self._trade_counter % self.config.learning_window != 0:
            return
            
        # [SIMULATION LOGIC]
        # Kita sedikit 'menggoyangkan' bobot untuk mensimulasikan perubahan rezim pasar
        # Decay factor mendekatkan bobot kembali ke nilai default (mean reversion) atau drift.
        
        decay = self.config.decay_factor
        
        # Random walk drift kecil (-5% sampai +5% dari bobot saat ini)
        drift_spread = random.uniform(0.95, 1.05)
        drift_depth = random.uniform(0.95, 1.05)
        
        # Update weights
        old_spread = self._current_spread_weight
        self._current_spread_weight = (old_spread * decay) * drift_spread
        
        old_depth = self._current_depth_weight
        self._current_depth_weight = (old_depth * decay) * drift_depth
        
        self.logger.info(
            f"Adaptive Learning Update [Window {self.config.learning_window}]: "
            f"Spread Weight {old_spread:.3f}->{self._current_spread_weight:.3f}"
        )


# ====================== 2. LATENCY IMPLEMENTATIONS ======================
class NetworkCongestionLatency(BaseLatencyModel):
    """
    Model Latency yang mensimulasikan kemacetan jaringan.
    Delay meningkat secara eksponensial saat volatilitas tinggi.
    Mendukung packet loss, retransmission, dan network topology.
    """
    
    def __init__(self, config: Optional[NetworkCongestionLatencyConfig] = None):
        super().__init__(config or NetworkCongestionLatencyConfig())
        self._network_state = {
            "congestion_level": 0.0,
            "packet_loss_count": 0,
            "last_congestion_time": datetime.now(timezone.utc)
        }
    
    def calculate_delay_ms(self, volatility: float) -> int:
        """
        Menghitung delay eksekusi dalam milliseconds.
        
        Args:
            volatility: Volatilitas pasar (0-1 atau dalam standar deviasi)
        
        Returns:
            int: Delay dalam milliseconds (rounded)
        """
        config = self.get_config()
        
        # Jika config fixed, langsung return
        if config.model_type == "fixed":
            return int(config.fixed_delay_ms)
        
        # Base latency
        base_ms = config.base_latency_ms
        
        # Network congestion factor
        # Volatility > threshold dianggap network congestion
        congestion_factor = 1.0
        if volatility > config.congestion_threshold:
            excess_vol = (volatility - config.congestion_threshold) / config.congestion_threshold
            congestion_factor = 1.0 + config.congestion_multiplier * excess_vol
        
        # Network topology effect (simulasi routing melalui node)
        topology_delay = self._calculate_topology_delay()
        
        # Packet loss simulation
        packet_loss_delay = self._simulate_packet_loss()
        
        # Clock drift
        clock_drift = random.gauss(0, config.clock_drift_std_ms)
        
        # Calculate total delay
        total_delay = (
            base_ms * congestion_factor +
            topology_delay +
            packet_loss_delay +
            clock_drift
        )
        
        # Apply jitter jika enabled
        if config.enable_jitter:
            jitter_range = total_delay * config.jitter_percentage
            jitter = random.uniform(-jitter_range, jitter_range)
            jitter = max(min(jitter, config.max_jitter_ms), -config.max_jitter_ms)
            jitter = max(jitter, config.min_jitter_ms if jitter > 0 else -config.min_jitter_ms)
            total_delay += jitter
        
        # Apply time-of-day multiplier
        current_hour = datetime.now(timezone.utc).hour
        time_multiplier = config.get_time_of_day_multiplier(current_hour)
        total_delay *= time_multiplier
        
        # Ensure within bounds dan bulatkan ke integer
        total_delay = max(0.0, min(total_delay, config.max_latency_ms))
        total_delay_int = int(round(total_delay))
        
        # Update network state
        self._update_network_state(volatility, total_delay_int)
        
        # Log jika delay tinggi
        if total_delay_int > 100:  # > 100ms dianggap tinggi
            self.logger.warning(f"High network latency detected: {total_delay_int}ms "
                              f"(volatility: {volatility:.4f})")
        
        return max(5, total_delay_int)  # Minimum 5ms (fisika jaringan)
    
    def _calculate_topology_delay(self) -> float:
        """Calculate delay berdasarkan network topology"""
        config = self.get_config()
        
        if not config.network_nodes or len(config.network_nodes) < 2:
            return 0.0
        
        # Simulasi routing melalui beberapa node
        total_path_delay = 0.0
        path = random.sample(config.network_nodes, min(3, len(config.network_nodes)))
        
        for i in range(len(path) - 1):
            node1 = path[i]
            node2 = path[i + 1]
            
            # Get latency antara node
            key1 = f"{node1}_{node2}"
            key2 = f"{node2}_{node1}"
            
            if key1 in config.node_latencies:
                total_path_delay += config.node_latencies[key1]
            elif key2 in config.node_latencies:
                total_path_delay += config.node_latencies[key2]
            else:
                # Default latency
                total_path_delay += 50.0  # 50ms default
        
        return total_path_delay
    
    def _simulate_packet_loss(self) -> float:
        """Simulate packet loss dan retransmission delay"""
        config = self.get_config()
        
        if random.random() < config.packet_loss_rate:
            self._network_state["packet_loss_count"] += 1
            self.logger.debug(f"Packet loss simulated, count: {self._network_state['packet_loss_count']}")
            return config.retransmission_delay_ms
        
        return 0.0


    @property
    def config(self) -> NetworkCongestionLatencyConfig:
        """
        [REPLACES get_config] 
        Menggunakan @property agar 'self.config' otomatis ter-cast 
        ke tipe NetworkCongestionLatencyConfig yang lengkap.
        """
        # Casting super().config ke tipe spesifik
        return super().config

    # ================= 2. NETWORK TELEMETRY =================

    def _update_network_state(self, volatility: float, delay_ms: float) -> None:
        """
        Memantau kesehatan jaringan simulasi.
        Mencatat log jika terjadi anomali (Lag Spike / Congestion).
        """
        cfg = self.config
        
        # 1. Deteksi Anomali: Apakah delay melebihi batas wajar?
        # Batas wajar = Base + (Jitter * 3)
        # Jika lebih dari ini, berarti ada Congestion atau Packet Loss
        threshold_ms = cfg.base_latency_ms + (cfg.base_jitter_ms * 3)
        
        is_lag_spike = delay_ms > threshold_ms
        is_packet_loss = delay_ms >= (cfg.retry_penalty_ms + cfg.base_latency_ms)
        
        # 2. Log Level Logic
        # Kita tidak mau log info setiap milidetik (spam), 
        # tapi kita WAJIB tahu kalau jaringan 'putus' (Packet Loss).
        
        if is_packet_loss:
            self.logger.warning(
                f"CRITICAL NETWORK EVENT: Packet Loss Detected! "
                f"Delay: {delay_ms}ms | Volatility: {volatility:.2%}"
            )
        elif is_lag_spike:
            # Log warning jika spike signifikan (> 2x base)
            if delay_ms > (cfg.base_latency_ms * 2):
                self.logger.info(
                    f"Network Congestion: High Latency {delay_ms}ms "
                    f"(Normal: ~{cfg.base_latency_ms}ms)"
                )
                
        # 3. Future Implementation Note:
        # Di sini kita bisa menyimpan metric ke 'self._latency_history' 
        # jika ingin membuat visualisasi ping chart nanti.




# ====================== 3. LIQUIDITY IMPLEMENTATIONS ======================
class StochasticLiquidity(BaseLiquidityModel):
    """
    IMPLEMENTASI 3: STOCHASTIC LIQUIDITY (THE GHOST)
    Mensimulasikan realitas pahit order limit:
    1. Ghosting: Likuiditas yang hilang saat didekati.
    2. Queue Priority: Kita selalu di belakang bot HFT.
    3. Impact Rejection: Order terlalu besar tidak bisa fill instan.
    """

    # ================= 0. INIT & STATE =================

    def __init__(self, config: Optional[StochasticLiquidityConfig] = None):
        """
        Init state untuk melacak 'Ghost Mode'.
        Kita butuh memori (state) untuk tahu simbol mana yang sedang 'dikutuk'.
        """
        super().__init__(config)

        self._ghost_liquidity_tracker: Dict[str, float] = {} 
        # [METRICS] Cache sederhana untuk debugging
        self._last_rejection_reason: Dict[str, str] = {}
        self._liquidity_cache: Dict[str, Any] = {}

    @property
    def config(self) -> StochasticLiquidityConfig:
        """Type-safe access ke konfigurasi sadis Liquidity"""
        return super().config

    # ================= 1. THE EXECUTION LOGIC =================

    def should_fill(
        self, 
        order: Order, 
        tick_volume: float, 
        bid_ask_spread: float = 0.0
    ) -> bool:
        """
        [PROTOCOL COMPLIANT] Menentukan nasib Limit Order.
        Returns: True (Filled) atau False (Rejected/Pending).
        """
        # 1. Master Switch Check
        if not self.config.enabled:
            return True

        cfg = self.config
        now = time.time()
        symbol = order.symbol

        # ---------------------------------------------------------
        # A. GHOST MODE CHECK (The Curse)
        # Apakah simbol ini sedang dalam fase 'Ghosting'? (Fake Liquidity)
        # ---------------------------------------------------------
        if symbol in self._ghost_liquidity_tracker:
            expiry = self._ghost_liquidity_tracker[symbol]
            if now < expiry:
                # Masih dalam mode hantu -> REJECT
                # Simulasi: Harga kena, tapi order book ditarik mundur
                self._last_rejection_reason[order.order_id] = "GHOST_MODE_ACTIVE"
                return False 
            else:
                # Sudah expired, hapus dari tracker
                del self._ghost_liquidity_tracker[symbol]

        # ---------------------------------------------------------
        # B. NEW GHOST TRIGGER (Random Event)
        # Ada peluang kecil likuiditas tiba-tiba hilang saat kita mau masuk
        # ---------------------------------------------------------
        # Roll the dice... (misal 5% chance)
        if random.random() < cfg.ghost_probability:
            # AKTIFKAN GHOST MODE!
            duration_sec = cfg.ghost_duration_ms / 1000.0
            self._ghost_liquidity_tracker[symbol] = now + duration_sec
            
            self.logger.info(f"👻 Ghost Liquidity Triggered on {symbol} for {cfg.ghost_duration_ms}ms")
            self._last_rejection_reason[order.order_id] = "GHOST_MODE_TRIGGERED"
            return False

        # ---------------------------------------------------------
        # C. VOLUME CRUNCH (Size Penalty)
        # Jika order kita 10 BTC tapi volume tick cuma 20 BTC, susah fill penuh.
        # ---------------------------------------------------------
        volume_penalty = 0.0
        
        # Safety check div by zero
        if tick_volume <= 0:
            self._last_rejection_reason[order.order_id] = "ZERO_LIQUIDITY"
            return False
            
        participation_rate = order.quantity / tick_volume
        
        if participation_rate > cfg.volume_participation_threshold:
            # Hitung seberapa 'rakus' order kita
            excess = participation_rate - cfg.volume_participation_threshold
            # Penalty sadis: Excess * Weight
            # Contoh: Excess 10% * Weight 2.0 = 20% penalty chance
            volume_penalty = excess * cfg.volume_penalty_weight

        # ---------------------------------------------------------
        # D. QUEUE POSITION (HFT Bias)
        # Simulasi kita ditaruh di antrean belakang.
        # Queue Bias 0.8 artinya hanya 20% likuiditas tersedia untuk kita.
        # ---------------------------------------------------------
        # 1.0 (Full Available) - 0.8 (Bias) = 0.2 (Chance to Front Run)
        queue_factor = 1.0 - cfg.queue_position_bias

        # ---------------------------------------------------------
        # E. SPREAD PENALTY (Market Quality)
        # Spread lebar = Market tidak efisien = Fill susah
        # ---------------------------------------------------------
        spread_factor = 1.0
        if bid_ask_spread > cfg.max_spread_threshold:
            spread_factor = 0.5 # Diskon probabilitas 50% jika spread lebar

        # ---------------------------------------------------------
        # F. FINAL PROBABILITY CALCULATION
        # ---------------------------------------------------------
        # Rumus Probabilitas:
        # Base * (1 - VolPenalty) * QueueFactor * SpreadFactor
        # ... (Kode should_fill sebelumnya) ...
        
        # [BARU] Ambil Time Factor
        time_factor = self._get_time_based_liquidity_factor()

        # F. FINAL PROBABILITY CALCULATION
        # Tambahkan time_factor ke rumus
        final_prob = (
            cfg.fill_probability 
            * (1.0 - min(volume_penalty, 0.9)) 
            * queue_factor 
            * spread_factor
            * time_factor # <--- INSERT DISINI
        )
        
        # Cap probabilitas max 1.0 (tidak mungkin > 100%)
        final_prob = min(1.0, final_prob)
        
        # ... (Logika Dice Roll & Cache Update) ...
        
        # [BARU] Panggil _update_liquidity_cache sebelum return
        self._update_liquidity_cache(symbol, {
            "order_id": order.order_id,
            "final_prob": final_prob,
            "queue_factor": queue_factor,
            "volume_penalty": volume_penalty,
            "time_factor": time_factor,
            "spread_factor": spread_factor
        })
        
        is_filled = random.random() < final_prob
        # ... return is_filled
        

        if not is_filled:
            # Log alasan kenapa ditolak (selain Ghost Mode)
            reason = f"PROB_FAIL (Prob: {final_prob:.2%}, VolPart: {participation_rate:.1%})"
            self._last_rejection_reason[order.order_id] = reason
            
        return is_filled

    
    def _is_ghost_liquidity_active(self, symbol: Symbol) -> bool:
        """Check jika ghost liquidity aktif untuk symbol"""
        config = self.get_config()
        
        if symbol not in self._ghost_liquidity_tracker:
            return False
        
        expiry_time = self._ghost_liquidity_tracker[symbol]
        current_time = time.time() * 1000  # Current time in milliseconds
        
        if current_time > expiry_time:
            # Ghost liquidity expired
            del self._ghost_liquidity_tracker[symbol]
            return False
        
        return True
    
    def _activate_ghost_liquidity(self, symbol: Symbol) -> None:
        """Activate ghost liquidity untuk symbol"""
        config = self.get_config()
        
        current_time = time.time() * 1000  # Current time in milliseconds
        expiry_time = current_time + config.ghost_duration_ms
        
        self._ghost_liquidity_tracker[symbol] = expiry_time
        self.logger.debug(f"Ghost liquidity activated for {symbol}, expires in {config.ghost_duration_ms}ms")
    
    def _calculate_queue_position_probability(self, order: Order) -> float:
        """Calculate probability based on orderbook queue position"""
        config = self.get_config()
        
        # Simulasi antrean orderbook
        # Hidden orders mengurangi visibility
        hidden_order_effect = 1.0
        if random.random() < config.hidden_order_probability:
            # Hidden order di depan kita
            hidden_order_effect = 0.7
        
        # Orderbook refresh rate effect
        # Faster refresh = lebih banyak competition
        refresh_effect = 1.0 / config.orderbook_refresh_rate_hz if config.orderbook_refresh_rate_hz > 0 else 1.0
        
        # Combine effects
        queue_prob = hidden_order_effect * refresh_effect
        
        # Untuk market orders, queue position tidak relevan
        if order.order_type == OrderType.MARKET:
            queue_prob = 1.0
        
        return max(0.1, min(1.0, queue_prob))  # Cap between 0.1 and 1.0


# ================= 3. TIME & CACHE UTILITIES =================

    def _get_time_based_liquidity_factor(self) -> float: 
        """
        Menghitung multiplier likuiditas berdasarkan jam trading (UTC).
        Market Crypto 24/7, tapi likuiditas tetap mengikuti jam kerja bank global.
        
        Returns:
            Float multiplier (e.g., 1.2 = High Liq, 0.8 = Low Liq)
        """
        # Gunakan UTC agar konsisten di server manapun
        now = datetime.now(timezone.utc)
        hour = now.hour
        
        base_factor = 1.0
        
        # [LOGIC] Global Session Overlaps
        if 13 <= hour <= 16:
            # London + New York Overlap (The Golden Hours)
            # Likuiditas tumpah ruah -> Fill Probability NAIK
            base_factor = 1.2
            
        elif 0 <= hour <= 2:
            # Tokyo Open (Asian Session start)
            # Likuiditas moderat
            base_factor = 1.1
            
        elif 21 <= hour <= 23:
            # The "Graveyard" Shift (US Close, Asia belum buka)
            # Likuiditas kering -> Spread lebar, fill susah
            base_factor = 0.8
            
        # [NOISE] Market tidak robotik
        # Tambahkan variasi random +/- 10%
        random_jitter = random.uniform(0.9, 1.1)
        
        return base_factor * random_jitter

    def _update_liquidity_cache(self, symbol: Symbol, metrics: Dict[str, Any]) -> None:
        """
        Menyimpan snapshot metrik likuiditas terakhir.
        Berguna untuk debugging: "Kenapa order gue ditolak tadi?"
        """
        # Lazy init jika belum ada (safety)
        if not hasattr(self, '_liquidity_cache'):
            self._liquidity_cache: Dict[str, Any] = {}
            
        # Tambahkan timestamp metadata
        metrics['updated_at'] = time.time()
        metrics['is_ghost_active'] = symbol in self._ghost_liquidity_tracker
        
        # Simpan ke memori (dictionary replace)
        self._liquidity_cache[symbol] = metrics
        
        # Log jika ada event menarik (misal probabilitas fill sangat rendah)
        if metrics.get('final_prob', 1.0) < 0.1:
            self.logger.debug(f"Low Liquidity Snapshot [{symbol}]: {metrics}")




# ====================== 4. FEE IMPLEMENTATIONS ======================
# Pastikan import StandardFeeConfig sudah ada di atas
class StandardFee(BaseFeeModel):
    """
    IMPLEMENTASI 4: STANDARD FEE MODEL
    Menghitung biaya transaksi dengan simulasi struktur Exchange nyata:
    1. Maker vs Taker Rates (Maker biasanya lebih murah).
    2. Token Discount (misal: Bayar pakai BNB diskon 25%).
    3. Tiered Fee Simulation (Opsional).
    """

    # ================= 0. INIT & STATE =================

    def __init__(self, config: Optional[StandardFeeConfig] = None):
        """
        Init StandardFee dengan konfigurasi tarif & diskon.
        """
        super().__init__(config)
        self._negotiated_rates: Dict[str, Dict[str, float]] = {}
        self._vip_overrides: Dict[str, str] = {}

    @property
    def config(self) -> StandardFeeConfig:
        """Type-safe access ke konfigurasi Fee"""
        return super().config

    # ================= 1. CALCULATION LOGIC =================
    def calculate_fee(
        self, 
        quantity: float, 
        price: float, 
        is_maker: bool,
        symbol: str # [WAJIB ADA] Sesuai Protocol
    ) -> float:
        """
        [PROTOCOL COMPLIANT] Menghitung nominal fee.
        
        Logic:
        Fee = (Qty * Price) * Rate * DiscountFactor
        """
        cfg = self.config
        
        # 1. Hitung Notional Value (Nilai transaksi dalam Quote Currency)
        notional_value = quantity * price
        
        if notional_value <= 0:
            return 0.0

        # 2. Tentukan Base Rate (Maker vs Taker)
        # Maker: Menambah likuiditas (Limit Order di buku)
        # Taker: Mengambil likuiditas (Market Order / Limit yang langsung fill)
        raw_rate = cfg.maker_fee_rate if is_maker else cfg.taker_fee_rate

        # 3. Tiered Fee Logic (Simulasi)
        # Jika user 'VIP', rate bisa lebih rendah.
        # Karena kita belum punya state 'Monthly Volume', kita skip logic detailnya.
        # Namun, jika enable_tiered_fees aktif, kita bisa kasih diskon random kecil
        # untuk simulasi variasi akun.
        effective_rate = raw_rate
        
        # 4. Token Discount (e.g. BNB Deduction)
        # Jika user bayar pakai token native exchange, dapat diskon (misal 25%)
        if cfg.enable_token_discount:
            discount_multiplier = 1.0 - cfg.token_discount_rate
            effective_rate *= discount_multiplier
            
        # 5. Hitung Final Fee
        fee_nominal = notional_value * effective_rate
        
        # 6. Apply Minimum Fee (Safety)
        # Exchange sering punya aturan fee minimal (misal karena presisi decimal)
        if fee_nominal < cfg.min_fee_nominal:
            fee_nominal = cfg.min_fee_nominal

        return self.calculate_fee_for_symbol(
            quantity=quantity,
            price=price,
            is_maker=is_maker,
            symbol=symbol,
            client_id="default" # Asumsi user biasa
        )

    # ================= 2. ADVANCED FEE LOGIC (CLIENT AWARE) =================

    def calculate_fee_for_symbol(
        self,
        quantity: float,
        price: float,
        is_maker: bool,
        symbol: Symbol,
        client_id: str = "default"
    ) -> float:
        """
        Menghitung fee dengan memperhitungkan profil Client (VIP, Market Maker).
        Method ini lebih canggih dari calculate_fee standar.
        """
        cfg = self.config
        notional_value = quantity * price
        
        if notional_value <= 0:
            return 0.0

        # 1. Cek Negotiated Rate (Override untuk Market Maker / Institutional)
        # Jika client punya deal khusus, abaikan fee standar.
        negotiated_rate = self._get_negotiated_fee(client_id, is_maker)
        
        if negotiated_rate is not None:
            # Rate bisa negatif (Rebate)!
            effective_rate = negotiated_rate
        else:
            # 2. Ambil Base Rate (Standard User)
            base_rate = cfg.maker_fee_rate if is_maker else cfg.taker_fee_rate
            
            # 3. Apply VIP Tiers (Jika diaktifkan di config)
            vip_discount = 0.0
            if cfg.enable_tiered_fees:
                vip_level = self._get_vip_level(client_id)
                # Simulasi diskon berdasarkan level (Logic sederhana)
                # VIP 0: 0%, VIP 1: 10%, ... VIP 9: 90%
                level_int = int(vip_level.replace("VIP", ""))
                vip_discount = min(0.9, level_int * 0.1) 
            
            # Rate setelah diskon VIP
            effective_rate = base_rate * (1.0 - vip_discount)

        # 4. Token Discount (BNB Deduction simulation)
        if cfg.enable_token_discount:
             effective_rate *= (1.0 - cfg.token_discount_rate)

        # 5. Final Calculation
        fee_nominal = notional_value * effective_rate
        
        # Apply Minimum Fee (hanya jika fee positif)
        if fee_nominal > 0:
            fee_nominal = max(fee_nominal, cfg.min_fee_nominal)
            
        return fee_nominal

    def _get_vip_level(self, client_id: str) -> str:
        """
        Simulasi VIP Level berdasarkan Client ID.
        Menggunakan Hashing agar deterministik (ID sama selalu dapat level sama).
        """
        if client_id == "default":
            return "VIP0"
            
        # [SIMULATION] Mengubah string ID menjadi angka 0-9
        # Kita pakai simple hash mechanism
        hash_val = sum(ord(c) for c in client_id)
        
        # Probabilitas: Kebanyakan user adalah VIP0 (Regular)
        # Kita buat distribusi miring agar VIP tinggi itu langka
        mod_val = hash_val % 100 # 0-99
        
        if mod_val < 50: return "VIP0"   # 50% user Regular
        elif mod_val < 70: return "VIP1" # 20% user VIP1
        elif mod_val < 85: return "VIP2" # 15% user VIP2
        elif mod_val < 95: return "VIP3" # 10% user VIP3
        else: return "VIP9"              # 5% user Whales (VIP9)

    def _get_negotiated_fee(self, client_id: str, is_maker: bool) -> Optional[float]:
        """
        Cek apakah client memiliki 'Special Deal' (seperti Market Maker).
        Returns: Float rate atau None.
        """
        # [SIMULATION] Deteksi flag khusus di nama ID
        # Misal ID diawali "MM_" (Market Maker)
        if client_id.startswith("MM_"):
            # Market Maker Deal:
            # Maker: -0.0001 (Rebate/Dibayar 1 bps)
            # Taker: 0.0002 (Sangat murah)
            if is_maker:
                return -0.0001 
            else:
                return 0.0002
                
        # [SIMULATION] Institutional Client
        if client_id.startswith("INST_"):
            return 0.0005 # Flat rate murah
            
        return None

# ================= 3. DYNAMIC MANAGEMENT API =================

    def negotiate_fee(
        self,
        client_id: str,
        maker_fee_bps: Optional[float] = None,
        taker_fee_bps: Optional[float] = None
    ) -> bool:
        """
        Menetapkan fee khusus untuk client tertentu secara runtime.
        Input dalam BPS (Basis Points). 1 bps = 0.01% = 0.0001.
        """
        if not client_id:
            return False
            
        # Init struct jika belum ada
        if client_id not in self._negotiated_rates:
            self._negotiated_rates[client_id] = {}
            
        # Update Maker Rate
        if maker_fee_bps is not None:
            # Convert BPS to Rate (e.g. -1.0 bps -> -0.0001)
            self._negotiated_rates[client_id]['maker'] = maker_fee_bps / 10000.0
            
        # Update Taker Rate
        if taker_fee_bps is not None:
            self._negotiated_rates[client_id]['taker'] = taker_fee_bps / 10000.0
            
        self.logger.info(f"Fee Negotiated for {client_id}: {self._negotiated_rates[client_id]}")
        return True

    def promote_to_vip(self, client_id: str, vip_level: str) -> bool:
        """
        Manual override untuk menaikkan level VIP client seketika.
        Format vip_level: "VIP0" sampai "VIP9".
        """
        # Validasi Format (VIP + Digit)
        if not vip_level.startswith("VIP") or not vip_level[-1].isdigit():
            self.logger.error(f"Invalid VIP format: {vip_level}")
            return False
            
        self._vip_overrides[client_id] = vip_level
        self.logger.info(f"Client {client_id} promoted to {vip_level}")
        return True

    def get_config(self) -> StandardFeeConfig:
        """
        [HELPER] Mengembalikan config dengan tipe yang benar.
        Berguna jika caller butuh akses ke parameter raw config.
        """
        # Menggunakan property self.config yang sudah kita define sebelumnya
        return self.config


__all__ = [
    'VolatilitySlippage',
    'NetworkCongestionLatency',
    'StochasticLiquidity',
    'StandardFee'
]
