"""
THE MOTHERBOARD: KALMAN MEAN REVERSION STRATEGY
Location: core/signals/strategies/kalman_mr.py
Role: Menghubungkan Data (HDD/IoT) ke Math Kernel (GPU) dengan MarketObservation.
      [UPGRADE] Robust untuk MTF: warmup panjang, anti-crash, auto-reset saat disconnect.
"""

import numpy as np
import pandas as pd
from typing import Optional, Tuple, Dict, Any, List, Union
from dataclasses import dataclass, field
import logging
import time
# IMPORT CHIP GPU (MATH KERNEL)
from core.math import (
    AdaptiveKalmanFilter, 
    KalmanConfig, 
    KalmanFactory,
    KalmanState,
    KalmanError,
    AdaptationMode
)

# IMPORT SYSTEM UTILS
from core.shared.result import Result, Ok, Err, match_result, safe_async

# IMPORT STRATEGY INTERFACE
from core.signals.base_signal import BaseStrategy
from core.signals.types import (
    SignalEvent, 
    SignalType,
    SignalConfig,
    MarketObservation,
)

# SETUP LOGGING
logger = logging.getLogger(__name__)

# ========== INTERNAL STATE MANAGEMENT ==========

@dataclass
class KalmanMRState:
    """Internal state untuk Kalman Mean Reversion"""
    current_estimate: float = 0.0
    current_uncertainty: float = 1.0
    last_zscore: float = 0.0
    position_size: float = 0.0
    total_pnl: float = 0.0
    trade_count: int = 0
    consecutive_errors: int = 0
    observation_count: int = 0
    spread_history: List[float] = field(default_factory=list)  # Buffer untuk vol calculation
    price_pair: Tuple[str, str] = ("", "")  # (asset_y, asset_x)
    last_signal: SignalType = SignalType.NEUTRAL
    last_signal_time: float = 0.0

# ========== MAIN STRATEGY CLASS ==========

class KalmanMeanReversion(BaseStrategy):
    """
    [THE MOTHERBOARD]
    Implementation of Mean Reversion using Adaptive Kalman Filter.
    Connects Silver Lake (Batch) and Live Feed (Stream) to Math Kernel.
    
    [UPGRADE] Robust untuk Medium‑Frequency Trading:
    - Warmup 100+ candle (tidak ada sinyal sebelum buffer cukup)
    - Anti‑NaN / Zero Price → Neutral + metadata, tidak crash
    - Auto‑reset Kalman jika gap > 5 menit (koneksi putus / server mati)
    - Semua error dikonversi ke sinyal netral, tidak pernah raise exception
    """
    
    # --- CONSTANTS FOR ROBUSTNESS (MTF) ---
    WARMUP_REQUIRED = 100          # Minimal 100 candle sebelum menghasilkan sinyal
    MAX_GAP_MS = 300000           # 5 menit: reset filter jika ada gap lebih
    
    def __init__(self,
                 signal_config: SignalConfig,           # [1] Geser ke depan (Wajib ada untuk Identity)
                 math_config: Optional[KalmanConfig] = None): # [2] Jadikan Optional (Support DI)
        """
        Initialize Motherboard dengan StrategyConfig yang terintegrasi.
        Supports Dependency Injection: Math parameters bisa null (akan pakai default).
        
        Args:
            signal_config: Logic parameters (Thresholds, Sizing)
            math_config: Math parameters (R, Q). Optional.
        """
        # [3] Init Base Strategy pakai data dari SignalConfig (Name & Version)
        super().__init__(name=signal_config.name, version=signal_config.version)
        
        self.sig_config = signal_config

        # [4] Handle Math Config: Pakai inputan user ATAU Default aman
        if math_config:
            self.math_config = math_config
        else:
            # Default Safe Values (Industrial Standard)
            # Mencegah crash jika Factory lupa inject math params
            self.math_config = KalmanConfig(
                R=0.1, 
                Q=1e-5, 
                initial_value=0.0,
                adaptation_mode=AdaptationMode.NIS_THRESHOLD
            )

        # [5] State management (TETAP SAMA - JANGAN UBAH)
        self._internal_state = KalmanMRState()
        self._kalman_filter: Optional[AdaptiveKalmanFilter] = None
        self._filter_initialized = False
        
        self._warmup_count = 0
        
        # --- [UPGRADE] Robustness attributes ---
        self._last_processed_timestamp = 0   # Untuk deteksi gap
        self._warmup_required = self.WARMUP_REQUIRED  # Bisa dioverride nanti
        
        # [6] Validasi konfigurasi (TETAP SAMA)
        validation = self.sig_config.validate()
        if validation.is_err():
            raise ValueError(f"Invalid signal config: {validation.unwrap_err()}")
        
        logger.info(f"Initialized KalmanMRStrategy: {self.name} v{self.version} | Warmup: {self._warmup_required} ticks")
    

    def update_math_params(self, math_config: KalmanConfig) -> None:
        """
        [DEPENDENCY INJECTION]
        Dipanggil oleh Factory untuk menyuntikkan/mengupdate parameter Math (R, Q).
        
        Effect:
        - Mengganti konfigurasi matematika.
        - ME-RESET filter dan buffer history (Hard Reset) untuk mencegah
          kontaminasi state lama ke parameter baru.
        """
        logger.info(f"Injecting Math Kernel: R={math_config.R}, Q={math_config.Q}, Mode={math_config.adaptation_mode}")
        
        # 1. Update Config
        self.math_config = math_config
        
        # 2. Hard Reset Math Kernel
        self._kalman_filter = None
        self._filter_initialized = False
        
        # 3. Reset Buffer & Metrics
        self._internal_state.spread_history.clear()
        self._internal_state.consecutive_errors = 0
        self._internal_state.current_estimate = 0.0
        self._internal_state.current_uncertainty = 1.0
        
        # 4. Reset timestamp gap tracker
        self._last_processed_timestamp = 0
        
        logger.info("Math Kernel & Internal State reset complete.")

    # ========== ABSTRACT METHOD IMPLEMENTATIONS ==========
    
    def _initialize_filter(self, initial_value: float) -> Result[AdaptiveKalmanFilter, str]:
        """Initialize the Kalman Filter mathematical kernel"""
        try:
            self.monitor.start_timer("filter_initialization")
            
            runtime_conf = KalmanConfig(
                R=self.math_config.R,
                Q=self.math_config.Q,
                initial_value=initial_value,
                state_dim=self.math_config.state_dim,
                adaptation_mode=self.math_config.adaptation_mode
            )
            
            filter_result = KalmanFactory.create(runtime_conf)
            
            self.monitor.stop_timer("filter_initialization")

            return filter_result

        except Exception as e:
            logger.error(f"Filter initialization error: {e}")
            return Err(f"Filter initialization error: {str(e)}")
    
    def _process_observation(self, spread: float, timestamp: int) -> Result[SignalEvent, str]:
        """
        [LOGIC GATE - KERNEL PROCESSING]
        Menerima Spread -> Tanya Kalman -> Hitung Z-Score -> Putuskan Sinyal.
        
        [UPGRADE] NEVER RETURN Err → semua error/kegagalan dikonversi ke SignalEvent(NEUTRAL)
        dengan metadata error. Strategi tidak pernah crash di Live.
        """
        try:
            # --- VALIDASI INPUT (Anti-NaN/Inf) ---
            if not isinstance(spread, (int, float)) or not np.isfinite(spread):
                return self._neutral_signal(
                    timestamp, 
                    "invalid_spread", 
                    f"Spread tidak valid: {spread}"
                )

            # --- DETEKSI GAP WAKTU (Koneksi Putus / Server Mati) ---
            if self._last_processed_timestamp > 0:
                gap_ms = timestamp - self._last_processed_timestamp
                if gap_ms > self.MAX_GAP_MS:
                    logger.warning(f"⚠️ Gap waktu {gap_ms/1000:.1f}s terdeteksi. Reset filter.")
                    self.reset()
            
            self._last_processed_timestamp = timestamp
            self._internal_state.observation_count += 1
            
            # --- LAZY INIT KALMAN FILTER ---
            if self._kalman_filter is None:
                init_result = self._initialize_filter(spread)
                if init_result.is_err():
                    return self._neutral_signal(
                        timestamp,
                        "kalman_init_failed",
                        init_result.unwrap_err()
                    )
                self._kalman_filter = init_result.unwrap()
                self._filter_initialized = True

            if self._kalman_filter is None:
                return self._neutral_signal(
                    timestamp,
                    "kalman_null",
                    "Critical: Kalman Filter gagal diinisialisasi"
                )
            
            # --- PROSES KALMAN (GPU READY) ---
            self.monitor.start_timer("kalman_update")
            
            lambda_val = getattr(self.math_config, 'lambda_factor', 1.0)
            adapt_mode = getattr(self.math_config, 'adapt', True)

            update_result = self._kalman_filter.update(
                z=spread,
                lambda_factor=lambda_val,
                adapt=adapt_mode
            )            
            update_duration = self.monitor.stop_timer(
                "kalman_update",
                metadata={"spread": spread, "timestamp": timestamp}
            )
            
            # --- HANDLE HASIL KALMAN ---
            return match_result(
                update_result,
                on_ok=lambda state: self._generate_signal_from_state(state, spread, timestamp),
                on_err=lambda err: self._handle_kalman_error(err, spread, timestamp)
            )
            
        except Exception as e:
            # SAFETY NET: Tangkap semua exception liar
            logger.error(f"Unhandled exception in _process_observation: {e}", exc_info=True)
            return self._neutral_signal(
                timestamp,
                "unhandled_exception",
                str(e)
            )

    def _extract_spread(self, data: Union[dict, pd.Series, MarketObservation]) -> Result[float, str]:
        """
        [VOLTAGE REGULATOR]
        Extract spread value dari berbagai format data dengan MarketObservation safety.
        
        Returns:
            Result[float, str]: Spread value atau error message
        """
        try:
            if isinstance(data, MarketObservation):
                return self._extract_spread_from_observation(data)
            elif isinstance(data, dict):
                return self._extract_spread_from_dict(data)
            elif isinstance(data, pd.Series):
                return self._extract_spread_from_series(data)
            else:
                return Err(f"Unsupported data type: {type(data)}")
        except Exception as e:
            logger.error(f"Spread extraction failed: {e}")
            return Err(f"Spread extraction failed: {str(e)}")

    def _extract_spread_from_observation(self, obs: MarketObservation) -> Result[float, str]:
        try:
            price_cols = [k for k in obs.data.keys() if k.startswith('close_')]
            if len(price_cols) < 2:
                return Err(f"Insufficient price data. Found: {list(obs.data.keys())}")
        
            col_y, col_x = price_cols[0], price_cols[1]
        
            if self._internal_state.price_pair == ("", ""):
                self._internal_state.price_pair = (col_y.replace('close_', ''), col_x.replace('close_', ''))
        
            p_y = obs.get_value(col_y, float)
            p_x = obs.get_value(col_x, float)
        
            if p_y.is_err() or p_x.is_err():
                return Err("Failed to extract prices")
        
            val_y = float(p_y.unwrap())
            val_x = float(p_x.unwrap())
        
        # 🔥 CEK NAN / INF
            if not np.isfinite(val_y) or not np.isfinite(val_x):
                return Err(f"Non-finite price detected: {val_y}, {val_x}")
        
            return Ok(self._calculate_spread(val_y, val_x))
        
        except Exception as e:
            return Err(f"Extract failed: {str(e)}")



    def _extract_spread_from_dict(self, data: dict) -> Result[float, str]:
        """Extract spread dari dictionary"""
        spread_columns = ['spread', 'spread_DOGE', 'beta_DOGE_BTC', 'log_spread']
        
        for col in spread_columns:
            if col in data:
                value = data[col]
                if isinstance(value, (int, float, np.number)) and np.isfinite(value):
                    return Ok(float(value))

        price_cols = [k for k in data.keys() if k.startswith('close_')]
        if len(price_cols) >= 2:
            try:
                p_y = float(data[price_cols[0]])
                p_x = float(data[price_cols[1]])
                if np.isfinite(p_y) and np.isfinite(p_x):
                    return Ok(self._calculate_spread(p_y, p_x))
            except: 
                pass
        return Err("No valid spread found")

    def _extract_spread_from_series(self, data: pd.Series) -> Result[float, str]:
        """
        [EXTRACTOR] Handle input dari DataFrame Row (pd.Series).
        Dipanggil saat Batch Processing (Backtest).
        """
        try:
            spread_columns = ['spread', 'spread_DOGE', 'beta_DOGE_BTC', 'log_spread']
            for col in spread_columns:
                if col in data.index:
                    val = data[col]
                    if pd.notnull(val) and np.isfinite(val):
                        return Ok(float(val))
            
            price_cols = [c for c in data.index if str(c).startswith('close_')]
            if len(price_cols) < 2:
                return Err(f"Insufficient price data. Found: {price_cols}")
            
            col_y, col_x = price_cols[0], price_cols[1]
            
            if self._internal_state.price_pair == ("", ""):
                asset_y = str(col_y).replace('close_', '')
                asset_x = str(col_x).replace('close_', '')
                self._internal_state.price_pair = (asset_y, asset_x)
            
            p_y = data.get(col_y)
            p_x = data.get(col_x)
            
            if p_y is None or p_x is None or pd.isnull(p_y) or pd.isnull(p_x):
                return Err("Price data contains NaNs or None")
            if not np.isfinite(p_y) or not np.isfinite(p_x):
                return Err("Non-finite price detected")

            return Ok(self._calculate_spread(float(p_y), float(p_x)))
            
        except Exception as e:
            return Err(f"Series extraction failed: {str(e)}")

    def _calculate_spread(self, price_y: float, price_x: float) -> float:
        """
        [VOLTAGE REGULATOR] - SINGLE SOURCE OF TRUTH
        Mengubah dua harga mentah menjadi satu sinyal Spread.
        Rumus: Log(Y) - Beta * Log(X)
        """
        log_y = np.log(price_y)
        log_x = np.log(price_x)
        beta = self.sig_config.hedge_ratio
        return log_y - (beta * log_x)

    # ========== SIGNAL GENERATION LOGIC ==========
    def _generate_signal_from_state(
        self, 
        state: KalmanState, 
        spread: float, 
        timestamp: int
    ) -> Result[SignalEvent, str]:
        """[DECISION MATRIX - UPGRADED]"""
        try:
            estimate = float(state.x[0, 0])
            uncertainty = float(state.P[0, 0])
            residual = spread - estimate
            
            # --- Update Rolling History dengan buffer size yang diperbesar ---
            self._internal_state.spread_history.append(residual)
            
            # [UPGRADE] Buffer minimal sebesar warmup_required
            max_buffer = max(self.sig_config.volatility_window, self._warmup_required)
            if len(self._internal_state.spread_history) > max_buffer:
                self._internal_state.spread_history.pop(0)
            
            # --- WARMUP: Pastikan buffer cukup sebelum generate sinyal ---
            if len(self._internal_state.spread_history) < self._warmup_required:
                self._warmup_count += 1
                return Ok(SignalEvent(
                    timestamp=timestamp,
                    signal_type=SignalType.NEUTRAL,
                    strength=0.0,
                    symbol=self._internal_state.price_pair[0] if self._internal_state.price_pair else "UNKNOWN",
                    _metadata={
                        "status": "warmup",
                        "samples": len(self._internal_state.spread_history),
                        "required": self._warmup_required
                    }
                ))
            
            # --- Z-SCORE Calculation ---
            volatility = np.std(self._internal_state.spread_history)
            if volatility < 1e-9:
                volatility = 1e-9
            zscore = residual / volatility
            self._internal_state.last_zscore = zscore
            
            # --- Generate Signal ---
            raw_signal_type, strength = self._determine_signal(zscore)
            
            final_signal_type = raw_signal_type

            current_time = time.time()
            if raw_signal_type != SignalType.NEUTRAL:
                time_since_last = current_time - getattr(self._internal_state, 'last_signal_time', 0.0)
                if time_since_last < 2.0:
                    final_signal_type = SignalType.NEUTRAL

            if final_signal_type in [SignalType.BUY, SignalType.SELL]:
                if final_signal_type == self._internal_state.last_signal:
                    final_signal_type = SignalType.NEUTRAL

            if final_signal_type != SignalType.NEUTRAL:
                self._internal_state.last_signal = final_signal_type
                self._internal_state.last_signal_time = current_time

            if final_signal_type in [SignalType.EXIT, SignalType.STOP]:
                self._internal_state.last_signal = SignalType.NEUTRAL

            return Ok(SignalEvent(
                timestamp=timestamp,
                signal_type=final_signal_type,
                strength=strength,
                symbol=self._internal_state.price_pair[0],
                strategy_name=self.sig_config.name,
                _metadata={
                    "zscore": float(zscore),
                    "spread": float(spread),
                    "estimate": float(estimate),
                    "volatility": float(volatility),
                    "residual": float(residual),
                    "window_size": self.sig_config.volatility_window,
                    "kalman_R": self.math_config.R,
                    "Blocked_by_guard": raw_signal_type != final_signal_type
                }
            ))
            
        except Exception as e:
            logger.error(f"Signal generation crashed: {e}")
            return self._neutral_signal(timestamp, "signal_gen_error", str(e))

    def _determine_signal(self, zscore: float) -> Tuple[SignalType, float]:
        """
        [TRADING LOGIC] - REVISI ENUM (BUY/SELL)
        Menggunakan parameter dari self.sig_config
        """
        entry_z = self.sig_config.entry_z_score
        exit_z = self.sig_config.exit_z_score
        stop_z = self.sig_config.stop_loss_z
        
        abs_z = abs(zscore)
        
        if abs_z > stop_z:
            return SignalType.STOP, zscore
        if abs_z < exit_z:
            return SignalType.EXIT, zscore
        if zscore > entry_z:
            return SignalType.SELL, zscore
        if zscore < -entry_z:
            return SignalType.BUY, zscore
        return SignalType.NEUTRAL, zscore

    def _handle_kalman_error(self, error: KalmanError, spread: float, timestamp: int) -> Result[SignalEvent, str]:
        """Handle Kalman filter errors gracefully - selalu return OK dengan sinyal NEUTRAL"""
        self._internal_state.consecutive_errors += 1
        
        if self._internal_state.consecutive_errors > 3:
            logger.error("Too many consecutive Kalman errors, resetting filter")
            self.reset()
        
        metadata = {
            "error": str(error),
            "spread": spread,
            "consecutive_errors": self._internal_state.consecutive_errors,
            "filter_state": "degraded"
        }
        
        return Ok(SignalEvent(
            timestamp=timestamp,
            signal_type=SignalType.NEUTRAL,
            strength=0.0,
            _metadata=metadata
        ))

    def _neutral_signal(self, timestamp: int, error_code: str, error_msg: str) -> Result[SignalEvent, str]:
        """Helper: buat sinyal NEUTRAL dengan metadata error"""
        return Ok(SignalEvent(
            timestamp=timestamp,
            signal_type=SignalType.NEUTRAL,
            strength=0.0,
            symbol=self._internal_state.price_pair[0] if self._internal_state.price_pair else "UNKNOWN",
            strategy_name=self.sig_config.name,
            _metadata={
                "error": error_code,
                "error_detail": error_msg,
                "status": "degraded"
            }
        ))
    
    # ========== DUAL-PATH PROCESSING ==========
    
    def generate_signals(self, df: pd.DataFrame) -> Result[pd.DataFrame, str]:
        """
        [PORT A: HARD DISK INPUT - Research/Backtest]
        Menerima DataFrame Silver Lake -> Return DataFrame dengan Sinyal.
        """
        try:
            self.monitor.start_timer("batch_processing")
            
            if df.empty:
                return Err("Empty DataFrame provided")
            if 'timestamp' not in df.columns:
                return Err("DataFrame must have 'timestamp' column")
            
            price_cols = [col for col in df.columns if col.startswith('close_')]
            if len(price_cols) < 2:
                return Err(f"Need at least 2 price columns. Found: {price_cols}")
            
            # Reset state untuk batch baru
            self._internal_state = KalmanMRState()
            self._kalman_filter = None
            self._filter_initialized = False
            self._last_processed_timestamp = 0
            
            signals = []
            for idx, row in df.iterrows():
                timestamp = row['timestamp']
                if hasattr(timestamp, 'timestamp'):
                    timestamp = int(timestamp.timestamp() * 1000)
                
                spread_result = self._extract_spread(row)
                
                if spread_result.is_err():
                    error_msg = spread_result.unwrap_err()
                    logger.debug(f"Row {idx}: {error_msg}")
                    signals.append(SignalEvent(
                        timestamp=timestamp,
                        signal_type=SignalType.NEUTRAL,
                        strength=0.0,
                        _metadata={"error": error_msg}
                    ))
                    continue
                
                spread = spread_result.unwrap()
                signal_result = self._process_observation(spread, timestamp)
                
                # [UPGRADE] _process_observation selalu return Ok, jadi is_ok() selalu True
                signals.append(signal_result.unwrap())
            
            result_df = df.copy()
            result_df['signal_type'] = [s.signal_type.value for s in signals]
            result_df['signal_type_name'] = [s.signal_type.name for s in signals]
            result_df['signal_strength'] = [s.strength for s in signals]
            result_df['signal_metadata'] = [s.metadata for s in signals]
            result_df['z_score'] = [s.metadata.get('zscore', 0.0) for s in signals]
            result_df['spread_val'] = [s.metadata.get('spread', 0.0) for s in signals]
            result_df['estimate'] = [s.metadata.get('estimate', 0.0) for s in signals]
            
            duration = self.monitor.stop_timer("batch_processing")
            logger.info(f"Batch processing complete: {len(df)} rows, {duration:.2f}ms")
            
            return Ok(result_df)
            
        except Exception as e:
            logger.error(f"Batch processing failed: {e}", exc_info=True)
            return Err(f"Batch processing failed: {str(e)}")

    def evaluate_state(self, observation: MarketObservation) -> Result[SignalEvent, str]:
        """
        [PORT B: LIVE INPUT - Real-time Execution]
        [UPGRADE] Semua data invalid dikonversi ke sinyal NEUTRAL, tidak pernah raise Err.
        """
        try:
            # --- POISON CONTROL (Input Validation) ---
            if not observation.data:
                return self._neutral_signal(observation.timestamp, "empty_data", "Observation data kosong")

            # Deteksi Pair
            p_target = observation.data.get("close_DOGE", observation.data.get("close"))
            p_ref = observation.data.get("close_BTC")

            # Cek keberadaan & validitas numerik
            if p_target is None or p_ref is None:
                return self._neutral_signal(
                    observation.timestamp,
                    "missing_price",
                    f"Missing price. Target: {p_target}, Ref: {p_ref}"
                )
            
            try:
                p_target_f = float(p_target)
                p_ref_f = float(p_ref)
            except (ValueError, TypeError):
                return self._neutral_signal(
                    observation.timestamp,
                    "price_cast_error",
                    f"Cannot cast to float: {p_target}, {p_ref}"
                )

            if not np.isfinite(p_target_f) or not np.isfinite(p_ref_f):
                return self._neutral_signal(
                    observation.timestamp,
                    "non_finite_price",
                    f"Non-finite price: {p_target_f}, {p_ref_f}"
                )
            
            if p_target_f <= 0 or p_ref_f <= 0:
                return self._neutral_signal(
                    observation.timestamp,
                    "zero_negative_price",
                    f"Zero/negative price: {p_target_f}, {p_ref_f}"
                )

            # --- CEK GAP WAKTU (LIVE ONLY) ---
            if self._last_processed_timestamp > 0:
                gap_ms = observation.timestamp - self._last_processed_timestamp
                if gap_ms > self.MAX_GAP_MS:
                    logger.warning(f"⚠️ Live gap {gap_ms/1000:.1f}s detected. Resetting filter.")
                    self.reset()
            
            # --- PROSES SPREAD ---
            spread = self._calculate_spread(p_target_f, p_ref_f)
            return self._process_observation(spread, observation.timestamp)

        except Exception as e:
            # SAFETY NET: tangkap semua exception
            logger.error(f"Live evaluation crash: {e}", exc_info=True)
            return self._neutral_signal(
                observation.timestamp,
                "live_crash",
                str(e)
            )

    # ========== STRATEGY MANAGEMENT ==========
    
    def update_position(self, size: float, price: float) -> Result[None, str]:
        """Update current position size (called by execution engine)"""
        try:
            max_position = getattr(self.sig_config, 'max_position', 1.0)
            if abs(size) > max_position:
                return Err(f"Position size {size} exceeds maximum {max_position}")
            
            old_position = self._internal_state.position_size
            if old_position != 0:
                pnl_change = (size - old_position) * price * 0.01
                self._internal_state.total_pnl += pnl_change
            
            self._internal_state.position_size = size
            if size != old_position:
                self._internal_state.trade_count += 1
            
            logger.info(f"Position updated: {old_position} -> {size}, PnL: {self._internal_state.total_pnl:.2f}")
            return Ok(None)
            
        except Exception as e:
            logger.error(f"Position update failed: {e}")
            return Err(f"Position update failed: {str(e)}")
    
    def get_current_state(self) -> Dict[str, Any]:
        """Get current strategy state untuk monitoring"""
        return {
            'strategy': self.name,
            'version': self.version,
            'position_size': self._internal_state.position_size,
            'current_estimate': self._internal_state.current_estimate,
            'current_uncertainty': self._internal_state.current_uncertainty,
            'last_zscore': self._internal_state.last_zscore,
            'total_pnl': self._internal_state.total_pnl,
            'trade_count': self._internal_state.trade_count,
            'consecutive_errors': self._internal_state.consecutive_errors,
            'buffer_size': len(self._internal_state.spread_history),
            'price_pair': self._internal_state.price_pair,
            'warmup_complete': len(self._internal_state.spread_history) >= self._warmup_required,
            'market_state': self.get_state().value,
            'config': {
                'entry': self.sig_config.entry_z_score,
                'exit': self.sig_config.exit_z_score,
                'R': self.math_config.R,
                'Q': self.math_config.Q
            }
        }
    
    def reset(self) -> Result[None, str]:
        """Reset strategy ke initial state"""
        try:
            self._kalman_filter = None
            self._internal_state = KalmanMRState()
            self._filter_initialized = False
            self._warmup_count = 0
            self._last_processed_timestamp = 0   # Reset timestamp gap tracker

            if hasattr(self.monitor, 'reset'):
                self.monitor.reset()
            
            logger.info("Strategy reset complete")
            return super().reset()
            
        except Exception as e:
            logger.error(f"Strategy reset failed: {e}")
            return Err(f"Strategy reset failed: {str(e)}")
    
    # ========== ASYNC SUPPORT ==========
    
    @safe_async
    async def evaluate_state_async(self, obs: Union[dict, MarketObservation]) -> Result[SignalEvent, str]:
        """Async version of evaluate_state untuk live trading"""
        import asyncio
        from concurrent.futures import ThreadPoolExecutor
        
        def sync_evaluate():
            return self.evaluate_state(obs)
        
        loop = asyncio.get_event_loop()
        with ThreadPoolExecutor(max_workers=1) as executor:
            result = await loop.run_in_executor(executor, sync_evaluate)
        return result
    
    # ========== DIAGNOSTICS & MONITORING ==========
    
    def get_performance_summary(self) -> Dict[str, Any]:
        """Get comprehensive performance summary"""
        base_metrics = super().get_performance_metrics()
        
        if self._internal_state.spread_history:
            spreads = self._internal_state.spread_history
            metrics = {
                'total_observations': self._internal_state.observation_count,
                'mean_spread': float(np.mean(spreads)),
                'std_spread': float(np.std(spreads)),
                'min_spread': float(np.min(spreads)),
                'max_spread': float(np.max(spreads)),
                'avg_zscore': float(self._internal_state.last_zscore),
                'avg_kalman_latency': self.monitor.get_avg_latency('kalman_update'),
                'avg_batch_latency': self.monitor.get_avg_latency('batch_processing'),
                'avg_live_latency': self.monitor.get_avg_latency('live_evaluation'),
                'filter_errors': self._internal_state.consecutive_errors,
                'warmup_periods': self._warmup_count,
                'signal_distribution': self._get_signal_distribution(),
                'sharpe_ratio': self._calculate_sharpe_ratio()
            }
        else:
            metrics = {
                'total_observations': 0,
                'status': 'awaiting_data'
            }
        return {**base_metrics, **metrics}
    
    def _get_signal_distribution(self) -> Dict[str, int]:
        """Calculate distribution of signal types"""
        return {'neutral': 0, 'buy': 0, 'sell': 0, 'exit': 0, 'stop': 0}
    
    def _calculate_sharpe_ratio(self) -> float:
        """Calculate Sharpe ratio dari P&L history (simplified)"""
        if self._internal_state.trade_count == 0:
            return 0.0
        avg_return = self._internal_state.total_pnl / max(self._internal_state.trade_count, 1)
        return avg_return / max(self._internal_state.current_uncertainty, 1e-9)
    
    def get_diagnostics(self) -> Dict[str, Any]:
        """Get diagnostic information untuk debugging"""
        monitor_stats = {
            'avg_kalman_latency': self.monitor.get_avg_latency('kalman_update'),
            'avg_batch_latency': self.monitor.get_avg_latency('batch_processing'),
            'avg_live_latency': self.monitor.get_avg_latency('live_evaluation')
        }
        return {
            'filter_initialized': self._filter_initialized,
            'kalman_filter': 'active' if self._kalman_filter else 'inactive',
            'buffer_size': len(self._internal_state.spread_history),
            'performance_metrics': self.monitor.get_summary() if hasattr(self.monitor, 'get_summary') else {},
            'monitor_stats': monitor_stats,
            'last_timestamp': self._last_processed_timestamp
        }
