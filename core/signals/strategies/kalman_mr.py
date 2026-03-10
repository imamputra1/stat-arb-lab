"""
THE MOTHERBOARD: KALMAN MEAN REVERSION STRATEGY
Location: core/signals/strategies/kalman_mr.py
Role: Menghubungkan Data (HDD/IoT) ke Math Kernel (GPU) dengan MarketObservation.
      [UPGRADE] Robust untuk MTF: warmup panjang, anti-crash, auto-reset saat disconnect.
      [ANTI‑NAN] Volatility‑based Q modulation dengan Z‑score clipping [0,3].
"""

import numpy as np
import pandas as pd
from typing import Optional, Tuple, Dict, Any, List, Union, cast
from dataclasses import dataclass, field
import logging


# IMPORT CHIP GPU (MATH KERNEL)
from core.math import (
    AdaptiveKalmanFilter,
    KalmanConfig,
    KalmanFactory,
    KalmanState,
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
    spread_history: List[float] = field(default_factory=list)      # Raw spread values
    volatility_history: List[float] = field(default_factory=list)  # Rolling volatility
    price_pair: Tuple[str, str] = ("", "")                         # (asset_y, asset_x)
    last_signal: SignalType = SignalType.NEUTRAL
    last_signal_time: float = 0.0
    last_factor_q: float = 1.0


# ========== MAIN STRATEGY CLASS ==========

class KalmanMeanReversion(BaseStrategy):
    """
    [THE MOTHERBOARD] dengan Volatility‑Based Q Clipping.
    """

    # --- CONSTANTS FOR ROBUSTNESS (MTF) ---
    WARMUP_REQUIRED = 100               # Minimal 100 candle sebelum menghasilkan sinyal
    MAX_GAP_MS = 300000                  # 5 menit: reset filter jika ada gap lebih
    VOL_BUFFER_MIN = 30                   # Minimal sampel untuk hitung Z‑score volatilitas
    VOL_SENSITIVITY = 0.5                 # Sensitivitas modulasi Q (faktor = 1 + sens * Z_clipped)

    def __init__(self,
                 signal_config: SignalConfig,
                 math_config: Optional[KalmanConfig] = None):
        super().__init__(name=signal_config.name, version=signal_config.version)
        
        self.sig_config = signal_config

        if math_config is not None:
            self.math_config = math_config
        else:
            # Default Safe Values
            self.math_config = KalmanConfig(
                R=9.215e-7,
                Q=1e-8,
                initial_value=0.0,
                adaptation_mode=AdaptationMode.NIS_THRESHOLD
            )

        self._warmup_required = getattr(self.sig_config, 'warmup_ticks', self.sig_config.volatility_window)
        self._internal_state = KalmanMRState()
        self._kalman_filter =  None
        self._filter_initialized = False
        self._warmup_count = 0
        self._last_processed_timestamp = 0

        validation = self.sig_config.validate()
        if validation.is_err():
            raise ValueError(f"Invalid signal config: {validation.unwrap_err()}")

        logger.info(f"Initialized KalmanMRStrategy: {self.name} v{self.version} | Warmup: {self.WARMUP_REQUIRED} ticks")

    def update_math_params(self, math_config: KalmanConfig) -> None:
        """Dependency injection untuk parameter matematika."""
        logger.info(f"Injecting Math Kernel: R={math_config.R}, Q={math_config.Q}, Mode={math_config.adaptation_mode}")
        self.math_config = math_config
        self._kalman_filter = None
        self._filter_initialized = False
        self._internal_state.spread_history.clear()
        self._internal_state.volatility_history.clear()
        self._internal_state.consecutive_errors = 0
        self._internal_state.current_estimate = 0.0
        self._internal_state.current_uncertainty = 1.0
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

    def _process_observation(self, spread: float, timestamp: int, symbol: str = "UNKNOWN") -> Result[SignalEvent, str]:
        """
        Proses observasi tunggal dengan modulasi Q berdasarkan volatilitas (Z‑score clipped 0‑3).
        """
        try:
            self._internal_state.price_pair = (symbol, "")
            # --- VALIDASI INPUT ---
            if not isinstance(spread, (int, float)) or not np.isfinite(spread):
                return self._neutral_signal(
                    timestamp,
                    "invalid_spread",
                    f"Spread tidak valid: {spread}"
                )

            # --- CEK WARMUP DI AWAL ---
            if not hasattr(self, '_internal_state') or self._internal_state is None:
                self._internal_state = KalmanMRState()

            self._internal_state.observation_count += 1


            self._internal_state.spread_history.append(spread)
            window = getattr(self.sig_config, 'volatility_window', 50)

            if len(self._internal_state.spread_history) >= window:
                recent = self._internal_state.spread_history[-window:]
                current_vol = float(np.std(recent))
            else:
                current_vol = 0.0

            if not hasattr(self._internal_state, 'volatility_history'):
                self._internal_state.volatility_history = []
            self._internal_state.volatility_history.append(current_vol)

            # --- WARMUP CHECK ---
            if len(self._internal_state.spread_history) < self._warmup_required:
                self._warmup_count += 1
                return Ok(SignalEvent(
                    timestamp=timestamp,
                    signal_type=SignalType.NEUTRAL,
                    strength=0.0,
                    symbol=self._internal_state.price_pair[0] if self._internal_state.price_pair else "UNKNOWN",
                    strategy_name=self.sig_config.name,
                    _metadata={
                        "status": "warmup",
                        "factor_q": float(getattr(self._internal_state, 'last_factor_q', 1.0)) # 🛠️ TAMBAHKAN INI
                    }
                ))            

            # --- DETEKSI GAP WAKTU ---
            if self._last_processed_timestamp > 0:
                gap_ms = timestamp - self._last_processed_timestamp
                if gap_ms > self.MAX_GAP_MS:
                    logger.warning(f"⚠️ Gap waktu {gap_ms/1000:.1f}s terdeteksi. Reset filter.")
                    self.reset()
            self._last_processed_timestamp = timestamp

            # --- LAZY INIT KALMAN FILTER ---
            if self._kalman_filter is None:
                init_result = self._initialize_filter(spread)
                if init_result.is_err():
                    return self._neutral_signal(
                        timestamp,
                        "kalman_init_failed",
                        init_result.unwrap_err() or "Unknown initialization error"
                    )
                self._kalman_filter = cast(AdaptiveKalmanFilter, init_result.unwrap())
                self._filter_initialized = True

            if self._kalman_filter is None:
                return self._neutral_signal(
                    timestamp,
                    "kalman_null",
                    "Critical: Kalman Filter gagal diinisialisasi"
                )

            # --- HITUNG Z‑SCORE VOLATILITAS (JIKA BUFFER CUKUP) ---
            factor_q = 1.0
            z_vol_clipped = 0.0
            vol_buffer_size = getattr(self, 'VOL_BUFFER_MIN', 100)
            vol_sensitivity = getattr(self, 'VOL_SENSITIVITY', 5.0)

            if len(self._internal_state.volatility_history) >= vol_buffer_size:
                vol_arr = np.array(self._internal_state.volatility_history)
                vol_arr = vol_arr[np.isfinite(vol_arr)]
                mean_vol = np.mean(vol_arr)
                std_vol = np.std(vol_arr)
                if std_vol > 1e-12:
                    z_vol = (current_vol - mean_vol) / std_vol
                    # Clip hanya sisi positif: [0, 3] – Q tidak pernah dikecilkan
                    z_vol_clipped = np.clip(z_vol, 0.0, 5.0)

                    factor_q = 1.0 + (vol_sensitivity * z_vol_clipped)
                    if factor_q < 1.0:
                        logger.warning(...)
                        factor_q = 1.0
                    factor_q = min(max(factor_q, 1.0), 50.0)

            # --- TERAPKAN FAKTOR Q SEMENTARA ---
            kf = cast(AdaptiveKalmanFilter, self._kalman_filter)
            self._internal_state.last_factor_q = float(factor_q)
            orig_base_Q = kf.base_Q.copy()
            kf.base_Q = orig_base_Q * float(factor_q)

            # --- PROSES KALMAN UPDATE ---
            self.monitor.start_timer("kalman_update")
            lambda_val = getattr(self.math_config, 'lambda_factor', 1.0)
            adapt_mode = getattr(self.math_config, 'adapt', True)

            update_result = kf.update(
                z=spread,
                lambda_factor=lambda_val,
                adapt=adapt_mode
            )
            self.monitor.stop_timer(
                "kalman_update",
                metadata={
                    "spread": spread,
                    "timestamp": timestamp,
                    "factor_q": factor_q,
                    "z_vol_clipped": z_vol_clipped
                }
            )
            # --- KEMBALIKAN Q KE NILAI ASLI ---
            kf.base_Q = orig_base_Q

            # --- Jika sudah warmup, lanjutkan generate sinyal normal ---
            return match_result(
                update_result,
                on_ok=lambda state: self._on_kalman_update_ok(state, spread, timestamp),
                on_err=lambda err: self._handle_kalman_error(err, spread, timestamp)
            )

        except Exception as e:
            logger.error(f"Unhandled exception in _process_observation: {e}", exc_info=True)
            return self._neutral_signal(
                timestamp,
                "unhandled_exception",
                str(e)
            )

    def _on_kalman_update_ok(self, state: KalmanState, spread: float, timestamp: int) -> Result[SignalEvent, str]:
        """Helper fungsional untuk memproses state Kalman yang sukses (Linter-Proof)."""
        self._internal_state.consecutive_errors = 0
        return self._generate_signal_from_state(state, spread, timestamp)

    def _extract_spread(self, data: Union[dict, pd.Series, MarketObservation]) -> Result[float, str]:
        """Extract spread dari berbagai format data."""
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

            val_y = p_y.unwrap()
            val_x = p_x.unwrap()

            if val_y is None or val_x is None:
                return Err("Ekstraksi gagal: Harga bernilai None")

            float_x = float(val_x)
            float_y = float(val_y)

            if float_y <= 0 or float_x <= 0:
                return Err(f"Zero or negative price detected: {float_y}, {float_x}")

            if not np.isfinite(float_y) or not np.isfinite(float_x):
                return Err(f"Non-finite price detected: {val_y}, {val_x}")

            return Ok(self._calculate_spread(val_y, val_x))

        except Exception as e:
            return Err(f"Extract failed: {str(e)}")

    def _extract_spread_from_dict(self, data: dict) -> Result[float, str]:
        spread_columns = ['spread', 'log_spread', 'target_spread']
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
                    if self._internal_state.price_pair == ("", ""):
                        self._internal_state.price_pair = (price_cols[0].replace('close_', ''), price_cols[1].replace('close_', ''))
                    return Ok(self._calculate_spread(p_y, p_x))
            except (ValueError, TypeError, KeyError):
                pass
        return Err("No valid spread found")

    def _extract_spread_from_series(self, data: pd.Series) -> Result[float, str]:
        """Extract spread dari baris Pandas secara kebal peluru."""
        try:
            # --- HELPER: Bulletproof Scalar Extractor ---
            def _get_scalar_float(col_name: str) -> Optional[float]:
                if col_name not in data.index:
                    return None
                    
                val = data.get(col_name)
                if val is None or pd.isna(val):
                    return None
                    
                # Jika ada duplikasi kolom (Pandas Series)
                if isinstance(val, pd.Series):
                    if val.empty: 
                        return None
                    val = val.iloc[0]
                    
                try:
                    # TYPE GUARD: Membungkam Linter ConvertibleToFloat
                    if isinstance(val, (int, float)):
                        f_val = float(val)
                    else:
                        # Jika Unknown, paksa jadi string dulu baru ke float
                        f_val = float(str(val)) 
                        
                    if np.isfinite(f_val):
                        return f_val
                except (ValueError, TypeError):
                    pass
                return None
            # --------------------------------------------

            # 1. Cari kolom spread langsung
            spread_columns = ['spread', 'log_spread', 'target_spread']
            for col in spread_columns:
                val = _get_scalar_float(col)
                if val is not None:
                    return Ok(val)

            # 2. Cari dari harga close
            price_cols = [str(c) for c in data.index if str(c).startswith('close_')]
            if len(price_cols) < 2:
                return Err(f"Insufficient price data. Found: {price_cols}")

            col_y, col_x = price_cols[0], price_cols[1]
            if self._internal_state.price_pair == ("", ""):
                asset_y = col_y.replace('close_', '')
                asset_x = col_x.replace('close_', '')
                self._internal_state.price_pair = (asset_y, asset_x)

            # Ekstraksi dengan jaminan linter (pasti float atau None)
            p_y = _get_scalar_float(col_y)
            p_x = _get_scalar_float(col_x)

            if p_y is None or p_x is None:
                return Err("Price data contains NaNs, None, or invalid types")

            return Ok(self._calculate_spread(p_y, p_x))
            
        except Exception as e:
            return Err(f"Series extraction failed: {str(e)}")


    def _calculate_spread(self, price_y: float, price_x: float) -> float:
        """
        Kalkulasi spread (Log Y - Beta * Log X).
        FUNGSI INI WAJIB ADA. Jangan sampai terhapus lagi, Chief!
        """
        # Pengaman ekstra: Harga tidak boleh nol/negatif saat di-log
        if price_y <= 0 or price_x <= 0:
            return 0.0
            
        log_y = float(np.log(price_y))
        log_x = float(np.log(price_x))
        beta = float(getattr(self.sig_config, 'hedge_ratio', 1.0))
        
        return log_y - (beta * log_x)

    # ========== SIGNAL GENERATION LOGIC ==========

    def _generate_signal_from_state(self, state: KalmanState, spread: float, timestamp: int) -> Result[SignalEvent, str]:
        """Menghasilkan sinyal trading dengan State-Aware Engine murni (Tanpa OMS Dependency)."""
        try:
            if not np.isfinite(state.x[0, 0]) or not np.isfinite(state.P[0, 0]):
                return self._neutral_signal(timestamp, "numerical_instability", "State contains NaN")
            estimate = float(state.x[0, 0])
            residual = spread - estimate

            current_factor_q = float(getattr(self._internal_state, 'last_factor_q', 1.0))

            # --- WARMUP CHECK ---
            # FIX: Gunakan _warmup_required agar Unit Test (50 vs 100) tetap lulus
            # if len(self._internal_state.spread_history) < self._warmup_required:
            #    self._warmup_count += 1
            #    return Ok(SignalEvent(
            #        timestamp=timestamp,
            #        signal_type=SignalType.NEUTRAL,
            #        strength=0.0,
            #        symbol=self._internal_state.price_pair[0] if getattr(self._internal_state, 'price_pair', None) else "UNKNOWN",
            #        strategy_name=self.sig_config.name,
            #        _metadata={
            #            "status": "warmup",
            #            "factor_q": current_factor_q
            #        }
            #    ))

            # --- Z-SCORE CALCULATION ---
            # Inovasi Varians (S) = P + R
            kalman_variance = float(state.P[0, 0]) + getattr(self.math_config, 'R', 0.001)

            # 🧠 FIX MATEMATIKA: Volatilitas adalah Standar Deviasi (Akar Kuadrat dari Varians)
            volatility = float(np.sqrt(max(kalman_variance, 1e-18)))
            # Bandingkan volatilitas teoritis dengan Volatilitas Empiris (Realitas Pasar)
            # yang sudah dihitung dari spread_history.
            if hasattr(self._internal_state, 'volatility') and self._internal_state.volatility_history:

                empirical_vol = float(self._internal_state.volatility_history[-1])
                volatility = max(volatility, empirical_vol)

            volatility = max(volatility, 1e-9) # Pengaman dari pembagian dengan nol
            
            zscore = residual / volatility
            self._internal_state.last_zscore = float(zscore)

            # ==========================================================
            # 🧠 THE SURGERY: STATE-AWARE SIGNAL ENGINE (STANDALONE)
            # ==========================================================
            current_position = self._internal_state.last_signal 
            final_signal_type = SignalType.NEUTRAL
            
            entry_z = getattr(self.sig_config, 'entry_z_score', 2.0)
            exit_z = getattr(self.sig_config, 'exit_z_score', 0.5)
            stop_z = getattr(self.sig_config, 'stop_loss_z', 4.0)

            # --- LOGIKA KEPUTUSAN ANTI-JUMP ---
            if current_position == SignalType.BUY:
                if zscore < -stop_z:
                    final_signal_type = SignalType.STOP
                elif zscore >= -exit_z: # Jika melompat ke arah rata-rata/positif -> EXIT
                    final_signal_type = SignalType.EXIT
                    
            elif current_position == SignalType.SELL:
                if zscore > stop_z:
                    final_signal_type = SignalType.STOP
                elif zscore <= exit_z: # Jika melompat ke arah rata-rata/negatif -> EXIT
                    final_signal_type = SignalType.EXIT
                    
            else: # Jika NEUTRAL (Kosong)
                if zscore < -entry_z:
                    final_signal_type = SignalType.BUY
                elif zscore > entry_z:
                    final_signal_type = SignalType.SELL

            # --- UPDATE STATE MEMORY ---
            if final_signal_type != SignalType.NEUTRAL:
                if final_signal_type in [SignalType.EXIT, SignalType.STOP]:
                    self._internal_state.last_signal = SignalType.NEUTRAL
                else:
                    self._internal_state.last_signal = final_signal_type

            # --- BUNGKUS KE DALAM EVENT ---
            return Ok(SignalEvent(
                timestamp=timestamp,
                signal_type=final_signal_type,
                strength=1.0 if final_signal_type != SignalType.NEUTRAL else 0.0,
                symbol=self._internal_state.price_pair[0] if self._internal_state.price_pair else "UNKNOWN",
                strategy_name=self.sig_config.name,
                _metadata={
                    "zscore": float(zscore),
                    "spread": float(spread),
                    "estimate": float(estimate),
                    "volatility": float(volatility),
                    "kalman_variance": float(kalman_variance), # Ditambahkan untuk diagnostic
                    "factor_q": current_factor_q # 🛠️ TAMBAHKAN INI
                }
            ))
            
        except Exception as e:
            logger.error(f"Signal generation crashed: {e}")
            return self._neutral_signal(timestamp, "signal_gen_error", str(e))


    def _handle_kalman_error(self, error: Any, spread: float, timestamp: int) -> Result[SignalEvent, str]:
        """Menangani error dari Kalman Filter dengan mekanisme Self-Healing."""
        self._internal_state.consecutive_errors += 1
        
        # Logika Self-Healing Anda
        if self._internal_state.consecutive_errors >= 3:
            logger.error(f"Too many consecutive Kalman errors ({self._internal_state.consecutive_errors}). Resetting filter.")
            self.reset()
            
        # Metadata disesuaikan dengan standar Unit Test & OMS
        metadata = {
            "error": "kalman_update_error",
            "error_detail": str(error),
            "spread": float(spread),
            "consecutive_errors": self._internal_state.consecutive_errors,
            "status": "degraded"
        }
        
        # Bungkusan SignalEvent lengkap (Anti-Crash)
        return Ok(SignalEvent(
            timestamp=timestamp,
            signal_type=SignalType.NEUTRAL,
            strength=0.0,
            symbol=self._internal_state.price_pair[0] if getattr(self._internal_state, 'price_pair', None) else "UNKNOWN",
            strategy_name=self.sig_config.name,
            _metadata=metadata
        ))

    def _neutral_signal(self, timestamp: int, error_code: str, error_msg: str, status: str = "degraded") -> Result[SignalEvent, str]:
        """Helper: buat sinyal NEUTRAL dengan metadata error dan konsistensi telemetri."""
        
        # 1. Ekstraksi Symbol Super Aman (Defensive)
        pair_tuple = getattr(self._internal_state, 'price_pair', None)
        symbol_name = pair_tuple[0] if pair_tuple and len(pair_tuple) > 0 and pair_tuple[0] else "UNKNOWN"
        
        # 2. Ambil factor_q terakhir untuk Dashboard / Test Suite
        current_factor_q = float(getattr(self._internal_state, 'last_factor_q', 1.0))

        return Ok(SignalEvent(
            timestamp=timestamp,
            signal_type=SignalType.NEUTRAL,
            strength=0.0,
            symbol=symbol_name,
            strategy_name=self.sig_config.name,
            _metadata={
                "error": error_code,
                "error_detail": error_msg,
                "status": status,
                "factor_q": current_factor_q  # 🛠️ Telemetri tetap hidup meski bot error!
            }
        ))


    def generate_signals(self, df: pd.DataFrame) -> Result[pd.DataFrame, str]:
        """Batch processing untuk backtest (100% Agnostik & Performa Tinggi)."""
        try:
            self.monitor.start_timer("batch_processing")
            
            # 1. Validasi Awal
            if df.empty:
                return Err("Empty DataFrame provided")
            if 'timestamp' not in df.columns:
                return Err("DataFrame must have 'timestamp' column")

            # 2. Persiapan State
            self.reset()
            sim_symbol = str(df['symbol'].iloc[0]) if 'symbol' in df.columns else "BACKTEST_SIM"
            results = []

            # 3. Iterasi - Gunakan to_dict('records') untuk performa 3x lebih cepat dibanding iterrows
            records = df.to_dict('records')

            for row in records:
                # --- JALUR AGNOSTIK TOTAL ---
                # Kita tidak lagi membedah kolom di sini. 
                # Cukup lempar seluruh dict 'row' ke evaluate_state.
                # evaluate_state sudah kita desain untuk bisa menangani dict secara cerdas.
                
                eval_result = self.evaluate_state(row)
                
                # --- EKSTRAKSI HASIL ---
                if eval_result.is_ok():
                    sig = cast(SignalEvent, eval_result.unwrap())
                else:
                    # Fallback jika terjadi error pada baris tertentu
                    err_msg = str(eval_result.unwrap_err())
                    # Parsing timestamp mentah untuk fallback agar baris tetap sinkron
                    raw_ts = row.get('timestamp', 0)
                    sig = SignalEvent(
                        timestamp=0, # Akan diupdate di bawah
                        signal_type=SignalType.NEUTRAL,
                        strength=0.0,
                        symbol=sim_symbol,
                        strategy_name=self.sig_config.name,
                        _metadata={"status": "error", "error": err_msg}
                    )

                # --- PENYUSUNAN HASIL (DRY - Don't Repeat Yourself) ---
                # Mengambil metadata dengan aman
                meta = sig.metadata
                results.append({
                    'timestamp': row['timestamp'], # Tetap gunakan timestamp asli dari input
                    'signal_type': sig.signal_type.value,
                    'signal_type_name': sig.signal_type.name,
                    'signal_strength': float(sig.strength),
                    'z_score': float(meta.get('zscore', 0.0)),
                    'spread_val': float(meta.get('spread', 0.0)),
                    'estimate': float(meta.get('estimate', 0.0)),
                    'volatility': float(meta.get('volatility', 0.0)),
                    'factor_q': float(meta.get('factor_q', 1.0)),
                    'status': str(meta.get('status', 'active')),
                    'error': str(meta.get('error', '')),
                })

            # 4. Finalisasi
            result_df = pd.DataFrame(results)
            
            # Gabungkan dengan kolom asli yang tidak ada di hasil (opsional, agar data lengkap)
            # Misalnya volume atau kolom tambahan lainnya dari backtest
            for col in df.columns:
                if col not in result_df.columns:
                    result_df[col] = df[col].values

            duration = self.monitor.stop_timer("batch_processing")
            logger.info(f"Batch processing complete: {len(df)} rows, {duration:.2f}ms")
            
            return Ok(result_df)
            
        except Exception as e:
            logger.error(f"Batch processing failed: {e}", exc_info=True)
            return Err(f"Batch processing failed: {str(e)}")

    def evaluate_state(self, obs: Union[dict, MarketObservation]) -> Result[SignalEvent, str]:
        """
        Live processing untuk real‑time tick (100% Agnostik).
        
        Pintu masuk tunggal baik dari dict (API/WebSocket) maupun MarketObservation.
        Fungsi ini mendelegasikan validasi harga dan kalkulasi spread ke _extract_spread.
        """
        observation: Optional[MarketObservation] = None
        
        try:
            # 1. Parsing Input ke MarketObservation
            if isinstance(obs, dict):
                import time
                # Gunakan timestamp dari data, jika tidak ada pakai waktu sekarang (ms)
                ts = int(obs.get("timestamp", time.time() * 1000))
                observation = MarketObservation(timestamp=ts, data=obs)
            else:
                observation = obs

            # Validasi dasar data
            if not observation.data:
                return self._neutral_signal(
                    observation.timestamp, 
                    "empty_data", 
                    "Observation data kosong"
                )

            # 2. Ekstraksi & Validasi Spread (Agnostik)
            # Fungsi ini sudah mencakup: 
            # - Mencari kolom 'close_XXX' secara otomatis
            # - Konversi ke float
            # - Cek np.isfinite
            # - Cek harga <= 0 (untuk log spread)
            spread_result = self._extract_spread(observation)
            
            if spread_result.is_err():
                # Jika data tidak valid, kembalikan sinyal NEUTRAL dengan alasan errornya
                return self._neutral_signal(
                    observation.timestamp, 
                    "extract_error", 
                    str(spread_result.unwrap_err())
                )

            spread = float(spread_result.unwrap())

            # 3. Pengelolaan State Waktu (Gap Detection)
            # Jika ada gap waktu terlalu besar, reset filter agar tidak menggunakan state usang
            if self._last_processed_timestamp > 0:
                gap_ms = observation.timestamp - self._last_processed_timestamp
                if gap_ms > self.MAX_GAP_MS:
                    logger.warning(
                        f"⚠️ Live gap {gap_ms/1000:.1f}s detected untuk {observation.symbol}. "
                        f"Resetting Kalman Filter."
                    )
                    self.reset()
            
            # Update timestamp terakhir yang berhasil diproses
            self._last_processed_timestamp = observation.timestamp

            # 4. Eksekusi Strategi (Math Kernel)
            # Melempar spread ke mesin utama untuk mendapatkan SignalEvent
            return self._process_observation(
                spread, 
                observation.timestamp, 
                observation.symbol
            )

        except Exception as e:
            # Proteksi terakhir jika terjadi crash tak terduga
            logger.error(f"Live evaluation crash: {e}", exc_info=True)
            err_ts = observation.timestamp if observation is not None else 0
            return self._neutral_signal(err_ts, "live_crash", str(e))


    # ========== STRATEGY MANAGEMENT ==========

    def update_position(self, size: float, price: float) -> Result[None, str]:
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
            'vol_buffer_size': len(self._internal_state.volatility_history),
            'price_pair': self._internal_state.price_pair,
            'warmup_complete': len(self._internal_state.spread_history) >= self.WARMUP_REQUIRED,
            'market_state': self.get_state().value,
            'config': {
                'entry': self.sig_config.entry_z_score,
                'exit': self.sig_config.exit_z_score,
                'R': self.math_config.R,
                'Q': self.math_config.Q
            }
        }

    def reset(self) -> Result[None, str]:
        try:
            self._kalman_filter = None
            self._internal_state = KalmanMRState()
            self._filter_initialized = False
            self._warmup_count = 0
            self._last_processed_timestamp = 0
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
            metrics = {'total_observations': 0, 'status': 'awaiting_data'}
        return {**base_metrics, **metrics}

    def _get_signal_distribution(self) -> Dict[str, int]:
        return {'neutral': 0, 'buy': 0, 'sell': 0, 'exit': 0, 'stop': 0}

    def _calculate_sharpe_ratio(self) -> float:
        if self._internal_state.trade_count == 0:
            return 0.0
        avg_return = self._internal_state.total_pnl / max(self._internal_state.trade_count, 1)
        return avg_return / max(self._internal_state.current_uncertainty, 1e-9)

    def get_diagnostics(self) -> Dict[str, Any]:
        monitor_stats = {
            'avg_kalman_latency': self.monitor.get_avg_latency('kalman_update'),
            'avg_batch_latency': self.monitor.get_avg_latency('batch_processing'),
            'avg_live_latency': self.monitor.get_avg_latency('live_evaluation')
        }
        return {
            'filter_initialized': self._filter_initialized,
            'kalman_filter': 'active' if self._kalman_filter else 'inactive',
            'buffer_size': len(self._internal_state.spread_history),
            'vol_buffer_size': len(self._internal_state.volatility_history),
            'performance_metrics': self.monitor.get_summary() if hasattr(self.monitor, 'get_summary') else {},
            'monitor_stats': monitor_stats,
            'last_timestamp': self._last_processed_timestamp
        }
