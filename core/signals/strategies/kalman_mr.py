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
        safe_config = cast(KalmanConfig, self.math_config)

        self.sig_config = signal_config

        if math_config is not None:
            self.math_config = math_config
        else:
            # Default Safe Values
            self.math_config = KalmanConfig(
                R=0.1,
                Q=1e-5,
                initial_value=0.0,
                adaptation_mode=AdaptationMode.NIS_THRESHOLD
            )
        self.warmup_required = getattr(self.sig_config, 'warmup_ticks', self.sig_config.volatility_window)
        self._internal_state = KalmanMRState()
        self._kalman_filter = KalmanFactory.create_adaptive_filter(safe_config)
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

    def _process_observation(self, spread: float, timestamp: int) -> Result[SignalEvent, str]:
        """
        Proses observasi tunggal dengan modulasi Q berdasarkan volatilitas (Z‑score clipped 0‑3).
        """
        try:
            # --- VALIDASI INPUT ---
            if not isinstance(spread, (int, float)) or not np.isfinite(spread):
                return self._neutral_signal(
                    timestamp,
                    "invalid_spread",
                    f"Spread tidak valid: {spread}"
                )

            # --- CEK WARMUP DI AWAL ---
            is_warmup = len(self._internal_state.spread_history) < self.WARMUP_REQUIRED
            if len(self._internal_state.spread_history) == self.warmup_required:
                logging.info(f"🔥 Warmup complete after {self.warmup_required} observations")


            # --- DETEKSI GAP WAKTU ---
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
                        init_result.unwrap_err() or "Unknown initialization error"
                    )
                self._kalman_filter = cast(AdaptiveKalmanFilter, init_result.unwrap_err())
                self._filter_initialized = True

            if self._kalman_filter is None:
                return self._neutral_signal(
                    timestamp,
                    "kalman_null",
                    "Critical: Kalman Filter gagal diinisialisasi"
                )

            # --- UPDATE SPREAD HISTORY & HITUNG VOLATILITAS ---
            self._internal_state.spread_history.append(spread)
            window = self.sig_config.volatility_window
            if len(self._internal_state.spread_history) >= window:
                recent = self._internal_state.spread_history[-window:]
                current_vol = float(np.std(recent))
            else:
                current_vol = 0.0

            # --- SIMPAN VOLATILITAS KE BUFFER ---
            self._internal_state.volatility_history.append(current_vol)

            # --- HITUNG Z‑SCORE VOLATILITAS (JIKA BUFFER CUKUP) ---
            factor_q = 1.0
            z_vol_clipped = 0.0
            if len(self._internal_state.volatility_history) >= self.VOL_BUFFER_MIN:
                vol_arr = np.array(self._internal_state.volatility_history)
                mean_vol = np.mean(vol_arr)
                std_vol = np.std(vol_arr)
                if std_vol > 1e-12:
                    z_vol = (current_vol - mean_vol) / std_vol
                    # Clip hanya sisi positif: [0, 3] – Q tidak pernah dikecilkan
                    z_vol_clipped = np.clip(z_vol, 0.0, 3.0)
                    factor_q = 1.0 + self.VOL_SENSITIVITY * z_vol_clipped
                    if factor_q < 1.0:
                        logger.warning(...)
                        factor_q = 1.0

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

            # --- JIKA WARMUP, LANGSUNG RETURN NEUTRAL ---
            if is_warmup:
                return self._neutral_signal(
                    timestamp,
                    "warmup",
                    f"Warmup: {len(self._internal_state.spread_history)}/{self.WARMUP_REQUIRED}",
                    status="warmup"
                )

            # --- Jika sudah warmup, lanjutkan generate sinyal normal ---
            return match_result(
                update_result,
                on_ok=lambda state: self._generate_signal_from_state(state, spread, timestamp),
                on_err=lambda err: self._handle_kalman_error(err, spread, timestamp)
            )

        except Exception as e:
            logger.error(f"Unhandled exception in _process_observation: {e}", exc_info=True)
            return self._neutral_signal(
                timestamp,
                "unhandled_exception",
                str(e)
            )

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

            if not np.isfinite(float_y) or not np.isfinite(float_x):
                return Err(f"Non-finite price detected: {val_y}, {val_x}")

            return Ok(self._calculate_spread(val_y, val_x))
        except Exception as e:
            return Err(f"Extract failed: {str(e)}")

    def _extract_spread_from_dict(self, data: dict) -> Result[float, str]:
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
            spread_columns = ['spread', 'spread_DOGE', 'beta_DOGE_BTC', 'log_spread']
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
            estimate = float(state.x[0, 0])
            _ = float(state.P[0, 0])
            residual = spread - estimate

            # --- WARMUP CHECK ---
            if len(self._internal_state.spread_history) < self.WARMUP_REQUIRED:
                self._warmup_count += 1
                return Ok(SignalEvent(
                    timestamp=timestamp,
                    signal_type=SignalType.NEUTRAL,
                    strength=0.0,
                    symbol=self._internal_state.price_pair[0] if self._internal_state.price_pair else "UNKNOWN",
                    strategy_name=self.sig_config.name,
                    _metadata={"status": "warmup"}
                ))

            # --- Z-SCORE CALCULATION ---
            if len(self._internal_state.spread_history) >= self.sig_config.volatility_window:
                recent = self._internal_state.spread_history[-self.sig_config.volatility_window:]
                volatility = float(np.std(recent))
            else:
                volatility = float(np.std(self._internal_state.spread_history))
                
            volatility = max(volatility, 1e-9)
            zscore = residual / volatility
            self._internal_state.last_zscore = float(zscore)

            # ==========================================================
            # 🧠 THE SURGERY: STATE-AWARE SIGNAL ENGINE (STANDALONE)
            # ==========================================================
            # Kita gunakan enum last_signal sebagai memori internal sementara untuk R&D.
            # (Saat live nanti, ini bisa diganti dengan memanggil OMS Inventory)
            current_position = self._internal_state.last_signal 
            final_signal_type = SignalType.NEUTRAL
            
            entry_z = self.sig_config.entry_z_score
            exit_z = self.sig_config.exit_z_score
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
                    "volatility": float(volatility)
                }
            ))
            
        except Exception as e:
            logger.error(f"Signal generation crashed: {e}")
            return self._neutral_signal(timestamp, "signal_gen_error", str(e))


    def _determine_signal(self, zscore: float) -> Tuple[SignalType, float]:
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

    def _neutral_signal(self, timestamp: int, error_code: str, error_msg: str, status: str = "degraded") -> Result[SignalEvent, str]:
        """Helper: buat sinyal NEUTRAL dengan metadata error."""
        return Ok(SignalEvent(
            timestamp=timestamp,
            signal_type=SignalType.NEUTRAL,
            strength=0.0,
            symbol=self._internal_state.price_pair[0] if self._internal_state.price_pair else "UNKNOWN",
            strategy_name=self.sig_config.name,
            _metadata={
                "error": error_code,
                "error_detail": error_msg,
                "status": status
            }
        ))

    # ========== DUAL-PATH PROCESSING ==========

    def generate_signals(self, df: pd.DataFrame) -> Result[pd.DataFrame, str]:
        """Batch processing untuk backtest."""
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
            for _, row in df.iterrows():
                raw_ts = row['timestamp']
                ts_int: int = 0
                if isinstance(raw_ts, pd.Timestamp):
                    ts_int = int(raw_ts.timestamp() * 1000)
                elif isinstance(raw_ts, (int, float)):
                    ts_int = int(raw_ts)
                else:
                    try:
                        # FIX 1: Casting raw_ts ke str() memaksa linter melihat tipe data valid
                        ts_int = int(pd.Timestamp(str(raw_ts)).timestamp() * 1000)
                    except Exception:
                        ts_int = 0

                spread_result = self._extract_spread(row)
                if spread_result.is_err():
                    err_spread = spread_result.unwrap_err()
                    signals.append(SignalEvent(
                        timestamp=ts_int,
                        signal_type=SignalType.NEUTRAL,
                        strength=0.0,
                        _metadata={"error": str(err_spread) if err_spread is not None else "Extract error"}
                    ))
                    continue

                # FIX 2: Type Narrowing dengan assert sebelum float casting
                spread_val = spread_result.unwrap()
                assert spread_val is not None, "Spread cannot be None"
                spread = float(spread_val)
                
                # Proses observasi
                signal_result = self._process_observation(spread, ts_int)
                
                # FIX 3: Type Narrowing dan fallback untuk string pesan error
                if signal_result.is_err():
                    err_msg = signal_result.unwrap_err()
                    safe_err_str = str(err_msg) if err_msg is not None else "Unknown process error"
                    neutral_fallback = self._neutral_signal(ts_int, "process_err", safe_err_str)
                    
                    fallback_val = neutral_fallback.unwrap()
                    assert fallback_val is not None, "Fallback signal cannot be None"
                    signals.append(fallback_val)
                else:
                    sig_val = signal_result.unwrap()
                    assert sig_val is not None, "Signal cannot be None"
                    signals.append(sig_val)

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

    def evaluate_state(self, obs: Union[dict, MarketObservation]) -> Result[SignalEvent, str]:
        """Live processing untuk real‑time tick."""
        
        # --- FIX: Deklarasikan di luar try-block agar linter tidak panik ---
        observation: Optional[MarketObservation] = None
        
        try:
            if isinstance(obs, dict):
                import time
                ts = int(obs.get("timestamp", time.time() * 1000))
                observation = MarketObservation(timestamp=ts, data=obs)
            else:
                observation = obs

            if not observation.data:
                return self._neutral_signal(observation.timestamp, "empty_data", "Observation data kosong")

            # Deteksi Pair
            p_target = observation.data.get("close_DOGE", observation.data.get("close"))
            p_ref = observation.data.get("close_BTC")

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

            # --- CEK GAP WAKTU ---
            if self._last_processed_timestamp > 0:
                gap_ms = observation.timestamp - self._last_processed_timestamp
                if gap_ms > self.MAX_GAP_MS:
                    logger.warning(f"⚠️ Live gap {gap_ms/1000:.1f}s detected. Resetting filter.")
                    self.reset()
            self._last_processed_timestamp = observation.timestamp

            # --- PROSES SPREAD ---
            spread = self._calculate_spread(p_target_f, p_ref_f)
            return self._process_observation(spread, observation.timestamp)

        except Exception as e:
            logger.error(f"Live evaluation crash: {e}", exc_info=True)
            
            # --- FIX: Ambil timestamp dengan aman jika observation sudah terisi ---
            err_ts = observation.timestamp if observation is not None else 0
            
            return self._neutral_signal(
                err_ts,
                "live_crash",
                str(e)
            )

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
