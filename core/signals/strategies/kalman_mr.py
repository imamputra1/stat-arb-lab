"""
THE MOTHERBOARD: KALMAN MEAN REVERSION STRATEGY
Location: core/signals/strategies/kalman_mr.py
Role: Menghubungkan Data (HDD/IoT) ke Math Kernel (GPU) dengan MarketObservation.
"""

import numpy as np
import pandas as pd
from typing import Optional, Tuple, Dict, Any, List, Union
from dataclasses import dataclass, field
import logging

# IMPORT CHIP GPU (MATH KERNEL)
from core.math import (
    AdaptiveKalmanFilter, 
    KalmanConfig, 
    KalmanFactory,
    KalmanState,
    KalmanError
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

# ========== MAIN STRATEGY CLASS ==========

class KalmanMeanReversion(BaseStrategy):
    """
    [THE MOTHERBOARD]
    Implementation of Mean Reversion using Adaptive Kalman Filter.
    Connects Silver Lake (Batch) and Live Feed (Stream) to Math Kernel.
    
    Dual-Path Architecture:
    1. Research Path: generate_signals(df) -> Historical DataFrame processing
    2. Live Path: evaluate_state(obs) -> Real-time MarketObservation processing
    
    Features:
    - MarketObservation compatible dengan get_value() safe extraction
    - Dynamic price pair detection
    - Rolling volatility window untuk Z-Score calculation
    - Full Result pattern error handling
    """
    
    def __init__(self,
                 math_config: KalmanConfig,
                 signal_config: SignalConfig):
        """
        Initialize Motherboard dengan StrategyConfig yang terintegrasi.
        
        Args:
            config: StrategyConfig yang sudah terintegrasi Kalman + Signal params
        """
        super().__init__(name=signal_config.name, version=signal_config.version)
        
        self.math_config = math_config
        self.sig_config = signal_config

        # State management
        self._internal_state = KalmanMRState()
        self._kalman_filter: Optional[AdaptiveKalmanFilter] = None
        self._filter_initialized = False
        
        self._warmup_count = 0
        
        # Validasi konfigurasi
        validation = self.sig_config.validate()
        if validation.is_err():
            raise ValueError(f"Invalid signal config: {validation.unwrap_err()}")
        
        logger.info(f"Initialized KalmanMRStrategy: {self.name} v{self.version}")
    
    # ========== ABSTRACT METHOD IMPLEMENTATIONS ==========
    
    def _initialize_filter(self, initial_value: float) -> Result[AdaptiveKalmanFilter, str]:
        """Initialize the Kalman Filter mathematical kernel"""
        try:
            self.monitor.start_timer("filter_initialization")
            
            # Create Kalman configuration dari StrategyConfig
            runtime_conf = KalmanConfig(
                R=self.math_config.R,
                Q=self.math_config.Q,
                initial_value=initial_value,
                state_dim=self.math_config.state_dim,
                adaptation_mode=self.math_config.adaptation_mode
            )
            
            # Create filter menggunakan factory dengan Result pattern
            filter_result = KalmanFactory.create(runtime_conf)
            
            self.monitor.stop_timer("filter_initialization")

            # Gunakan match_result untuk handle match_result
            return filter_result

        except Exception as e:
            logger.error(f"Filter initialization error: {e}")
            return Err(f"Filter initialization error: {str(e)}")
    
    def _process_observation(self, spread: float, timestamp: int) -> Result[SignalEvent, str]:
        """
        [LOGIC GATE - KERNEL PROCESSING]
        Menerima Spread -> Tanya Kalman -> Hitung Z-Score -> Putuskan Sinyal.
        Outputnya PASTI Result[SignalEvent].
        """
        try:
            # Validate input
            if not np.isfinite(spread):
                return Err(f"Invalid spread value: {spread}")

            self._internal_state.observation_count +=1
            
            # Ensure filter is initialized
            if self._kalman_filter is None:
                init_result = self._initialize_filter(spread)
                if init_result.is_err():
                    return init_result.map(lambda _: None)
                self._kalman_filter = init_result.unwrap()
                self._filter_initialized = True
            
            # Process through mathematical kernel (GPU-ready)
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
            
            # Handle Kalman update result dengan pattern matching
            return match_result(
                update_result,
                on_ok=lambda state: self._generate_signal_from_state(state, spread, timestamp),
                on_err=lambda err: self._handle_kalman_error(err, spread, timestamp)
            )
            
        except Exception as e:
            logger.error(f"Observation processing failed: {e}")
            return Err(f"Observation processing failed: {str(e)}")
    
    def _extract_spread(self, data: Union[dict, pd.Series, MarketObservation]) -> Result[float, str]:
        """
        [VOLTAGE REGULATOR]
        Extract spread value dari berbagai format data dengan MarketObservation safety.
        
        Args:
            data: dict, pd.Series, atau MarketObservation
            
        Returns:
            Result[float, str]: Spread value atau error message
        """
        try:
            # Handle MarketObservation (prioritized)
            if isinstance(data, MarketObservation):
                return self._extract_spread_from_observation(data)
            
            # Handle dictionary (live observation)
            elif isinstance(data, dict):
                return self._extract_spread_from_dict(data)
            
            # Handle pandas Series (research DataFrame row)
            elif isinstance(data, pd.Series):
                return self._extract_spread_from_series(data)
            
            else:
                return Err(f"Unsupported data type: {type(data)}")
                
        except Exception as e:
            logger.error(f"Spread extraction failed: {e}")
            return Err(f"Spread extraction failed: {str(e)}")

    def _extract_spread_from_observation(self, obs: MarketObservation) -> Result[float, str]:
        """Extract spread dari MarketObservation"""
        try:
            # Detect price columns secara dinamis
            price_cols = [k for k in obs.data.keys() if k.startswith('close_')]
            
            if len(price_cols) < 2:
                # Fallback logic... (kode lama Anda ok di sini)
                return Err(f"Insufficient price data. Found: {list(obs.data.keys())}")
            
            col_y, col_x = price_cols[0], price_cols[1]
            
            # Update pair tracking
            if self._internal_state.price_pair == ("", ""):
                self._internal_state.price_pair = (col_y.replace('close_', ''), col_x.replace('close_', ''))
            
            # Get prices
            p_y = obs.get_value(col_y, float)
            p_x = obs.get_value(col_x, float)
            
            if p_y.is_err() or p_x.is_err():
                return Err("Failed to extract prices")
            
            # [FIX] Panggil _calculate_spread, jangan hitung sendiri!
            return Ok(self._calculate_spread(p_y.unwrap(), p_x.unwrap()))
            
        except Exception as e:
            return Err(f"Extract failed: {str(e)}")

    def _extract_spread_from_dict(self, data: dict) -> Result[float, str]:
        """Extract spread dari dictionary"""
        # Try multiple possible spread column names
        spread_columns = ['spread', 'spread_DOGE', 'beta_DOGE_BTC', 'log_spread']
        
        for col in spread_columns:
            if col in data:
                value = data[col]
                if isinstance(value, (int, float, np.number)):
                    return Ok(float(value))

        price_cols = [k for k in data.keys() if k.startswith('close_')]
        if len(price_cols) >= 2:
            try:
                p_y = float(data[price_cols[0]])
                p_x = float(data[price_cols[1]])
                # [FIX] Panggil _calculate_spread
                return Ok(self._calculate_spread(p_y, p_x))
            except: 
                pass
        return Err("No spread found")

    def _extract_spread_from_series(self, data: pd.Series) -> Result[float, str]:
        """
        [EXTRACTOR] Handle input dari DataFrame Row (pd.Series).
        Dipanggil saat Batch Processing (Backtest).
        """
        try:
            # 1. Cek jika kolom 'spread' sudah ada (pre-calculated)
            spread_columns = ['spread', 'spread_DOGE', 'beta_DOGE_BTC', 'log_spread']
            for col in spread_columns:
                if col in data.index:
                    val = data[col]
                    if pd.notnull(val): return Ok(float(val))
            
            # 2. Deteksi dinamis kolom harga (close_*)
            price_cols = [c for c in data.index if str(c).startswith('close_')]
            
            if len(price_cols) < 2:
                return Err(f"Insufficient price data in row. Found: {price_cols}")
            
            # Asumsi konvensi: Kolom pertama adalah Y (Dependent), kedua adalah X (Independent)
            col_y, col_x = price_cols[0], price_cols[1]
            
            # 3. Update Pair Info (sekali saja agar report cantik)
            if self._internal_state.price_pair == ("", ""):
                asset_y = str(col_y).replace('close_', '')
                asset_x = str(col_x).replace('close_', '')
                self._internal_state.price_pair = (asset_y, asset_x)
            
            # 4. Ambil nilai harga dengan aman
            p_y = data.get(col_y)
            p_x = data.get(col_x)
            
            # Cek validitas data (Anti-NaN/None)
            if p_y is None or p_x is None or pd.isnull(p_y) or pd.isnull(p_x):
                return Err("Price data contains NaNs or None")

            # 5. Panggil Voltage Regulator (Single Source of Truth)
            # [FIX UTAMA] Menggunakan _calculate_spread agar konsisten
            return Ok(self._calculate_spread(float(p_y), float(p_x)))
            
        except Exception as e:
            return Err(f"Series extraction failed: {str(e)}")

    def _calculate_spread(self, price_y: float, price_x: float) -> float:
        """
        [VOLTAGE REGULATOR] - SINGLE SOURCE OF TRUTH
        Mengubah dua harga mentah menjadi satu sinyal Spread.
        Rumus: Log(Y) - Beta * Log(X)
        """
        # Gunakan Log-Prices agar statistik lebih stasioner
        log_y = np.log(price_y)
        log_x = np.log(price_x)
        
        # [FIX] Gunakan self.sig_config (bukan self.config yang tidak ada)
        beta = self.sig_config.hedge_ratio
        
        # Spread calculation
        spread = log_y - (beta * log_x)
        return spread

    # ========== SIGNAL GENERATION LOGIC ==========
    
    def _generate_signal_from_state(
        self, 
        state: KalmanState, 
        spread: float, 
        timestamp: int
    ) -> Result[SignalEvent, str]:
        """[DECISION MATRIX]"""
        try:
            estimate = float(state.x[0, 0])
            uncertainty = float(state.P[0, 0])
            residual = spread - estimate
            
            self._internal_state.spread_history.append(residual)
            
            # [FIX] Ganti self.config -> self.sig_config (pastikan ada di SignalConfig)
            # Jika rolling_window tidak ada di config, hardcode atau tambahkan ke SignalConfig
            WINDOW_SIZE = 50 # Atau self.sig_config.volatility_window
            if len(self._internal_state.spread_history) > WINDOW_SIZE:
                self._internal_state.spread_history.pop(0)
            
            # [FIX] Warmup check
            MIN_SAMPLES = 20
            if len(self._internal_state.spread_history) < MIN_SAMPLES:
                self._warmup_count += 1
                return Ok(SignalEvent(
                    timestamp=timestamp, 
                    signal_type=SignalType.NEUTRAL, 
                    strength=0.0,
                    metadata={"status": "warmup"}
                ))
            
            # Z-Score Calculation
            volatility = np.std(self._internal_state.spread_history)
            if volatility < 1e-9: volatility = 1e-9
            
            zscore = residual / volatility
            
            # Update Internal State
            self._internal_state.last_zscore = zscore
            self._internal_state.current_estimate = estimate
            self._internal_state.current_uncertainty = uncertainty
            
            # Generate Signal
            signal_type, strength = self._determine_signal(zscore)            
            # Build metadata
            metadata = {
                "estimate": estimate,
                "uncertainty": uncertainty,
                "zscore": zscore,
                "spread": spread,
                "volatility": volatility,
                "residual": residual,
                "position_size": self._internal_state.position_size,
                "trade_count": self._internal_state.trade_count,
                "warmup_complete": True,
                "price_pair": self._internal_state.price_pair
            }
            
            return Ok(SignalEvent(
                timestamp=timestamp,
                signal_type=signal_type, #
                strength=strength,
                metadata=metadata # (variable 'metadata' must be defined before use)
            ))
            
        except Exception as e:
            return Err(f"Signal gen failed: {str(e)}")

    
    def _determine_signal(self, zscore: float) -> Tuple[SignalType, float]:
        """
        [TRADING LOGIC] - REVISI ENUM (BUY/SELL)
        Menggunakan parameter dari self.sig_conf
        """
        entry_z = self.sig_config.entry_z_score
        exit_z = self.sig_config.exit_z_score
        stop_z = self.sig_config.stop_loss_z
        
        abs_z = abs(zscore)
        
        # 1. CIRCUIT BREAKER / STOP LOSS (Prioritas Tertinggi)
        # Jika volatilitas terlalu gila (> 4 sigma), matikan sinyal (STOP).
        if abs_z > stop_z:
            return SignalType.STOP, zscore 
            
        # 2. EXIT LOGIC (Mean Reversion)
        # Jika harga sudah kembali ke wajar (dekat mean), tutup posisi.
        if abs_z < exit_z:
            return SignalType.EXIT, zscore
            
        # 3. ENTRY LOGIC (The Alpha)
        
        # KASUS A: Z-Score Positif Tinggi (> Entry Threshold)
        # Artinya: Spread "Mahal". Harga Y terlalu tinggi vs X.
        # Aksi: Jual Y, Beli X -> SELL SPREAD.
        if zscore > entry_z:
            return SignalType.SELL, zscore  # <--- FIXED: BUY/SELL ENUM
            
        # KASUS B: Z-Score Negatif Dalam (< -Entry Threshold)
        # Artinya: Spread "Murah". Harga Y terlalu rendah vs X.
        # Aksi: Beli Y, Jual X -> BUY SPREAD.
        if zscore < -entry_z:
            return SignalType.BUY, zscore   # <--- FIXED: BUY/SELL ENUM
            
        # 4. NO MAN'S LAND (HOLD)
        # Area nanggung antara Exit dan Entry. Jangan ngapa-ngapain.
        return SignalType.NEUTRAL, zscore

    
    def _handle_kalman_error(self, error: KalmanError, spread: float, timestamp: int) -> Result[SignalEvent, str]:
        """Handle Kalman filter errors gracefully"""
        self._internal_state.consecutive_errors += 1
        
        logger.warning(f"Kalman error #{self._internal_state.consecutive_errors}: {error}")
        
        # Jika terlalu banyak error berturut-turut, reset filter
        if self._internal_state.consecutive_errors > 3:
            logger.error("Too many consecutive Kalman errors, resetting filter")
            self.reset()
        
        # Return neutral signal dengan error metadata
        metadata = {
            "error": str(error),
            "spread": spread,
            "consecutive_errors": self._internal_state.consecutive_errors,
            "timestamp": timestamp,
            "filter_state": "degraded"
        }
        
        signal = SignalEvent(
            timestamp=timestamp,
            signal_type=SignalType.NEUTRAL,
            strength=0.0,
            metadata=metadata
        )
        
        return Ok(signal)
    
    # ========== DUAL-PATH PROCESSING ==========
    
    def generate_signals(self, df: pd.DataFrame) -> Result[pd.DataFrame, str]:
        """
        [PORT A: HARD DISK INPUT - Research/Backtest]
        Menerima DataFrame Silver Lake -> Return DataFrame dengan Sinyal.
        
        Args:
            df: DataFrame dengan kolom timestamp dan price columns (close_*)
            
        Returns:
            DataFrame asli + signal columns
        """
        try:
            # Start monitoring
            self.monitor.start_timer("batch_processing")
            
            # Validate input DataFrame
            if df.empty:
                return Err("Empty DataFrame provided")
            
            if 'timestamp' not in df.columns:
                return Err("DataFrame must have 'timestamp' column")
            
            # Deteksi kolom harga
            price_cols = [col for col in df.columns if col.startswith('close_')]
            if len(price_cols) < 2:
                return Err(f"Need at least 2 price columns. Found: {price_cols}")
            
            # Reset state untuk batch baru
            self._internal_state = KalmanMRState()
            self._kalman_filter = None
            self._filter_initialized = False
            
            # Process each row
            signals = []
            
            for idx, row in df.iterrows():
                timestamp = row['timestamp']
                # Convert timestamp to milliseconds integer jika perlu
                if hasattr(timestamp, 'timestamp'):
                    timestamp = int(timestamp.timestamp() * 1000)
                
                # Extract spread
                spread_result = self._extract_spread(row)
                
                if spread_result.is_err():
                    # Jika gagal extract spread, catat error dan continue
                    error_msg = spread_result.unwrap_err()
                    logger.debug(f"Row {idx}: {error_msg}")
                    
                    signals.append(SignalEvent(
                        timestamp=timestamp,
                        signal_type=SignalType.NEUTRAL,
                        strength=0.0,
                        metadata={"error": error_msg}
                    ))
                    continue
                
                spread = spread_result.unwrap()
                
                # Process observation
                signal_result = self._process_observation(spread, timestamp)
                
                if signal_result.is_ok():
                    signals.append(signal_result.unwrap())
                else:
                    error_msg = signal_result.unwrap_err()
                    logger.debug(f"Row {idx} processing error: {error_msg}")
                    
                    signals.append(SignalEvent(
                        timestamp=timestamp,
                        signal_type=SignalType.NEUTRAL,
                        strength=0.0,
                        metadata={"error": error_msg}
                    ))
            
            # Create results DataFrame
            result_df = df.copy()
            
            # Add signal columns
            result_df['signal_type'] = [s.signal_type.value for s in signals]
            result_df['signal_type_name'] = [s.signal_type.name for s in signals]
            result_df['signal_strength'] = [s.strength for s in signals]
            result_df['signal_metadata'] = [s.metadata for s in signals]
            
            # Add derived columns
            result_df['z_score'] = [s.metadata.get('zscore', 0.0) for s in signals]

            result_df['spread_val'] = [s.metadata.get('spread', 0.0) for s in signals]
            result_df['estimate'] = [s.metadata.get('estimate', 0.0) for s in signals]
            # Update performance metrics
            duration = self.monitor.stop_timer("batch_processing")
            
            logger.info(f"Batch processing complete: {len(df)} rows, {duration:.2f}ms")
            
            return Ok(result_df)
            
        except Exception as e:
            logger.error(f"Batch processing failed: {e}")
            return Err(f"Batch processing failed: {str(e)}")
    
    def evaluate_state(self, obs: Union[dict, MarketObservation]) -> Result[SignalEvent, str]:
        """
        [PORT B: IOT SENSOR INPUT - Live Trading]
        Menerima Single Observation -> Return Single SignalEvent.
        
        Args:
            obs: dict atau MarketObservation dengan data market
            
        Returns:
            SignalEvent dengan trading decision
        """
        try:
            # Start monitoring
            self.monitor.start_timer("live_evaluation")
            
            # Convert dict ke MarketObservation jika perlu
            if isinstance(obs, dict):
                timestamp = obs.get('timestamp', 0)
                obs = MarketObservation(timestamp=timestamp, data=obs, source="live")
            
            # Validate observation
            if not isinstance(obs, MarketObservation):
                return Err("Observation must be dict or MarketObservation")
            
            # Extract spread
            spread_result = self._extract_spread(obs)
            if spread_result.is_err():
                return Err(f"Failed to extract spread: {spread_result.unwrap_err()}")
            
            spread = spread_result.unwrap()
            
            # Process observation
            signal_result = self._process_observation(spread, obs.timestamp) 
            # Update performance
            duration = self.monitor.stop_timer("live_evaluation")
            
            logger.debug(f"Live evaluation: {obs.timestamp}, spread={spread:.4f}, duration={duration:.2f}ms")
            
            return signal_result
            
        except Exception as e:
            logger.error(f"Live evaluation failed: {e}")
            return Err(f"Live evaluation failed: {str(e)}")
    
    # ========== STRATEGY MANAGEMENT ==========
    
    def update_position(self, size: float, price: float) -> Result[None, str]:
        """Update current position size (called by execution engine)"""
        try:
            # Validate position size
            max_position = getattr(self.sig_config, 'max_position', 1.0)
            if abs(size) > max_position:
                return Err(f"Position size {size} exceeds maximum {max_position}")
            
            # Calculate P&L dari position change (simplified)
            old_position = self._internal_state.position_size
            if old_position != 0:
                # Asumsi P&L proportional dengan price change
                # Note: Ini simplified - real implementation akan lebih kompleks
                pnl_change = (size - old_position) * price * 0.01  # 1% movement
                self._internal_state.total_pnl += pnl_change
            
            # Update position
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
            'warmup_complete': len(self._internal_state.spread_history) >= 20,
            'market_state': self.get_state().value,
            'config': {
                # [FIX] Ambil dari config yang benar
                'entry': self.sig_config.entry_z_score,
                'exit': self.sig_config.exit_z_score,
                'R': self.math_config.R,
                'Q': self.math_config.Q
            }        }
    
    def reset(self) -> Result[None, str]:
        """Reset strategy ke initial state"""
        try:
            self._kalman_filter = None
            self._internal_state = KalmanMRState()
            self._filter_initialized = False
            self._warmup_count = 0

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
        # Note: Kalman filter saat ini synchronous
        # Untuk true async, perlu async Kalman implementation
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
        
        # Calculate additional metrics dari history
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
                'sharpe_ratio': self._calculate_sharpe_ratio()            }
        else:
            metrics = {
                'total_observations': 0,
                'status': 'awaiting_data'
            }
        
        return {**base_metrics, **metrics}
    
    def _get_signal_distribution(self) -> Dict[str, int]:
        """Calculate distribution of signal types"""
        # Note: Implementasi lengkap akan track history signals
        return {
            'neutral': 0,
            'buy': 0,
            'sell': 0,
            'exit': 0,
            'stop': 0
        }
    
    def _calculate_sharpe_ratio(self) -> float:
        """Calculate Sharpe ratio dari P&L history"""
        # Simplified - real implementation akan lebih kompleks
        if self._internal_state.trade_count == 0:
            return 0.0
        
        # Asumsi risk-free rate = 0
        avg_return = self._internal_state.total_pnl / max(self._internal_state.trade_count, 1)
        return avg_return / max(self._internal_state.current_uncertainty, 1e-9)
    
    def get_diagnostics(self) -> Dict[str, Any]:
        """Get diagnostic information untuk debugging"""
        # [FIX] Ambil stats dari monitor yang benar
        monitor_stats = {
            'avg_kalman_latency': self.monitor.get_avg_latency('kalman_update'),
            'avg_batch_latency': self.monitor.get_avg_latency('batch_processing'),
            'avg_live_latency': self.monitor.get_avg_latency('live_evaluation')
        }
        
        return {
            'filter_initialized': self._filter_initialized,
            'kalman_filter': 'active' if self._kalman_filter else 'inactive',
            'buffer_size': len(self._internal_state.spread_history),
            # [FIX] Ganti performance_metrics dengan summary dari monitor
            'performance_metrics': self.monitor.get_summary() if hasattr(self.monitor, 'get_summary') else {},
            'monitor_stats': monitor_stats
        }
