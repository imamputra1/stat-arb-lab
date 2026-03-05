"""
OrcaCore Adaptive Kalman Filter (AKF) with Rust-style Monadic Error Handling
Pure stateless mathematical implementation
"""

import numpy as np
import numpy.typing as npt
from dataclasses import dataclass, field
from typing import Protocol, TypeAlias, Optional, TypeVar, Callable, final, Union
from enum import Enum
import datetime as dt

# Import Result Pattern dari shared
from core.shared.result import Result, Ok, Err, match_result, safe_async

# Type aliases
T = TypeVar('T')
E = TypeVar('E', bound=Exception)
FloatMatrix: TypeAlias = npt.NDArray[np.float64]
StateVector: TypeAlias = FloatMatrix
CovarianceMatrix: TypeAlias = FloatMatrix
KalmanGain: TypeAlias = FloatMatrix

class KalmanError(Exception):
    """Base error untuk semua failure di Kalman Filter"""
    pass

class SingularMatrixError(KalmanError):
    """Error ketika matrix singular/tidak invertible"""
    pass

class NumericalStabilityError(KalmanError):
    """Error ketika ada masalah numerical stability"""
    pass

class AdaptationMode(Enum):
    """Mode adaptasi dengan trade-off berbeda"""
    NONE = "none"
    NIS_THRESHOLD = "nis"  # Normalized Innovation Squared
    EXPONENTIAL = "exp"    # Exponential adaptation
    ROBUST = "robust"      # Robust Kalman filter
    HYBRID = "hybrid"

@dataclass(frozen=True)
class KalmanConfig:
    """Immutable configuration - pure value object"""
    R: float = field(metadata={"description": "Measurement noise variance"})
    Q: float = field(metadata={"description": "Process noise baseline"})
    initial_value: float
    state_dim: int = 2
    shock_threshold: float = 4.0
    max_boost_factor: float = 10.0
    min_lambda: float = 0.8
    max_lambda: float = 1.0
    adaptation_mode: AdaptationMode = AdaptationMode.NIS_THRESHOLD
    
    def __post_init__(self):
        """Fast validation menggunakan monadic pattern"""
        validations = [
            (self.R > 0, "R must be positive"),
            (self.Q > 0, "Q must be positive"),
            (self.state_dim > 0, "state_dim must be positive"),
            (self.min_lambda > 0, "lambda must be positive"),
        ]
        
        for condition, error_msg in validations:
            if not condition:
                raise ValueError(error_msg)

@dataclass(frozen=True)
class KalmanState:
    """Immutable state snapshot - pure monadic container"""
    x: StateVector
    P: CovarianceMatrix
    K: KalmanGain
    Q_adaptive: CovarianceMatrix
    innovation: float = 0.0
    nis: float = 0.0
    timestamp: dt.datetime = field(default_factory=lambda: dt.datetime.now(dt.timezone.utc))
    
    def flat_map(self, func: Callable[['KalmanState'], Result[T, E]]) -> Result[T, E]:
        """Monadic flatMap operation"""
        return func(self)
    
    def map(self, func: Callable[['KalmanState'], T]) -> T:
        """Monadic map operation"""
        return func(self)

# ============================================================================
# MONADIC KALMAN FILTER INTERFACE
# ============================================================================

class KalmanMonad(Protocol[T]):
    """Protocol untuk monadic Kalman operations"""
    def bind(self, func: Callable[[KalmanState], Result[T, KalmanError]]) -> Result[T, KalmanError]: ...
    def and_then(self, func: Callable[[KalmanState], 'KalmanMonad[T]']) -> 'KalmanMonad[T]': ...

@final
class AdaptiveKalmanFilter:
    """
    Production-grade AKF dengan Rust-style monadic error handling.
    
    Prinsip:
    1. Semua operations return Result[T, E]
    2. Tidak ada exception yang dilempar
    3. Pure functions sebanyak mungkin
    4. Immutable state transitions
    """
    
    def __init__(self, config: KalmanConfig):
        self.config = config
        self._validate_config(config)

        self.n_states = config.state_dim
        
        # State matrices
        self.F = np.eye(config.state_dim, dtype=np.float64)
        
        self.H = np.array([[1.0, 0.0]], dtype=np.float64)
        self.base_Q = np.eye(config.state_dim, dtype=np.float64) * config.Q
        self.R = np.array([[config.R]], dtype=np.float64)
        
        # Initial state
        self.x = np.array([[config.initial_value], [0.0]], dtype=np.float64)
        self.P = np.eye(config.state_dim, dtype=np.float64)
        
        # Cache untuk performance
        self.K = np.zeros((config.state_dim, 1), dtype=np.float64)
        self.last_innovation = 0.0
        self.last_nis = 0.0
        self._H_T = self.H.T
        self._I = np.eye(config.state_dim, dtype=np.float64)
        
        # Track history untuk rollback capability
        self._history: list[KalmanState] = []
    
    # =========================================================================
    # INTERNAL CONTEXT (Data Carrier for Monadic Chain)
    # =========================================================================
    @dataclass
    class _UpdateContext:
        """Helper untuk membawa data antar-step di Monadic Chain"""
        x_pred: FloatMatrix
        P_pred: FloatMatrix
        Q_used: FloatMatrix
        y: Optional[FloatMatrix] = None  # Innovation
        S: Optional[FloatMatrix] = None  # Innovation Covariance
        innovation: float = 0.0
        nis: float = 0.0

    # =========================================================================
    # PUBLIC UPDATE METHOD
    # =========================================================================

    def update(self, 
               z: float, 
               lambda_factor: float = 1.0,
               adapt: bool = True) -> Result[KalmanState, KalmanError]:
        """
        Monadic update operation (Unrolled for strict Type Checking).
        Atomic & Transactional: State self.x dan self.P hanya berubah jika semua step sukses.
        """
        try:
            # 1. Validasi Input
            validation = self._validate_input(z, lambda_factor)
            if validation.is_err():
                err = validation.unwrap_err()
                return Err(err if err is not None else KalmanError("Validation error"))
            
            # 2. Predict Step
            predict_res = self._predict(lambda_factor)
            if predict_res.is_err():
                err = predict_res.unwrap_err()
                return Err(err if err is not None else KalmanError("Predict error"))
            ctx = predict_res.unwrap()
            assert ctx is not None, "Context cannot be None"
            
            # 3. Compute Innovation
            innov_res = self._compute_innovation(z, ctx)
            if innov_res.is_err():
                err = innov_res.unwrap_err()
                return Err(err if err is not None else KalmanError("Innovation error"))
            ctx = innov_res.unwrap()
            assert ctx is not None, "Context cannot be None"
            
            # 4. Adapt Q (Jika diaktifkan)
            if adapt:
                adapt_res = self._adapt_Q(ctx)
                if adapt_res.is_err():
                    err = adapt_res.unwrap_err()
                    return Err(err if err is not None else KalmanError("Adapt Q error"))
                ctx = adapt_res.unwrap()
                assert ctx is not None, "Context cannot be None"
                
            # 5. Finalize Update
            final_res = self._finalize_update(ctx)
            if final_res.is_err():
                err = final_res.unwrap_err()
                return Err(err if err is not None else KalmanError("Finalize error"))
            state = final_res.unwrap()
            assert state is not None, "State cannot be None"
            
            # Sukses total!
            return Ok(state)
            
        except Exception as e:
            return Err(KalmanError(f"Unexpected panic in update chain: {str(e)}"))

    # =========================================================================
    # PRIVATE MONADIC STEPS
    # =========================================================================

    def _validate_input(self, z: float, lambda_factor: float) -> Result[bool, KalmanError]:
        """Step 0: Pre-flight check"""
        if not np.isfinite(z):
            return Err(NumericalStabilityError(f"Input measurement is not finite: {z}"))
        if not (0.0 < lambda_factor <= 1.0):
            return Err(KalmanError(f"Invalid lambda_factor: {lambda_factor}. Must be (0, 1]."))
        return Ok(True)

    def _predict(self, lambda_factor: float) -> Result['_UpdateContext', KalmanError]:
        """Step 1: Predict State & Covariance"""
        try:
            # x(k|k-1) = F * x(k-1|k-1)
            x_pred = self.F @ self.x
            
            # P(k|k-1) = (F * P * F.T) / lambda + Q
            # Fading Memory implementation
            P_pred = (self.F @ self.P @ self.F.T) / lambda_factor + self.base_Q
            
            return Ok(self._UpdateContext(
                x_pred=x_pred, 
                P_pred=P_pred, 
                Q_used=self.base_Q.copy()
            ))
        except Exception as e:
            return Err(KalmanError(f"Prediction step failed: {str(e)}"))

    def _compute_innovation(self, z: float, ctx: '_UpdateContext') -> Result['_UpdateContext', KalmanError]:
        """Step 2: Compute Innovation (y) and Covariance (S)"""
        try:
            # Format z ke matrix (1,1)
            z_mat = np.array([[z]], dtype=np.float64)
            
            # y = z - H * x_pred
            y = z_mat - (self.H @ ctx.x_pred)
            
            # S = H * P_pred * H.T + R
            S = (self.H @ ctx.P_pred @ self.H.T) + self.R
            
            # Cek Singularitas S
            det_S = np.linalg.det(S)
            if np.abs(det_S) < 1e-12:
                return Err(SingularMatrixError("S matrix is singular (non-invertible)"))
            
            # Update Context
            ctx.y = y
            ctx.S = S
            ctx.innovation = float(y[0, 0])
            return Ok(ctx)
        except Exception as e:
            return Err(KalmanError(f"Innovation step failed: {str(e)}"))

    def _adapt_Q(self, ctx: '_UpdateContext') -> Result['_UpdateContext', KalmanError]:
        """
        [STEP 3] Adaptive Q Injection (Shock Absorber).
        """
        try:
            # --- TUNTUTAN LINTER: JAMINAN BAHWA NILAI TIDAK NONE ---
            assert ctx.S is not None, "S matrix cannot be None in adapt_Q"
            assert ctx.y is not None, "y matrix cannot be None in adapt_Q"
            # -------------------------------------------------------

            # 1. Hitung NIS: y.T * inv(S) * y
            inv_S = np.linalg.inv(ctx.S)
            nis = (ctx.y.T @ inv_S @ ctx.y)[0, 0]
            ctx.nis = float(nis)
            
            # Threshold Statistical (Chi-square distribution).
            SHOCK_THRESHOLD = self.config.shock_threshold
            
            # Jika terdeteksi Shock, lakukan 'Q-Boosting'
            if nis > SHOCK_THRESHOLD:
                scale = max(1.0, nis / 2.0)
                scale = min(scale, 50.0) 
                
                Q_boost = self.base_Q * scale
                
                ctx.P_pred = ctx.P_pred - self.base_Q + Q_boost
                ctx.Q_used = Q_boost
                
                # CRITICAL: Recalculate S (Innovation Covariance)
                ctx.S = (self.H @ ctx.P_pred @ self.H.T) + self.R
                
                # Re-check singularity setelah update S
                if np.abs(np.linalg.det(ctx.S)) < 1e-12:
                     return Err(SingularMatrixError("S matrix became singular after Adaptive Q boost"))

            return Ok(ctx)

        except np.linalg.LinAlgError:
            return Err(SingularMatrixError("Linear Algebra failed during Adaptation step"))
        except Exception as e:
            return Err(KalmanError(f"Unexpected error in Adaptive Q logic: {str(e)}"))

    def _finalize_update(self, ctx: '_UpdateContext') -> Result[KalmanState, KalmanError]:
        """Step 4: Calculate Gain, Update State & Save to Self"""
        try:
            # --- 1. TYPE NARROWING (Memuaskan Linter) ---
            assert ctx.S is not None, "S matrix missing"
            assert ctx.y is not None, "y matrix missing"
            # --------------------------------------------

            # Kalman Gain: K = P_pred * H.T * inv(S)
            K = ctx.P_pred @ self.H.T @ np.linalg.inv(ctx.S)
            
            # State Update: x = x_pred + K * y
            new_x = ctx.x_pred + (K @ ctx.y)
            
            # Covariance Update (Joseph Form for Stability)
            # P = (I - KH)P(I - KH).T + KRK.T
            # Ini menjamin P tetap Positive Definite
            identity_mat = np.eye(self.n_states)  # <-- 2. FIX E741: Ganti nama 'I' jadi 'identity_mat'
            I_KH = identity_mat - (K @ self.H)
            new_P = (I_KH @ ctx.P_pred @ I_KH.T) + (K @ self.R @ K.T)
            
            # --- TRANSACTION COMMIT ---
            # Baru di sini kita ubah state object (self)
            self.x = new_x
            self.P = new_P

            # Simpan K, innovation, dan nis ke instance variable
            self.K = K.copy()
            self.last_innovation = ctx.innovation
            self.last_nis = ctx.nis

            # Return Immutable Snapshot
            return Ok(KalmanState(
                x=new_x.copy(),
                P=new_P.copy(),
                K=K.copy(),
                Q_adaptive=ctx.Q_used.copy(),
                innovation=ctx.innovation,
                nis=ctx.nis,
                timestamp=dt.datetime.now(dt.timezone.utc)  # <-- 3. FIX DATETIME SESUAI FORMAT AWAL
            ))
            
        except Exception as e:
            return Err(KalmanError(f"Final update step failed: {str(e)}"))


    # ============================================================================
    # MONADIC QUERY OPERATIONS
    # ============================================================================
    
    def query_state(self) -> Result[KalmanState, KalmanError]:
        """Monadic query untuk current state"""
        try:
            state = KalmanState(
                x=self.x.copy(),
                P=self.P.copy(),
                K=self.K.copy(),  # Placeholder
                Q_adaptive=self.base_Q.copy(),
                innovation=self.last_innovation,
                nis=self.last_nis
            )
            return Ok(state)
        except Exception as e:
            return Err(KalmanError(f"Query failed: {str(e)}"))
    
    def get_estimate(self) -> Result[float, KalmanError]:
        """Monadic get current estimate"""
        return self.query_state().map(lambda s: float(s.x[0, 0]))
    
    def get_uncertainty(self) -> Result[float, KalmanError]:
        """Monadic get current uncertainty"""
        return self.query_state().map(lambda s: float(s.P[0, 0]))
    
    def get_trend(self) -> Result[float, KalmanError]:
        """Monadic get current trend"""
        return self.query_state().map(lambda s: float(s.x[1, 0]))
    
    # ============================================================================
    # UTILITY METHODS
    # ============================================================================
    
    def _validate_config(self, config: KalmanConfig) -> None:
        """Internal validation"""
        if config.state_dim != 2:
            raise ValueError("Only 2D state (level, trend) supported")
    
    @property
    def current_estimate(self) -> float:
        """Property untuk compatibility - gunakan get_estimate() untuk monadic version"""
        return float(self.x[0, 0])
    
    @property
    def uncertainty(self) -> float:
        """Property untuk compatibility"""
        return float(self.P[0, 0])
    
    def reset(self, initial_value: float) -> None:
        """Reset filter ke initial state"""
        self.x = np.array([[initial_value], [0.0]], dtype=np.float64)
        self.P = np.eye(self.config.state_dim, dtype=np.float64)
        self._history.clear()

# ============================================================================
# MONADIC FACTORY PATTERN
# ============================================================================

class KalmanFactory:
    """Factory dengan monadic error handling"""
    
    @staticmethod
    def create(config: KalmanConfig) -> Result[AdaptiveKalmanFilter, str]:
        """Monadic factory method"""
        try:
            return Ok(AdaptiveKalmanFilter(config))
        except Exception as e:
            return Err(f"Failed to create Kalman filter: {str(e)}")
    
    @staticmethod
    def create_from_params(R: float, Q: float, initial_value: float, **kwargs) -> Result[AdaptiveKalmanFilter, str]:
        """Monadic factory dari parameters"""
        try:
            config = KalmanConfig(
                R=R,
                Q=Q,
                initial_value=initial_value,
                **{k: v for k, v in kwargs.items() if hasattr(KalmanConfig, k)}
            )
            return KalmanFactory.create(config)
        except Exception as e:
            return Err(f"Invalid parameters: {str(e)}")
    
    @staticmethod
    def create_fading_memory(R: float, Q: float, initial_value: float) -> Result[AdaptiveKalmanFilter, str]:
        """Factory untuk filter dengan fading memory default"""
        return KalmanFactory.create_from_params(
            R=R, Q=Q, initial_value=initial_value,
            min_lambda=0.9, max_lambda=0.99
        )
    
    @staticmethod
    def create_robust(R: float, Q: float, initial_value: float) -> Result[AdaptiveKalmanFilter, str]:
        """Factory untuk robust filter"""
        return KalmanFactory.create_from_params(
            R=R, Q=Q, initial_value=initial_value,
            adaptation_mode=AdaptationMode.ROBUST,
            shock_threshold=9.0  # 3-sigma
        )

    @staticmethod
    def create_adaptive_filter(config: KalmanConfig) -> Result[AdaptiveKalmanFilter, str]:
        """
        Factory method khusus untuk membuat AdaptiveKalmanFilter.
        Menerima konfigurasi lengkap (termasuk initial_value) dan mengembalikan Result.
        """
        # Validasi dasar: pastikan mode adaptasi ditentukan (opsional, tergantung desain)
        if config.adaptation_mode is None:
            return Err("Adaptation mode must be specified for adaptive filter")
        
        # Jika perlu, tambahkan logika pemilihan tipe filter berdasarkan konfigurasi
        # Misalnya: jika state_dim > 2, gunakan implementasi berbeda
        
        # Delegasikan ke method create yang sudah ada (yang mengembalikan Result)
        return KalmanFactory.create(config)

# ============================================================================
# MONADIC BATCH PROCESSOR
# ============================================================================

class KalmanBatchProcessor:
    """
    Batch processor dengan monadic error handling.
    Mendukung rollback dan recovery.
    """
    
    def __init__(self, filter_result: Result[AdaptiveKalmanFilter, str]):
        self.filter_result = filter_result
        self.states: list[KalmanState] = []
        self.errors: list[tuple[int, str]] = []  # (index, error_message)

    def process_batch(self, 
                     measurements: list[float],
                     lambda_factors: Optional[list[float]] = None) -> Result[list[KalmanState], str]:
        """
        Process batch secara monadic.
        Jika ada error, semua state sejak error akan di-discard.
        """
        # --- FIX ERROR 1: Berikan Fallback String ---
        if self.filter_result.is_err():
            err = self.filter_result.unwrap_err()
            return Err(err if err is not None else "Unknown filter initialization error")
        # --------------------------------------------
        
        filter_instance = self.filter_result.unwrap()
        
        # --- FIX ERROR 2: Type Narrowing (Assertion) ---
        assert filter_instance is not None, "Filter instance cannot be None"
        # -----------------------------------------------
       
        if lambda_factors is None:
            lambda_factors = [1.0] * len(measurements)
        
        for i, (z, lambda_factor) in enumerate(zip(measurements, lambda_factors)):
            result = filter_instance.update(z, lambda_factor)
            
            # Pattern matching untuk handle result
            def handle_success(state: KalmanState) -> KalmanState:
                self.states.append(state)
                return state
            
            def handle_error(error: KalmanError) -> KalmanError:
                self.errors.append((i, str(error)))
                return error
            
            match_result(result, handle_success, handle_error)
            
            # Jika error, stop processing dan return partial results dengan error
            if result.is_err():
                return Err(f"Batch processing failed at index {i}: {self.errors[-1][1]}")

        return Ok(self.states[-len(measurements):])

    def rollback(self, steps: int = 1) -> Result[None, str]:
        """Monadic rollback operation"""
        if self.filter_result.is_err():
            return self.filter_result.map(lambda _: None)
        
        if steps > len(self.states):
            return Err(f"Cannot rollback {steps} steps, only {len(self.states)} available")
        
        # Rollback states
        self.states = self.states[:-steps]
        
        # --- FIX: Ekstrak dan berikan Type Narrowing (Assertion) ---
        filter_instance = self.filter_result.unwrap()
        assert filter_instance is not None, "Filter instance cannot be None in rollback"
        # -----------------------------------------------------------
        
        # Rollback filter internal state
        if self.states:
            last_state = self.states[-1]
            filter_instance.x = last_state.x.copy()
            filter_instance.P = last_state.P.copy()
        else:
            # Reset ke initial
            filter_instance.reset(filter_instance.config.initial_value)
        
        return Ok(None)

    def _ensure_matrix(self, x: Union[float, np.ndarray, list], shape: tuple) -> FloatMatrix:
        """
        [REVISI] Force-cast input jadi 2D Matrix yang strict.
        Mencegah error dimensi (N,) vs (N,1) yang sering kejadian di Numpy.
        """
        try:
            arr = np.array(x, dtype=np.float64)
            
            # Kasus 1: Input skalar float -> ubah ke matrix sesuai shape
            if arr.ndim == 0:
                if shape == (1, 1):
                    return arr.reshape(1, 1)
                # --- FIX: Gunakan float(arr.item()) alih-alih float(x) ---
                return np.eye(shape[0]) * float(arr.item())
                # ---------------------------------------------------------
            
            # Kasus 2: Array 1D (2,) -> ubah ke (2, 1) atau (1, 2)
            if arr.ndim == 1:
                # Jika target kolom 1 (State Vector), transpose jadi vertikal
                if shape[1] == 1:
                    return arr.reshape(-1, 1)
                # Jika target baris 1 (Measurement Matrix), horizontal
                else:
                    return arr.reshape(1, -1)
            
            return arr
        except Exception as e:
            raise KalmanError(f"Matrix casting failed: {str(e)}")


# ============================================================================
# MONADIC OPERATOR FUNCTIONS
# ============================================================================

def compose_kalman_operations(
    operations: list[Callable[[KalmanState], Result[KalmanState, KalmanError]]]
) -> Callable[[KalmanState], Result[KalmanState, KalmanError]]:
    """
    Compose multiple monadic operations menjadi single operation.
    Rust-style monadic chaining.
    """
    def composed(state: KalmanState) -> Result[KalmanState, KalmanError]:
        current_result: Result[KalmanState, KalmanError] = Ok(state)
        
        for op in operations:
            if current_result.is_err():
                return current_result
            
            current_result = current_result.and_then(op)
        
        return current_result
    
    return composed
    


def with_retry(
    operation: Callable[[KalmanState], Result[KalmanState, KalmanError]],
    max_retries: int = 3
) -> Callable[[KalmanState], Result[KalmanState, KalmanError]]:
    """
    Decorate monadic operation dengan retry logic.
    """
    def retry_operation(state: KalmanState) -> Result[KalmanState, KalmanError]:
        last_error = None
        
        for attempt in range(max_retries):
            result = operation(state)
            
            if result.is_ok():
                return result
            
            last_error = result.unwrap_err()
            
            # Hanya retry pada specific errors
            if isinstance(last_error, NumericalStabilityError):
                # Reset sedikit noise
                state = state.map(lambda s: s)  # Placeholder untuk recovery logic
                continue
            else:
                # Jika error selain NumericalStability, langsung keluar dari loop
                break
        
        # --- FIX: Type Narrowing & Fallback ---
        if last_error is None:
            return Err(KalmanError("Operation failed without specific error or max_retries=0"))
        
        return Err(last_error)
    
    return retry_operation

# ============================================================================
# ASYNC MONADIC OPERATIONS
# ============================================================================
@safe_async
async def async_batch_process(
    filter_instance: AdaptiveKalmanFilter,
    measurements: list[float],
    lambda_factors: Optional[list[float]] = None
) -> Result[list[KalmanState], str]:
    """
    [RESTORED] Async wrapper untuk mengeksekusi KalmanBatchProcessor.
    Mencegah blocking pada event loop utama saat melakukan batch research/backtest.
    """
    import asyncio
    from concurrent.futures import ThreadPoolExecutor
    
    def sync_task() -> Result[list[KalmanState], str]:
        # Buat instance processor dan masukkan filter_instance ke dalamnya
        processor = KalmanBatchProcessor(Ok(filter_instance))
        return processor.process_batch(measurements, lambda_factors)

    # Eksekusi fungsi berat secara paralel di ThreadPool agar tidak nge-block
    loop = asyncio.get_event_loop()
    with ThreadPoolExecutor(max_workers=1) as pool:
        result = await loop.run_in_executor(pool, sync_task)
        return result

@safe_async
async def async_kalman_update(
    filter_instance: AdaptiveKalmanFilter,
    z: float,
    lambda_factor: float = 1.0
) -> Result[KalmanState, str]:
    """
    Async monadic update untuk integration dengan async systems.
    """
    # Eksekusi fungsi update yang mengembalikan Result[KalmanState, KalmanError]
    result = filter_instance.update(z, lambda_factor)
    
    # --- FIX: Casting dari KalmanError menjadi string ---
    if result.is_err():
        err = result.unwrap_err()
        return Err(str(err) if err is not None else "Unknown async update error")
    
    state = result.unwrap()
    assert state is not None, "State cannot be None"
    
    return Ok(state)

# ============================================================================
# FACADE EXPORTS
# ============================================================================

# Default exports untuk __init__.py
__all__ = [
    # Core Types
    'AdaptiveKalmanFilter',
    'KalmanConfig',
    'KalmanState',
    
    # Monadic Factories
    'KalmanFactory',
    'KalmanBatchProcessor',
    
    # Error Types
    'KalmanError',
    'SingularMatrixError',
    'NumericalStabilityError',
    
    # Enums
    'AdaptationMode',
    
    # Monadic Utilities
    'compose_kalman_operations',
    'with_retry',
    
    # Async Operations
    'async_kalman_update',
    'async_batch_process',
    
    # Result Pattern Integration
    'Result', 'Ok', 'Err', 'match_result', 'safe_async'
]
