"""
UNIT TEST: ADAPTIVE KALMAN FILTER (CORE MATH)
Focus: Validating Math Kernel, Monadic Error Handling, & Adaptation Logic.
Rule: Import strictly via Facade (core.math).
"""
from core.math import (
    AdaptiveKalmanFilter, 
    KalmanConfig,       # [FIX] Import Config Object
    KalmanError, 
    SingularMatrixError,
    NumericalStabilityError
)

# ============================================================================
# 1. BASIC FUNCTIONALITY & CONVERGENCE
# ============================================================================
def test_initialization():
    """Memastikan filter inisialisasi dengan benar via KalmanConfig."""
    # [FIX] Gunakan Config Object
    conf = KalmanConfig(R=0.1, Q=0.01, initial_value=100.0)
    kf = AdaptiveKalmanFilter(conf)
    
    # Gunakan property compatibility atau method baru
    assert kf.current_estimate == 100.0
    assert kf.uncertainty == 1.0  # Default initial P
    assert kf.x.shape == (2, 1)   # Pastikan shape 2D (State Vector)

def test_convergence_on_flat_data():
    """
    Test 1: Convergence.
    Diberikan data flat, uncertainty (P) harus turun dan estimasi stabil.
    """
    conf = KalmanConfig(R=1.0, Q=0.001, initial_value=100.0)
    kf = AdaptiveKalmanFilter(conf)
    
    prices = [100.0] * 50 # Harga flat sempurna
    
    final_state = None
    for z in prices:
        result = kf.update(z)
        assert result.is_ok(), f"Update failed: {result.unwrap_err()}"
        final_state = result.unwrap()
    
    # Assertions
    assert final_state is not None
    # Estimasi harus sangat dekat dengan 100
    assert abs(final_state.x[0,0] - 100.0) < 0.1
    # Uncertainty harus turun drastis dari 1.0
    assert final_state.P[0,0] < 0.1
    print(f"\n✅ Convergence Valid: P dropped to {final_state.P[0,0]:.5f}")

# ============================================================================
# 2. SHOCK HANDLING (ADAPTIVE Q)
# ============================================================================
def test_adaptive_shock_response():
    """
    Test 2: Shock Response.
    Cek apakah filter mendeteksi 'Market Shock' dan menaikkan Q.
    """
    # Setup filter yang sangat "percaya diri" (P rendah)
    conf = KalmanConfig(R=0.1, Q=0.0001, initial_value=100.0)
    kf = AdaptiveKalmanFilter(conf)
    
    # Phase 1: Train to stability
    for _ in range(50):
        kf.update(100.0)
        
    state_before_shock = kf.update(100.0).unwrap()
    base_Q_val = kf.base_Q[0,0]
    
    # Phase 2: THE SHOCK (Harga loncat ke 105 - 50 sigma event)
    shock_result = kf.update(105.0, adapt=True)
    
    assert shock_result.is_ok()
    state_shock = shock_result.unwrap()
    
    # Check 1: Adaptive Q harus dipakai (lebih besar dari base Q)
    used_Q = state_shock.Q_adaptive[0,0]
    print(f"\n✅ Shock Test: Base Q={base_Q_val}, Used Q={used_Q}")
    
    assert used_Q > base_Q_val, "Adaptive Logic GAGAL menaikkan Q saat shock!"
    
    # Check 2: Uncertainty (P) harus 'meledak' sesaat biar filter cepat belajar
    assert state_shock.P[0,0] > state_before_shock.P[0,0], "P tidak mengembang saat shock!"

# ============================================================================
# 3. MONADIC ERROR HANDLING & ROBUSTNESS
# ============================================================================
def test_input_validation():
    """Test handling input sampah (NaN/Inf)."""
    conf = KalmanConfig(R=0.1, Q=0.01, initial_value=100.0)
    kf = AdaptiveKalmanFilter(conf)
    
    # Case: NaN Input
    res = kf.update(float('nan'))
    assert res.is_err()
    err = res.unwrap_err()
    # Pastikan error tipe NumericalStabilityError atau setidaknya KalmanError
    assert isinstance(err, (NumericalStabilityError, KalmanError)) or "finite" in str(err)
    print(f"\n✅ Validation Check: Caught {err}")

def test_singular_matrix_prevention():
    """
    Mencoba memaksa singularitas.
    R=0, Q=0 -> Resep bencana numerik.
    """
    conf = KalmanConfig(R=1e-9, Q=1e-10, initial_value=100.0) # R mendekati 0
    kf = AdaptiveKalmanFilter(conf)
    
    res = kf.update(100.0)
    
    # Filter harus survive atau return Err yang sopan
    if res.is_err():
        assert isinstance(res.unwrap_err(), (SingularMatrixError, KalmanError))
        print("\n✅ Singular Check: Handled gracefully via Result type")
    else:
        print("\n✅ Singular Check: Math is robust enough to handle zero noise")

# ============================================================================
# 4. IMMUTABILITY CHECK
# ============================================================================
def test_state_immutability():
    """
    Memastikan output KalmanState adalah snapshot yang aman (Deep Copy).
    """
    conf = KalmanConfig(R=0.1, Q=0.1, initial_value=10.0)
    kf = AdaptiveKalmanFilter(conf)
    
    res = kf.update(10.0)
    state = res.unwrap()
    
    # Hack the output
    original_val = state.x[0,0]
    state.x[0,0] = 9999.0 # Ubah nilai di object hasil
    
    # Cek internal filter
    assert kf.current_estimate == original_val, "Internal state BOCOR! Return value harus copy."
    print("\n✅ Immutability Check: Internal state safe.")
