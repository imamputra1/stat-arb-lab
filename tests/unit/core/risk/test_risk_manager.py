"""
UNIT TEST: RISK MANAGER (THE GUARDIAN)
Location: tests/unit/test_risk_manager.py
Focus: Capital Protection, Sizing Logic, & Circuit Breakers.
"""

import pytest

# Import Core Risk Modules
from core.risk import (
    RiskManager, 
    RiskConfig, 
    AccountState, 
    TradeRequest, 
    TradeAction, 
    RiskLevel, 
    RejectionCode
)

# --- FIXTURES (DATA DUMMY UNTUK TEST) ---

@pytest.fixture
def conservative_config():
    """Config Konservatif: Max Risk 1%, Max DD 3%"""
    return RiskConfig(
        max_account_risk_per_trade=0.01,
        max_daily_drawdown=0.03,
        max_open_trades=5,
        max_leverage_per_trade=3.0,
        circuit_breaker_threshold=0.05
    )

@pytest.fixture
def healthy_account():
    """Akun Sehat: $10,000 Equity, No Loss"""
    return AccountState(
        balance=10000.0,
        equity=10000.0,
        unrealized_pnl=0.0,
        daily_pnl=0.0,
        open_positions_count=0,
        used_margin=0.0
    )

@pytest.fixture
def standard_request():
    """Request Beli BTC standar"""
    return TradeRequest(
        symbol="BTC/USD",
        action=TradeAction.ENTER,
        requested_size=1.0, # Minta 1 BTC (Jelas kegedean, nanti di-resize)
        entry_price=50000.0,
        stop_loss=49000.0,  # Risk $1000 per coin
        metadata={
            "volatility": 0.04, 
            "conviction": 0.8  # Strong signal
        }
    )

@pytest.fixture
def risk_manager(conservative_config):
    return RiskManager(config=conservative_config)

# --- TEST SCENARIOS ---

def test_initialization(risk_manager):
    """Test apakah Satpam bisa disewa (Init)"""
    assert risk_manager is not None
    # Cek Protocol
    assert risk_manager.validate().is_ok()

def test_happy_path_approval(risk_manager, healthy_account, standard_request):
    """
    Skenario: Akun sehat, Sinyal bagus.
    Harapan: APPROVED, tapi Size disesuaikan (bukan 1 BTC mentah-mentah).
    """
    res = risk_manager.evaluate_trade(standard_request, healthy_account)
    
    assert res.is_ok()
    verdict = res.unwrap()
    
    assert verdict.approved is True
    assert verdict.approved_size > 0.0
    assert verdict.approved_size < 1.0  # Harus di-resize (Kelly Logic)
    
    # Cek Audit Trail
    assert verdict.risk_level == RiskLevel.NORMAL
    assert verdict.position_risk_pct > 0

def test_circuit_breaker_daily_loss(risk_manager, healthy_account, standard_request):
    """
    Skenario: Hari ini sudah rugi 4% (Batas 3%).
    Harapan: REJECT TOTAL (Circuit Breaker).
    """
    # Simulasi akun boncos
    boncos_account = AccountState(
        balance=10000.0,
        equity=9600.0,     # Rugi 400
        unrealized_pnl=0.0,
        daily_pnl=-400.0,  # -4% (Melebihi limit 3%)
        open_positions_count=0
    )
    
    res = risk_manager.evaluate_trade(standard_request, boncos_account)
    verdict = res.unwrap()
    
    assert verdict.approved is False
    assert verdict.approved_size == 0.0
    assert "limit" in verdict.rejection_reason.lower() or "drawdown" in verdict.rejection_reason.lower()

def test_negative_equity_protection(risk_manager, standard_request):
    """
    Skenario: Akun Minus (Equity Negatif/Nol).
    Harapan: APPROVED SIZE = 0.0 (Jangan sampai crash atau return size negatif).
    """
    bankrupt_account = AccountState(
        balance=-50.0,
        equity=-50.0,
        unrealized_pnl=0.0,
        daily_pnl=0.0,
        open_positions_count=0
    )
    
    res = risk_manager.evaluate_trade(standard_request, bankrupt_account)
    verdict = res.unwrap()
    
    # Kelly Calculator sudah kita patch di Task sebelumnya agar return 0.0
    # Tapi verdict logic mungkin me-reject karena insufficient equity
    
    assert verdict.approved_size == 0.0 
    # Bisa rejected karena equity < 100 (Basic Validation) atau Size calculation 0
    assert verdict.approved is False 

def test_max_open_trades_limit(risk_manager, healthy_account, standard_request):
    """
    Skenario: Sudah punya 5 posisi (Limit 5).
    Harapan: REJECT (Overtrading).
    """
    busy_account = AccountState(
        balance=10000.0, equity=10000.0, unrealized_pnl=0.0, daily_pnl=0.0,
        open_positions_count=5, # Limit Hit
        used_margin=5000.0
    )
    
    res = risk_manager.evaluate_trade(standard_request, busy_account)
    verdict = res.unwrap()
    
    assert verdict.approved is False
    assert verdict.rejection_code == RejectionCode.ACCOUNT_RISK_LIMIT

def test_volatility_sizing_adjustment(risk_manager, healthy_account):
    """
    Skenario: Bandingkan Sinyal High Volatility vs Low Volatility.
    Harapan: High Volatility -> Size Lebih Kecil.
    """
    # 1. Low Volatility Request
    req_low_vol = TradeRequest(
        symbol="BTC/USD", action=TradeAction.ENTER, requested_size=1.0, 
        entry_price=100.0, stop_loss=90.0,
        metadata={"volatility": 0.01, "conviction": 0.8} # 1% Vol
    )
    
    # 2. High Volatility Request
    req_high_vol = TradeRequest(
        symbol="BTC/USD", action=TradeAction.ENTER, requested_size=1.0, 
        entry_price=100.0, stop_loss=90.0,
        metadata={"volatility": 0.10, "conviction": 0.8} # 10% Vol
    )
    
    # Evaluate
    verd_low = risk_manager.evaluate_trade(req_low_vol, healthy_account).unwrap()
    verd_high = risk_manager.evaluate_trade(req_high_vol, healthy_account).unwrap()
    
    print(f"\nLow Vol Size: {verd_low.approved_size:.4f}")
    print(f"High Vol Size: {verd_high.approved_size:.4f}")
    
    # Assert
    assert verd_low.approved_size > verd_high.approved_size
    assert verd_high.approved_size > 0.0 # Masih boleh masuk, tapi kecil

def test_missing_data_resilience(risk_manager, healthy_account, standard_request):
    """
    Skenario: Data market kosong, Metadata kosong.
    Harapan: Tidak Crash, pakai Default.
    """
    # Request tanpa metadata volatility
    blind_request = TradeRequest(
        symbol="UNK/USD", action=TradeAction.ENTER, requested_size=1.0,
        entry_price=10.0,
        metadata={} # Kosong melompong
    )
    
    # Evaluate tanpa market data
    res = risk_manager.evaluate_trade(blind_request, healthy_account, market_data=None)
    
    assert res.is_ok()
    verdict = res.unwrap()
    assert verdict.approved_size >= 0.0
    # Kalau data kosong, Kelly mungkin return kecil atau fallback default 5% vol

def test_pnl_tracker_update(risk_manager, healthy_account):
    """
    Skenario: Update state berkali-kali.
    Harapan: Max Drawdown ter-update.
    """
    # Hari 1: Profit
    state1 = AccountState(10000, 10500, 500, 500, 0) # Peak 500
    risk_manager.update_account_state(state1)
    
    # Hari 2: Rugi
    state2 = AccountState(10000, 9500, -500, -500, 0) # Drop dari peak?
    risk_manager.update_account_state(state2)
    
    # Cek metrics
    # Logic update_account_state Anda membandingkan peak dari history.
    # Pastikan tidak crash.
    assert len(risk_manager.daily_pnl_tracker) == 2
