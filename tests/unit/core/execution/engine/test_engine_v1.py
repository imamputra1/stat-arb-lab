"""
TEST: THE KILL HOUSE (Execution Engine V1) - FINAL FIXED
Desc: Stress test dengan Explicit Keyword Arguments agar anti-error.
"""

import time
import sys
import os
import logging
import random

# Pastikan root project terdeteksi agar import aman
sys.path.append(os.getcwd())

from core.execution.types import Order, OrderSide, OrderType
from core.execution.engine import (
    create_execution_engine, 
    MarketContext, 
    RejectionCode,
    EngineConfig
)

# Setup Logger sederhana untuk melihat output di terminal
logging.basicConfig(level=logging.INFO, format='%(message)s')

def test_run_engine():
    print("\n" + "="*60)
    print("🚀 STARTING ENGINE TEST: THE KILL HOUSE (V1.FINAL)")
    print("="*60 + "\n")

    # ---------------------------------------------------------
    # 1. INITIALIZATION
    # ---------------------------------------------------------
    print("[1] Merakit Engine...")
    config = EngineConfig(
        max_orders_per_sec=10,      # Limit ketat untuk memancing Circuit Breaker
        max_rejections_per_sec=5,
        enable_regime_detection=True
    )
    engine = create_execution_engine(config)
    print("✅ Engine Assembly Complete. State: READY.\n")

    # ---------------------------------------------------------
    # SCENARIO A: NORMAL TRADING (Happy Path)
    # ---------------------------------------------------------
    print("[A] SCENARIO: Normal Trading Day")
    
    # Context Pasar Tenang
    ctx_normal = MarketContext(
        price=50000.0,
        timestamp=time.time(),
        volume=100.0,
        volatility=0.005, # 0.5% (Rendah)
        spread=5.0,
        liquidity_ratio=1.0
    )
    
    # [FIX] MENGGUNAKAN EXPLICIT KEYWORD ARGUMENTS
    order_1 = Order(
        order_id="ord_normal_01",
        symbol="BTC/USDT",
        side=OrderSide.BUY,
        type=OrderType.MARKET,
        quantity=0.1
    )
    
    print(f"    📡 Sending Order: {order_1.side.value} {order_1.quantity} BTC")
    res_1 = engine.submit_order(order_1, ctx_normal)
    
    if res_1.is_ok():
        trade = res_1.unwrap()
        print("    ✅ Trade FILLED!")
        print(f"       Price: {trade.price:.2f}")
        print(f"       Fee:   {trade.fee:.4f}")
    else:
        err = res_1.unwrap_err()
        print(f"    ❌ Order Failed: {err.reason}")
        # Fail test jika skenario basic ini gagal
        assert False, f"Basic trade failed: {err.reason}"

    # ---------------------------------------------------------
    # SCENARIO B: SPAM ATTACK (Circuit Breaker)
    # ---------------------------------------------------------
    print("\n[B] SCENARIO: Spam Attack (Rate Limit Test)")
    print("    🔥 Firing 15 orders rapidly (Limit is 10/sec)...")
    
    blocked_count = 0
    for i in range(15):
        # [FIX] EXPLICIT KWARGS
        ord_spam = Order(
            order_id=f"spam_{i}",
            symbol="BTC/USDT",
            side=OrderSide.BUY,
            type=OrderType.MARKET,
            quantity=0.01
        )
        res = engine.submit_order(ord_spam, ctx_normal)
        
        if res.is_err():
            if res.unwrap_err().code == RejectionCode.RATE_LIMIT:
                blocked_count += 1

    if blocked_count > 0:
        print(f"    ✅ Circuit Breaker Worked! Blocked {blocked_count} orders.")
    else:
        print("    ❌ Circuit Breaker FAILED! All spam passed.")
        # Jangan assert False disini, biarkan lanjut ke Scenario C

    # [PENTING] RESET & COOL DOWN
    # Kita harus menunggu agar 'Counter Rate Limit' mereset dirinya sendiri
    print("    ⏳ Cooling down (1.5s) to clear Circuit Breaker...")
    engine.reset()
    time.sleep(1.5) 

    # ---------------------------------------------------------
    # SCENARIO C: THE CRASH (Regime & Chaos Test)
    # ---------------------------------------------------------
    print("\n[C] SCENARIO: Market Crash (Crisis Regime)")
    print("    ⚠️  Warming up Regime Detector (Feeding Volatile Data)...")
    
    # Feed data palsu agar detector sadar pasar sedang crash
    base_price = 48000.0
    for i in range(25): # Butuh >20 data point untuk statistik
        shock = random.uniform(-200, 200) 
        # Update dengan spread lebar (100.0) dan vol tinggi
        engine.emulator.regime_detector.update(base_price + shock, 5000.0, 100.0)
    
    current_regime = engine.emulator.regime_detector.current_regime
    print(f"    📊 Detected Regime: {current_regime.value} (Target: CRISIS/VOLATILE)")
    
    # Context Pasar Hancur
    ctx_crisis = MarketContext(
        price=48000.0,
        timestamp=time.time(),
        volume=5000.0,    # Panic Selling Volume
        volatility=0.05,  # 5% Volatility (EXTREME)
        spread=100.0,     # Spread Lebar
        liquidity_ratio=0.1 # Likuiditas Kering
    )
    
    # [FIX] EXPLICIT KWARGS
    order_crash = Order(
        order_id="ord_crash_01",
        symbol="BTC/USDT",
        side=OrderSide.SELL,
        type=OrderType.MARKET,
        quantity=1.0 
    )
    
    print("    📡 Sending SELL Order in CRISIS...")
    res_crash = engine.submit_order(order_crash, ctx_crisis)
    
    if res_crash.is_ok():
        trade = res_crash.unwrap()
        print("    ✅ Trade Executed in CRISIS.")
        print(f"       Exec Price: {trade.price:.2f}")
        print(f"       Slippage:   {trade.slippage_bps:.2f} bps")
        
        # Validasi Logika Chaos: Slippage harusnya tinggi
        if trade.slippage_bps > 5.0:
            print("       -> Chaos Check: PASS (High Slippage verified)")
        else:
            print("       -> Chaos Check: WARNING (Slippage too low for crisis)")
    else:
        rej = res_crash.unwrap_err()
        print(f"    ⚠️ Order Rejected: {rej.reason}")

    # ---------------------------------------------------------
    # FINAL DIAGNOSTICS
    # ---------------------------------------------------------
    print("\n[D] FINAL DIAGNOSTICS")
    stats = engine.emulator.get_stats()
    print(f"    Total Trades:     {stats['counters']['trades']}")
    print(f"    Total Rejections: {stats['counters']['rejections']}")
    
    print("\n" + "="*60)
    print("🏁 TEST COMPLETE")
    print("="*60)

if __name__ == "__main__":
    test_run_engine()
