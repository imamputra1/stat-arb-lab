"""
WAR GAME: EXECUTION FLOW TEST (CORRECTED)
Location: tests/integration/test_execution_flow.py
Desc: Menambahkan langkah 'refresh_orders' agar OMS sadar order sudah Filled.
"""

import pytest
import asyncio
from types import SimpleNamespace
from core.execution.oms import OMSFacade, OMSMode
from core.execution.simulator import ExecutionSimulator, SimulatorConfig

@pytest.mark.asyncio
async def test_full_trading_cycle():
    print("\n🚀 STARTING OMS INTEGRATION TEST...")

    # 1. SETUP
    sim_config = SimulatorConfig(
        initial_cash=100_000.0,
        latency_mean_ms=10.0,
        slippage_std_bps=5.0
    )
    simulator = ExecutionSimulator(sim_config)
    
    facade_res = OMSFacade.create(
        broker=simulator,
        mode=OMSMode.PAPER,
        max_open_orders=10
    )
    assert facade_res.is_ok(), f"Gagal init OMS: {facade_res.unwrap_err()}"
    oms = facade_res.unwrap()
    await oms.start()
    
    # --- SCENARIO 1: AGGRESSIVE ENTRY (MARKET BUY) ---
    print("\n[1] 🔫 Sending MARKET BUY: 1.5 BTC...")
    start_tick = SimpleNamespace(symbol='BTC/USDT', price=50000.0)
    simulator.process_tick(start_tick)
    
    buy_res = await oms.buy_market("BTC/USDT", 1.5)
    assert buy_res.is_ok(), f"Buy failed: {buy_res.unwrap_err()}"
    print(f"    ✅ Filled at: {buy_res.unwrap().vwap_execution:.2f}")

    # --- SCENARIO 2: PASSIVE EXIT (LIMIT SELL) ---
    print("\n[2] 🛡️ Placing LIMIT SELL: 0.5 BTC at $55,000...")
    sell_res = await oms.sell_limit("BTC/USDT", 0.5, 55000.0)
    assert sell_res.is_ok()
    order_id = sell_res.unwrap().order.order_id
    print(f"    ✅ Order Placed: {order_id} (Status: OPEN)")

    # --- SCENARIO 3: MARKET MOVE & EXECUTION ---
    print("\n[3] 📈 Market pumps to $55,100...")
    
    # Gerakkan harga di Simulator
    pump_tick = SimpleNamespace(symbol='BTC/USDT', price=55100.0)
    simulator.process_tick(pump_tick)
    
    # Tunggu sebentar agar simulator memproses
    await asyncio.sleep(0.1)
    
    # =========================================================================
    # [CRITICAL FIX] 
    # Kita harus menyuruh OMS update status secara manual (Polling)
    # karena Simulator tidak punya WebSocket push ke OMS.
    # =========================================================================
    print("    🔄 Syncing OMS state (Polling)...")
    await oms.refresh_orders()
    # =========================================================================

    # Validasi bahwa OMS sekarang tahu order sudah filled
    # Kita cek lewat OMS (bukan lewat broker langsung) untuk memastikan sync berhasil
    portfolio = oms.get_portfolio()
    # Harusnya sisa 1.0 BTC (1.5 Beli - 0.5 Jual)
    pos_btc = oms.get_position("BTC/USDT")
    print(f"    📦 Updated Inventory: {pos_btc.quantity} BTC")

    # --- FINAL CHECK ---
    print("\n[4] 💰 FINAL REPORT:")
    
    pnl = oms.get_pnl()
    stats = oms.get_summary_stats()
    current_btc = pos_btc.quantity
    
    print(f"    Realized PnL : ${pnl:.2f}")
    print(f"    Fees Paid    : {stats['fees_paid']}")
    print(f"    Remaining BTC: {current_btc}")

    # Assertions
    # 0.5 BTC * (55000 - 50000) = ~2500 profit (dikurangi fee)
    assert pnl > 0, f"PnL Masih 0! Pastikan trade tercatat. Got: {pnl}"
    assert abs(current_btc - 1.0) < 0.001, f"Inventory salah! Got: {current_btc}"

    print("\n🏆 MISSION SUCCESS")
