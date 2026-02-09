"""
WAR GAME: EXECUTION FLOW TEST
Location: tests/integration/test_execution_flow.py
Desc: Membuktikan bahwa OMSFacade bisa mengendalikan ExecutionSimulator
      untuk melakukan trading end-to-end.
"""

import pytest
import asyncio
from core.execution.oms import OMSFacade, OMSMode
from core.execution.simulator import ExecutionSimulator, SimulatorConfig

@pytest.mark.asyncio
async def test_full_trading_cycle():
    print("\n🚀 STARTING OMS INTEGRATION TEST...")

    # 1. SETUP: Siapkan Medan Perang (Simulator)
    # Kita buat Simulator yang agak 'jahat' (Latency 10ms, Slippage 5 bps)
    sim_config = SimulatorConfig(
        initial_cash=100_000.0,
        latency_mean_ms=10.0,
        slippage_std_bps=5.0
    )
    simulator = ExecutionSimulator(sim_config)
    
    # 2. SETUP: Siapkan OMS via Facade (The Controller)
    # Perhatikan betapa bersihnya inisialisasi ini berkat Factory Function
    facade_res = OMSFacade.create(
        broker=simulator,
        mode=OMSMode.PAPER,
        max_open_orders=10
    )
    assert facade_res.is_ok(), f"Gagal init OMS: {facade_res.unwrap_err()}"
    oms = facade_res.unwrap()

    # Nyalakan Mesin
    await oms.start()
    
    # --- SCENARIO 1: AGGRESSIVE ENTRY (MARKET BUY) ---
    print("\n[1] 🔫 Sending MARKET BUY: 1.5 BTC...")
    
    # Simulator perlu harga pasar reference (biasanya dari Streamer)
    # Kita inject manual dulu ke simulator engine
    simulator.process_tick(type('Tick', (), {'symbol': 'BTC/USDT', 'price': 50000.0}))

    # Action!
    buy_res = await oms.buy_market("BTC/USDT", 1.5)
    
    # Assertions
    assert buy_res.is_ok(), f"Buy failed: {buy_res.unwrap_err()}"
    report = buy_res.unwrap()
    print(f"    ✅ Filled at: {report.avg_price:.2f} (Slippage included)")
    
    # Cek Portfolio lewat Facade
    portfolio = oms.get_portfolio()
    pos_btc = oms.get_position("BTC/USDT")
    
    assert pos_btc.quantity == 1.5
    assert portfolio.positions[0].symbol == "BTC/USDT"
    print(f"    📦 Inventory: {pos_btc.quantity} BTC | AvgPrice: {pos_btc.average_entry_price:.2f}")

    # --- SCENARIO 2: PASSIVE EXIT (LIMIT SELL) ---
    print("\n[2] 🛡️ Placing LIMIT SELL: 0.5 BTC at $55,000...")
    
    sell_res = await oms.sell_limit("BTC/USDT", 0.5, 55000.0)
    assert sell_res.is_ok()
    order_id = sell_res.unwrap().order.order_id
    print(f"    ✅ Order Placed: {order_id} (Status: {sell_res.unwrap().order.status.value})")
    
    # Cek Inventory (Harusnya belum berkurang karena belum fill)
    pos_btc_check = oms.get_position("BTC/USDT")
    assert pos_btc_check.quantity == 1.5, "Inventory tidak boleh berkurang sebelum fill!"

    # --- SCENARIO 3: MARKET MOVE & EXECUTION ---
    print("\n[3] 📈 Market pumps to $55,100...")
    
    # Harga bergerak naik menembus limit kita
    simulator.process_tick(type('Tick', (), {'symbol': 'BTC/USDT', 'price': 55100.0}))
    
    # Tunggu sebentar (Asyncio sleep) agar simulator memproses antrian order
    await asyncio.sleep(0.1)
    
    # Cek status order lagi
    order_report = await oms._oms.broker.get_order(order_id) # Akses low level untuk cek broker
    print(f"    ✅ Order Status Update: {order_report.unwrap().order.status.value}")
    
    # Cek Realized PnL via Facade
    pnl = oms.get_pnl()
    stats = oms.get_summary_stats()
    
    print("\n[4] 💰 FINAL REPORT:")
    print(f"    Realized PnL : ${pnl:.2f}")
    print(f"    Fees Paid    : {stats['fees_paid']}")
    print(f"    Remaining BTC: {oms.get_position('BTC/USDT').quantity}")

    # Final Assertion: PnL harus positif (Beli 50k, Jual 55k)
    assert pnl > 0
    assert oms.get_position("BTC/USDT").quantity == 1.0 # 1.5 - 0.5

    await oms.stop()
    print("\n🏆 MISSION SUCCESS: OMS & Simulator are fully integrated.")

if __name__ == "__main__":
    # Allow running directly without pytest
    asyncio.run(test_full_trading_cycle())
