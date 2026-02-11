"""
THE ORCHESTRATOR (PERFORMANCE & STRESS TEST EDITION)
Location: live/engine.py
Desc: Engine tanpa batas kecepatan (No Sleep). Dilengkapi Speedometer (TPS).
      Tujuannya mengukur raw performance dari Core Strategy.
"""

import logging
import time
import math
import random
import traceback
import pandas as pd
from dataclasses import dataclass
from datetime import datetime
from typing import List

# [VISUALIZATION TOOLS]
try:
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
except ImportError:
    print("⚠️ Plotly/Pandas belum terinstall.")

# [CORE IMPORTS]
from core.shared.result import Ok
from core.signals.factory import FactoryManager
from core.signals.base_signal import BaseStrategy
from core.signals.types import MarketObservation

# [MOCKED COMPONENTS - TETAP DIPAKAI UNTUK STRESS TEST]
class RiskManager:
    def __init__(self, config): self.config = config
    def validate_signal(self, signal): return Ok(True)

class OrderManagementSystem:
    def __init__(self, config): self.config = config
    def execute(self, signal): return Ok(f"MOCK-ORD-{int(time.time())}")

# [CONFIG IMPORT]
try:
    from live.config import STRATEGY_CONFIG, EXECUTION_CONFIG, RISK_CONFIG, DATA_CONFIG
except ImportError:
    STRATEGY_CONFIG = {}

# Matikan Log Info biasa agar tidak memperlambat Terminal
logging.basicConfig(level=logging.WARNING) 

@dataclass
class TickRecord:
    timestamp: datetime
    price: float
    kalman: float
    signal: str
    equity: float

class TradingEngine:
    
    def __init__(self, strategy: BaseStrategy, risk_manager, oms):
        self.strategy = strategy
        self.risk = risk_manager
        self.oms = oms
        
        self.history: List[TickRecord] = []
        self.current_equity = 100000.0
        self.position = 0.0 
        self.tick_count = 0
        
        # [PERFORMANCE METRICS]
        self.start_time = 0
        self.last_report_time = 0
        
        print("\n🚀 ORCA ENGINE: STRESS TEST MODE (MAX SPEED)")
        print(f"   Strategy: {self.strategy.name}")
        print("   Status: Running without speed limit...")
        print("-" * 80)

    def process_tick(self, observation: MarketObservation) -> None:
        self.tick_count += 1
        
        try:
            # 1. Strategy Logic (THE HEAVY LIFTING)
            signal_res = self.strategy.evaluate_state(observation)
            
            # 2. Mock Execution Logic (Minimal Overhead)
            price = observation.data.get('close_DOGE', 0.0)
            sig_name = "NEUTRAL"
            
            if signal_res.is_ok():
                signal = signal_res.unwrap()
                sig_name = signal.signal_type.name
                
                s_type = signal.signal_type.name
                if "LONG" in s_type and "ENTRY" in s_type:
                    self.position += 100
                    self.current_equity -= (price * 100)
                elif "SHORT" in s_type and "ENTRY" in s_type:
                    self.position -= 100
                    self.current_equity += (price * 100)
                elif "EXIT" in s_type and self.position != 0:
                    self.current_equity += (self.position * price)
                    self.position = 0

            # 3. RECORDING (Sampling Only)
            # Agar memori tidak meledak saat stress test jutaan tick,
            # kita hanya rekam 1 dari setiap 100 tick, atau jika ada signal.
            if sig_name != "NEUTRAL" or self.tick_count % 100 == 0:
                kalman_val = 0.0
                if hasattr(self.strategy, 'get_state'):
                    try:
                        st = self.strategy.get_state()
                        obj = st.unwrap() if hasattr(st, 'is_ok') and st.is_ok() else st
                        if isinstance(obj, dict): kalman_val = obj.get('kalman_value', 0.0)
                        else: kalman_val = getattr(obj, 'kalman_value', 0.0)
                    except: pass

                self.history.append(TickRecord(
                    timestamp=datetime.now(),
                    price=price,
                    kalman=kalman_val,
                    signal=sig_name,
                    equity=self.current_equity + (self.position * price)
                ))

        except Exception as e:
            print(f"🔥 CRASH: {e}")
            traceback.print_exc()

    def report_performance(self):
        """Menghitung Kecepatan Engine"""
        now = time.time()
        elapsed = now - self.start_time
        
        if elapsed > 0:
            tps = self.tick_count / elapsed
            print(f"⏱️  PERFORMANCE: {self.tick_count:,.0f} Ticks Processed | Speed: {tps:,.2f} ticks/sec | Equity: {self.current_equity:,.2f}")
        
    def generate_html_report(self):
        print("\n⏳ Generating Stress Test Report...")
        if not self.history: return
        
        df = pd.DataFrame([vars(x) for x in self.history])
        
        fig = make_subplots(rows=2, cols=1, shared_xaxes=True, subplot_titles=("Price & Signals (Sampled)", "Equity Curve"))
        
        fig.add_trace(go.Scatter(x=df['timestamp'], y=df['price'], name='Price'), row=1, col=1)
        fig.add_trace(go.Scatter(x=df['timestamp'], y=df['kalman'], name='Kalman'), row=1, col=1)
        
        buys = df[df['signal'].str.contains('LONG')]
        sells = df[df['signal'].str.contains('SHORT')]
        
        fig.add_trace(go.Scatter(x=buys['timestamp'], y=buys['price'], mode='markers', marker=dict(color='green', size=8), name='BUY'), row=1, col=1)
        fig.add_trace(go.Scatter(x=sells['timestamp'], y=sells['price'], mode='markers', marker=dict(color='red', size=8), name='SELL'), row=1, col=1)
        
        fig.add_trace(go.Scatter(x=df['timestamp'], y=df['equity'], name='Equity', fill='tozeroy'), row=2, col=1)
        
        fig.update_layout(height=800, title="Orca Stress Test Result", template="plotly_dark")
        fig.write_html(f"stress_test_{int(time.time())}.html")
        print("✅ Report saved.")

def bootstrap_engine():
    strat = FactoryManager.create_strategy(STRATEGY_CONFIG).unwrap()
    return TradingEngine(strat, RiskManager(RISK_CONFIG), OrderManagementSystem(EXECUTION_CONFIG))

if __name__ == "__main__":
    engine = bootstrap_engine()
    engine.start_time = time.time()
    
    try:
        # STRESS TEST LOOP
        # Target: Jalankan selama 10 detik secepat mungkin
        TARGET_DURATION = 10 
        end_time = time.time() + TARGET_DURATION
        
        t = 0
        while time.time() < end_time:
            t += 1
            # Generator Super Cepat
            noise = random.gauss(0, 0.5)
            trend = math.sin(t * 0.01) * 5
            
            obs = MarketObservation(
                timestamp=int(time.time() * 1000),
                symbol="DOGE/USDT",
                source="STRESS_TEST",
                data={"close_DOGE": 100 + trend + noise, "close_BTC": 100 + trend*0.8, "volume": 1000}
            )
            
            engine.process_tick(obs)
            
            # Lapor speed setiap 10.000 tick
            if t % 10000 == 0:
                engine.report_performance()
                
        print("\n🏁 STRESS TEST FINISHED!")
        engine.report_performance()
        engine.generate_html_report()
        
    except KeyboardInterrupt:
        engine.report_performance()
        engine.generate_html_report()
