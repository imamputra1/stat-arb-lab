"""
LIVE ENGINE - THE HEARTBEAT
Location: core/engine/live_engine.py
Desc: Menghubungkan Market Data Stream ke Strategy via Factory.
      Handling Real-time Latency dan Error Recovery.
"""

import time
from typing import Dict, Any
"""
THE ORCHESTRATOR: LIVE TRADING ENGINE
Location: live/engine.py
Desc: Menggabungkan Strategy, Risk, dan Execution (OMS) dalam satu loop.
      Single Source of Truth configuration dari 'live/config.py'.
"""

import logging

# [CORE IMPORTS]
from core.shared.result import Result, Ok, Err
from core.signals.factory import FactoryManager
from core.signals.types import MarketObservation, SignalEvent, SignalType

# [CONFIG IMPORT]
try:
    # Mengambil block config terpisah
    from live.config import (
        STRATEGY_CONFIG, 
        RISK_CONFIG, 
        EXECUTION_CONFIG,
        SYSTEM_CONFIG
    )
except ImportError:
    # Fallback darurat jika config.py belum ada/rusak
    logging.warning("⚠️ Config file not found/broken. Using fallback.")
    STRATEGY_CONFIG = {"type": "kalman_mr", "name": "Fallback", "signal_params": {}, "math_params": {}}
    RISK_CONFIG = {}
    EXECUTION_CONFIG = {}

# Setup Logger
logger = logging.getLogger("Engine")
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s'
)

# ==============================================================================
# MOCK / PLACEHOLDER COMPONENTS (Jika file Risk/OMS belum final)
# ==============================================================================
class RiskManager:
    """Penjaga Gawang: Memastikan sinyal aman dieksekusi"""
    def __init__(self, config: Dict):
        self.config = config
        self.max_drawdown = config.get("max_drawdown", 0.1)
    
    def validate(self, signal: SignalEvent) -> bool:
        # TODO: Implementasi Real Risk Check (Exposure, Drawdown, etc)
        # Di sini kita reject jika sinyalnya STOP
        if signal.signal_type == SignalType.STOP:
            logger.warning(f"🛡️ RISK BLOCK: Signal STOP triggered for {signal.symbol}")
            return False
        return True

class OrderManagementSystem:
    """Eksekutor: Mengirim order ke Binance/Exchange"""
    def __init__(self, config: Dict):
        self.config = config
        self.mode = config.get("mode", "PAPER") # Paper or Live
    
    def execute(self, signal: SignalEvent) -> Result[str, str]:
        # TODO: Connect to CCXT / Binance API
        action = signal.signal_type.name
        log_msg = f"Orders sent to Exchange: {action} {signal.symbol} (Simulated)"
        logger.info(f"⚡ OMS EXECUTION [{self.mode}]: {log_msg}")
        return Ok("Order_ID_12345")

# ==============================================================================
# MAIN ENGINE CLASS
# ==============================================================================

class TradingEngine:
    """
    Central Nervous System.
    Flow: Data -> Strategy -> Risk -> OMS
    """
    
    def __init__(self):
        logger.info(f"🚀 ENGINE IGNITION ({SYSTEM_CONFIG.get('env', 'DEV')})")
        
        # 1. Load Components
        self._init_strategy()
        self._init_risk()
        self._init_oms()
        
        # State
        self.tick_count = 0
        self.is_running = True

    def _init_strategy(self):
        """Merakit Strategi via Factory menggunakan STRATEGY_CONFIG"""
        logger.info(f"🧠 Initializing Strategy: {STRATEGY_CONFIG.get('name')}")
        
        # Panggil FactoryManager (Singleton)
        res = FactoryManager.create_strategy(STRATEGY_CONFIG)
        
        if res.is_err():
            raise RuntimeError(f"Strategy Init Failed: {res.unwrap_err()}")
            
        self.strategy = res.unwrap()
        logger.info("✅ Brain Online")

    def _init_risk(self):
        """Menyiapkan Risk Manager"""
        self.risk = RiskManager(RISK_CONFIG)
        logger.info("✅ Shield Online")

    def _init_oms(self):
        """Menyiapkan OMS"""
        self.oms = OrderManagementSystem(EXECUTION_CONFIG)
        logger.info("✅ Hands Online")

    def process_tick(self, raw_data: Dict[str, Any]) -> Result[str, str]:
        """
        [THE PIPELINE]
        Satu putaran penuh pemrosesan data.
        """
        try:
            self.tick_count += 1
            
            # --- PHASE 1: SENSOR (Data Ingestion) ---
            # Konversi Raw Dict -> MarketObservation (Industrial Standard)
            # Pastikan timestamp valid (handle float/int/datetime di luar atau di sini)
            ts = raw_data.get('timestamp', int(time.time()*1000))
            
            observation = MarketObservation(
                timestamp=ts,
                data=raw_data,
                source="live_feed",
                symbol=raw_data.get('symbol', 'UNKNOWN')
            )

            # --- PHASE 2: BRAIN (Strategy Logic) ---
            # Signal Generation (Kalman Calculation)
            sig_res = self.strategy.evaluate_state(observation)
            
            if sig_res.is_err():
                # Error di strategi (misal data kurang) bukan fatal error engine
                # Cukup log warning dan skip tick ini
                return Err(f"Strategy Skip: {sig_res.unwrap_err()}")
            
            signal = sig_res.unwrap()

            # Filter Noise: Jika NEUTRAL, berhenti di sini
            if signal.signal_type == SignalType.NEUTRAL:
                if self.tick_count % 50 == 0: # Heartbeat log
                    logger.info(f"💤 Monitoring... Z-Score: {signal.strength:.2f}")
                return Ok("NEUTRAL")

            # --- PHASE 3: SHIELD (Risk Check) ---
            # Validasi apakah sinyal aman untuk dieksekusi
            logger.info(f"🚨 SIGNAL DETECTED: {signal.signal_type.name} | Z: {signal.strength:.2f}")
            
            if not self.risk.validate(signal):
                return Ok("REJECTED_BY_RISK")

            # --- PHASE 4: HANDS (Execution) ---
            # Kirim ke Binance/Exchange
            exec_res = self.oms.execute(signal)
            
            if exec_res.is_ok():
                return Ok(f"EXECUTED: {exec_res.unwrap()}")
            else:
                return Err(f"OMS Failed: {exec_res.unwrap_err()}")

        except Exception as e:
            logger.error(f"CRITICAL LOOP ERROR: {e}", exc_info=True)
            return Err(f"Crash: {str(e)}")

# ==============================================================================
# DIRECT RUNNER (Untuk Test Manual Cepat)
# ==============================================================================
if __name__ == "__main__":
    # Test sederhana seolah-olah ada data masuk
    engine = TradingEngine()
    
    # Simulasi Data Dummy
    import math
    
    print("\n📺 STARTING SIMULATION FEED...")
    for i in range(50):
        # Bikin harga sinusoidal supaya trigger signal
        price = 100 + 5 * math.sin(i * 0.5) 
        
        dummy_packet = {
            "timestamp": int(time.time() * 1000),
            "symbol": "DOGE/USDT",
            "close_DOGE": price,
            "close_BTC": 100, # Flat
            "volume": 1000
        }
        
        engine.process_tick(dummy_packet)
        time.sleep(0.05)
