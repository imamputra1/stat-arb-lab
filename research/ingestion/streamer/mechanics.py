"""
STREAMER MECHANICS (INDUSTRIAL GRADE)
Location: research/ingestion/streamer/mechanics.py
Desc: 'Director' yang mengatur koreografi chaos pada data stream.
      Terintegrasi langsung dengan Core Chaos Engine tanpa mock.
"""

import random
import time
from dataclasses import dataclass
from typing import Optional, Dict, Any, List

from core.data.types import MarketTick
from core.data.chaos import create_chaos_injector, ChaosInjector
from core.shared.result import Result, Ok
from core.shared.performance import PerformanceMonitor
from core.shared.utils import get_logger

logger = get_logger("streamer.mechanics")

@dataclass
class ChaosEventLog:
    """Audit trail untuk setiap kejadian chaos"""
    timestamp: float
    event_type: str
    duration_ticks: int
    severity: str
    description: str

class StreamMechanic:
    """
    Mengontrol injeksi chaos ke dalam stream data.
    Menggunakan pattern 'apply_mechanics' -> '_apply_chaos'.
    """
    
    def __init__(self, enable_chaos: bool = False, probability: float = 0.01):
        self.enabled = enable_chaos
        self.probability = probability
        
        # Performance & Metrics
        self.metrics = PerformanceMonitor(history_size=1000)
        self.event_history: List[ChaosEventLog] = []
        
        # Core Chaos Engine
        # Kita assume core.data sudah lengkap, tidak pakai mock
        self.injector: ChaosInjector = create_chaos_injector(enable=enable_chaos)
        
        # Internal State
        self._active_scenario_type: Optional[str] = None
        self._scenario_tick_counter: int = 0
        self._scenario_duration_ticks: int = 0

        if self.enabled:
            logger.info(f"🌪️ Chaos Mechanics ARMED (Prob: {self.probability:.1%})")

    def apply_mechanics(self, tick: MarketTick) -> Result[MarketTick, str]:
        """
        [PUBLIC API] Pintu masuk utama untuk memproses tick.
        Mengembalikan Result agar error handling rapi.
        """
        try:
            # 1. Jika chaos dimatikan, pass-through secepat kilat (Fast Path)
            if not self.enabled:
                return Ok(tick)

            # 2. Delegasikan ke Private Method (sesuai request)
            modified_tick = self._apply_chaos(tick)
            
            return Ok(modified_tick)
            
        except Exception as e:
            logger.error(f"Mechanics Error: {str(e)}")
            # Fail-safe: Kembalikan tick asli jika engine chaos meledak
            return Ok(tick)

    def _apply_chaos(self, tick: MarketTick) -> MarketTick:
        """
        [PRIVATE] Logika inti injeksi chaos.
        Memisahkan logic 'kapan' (Trigger) dan 'bagaimana' (Injector).
        """
        # A. Trigger Check (Hanya jika idle)
        if self._active_scenario_type is None:
            if random.random() < self.probability:
                self._trigger_random_scenario()

        # B. Execution (Jika ada scenario aktif)
        if self._active_scenario_type:
            # Injector Core melakukan tugas kotornya (memodifikasi harga/vol/meta)
            tick = self.injector.apply_chaos(tick)
            
            # Update Internal Counter
            self._scenario_tick_counter += 1
            
            # Check Expiration
            if self._scenario_tick_counter >= self._scenario_duration_ticks:
                self._end_scenario()

        return tick

    def _trigger_random_scenario(self):
        """Memutar dadu nasib untuk memilih skenario bencana"""
        dice = random.random()
        
        if dice < 0.30:
            self._activate_flash_crash()
        elif dice < 0.60:
            self._activate_zombie_feed()
        else:
            self._activate_latency_spike()

    def _activate_flash_crash(self):
        """Skenario: Harga jatuh 15% dalam 60 detik"""
        self._active_scenario_type = "FLASH_CRASH"
        self._scenario_duration_ticks = 100 
        
        self.injector.inject_flash_crash(drop_pct=15.0, duration_sec=60.0)
        self._log_event("FLASH_CRASH", 100, "CRITICAL", "Market Panic -15%")
        logger.warning("📉 TRIGGERED: FLASH CRASH (-15%)")

    def _activate_zombie_feed(self):
        """Skenario: Data macet (Stale) selama 2 menit"""
        self._active_scenario_type = "ZOMBIE_FEED"
        self._scenario_duration_ticks = 200
        
        self.injector.inject_zombie_feed(duration_sec=120.0, staleness_sec=10.0)
        self._log_event("ZOMBIE_FEED", 200, "HIGH", "Stale Data Feed")
        logger.warning("🧟 TRIGGERED: ZOMBIE FEED (Dead Air)")

    def _activate_latency_spike(self):
        """Skenario: Lag jaringan 2000ms"""
        self._active_scenario_type = "LATENCY_SPIKE"
        self._scenario_duration_ticks = 150
        
        self.injector.inject_latency_spike(delay_ms=2000, probability=0.8)
        self._log_event("LATENCY_SPIKE", 150, "MEDIUM", "Network Congestion")
        logger.warning("🐌 TRIGGERED: LATENCY SPIKE (2000ms)")

    def _end_scenario(self):
        """Reset chaos engine ke kondisi normal"""
        self.injector.reset()
        self._active_scenario_type = None
        self._scenario_tick_counter = 0
        logger.info("✅ SCENARIO ENDED: Returning to Normal")

    def _log_event(self, type: str, duration: int, severity: str, desc: str):
        """Mencatat kejadian ke audit log"""
        log = ChaosEventLog(
            timestamp=time.time(),
            event_type=type,
            duration_ticks=duration,
            severity=severity,
            description=desc
        )
        self.event_history.append(log)

    def get_stats(self) -> Dict[str, Any]:
        """Telemetri untuk monitoring"""
        return {
            "active_scenario": self._active_scenario_type,
            "total_events": len(self.event_history),
            "injector_active": self.injector.is_active if hasattr(self.injector, 'is_active') else False
        }
