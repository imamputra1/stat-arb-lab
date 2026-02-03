"""
QUANTUM COMMAND CENTER (THE INTEGRATED WAR-DASHBOARD) - V9.0 INDUSTRIAL
Location: research/strategy/optimization/war_room.py
Focus: Automated Alpha hunting with real-time visualization pipeline and adaptive resource management.
Architecture: Unified Orchestrator with Self-Healing Optimization and Instant Visual Intelligence.
"""

import sys
import time
import json
import signal
import psutil
import warnings
import logging
import webbrowser
from dataclasses import dataclass, field, asdict
from typing import List, Tuple, Optional, Dict, Any
from datetime import datetime, timedelta
from pathlib import Path
from enum import Enum, auto
from threading import Thread, Event

# Suppress warnings for clean industrial output
warnings.filterwarnings('ignore')

# --- ABSOLUTE PATH INJECTION & SHARED SYNC ---
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent.absolute()
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from research.shared import Result, Ok, Err
from research.strategy.optimization.shotgun import HyperParallelEngine
from research.strategy.optimization.storage import OptimizationClerk, QuantumVault
from research.strategy.optimization.dashboard import QuantumDashboard

# --- INDUSTRIAL TELEMETRY ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(name)-24s | %(levelname)-8s | %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger("QuantumCommandCenter")

# --- MISSION LIFECYCLE MANAGEMENT ---
class MissionPhase(Enum):
    """Mission lifecycle phases for state machine tracking."""
    IDLE = auto()
    PREPARATION = auto()
    OPTIMIZATION = auto()
    VISUALIZATION = auto()
    ANALYSIS = auto()
    REPORTING = auto()
    COMPLETE = auto()
    FAILED = auto()

@dataclass
class MissionState:
    """Complete mission state tracking for Node O diagnostics."""
    mission_id: str
    phase: MissionPhase = MissionPhase.IDLE
    target_pair: Tuple[str, str] = ("DOGE", "BTC")
    total_combos: int = 2000
    completed_combos: int = 0
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    results_path: Optional[Path] = None
    dashboard_paths: Dict[str, Path] = field(default_factory=dict)
    resource_snapshot: Dict[str, Any] = field(default_factory=dict)
    performance_metrics: Dict[str, Any] = field(default_factory=dict)
    errors: List[str] = field(default_factory=list)
    
    @property
    def duration(self) -> Optional[timedelta]:
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        elif self.start_time:
            return datetime.now() - self.start_time
        return None
    
    @property
    def progress_percentage(self) -> float:
        return (self.completed_combos / max(self.total_combos, 1)) * 100 if self.total_combos > 0 else 0

# --- RESOURCE AWARENESS (THE RYZEN GUARD) ---
class QuantumResourceManager:
    """Intelligent resource manager for Ryzen 5 5600U (12 Threads)."""
    def __init__(self, power_profile: str = 'BALANCED'):
        self.profile = power_profile
        self.cpu_threads = psutil.cpu_count(logical=True)
        self.max_temp = 85.0  # Safe threshold for Aero-13
        self.stop_event = Event()
        
        # State tracking
        self.current_temp = 0.0
        self.cpu_usage = 0.0
        self.ram_usage = 0.0
        self.optimal_jobs = self._get_base_jobs()
        self.max_batch_size = 500
        
        self.monitor_thread = Thread(target=self._monitor_loop, daemon=True)
        self.monitor_thread.start()

    def _get_base_jobs(self) -> int:
        if self.profile == 'PERFORMANCE': return self.cpu_threads
        if self.profile == 'POWER_SAVER': return self.cpu_threads // 3
        return self.cpu_threads // 2

    def _monitor_loop(self):
        while not self.stop_event.is_set():
            try:
                self.cpu_usage = psutil.cpu_percent(interval=1)
                self.ram_usage = psutil.virtual_memory().percent
                # Temperature monitoring for Ryzen
                temps = psutil.sensors_temperatures()
                if 'k10temp' in temps:
                    self.current_temp = temps['k10temp'][0].current
                
                self._apply_adaptive_throttling()
                time.sleep(2)
            except Exception: pass

    def _apply_adaptive_throttling(self):
        """Adaptive throttling to prevent thermal shutdown."""
        if self.current_temp > self.max_temp:
            self.optimal_jobs = max(2, self.optimal_jobs - 2)
            logger.warning(f"🌡️ Thermal Guard Active: {self.current_temp}C. Reducing threads.")
        if self.ram_usage > 90:
            self.max_batch_size = max(50, self.max_batch_size // 2)
            logger.warning(f"💾 RAM Guard Active: {self.ram_usage}%. Reducing batch size.")

    def get_safe_configuration(self) -> Dict[str, Any]:
        return {
            'n_jobs': self.optimal_jobs,
            'max_combos_per_batch': self.max_batch_size,
            'metrics': {'cpu': self.cpu_usage, 'ram': self.ram_usage, 'temp': self.current_temp}
        }

    def stop_monitoring(self):
        self.stop_event.set()

# --- INTELLIGENT BATCH SCHEDULER ---
class QuantumBatchScheduler:
    """High-level batch orchestrator with fault tolerance."""
    def __init__(self, resource_manager: QuantumResourceManager):
        self.rm = resource_manager
        self.clerk = OptimizationClerk()

    def execute_mission_batches(self, target_pair: Tuple[str, str], space_name: str, 
                               total: int, mission_id: str) -> Result[Path, str]:
        """Splits optimization mission into safe, manageable shards."""
        completed = 0
        batch_idx = 0
        
        while completed < total:
            config = self.rm.get_safe_configuration()
            current_batch_size = min(config['max_combos_per_batch'], total - completed)
            
            # Thermal cool-down check
            if config['metrics']['temp'] > 88.0:
                logger.info("⏸️ Thermal Cool-down in progress... waiting 30s")
                time.sleep(30)
                continue

            engine = HyperParallelEngine(n_jobs=config['n_jobs'])
            logger.info(f"🚀 Firing Batch {batch_idx} | Size: {current_batch_size} | Jobs: {config['n_jobs']}")
            
            res = engine.fire(
                target_pairs=[target_pair],
                space_name=space_name,
                max_combos=current_batch_size,
                batch_id=f"{mission_id}_B{batch_idx}"
            )
            
            if res.is_err():
                logger.error(f"❌ Batch {batch_idx} Failed: {res.error}")
                # We continue to next batch to maximize discovery despite failures
            
            completed += current_batch_size
            batch_idx += 1
            
        return Ok(self.clerk.root)

# --- REAL-TIME TELEMETRY & ANALYTICS ---
class LiveTelemetry:
    """Real-time monitoring and predictive time estimation."""
    def __init__(self):
        self.history = []
        
    def capture(self, state: MissionState, rm: QuantumResourceManager):
        config = rm.get_safe_configuration()
        snapshot = {
            'timestamp': datetime.now(),
            'progress': state.progress_percentage,
            'cpu': config['metrics']['cpu'],
            'ram': config['metrics']['ram'],
            'temp': config['metrics']['temp'],
            'throughput': self._calc_throughput(state)
        }
        self.history.append(snapshot)
        return snapshot

    def _calc_throughput(self, state: MissionState) -> float:
        if not state.start_time or state.completed_combos == 0: return 0.0
        elapsed = (datetime.now() - state.start_time).total_seconds()
        return state.completed_combos / elapsed if elapsed > 0 else 0.0

# --- THE UNIFIED COMMAND CENTER ---
class QuantumCommandCenter:
    """
    Unified Orchestrator for the entire Node O pipeline.
    Combines War Room (Search) and Dashboard (Intelligence).
    """
    def __init__(self, power_profile: str = 'BALANCED', auto_viz: bool = True):
        self.resource_manager = QuantumResourceManager(power_profile)
        self.batch_scheduler = QuantumBatchScheduler(self.resource_manager)
        self.telemetry = LiveTelemetry()
        self.dashboard = QuantumDashboard()
        self.clerk = OptimizationClerk()
        self.vault = QuantumVault()
        
        self.auto_viz = auto_viz
        self.mission_state: Optional[MissionState] = None
        self.stop_signal = Event()
        
        signal.signal(signal.SIGINT, self._graceful_exit)

    def _graceful_exit(self, signum, frame):
        logger.warning("🛑 INTERRUPT RECEIVED. Safeguarding Ryzen 5 and archiving state...")
        self.stop_signal.set()
        self.resource_manager.stop_monitoring()
        sys.exit(0)

    def launch_mission(self, target_pair: Tuple[str, str], total_combos: int = 2000, 
                      space_name: str = "shotgun") -> Result[MissionState, str]:
        """Main entry point for integrated Alpha Hunting missions."""
        mission_id = f"MISSION_{target_pair[0]}_{datetime.now().strftime('%m%d_%H%M')}"
        self.mission_state = MissionState(mission_id=mission_id, target_pair=target_pair, 
                                         total_combos=total_combos, start_time=datetime.now())
        
        try:
            # Phase 1: Preparation
            self.mission_state.phase = MissionPhase.PREPARATION
            self.clerk.cleanup_artifacts(days_old=1)
            
            # Phase 2: Optimization Sharding
            self.mission_state.phase = MissionPhase.OPTIMIZATION
            opt_res = self.batch_scheduler.execute_mission_batches(
                target_pair=target_pair, 
                space_name=space_name, 
                total=total_combos, 
                mission_id=mission_id
            )
            
            if opt_res.is_err(): return Err(opt_res.error)
            self.mission_state.completed_combos = total_combos
            
            # Phase 3: Autopilot Visualization
            if self.auto_viz:
                self.mission_state.phase = MissionPhase.VISUALIZATION
                logger.info("🎨 Mission complete. Triggering Autopilot Dashboard...")
                dash_res = self.dashboard.generate_dashboard(layout_name="full_analytics", interactive=True)
                if dash_res.is_ok():
                    self.mission_state.dashboard_paths = dash_res.unwrap()
                    self._open_autopilot_visuals()

            # Phase 4: Final Reporting
            self.mission_state.phase = MissionPhase.REPORTING
            self.mission_state.end_time = datetime.now()
            self._save_mission_artifact()
            
            self.mission_state.phase = MissionPhase.COMPLETE
            return Ok(self.mission_state)

        except Exception as e:
            self.mission_state.phase = MissionPhase.FAILED
            self.mission_state.errors.append(str(e))
            return Err(f"Mission {mission_id} Crash: {str(e)}")
        finally:
            self.resource_manager.stop_monitoring()

    def _open_autopilot_visuals(self):
        """Automatically launches reports in browser."""
        for name, path in self.mission_state.dashboard_paths.items():
            if isinstance(path, Path) and path.suffix in ['.html', '.png']:
                logger.info(f"🔓 Opening {name} in browser...")
                webbrowser.open(f"file://{path.absolute()}")

    def _save_mission_artifact(self):
        """Persists mission metadata for future analytics."""
        path = PROJECT_ROOT / "research" / "missions" / f"{self.mission_state.mission_id}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, 'w') as f:
            json.dump(asdict(self.mission_state), f, indent=2, default=str)

    def _display_summary(self):
        """Industrial terminal summary."""
        s = self.mission_state
        print("\n" + "🏁 MISSION SUMMARY ".center(60, "="))
        print(f"ID         : {s.mission_id}")
        print(f"Status     : {s.phase.name}")
        print(f"Duration   : {s.duration}")
        print(f"Combos     : {s.completed_combos}/{s.total_combos}")
        if s.dashboard_paths:
            print(f"Dashboards : {len(s.dashboard_paths)} generated")
        print("=" * 60 + "\n")

# --- MISSION CONTROL INTERFACE ---
def launch_mission_control():
    """Main CLI entry point for the integrated Quant Lab."""
    print("""
    ╔══════════════════════════════════════════════════════════╗
    ║               QUANTUM COMMAND CENTER v9.0                ║
    ║         Automated Alpha Hunter (Ryzen 5 Ready)           ║
    ╚══════════════════════════════════════════════════════════╝
    """)
    
    # Configuration prompts
    print("1. Quick Scan   (500 combos | Balanced)")
    print("2. Standard Hunt (2000 combos | Performance)")
    print("3. Deep Analysis (5000 combos | Heavy Duty)")
    print("4. Custom Mission")
    
    try:
        choice = int(input("\nSelect profile (1-4): ").strip())
    except: choice = 2

    target, anchor = ("DOGE", "BTC")
    if choice == 1: combos, profile = 500, 'BALANCED'
    elif choice == 2: combos, profile = 2000, 'PERFORMANCE'
    elif choice == 3: combos, profile = 5000, 'PERFORMANCE'
    else:
        target = input("Target Coin: ").upper()
        combos = int(input("Total Combos: "))
        profile = 'PERFORMANCE'

    cc = QuantumCommandCenter(power_profile=profile, auto_viz=True)
    res = cc.launch_mission(target_pair=(target, anchor), total_combos=combos)
    
    if res.is_ok():
        cc._display_summary()
        return True
    else:
        print(f"❌ MISSION FAILED: {res.error}")
        return False

if __name__ == "__main__":
    launch_mission_control()
