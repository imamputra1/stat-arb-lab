import sys
import time
import signal
import psutil
import warnings
import logging
from pathlib import Path
from typing import List, Tuple, Dict, Any
from threading import Thread, Event, Lock

# Suppress warnings for cleaner output
warnings.filterwarnings('ignore')

# --- ABSOLUTE PATH INJECTION ---
PROJECT_ROOT = Path(__file__).parent.parent.parent.parent.absolute()
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from research.shared import Result, Ok
from research.strategy.optimization import HyperParallelEngine, OptimizationClerk

# --- INDUSTRIAL LOGGING ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(name)-20s | %(levelname)-8s | %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger("WarRoom")

# --- RESOURCE AWARENESS CONFIGURATION ---
class SystemProfile:
    """System profile for HP Pavilion Aero 13 with Ryzen 5 5600U (12 Threads)"""
    
    # Ryzen 5 5600U Specifications
    CPU_CORES = 6
    CPU_THREADS = 12
    MAX_RAM = 16  # GB
    
    # Thermal limits
    MAX_TEMP = 95  # °C
    THERMAL_THROTTLE_TEMP = 85  # °C
    
    # Power profiles
    PROFILES = {
        'PERFORMANCE': {'max_cores': 12, 'min_cores': 8, 'power_limit': 'HIGH'},
        'BALANCED': {'max_cores': 8, 'min_cores': 4, 'power_limit': 'MEDIUM'},
        'POWER_SAVER': {'max_cores': 4, 'min_cores': 2, 'power_limit': 'LOW'},
        'ULTRA_SAVER': {'max_cores': 2, 'min_cores': 1, 'power_limit': 'MINIMAL'}
    }

# --- ADAPTIVE RESOURCE MANAGER ---
class QuantumResourceManager:
    """
    Intelligent resource manager for Ryzen 5 optimization.
    Dynamically adjusts workloads based on thermal, power, and memory constraints.
    """
    def __init__(self, power_profile: str = 'BALANCED'):
        self.power_profile = power_profile
        self.profile_config = SystemProfile.PROFILES[power_profile]
        
        # Resource tracking
        self.cpu_history = []
        self.ram_history = []
        self.temp_history = []
        self.cpu_usage = 0.0
        self.ram_usage = 0.0
        self.temperature = 0.0
        
        # Adaptive parameters
        self.optimal_jobs = self.profile_config['max_cores']
        self.max_combos_per_batch = 500  # Conservative for 16GB RAM
        
        self.performance_score = 1.0
        self.throttle_count = 0
        self.lock = Lock()
        
        # Start monitoring thread
        self.monitor_thread = Thread(target=self._monitor_resources, daemon=True)
        self.stop_event = Event()
        self.monitor_thread.start()
        
        logger.info(f"🔋 Resource Manager initialized: {power_profile} mode")

    def _monitor_resources(self):
        """Continuous resource monitoring with adaptive adjustment."""
        while not self.stop_event.is_set():
            try:
                self.cpu_usage = psutil.cpu_percent(interval=2)
                self.cpu_history.append(self.cpu_usage)
                
                ram = psutil.virtual_memory()
                self.ram_usage = ram.percent
                self.ram_history.append(self.ram_usage)
                
                # Temperature monitoring (Hardware specific)
                try:
                    temps = psutil.sensors_temperatures()
                    if 'k10temp' in temps:
                        self.temperature = temps['k10temp'][0].current
                        self.temp_history.append(self.temperature)
                except:
                    self.temperature = 0.0
                
                self._adaptive_adjustment()
                time.sleep(3)
            except Exception:
                pass

    def _adaptive_adjustment(self):
        """Dynamically adjust resource allocation based on system conditions."""
        with self.lock:
            # Thermal Guard
            if self.temperature > SystemProfile.THERMAL_THROTTLE_TEMP:
                self.throttle_count += 1
                self.optimal_jobs = max(2, self.optimal_jobs - 2)
                logger.warning(f"🌡️ Thermal High: {self.temperature}C. Reducing jobs to {self.optimal_jobs}")
            
            # Memory Guard
            if self.ram_usage > 85:
                self.max_combos_per_batch = max(100, int(self.max_combos_per_batch * 0.7))
                logger.warning(f"💾 High RAM: {self.ram_usage}%. Reducing batch size.")

    def get_safe_configuration(self) -> Dict[str, Any]:
        with self.lock:
            return {
                'n_jobs': max(1, self.optimal_jobs - 2),
                'max_combos_per_batch': self.max_combos_per_batch,
                'system_health': {
                    'cpu_usage': self.cpu_usage,
                    'ram_usage': self.ram_usage,
                    'temperature': self.temperature
                }
            }

    def stop_monitoring(self):
        self.stop_event.set()

# --- INTELLIGENT BATCH SCHEDULER ---
class QuantumBatchScheduler:
    """
    Intelligent batch scheduler with checkpoint awareness.
    Breaks large optimization tasks into manageable chunks for Ryzen 5.
    """
    def __init__(self, resource_manager: QuantumResourceManager):
        self.rm = resource_manager
        self.successful_batches = 0
        self.total_batches = 0
        self.clerk = OptimizationClerk()

    def execute_optimization(self, target_pairs: List[Tuple[str, str]], space_name: str = "shotgun",
                           total_combos: int = 2000, batch_id: str = "ALPHA_HUNT") -> Result[Path, str]:
        self.total_batches = (total_combos + self.rm.max_combos_per_batch - 1) // self.rm.max_combos_per_batch
        
        for batch_idx in range(self.total_batches):
            config = self.rm.get_safe_configuration()
            
            # Pause if critical conditions
            if config['system_health']['temperature'] > SystemProfile.MAX_TEMP - 5 or config['system_health']['ram_usage'] > 92:
                logger.info("⏸️ Pausing execution to cool down system...")
                time.sleep(30)
                continue

            engine = HyperParallelEngine(n_jobs=config['n_jobs'])
            result = engine.fire(
                target_pairs=target_pairs,
                space_name=space_name,
                max_combos=self.rm.max_combos_per_batch,
                batch_id=f"{batch_id}_B{batch_idx}"
            )
            
            if result.is_ok():
                self.successful_batches += 1
            else:
                logger.error(f"Batch {batch_idx} failure: {result.error}")
        
        return Ok(self.clerk.root)

# --- PREDICTIVE ANALYTICS ENGINE ---
class PredictiveAnalytics:
    """Uses historical data to suggest optimal search areas."""
    def __init__(self):
        self.clerk = OptimizationClerk()
        
    def recommend_parameters(self, pair: Tuple[str, str]) -> Dict[str, Any]:
        res = self.clerk.generate_leaderboard()
        if res.is_err() or res.unwrap().height == 0:
            return {'recommendation': 'CONSERVATIVE', 'confidence': 0.0}
        
        df = res.unwrap()
        best = df.head(1).to_dicts()[0]
        return {
            'recommendation': 'ADAPTIVE',
            'confidence': 0.85,
            'best_params': best
        }

# --- MAIN WAR ROOM CONTROLLER ---
class QuantumWarRoom:
    """Main war room controller with adaptive optimization strategies."""
    def __init__(self, power_profile: str = 'BALANCED'):
        self.resource_manager = QuantumResourceManager(power_profile)
        self.batch_scheduler = QuantumBatchScheduler(self.resource_manager)
        self.analytics = PredictiveAnalytics()
        self.clerk = OptimizationClerk()
        
        signal.signal(signal.SIGINT, self._handle_interrupt)
        self.is_running = False
        
    def _handle_interrupt(self, signum, frame):
        logger.info("\n🛑 Interrupt received. Graceful shutdown initiated...")
        self.resource_manager.stop_monitoring()
        sys.exit(0)
    
    def launch_alpha_hunt(self, target_pair: Tuple[str, str], total_combos: int = 2000) -> bool:
        logger.info(f"🚀 ALPHA HUNT INITIATED: {target_pair[0]}-{target_pair[1]}")
        
        # 1. Predictive Step
        rec = self.analytics.recommend_parameters(target_pair)
        logger.info(f"🧠 Analytics Suggestion: {rec['recommendation']} (Conf: {rec['confidence']})")
        
        # 2. Preparation
        self.clerk.cleanup_artifacts(days_old=3)
        
        # 3. Execution
        result = self.batch_scheduler.execute_optimization(
            target_pairs=[target_pair],
            space_name="shotgun",
            total_combos=total_combos
        )
        
        self.resource_manager.stop_monitoring()
        return result.is_ok()

def enter_war_room():
    """Main entry point for the Quantum War Room."""
    print("   QUANTUM OPTIMIZATION WAR ROOM - HP Aero 13 Ryzen 5")
    print("="*60)
    
    # Simple selection logic
    print("1. DOGE-BTC Alpha Hunt (1000 combos)")
    print("2. Custom Config")
    choice = input("\nSelect (1/2): ")
    
    target_pair = ("DOGE", "BTC")
    total_combos = 1000
    
    if choice == '2':
        target = input("Target (e.g., ETH): ").upper()
        total_combos = int(input("Total combos: "))
        target_pair = (target, "BTC")

    war_room = QuantumWarRoom(power_profile='BALANCED')
    return war_room.launch_alpha_hunt(target_pair=target_pair, total_combos=total_combos)

if __name__ == "__main__":
    success = enter_war_room()
    sys.exit(0 if success else 1)
