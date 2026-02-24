"""
PERFORMANCE MONITORING UTILITIES
Location: core/shared/performance.py
Focus: Industrial-grade telemetry and latency tracking.
"""
import time
import statistics
from dataclasses import dataclass, field
from typing import Dict, Any, Optional
from datetime import datetime, timezone
from collections import deque

@dataclass
class PerformanceMetrics:
    """Snapshot metrik performa untuk satu batch operasi."""
    label: str
    duration_ms: float
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    metadata: Dict[str, Any] = field(default_factory=dict)
    is_error: bool = False

class PerformanceMonitor:
    """
    Monitor performa dengan buffer melingkar (Circular Buffer).
    Mendukung pelacakan latensi, throughput, dan error rate.
    """
    def __init__(self, history_size: int = 1000):
        self._history: deque = deque(maxlen=history_size)
        self._timers: Dict[str, float] = {}
        
        # Operational Counters (Attributes yang hilang sebelumnya)
        self.total_operations: int = 0
        self.success_count: int = 0
        self.error_count: int = 0

    def start_timer(self, label: str) -> None:
        """Memulai penghitungan waktu untuk label tertentu."""
        self._timers[label] = time.perf_counter()

    def stop_timer(self, label: str, metadata: Optional[Dict[str, Any]] = None, is_error: bool = False) -> float:
        """Menghentikan timer dan mencatat durasi dalam milidetik."""
        if label not in self._timers:
            return 0.0
        
        start_time = self._timers.pop(label)
        duration_ms = (time.perf_counter() - start_time) * 1000
        
        self.record_metric(label, duration_ms, metadata, is_error)
        return duration_ms

    def record_metric(self, label: str, duration_ms: float, metadata: Optional[Dict[str, Any]] = None, is_error: bool = False) -> None:
        """Mencatat metrik manual tanpa timer."""
        metric = PerformanceMetrics(
            label=label,
            duration_ms=duration_ms,
            metadata=metadata or {},
            is_error=is_error
        )
        self._history.append(metric)
        
        # Update Live Counters
        self.total_operations += 1
        if is_error:
            self.error_count += 1
        else:
            self.success_count += 1

    def record_batch(self, metrics_obj: Any) -> None:
        """Pencatatan batch metrik (Legacy/Generic Support)."""
        self._history.append(metrics_obj)
        self.total_operations += 1

    def get_avg_latency(self, label: str) -> float:
        """Menghitung rata-rata latensi untuk label tertentu."""
        relevant = [m.duration_ms for m in self._history 
                    if isinstance(m, PerformanceMetrics) and m.label == label]
        return statistics.mean(relevant) if relevant else 0.0

    def get_max_latency(self, label: str) -> float:
        """Mendapatkan max latency."""
        relevant = [m.duration_ms for m in self._history 
                    if isinstance(m, PerformanceMetrics) and m.label == label]
        return max(relevant) if relevant else 0.0

    def get_success_rate(self) -> float:
        """Menghitung success rate (0.0 - 1.0)."""
        if self.total_operations == 0:
            return 1.0 # Optimistic default
        return self.success_count / self.total_operations

    def get_summary(self) -> Dict[str, Any]:
        """Dump semua statistik vital."""
        return {
            "total_ops": self.total_operations,
            "success_rate": self.get_success_rate(),
            "errors": self.error_count,
            "history_len": len(self._history)
        }

    def reset(self) -> None:
        """Reset monitor state."""
        self._history.clear()
        self._timers.clear()
        self.total_operations = 0
        self.success_count = 0
        self.error_count = 0


__all__ = ['PerformanceMonitor', 'PerformanceMetrics']
