"""
PERFORMANCE MONITORING UTILITIES
Location: core/shared/performance.py
Focus: Industrial-grade telemetry and latency tracking.
"""
import time
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

class PerformanceMonitor:
    """
    Monitor performa dengan buffer melingkar (Circular Buffer).
    Mendukung pelacakan latensi rata-rata dan pencatatan batch.
    """
    def __init__(self, history_size: int = 1000):
        self._history: deque = deque(maxlen=history_size)
        self._timers: Dict[str, float] = {}

    def start_timer(self, label: str) -> None:
        """Memulai penghitungan waktu untuk label tertentu."""
        self._timers[label] = time.perf_counter()

    def stop_timer(self, label: str, metadata: Optional[Dict[str, Any]] = None) -> float:
        """Menghentikan timer dan mencatat durasi dalam milidetik."""
        if label not in self._timers:
            return 0.0
        
        start_time = self._timers.pop(label)
        duration_ms = (time.perf_counter() - start_time) * 1000
        
        metric = PerformanceMetrics(
            label=label,
            duration_ms=duration_ms,
            metadata=metadata or {}
        )
        self._history.append(metric)
        return duration_ms

    def record_batch(self, metrics_obj: Any) -> None:
        """Pencatatan batch metrik dari SignalGenerator."""
        self._history.append(metrics_obj)

    def get_avg_latency(self, label: str) -> float:
        """Menghitung rata-rata latensi untuk label tertentu."""
        relevant = [m.duration_ms for m in self._history 
                    if hasattr(m, 'label') and m.label == label]
        return sum(relevant) / len(relevant) if relevant else 0.0
