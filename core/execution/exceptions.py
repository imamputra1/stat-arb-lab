"""
EXECUTION EXCEPTIONS
Location: core/execution/exceptions.py
Desc: Standardized exceptions for execution subsystem.
"""

from typing import Optional

try:
    import orjson as json
except ImportError:
    import json
    # Monkey patch agar interface mirip orjson (optional, tapi aman)
    if not hasattr(json, "dumps_bytes"):
        # orjson return bytes, json return str. Kita handle nanti jika perlu.
        pass

class ExecutionError(Exception):
    """Base class untuk semua error eksekusi"""
    def __init__(self, message: str, order_id: Optional[str] = None, code: str = "EXEC_ERR"):
        self.order_id = order_id
        self.code = code
        super().__init__(message)

class InsufficientFundsError(ExecutionError):
    """Saldo tidak cukup"""
    def __init__(self, message: str, order_id: Optional[str] = None):
        super().__init__(message, order_id, "INSUFFICIENT_FUNDS")

class OrderNotFoundError(ExecutionError):
    """Order ID tidak ditemukan"""
    def __init__(self, message: str, order_id: Optional[str] = None):
        super().__init__(message, order_id, "ORDER_NOT_FOUND")

class RateLimitError(ExecutionError):
    """Terkena limit API"""
    def __init__(self, message: str):
        super().__init__(message, None, "RATE_LIMIT")
