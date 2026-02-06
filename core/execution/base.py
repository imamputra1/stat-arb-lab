"""
BASE EXECUTION HANDLER
Location: core/execution/base.py
Desc: Abstract Base Class yang menyediakan shared logic untuk semua executor.
      Implementasi pattern: Template Method.
"""

import asyncio
from abc import ABC, abstractmethod
from typing import List
from datetime import datetime

# Imports
from core.shared.result import Result, Ok, is_err
from core.shared.utils import get_logger
from .types import Order, OrderRequest, OrderResult, OrderStatus

logger = get_logger(__name__)

class BaseExecutionHandler(ABC):
    """
    Base class untuk Simulator dan Live Executor.
    Menyediakan fitur 'Smart Polling' dan error handling dasar.
    """
    
    def __init__(self):
        self._started_at = datetime.now()

    # ==================== SMART POLLING LOGIC ====================
    
    async def submit_and_wait(
        self, 
        request: OrderRequest, 
        timeout_sec: float = 10.0,
        poll_interval: float = 0.5
    ) -> OrderResult:
        """
        [DEBUGGED] Submit order dan block sampai status TERMINAL.
        Fixes: UnboundLocalError, DeprecationWarning, Missing Attribute.
        """
        # 1. Submit (Delegasi ke child implementation)
        submit_res = await self.submit_order(request)
        
        if is_err(submit_res):
            logger.error(f"Submit failed: {submit_res.unwrap_err()}")
            return submit_res
            
        # [FIX 1] Inisialisasi variabel di luar loop
        # Agar jika loop tidak jalan, kita tetap punya data untuk direturn
        current_order = submit_res.unwrap() 
        order_id = current_order.order_id
        
        # [FIX 2] Gunakan get_running_loop (Python 3.10+ safe)
        loop = asyncio.get_running_loop()
        start_time = loop.time()
        
        # 2. Polling Loop
        while (loop.time() - start_time) < timeout_sec:
            fetch_res = await self.get_order(order_id)
            
            if is_err(fetch_res):
                logger.warning(f"Polling warning for {order_id}: {fetch_res.unwrap_err()}")
                await asyncio.sleep(poll_interval)
                continue
                
            current_order = fetch_res.unwrap()
            
            # [FIX 3] Cek status manual jika property is_terminal belum ada di types.py
            # Atau pastikan Anda menambahkan property is_terminal di types.py
            terminal_statuses = [
                OrderStatus.FILLED, 
                OrderStatus.CANCELED, 
                OrderStatus.REJECTED
            ]
            
            if current_order.status in terminal_statuses:
                logger.info(f"Order {order_id} finished with status: {current_order.status}")
                return Ok(current_order)
                
            await asyncio.sleep(poll_interval)
            
        # 3. Timeout Handling
        logger.warning(f"Order {order_id} timed out after {timeout_sec}s. Cancelling...")
        
        # Coba cancel
        await self.cancel_order(order_id)
        
        # Return state terakhir (sekarang aman karena current_order sudah di-init di atas)
        return Ok(current_order)
    # ==================== ABSTRACT METHODS (CONTRACT) ====================
    # Child class (Simulator/Binance) WAJIB mengimplementasikan ini.
    
    @abstractmethod
    async def submit_order(self, request: OrderRequest) -> OrderResult:
        """Kirim order ke venue"""
        pass
        
    @abstractmethod
    async def cancel_order(self, order_id: str) -> Result[bool, str]:
        """Cancel order"""
        pass
        
    @abstractmethod
    async def get_order(self, order_id: str) -> OrderResult:
        """Get single order"""
        pass
        
    @abstractmethod
    async def get_open_orders(self) -> Result[List[Order], str]:
        """Get all open orders"""
        pass
