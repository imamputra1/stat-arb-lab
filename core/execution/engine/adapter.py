"""
THE BRIDGE: EMULATOR ADAPTER
Location: core/execution/engine/adapter.py
Desc: Penerjemah universal. Mengubah protokol OMS menjadi instruksi Emulator,
      dan menerjemahkan hasil eksekusi (Trade/Rejection) menjadi Laporan Resmi (ExecutionReport).
"""

import time
from typing import List, Dict, Optional

# Protocol & Types (Bahasa OMS - BrokerProtocol)
from core.execution.protocols import BrokerProtocol
from core.execution.types import (
    OrderRequest, 
    ExecutionReport, 
    Order as OMSOrder, 
    TradeFill, 
    Position, 
    OrderSide
)
from core.shared.result import Result, Ok, Err

# Emulator Types (Bahasa Engine - The Wild West)
from core.execution.engine.emulator import ExchangeEmulator, MarketContext
from core.execution.engine.types import Trade, Rejection

# ---------------------------------------------------------
    # 0. CONFIGURATION
    # ---------------------------------------------------------
    # Arahkan ke file spesifik simbol (Brown Data)
PARQUET_PATH = "data/brown/symbol=BTC-USDT/interval=1m/year=2024/month=01/data.parquet"
SYMBOL = "BTC/USDT"

class EmulatorAdapter(BrokerProtocol):
    """
    [ADAPTER PATTERN]
    Menyamar sebagai Broker agar OMS tidak curiga bahwa dia sedang
    berbicara dengan simulasi.
    """
    
    def __init__(self, emulator: ExchangeEmulator, initial_cash: float = 100_000.0):
        self.emulator = emulator
        
        # State Internal (Simulasi Akun Broker)
        self._current_context: Optional[MarketContext] = None
        self._positions: Dict[str, float] = {} 
        self._cash = initial_cash
        self._order_store: Dict[str, ExecutionReport] = {} # Simpan status terakhir order

    def update_market_context(self, context: MarketContext):
        """
        [LIVE FEED] Disuntikkan oleh Main Loop (Engine) setiap tick.
        Agar saat OMS kirim order, Emulator tahu harga & volume detik ini.
        """
        self._current_context = context

    # =========================================================================
    # IMPLEMENTASI PROTOKOL BROKER (Wajib Ada)
    # =========================================================================

    async def submit_order(self, request: OrderRequest) -> Result[ExecutionReport, str]:
        """
        Menerima OrderRequest -> Konversi ke Order -> Lempar ke Emulator.
        """
        # 1. Cek Ketersediaan Pasar
        if not self._current_context:
            return Err("Emulator offline: No Market Data Context")

        # 2. Konversi Bahasa (Request -> OMSOrder)
        # Emulator kita menggunakan definisi Order yang sama dengan OMS (Shared Types)
        # Jadi kita cukup instantiate Order dari request.
        order = OMSOrder.from_request(request, exchange_order_id=f"EMU-{int(time.time()*1000000)}")
        
        # 3. EKSEKUSI (Masuk ke The Kill Zone)
        # Result di sini bisa berupa Trade (Sukses) atau Rejection (Gagal)
        result = self.emulator.process_order(order, self._current_context)
        
        # 4. Terjemahkan Hasil Balik
        if result.is_ok():
            trade: Trade = result.unwrap()
            return self._handle_success(order, trade)
        else:
            rejection: Rejection = result.unwrap_err()
            return self._handle_rejection(order, rejection)

    async def cancel_order(self, order_id: str) -> Result[ExecutionReport, str]:
        """
        Di Emulator 'Immediate Fill', order hampir tidak pernah pending.
        Tapi untuk kelengkapan protokol, kita return sukses jika order sudah selesai.
        """
        if order_id in self._order_store:
            # Ambil report terakhir
            return Ok(self._order_store[order_id])
        return Err(f"Order {order_id} not found in Emulator Adapter")

    async def get_order(self, order_id: str) -> Result[ExecutionReport, str]:
        """Polling status order"""
        if order_id in self._order_store:
            return Ok(self._order_store[order_id])
        return Err(f"Order {order_id} not found")

    async def get_all_positions(self) -> Result[List[Position], str]:
        """Mengembalikan snapshot posisi inventory"""
        pos_list = []
        for sym, qty in self._positions.items():
            if abs(qty) > 1e-9: # Filter dust
                # Di simulator harga entry kita rata-rata atau pakai harga market terakhir (simplified)
                current_price = self._current_context.price if self._current_context else 0.0
                pos_list.append(Position(sym, qty, current_price)) 
        return Ok(pos_list)

    async def get_balance(self) -> Result[Dict[str, float], str]:
        """Mengembalikan saldo cash"""
        return Ok({"USDT": self._cash})

    # =========================================================================
    # INTERNAL TRANSLATION LOGIC
    # =========================================================================

    def _handle_success(self, order: OMSOrder, trade: Trade) -> Result[ExecutionReport, str]:
        """Menerjemahkan Trade (Engine) -> ExecutionReport (OMS)"""
        
        # 1. Buat Fill Object (Bahasa OMS)
        fill_res = TradeFill.create(
            order_id=trade.order_id,
            symbol=trade.symbol,
            side=trade.side,
            quantity=trade.quantity,
            price=trade.price,
            fee=trade.fee,
            fee_currency=trade.fee_currency,
            latency_ms=trade.latency_ms,
            slippage_bps=trade.slippage_bps
        )
        
        if fill_res.is_err():
            return Err(f"Adapter Conversion Error: {fill_res.unwrap_err()}")
        
        fill = fill_res.unwrap()

        # 2. Update 'Dompet' Adapter (Simulasi Saldo Broker)
        self._update_internal_balance(trade)

        # 3. Update Status Order menjadi FILLED
        # Kita gunakan method add_fill dari Order untuk kalkulasi avg_price otomatis
        order_update_res = order.add_fill(trade.quantity, trade.price)
        if order_update_res.is_err():
             return Err(f"Order Update Failed: {order_update_res.unwrap_err()}")
        
        updated_order = order_update_res.unwrap()

        # 4. Buat Report Akhir
        report = ExecutionReport.from_order_and_fills(updated_order, [fill])
        
        # Simpan ke memori adapter untuk polling nanti
        self._order_store[order.order_id] = report
        
        return Ok(report)

    def _handle_rejection(self, order: OMSOrder, rejection: Rejection) -> Result[ExecutionReport, str]:
        """Menerjemahkan Rejection (Engine) -> ExecutionReport (OMS)"""
        
        # Update status order menjadi REJECTED
        rej_res = order.mark_rejected(rejection.code.value, rejection.reason)
        if rej_res.is_err():
            return Err(f"Order Reject Update Failed: {rej_res.unwrap_err()}")
            
        rejected_order = rej_res.unwrap()
        
        report = ExecutionReport(
            order=rejected_order,
            fills=(),
            is_complete=True,
            completion_reason=rejection.reason
        )
        
        self._order_store[order.order_id] = report
        return Ok(report)

    def _update_internal_balance(self, trade: Trade):
        """Logic pembukuan sederhana untuk adapter"""
        cost = trade.quantity * trade.price
        
        # Update Cash
        if trade.side == OrderSide.BUY:
            self._cash -= (cost + trade.fee)
            self._positions[trade.symbol] = self._positions.get(trade.symbol, 0.0) + trade.quantity
        else:
            self._cash += (cost - trade.fee)
            self._positions[trade.symbol] = self._positions.get(trade.symbol, 0.0) - trade.quantity


__all__ = ['EmulatorAdapter']
