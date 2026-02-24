"""
INVENTORY MANAGER (THE WAREHOUSE)
Location: core/execution/oms/components/inventory.py
Desc: Komponen yang mengelola 'Barang' (Posisi) dan 'Uang' (Cash).
      Menggunakan aritmatika Signed Quantity (+Long, -Short).
"""

from typing import Dict, List, Optional, Any
from datetime import datetime, timezone
from dataclasses import dataclass
import math

from core.shared.result import Result, Ok, Err
from core.shared.utils import get_logger
from core.execution.types import (
    Position, 
    TradeFill, 
    Symbol, 
    Currency, 
    OrderSide,
    PositionFactory
)

logger = get_logger("oms.inventory")

@dataclass
class PortfolioSnapshot:
    """Snapshot total portfolio pada satu titik waktu"""
    timestamp: float
    positions: List[Position]
    total_realized_pnl: float

class InventoryManager:
    """
    Manajer Gudang yang bertugas mencatat keluar masuk barang.
    Sifat: 
    - In-Memory (Cepat)
    - Thread-Safe (Via Immutable Position)
    - Strict (Tidak boleh ada posisi 'kira-kira')
    """
    
    def __init__(self, base_currency: Currency = "USDT"):
        self.base_currency = base_currency
        # Storage: { "BTC/USDT": Position(...) }
        self._positions: Dict[Symbol, Position] = {}
        # Cash: { "USDT": 10000.0, "BNB": 0.5 }
        self._cash_balances: Dict[Currency, float] = {base_currency: 0.0}
        self._lock: Any = None

    # ========== READ OPERATIONS ==========

    def get_position(self, symbol: str) -> Position:
        """Return posisi atau dummy kosong jika tidak ada"""
        return self._positions.get(symbol, Position(symbol=symbol, quantity=0.0, average_entry_price=0.0))

    def get_all_positions(self) -> List[Position]:
        return list(self._positions.values())

    def get_cash_balance(self, currency: Optional[Currency] = None) -> float:
        """Ambil saldo cash"""
        tgt_curr = currency or self.base_currency
        return self._cash_balances.get(tgt_curr, 0.0)

    def get_stats(self) -> Dict[str, Any]:
        return {
            "count": len(self._positions),
            "base_currency": self.base_currency,
            "exposure_symbols": list(self._positions.keys())
        }

    # ========== WRITE OPERATIONS (ATOMIC) ==========

    def on_fill(self, fill: TradeFill) -> Result[Position, str]:
        """
        [CORE LOGIC] Menangani Trade Fill.
        Menghitung Average Entry Price baru dan Realized PnL.
        """
        try:
            current_pos = self.get_position(fill.symbol)
            
            # 1. Tentukan Signed Quantity (+Buy, -Sell)
            fill_qty_signed = fill.quantity if fill.side == OrderSide.BUY else -fill.quantity
            
            # Variabel untuk posisi baru
            new_qty = current_pos.quantity
            new_avg_price = current_pos.average_entry_price
            realized_pnl = current_pos.realized_pnl # Akumulasi PnL sebelumnya
            
            # --- LOGIKA MATEMATIKA POSISI ---
            
            # KASUS A: Membuka Posisi Baru (Dari 0)
            if not current_pos.is_open:
                new_qty = fill_qty_signed
                new_avg_price = fill.price
                
            # KASUS B: Menambah Posisi (Averaging / Pyramiding)
            # Syarat: Arah fill SAMA dengan arah posisi
            elif (current_pos.quantity > 0 and fill_qty_signed > 0) or \
                 (current_pos.quantity < 0 and fill_qty_signed < 0):
                
                # Weighted Average Price Formula
                total_cost_old = current_pos.quantity * current_pos.average_entry_price
                total_cost_new = fill_qty_signed * fill.price
                
                new_qty = current_pos.quantity + fill_qty_signed
                new_avg_price = abs((total_cost_old + total_cost_new) / new_qty)
                
            # KASUS C: Mengurangi / Menutup Posisi (Realizing PnL)
            # Syarat: Arah fill BERLAWANAN dengan arah posisi
            else:
                # Logika: FIFO (First In First Out) secara implisit
                # PnL hanya terjadi pada jumlah yang 'bertabrakan'
                
                # 1. Hitung PnL Trade ini
                # Rumus: (Harga Jual - Harga Beli) * Jumlah
                # Long Close: (FillPrice - Entry) * QtyClose
                # Short Close: (Entry - FillPrice) * QtyClose
                # Kita sederhanakan dengan logika Signed Qty:
                # PnL = -1 * QtyFillSigned * (FillPrice - AvgEntry)
                
                # Berapa banyak yang di-close? (Min absolut dari posisi vs fill)
                qty_closing = min(abs(current_pos.quantity), abs(fill_qty_signed))
                qty_closing_signed = qty_closing if fill_qty_signed > 0 else -qty_closing
                
                trade_pnl = -1 * qty_closing_signed * (fill.price - current_pos.average_entry_price)
                realized_pnl += trade_pnl
                
                # 2. Update Quantity
                new_qty = current_pos.quantity + fill_qty_signed
                
                # 3. Cek Flip (Posisi berbalik arah)
                # Misal: Long 10, Sell 15 -> Short 5
                if (current_pos.quantity > 0 and new_qty < 0) or \
                   (current_pos.quantity < 0 and new_qty > 0):
                    # Jika flip, harga rata-rata sisa posisi mengikuti harga fill terakhir
                    new_avg_price = fill.price
                    
            # Update Immutable Object
            updated_pos = current_pos.copy(
                quantity=new_qty,
                average_entry_price=new_avg_price,
                realized_pnl=realized_pnl,
                last_update_at=datetime.now(timezone.utc).timestamp()
            )
            
            self._positions[fill.symbol] = updated_pos
            
            # Update Cash (Simple: Fee deduction)
            # Logic detail cash flow (margin) ada di Accountant/Broker, 
            # Inventory cuma catat fee expense di sini
            if fill.fee > 0:
                self.adjust_cash(-fill.fee, fill.fee_currency)
                
            return Ok(updated_pos)
            
        except Exception as e:
            logger.error(f"Inventory Calculation Error: {e}")
            return Err(f"Inventory Error: {str(e)}")

    def sync_positions(self, broker_positions: List[Position]):
        """
        [RECONCILIATION] Paksa inventory lokal mengikuti data broker.
        """
        # 1. Update yang ada / baru
        seen_symbols = set()
        for b_pos in broker_positions:
            self._positions[b_pos.symbol] = b_pos
            seen_symbols.add(b_pos.symbol)
            
        # 2. Hapus yang tidak ada di broker (Zombie positions)
        local_symbols = list(self._positions.keys())
        for sym in local_symbols:
            if sym not in seen_symbols and self._positions[sym].is_open:
                # Force close/zero di lokal
                self._positions[sym] = PositionFactory.create_empty(sym)
                logger.warning(f"🧟 Zombie position {sym} removed during sync")

    def adjust_cash(self, amount: float, currency: str = "USDT"):
        """Debit/Credit cash balance"""
        current = self._cash_balances.get(currency, 0.0)
        self._cash_balances[currency] = current + amount

# ========== 🔥 NEW METHODS – EQUITY & STATE VALIDATION ==========

    def get_equity(self, market_prices: Dict[Symbol, float]) -> float:
        """
        Hitung total ekuiti = cash + nilai pasar seluruh posisi.
        
        Args:
            market_prices: mapping symbol -> harga terkini (float).
                          Harga None/NaN akan di-skip, posisi dianggap 0.
        Returns:
            Total ekuiti dalam base currency.
        """
        # 1. Cash dalam base currency
        total = self.get_cash_balance(self.base_currency)
        
        # 2. Tambahkan nilai pasar tiap posisi
        for sym, pos in self._positions.items():
            if not pos.is_open or pos.quantity == 0.0:
                continue
            
            price = market_prices.get(sym)
            if price is None:
                # [SURGERY] Ubah warning menjadi debug agar terminal rapi
                logger.debug(f"⚠️ Missing market price for {sym}, skipping position valuation")
                continue
            if not isinstance(price, (int, float)) or math.isnan(price) or math.isinf(price):
                # [SURGERY] Ubah warning menjadi debug agar terminal rapi
                logger.debug(f"⚠️ Invalid market price {price} for {sym}, skipping")
                continue
                
            total += pos.quantity * price
        
        return total

    def validate_state(self) -> Result[bool, str]:
        """
        Sanity check internal state.
        Returns:
            Ok(True) jika semua komponen konsisten.
            Err(deskripsi) jika ditemukan anomali.
        """
        # 1. Cek cash balances tidak negatif
        for curr, bal in self._cash_balances.items():
            if not isinstance(bal, (int, float)) or math.isnan(bal) or math.isinf(bal):
                return Err(f"Cash balance {curr} is invalid: {bal}")
            if bal < 0:
                return Err(f"Cash balance {curr} is negative: {bal}")
        
        # 2. Cek setiap posisi
        for sym, pos in self._positions.items():
            # quantity harus numerik valid
            if not isinstance(pos.quantity, (int, float)) or math.isnan(pos.quantity) or math.isinf(pos.quantity):
                return Err(f"Position {sym} quantity is invalid: {pos.quantity}")
            
            # average_entry_price tidak negatif dan numerik
            if not isinstance(pos.average_entry_price, (int, float)) or math.isnan(pos.average_entry_price) or math.isinf(pos.average_entry_price):
                return Err(f"Position {sym} avg price is invalid: {pos.average_entry_price}")
            if pos.average_entry_price < 0:
                return Err(f"Position {sym} avg price is negative: {pos.average_entry_price}")
            
            # realized_pnl tidak NaN / Inf
            if not isinstance(pos.realized_pnl, (int, float)) or math.isnan(pos.realized_pnl) or math.isinf(pos.realized_pnl):
                return Err(f"Position {sym} realized_pnl is invalid: {pos.realized_pnl}")
        
        return Ok(True)

    def apply_funding_fee(self, fee: float, currency: str = "USDT") -> None:
        """Mengurangi/menambah cash balance karena funding fee."""
        self._cash_balances[currency] = self._cash_balances.get(currency, 0.0) - fee

        
