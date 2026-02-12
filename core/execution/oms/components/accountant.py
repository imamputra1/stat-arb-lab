"""
THE ACCOUNTANT (THE SCORER)
Location: core/execution/oms/components/accountant.py
Desc: Mencatat Ledger, menghitung Realized PnL, dan Audit Trail.
      Menerapkan Double-Entry Check terhadap Inventory.
"""

from typing import Dict, List, Any
from dataclasses import dataclass
import math
import warnings

from core.shared.utils import get_logger
from core.shared.result import Result, Ok, Err
from core.execution.types import (
    TradeFill, 
    ExecutionReport, 
    Symbol, 
    Currency, 
    OrderSide,
    Position
)
from .inventory import InventoryManager

logger = get_logger("oms.accountant")

@dataclass(frozen=True)
class TradeRecord:
    """
    Catatan abadi satu transaksi (Ledger Entry).
    Berisi data eksekusi PLUS data akuntansi (PnL).
    """
    fill_id: str
    timestamp: float
    symbol: Symbol
    side: OrderSide
    quantity: float
    price: float
    
    # Accounting Metrics
    fee: float
    fee_currency: Currency
    realized_pnl: float    # PnL yang terealisasi dari trade ini (0 jika opening)
    cost_basis: float      # Harga rata-rata entry saat trade ini terjadi

class Accountant:
    """
    Juru Tulis yang mencatat setiap sen uang yang bergerak.
    Menyediakan 'Source of Truth' untuk performa strategi.
    """
    
    def __init__(self, base_currency: Currency = "USDT"):
        self.base_currency = base_currency
        
        # 1. The Ledger (Buku Besar)
        self._trade_history: List[TradeRecord] = []
        self._execution_reports: List[ExecutionReport] = []
        
        # 2. Cumulative Metrics (Global)
        self._total_realized_pnl: float = 0.0
        self._total_fees: Dict[Currency, float] = {}
        
        # 3. Internal State untuk PnL Calculation (Shadow Inventory)
        # Kita track avg_price sendiri untuk memverifikasi InventoryManager
        self._shadow_positions: Dict[str, Dict[str, float]] = {} 

    # ================== EXISTING METHODS (MODIFIED SAFELY) ==================

    def get_unrealized_pnl(self, current_positions: List[Position]) -> float:
        """
        ⚠️ DEPRECATED – Tidak bisa dipakai karena butuh harga pasar.
        Gunakan `compute_unrealized_pnl()` sebagai gantinya.
        Method ini akan selalu mengembalikan 0.0 dan log warning.
        """
        warnings.warn(
            "Accountant.get_unrealized_pnl() is deprecated. Use compute_unrealized_pnl() with market_prices.",
            DeprecationWarning,
            stacklevel=2
        )
        logger.warning("⚠️ Called deprecated get_unrealized_pnl() – returning 0.0")
        return 0.0

    # ================== EXISTING METHODS (UNCHANGED) ==================

    def on_fill(self, fill: TradeFill) -> None:
        """
        Mencatat Fill dan menghitung PnL-nya.
        Dipanggil oleh OMS setiap kali ada fill baru.
        """
        # 1. Hitung PnL Transaksi ini (Independent Calculation)
        pnl_data = self._calculate_trade_pnl(fill)
        realized_pnl = pnl_data['realized_pnl']
        cost_basis = pnl_data['cost_basis']
        
        # 2. Buat Record
        record = TradeRecord(
            fill_id=fill.fill_id,
            timestamp=fill.timestamp.timestamp(),
            symbol=fill.symbol,
            side=fill.side,
            quantity=fill.quantity,
            price=fill.price,
            fee=fill.fee,
            fee_currency=fill.fee_currency,
            realized_pnl=realized_pnl,
            cost_basis=cost_basis
        )
        
        # 3. Update Ledger & Globals
        self._trade_history.append(record)
        self._total_realized_pnl += realized_pnl
        
        # Update Fees
        current_fee = self._total_fees.get(fill.fee_currency, 0.0)
        self._total_fees[fill.fee_currency] = current_fee + fill.fee
        
        # Log jika ada profit/loss signifikan
        if abs(realized_pnl) > 0:
            logger.info(f"💰 PnL Realized: {fill.symbol} = {realized_pnl:.2f} {self.base_currency}")

    def record_trade(self, report: ExecutionReport) -> None:
        """Arsip ExecutionReport lengkap (untuk audit)"""
        self._execution_reports.append(report)

    def get_total_realized_pnl(self) -> float:
        """Total PnL yang sudah dikunci (Closed Positions)"""
        return self._total_realized_pnl

    def get_total_fees(self) -> Dict[Currency, float]:
        """Total biaya trading"""
        return self._total_fees.copy()

    def get_trade_history(self) -> List[TradeRecord]:
        """Export ledger"""
        return list(self._trade_history)

    def get_stats(self) -> Dict[str, Any]:
        """Statistik Akuntansi"""
        return {
            "realized_pnl": self._total_realized_pnl,
            "trades_count": len(self._trade_history),
            "reports_count": len(self._execution_reports),
            "fees_paid": self._total_fees
        }

    # ================== 🔥 NEW METHODS ==================

    def compute_unrealized_pnl(self, 
                              positions: List[Position], 
                              market_prices: Dict[Symbol, float]) -> float:
        """
        Hitung unrealized PnL secara mark-to-market.
        
        Args:
            positions: Daftar posisi dari InventoryManager.
            market_prices: Mapping symbol -> harga terkini.
        
        Returns:
            Total unrealized PnL dalam base currency.
        """
        total = 0.0
        for pos in positions:
            if not pos.is_open or pos.quantity == 0.0:
                continue
            price = market_prices.get(pos.symbol)
            if price is None:
                logger.warning(f"⚠️ Missing market price for {pos.symbol}, skipping unrealized PnL")
                continue
            if not isinstance(price, (int, float)) or math.isnan(price) or math.isinf(price):
                logger.warning(f"⚠️ Invalid market price {price} for {pos.symbol}, skipping")
                continue
            # Long: (price - avg) * qty; Short: (avg - price) * abs(qty)
            # Karena quantity bisa negatif, rumus tetap sama: quantity * (price - avg_entry)
            total += pos.quantity * (price - pos.average_entry_price)
        return total

    def validate_state(self) -> Result[bool, str]:
        """
        Sanity check internal state Accountant.
        
        Returns:
            Ok(True) jika ledger konsisten dengan accumulator.
            Err(deskripsi) jika ditemukan mismatch atau nilai invalid.
        """
        # 1. Total realized PnL harus sama dengan sum trade history
        sum_pnl = sum(t.realized_pnl for t in self._trade_history)
        if abs(sum_pnl - self._total_realized_pnl) > 1e-8:
            return Err(f"Realized PnL mismatch: ledger {sum_pnl:.2f} vs accumulator {self._total_realized_pnl:.2f}")

        # 2. Fee accumulator harus match dengan trade history
        fee_check: Dict[Currency, float] = {}
        for t in self._trade_history:
            fee_check[t.fee_currency] = fee_check.get(t.fee_currency, 0.0) + t.fee
        if fee_check != self._total_fees:
            return Err(f"Fee mismatch: ledger {fee_check} vs accumulator {self._total_fees}")

        # 3. Shadow positions tidak boleh mengandung NaN/inf
        for sym, pos in self._shadow_positions.items():
            qty = pos['qty']
            avg = pos['avg_price']
            if not isinstance(qty, (int, float)) or math.isnan(qty) or math.isinf(qty):
                return Err(f"Shadow position {sym} quantity invalid: {qty}")
            if not isinstance(avg, (int, float)) or math.isnan(avg) or math.isinf(avg):
                return Err(f"Shadow position {sym} avg price invalid: {avg}")
            if avg < 0:
                return Err(f"Shadow position {sym} avg price negative: {avg}")

        return Ok(True)

    def reset(self) -> None:
        """
        Reset seluruh state akuntansi.
        Berguna untuk RESEARCH/PAPER mode atau backtest sesi baru.
        """
        self._trade_history.clear()
        self._execution_reports.clear()
        self._total_realized_pnl = 0.0
        self._total_fees.clear()
        self._shadow_positions.clear()
        logger.info("🧹 Accountant state reset")

    def reconcile_with_inventory(self, inventory: 'InventoryManager') -> Result[bool, str]:
        """
        Bandingkan total realized PnL dengan akumulasi dari Inventory.
        Juga verifikasi shadow positions vs real positions (warning saja).
        
        Args:
            inventory: Instance InventoryManager yang aktif.
        
        Returns:
            Ok(True) jika sinkron, Err jika ada perbedaan signifikan.
        """
        # 1. Hitung total realized PnL dari semua posisi di inventory
        inventory_pnl = sum(pos.realized_pnl for pos in inventory.get_all_positions())
        if abs(inventory_pnl - self._total_realized_pnl) > 1e-8:
            return Err(f"PnL mismatch: Inventory {inventory_pnl:.2f} vs Accountant {self._total_realized_pnl:.2f}")

        # 2. Opsional: cocokkan shadow qty dengan posisi terbuka (warning only)
        for sym, shadow in self._shadow_positions.items():
            inv_pos = inventory.get_position(sym)
            shadow_qty = shadow['qty']
            inv_qty = inv_pos.quantity
            if abs(shadow_qty - inv_qty) > 1e-8:
                logger.warning(f"⚠️ Shadow qty mismatch for {sym}: {shadow_qty} vs inventory {inv_qty}")

        return Ok(True)

    def get_performance_summary(self) -> Dict[str, Any]:
        """
        Ringkasan performa trading:
        - total_trades = semua trade (termasuk opening)
        - winning_trades = trade dengan realized_pnl > 0
        - losing_trades  = trade dengan realized_pnl < 0
        - win_rate = winning / (winning + losing) [hanya closed trades]
        - avg_win, avg_loss, profit_factor, dll.
        """
        trades = self._trade_history
        if not trades:
            return {
                "total_trades": 0,
                "winning_trades": 0,
                "losing_trades": 0,
                "win_rate": 0.0,
                "avg_win": 0.0,
                "avg_loss": 0.0,
                "profit_factor": 0.0,
                "total_realized_pnl": 0.0,
                "total_fees": {}
            }

        winning = [t.realized_pnl for t in trades if t.realized_pnl > 0]
        losing  = [t.realized_pnl for t in trades if t.realized_pnl < 0]
    
        total_win = sum(winning) if winning else 0.0
        total_loss = abs(sum(losing)) if losing else 0.0
        closed_count = len(winning) + len(losing)

        return {
            "total_trades": len(trades),
            "winning_trades": len(winning),
            "losing_trades": len(losing),
            "win_rate": len(winning) / closed_count if closed_count > 0 else 0.0,
            "avg_win": total_win / len(winning) if winning else 0.0,
            "avg_loss": total_loss / len(losing) if losing else 0.0,
            "profit_factor": total_win / total_loss if total_loss > 0 else float('inf'),
            "total_realized_pnl": self._total_realized_pnl,
            "total_fees": self._total_fees.copy()
        }

    # ================== INTERNAL LOGIC (SHADOW CALC) ==================

    def _calculate_trade_pnl(self, fill: TradeFill) -> Dict[str, float]:
        """
        Menghitung PnL trade secara independen dari InventoryManager.
        Menggunakan logika Weighted Average Price.
        """
        symbol = fill.symbol
        
        # Init state jika belum ada
        if symbol not in self._shadow_positions:
            self._shadow_positions[symbol] = {'qty': 0.0, 'avg_price': 0.0}
            
        pos = self._shadow_positions[symbol]
        current_qty = pos['qty']
        avg_price = pos['avg_price']
        
        # Signed Qty (+Buy, -Sell)
        fill_qty_signed = fill.quantity if fill.side == OrderSide.BUY else -fill.quantity
        
        realized_pnl = 0.0
        
        # Logic 1: Opening / Averaging (Menambah Posisi)
        # Jika qty 0, atau arah sama (Long tambah Long, Short tambah Short)
        is_opening = (current_qty == 0) or \
                     (current_qty > 0 and fill_qty_signed > 0) or \
                     (current_qty < 0 and fill_qty_signed < 0)
                     
        if is_opening:
            # Hitung Average Price Baru
            total_cost = (current_qty * avg_price) + (fill_qty_signed * fill.price)
            new_qty = current_qty + fill_qty_signed
            # Avoid division by zero
            new_avg_price = abs(total_cost / new_qty) if new_qty != 0 else 0.0
            
            # Update Shadow State
            pos['qty'] = new_qty
            pos['avg_price'] = new_avg_price
            
        # Logic 2: Closing / Reducing (Realisasi PnL)
        else:
            # Hitung PnL: -(Qty Trade) * (Harga Trade - Harga Rata2)
            # Minus karena Qty Trade berlawanan dengan posisi
            # Contoh Long Close (Sell): -(-10) * (110 - 100) = +100 Profit
            realized_pnl = -1 * fill_qty_signed * (fill.price - avg_price)
            
            new_qty = current_qty + fill_qty_signed
            
            # Check Flip (Berbalik Arah)
            if (current_qty > 0 and new_qty < 0) or (current_qty < 0 and new_qty > 0):
                # Reset avg price ke harga trade saat ini untuk sisa flip
                pos['avg_price'] = fill.price
            elif new_qty == 0:
                pos['avg_price'] = 0.0
                
            pos['qty'] = new_qty
            
        return {
            'realized_pnl': realized_pnl,
            'cost_basis': avg_price # Kembalikan cost basis SEBELUM trade ini berubah (atau sesudah? Biasanya snapshot saat trade)
        }
