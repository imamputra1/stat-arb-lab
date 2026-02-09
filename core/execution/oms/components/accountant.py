"""
THE ACCOUNTANT (THE SCORER)
Location: core/execution/oms/components/accountant.py
Desc: Mencatat Ledger, menghitung Realized PnL, dan Audit Trail.
      Menerapkan Double-Entry Check terhadap Inventory.
"""

from typing import Dict, List, Any
from dataclasses import dataclass

from core.shared.utils import get_logger
from core.execution.types import (
    TradeFill, 
    ExecutionReport, 
    Symbol, 
    Currency, 
    OrderSide,
    Position
)

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

    # ================== WRITE OPERATIONS ==================

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

    # ================== READ OPERATIONS ==================

    def get_total_realized_pnl(self) -> float:
        """Total PnL yang sudah dikunci (Closed Positions)"""
        return self._total_realized_pnl

    def get_unrealized_pnl(self, current_positions: List[Position]) -> float:
        """
        Menghitung Unrealized PnL dari daftar posisi Inventory.
        (Mark-to-Market PnL)
        """
        return sum(pos.unrealized_pnl for pos in current_positions)

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
