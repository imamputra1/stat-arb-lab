"""
LIVE/PAPER TRADING METRICS REPORTER
Location: live/reporter.py
Desc: Menghasilkan laporan akhir performa trading berdasarkan data dari OMS.
      Menggunakan data dari Accountant dan Inventory tanpa mengubah komponen inti.
"""

from typing import Dict, Any
from core.execution.oms.facade import OMSFacade


class MetricsReporter:
    """
    Menghasilkan laporan metrik trading setelah sesi berakhir.
    Memanfaatkan data yang sudah dikumpulkan oleh Accountant dan Inventory.
    """

    def __init__(self, oms: OMSFacade, initial_cash: float):
        """
        Args:
            oms: Instance OMSFacade yang sudah berjalan.
            initial_cash: Modal awal dalam base currency.
        """
        self.oms = oms
        self.initial_cash = initial_cash

    def generate_pnl_report(self) -> Dict[str, Any]:
        """
        Menghitung metrik PnL dan equity:
        - initial_cash: dari parameter
        - final_equity: nilai portofolio terkini (cash + nilai posisi)
        - realized_pnl: total PnL dari posisi yang sudah ditutup
        - floating_pnl: PnL dari posisi terbuka (unrealized)
        - roi_pct: persentase return terhadap modal awal
        - win_rate: persentase trade yang profit (dari accountant)
        - total_trades: jumlah semua trade (termasuk opening)
        """
        accountant = self.oms._oms.accountant
        perf_summary = accountant.get_performance_summary()

        realized = perf_summary["total_realized_pnl"]
        win_rate = perf_summary["win_rate"] * 100.0
        total_trades = perf_summary["total_trades"]

        final_equity = self.oms.get_equity()
        floating_pnl = final_equity - self.initial_cash - realized
        roi_pct = ((final_equity - self.initial_cash) / self.initial_cash) * 100.0

        return {
            "Initial Cash": self.initial_cash,
            "Final Equity": final_equity,
            "Realized PnL": realized,
            "Floating PnL": floating_pnl,
            "ROI (%)": roi_pct,
            "Win Rate (%)": win_rate,
            "Total Trades": total_trades
        }

    def generate_risk_report(self) -> Dict[str, Any]:
        """
        Menghitung metrik risiko:
        - Peak Equity: nilai equity tertinggi yang pernah dicapai
        - Current Drawdown: penurunan dari peak saat ini
        - Max Drawdown Observed: penurunan terdalam selama sesi (diambil dari Sentry)
        - Max Drawdown Limit: batas drawdown yang dikonfigurasi
        - Kill Switch status dan alasan
        """
        sentry_stats = self.oms._oms.sentry.get_stats()
        peak = sentry_stats["peak_equity"]
        current = self.oms.get_equity()

        if peak > 0:
            current_drawdown_pct = ((peak - current) / peak) * 100.0
        else:
            current_drawdown_pct = 0.0

        # Asumsi Sentry menyimpan max drawdown observed dalam bentuk desimal (misal 0.1 = 10%)
        observed_max_drawdown = abs(sentry_stats.get("max_drawdown_observed", 0.0) *100)

        return {
            "Peak Equity": peak,
            "Current Drawdown (%)": current_drawdown_pct,
            "Max Drawdown Observed (%)": observed_max_drawdown,
            "Max Drawdown Limit (%)": sentry_stats.get("max_drawdown_pct", 0.0) * 100,
            "Kill Switch Engaged": sentry_stats["kill_switch"],
            "Kill Reason": sentry_stats.get("kill_reason", "")
        }

    def generate_system_health(self) -> Dict[str, Any]:
        """
        Menghitung metrik kesehatan sistem:
        - Rata-rata latensi eksekusi (dari fills)
        - Jumlah order yang diblokir Sentry
        - Jumlah fill sukses
        - Rasio penolakan (rejections / total order)
        """
        accountant = self.oms._oms.accountant
        sentry_stats = self.oms._oms.sentry.get_stats()

        total_latency = 0.0
        fill_count = 0
        for report in accountant._execution_reports:
            for fill in report.fills:
                total_latency += fill.latency_ms if fill.latency_ms is not None else 0.0
                fill_count += 1

        avg_latency = (total_latency / fill_count) if fill_count > 0 else 0.0

        rejections = sentry_stats.get("violations_count", 0)
        total_orders = fill_count + rejections
        rejection_ratio = (rejections / total_orders * 100.0) if total_orders > 0 else 0.0

        return {
            "Avg Execution Latency (ms)": avg_latency,
            "Total Sentry Blocks": rejections,
            "Successful Fills": fill_count,
            "Rejection Ratio (%)": rejection_ratio
        }

    def print_report(self):
        """Cetak laporan lengkap dalam format rapi ke terminal."""
        pnl_report = self.generate_pnl_report()
        risk_report = self.generate_risk_report()
        health_report = self.generate_system_health()

        print("\n" + "=" * 60)
        print("📊 FINAL PERFORMANCE REPORT".center(60))
        print("=" * 60)

        print("\n💰 PNL & EQUITY:")
        print(f"  Initial Cash    : ${pnl_report['Initial Cash']:>12,.2f}")
        print(f"  Final Equity    : ${pnl_report['Final Equity']:>12,.2f}")
        print(f"  Peak Equity     : ${risk_report['Peak Equity']:>12,.2f}")
        print("-" * 60)
        print(f"  Realized PnL    : ${pnl_report['Realized PnL']:>12,.2f}")
        print(f"  Floating PnL    : ${pnl_report['Floating PnL']:>12,.2f}")
        print(f"  Total ROI       : {pnl_report['ROI (%)']:>13.2f}%")

        print("\n📈 TRADING STATS:")
        print(f"  Win Rate        : {pnl_report['Win Rate (%)']:>13.2f}%")
        print(f"  Total Trades    : {pnl_report['Total Trades']:>14}")
        print(f"  Current Drawdown: {risk_report['Current Drawdown (%)']:>13.2f}%")
        print(f"  Max Drawdown Obs: {risk_report['Max Drawdown Observed (%)']:>13.2f}%")

        print("\n⚙️ SYSTEM HEALTH:")
        print(f"  Avg Latency     : {health_report['Avg Execution Latency (ms)']:>10.2f} ms")
        print(f"  Sentry Blocks   : {health_report['Total Sentry Blocks']:>14}")
        print(f"  Rejection Ratio : {health_report['Rejection Ratio (%)']:>13.2f}%")
        print(f"  Kill Switch     : {'🚨 ENGAGED' if risk_report['Kill Switch Engaged'] else '✅ SAFE'}")
        if risk_report['Kill Switch Engaged']:
            print(f"  Reason          : {risk_report['Kill Reason']}")
        print("=" * 60 + "\n")
