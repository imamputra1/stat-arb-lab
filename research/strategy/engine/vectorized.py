import polars as pl
import numpy as np
from typing import Dict, Any
from dataclasses import dataclass
from core.shared import Result, Ok, Err
from core.signals.base_signal import BaseStrategy

@dataclass
class BacktestConfig:
    initial_capital: float
    transaction_cost_pct: float
    slippage_pct: float

class VectorizedBacktestEngine:
    def __init__(
        self, 
        initial_capital: float = 10_000.0, 
        transaction_cost_pct: float = 0.001,
        slippage_pct: float = 0.0005
    ):
        self.config = BacktestConfig(
            initial_capital=initial_capital,
            transaction_cost_pct=transaction_cost_pct,
            slippage_pct=slippage_pct
        )

    def run(self, data: pl.DataFrame, strategy: BaseStrategy) -> Result[Dict[str, Any], str]:
        try:
            sig_res = strategy.generate_signals(data)
            if sig_res.is_err(): return Err(f"Signal Gen Failed: {sig_res.error}")
            
            signals = sig_res.unwrap()
            
            # Smart Align: Join on index/timestamp is safer, but assuming aligned for speed if index matches
            # Efficient Projection: Only keep necessary columns
            df = data.select(["timestamp", "target_price"]).with_columns([
                signals["position"],
                signals["action"]
            ])

            # 1. Market Returns (Vectorized)
            df = df.with_columns([
                pl.col("target_price").pct_change().fill_null(0.0).alias("market_ret")
            ])

            # 2. Strategy Returns (Lagged Position * Market Return)
            # Position hari ini (T) hanya menikmati return besok (T+1), maka shift(1) posisi.
            df = df.with_columns([
                pl.col("position").shift(1).fill_null(0).alias("prev_pos")
            ])
            
            df = df.with_columns([
                (pl.col("prev_pos") * pl.col("market_ret")).alias("gross_ret")
            ])

            # 3. Cost Model (Transaction + Slippage)
            # Cost dikenakan saat posisi berubah (Delta Position != 0)
            total_cost_factor = self.config.transaction_cost_pct + self.config.slippage_pct
            
            df = df.with_columns([
                (pl.col("position") - pl.col("prev_pos")).abs().alias("pos_delta")
            ])
            
            df = df.with_columns([
                (pl.col("pos_delta") * total_cost_factor).alias("total_cost")
            ])

            # 4. Net Returns & Equity Curve
            df = df.with_columns([
                (pl.col("gross_ret") - pl.col("total_cost")).alias("net_ret")
            ])

            df = df.with_columns([
                (1 + pl.col("net_ret")).cum_prod().alias("cum_ret")
            ])
            
            df = df.with_columns([
                (pl.col("cum_ret") * self.config.initial_capital).alias("equity")
            ])

            # 5. Advanced Metrics (Numpy Optimized)
            eq_curve = df["equity"].to_numpy()
            net_rets = df["net_ret"].to_numpy()
            
            final_cap = eq_curve[-1]
            total_ret_pct = ((final_cap - self.config.initial_capital) / self.config.initial_capital) * 100
            
            # Sharpe (Annualized assumption: Crypto 365d or Trading Days 252d? Defaulting 365 for 24/7 markets)
            # Menggunakan safe division untuk menghindari zero division error
            std_dev = np.std(net_rets)
            sharpe = (np.mean(net_rets) / std_dev * np.sqrt(365 * 24 * 60)) if std_dev > 1e-9 else 0.0 # Assumes minute data
            
            # Max Drawdown
            peak = np.maximum.accumulate(eq_curve)
            dd = (eq_curve - peak) / peak
            max_dd_pct = np.min(dd) * 100

            # Trade Statistics
            trade_mask = df["pos_delta"] > 0
            total_trades = df.filter(trade_mask).height
            
            metrics = {
                "initial_capital": self.config.initial_capital,
                "final_capital": final_cap,
                "total_return_pct": total_ret_pct,
                "sharpe_ratio": sharpe,
                "max_drawdown_pct": max_dd_pct,
                "total_trades": total_trades
            }
            
            return Ok({
                "metrics": metrics,
                "equity_curve": df,
                "trade_log": df.filter(trade_mask) # Return rows where trades happened
            })

        except Exception as e:
            return Err(f"Backtest Runtime Error: {str(e)}")

def create_vectorized_backtest_engine(
    initial_capital: float = 10_000.0,
    transaction_cost_pct: float = 0.001,
    slippage_pct: float = 0.0005
) -> VectorizedBacktestEngine:
    return VectorizedBacktestEngine(
        initial_capital=initial_capital,
        transaction_cost_pct=transaction_cost_pct,
        slippage_pct=slippage_pct
    )
