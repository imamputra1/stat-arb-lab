"""
RISK MANAGEMENT TYPES - FINAL
Location: core/risk/types.py
Role: Data structures for risk rules and trade approval logic.
Standardized: Dataclass + Protocol + Result Pattern.
"""

from dataclasses import dataclass, field, fields
from enum import Enum
from typing import Optional, Dict, Any, Protocol, runtime_checkable, Tuple
from datetime import datetime

import numpy as np
from core.shared.result import Result, Err, Ok

# ==================== PROTOCOLS (INTERFACES) ====================
@runtime_checkable
class RiskValidatable(Protocol):
    """Protocol for risk objects that can validate themselves"""
    def validate(self) -> Result[bool, str]: ...
    def to_dict(self) -> Dict[str, Any]: ...

@runtime_checkable
class SizeCalculator(Protocol):
    """Protocol for position sizing calculators"""
    def calculate(self, account_state: 'AccountState', risk_params: Dict[str, Any]) -> float: ...

# ==================== ENUMS ====================
class RiskLevel(str, Enum):
    NORMAL = "normal"           # Operasi normal
    CAUTION = "caution"         # Kurangi size (Warning)
    HALT = "halt"               # Stop trading total
    PANIC = "panic"             # Liquidation mode

class TradeAction(str, Enum):
    ENTER = "enter"
    EXIT = "exit"
    HEDGE = "hedge"
    SCALE_IN = "scale_in"
    SCALE_OUT = "scale_out"

class RejectionCode(str, Enum):
    DAILY_DRAWDOWN_LIMIT = "daily_drawdown_limit"
    ACCOUNT_RISK_LIMIT = "account_risk_limit"
    LEVERAGE_LIMIT = "leverage_limit"
    OPEN_TRADES_LIMIT = "open_trades_limit"
    ASSET_EXPOSURE_LIMIT = "asset_exposure_limit"
    CIRCUIT_BREAKER = "circuit_breaker"
    INSUFFICIENT_LIQUIDITY = "insufficient_liquidity"
    VOLATILITY_LIMIT = "volatility_limit"
    CORRELATION_LIMIT = "correlation_limit"
    MARKET_CLOSED = "market_closed"

# ==================== DATA CLASSES ====================

@dataclass(frozen=True)
class RiskConfig:
    """
    Aturan Main Risk Manager (Single Source of Truth).
    """
    # --- 1. RISK PER TRADE ---
    max_account_risk_per_trade: float = 0.01   # 1% equity risk
    max_leverage_per_trade: float = 3.0
    
    # --- 2. GLOBAL LIMITS ---
    max_daily_drawdown: float = 0.03           # 3% daily limit
    max_weekly_drawdown: float = 0.10
    max_open_trades: int = 5
    max_exposure_per_asset: float = 0.20
    
    # --- 3. CIRCUIT BREAKER ---
    circuit_breaker_cooldown_min: int = 60
    # [FIX] Field ini tadi hilang, kita kembalikan
    circuit_breaker_threshold: float = 0.05    # 5% intraday drop triggers halt
    
    # --- 4. EXTRAS ---
    enable_portfolio_heat_check: bool = True
    
    def validate(self) -> Result[bool, str]:
        """Self-validation logic."""
        if not (0.001 <= self.max_account_risk_per_trade <= 0.05):
            return Err(f"Risk per trade {self.max_account_risk_per_trade:.1%} unsafe (0.1% - 5%)")
            
        if self.max_leverage_per_trade > 20.0:
             return Err(f"Leverage {self.max_leverage_per_trade}x is too high")

        if self.max_daily_drawdown > 0.10:
            return Err("Max daily drawdown > 10% is dangerous")
            
        return Ok(True)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> Result['RiskConfig', str]:
        """Factory method aman untuk load dari JSON/YAML."""
        try:
            valid_fields = {f.name for f in fields(cls)}
            filtered_data = {k: v for k, v in data.items() if k in valid_fields}

            # Type casting manual untuk integer
            for int_field in ['max_open_trades', 'circuit_breaker_cooldown_min']:
                if int_field in filtered_data:
                    filtered_data[int_field] = int(filtered_data[int_field])
            
            config = cls(**filtered_data)
            
            val_res = config.validate()
            if val_res.is_err():
                return Err(f"Risk Config Validation Failed: {val_res.unwrap_err()}")
                
            return Ok(config)
        except Exception as e:
            return Err(f"Failed to parse RiskConfig: {str(e)}")


@dataclass(frozen=True)
class AccountState:
    """Snapshot kondisi akun (Immutable)."""
    # --- CORE METRICS ---
    balance: float
    equity: float
    unrealized_pnl: float
    daily_pnl: float
    open_positions_count: int
    
    # --- MARGIN METRICS ---
    used_margin: float = 0.0
    free_margin: float = 0.0
    margin_ratio: float = 0.0
    
    # --- METADATA ---
    is_locked: bool = False
    account_id: str = "default"
    currency: str = "USD"
    updated_at: datetime = field(default_factory=datetime.now)

    # --- CALCULATED PROPERTIES ---
    @property
    def has_open_positions(self) -> bool:
        return self.open_positions_count > 0

    @property
    def leverage_used(self) -> float:
        return (self.equity / self.used_margin) if self.used_margin > 0 else 0.0

    # --- METHODS ---
    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.account_id,
            "equity": self.equity,
            "daily_pnl": self.daily_pnl,
            "positions": self.open_positions_count,
            "updated_at": self.updated_at.isoformat()
        }

    @classmethod
    def from_snapshot(cls, snapshot: Dict[str, Any]) -> 'AccountState':
        return cls(
            balance=float(snapshot.get('balance', 0.0)),
            equity=float(snapshot.get('equity', 0.0)),
            unrealized_pnl=float(snapshot.get('unrealized_pnl', 0.0)),
            daily_pnl=float(snapshot.get('daily_pnl', 0.0)),
            open_positions_count=int(snapshot.get('open_positions', 0)),
            used_margin=float(snapshot.get('used_margin', 0.0)),
            free_margin=float(snapshot.get('free_margin', 0.0)),
            margin_ratio=float(snapshot.get('margin_ratio', 0.0)),
            is_locked=bool(snapshot.get('is_locked', False)),
            currency=str(snapshot.get('asset', 'USD'))
        )


@dataclass
class TradeRequest:
    """Trade request from strategy to risk manager"""
    symbol: str
    action: TradeAction
    requested_size: float
    entry_price: float
    stop_loss: Optional[float] = None
    take_profit: Optional[float] = None
    leverage: float = 1.0
    strategy_id: str = "unknown"
    request_id: str = field(default_factory=lambda: f"req_{datetime.now().timestamp()}")
    timestamp: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def risk_per_unit(self) -> float:
        """Calculate risk per unit if stop loss is set"""
        if self.stop_loss and self.entry_price > 0:
            return abs(self.entry_price - self.stop_loss) / self.entry_price
        return 0.0


@dataclass(frozen=True)
class TradeVerdict:
    """Keputusan Final Hakim (Risk Manager)."""
    approved: bool
    approved_size: float
    risk_level: RiskLevel

    rejection_reason: str = ""
    rejection_code: Optional[RejectionCode] = None

    verdict_id: str = field(default_factory=lambda: f"vd_{datetime.now().timestamp()}")
    timestamp: datetime = field(default_factory=datetime.now)
    request_id: str = ""
        
    # Metrics
    position_risk_pct: float = 0.0
    new_exposure_pct: float = 0.0
    leverage_contribution: float = 0.0
    
    metadata: Dict[str, Any] = field(default_factory=dict)

    @property
    def is_rejected(self) -> bool:
        return not self.approved
    
    @property
    def is_caution(self) -> bool:
        return self.risk_level == RiskLevel.CAUTION
    
    @property
    def is_halt(self) -> bool:
        return self.risk_level == RiskLevel.HALT
    
    @classmethod
    def approve(cls, approved_size: float, request_id: str, risk_metrics: Dict[str, float] = None, metadata: Dict[str, Any] = None) -> 'TradeVerdict':
        metrics = risk_metrics or {}
        return cls(
            approved=True,
            approved_size=approved_size,
            risk_level=RiskLevel.NORMAL,
            request_id=request_id,
            position_risk_pct=metrics.get('risk_pct', 0.0),
            new_exposure_pct=metrics.get('exposure_pct', 0.0),
            leverage_contribution=metrics.get('leverage', 0.0),
            metadata=metadata or {}
        )
    
    @classmethod
    def reject(cls, reason: str, code: RejectionCode, request_id: str, risk_level: RiskLevel = RiskLevel.HALT) -> 'TradeVerdict':
        return cls(
            approved=False,
            approved_size=0.0,
            risk_level=risk_level,
            rejection_reason=reason,
            rejection_code=code,
            request_id=request_id,
            metadata={"status": "REJECTED"}
        )
        
    @classmethod
    def caution(cls, approved_size: float, warning_msg: str, request_id: str, risk_metrics: Dict[str, float] = None) -> 'TradeVerdict':
        metrics = risk_metrics or {}
        return cls(
            approved=True,
            approved_size=approved_size,
            risk_level=RiskLevel.CAUTION,
            request_id=request_id,
            position_risk_pct=metrics.get('risk_pct', 0.0),
            new_exposure_pct=metrics.get('exposure_pct', 0.0),
            leverage_contribution=metrics.get('leverage', 0.0),
            metadata={"warning": warning_msg}
        )

    def replace_risk_level(self, new_level: 'RiskLevel') -> 'TradeVerdict':
        """Helper untuk update level pada frozen dataclass"""
        from dataclasses import replace
        return replace(self, risk_level=new_level)

@dataclass
class RiskMetrics:
    """Real-time risk metrics"""
    sharpe_ratio: Optional[float] = None
    max_drawdown: float = 0.0
    volatility: float = 0.0
    correlation_matrix: Optional[np.ndarray] = None
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class RiskContext:
    """Composition of all risk-related data for decision making"""
    config: RiskConfig
    account_state: AccountState
    trade_request: Optional[TradeRequest] = None
    market_data: Dict[str, Any] = field(default_factory=dict)
    
    @property
    def is_circuit_breaker_active(self) -> bool:
        """Check if circuit breaker should be triggered"""
        if self.account_state.equity <= 0: 
            return True
        
        # [FIX] Hitung manual karena property daily_return sudah dihapus
        current_daily_return = self.account_state.daily_pnl / self.account_state.equity
        
        # [FIX] Config sekarang sudah punya circuit_breaker_threshold
        return current_daily_return <= -self.config.circuit_breaker_threshold
    
    @property
    def available_risk_capital(self) -> float:
        """Calculate available capital for risk"""
        daily_limit_abs = self.account_state.equity * self.config.max_daily_drawdown
        already_lost = max(0, -self.account_state.daily_pnl)
        return max(0, daily_limit_abs - already_lost)


# ==================== FACTORY (FACADE) ====================
class RiskFactory:
    """Factory helper"""
    @staticmethod
    def create_config(**kwargs) -> RiskConfig:
        return RiskConfig.from_dict(kwargs).unwrap_or(RiskConfig()) # Fallback to default
    
    @staticmethod
    def create_account_state(**kwargs) -> AccountState:
        return AccountState(**kwargs)


# ==================== TYPE ALIASES ====================
RiskResult = Result[TradeVerdict, str]
SizeCalculation = Tuple[float, Dict[str, float]]

# ==================== EXPORT ALL ====================
__all__ = [
    'RiskValidatable', 'SizeCalculator',
    'RiskLevel', 'TradeAction', 'RejectionCode',
    'RiskConfig', 'AccountState', 'TradeRequest', 'TradeVerdict', 'RiskMetrics',
    'RiskContext', 'RiskFactory', 'RiskResult', 'SizeCalculation'
]
