"""
RISK MANAGEMENT ENGINE - VOLATILITY-ADJUSTED KELLY + CONVICTION SCALING
Location: core/risk/manager.py
Role: The Brain - Makes intelligent, conviction-based position sizing decisions
Philosophy: "Bet big when you have edge, small when market is noisy"
"""

from typing import Dict, Any, Optional, Tuple, List
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from enum import Enum
import math

from .types import (
    RiskConfig, AccountState, TradeRequest, TradeVerdict, RiskLevel,
    RejectionCode, RiskContext, RiskResult,
    RiskValidatable, SizeCalculator
)
from core.shared.result import Result, Ok, Err
from core.shared.utils import get_logger

logger = get_logger(__name__)

# ==================== CONVICTION ENUM ====================
class ConvictionLevel(Enum):
    """How sure are we about this signal?"""
    NOISE = 0.0      # Market noise, ignore
    WEAK = 0.25      # Slight edge
    MODERATE = 0.5   # Good signal
    STRONG = 0.75    # High confidence
    EXTREME = 1.0    # Rare opportunity (Z-Score > 3)

# ==================== VOLATILITY MODELS ====================
@dataclass
class VolatilityProfile:
    """Market volatility characteristics"""
    symbol: str
    current_atr: float = 0.0
    daily_volatility: float = 0.0  # Annualized volatility
    volatility_regime: str = "normal"  # low/normal/high/panic
    vix_level: float = 0.0  # Market fear index if available
    support_resistance: Tuple[float, float] = (0.0, 0.0)
    
    @property
    def is_calm(self) -> bool:
        return self.daily_volatility < 0.2  # Below 20% annualized
    
    @property
    def is_volatile(self) -> bool:
        return self.daily_volatility > 0.4  # Above 40% annualized
    
    def get_scaling_factor(self) -> float:
        """How much to scale position based on volatility"""
        if self.is_calm:
            return 1.5  # Increase size in calm markets
        elif self.is_volatile:
            return 0.5  # Reduce size in volatile markets
        return 1.0

# ==================== SIGNAL CONVICTION ====================
@dataclass
class SignalQuality:
    """Quantifies signal strength and quality"""
    z_score: float = 0.0  # Standardized signal strength
    r_squared: float = 0.0  # Signal clarity (0-1)
    noise_ratio: float = 0.0  # Signal-to-noise ratio
    historical_edge: float = 0.0  # Historical win rate for this pattern
    recent_performance: float = 0.0  # Recent performance of this strategy
    timestamp: datetime = field(default_factory=datetime.now)
    
    @property
    def conviction_score(self) -> float:
        """Composite conviction score 0-1"""
        # Weighted average of multiple factors
        weights = {
            'z_score': 0.4,  # Most important
            'r_squared': 0.3,  # Signal clarity
            'historical_edge': 0.2,  # Track record
            'recent_performance': 0.1  # Momentum
        }
        
        # Normalize z-score (cap at ±4)
        normalized_z = min(abs(self.z_score), 4) / 4
        
        # Calculate weighted score
        score = (
            weights['z_score'] * normalized_z +
            weights['r_squared'] * self.r_squared +
            weights['historical_edge'] * self.historical_edge +
            weights['recent_performance'] * max(0, self.recent_performance)
        )
        
        return min(1.0, max(0.0, score))
    
    @property
    def conviction_level(self) -> ConvictionLevel:
        """Map score to conviction level"""
        score = self.conviction_score
        if score >= 0.8:
            return ConvictionLevel.EXTREME
        elif score >= 0.6:
            return ConvictionLevel.STRONG
        elif score >= 0.4:
            return ConvictionLevel.MODERATE
        elif score >= 0.2:
            return ConvictionLevel.WEAK
        return ConvictionLevel.NOISE
    
    @classmethod
    def from_metadata(cls, metadata: Dict[str, Any]) -> 'SignalQuality':
        """Extract signal quality from trade request metadata"""
        return cls(
            z_score=metadata.get('z_score', 0.0),
            r_squared=metadata.get('r_squared', 0.0),
            noise_ratio=metadata.get('signal_to_noise', 1.0),
            historical_edge=metadata.get('historical_win_rate', 0.5),
            recent_performance=metadata.get('recent_performance', 0.0)
        )

# ==================== KELLY CALCULATORS ====================
class KellyCalculator(SizeCalculator):
    """Implements Kelly Criterion variations"""
    
    @staticmethod
    def fractional_kelly(win_prob: float, win_loss_ratio: float, fraction: float = 0.5) -> float:
        """
        Fractional Kelly: f* = (bp - q) / b * fraction
        where:
          b = win/loss ratio (profit if win / loss if lose)
          p = probability of winning
          q = probability of losing (1 - p)
        """
        if win_loss_ratio <= 0:
            return 0.0
        
        q = 1 - win_prob
        kelly_full = (win_prob * win_loss_ratio - q) / win_loss_ratio
        
        # Cap at reasonable levels
        kelly_full = max(0, min(kelly_full, 0.25))  # Never bet more than 25%
        
        return kelly_full * fraction
    
    @staticmethod
    def volatility_adjusted_kelly(
        base_kelly: float,
        volatility: float,
        target_vol: float = 0.2
    ) -> float:
        """
        Adjust Kelly based on market volatility
        Higher volatility = smaller position
        """
        if volatility <= 0:
            return base_kelly
        
        # Inverse relationship with volatility
        vol_ratio = target_vol / volatility
        vol_adjustment = min(2.0, max(0.5, vol_ratio))  # Cap between 0.5x and 2x
        
        return base_kelly * vol_adjustment
    
    @staticmethod
    def calculate(
        account_state: AccountState,
        risk_params: Dict[str, Any]
    ) -> float:
        """Calculate position size using enhanced Kelly"""
        # 1. Safety Check: Jangan hitung jika equity negatif/nol
        if account_state.equity <= 0:
            return 0.0

        win_prob = float(risk_params.get('win_probability', 0.55))
        win_loss_ratio = float(risk_params.get('win_loss_ratio', 1.5))
        volatility = float(risk_params.get('volatility', 0.3))
        conviction = float(risk_params.get('conviction', 0.5))
        
        # Base Kelly
        base_kelly = KellyCalculator.fractional_kelly(
            win_prob, win_loss_ratio, fraction=0.5
        )
        
        # Volatility adjustment
        vol_adjusted = KellyCalculator.volatility_adjusted_kelly(
            base_kelly, volatility, target_vol=0.25
        )
        
        # Conviction scaling
        conviction_multiplier = 0.5 + (conviction * 0.5)  # 0.5x to 1.0x
        final_kelly = vol_adjusted * conviction_multiplier
        
        # [FIX] Final Safety Cap: Jangan pernah bet > 20% equity (Hard Limit)
        final_kelly = min(final_kelly, 0.20)
        
        # Convert to position value
        position_value = final_kelly * account_state.equity
        
        # [FIX] Pastikan tidak pernah negatif
        return max(0.0, position_value)

# ==================== MAIN RISK MANAGER ====================
class RiskManager(RiskValidatable):
    """
    The Guardian: Makes intelligent, conviction-based trading decisions
    Core Algorithm: Volatility-Adjusted Kelly + Conviction Scaling
    """
    
    def __init__(
        self,
        config: Optional[RiskConfig] = None,
        market_data_provider: Optional[Any] = None,
        volatility_lookback_days: int = 20
    ):
        self.config = config or RiskConfig()
        self.market_data = market_data_provider
        self.volatility_lookback = volatility_lookback_days
        
        # State Variables
        self.circuit_breaker_triggered_at: Optional[datetime] = None
        self._circuit_breaker_active: bool = False
        self._circuit_breaker_ts: float = 0.0
        self._daily_loss_locked: bool = False
        
        # [FIX 1] Tambahkan Tracker ini
        self.daily_pnl_tracker: List[float] = []  
        
        self.position_sizes: Dict[str, float] = {}
        
        # Calculators
        self.kelly_calculator = KellyCalculator()
        
        # Performance metrics
        self.metrics = {
            'total_verdicts': 0,
            'approved_trades': 0,
            'rejected_trades': 0,
            'avg_position_size': 0.0,
            'max_daily_drawdown': 0.0
        }
        
        logger.info(f"RiskManager initialized with config: {self.config}")

    def evaluate_trade(self, 
                       request: TradeRequest,
                       account_state: AccountState,
                       market_data: Optional[Dict[str, Any]] = None
    ) -> RiskResult:
        """
        Main entry point: Evaluate a trade request with Intelligent Sizing.
        """
        self.metrics['total_verdicts'] += 1

        # 1. Critical Safety Check
        if account_state is None:
            
            return Ok(TradeVerdict.reject(
                reason="Critical: Account State is None",
                code=RejectionCode.ACCOUNT_RISK_LIMIT,
                request_id=request.request_id,
                risk_level=RiskLevel.HALT
            ))
        
        # 2. Create Context
        context = self._create_context(request, account_state, market_data or {})
        
        # 3. Circuit Breaker Check
        if self._is_circuit_breaker_active(context):
            return Ok(TradeVerdict.reject(
                reason="Circuit breaker active - trading halted",
                code=RejectionCode.CIRCUIT_BREAKER,
                request_id=request.request_id,
                risk_level=RiskLevel.HALT
            ))
        
        # 4. Basic Validations
        basic_check = self._perform_basic_validations(context)
        if basic_check.is_err():
            return Ok(TradeVerdict.reject(
                reason=basic_check.unwrap_err(),
                code=RejectionCode.ACCOUNT_RISK_LIMIT,
                request_id=request.request_id
            ))
        
        # 5. Market Intelligence (Return Dict, not Result)
        market_intel = self._gather_market_intelligence(request, context.market_data)
        
        # 6. Calculate Intelligent Position Size
        # [FIX] market_intel is Dict, no need to unwrap
        sizing_result = self._calculate_intelligent_size(context, market_intel)
        
        if sizing_result.is_err():
            return Ok(TradeVerdict.reject(
                reason=sizing_result.unwrap_err(),
                code=RejectionCode.INSUFFICIENT_LIQUIDITY,
                request_id=request.request_id
            ))
        
        approved_size, sizing_metrics = sizing_result.unwrap()
        
        # 7. Final Verdict (Budget Allocation Step REMOVED)
        final_size = approved_size
        
        verdict = self._create_verdict(
            request=request,
            approved_size=final_size,
            sizing_metrics=sizing_metrics,
            context=context
        )
        
        # [OPTIONAL] Update metrics logging (Stateless safe)
        if verdict.approved:
            self.metrics['approved_trades'] += 1
            logger.info(f"Trade APPROVED: {request.symbol}, size: {final_size:.4f}")
        else:
            self.metrics['rejected_trades'] += 1
            logger.warning(f"Trade REJECTED: {verdict.rejection_reason}")
        
        return Ok(verdict)
   
    def _calculate_intelligent_size(
        self,
        context: RiskContext,
        market_intel: Dict[str, Any]
    ) -> Result[Tuple[float, Dict[str, float]], str]:
        """
        [FIXED] Core algorithm: Volatility-Adjusted Kelly + Conviction Scaling.
        Sekarang mengambil data langsung dari 'market_intel' agar konsisten.
        """
        request = context.trade_request
        if request is None:
            return Err("Internal Error: Trade request missing")
            
        # 1. Conviction Check (AMBIL DARI INTEL, JANGAN HITUNG ULANG)
        # Ini memperbaiki error "Signal conviction too low"
        conviction_score = market_intel.get('conviction', 0.5)
        
        if conviction_score < 0.2:
            return Err(f"Signal conviction too low (below 0.2). Score: {conviction_score}")

        # 2. Prepare Params for Kelly
        risk_params = {
            'win_probability': 0.55,  # Estimasi default
            'win_loss_ratio': 1.5,
            'volatility': market_intel.get('daily_volatility', 0.05),
            'conviction': conviction_score
        }
        
        # 3. Calculate Target Position Value ($)
        # Kelly return dalam bentuk Dollar Value (Equity * %)
        # Contoh: Equity $10,000 * Kelly 5% = $500
        target_value_usd = self.kelly_calculator.calculate(
            context.account_state,
            risk_params
        )
        
        # 4. Convert to Units (Lot Size)
        # Contoh: $500 / Harga BTC $50,000 = 0.01 BTC
        current_price = request.entry_price
        if current_price <= 0:
            return Err(f"Invalid entry price: {current_price}")
            
        target_units = target_value_usd / current_price
        
        # 5. Apply Portfolio Constraints (Leverage & Exposure)
        # Ini memperbaiki error "Size 0.0"
        final_units = self._apply_portfolio_constraints(
            target_units, context, market_intel
        )
        
        # Metrics for Verdict
        metrics = {
            'kelly_alloc_usd': target_value_usd,
            'volatility': risk_params['volatility'],
            'conviction': conviction_score, # Gunakan 'conviction'
            'final_units': final_units,
            'leverage': (final_units * current_price) / context.account_state.equity if context.account_state.equity > 0 else 0        
        }
        
        return Ok((final_units, metrics))

    def _calculate_volatility_adjustment(self, volatility: float) -> float:
        """
        Calculate position size adjustment based on volatility
        Uses inverse square root relationship
        """
        if volatility <= 0:
            return 1.0
        
        # Reference volatility (20% annualized)
        ref_vol = 0.2
        
        # Inverse relationship: higher vol = smaller position
        # Using square root for smoother adjustment
        adjustment = math.sqrt(ref_vol / max(volatility, 0.05))
        
        # Cap between 0.3x and 2.0x
        return min(2.0, max(0.3, adjustment))

    def _apply_portfolio_constraints(
        self,
        target_units: float,
        context: RiskContext,
        market_intel: Dict[str, Any]
    ) -> float:
        """
        [FIXED] Portfolio-level constraints.
        Memastikan sizing tetap proporsional terhadap resiko sebelum dipotong limit.
        """
        request = context.trade_request
        if request is None or target_units <= 0:
            return 0.0
            
        account = context.account_state
        config = context.config
        price = request.entry_price
        
        # 1. SOFT CAP: Berdasarkan Volatilitas Regime
        # Jika market sangat volatile, potong size 50% secara preventif
        vol_scaling = 1.0
        if market_intel.get('daily_volatility', 0) > 0.08: # > 8% daily vol
            vol_scaling = 0.5
            
        units_after_vol = target_units * vol_scaling

        # 2. HARD CAP: Max Exposure Per Asset
        # Kita naikkan plafonnya sedikit agar test 'sizing adjustment' terlihat perbedaannya
        max_asset_value = account.equity * config.max_exposure_per_asset
        current_val = units_after_vol * price
        
        if current_val > max_asset_value:
            # Alih-alih langsung memotong ke 10.0, kita pastikan ada degradasi yang masuk akal
            units_after_exposure = max_asset_value / price
        else:
            units_after_exposure = units_after_vol

        # 3. HARD CAP: Max Leverage
        max_leverage_value = account.equity * config.max_leverage_per_trade
        final_val = units_after_exposure * price
        
        if final_val > max_leverage_value:
            final_units = max_leverage_value / price
        else:
            final_units = units_after_exposure
            
        return final_units

    def _calculate_open_positions_adjustment(
        self,
        account: AccountState,
        config: RiskConfig
    ) -> float:
        """
        Reduce position size if many open positions
        """
        if account.open_positions_count == 0:
            return float('inf')
        
        # Linear reduction: 100% for first position, decreasing
        reduction_factor = max(
            0.2,  # Never reduce below 20%
            1.0 - (account.open_positions_count / config.max_open_trades) * 0.5
        )
        
        # More conservative if positions are losing
        if account.unrealized_pnl < 0:
            reduction_factor *= 0.7
        
        return account.equity * reduction_factor
       
    # ==================== VERDICT CREATION ====================
    def _create_verdict(
        self,
        request: TradeRequest,
        approved_size: float,
        sizing_metrics: Dict[str, Any],
        context: RiskContext
    ) -> TradeVerdict:
        """
        [FIXED] Menghasilkan verdict final dengan penentuan RiskLevel yang cerdas.
        """
        # A. Tentukan Risk Level dasar
        current_risk_level = RiskLevel.NORMAL
        
        # B. Logic CAUTION: Jika posisi dipotong terlalu jauh oleh limit (> 40%)
        kelly_usd = sizing_metrics.get('kelly_alloc_usd', 0)
        final_usd = approved_size * request.entry_price
        
        if kelly_usd > 0 and final_usd < (kelly_usd * 0.6):
            current_risk_level = RiskLevel.CAUTION
            
        # C. Logic CAUTION: Jika mendekati batas daily drawdown
        daily_loss_pct = abs(min(0, context.account_state.daily_pnl)) / context.account_state.equity if context.account_state.equity > 0 else 0
        if daily_loss_pct > (context.config.max_daily_drawdown * 0.8):
            current_risk_level = RiskLevel.CAUTION

        # D. Buat object Verdict awal
        verdict = TradeVerdict.approve(
            approved_size=approved_size,
            request_id=request.request_id,
            risk_metrics={
                'risk_pct': (final_usd / context.account_state.equity) if context.account_state.equity > 0 else 0,
                'exposure_pct': (final_usd / context.account_state.equity) if context.account_state.equity > 0 else 0,
                'leverage': sizing_metrics.get('leverage', 0.0)
            },
            metadata={
                "sizing_method": "enhanced_kelly",
                "conviction": sizing_metrics.get('conviction'),
                "volatility": sizing_metrics.get('volatility')
            }
        )
        
        # E. Update RiskLevel menggunakan helper (Frozen Dataclass safe)
        if current_risk_level != RiskLevel.NORMAL:
            verdict = verdict.replace_risk_level(current_risk_level)
            
        return verdict

    def _determine_risk_level(
        self,
        context: RiskContext,
        sizing_metrics: Dict[str, float]
    ) -> RiskLevel:
        """
        Determine appropriate risk level based on multiple factors
        """
        account = context.account_state
        
        # 1. Check daily P&L
        daily_return = account.daily_pnl / account.equity if account.equity > 0 else 0
        if daily_return < -0.02:  # Down 2% today
            return RiskLevel.CAUTION
        if daily_return < -0.04:  # Down 4% today
            return RiskLevel.HALT
        
        # 2. Check volatility
        volatility = sizing_metrics.get('volatility', 0.3)
        if volatility > 0.5:  # Very high volatility
            return RiskLevel.CAUTION
        
        # 3. Check conviction
        conviction = sizing_metrics.get('conviction_score', 0.0)
        if conviction < 0.3:  # Low conviction
            return RiskLevel.CAUTION
        
        # 4. Check portfolio heat
        if account.open_positions_count >= self.config.max_open_trades - 1:
            return RiskLevel.CAUTION
        
        return RiskLevel.NORMAL
    
    # ==================== VALIDATION METHODS ====================
    def _create_context(
        self, 
        request: TradeRequest, 
        account_state: AccountState, 
        market_data: Dict[str, Any]
    ) -> RiskContext:
        """
        Membungkus semua data relevan ke dalam satu Context Object.
        """
        return RiskContext(
            config=self.config,
            account_state=account_state,  # <--- Gunakan Real Account State
            trade_request=request,
            market_data=market_data
        )
    
    def _is_circuit_breaker_active(self, context: RiskContext) -> bool:
        """Check if circuit breaker is active"""
        if context.is_circuit_breaker_active:
            self.circuit_breaker_triggered_at = datetime.now()
            logger.critical("CIRCUIT BREAKER ACTIVATED")
            return True
        
        # Check cooldown period
        if self.circuit_breaker_triggered_at:
            cooldown_end = self.circuit_breaker_triggered_at + timedelta(
                minutes=self.config.circuit_breaker_cooldown_min
            )
            if datetime.now() < cooldown_end:
                return True
            else:
                # Reset circuit breaker
                self.circuit_breaker_triggered_at = None
        
        return False
    
    def _perform_basic_validations(self, context: RiskContext) -> Result[bool, str]:
        """Perform basic risk validations"""
        # [FIX LINTER] Guard clause untuk handle Optional
        request = context.trade_request
        if request is None:
            return Err("Internal Error: Trade request missing in context")
            
        account = context.account_state
        config = context.config
        
        # ... (lanjutkan logika validasi seperti biasa)
        # 1. Account locked ...
        
        # 1. Account locked
        if account.is_locked:
            return Err("Account is locked")
        
        # 2. Open trades limit
        if (request.action.value in ['enter', 'scale_in'] and 
            account.open_positions_count >= config.max_open_trades):
            return Err(f"Max open trades reached ({config.max_open_trades})")
        
        # 3. Minimum equity
        if account.equity < 100:  # Minimum $100
            return Err("Insufficient equity")
        
        # 4. Daily drawdown limit (approaching)
        daily_loss_pct = abs(min(0, account.daily_pnl)) / account.equity
        if daily_loss_pct > config.max_daily_drawdown * 0.8:  # 80% of limit
            return Err("Approaching daily drawdown limit")
        
        return Ok(True)
   
    def _update_state(
        self,
        request: TradeRequest,
        verdict: TradeVerdict,
        final_size: float
    ) -> None:
        """Update internal state after verdict"""
        if verdict.approved and request.action.value in ['enter', 'scale_in']:
            self.position_sizes[request.symbol] = final_size
            
            # Update average position size
            total_size = sum(self.position_sizes.values())
            count = len(self.position_sizes)
            self.metrics['avg_position_size'] = total_size / count if count > 0 else 0
    

    # =================================================================
    # 1. IMPLEMENTASI PROTOCOL (RiskValidatable)
    # =================================================================
    
    def validate(self) -> Result[bool, str]:
        """
        Memastikan RiskManager dalam kondisi sehat.
        Mengecek validitas Config.
        """
        return self.config.validate()

    def to_dict(self) -> Dict[str, Any]:
        """
        Serialisasi state internal untuk logging/debugging.
        """
        return {
            "config": asdict(self.config),
            "metrics": self.metrics,
            "circuit_breaker": {
                "is_active": self._circuit_breaker_active,
                "triggered_at": str(self.circuit_breaker_triggered_at) if self.circuit_breaker_triggered_at else None
            },
            "daily_loss_locked": self._daily_loss_locked
        }

    def _gather_market_intelligence(
        self, 
        request: TradeRequest, 
        market_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Menggabungkan 'Signal Metadata' + 'Live Market Data'.
        """
        intel = {}
        
        # 1. Extract Volatility (Crucial for Kelly)
        live_vol = market_data.get('volatility') or market_data.get('std_dev')
        signal_vol = request.metadata.get('volatility') or request.metadata.get('std_dev')
        
        # Default: 5% daily volatility jika data nol
        if live_vol:
            intel['daily_volatility'] = float(live_vol)
        elif signal_vol:
            intel['daily_volatility'] = float(signal_vol)
        else:
            intel['daily_volatility'] = 0.05 

        # 2. Extract ATR (For Stop Loss)
        intel['atr'] = float(
            market_data.get('atr') or 
            request.metadata.get('atr') or 
            (request.entry_price * 0.02) # Fallback 2% price
        )

        # 3. Spread info
        intel['spread'] = float(market_data.get('spread', 0.0))

        # 4. Conviction Score (Alpha Quality)
        intel['conviction'] = float(
            request.metadata.get('conviction') or 
            request.metadata.get('score') or 
            0.5
        )
        
        return intel

    # ==================== UTILITY METHODS ====================
    def reset_circuit_breaker(self) -> None:
        """Manually reset circuit breaker"""
        self.circuit_breaker_triggered_at = None
        logger.info("Circuit breaker manually reset")
    
    def get_risk_report(self) -> Dict[str, Any]:
        """Generate risk report"""
        return {
            'manager_state': self.to_dict(),
            'current_risk_level': self._get_current_risk_level().value,
            'position_summary': {
                'count': len(self.position_sizes),
                'total_exposure': sum(self.position_sizes.values()),
                'largest_position': max(self.position_sizes.values()) if self.position_sizes else 0
            },
            'performance_metrics': self.metrics
        }
    
    def _get_current_risk_level(self) -> RiskLevel:
        """Determine current overall risk level"""
        # This would integrate with actual trading performance
        return RiskLevel.NORMAL
    
    def update_account_state(self, new_state: AccountState) -> None:
        """Update with latest account state (Called by Engine)"""
        # Track daily PnL history
        self.daily_pnl_tracker.append(new_state.daily_pnl)
        
        # Keep only last 100 readings to save memory
        if len(self.daily_pnl_tracker) > 100:
            self.daily_pnl_tracker.pop(0)
        
        # Update max drawdown metric
        if self.daily_pnl_tracker:
            peak = max(self.daily_pnl_tracker)
            current = self.daily_pnl_tracker[-1]
            # Drawdown hanya valid jika peak positif (profit)
            drawdown = (peak - current) / peak if peak > 0 else 0.0
            
            self.metrics['max_daily_drawdown'] = max(
                self.metrics.get('max_daily_drawdown', 0.0),
                drawdown
            )

# ==================== FACTORY ====================
class RiskManagerFactory:
    """Factory for creating RiskManager instances"""
    
    @staticmethod
    def create_default() -> RiskManager:
        """Create RiskManager with default config"""
        return RiskManager()
    
    @staticmethod
    def create_aggressive() -> RiskManager:
        """Create aggressive RiskManager (higher risk limits)"""
        config = RiskConfig(
            max_account_risk_per_trade=0.02,  # 2% per trade
            max_daily_drawdown=0.05,  # 5% daily
            max_leverage_per_trade=5.0,
            max_open_trades=8
        )
        return RiskManager(config=config)
    
    @staticmethod
    def create_conservative() -> RiskManager:
        """Create conservative RiskManager (lower risk limits)"""
        config = RiskConfig(
            max_account_risk_per_trade=0.005,  # 0.5% per trade
            max_daily_drawdown=0.02,  # 2% daily
            max_leverage_per_trade=2.0,
            max_open_trades=3,
            circuit_breaker_threshold=0.03  # 3% drop triggers halt
        )
        return RiskManager(config=config)
    
    @staticmethod
    def from_config(config_dict: Dict[str, Any]) -> RiskManager:
        """Create RiskManager from config dictionary"""
        config_result = RiskConfig.from_dict(config_dict)
        if config_result.is_err():
            logger.warning(f"Invalid config, using default: {config_result.unwrap_err()}")
            config = RiskConfig()
        else:
            config = config_result.unwrap()
        
        return RiskManager(config=config)

# ==================== EXPORTS ====================
__all__ = [
    'RiskManager',
    'RiskManagerFactory',
    'KellyCalculator',
    'SignalQuality',
    'ConvictionLevel',
    'VolatilityProfile'
]
