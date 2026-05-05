from __future__ import annotations

from dataclasses import dataclass

from multi_layer_trading_lab.risk.manager import RiskLimits


@dataclass(frozen=True, slots=True)
class PersonalAccountProfile:
    """Translate a personal account size into conservative trading limits."""

    account_equity: float = 1_000_000.0
    max_single_name_pct: float = 0.08
    max_strategy_pct: float = 0.20
    max_daily_drawdown_pct: float = 0.01
    max_open_slippage_bps: float = 35.0
    max_gross_leverage: float = 1.0
    default_kelly_scale: float = 0.125

    def __post_init__(self) -> None:
        if self.account_equity <= 0:
            raise ValueError("account_equity must be positive")
        for name in (
            "max_single_name_pct",
            "max_strategy_pct",
            "max_daily_drawdown_pct",
            "max_gross_leverage",
            "default_kelly_scale",
        ):
            value = getattr(self, name)
            if value <= 0:
                raise ValueError(f"{name} must be positive")
        if self.max_single_name_pct > self.max_strategy_pct:
            raise ValueError("max_single_name_pct cannot exceed max_strategy_pct")
        if self.max_strategy_pct > self.max_gross_leverage:
            raise ValueError("max_strategy_pct cannot exceed max_gross_leverage")

    @property
    def max_single_name_notional(self) -> float:
        return self.account_equity * self.max_single_name_pct

    @property
    def max_strategy_notional(self) -> float:
        return self.account_equity * self.max_strategy_pct

    @property
    def max_daily_drawdown(self) -> float:
        return self.account_equity * self.max_daily_drawdown_pct

    @property
    def max_gross_notional(self) -> float:
        return self.account_equity * self.max_gross_leverage

    def to_risk_limits(self) -> RiskLimits:
        return RiskLimits(
            max_position_notional=self.max_single_name_notional,
            max_strategy_notional=self.max_strategy_notional,
            max_daily_drawdown=self.max_daily_drawdown,
            max_open_slippage_bps=self.max_open_slippage_bps,
        )


def personal_trader_profile(account_equity: float = 1_000_000.0) -> PersonalAccountProfile:
    return PersonalAccountProfile(account_equity=account_equity)
