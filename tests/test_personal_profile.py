from __future__ import annotations

import pytest

from multi_layer_trading_lab.risk.profile import PersonalAccountProfile, personal_trader_profile


def test_personal_trader_profile_translates_account_equity_to_limits() -> None:
    profile = personal_trader_profile(account_equity=1_000_000)

    limits = profile.to_risk_limits()

    assert limits.max_position_notional == 80_000
    assert limits.max_strategy_notional == 200_000
    assert limits.max_daily_drawdown == 10_000
    assert limits.max_open_slippage_bps == 35
    assert profile.max_gross_notional == 1_000_000
    assert profile.default_kelly_scale == 0.125


def test_personal_profile_rejects_inconsistent_position_budget() -> None:
    with pytest.raises(ValueError, match="max_single_name_pct"):
        PersonalAccountProfile(max_single_name_pct=0.30, max_strategy_pct=0.20)
