from __future__ import annotations

from datetime import UTC, datetime

from multi_layer_trading_lab.backtest.types import Fill, Side
from multi_layer_trading_lab.research.cost_capacity import (
    audit_personal_capacity,
    audit_trade_costs,
)
from multi_layer_trading_lab.risk.profile import personal_trader_profile


def make_fill(symbol: str = "00700.HK", quantity: float = 100, price: float = 320.0) -> Fill:
    return Fill(
        order_id="ord-1",
        symbol=symbol,
        side=Side.BUY,
        quantity=quantity,
        price=price,
        timestamp=datetime.now(UTC),
        slippage=0.05,
    )


def test_cost_audit_passes_reasonable_fills() -> None:
    result = audit_trade_costs([make_fill()], max_total_cost_bps=40.0)

    assert result.passed is True
    assert result.total_notional == 32_000.0
    assert result.total_cost_bps > 0


def test_cost_audit_rejects_no_fills() -> None:
    result = audit_trade_costs([])

    assert result.passed is False
    assert result.failed_reasons == ("no_fills",)


def test_capacity_audit_rejects_oversized_symbol_for_personal_account() -> None:
    profile = personal_trader_profile(account_equity=1_000_000)
    result = audit_personal_capacity(
        [make_fill(quantity=400, price=320.0)],
        profile=profile,
    )

    assert result.passed is False
    assert "single_fill_exceeds_single_name_limit" in result.failed_reasons
    assert "symbol_notional_exceeds_single_name_limit" in result.failed_reasons
