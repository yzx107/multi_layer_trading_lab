from __future__ import annotations

from datetime import UTC, datetime, timedelta

from multi_layer_trading_lab.backtest.types import Fill, Side, SignalEvent
from multi_layer_trading_lab.execution.interfaces import Quote
from multi_layer_trading_lab.risk.manager import RiskLimits, RiskManager


def build_quote(last: float = 100.0, bid: float = 99.9, ask: float = 100.1) -> Quote:
    return Quote(
        symbol="0700.HK",
        timestamp=datetime.now(UTC),
        bid=bid,
        ask=ask,
        last=last,
    )


def test_risk_rejects_expired_signal() -> None:
    manager = RiskManager()
    signal = SignalEvent(
        signal_id="sig-1",
        timestamp=datetime.now(UTC) - timedelta(minutes=10),
        symbol="0700.HK",
        side=Side.BUY,
        quantity=100,
        ttl_seconds=60,
    )

    approved, reason = manager.validate_signal(
        signal=signal,
        now=datetime.now(UTC),
        quote=build_quote(),
    )

    assert approved is False
    assert reason == "signal_expired"


def test_risk_halts_after_drawdown_breach() -> None:
    manager = RiskManager(limits=RiskLimits(max_daily_drawdown=100.0))
    manager.state.realized_pnl = -120.0
    signal = SignalEvent(
        signal_id="sig-2",
        timestamp=datetime.now(UTC),
        symbol="0700.HK",
        side=Side.BUY,
        quantity=10,
    )

    approved, reason = manager.validate_signal(
        signal=signal,
        now=datetime.now(UTC),
        quote=build_quote(),
    )

    assert approved is False
    assert reason == "daily_drawdown_breached"
    assert manager.state.halted is True


def test_risk_updates_positions_on_fill() -> None:
    manager = RiskManager()
    fill = Fill(
        order_id="ord-1",
        symbol="0700.HK",
        side=Side.BUY,
        quantity=100,
        price=100.0,
        timestamp=datetime.now(UTC),
        fees=1.0,
    )

    manager.on_fill(fill=fill, strategy_id="open-auction")

    assert manager.state.positions["0700.HK"] == 100
    assert manager.state.strategy_notional["open-auction"] == 10_000.0
