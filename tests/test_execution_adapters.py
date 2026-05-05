from __future__ import annotations

from datetime import UTC, datetime

from multi_layer_trading_lab.backtest.types import ExecutionMode, Order, Side
from multi_layer_trading_lab.execution.adapters import DryRunBrokerAdapter, PaperBrokerAdapter
from multi_layer_trading_lab.execution.interfaces import Quote


def test_dry_run_broker_fills_order_from_quote() -> None:
    adapter = DryRunBrokerAdapter()
    order = Order(
        order_id="ord-1",
        created_at=datetime.now(UTC),
        symbol="0700.HK",
        side=Side.BUY,
        quantity=100,
        mode=ExecutionMode.DRY_RUN,
    )
    quote = Quote(
        symbol="0700.HK",
        timestamp=datetime.now(UTC),
        bid=320.0,
        ask=320.2,
        last=320.1,
    )

    result = adapter.submit_order(order=order, quote=quote)

    assert result.accepted is True
    assert result.fill is not None
    assert result.fill.price > quote.ask
    assert result.fill.fees > 0


def test_paper_broker_rejects_missing_quote() -> None:
    adapter = PaperBrokerAdapter()
    order = Order(
        order_id="ord-2",
        created_at=datetime.now(UTC),
        symbol="AAPL.US",
        side=Side.SELL,
        quantity=5,
        mode=ExecutionMode.PAPER,
    )

    result = adapter.submit_order(order=order, quote=None)

    assert result.accepted is False
    assert result.reason == "missing_quote"

