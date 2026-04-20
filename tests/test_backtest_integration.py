from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from multi_layer_trading_lab.backtest.engine import EventDrivenBacktester
from multi_layer_trading_lab.backtest.types import Side, SignalEvent
from multi_layer_trading_lab.execution.adapters import DryRunBrokerAdapter
from multi_layer_trading_lab.execution.interfaces import MarketDataAdapter, Quote
from multi_layer_trading_lab.execution.logging import ExecutionLogWriter
from multi_layer_trading_lab.execution.order_manager import OrderManager
from multi_layer_trading_lab.risk.manager import RiskLimits, RiskManager


class StaticMarketDataAdapter(MarketDataAdapter):
    def __init__(self, quotes: dict[str, Quote]) -> None:
        self.quotes = quotes

    def get_quote(self, symbol: str, timestamp: datetime | None = None) -> Quote | None:
        return self.quotes.get(symbol)


def test_event_driven_backtest_generates_fills_and_execution_log(tmp_path: Path) -> None:
    now = datetime.now(timezone.utc)
    quote = Quote(
        symbol="0700.HK",
        timestamp=now,
        bid=320.0,
        ask=320.2,
        last=320.1,
    )
    market_data = StaticMarketDataAdapter({"0700.HK": quote})
    risk_manager = RiskManager(
        limits=RiskLimits(
            max_position_notional=1_000_000.0,
            max_strategy_notional=1_000_000.0,
            max_daily_drawdown=10_000.0,
        )
    )
    log_path = tmp_path / "execution_log.jsonl"
    order_manager = OrderManager(
        broker=DryRunBrokerAdapter(),
        risk_manager=risk_manager,
        execution_log_writer=ExecutionLogWriter(log_path),
    )
    signals = [
        SignalEvent(
            signal_id="sig-buy",
            timestamp=now,
            symbol="0700.HK",
            side=Side.BUY,
            quantity=100,
            strategy_id="open-auction",
        ),
        SignalEvent(
            signal_id="sig-sell",
            timestamp=now + timedelta(minutes=1),
            symbol="0700.HK",
            side=Side.SELL,
            quantity=100,
            strategy_id="open-auction",
        ),
    ]

    result = EventDrivenBacktester(market_data=market_data, order_manager=order_manager).run(signals=signals)

    assert result.metrics is not None
    assert result.metrics.fills == 2
    assert result.metrics.turnover > 0
    assert log_path.exists()

    lines = log_path.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 2
    first = json.loads(lines[0])
    assert first["status"] == "filled"
    assert first["mode"] == "dry_run"
