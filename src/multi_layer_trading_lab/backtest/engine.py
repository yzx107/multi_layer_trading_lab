from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone

from multi_layer_trading_lab.backtest.types import Fill, SignalEvent
from multi_layer_trading_lab.execution.interfaces import MarketDataAdapter
from multi_layer_trading_lab.execution.order_manager import ManagedOrderResult, OrderManager


@dataclass(slots=True)
class BacktestMetrics:
    total_pnl: float
    turnover: float
    max_drawdown: float
    hit_ratio: float
    fills: int
    rejected: int


@dataclass(slots=True)
class BacktestResult:
    fills: list[Fill] = field(default_factory=list)
    rejections: list[ManagedOrderResult] = field(default_factory=list)
    metrics: BacktestMetrics | None = None


class EventDrivenBacktester:
    def __init__(self, market_data: MarketDataAdapter, order_manager: OrderManager) -> None:
        self.market_data = market_data
        self.order_manager = order_manager

    def run(self, signals: list[SignalEvent]) -> BacktestResult:
        ordered_signals = sorted(signals, key=lambda item: item.timestamp)
        fills: list[Fill] = []
        rejections: list[ManagedOrderResult] = []
        equity_curve: list[float] = [0.0]
        gross_turnover = 0.0
        wins = 0

        for signal in ordered_signals:
            now = signal.timestamp.astimezone(timezone.utc)
            quote = self.market_data.get_quote(signal.symbol, timestamp=now)
            managed = self.order_manager.submit_signal(signal=signal, now=now, quote=quote)
            fill = managed.order_result.fill
            if fill is None:
                rejections.append(managed)
                continue

            fills.append(fill)
            gross_turnover += fill.price * fill.quantity
            avg_cost = self.order_manager.risk_manager.state.avg_cost.get(fill.symbol, fill.price)
            realized = self.order_manager.risk_manager.state.realized_pnl

            mark = quote.mid if quote else fill.price
            unrealized = self.order_manager.risk_manager.state.positions.get(fill.symbol, 0.0) * (mark - avg_cost)
            equity = realized + unrealized
            equity_curve.append(equity)
            if realized > equity_curve[-2]:
                wins += 1

        metrics = self._build_metrics(equity_curve, gross_turnover, fills, rejections, wins)
        return BacktestResult(fills=fills, rejections=rejections, metrics=metrics)

    def _build_metrics(
        self,
        equity_curve: list[float],
        turnover: float,
        fills: list[Fill],
        rejections: list[ManagedOrderResult],
        wins: int,
    ) -> BacktestMetrics:
        peak = equity_curve[0]
        max_drawdown = 0.0
        for equity in equity_curve:
            peak = max(peak, equity)
            max_drawdown = min(max_drawdown, equity - peak)

        hit_ratio = wins / len(fills) if fills else 0.0
        return BacktestMetrics(
            total_pnl=equity_curve[-1],
            turnover=turnover,
            max_drawdown=abs(max_drawdown),
            hit_ratio=hit_ratio,
            fills=len(fills),
            rejected=len(rejections),
        )

