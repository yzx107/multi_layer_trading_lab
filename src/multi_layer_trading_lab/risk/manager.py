from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from multi_layer_trading_lab.backtest.types import Fill, Side, SignalEvent
from multi_layer_trading_lab.execution.interfaces import Quote


@dataclass(slots=True)
class RiskLimits:
    max_position_notional: float = 100_000.0
    max_strategy_notional: float = 250_000.0
    max_daily_drawdown: float = 5_000.0
    max_open_slippage_bps: float = 50.0


@dataclass(slots=True)
class RiskState:
    positions: dict[str, float] = field(default_factory=dict)
    avg_cost: dict[str, float] = field(default_factory=dict)
    strategy_notional: dict[str, float] = field(default_factory=dict)
    realized_pnl: float = 0.0
    halted: bool = False


class RiskManager:
    def __init__(self, limits: RiskLimits | None = None) -> None:
        self.limits = limits or RiskLimits()
        self.state = RiskState()

    def validate_signal(
        self,
        signal: SignalEvent,
        now: datetime,
        quote: Quote | None,
    ) -> tuple[bool, str | None]:
        if self.state.halted:
            return False, "risk_halted"
        if signal.is_expired(now):
            return False, "signal_expired"
        if quote is None or quote.last <= 0:
            return False, "invalid_market_data"

        ref_price = quote.ask if signal.side == Side.BUY else quote.bid
        if ref_price <= 0:
            return False, "invalid_quote"

        opening_slippage_bps = abs(ref_price - quote.mid) / quote.mid * 10_000 if quote.mid else 0.0
        if opening_slippage_bps > self.limits.max_open_slippage_bps:
            return False, "open_slippage_too_high"

        notional = signal.quantity * ref_price
        current_symbol_notional = abs(self.state.positions.get(signal.symbol, 0.0) * ref_price)
        if current_symbol_notional + notional > self.limits.max_position_notional:
            return False, "max_position_exceeded"

        strategy_notional = self.state.strategy_notional.get(signal.strategy_id, 0.0)
        if strategy_notional + notional > self.limits.max_strategy_notional:
            return False, "max_strategy_budget_exceeded"

        if self.state.realized_pnl <= -abs(self.limits.max_daily_drawdown):
            self.state.halted = True
            return False, "daily_drawdown_breached"

        return True, None

    def on_fill(self, fill: Fill, strategy_id: str) -> None:
        signed_qty = fill.quantity if fill.side == Side.BUY else -fill.quantity
        current_qty = self.state.positions.get(fill.symbol, 0.0)
        next_qty = current_qty + signed_qty
        prev_avg = self.state.avg_cost.get(fill.symbol, fill.price)

        if current_qty == 0 or (current_qty > 0) == (signed_qty > 0):
            gross_qty = abs(current_qty) + abs(signed_qty)
            weighted_cost = (abs(current_qty) * prev_avg) + (abs(signed_qty) * fill.price)
            self.state.avg_cost[fill.symbol] = weighted_cost / gross_qty if gross_qty else fill.price
        else:
            closing_qty = min(abs(current_qty), abs(signed_qty))
            if current_qty > 0:
                pnl = closing_qty * (fill.price - prev_avg)
            else:
                pnl = closing_qty * (prev_avg - fill.price)
            self.state.realized_pnl += pnl - fill.fees
            if next_qty == 0:
                self.state.avg_cost.pop(fill.symbol, None)
            else:
                self.state.avg_cost[fill.symbol] = fill.price

        self.state.positions[fill.symbol] = next_qty
        self.state.strategy_notional[strategy_id] = self.state.strategy_notional.get(strategy_id, 0.0) + (
            abs(fill.price * fill.quantity)
        )
        if self.state.realized_pnl <= -abs(self.limits.max_daily_drawdown):
            self.state.halted = True

