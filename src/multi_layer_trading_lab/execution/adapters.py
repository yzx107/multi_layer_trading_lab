from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC
from uuid import uuid4

from multi_layer_trading_lab.backtest.types import ExecutionMode, Fill, Order, Side
from multi_layer_trading_lab.execution.interfaces import BrokerAdapter, OrderResult, Quote


@dataclass(slots=True)
class CostModel:
    commission_bps: float = 5.0
    slippage_bps: float = 10.0
    stamp_duty_bps: float = 10.0

    def estimate_fees(self, notional: float, apply_stamp_duty: bool) -> float:
        fees = notional * self.commission_bps / 10_000.0
        if apply_stamp_duty:
            fees += notional * self.stamp_duty_bps / 10_000.0
        return fees

    def estimate_slippage(self, reference_price: float) -> float:
        return reference_price * self.slippage_bps / 10_000.0


class SimulatedBrokerAdapter(BrokerAdapter):
    """Simple simulator shared by dry-run and paper adapters."""

    def __init__(self, mode: ExecutionMode, cost_model: CostModel | None = None) -> None:
        super().__init__(mode=mode)
        self.cost_model = cost_model or CostModel()
        self._open_orders: dict[str, Order] = {}

    def submit_order(self, order: Order, quote: Quote | None = None) -> OrderResult:
        if quote is None:
            return OrderResult(
                accepted=False,
                status="rejected",
                order=order,
                reason="missing_quote",
            )

        ref_price = quote.ask if order.side == Side.BUY else quote.bid
        slip = self.cost_model.estimate_slippage(ref_price)
        fill_price = ref_price + slip if order.side == Side.BUY else max(ref_price - slip, 0.0)
        notional = fill_price * order.quantity
        fill = Fill(
            order_id=order.order_id,
            symbol=order.symbol,
            side=order.side,
            quantity=order.quantity,
            price=fill_price,
            timestamp=quote.timestamp.astimezone(UTC),
            fees=self.cost_model.estimate_fees(
                notional,
                apply_stamp_duty=order.symbol.endswith((".HK", "-HK")),
            ),
            slippage=abs(fill_price - ref_price),
            broker_fill_id=f"{self.mode.value}-{uuid4().hex[:12]}",
        )
        self._open_orders[order.order_id] = order
        return OrderResult(
            accepted=True,
            status="filled",
            order=order,
            fill=fill,
            broker_order_id=fill.broker_fill_id,
        )

    def cancel_order(self, order_id: str) -> bool:
        return self._open_orders.pop(order_id, None) is not None


class DryRunBrokerAdapter(SimulatedBrokerAdapter):
    def __init__(self, cost_model: CostModel | None = None) -> None:
        super().__init__(mode=ExecutionMode.DRY_RUN, cost_model=cost_model)


class PaperBrokerAdapter(SimulatedBrokerAdapter):
    def __init__(self, cost_model: CostModel | None = None) -> None:
        super().__init__(mode=ExecutionMode.PAPER, cost_model=cost_model)


class FutuBrokerAdapter(DryRunBrokerAdapter):
    """Placeholder adapter; live API integration remains a follow-up task."""


class IBKRBrokerAdapter(PaperBrokerAdapter):
    """Placeholder adapter; live API integration remains a follow-up task."""
