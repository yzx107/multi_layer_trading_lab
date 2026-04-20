from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import uuid4

from multi_layer_trading_lab.backtest.types import ExecutionLogRecord, Order, SignalEvent
from multi_layer_trading_lab.execution.interfaces import BrokerAdapter, OrderResult, Quote
from multi_layer_trading_lab.execution.logging import ExecutionLogWriter
from multi_layer_trading_lab.risk.manager import RiskManager


@dataclass(slots=True)
class ManagedOrderResult:
    order_result: OrderResult
    risk_reason: str | None = None


class OrderManager:
    def __init__(
        self,
        broker: BrokerAdapter,
        risk_manager: RiskManager,
        execution_log_writer: ExecutionLogWriter,
    ) -> None:
        self.broker = broker
        self.risk_manager = risk_manager
        self.execution_log_writer = execution_log_writer

    def create_order_from_signal(self, signal: SignalEvent, now: datetime) -> Order:
        return Order(
            order_id=f"ord-{uuid4().hex[:12]}",
            created_at=now.astimezone(timezone.utc),
            symbol=signal.symbol,
            side=signal.side,
            quantity=signal.quantity,
            order_type=signal.order_type,
            limit_price=signal.limit_price,
            strategy_id=signal.strategy_id,
            signal_id=signal.signal_id,
            mode=self.broker.mode,
            metadata=dict(signal.metadata),
        )

    def submit_signal(self, signal: SignalEvent, now: datetime, quote: Quote | None) -> ManagedOrderResult:
        approved, reason = self.risk_manager.validate_signal(signal=signal, now=now, quote=quote)
        order = self.create_order_from_signal(signal, now=now)
        if not approved:
            self.execution_log_writer.append(
                ExecutionLogRecord(
                    event_time=now,
                    order_id=order.order_id,
                    symbol=order.symbol,
                    side=order.side.value,
                    quantity=order.quantity,
                    mode=self.broker.mode.value,
                    status="rejected",
                    requested_price=order.limit_price,
                    fill_price=None,
                    fees=0.0,
                    slippage=0.0,
                    strategy_id=order.strategy_id,
                    signal_id=order.signal_id,
                    notes=reason,
                )
            )
            return ManagedOrderResult(
                order_result=OrderResult(accepted=False, status="rejected", order=order, reason=reason),
                risk_reason=reason,
            )

        result = self.broker.submit_order(order=order, quote=quote)
        fill = result.fill
        self.execution_log_writer.append(
            ExecutionLogRecord(
                event_time=fill.timestamp if fill else now,
                order_id=order.order_id,
                symbol=order.symbol,
                side=order.side.value,
                quantity=order.quantity,
                mode=self.broker.mode.value,
                status=result.status,
                requested_price=order.limit_price,
                fill_price=fill.price if fill else None,
                fees=fill.fees if fill else 0.0,
                slippage=fill.slippage if fill else 0.0,
                strategy_id=order.strategy_id,
                signal_id=order.signal_id,
                broker_order_id=result.broker_order_id,
                notes=result.reason,
            )
        )
        if fill:
            self.risk_manager.on_fill(fill=fill, strategy_id=order.strategy_id)
        return ManagedOrderResult(order_result=result)

