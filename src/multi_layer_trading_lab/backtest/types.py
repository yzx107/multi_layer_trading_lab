from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any


class Side(StrEnum):
    BUY = "BUY"
    SELL = "SELL"


class OrderType(StrEnum):
    MARKET = "MARKET"
    LIMIT = "LIMIT"


class TimeInForce(StrEnum):
    DAY = "DAY"
    IOC = "IOC"


class ExecutionMode(StrEnum):
    DRY_RUN = "dry_run"
    PAPER = "paper"
    LIVE = "live"


@dataclass(slots=True)
class SignalEvent:
    signal_id: str
    timestamp: datetime
    symbol: str
    side: Side
    quantity: float
    order_type: OrderType = OrderType.MARKET
    limit_price: float | None = None
    strategy_id: str = "default"
    alpha_name: str = "demo"
    ttl_seconds: int = 300
    metadata: dict[str, Any] = field(default_factory=dict)

    def is_expired(self, now: datetime) -> bool:
        return (now - self.timestamp).total_seconds() > self.ttl_seconds


@dataclass(slots=True)
class Order:
    order_id: str
    created_at: datetime
    symbol: str
    side: Side
    quantity: float
    order_type: OrderType = OrderType.MARKET
    limit_price: float | None = None
    strategy_id: str = "default"
    signal_id: str | None = None
    tif: TimeInForce = TimeInForce.DAY
    mode: ExecutionMode = ExecutionMode.DRY_RUN
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class Fill:
    order_id: str
    symbol: str
    side: Side
    quantity: float
    price: float
    timestamp: datetime
    fees: float = 0.0
    slippage: float = 0.0
    broker_fill_id: str | None = None


@dataclass(slots=True)
class Position:
    symbol: str
    quantity: float = 0.0
    avg_price: float = 0.0


@dataclass(slots=True)
class ExecutionLogRecord:
    event_time: datetime
    order_id: str
    symbol: str
    side: str
    quantity: float
    mode: str
    status: str
    requested_price: float | None
    fill_price: float | None
    fees: float
    slippage: float
    strategy_id: str
    signal_id: str | None
    broker_order_id: str | None = None
    notes: str | None = None

    def as_dict(self) -> dict[str, Any]:
        return {
            "event_time": self.event_time.astimezone(UTC).isoformat(),
            "order_id": self.order_id,
            "symbol": self.symbol,
            "side": self.side,
            "quantity": self.quantity,
            "mode": self.mode,
            "status": self.status,
            "requested_price": self.requested_price,
            "fill_price": self.fill_price,
            "fees": self.fees,
            "slippage": self.slippage,
            "strategy_id": self.strategy_id,
            "signal_id": self.signal_id,
            "broker_order_id": self.broker_order_id,
            "notes": self.notes,
        }
