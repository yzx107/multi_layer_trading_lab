from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from multi_layer_trading_lab.backtest.types import ExecutionMode, Fill, Order


@dataclass(slots=True)
class Quote:
    symbol: str
    timestamp: datetime
    bid: float
    ask: float
    last: float
    bid_size: float = 0.0
    ask_size: float = 0.0
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def mid(self) -> float:
        if self.bid and self.ask:
            return (self.bid + self.ask) / 2.0
        return self.last


@dataclass(slots=True)
class OrderResult:
    accepted: bool
    status: str
    order: Order
    fill: Fill | None = None
    broker_order_id: str | None = None
    reason: str | None = None


class MarketDataAdapter(ABC):
    @abstractmethod
    def get_quote(self, symbol: str, timestamp: datetime | None = None) -> Quote | None:
        raise NotImplementedError


class BrokerAdapter(ABC):
    def __init__(self, mode: ExecutionMode) -> None:
        self.mode = mode

    @abstractmethod
    def submit_order(self, order: Order, quote: Quote | None = None) -> OrderResult:
        raise NotImplementedError

    @abstractmethod
    def cancel_order(self, order_id: str) -> bool:
        raise NotImplementedError

