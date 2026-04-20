from __future__ import annotations

from dataclasses import dataclass

import polars as pl


@dataclass
class FutuMarketDataAdapter:
    mode: str = "dry_run"

    def subscribe_quotes(self, symbols: list[str]) -> pl.DataFrame:
        return pl.DataFrame({"symbol": symbols, "bid": [100.0] * len(symbols), "ask": [100.2] * len(symbols), "mode": [self.mode] * len(symbols)})


@dataclass
class FutuBrokerClient:
    mode: str = "dry_run"

    def place_order(self, symbol: str, side: str, qty: int, limit_price: float | None = None) -> dict:
        return {
            "broker": "futu",
            "mode": self.mode,
            "symbol": symbol,
            "side": side,
            "qty": qty,
            "limit_price": limit_price,
            "status": "accepted" if self.mode != "live" else "submitted",
        }
