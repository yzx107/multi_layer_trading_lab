from __future__ import annotations

from dataclasses import dataclass


@dataclass
class IBKRBrokerClient:
    mode: str = "dry_run"

    def place_order(
        self,
        symbol: str,
        side: str,
        qty: int,
        limit_price: float | None = None,
    ) -> dict:
        return {
            "broker": "ibkr",
            "mode": self.mode,
            "symbol": symbol,
            "side": side,
            "qty": qty,
            "limit_price": limit_price,
            "status": "accepted" if self.mode != "live" else "submitted",
        }
