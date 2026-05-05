from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum

import polars as pl


class FutuMode(StrEnum):
    DRY_RUN = "dry_run"
    PAPER = "paper"
    LIVE = "live"


@dataclass(frozen=True, slots=True)
class FutuOpenDConfig:
    host: str = "127.0.0.1"
    port: int = 11111
    market: str = "HK"
    env: str = "SIMULATE"
    mode: FutuMode = FutuMode.DRY_RUN
    unlock_password_set: bool = False
    manual_live_enable: bool = False


@dataclass(frozen=True, slots=True)
class FutuReadiness:
    ready: bool
    failed_reasons: tuple[str, ...]


def check_opend_readiness(config: FutuOpenDConfig) -> FutuReadiness:
    failed: list[str] = []
    if not config.host:
        failed.append("missing_host")
    if config.port <= 0:
        failed.append("invalid_port")
    if config.market != "HK":
        failed.append("unsupported_market")
    if config.mode == FutuMode.PAPER and config.env != "SIMULATE":
        failed.append("paper_requires_simulate_env")
    if config.mode == FutuMode.LIVE:
        if config.env != "REAL":
            failed.append("live_requires_real_env")
        if not config.unlock_password_set:
            failed.append("live_requires_unlock_password")
        if not config.manual_live_enable:
            failed.append("manual_live_enable_missing")
    return FutuReadiness(ready=not failed, failed_reasons=tuple(failed))


@dataclass
class FutuMarketDataAdapter:
    mode: str = "dry_run"

    def subscribe_quotes(self, symbols: list[str]) -> pl.DataFrame:
        return pl.DataFrame(
            {
                "symbol": symbols,
                "bid": [100.0] * len(symbols),
                "ask": [100.2] * len(symbols),
                "mode": [self.mode] * len(symbols),
            }
        )


@dataclass
class FutuBrokerClient:
    mode: str = "dry_run"

    def place_order(
        self,
        symbol: str,
        side: str,
        qty: int,
        limit_price: float | None = None,
    ) -> dict:
        return {
            "broker": "futu",
            "mode": self.mode,
            "symbol": symbol,
            "side": side,
            "qty": qty,
            "limit_price": limit_price,
            "status": "accepted" if self.mode != "live" else "submitted",
        }
