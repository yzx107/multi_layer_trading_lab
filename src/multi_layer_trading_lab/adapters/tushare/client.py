from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta

import polars as pl


@dataclass
class TushareClient:
    token: str | None = None

    def fetch_security_master(self, market: str = "HK") -> pl.DataFrame:
        rows = [
            {"symbol": "00700.HK", "market": "HK", "name": "Tencent", "currency": "HKD", "lot_size": 100},
            {"symbol": "AAPL.US", "market": "US", "name": "Apple", "currency": "USD", "lot_size": 1},
            {"symbol": "600519.SH", "market": "CN", "name": "Kweichow Moutai", "currency": "CNY", "lot_size": 100},
        ]
        return pl.DataFrame(rows).filter(pl.col("market") == market if market else pl.lit(True))

    def fetch_daily_bars(self, symbol: str, start: date, end: date) -> pl.DataFrame:
        dates: list[date] = []
        current = start
        while current <= end:
            if current.weekday() < 5:
                dates.append(current)
            current += timedelta(days=1)
        base = 100.0 + (hash(symbol) % 20)
        return pl.DataFrame(
            {
                "symbol": [symbol] * len(dates),
                "trade_date": dates,
                "open": [base + idx * 0.4 for idx, _ in enumerate(dates)],
                "high": [base + idx * 0.4 + 1.0 for idx, _ in enumerate(dates)],
                "low": [base + idx * 0.4 - 1.0 for idx, _ in enumerate(dates)],
                "close": [base + idx * 0.5 for idx, _ in enumerate(dates)],
                "volume": [1_000_000 + idx * 10000 for idx, _ in enumerate(dates)],
                "turnover": [10_000_000 + idx * 100000 for idx, _ in enumerate(dates)],
                "source": ["tushare_stub"] * len(dates),
            }
        )

    def fetch_minute_bars(self, symbol: str, trade_date: date, minutes: int = 60) -> pl.DataFrame:
        start_dt = datetime.combine(trade_date, datetime.min.time()).replace(hour=9, minute=30)
        rows = []
        base = 100.0 + (hash(symbol) % 20)
        for idx in range(minutes):
            ts = start_dt + timedelta(minutes=idx)
            rows.append(
                {
                    "symbol": symbol,
                    "ts": ts,
                    "open": base + idx * 0.02,
                    "high": base + idx * 0.02 + 0.08,
                    "low": base + idx * 0.02 - 0.08,
                    "close": base + idx * 0.02 + (0.03 if idx % 2 == 0 else -0.01),
                    "volume": 1000 + idx * 5,
                    "turnover": 100000 + idx * 300,
                    "source": "tushare_stub",
                }
            )
        return pl.DataFrame(rows)
