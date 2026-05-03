from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta

import polars as pl


@dataclass
class TushareClient:
    token: str | None = None

    def fetch_security_master(self, market: str = "HK") -> pl.DataFrame:
        rows = [
            {
                "security_id": "HK.00700",
                "symbol": "00700.HK",
                "ticker": "00700",
                "market": "HK",
                "exchange": "SEHK",
                "asset_type": "equity",
                "currency": "HKD",
                "lot_size": 100,
                "name": "Tencent",
                "sector": "Internet",
                "country": "HK",
                "primary_listing_flag": True,
                "southbound_eligible_flag": True,
                "northbound_proxy_flag": False,
                "listed_date": date(2004, 6, 16),
                "delisted_date": None,
                "active_flag": True,
                "effective_from": datetime(2026, 4, 1, tzinfo=UTC),
                "effective_to": None,
                "data_source": "tushare_stub",
                "source_symbol": "00700.HK",
                "source_dataset": "security_master",
                "source_run_id": "demo-security-master",
                "ingested_at": datetime(2026, 4, 1, 8, 0, tzinfo=UTC),
            },
            {
                "security_id": "US.AAPL",
                "symbol": "AAPL.US",
                "ticker": "AAPL",
                "market": "US",
                "exchange": "NASDAQ",
                "asset_type": "equity",
                "currency": "USD",
                "lot_size": 1,
                "name": "Apple",
                "sector": "Technology",
                "country": "US",
                "primary_listing_flag": True,
                "southbound_eligible_flag": False,
                "northbound_proxy_flag": False,
                "listed_date": date(1980, 12, 12),
                "delisted_date": None,
                "active_flag": True,
                "effective_from": datetime(2026, 4, 1, tzinfo=UTC),
                "effective_to": None,
                "data_source": "tushare_stub",
                "source_symbol": "AAPL.US",
                "source_dataset": "security_master",
                "source_run_id": "demo-security-master",
                "ingested_at": datetime(2026, 4, 1, 8, 0, tzinfo=UTC),
            },
            {
                "security_id": "CN.600519",
                "symbol": "600519.SH",
                "ticker": "600519",
                "market": "CN",
                "exchange": "SSE",
                "asset_type": "equity",
                "currency": "CNY",
                "lot_size": 100,
                "name": "Kweichow Moutai",
                "sector": "Consumer Staples",
                "country": "CN",
                "primary_listing_flag": True,
                "southbound_eligible_flag": False,
                "northbound_proxy_flag": True,
                "listed_date": date(2001, 8, 27),
                "delisted_date": None,
                "active_flag": True,
                "effective_from": datetime(2026, 4, 1, tzinfo=UTC),
                "effective_to": None,
                "data_source": "tushare_stub",
                "source_symbol": "600519.SH",
                "source_dataset": "security_master",
                "source_run_id": "demo-security-master",
                "ingested_at": datetime(2026, 4, 1, 8, 0, tzinfo=UTC),
            },
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
        market = symbol.split(".")[-1].replace("SH", "CN").replace("SZ", "CN")
        security_id = self._security_id_from_symbol(symbol)
        return pl.DataFrame(
            {
                "security_id": [security_id] * len(dates),
                "symbol": [symbol] * len(dates),
                "market": [market] * len(dates),
                "trade_date": dates,
                "open": [base + idx * 0.4 for idx, _ in enumerate(dates)],
                "high": [base + idx * 0.4 + 1.0 for idx, _ in enumerate(dates)],
                "low": [base + idx * 0.4 - 1.0 for idx, _ in enumerate(dates)],
                "close": [base + idx * 0.5 for idx, _ in enumerate(dates)],
                "adj_close": [base + idx * 0.49 for idx, _ in enumerate(dates)],
                "volume": [1_000_000 + idx * 10000 for idx, _ in enumerate(dates)],
                "turnover": [10_000_000 + idx * 100000 for idx, _ in enumerate(dates)],
                "turnover_rate": [0.012 + idx * 0.0003 for idx, _ in enumerate(dates)],
                "event_tag": ["earnings" if idx % 5 == 0 else None for idx, _ in enumerate(dates)],
                "data_source": ["tushare_stub"] * len(dates),
                "source_dataset": ["daily_bars"] * len(dates),
                "source_run_id": ["demo-daily-bars"] * len(dates),
                "ingested_at": [datetime(2026, 4, 1, 16, 30, tzinfo=UTC)] * len(dates),
            }
        )

    def fetch_minute_bars(self, symbol: str, trade_date: date, minutes: int = 60) -> pl.DataFrame:
        start_dt = datetime.combine(trade_date, datetime.min.time()).replace(hour=9, minute=30)
        rows = []
        base = 100.0 + (hash(symbol) % 20)
        security_id = self._security_id_from_symbol(symbol)
        market = symbol.split(".")[-1].replace("SH", "CN").replace("SZ", "CN")
        for idx in range(minutes):
            ts = start_dt + timedelta(minutes=idx)
            rows.append(
                {
                    "security_id": security_id,
                    "symbol": symbol,
                    "market": market,
                    "ts": ts,
                    "open": base + idx * 0.02,
                    "high": base + idx * 0.02 + 0.08,
                    "low": base + idx * 0.02 - 0.08,
                    "close": base + idx * 0.02 + (0.03 if idx % 2 == 0 else -0.01),
                    "volume": 1000 + idx * 5,
                    "turnover": 100000 + idx * 300,
                    "data_source": "tushare_stub",
                    "source_dataset": "minute_bars",
                    "source_run_id": "demo-minute-bars",
                    "ingested_at": datetime(2026, 4, 1, 16, 30, tzinfo=UTC),
                }
            )
        return pl.DataFrame(rows)

    @staticmethod
    def _security_id_from_symbol(symbol: str) -> str:
        ticker, suffix = symbol.split(".")
        market = {"HK": "HK", "US": "US", "SH": "CN", "SZ": "CN"}.get(suffix, suffix)
        return f"{market}.{ticker}"
