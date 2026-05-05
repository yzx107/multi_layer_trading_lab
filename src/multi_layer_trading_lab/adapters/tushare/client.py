from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from typing import Any

import polars as pl


@dataclass
class TushareClient:
    token: str | None = None
    use_real: bool = False
    pro_factory: Callable[[], Any] | None = None

    def _get_pro(self) -> Any:
        if self.pro_factory is not None:
            return self.pro_factory()
        if not self.token:
            raise ValueError("TUSHARE_TOKEN is required for real Tushare access")
        try:
            import tushare as ts
        except ImportError as exc:
            raise RuntimeError(
                "tushare SDK is not installed; install the project extras or `pip install tushare`"
            ) from exc
        ts.set_token(self.token)
        return ts.pro_api()

    def fetch_security_master(self, market: str = "HK") -> pl.DataFrame:
        if self.use_real:
            return self._fetch_real_security_master(market)
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
        if self.use_real:
            return self._fetch_real_daily_bars(symbol, start, end)
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

    def fetch_trade_calendar(self, start: date, end: date, exchange: str = "SSE") -> pl.DataFrame:
        if self.use_real:
            return self._fetch_real_trade_calendar(start, end, exchange=exchange)
        dates: list[date] = []
        current = start
        while current <= end:
            dates.append(current)
            current += timedelta(days=1)
        return pl.DataFrame(
            {
                "exchange": [exchange] * len(dates),
                "trade_date": dates,
                "is_open": [item.weekday() < 5 for item in dates],
                "pretrade_date": [
                    self._previous_weekday(item) if item.weekday() < 5 else None
                    for item in dates
                ],
                "data_source": ["tushare_stub"] * len(dates),
                "source_dataset": ["trade_cal"] * len(dates),
                "source_run_id": ["demo-trade-calendar"] * len(dates),
                "ingested_at": [datetime(2026, 4, 1, 16, 30, tzinfo=UTC)] * len(dates),
            }
        )

    def fetch_minute_bars(self, symbol: str, trade_date: date, minutes: int = 60) -> pl.DataFrame:
        if self.use_real:
            return self._empty_minute_bars(symbol, trade_date)
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

    def _fetch_real_security_master(self, market: str = "CN") -> pl.DataFrame:
        if market not in {"", "CN", "A", "ASHARE"}:
            raise NotImplementedError(
                f"real Tushare security master unsupported for market={market}"
            )
        ingested_at = datetime.now(UTC)
        raw = self._get_pro().stock_basic(
            exchange="",
            list_status="L",
            fields="ts_code,symbol,name,area,industry,list_date,exchange",
        )
        frame = pl.from_pandas(raw)
        if frame.is_empty():
            return self._empty_security_master()
        return frame.with_columns(
            [
                pl.col("ts_code").alias("source_symbol"),
                pl.col("ts_code").alias("symbol"),
                pl.col("symbol").alias("ticker"),
                pl.col("ts_code")
                .map_elements(self._security_id_from_symbol, return_dtype=pl.Utf8)
                .alias("security_id"),
                pl.lit("CN").alias("market"),
                pl.col("exchange").fill_null("").alias("exchange"),
                pl.lit("equity").alias("asset_type"),
                pl.lit("CNY").alias("currency"),
                pl.lit(100).alias("lot_size"),
                pl.col("industry").alias("sector"),
                pl.lit("CN").alias("country"),
                pl.lit(True).alias("primary_listing_flag"),
                pl.lit(False).alias("southbound_eligible_flag"),
                pl.lit(True).alias("northbound_proxy_flag"),
                pl.col("list_date")
                .str.strptime(pl.Date, "%Y%m%d", strict=False)
                .alias("listed_date"),
                pl.lit(None, dtype=pl.Date).alias("delisted_date"),
                pl.lit(True).alias("active_flag"),
                pl.lit(ingested_at).alias("effective_from"),
                pl.lit(None, dtype=pl.Datetime(time_zone="UTC")).alias("effective_to"),
                pl.lit("tushare_pro").alias("data_source"),
                pl.lit("stock_basic").alias("source_dataset"),
                pl.lit(f"tushare-stock-basic-{ingested_at:%Y%m%dT%H%M%S}").alias(
                    "source_run_id"
                ),
                pl.lit(ingested_at).alias("ingested_at"),
            ]
        ).select(
            [
                "security_id",
                "symbol",
                "ticker",
                "market",
                "exchange",
                "asset_type",
                "currency",
                "lot_size",
                "name",
                "sector",
                "country",
                "primary_listing_flag",
                "southbound_eligible_flag",
                "northbound_proxy_flag",
                "listed_date",
                "delisted_date",
                "active_flag",
                "effective_from",
                "effective_to",
                "data_source",
                "source_symbol",
                "source_dataset",
                "source_run_id",
                "ingested_at",
            ]
        )

    def _fetch_real_daily_bars(self, symbol: str, start: date, end: date) -> pl.DataFrame:
        suffix = symbol.split(".")[-1]
        if suffix not in {"SH", "SZ"}:
            raise NotImplementedError(f"real Tushare daily bars unsupported for symbol={symbol}")
        ingested_at = datetime.now(UTC)
        raw = self._get_pro().daily(
            ts_code=symbol,
            start_date=start.strftime("%Y%m%d"),
            end_date=end.strftime("%Y%m%d"),
        )
        frame = pl.from_pandas(raw)
        if frame.is_empty():
            return self._empty_daily_bars()
        return frame.with_columns(
            [
                pl.col("ts_code")
                .map_elements(self._security_id_from_symbol, return_dtype=pl.Utf8)
                .alias("security_id"),
                pl.col("ts_code").alias("symbol"),
                pl.lit("CN").alias("market"),
                pl.col("trade_date").str.strptime(pl.Date, "%Y%m%d", strict=False),
                pl.col("vol").cast(pl.Float64, strict=False).fill_null(0.0).alias("volume"),
                (
                    pl.col("amount").cast(pl.Float64, strict=False).fill_null(0.0) * 1000.0
                ).alias("turnover"),
                pl.lit(None, dtype=pl.Float64).alias("turnover_rate"),
                pl.lit(None, dtype=pl.Utf8).alias("event_tag"),
                pl.lit("tushare_pro").alias("data_source"),
                pl.lit("daily").alias("source_dataset"),
                pl.lit(f"tushare-daily-{ingested_at:%Y%m%dT%H%M%S}").alias("source_run_id"),
                pl.lit(ingested_at).alias("ingested_at"),
            ]
        ).select(
            [
                "security_id",
                "symbol",
                "market",
                "trade_date",
                "open",
                "high",
                "low",
                "close",
                pl.col("close").alias("adj_close"),
                "volume",
                "turnover",
                "turnover_rate",
                "event_tag",
                "data_source",
                "source_dataset",
                "source_run_id",
                "ingested_at",
            ]
        )

    def _fetch_real_trade_calendar(
        self,
        start: date,
        end: date,
        *,
        exchange: str = "SSE",
    ) -> pl.DataFrame:
        ingested_at = datetime.now(UTC)
        raw = self._get_pro().trade_cal(
            exchange=exchange,
            start_date=start.strftime("%Y%m%d"),
            end_date=end.strftime("%Y%m%d"),
        )
        frame = pl.from_pandas(raw)
        if frame.is_empty():
            return self._empty_trade_calendar()
        return frame.with_columns(
            [
                pl.col("exchange").fill_null(exchange).alias("exchange"),
                pl.col("cal_date").str.strptime(pl.Date, "%Y%m%d", strict=False).alias(
                    "trade_date"
                ),
                (pl.col("is_open").cast(pl.Int64, strict=False).fill_null(0) == 1).alias(
                    "is_open"
                ),
                pl.col("pretrade_date")
                .str.strptime(pl.Date, "%Y%m%d", strict=False)
                .alias("pretrade_date"),
                pl.lit("tushare_pro").alias("data_source"),
                pl.lit("trade_cal").alias("source_dataset"),
                pl.lit(f"tushare-trade-cal-{ingested_at:%Y%m%dT%H%M%S}").alias(
                    "source_run_id"
                ),
                pl.lit(ingested_at).alias("ingested_at"),
            ]
        ).select(
            [
                "exchange",
                "trade_date",
                "is_open",
                "pretrade_date",
                "data_source",
                "source_dataset",
                "source_run_id",
                "ingested_at",
            ]
        )

    def _empty_security_master(self) -> pl.DataFrame:
        return pl.DataFrame(
            schema={
                "security_id": pl.Utf8,
                "symbol": pl.Utf8,
                "ticker": pl.Utf8,
                "market": pl.Utf8,
                "exchange": pl.Utf8,
                "asset_type": pl.Utf8,
                "currency": pl.Utf8,
                "lot_size": pl.Int64,
                "name": pl.Utf8,
                "sector": pl.Utf8,
                "country": pl.Utf8,
                "primary_listing_flag": pl.Boolean,
                "southbound_eligible_flag": pl.Boolean,
                "northbound_proxy_flag": pl.Boolean,
                "listed_date": pl.Date,
                "delisted_date": pl.Date,
                "active_flag": pl.Boolean,
                "effective_from": pl.Datetime(time_zone="UTC"),
                "effective_to": pl.Datetime(time_zone="UTC"),
                "data_source": pl.Utf8,
                "source_symbol": pl.Utf8,
                "source_dataset": pl.Utf8,
                "source_run_id": pl.Utf8,
                "ingested_at": pl.Datetime(time_zone="UTC"),
            }
        )

    def _empty_daily_bars(self) -> pl.DataFrame:
        return pl.DataFrame(
            schema={
                "security_id": pl.Utf8,
                "symbol": pl.Utf8,
                "market": pl.Utf8,
                "trade_date": pl.Date,
                "open": pl.Float64,
                "high": pl.Float64,
                "low": pl.Float64,
                "close": pl.Float64,
                "adj_close": pl.Float64,
                "volume": pl.Float64,
                "turnover": pl.Float64,
                "turnover_rate": pl.Float64,
                "event_tag": pl.Utf8,
                "data_source": pl.Utf8,
                "source_dataset": pl.Utf8,
                "source_run_id": pl.Utf8,
                "ingested_at": pl.Datetime(time_zone="UTC"),
            }
        )

    def _empty_minute_bars(self, symbol: str, trade_date: date) -> pl.DataFrame:
        del symbol, trade_date
        return pl.DataFrame(
            schema={
                "security_id": pl.Utf8,
                "symbol": pl.Utf8,
                "market": pl.Utf8,
                "ts": pl.Datetime,
                "open": pl.Float64,
                "high": pl.Float64,
                "low": pl.Float64,
                "close": pl.Float64,
                "volume": pl.Float64,
                "turnover": pl.Float64,
                "data_source": pl.Utf8,
                "source_dataset": pl.Utf8,
                "source_run_id": pl.Utf8,
                "ingested_at": pl.Datetime(time_zone="UTC"),
            }
        )

    def _empty_trade_calendar(self) -> pl.DataFrame:
        return pl.DataFrame(
            schema={
                "exchange": pl.Utf8,
                "trade_date": pl.Date,
                "is_open": pl.Boolean,
                "pretrade_date": pl.Date,
                "data_source": pl.Utf8,
                "source_dataset": pl.Utf8,
                "source_run_id": pl.Utf8,
                "ingested_at": pl.Datetime(time_zone="UTC"),
            }
        )

    @staticmethod
    def _security_id_from_symbol(symbol: str) -> str:
        ticker, suffix = symbol.split(".")
        market = {"HK": "HK", "US": "US", "SH": "CN", "SZ": "CN"}.get(suffix, suffix)
        return f"{market}.{ticker}"

    @staticmethod
    def _previous_weekday(value: date) -> date:
        current = value - timedelta(days=1)
        while current.weekday() >= 5:
            current -= timedelta(days=1)
        return current
