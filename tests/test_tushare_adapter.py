from __future__ import annotations

from datetime import date

import pandas as pd

from multi_layer_trading_lab.adapters.tushare.client import TushareClient


class FakeTusharePro:
    def stock_basic(self, exchange: str, list_status: str, fields: str) -> pd.DataFrame:
        del exchange, list_status, fields
        return pd.DataFrame(
            [
                {
                    "ts_code": "600519.SH",
                    "symbol": "600519",
                    "name": "Kweichow Moutai",
                    "area": "Guizhou",
                    "industry": "Beverage",
                    "list_date": "20010827",
                    "exchange": "SSE",
                }
            ]
        )

    def daily(self, ts_code: str, start_date: str, end_date: str) -> pd.DataFrame:
        del start_date, end_date
        return pd.DataFrame(
            [
                {
                    "ts_code": ts_code,
                    "trade_date": "20260401",
                    "open": 100.0,
                    "high": 101.0,
                    "low": 99.0,
                    "close": 100.5,
                    "vol": 1200.0,
                    "amount": 120000.0,
                }
            ]
        )

    def trade_cal(self, exchange: str, start_date: str, end_date: str) -> pd.DataFrame:
        del start_date, end_date
        return pd.DataFrame(
            [
                {
                    "exchange": exchange,
                    "cal_date": "20260401",
                    "is_open": 1,
                    "pretrade_date": "20260331",
                },
                {
                    "exchange": exchange,
                    "cal_date": "20260402",
                    "is_open": 0,
                    "pretrade_date": "20260401",
                },
            ]
        )


def test_real_tushare_security_master_maps_stock_basic() -> None:
    client = TushareClient(use_real=True, pro_factory=FakeTusharePro)

    frame = client.fetch_security_master("CN")

    assert frame.height == 1
    assert frame["security_id"][0] == "CN.600519"
    assert frame["symbol"][0] == "600519.SH"
    assert frame["data_source"][0] == "tushare_pro"
    assert frame["source_dataset"][0] == "stock_basic"


def test_real_tushare_daily_bars_maps_daily() -> None:
    client = TushareClient(use_real=True, pro_factory=FakeTusharePro)

    frame = client.fetch_daily_bars("600519.SH", date(2026, 4, 1), date(2026, 4, 2))

    assert frame.height == 1
    assert frame["security_id"][0] == "CN.600519"
    assert frame["trade_date"][0] == date(2026, 4, 1)
    assert frame["turnover"][0] == 120000000.0
    assert frame["data_source"][0] == "tushare_pro"


def test_real_tushare_trade_calendar_maps_trade_cal() -> None:
    client = TushareClient(use_real=True, pro_factory=FakeTusharePro)

    frame = client.fetch_trade_calendar(date(2026, 4, 1), date(2026, 4, 2))

    assert frame.height == 2
    assert frame["trade_date"][0] == date(2026, 4, 1)
    assert frame["is_open"].to_list() == [True, False]
    assert frame["pretrade_date"][0] == date(2026, 3, 31)
    assert frame["data_source"][0] == "tushare_pro"
    assert frame["source_dataset"][0] == "trade_cal"
