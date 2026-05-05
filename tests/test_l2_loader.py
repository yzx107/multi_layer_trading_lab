from __future__ import annotations

from datetime import datetime
from pathlib import Path

import polars as pl
import pytest

from multi_layer_trading_lab.adapters.l2_loader.loader import L2ColumnMapping, L2Loader


def test_l2_loader_normalizes_raw_vendor_columns() -> None:
    raw = pl.DataFrame(
        {
            "code": ["00700.HK", "00700.HK"],
            "event_time": [datetime(2026, 4, 1, 9, 30), datetime(2026, 4, 1, 9, 30, 30)],
            "bid1": [320.0, 320.1],
            "ask1": [320.2, 320.3],
            "bidvol1": [1000, 1100],
            "askvol1": [900, 1000],
            "price": [320.1, 320.2],
            "volume": [500, 600],
            "bs_flag": ["buy", "sell"],
            "is_cancel": [0, 1],
        }
    )
    mapping = L2ColumnMapping(
        symbol="code",
        ts="event_time",
        bid_px_1="bid1",
        ask_px_1="ask1",
        bid_sz_1="bidvol1",
        ask_sz_1="askvol1",
        last_px="price",
        last_sz="volume",
        side="bs_flag",
        cancel_flag="is_cancel",
    )

    normalized = L2Loader(Path("unused")).normalize_raw_frame(raw, mapping=mapping)
    aggregated = L2Loader(Path("unused")).aggregate(normalized, bucket="1m")

    assert normalized.columns == list(
        [
            "symbol",
            "ts",
            "bid_px_1",
            "ask_px_1",
            "bid_sz_1",
            "ask_sz_1",
            "last_px",
            "last_sz",
            "side",
            "cancel_flag",
        ]
    )
    assert normalized["side"].to_list() == ["BUY", "SELL"]
    assert aggregated.height == 1
    assert "bid_ask_imbalance" in aggregated.columns


def test_l2_loader_reports_missing_vendor_columns() -> None:
    raw = pl.DataFrame({"code": ["00700.HK"]})
    mapping = L2ColumnMapping(symbol="code", ts="event_time")

    with pytest.raises(ValueError, match="missing L2 columns"):
        L2Loader(Path("unused")).normalize_raw_frame(raw, mapping=mapping)
