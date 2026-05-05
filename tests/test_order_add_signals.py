from datetime import datetime

import polars as pl

from multi_layer_trading_lab.signals.order_add import (
    build_order_add_signal_candidates,
    order_add_candidates_to_signal_events,
)


def test_order_add_signal_candidates_filter_pressure_buckets() -> None:
    features = pl.DataFrame(
        {
            "security_id": ["HK.00001", "HK.00001"],
            "symbol": ["00001.HK", "00001.HK"],
            "market": ["HK", "HK"],
            "trade_date": [datetime(2025, 1, 23).date(), datetime(2025, 1, 23).date()],
            "bar_start_ts": [
                datetime(2025, 1, 23, 9, 20),
                datetime(2025, 1, 23, 9, 21),
            ],
            "order_add_count": [10, 2],
            "order_add_volume": [50_000, 5_000],
            "large_order_ratio": [0.4, 0.0],
        }
    )

    candidates = build_order_add_signal_candidates(features)

    assert candidates.height == 1
    assert candidates["strategy_id"].to_list() == ["order_add_pressure"]
    assert candidates["score"].to_list()[0] == 1.0


def test_order_add_candidates_convert_to_signal_events() -> None:
    candidates = build_order_add_signal_candidates(
        pl.DataFrame(
            {
                "security_id": ["HK.00001"],
                "symbol": ["00001.HK"],
                "market": ["HK"],
                "trade_date": [datetime(2025, 1, 23).date()],
                "bar_start_ts": [datetime(2025, 1, 23, 9, 20)],
                "order_add_count": [10],
                "order_add_volume": [50_000],
                "large_order_ratio": [0.4],
            }
        )
    )

    signals = order_add_candidates_to_signal_events(candidates)

    assert signals.height == 1
    assert signals["strategy_id"].to_list() == ["order_add_pressure"]
    assert signals["data_source"].to_list() == ["l2_order_add_features"]
    assert signals["signal_id"].to_list()[0].endswith("-order_add_pressure")
