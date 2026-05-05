from datetime import datetime

import polars as pl

from multi_layer_trading_lab.features.l2.order_add import build_order_add_bucket_features


def test_build_order_add_bucket_features_aggregates_order_events() -> None:
    raw = pl.DataFrame(
        {
            "symbol": ["00001.HK", "00001.HK", "00001.HK"],
            "ts": [
                datetime(2025, 1, 23, 9, 20, 6),
                datetime(2025, 1, 23, 9, 20, 30),
                datetime(2025, 1, 23, 9, 21, 1),
            ],
            "trade_date": ["20250123", "20250123", "20250123"],
            "seq_num": [0, 1, 2],
            "order_id": [1, 2, 3],
            "order_type": [1, 1, 1],
            "ext": [110, 110, 110],
            "price": [78.9, 79.0, 79.2],
            "volume": [9_600, 20_000, 4_000],
            "level": [0, 1, 2],
            "broker_no": [None, None, None],
            "volume_pre": [0, 0, 0],
        }
    )

    features = build_order_add_bucket_features(raw, bucket_size="1m", large_order_volume=10_000)

    assert features.height == 2
    first = features.row(0, named=True)
    assert first["security_id"] == "HK.00001"
    assert first["order_add_count"] == 2
    assert first["order_add_volume"] == 29_600
    assert first["large_order_ratio"] == 0.5
    assert first["preopen_stat_flag"] is True
