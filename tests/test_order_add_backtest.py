from datetime import UTC, datetime

import polars as pl

from multi_layer_trading_lab.backtest.order_add import (
    backtest_order_add_candidates,
    sweep_order_add_thresholds,
)


def test_backtest_order_add_candidates_uses_forward_bucket_return() -> None:
    features = pl.DataFrame(
        {
            "symbol": ["00001.HK", "00001.HK", "00001.HK"],
            "bar_start_ts": [
                datetime(2025, 1, 23, 9, 20),
                datetime(2025, 1, 23, 9, 21),
                datetime(2025, 1, 23, 9, 22),
            ],
            "order_add_price_mean": [100.0, 101.0, 102.0],
        }
    )
    candidates = pl.DataFrame(
        {
            "security_id": ["HK.00001"],
            "symbol": ["00001.HK"],
            "event_ts": [datetime(2025, 1, 23, 9, 20, tzinfo=UTC)],
            "score": [1.0],
        }
    )

    result = backtest_order_add_candidates(candidates, features, horizon_buckets=1, cost_bps=10)

    trade = result.trades.row(0, named=True)
    summary = result.summary.row(0, named=True)
    assert round(trade["gross_ret"], 6) == 0.01
    assert round(trade["net_ret"], 6) == 0.009
    assert summary["trade_count"] == 1
    assert summary["win_rate"] == 1.0


def test_backtest_order_add_candidates_returns_zero_summary_for_no_trades() -> None:
    result = backtest_order_add_candidates(pl.DataFrame(), pl.DataFrame())

    summary = result.summary.row(0, named=True)
    assert summary["trade_count"] == 0
    assert summary["total_net_ret"] == 0.0


def test_sweep_order_add_thresholds_ranks_by_net_return() -> None:
    features = pl.DataFrame(
        {
            "security_id": ["HK.00001", "HK.00001", "HK.00001"],
            "symbol": ["00001.HK", "00001.HK", "00001.HK"],
            "market": ["HK", "HK", "HK"],
            "trade_date": [
                datetime(2025, 1, 23).date(),
                datetime(2025, 1, 23).date(),
                datetime(2025, 1, 23).date(),
            ],
            "bar_start_ts": [
                datetime(2025, 1, 23, 9, 20),
                datetime(2025, 1, 23, 9, 21),
                datetime(2025, 1, 23, 9, 22),
            ],
            "order_add_price_mean": [100.0, 101.0, 99.0],
            "order_add_count": [10, 10, 10],
            "order_add_volume": [50_000, 20_000, 10_000],
            "large_order_ratio": [0.5, 0.1, 0.0],
        }
    )

    sweep = sweep_order_add_thresholds(
        features,
        volume_thresholds=[10_000, 50_000],
        large_order_ratio_thresholds=[0.0, 0.5],
        cost_bps=10,
        planned_notional=8_000,
    )

    best = sweep.row(0, named=True)
    assert sweep.height == 4
    assert best["min_large_order_ratio"] == 0.5
    assert best["trade_count"] == 1
    assert best["planned_notional"] == 8_000
    assert best["avg_net_ret"] > 0
