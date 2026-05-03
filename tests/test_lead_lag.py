import polars as pl

from multi_layer_trading_lab.models import batch_scan_lead_lag, estimate_transfer_entropy


def test_transfer_entropy_is_non_negative():
    frame = pl.DataFrame(
        {
            "leader": [0.0, 0.1, 0.2, 0.3, 0.4, 0.5],
            "follower": [0.0, 0.05, 0.15, 0.28, 0.35, 0.52],
        }
    )
    score = estimate_transfer_entropy(frame, "leader", "follower", bins=3, lag=1)
    assert score >= 0


def test_batch_scan_lead_lag_returns_ranked_pairs():
    frame = pl.DataFrame(
        {
            "a": [1, 2, 3, 4, 5, 6],
            "b": [2, 3, 4, 5, 6, 7],
            "c": [6, 5, 4, 3, 2, 1],
        }
    )
    result = batch_scan_lead_lag(frame, column_pairs=[("a", "b"), ("c", "b")], max_lag=2)
    assert result.height == 2
    assert result.columns == [
        "leader_col",
        "follower_col",
        "best_lag",
        "corr_score",
        "transfer_entropy",
    ]
