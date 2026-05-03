from datetime import UTC, datetime, timedelta

import polars as pl

from multi_layer_trading_lab.labels import (
    add_horizon_labels,
    extract_event_outcomes,
    normalize_symbol,
)
from multi_layer_trading_lab.models import attach_setup_posteriors, summarize_setup_posteriors


def test_symbol_normalization_handles_hk_numeric():
    assert normalize_symbol("700", default_market="HK") == "00700.HK"


def test_horizon_labels_and_bayes_summary():
    frame = pl.DataFrame(
        {
            "symbol": ["00700.HK"] * 5,
            "trade_date": [
                datetime(2026, 4, 1, tzinfo=UTC) + timedelta(days=idx) for idx in range(5)
            ],
            "close": [100.0, 102.0, 101.0, 103.0, 105.0],
            "setup_id": ["open_strength"] * 5,
        }
    )
    labeled = add_horizon_labels(frame, price_col="close", horizons=[1], group_cols=("symbol",))
    posterior = attach_setup_posteriors(
        labeled,
        label_col="label_up_1b",
        group_cols=("setup_id",),
    )
    summary = summarize_setup_posteriors(
        posterior,
        label_col="label_up_1b",
        group_cols=("setup_id",),
    )
    assert "posterior_mean" in posterior.columns
    assert summary.height == 1


def test_extract_event_outcomes_produces_labels():
    frame = pl.DataFrame(
        {
            "symbol": ["00700.HK"] * 5,
            "ts": [datetime(2026, 4, 1, tzinfo=UTC) + timedelta(minutes=idx) for idx in range(5)],
            "close": [100.0, 103.0, 102.0, 104.0, 105.0],
            "setup_event": [True, False, False, True, False],
        }
    )
    outcomes = extract_event_outcomes(
        frame,
        event_col="setup_event",
        price_col="close",
        horizon=2,
        upper_barrier=0.02,
        lower_barrier=-0.02,
        group_cols=("symbol",),
    )
    assert "event_outcome" in outcomes.columns
    assert outcomes.height >= 1
