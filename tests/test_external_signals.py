import json
from pathlib import Path

import polars as pl

from multi_layer_trading_lab.research.external_signals import (
    build_external_research_signal_events,
)


def test_build_external_research_signal_events_from_portfolio_and_hshare_dates(tmp_path: Path):
    summary_path = tmp_path / "summary.json"
    summary_path.write_text(
        json.dumps({"tables": {"verified_orders": {"dates": ["2025-01-02", "2025-01-03"]}}}),
        encoding="utf-8",
    )
    portfolio = pl.DataFrame(
        {
            "factor_name": ["factor_a", "factor_b"],
            "direction_hint": ["as_is_candidate", "inverse_candidate"],
            "candidate_status": ["review_candidate", "blocked"],
            "target_notional": [10_000.0, 0.0],
        }
    )

    signals = build_external_research_signal_events(
        portfolio,
        hshare_summary_path=summary_path,
    )

    assert signals.height == 2
    assert signals["strategy_id"].to_list() == [
        "external_factor::factor_a",
        "external_factor::factor_a",
    ]
    assert signals["side"].to_list() == ["buy", "buy"]
