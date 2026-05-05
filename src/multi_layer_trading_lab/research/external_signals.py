from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import polars as pl


def _hshare_verified_dates(summary_path: Path) -> list[str]:
    payload = json.loads(summary_path.read_text(encoding="utf-8"))
    tables = payload.get("tables", {})
    orders = tables.get("verified_orders", {})
    dates = orders.get("dates") or payload.get("selection", {}).get("explicit_dates") or []
    return [str(value) for value in dates]


def build_external_research_signal_events(
    portfolio: pl.DataFrame,
    *,
    hshare_summary_path: Path,
    max_dates: int | None = None,
) -> pl.DataFrame:
    schema = {
        "signal_id": pl.String,
        "strategy_id": pl.String,
        "security_id": pl.String,
        "market": pl.String,
        "trade_date": pl.Date,
        "event_ts": pl.Datetime(time_zone="UTC"),
        "signal_type": pl.String,
        "side": pl.String,
        "data_source": pl.String,
        "created_at": pl.Datetime(time_zone="UTC"),
    }
    if portfolio.is_empty() or not hshare_summary_path.exists():
        return pl.DataFrame(schema=schema)

    reviewable = portfolio.filter(pl.col("candidate_status") == "review_candidate")
    if reviewable.is_empty():
        return pl.DataFrame(schema=schema)

    dates = _hshare_verified_dates(hshare_summary_path)
    if max_dates is not None:
        dates = dates[:max_dates]
    if not dates:
        return pl.DataFrame(schema=schema)

    created_at = datetime.now(UTC)
    rows = []
    for item in reviewable.iter_rows(named=True):
        factor_name = str(item["factor_name"])
        side = "sell" if "inverse" in str(item.get("direction_hint", "")) else "buy"
        for trade_date in dates:
            rows.append(
                {
                    "signal_id": f"external-{factor_name}-{trade_date}",
                    "strategy_id": f"external_factor::{factor_name}",
                    "security_id": factor_name,
                    "market": "HK",
                    "trade_date": trade_date,
                    "event_ts": f"{trade_date} 16:00:00+00:00",
                    "signal_type": "research_review",
                    "side": side,
                    "data_source": "hk_factor_autoresearch+hshare_verified",
                    "created_at": created_at,
                }
            )

    return pl.DataFrame(rows).with_columns(
        [
            pl.col("trade_date").str.strptime(pl.Date),
            pl.col("event_ts").str.strptime(pl.Datetime(time_zone="UTC")),
        ]
    ).select(list(schema))
