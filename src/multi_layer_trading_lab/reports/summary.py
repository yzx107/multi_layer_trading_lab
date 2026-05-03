from __future__ import annotations

from pathlib import Path

import polars as pl


def _top_rows(frame: pl.DataFrame, limit: int = 5) -> str:
    if frame.is_empty():
        return "_empty_"
    return frame.head(limit).write_json()


def render_research_summary(
    path: Path,
    *,
    validation_summary: pl.DataFrame,
    quality_summary: pl.DataFrame,
    posterior_summary: pl.DataFrame,
    lead_lag_summary: pl.DataFrame,
) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    content = "\n".join(
        [
            "# Research Summary",
            "",
            "## Contract Validation",
            _top_rows(validation_summary),
            "",
            "## Data Quality",
            _top_rows(quality_summary),
            "",
            "## Bayesian Posterior Summary",
            _top_rows(posterior_summary),
            "",
            "## Lead-Lag Candidates",
            _top_rows(lead_lag_summary),
            "",
            "> transfer entropy scores here are ranking aids, not causal proof.",
        ]
    )
    path.write_text(content, encoding="utf-8")
    return path
