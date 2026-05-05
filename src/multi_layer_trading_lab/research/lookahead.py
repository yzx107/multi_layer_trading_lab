from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import polars as pl


@dataclass(frozen=True, slots=True)
class LookaheadAuditEvidence:
    passed: bool
    reviewed_factor_count: int
    matched_pre_eval_count: int
    failed_reasons: tuple[str, ...]


def audit_factor_factory_lookahead_lineage(
    portfolio: pl.DataFrame,
    *,
    registry_root: Path,
) -> LookaheadAuditEvidence:
    gate_b_path = registry_root / "gate_b_log.tsv"
    pre_eval_path = registry_root / "pre_eval_log.tsv"
    failed: list[str] = []

    if portfolio.is_empty():
        return LookaheadAuditEvidence(False, 0, 0, ("missing_external_factor_portfolio",))
    reviewable = portfolio.filter(pl.col("candidate_status") == "review_candidate")
    reviewed_factors = (
        set(reviewable["factor_name"].to_list()) if "factor_name" in reviewable.columns else set()
    )
    if not reviewed_factors:
        return LookaheadAuditEvidence(False, 0, 0, ("no_reviewable_external_factors",))
    if not gate_b_path.exists():
        failed.append("missing_gate_b_log")
    if not pre_eval_path.exists():
        failed.append("missing_pre_eval_log")
    if failed:
        return LookaheadAuditEvidence(False, len(reviewed_factors), 0, tuple(failed))

    gate_b = pl.read_csv(gate_b_path, separator="\t", infer_schema_length=1000)
    pre_eval = pl.read_csv(pre_eval_path, separator="\t", infer_schema_length=1000)
    latest_gate_b = (
        gate_b.filter(pl.col("factor_name").is_in(list(reviewed_factors)))
        .sort("created_at")
        .group_by("factor_name", maintain_order=True)
        .tail(1)
    )
    joined = latest_gate_b.join(
        pre_eval.select(
            [
                "pre_eval_id",
                "created_at",
                "factor_name",
                "label_name",
                "evaluated_dates",
                "joined_rows",
                "summary_path",
            ]
        ).rename(
            {
                "created_at": "pre_eval_created_at",
                "summary_path": "pre_eval_summary_path",
            }
        ),
        on=["pre_eval_id", "factor_name"],
        how="left",
    )
    missing_gate_factors = reviewed_factors.difference(set(latest_gate_b["factor_name"].to_list()))
    if missing_gate_factors:
        failed.append("missing_gate_b_for_review_factors")
    if joined.height < len(reviewed_factors):
        failed.append("missing_pre_eval_for_review_factors")
    if joined.filter(pl.col("decision") != "pass").height:
        failed.append("non_pass_gate_b_in_review_factors")
    if joined.filter(pl.col("joined_rows").fill_null(0) <= 0).height:
        failed.append("empty_pre_eval_joined_rows")
    if joined.filter(~pl.col("label_name").fill_null("").str.starts_with("forward_return")).height:
        failed.append("unexpected_pre_eval_label")
    if joined.filter(pl.col("evaluated_dates").fill_null("") == "").height:
        failed.append("missing_evaluated_dates")

    for path in joined["summary_path"].drop_nulls().to_list():
        if not Path(path).exists():
            failed.append("missing_gate_b_summary_path")
            break
    for path in joined["pre_eval_summary_path"].drop_nulls().to_list():
        if not Path(path).exists():
            failed.append("missing_pre_eval_summary_path")
            break

    if "created_at" in joined.columns and "pre_eval_created_at" in joined.columns:
        timing_failures = joined.filter(pl.col("created_at") < pl.col("pre_eval_created_at")).height
        if timing_failures:
            failed.append("gate_b_created_before_pre_eval")

    return LookaheadAuditEvidence(
        passed=not failed,
        reviewed_factor_count=len(reviewed_factors),
        matched_pre_eval_count=joined.height,
        failed_reasons=tuple(dict.fromkeys(failed)),
    )
