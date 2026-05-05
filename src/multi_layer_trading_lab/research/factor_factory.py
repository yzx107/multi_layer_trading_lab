from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import polars as pl


@dataclass(frozen=True, slots=True)
class FactorFactorySummary:
    candidates: pl.DataFrame
    family_summary: pl.DataFrame


def _empty_candidates() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "factor_name": pl.String,
            "decision": pl.String,
            "direction_hint": pl.String,
            "mean_abs_rank_ic": pl.Float64,
            "mean_normalized_mutual_info": pl.Float64,
            "mean_coverage_ratio": pl.Float64,
            "sign_consistency": pl.Float64,
            "summary_path": pl.String,
            "family_id": pl.String,
            "family_name": pl.String,
            "mechanism": pl.String,
            "forbidden_semantic_assumptions": pl.String,
            "candidate_status": pl.String,
        }
    )


def load_factor_factory_summary(registry_root: Path) -> FactorFactorySummary:
    gate_b_path = registry_root / "gate_b_log.tsv"
    families_path = registry_root / "factor_families.tsv"
    if not gate_b_path.exists():
        return FactorFactorySummary(candidates=_empty_candidates(), family_summary=pl.DataFrame())

    gate_b = pl.read_csv(gate_b_path, separator="\t", infer_schema_length=1000)
    families = (
        pl.read_csv(families_path, separator="\t", infer_schema_length=1000)
        if families_path.exists()
        else pl.DataFrame()
    )
    latest_gate_b = (
        gate_b.sort("created_at")
        .group_by("factor_name", maintain_order=True)
        .tail(1)
        .with_columns(
            pl.when(pl.col("decision") == "pass")
            .then(pl.lit("gate_b_passed_review_required"))
            .otherwise(pl.lit("rejected_or_review"))
            .alias("candidate_status")
        )
    )

    if families.is_empty():
        return FactorFactorySummary(candidates=latest_gate_b, family_summary=families)

    family_members = families.select(
        [
            "family_id",
            "family_name",
            "mechanism",
            "current_members",
            "forbidden_semantic_assumptions",
            "status",
        ]
    )
    exploded = (
        family_members.with_columns(pl.col("current_members").str.split(","))
        .explode("current_members")
        .with_columns(pl.col("current_members").str.strip_chars().alias("factor_name"))
        .drop("current_members")
    )
    candidates = latest_gate_b.join(exploded, on="factor_name", how="left")
    family_summary = (
        candidates.group_by("family_id", "family_name", "mechanism")
        .agg(
            [
                pl.len().alias("candidate_count"),
                (pl.col("decision") == "pass").sum().alias("gate_b_pass_count"),
                pl.col("mean_abs_rank_ic").mean().alias("avg_abs_rank_ic"),
                pl.col("mean_normalized_mutual_info").mean().alias("avg_nmi"),
            ]
        )
        .sort(["gate_b_pass_count", "avg_abs_rank_ic"], descending=[True, True])
    )
    return FactorFactorySummary(candidates=candidates, family_summary=family_summary)
