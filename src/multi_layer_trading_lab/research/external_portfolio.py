from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from multi_layer_trading_lab.research.cost_capacity import CapacityAuditResult, CostAuditResult
from multi_layer_trading_lab.risk.profile import PersonalAccountProfile


@dataclass(frozen=True, slots=True)
class ExternalPortfolioEvidence:
    candidate_count: int
    review_candidate_count: int
    target_notional: float
    max_single_candidate_notional: float
    failed_reasons: tuple[str, ...]

    @property
    def approved(self) -> bool:
        return not self.failed_reasons


def build_external_factor_portfolio(
    factor_candidates: pl.DataFrame,
    *,
    account_profile: PersonalAccountProfile,
    min_abs_rank_ic: float = 0.05,
    min_nmi: float = 0.01,
    max_family_weight: float = 0.50,
) -> pl.DataFrame:
    if factor_candidates.is_empty():
        return pl.DataFrame(
            schema={
                "factor_name": pl.String,
                "family_id": pl.String,
                "direction_hint": pl.String,
                "evidence_score": pl.Float64,
                "target_weight": pl.Float64,
                "target_notional": pl.Float64,
                "candidate_status": pl.String,
                "failed_reasons": pl.String,
            }
        )

    required = {
        "factor_name",
        "decision",
        "direction_hint",
        "mean_abs_rank_ic",
        "mean_normalized_mutual_info",
        "mean_coverage_ratio",
        "family_id",
    }
    missing = required.difference(factor_candidates.columns)
    if missing:
        raise ValueError(f"factor_candidates missing columns: {sorted(missing)}")

    eligible = factor_candidates.with_columns(
        [
            (
                pl.col("mean_abs_rank_ic").fill_null(0.0)
                * pl.col("mean_normalized_mutual_info").fill_null(0.0)
                * pl.col("mean_coverage_ratio").fill_null(0.0)
            ).alias("evidence_score"),
            pl.concat_str(
                [
                    pl.when(pl.col("decision") == "pass")
                    .then(pl.lit(""))
                    .otherwise(pl.lit("gate_b_not_passed;")),
                    pl.when(pl.col("mean_abs_rank_ic").fill_null(0.0) >= min_abs_rank_ic)
                    .then(pl.lit(""))
                    .otherwise(pl.lit("weak_rank_ic;")),
                    pl.when(pl.col("mean_normalized_mutual_info").fill_null(0.0) >= min_nmi)
                    .then(pl.lit(""))
                    .otherwise(pl.lit("weak_nmi;")),
                ]
            ).str.strip_chars(";").alias("failed_reasons"),
        ]
    ).with_columns(
        pl.when(pl.col("failed_reasons") == "")
        .then(pl.lit("review_candidate"))
        .otherwise(pl.lit("blocked"))
        .alias("candidate_status")
    )

    reviewable = eligible.filter(pl.col("candidate_status") == "review_candidate")
    if reviewable.is_empty():
        return eligible.with_columns(
            [
                pl.lit(0.0).alias("target_weight"),
                pl.lit(0.0).alias("target_notional"),
            ]
        ).select(
            [
                "factor_name",
                "family_id",
                "direction_hint",
                "evidence_score",
                "target_weight",
                "target_notional",
                "candidate_status",
                "failed_reasons",
            ]
        )

    total_score = reviewable["evidence_score"].sum()
    if total_score <= 0:
        weighted = reviewable.with_columns(pl.lit(1.0 / reviewable.height).alias("raw_weight"))
    else:
        weighted = reviewable.with_columns(
            (pl.col("evidence_score") / total_score).alias("raw_weight")
        )

    family_weight = weighted.group_by("family_id").agg(
        pl.col("raw_weight").sum().alias("family_raw_weight")
    )
    weighted = (
        weighted.join(family_weight, on="family_id", how="left")
        .with_columns(
            pl.when(pl.col("family_raw_weight") > max_family_weight)
            .then(pl.col("raw_weight") * max_family_weight / pl.col("family_raw_weight"))
            .otherwise(pl.col("raw_weight"))
            .alias("capped_weight")
        )
    )
    capped_total = weighted["capped_weight"].sum()
    weighted = weighted.with_columns(
        [
            (pl.col("capped_weight") / capped_total).alias("target_weight"),
            (
                pl.col("capped_weight")
                / capped_total
                * account_profile.max_strategy_notional
                * account_profile.default_kelly_scale
            ).alias("target_notional"),
        ]
    )

    blocked = eligible.filter(pl.col("candidate_status") == "blocked").with_columns(
        [
            pl.lit(0.0).alias("target_weight"),
            pl.lit(0.0).alias("target_notional"),
        ]
    )
    return pl.concat(
        [
            weighted,
            blocked,
        ],
        how="diagonal_relaxed",
    ).select(
        [
            "factor_name",
            "family_id",
            "direction_hint",
            "evidence_score",
            "target_weight",
            "target_notional",
            "candidate_status",
            "failed_reasons",
        ]
    ).sort(["candidate_status", "target_weight"], descending=[True, True])


def evaluate_external_factor_portfolio(
    portfolio: pl.DataFrame,
    *,
    account_profile: PersonalAccountProfile,
    min_review_candidates: int = 1,
    min_target_notional: float = 1.0,
) -> ExternalPortfolioEvidence:
    failed: list[str] = []
    if portfolio.is_empty():
        return ExternalPortfolioEvidence(
            candidate_count=0,
            review_candidate_count=0,
            target_notional=0.0,
            max_single_candidate_notional=0.0,
            failed_reasons=("missing_external_factor_portfolio",),
        )
    required = {"candidate_status", "target_notional"}
    missing = required.difference(portfolio.columns)
    if missing:
        return ExternalPortfolioEvidence(
            candidate_count=portfolio.height,
            review_candidate_count=0,
            target_notional=0.0,
            max_single_candidate_notional=0.0,
            failed_reasons=(f"external_factor_portfolio_missing_columns:{','.join(sorted(missing))}",),
        )

    reviewable = portfolio.filter(pl.col("candidate_status") == "review_candidate")
    target_notional = float(reviewable["target_notional"].sum()) if reviewable.height else 0.0
    max_single = float(reviewable["target_notional"].max()) if reviewable.height else 0.0

    if reviewable.height < min_review_candidates:
        failed.append("insufficient_external_review_candidates")
    if target_notional < min_target_notional:
        failed.append("external_target_notional_too_small")
    if max_single > account_profile.max_single_name_notional:
        failed.append("external_candidate_exceeds_single_name_limit")
    if target_notional > account_profile.max_strategy_notional:
        failed.append("external_portfolio_exceeds_strategy_limit")

    return ExternalPortfolioEvidence(
        candidate_count=portfolio.height,
        review_candidate_count=reviewable.height,
        target_notional=target_notional,
        max_single_candidate_notional=max_single,
        failed_reasons=tuple(failed),
    )


def audit_external_portfolio_cost_capacity(
    portfolio: pl.DataFrame,
    *,
    account_profile: PersonalAccountProfile,
    assumed_total_cost_bps: float = 35.0,
    max_total_cost_bps: float = 35.0,
) -> tuple[CostAuditResult, CapacityAuditResult]:
    if portfolio.is_empty() or "candidate_status" not in portfolio.columns:
        return (
            CostAuditResult(
                passed=False,
                total_notional=0.0,
                estimated_fees=0.0,
                estimated_slippage=0.0,
                total_cost=0.0,
                total_cost_bps=0.0,
                failed_reasons=("missing_external_factor_portfolio",),
            ),
            CapacityAuditResult(
                passed=False,
                max_fill_notional=0.0,
                max_symbol_notional=0.0,
                failed_reasons=("missing_external_factor_portfolio",),
            ),
        )

    reviewable = portfolio.filter(pl.col("candidate_status") == "review_candidate")
    target_notional = (
        float(reviewable["target_notional"].sum())
        if "target_notional" in reviewable.columns and reviewable.height
        else 0.0
    )
    max_single = (
        float(reviewable["target_notional"].max())
        if "target_notional" in reviewable.columns and reviewable.height
        else 0.0
    )
    estimated_cost = target_notional * assumed_total_cost_bps / 10_000

    cost_failed: list[str] = []
    if reviewable.is_empty() or target_notional <= 0:
        cost_failed.append("no_reviewable_external_notional")
    if assumed_total_cost_bps > max_total_cost_bps:
        cost_failed.append("external_cost_assumption_too_high")

    capacity_failed: list[str] = []
    if reviewable.is_empty() or target_notional <= 0:
        capacity_failed.append("no_reviewable_external_notional")
    if max_single > account_profile.max_single_name_notional:
        capacity_failed.append("external_candidate_exceeds_single_name_limit")
    if target_notional > account_profile.max_strategy_notional:
        capacity_failed.append("external_portfolio_exceeds_strategy_limit")

    return (
        CostAuditResult(
            passed=not cost_failed,
            total_notional=target_notional,
            estimated_fees=estimated_cost,
            estimated_slippage=0.0,
            total_cost=estimated_cost,
            total_cost_bps=assumed_total_cost_bps if target_notional > 0 else 0.0,
            failed_reasons=tuple(cost_failed),
        ),
        CapacityAuditResult(
            passed=not capacity_failed,
            max_fill_notional=max_single,
            max_symbol_notional=max_single,
            failed_reasons=tuple(capacity_failed),
        ),
    )
