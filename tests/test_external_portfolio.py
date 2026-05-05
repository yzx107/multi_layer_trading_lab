import polars as pl

from multi_layer_trading_lab.research.external_portfolio import (
    audit_external_portfolio_cost_capacity,
    build_external_factor_portfolio,
    evaluate_external_factor_portfolio,
)
from multi_layer_trading_lab.risk.profile import personal_trader_profile


def test_external_factor_portfolio_allocates_review_candidates_with_kelly_scale():
    candidates = pl.DataFrame(
        {
            "factor_name": ["factor_a", "factor_b", "factor_c"],
            "decision": ["pass", "pass", "fail"],
            "direction_hint": ["as_is", "inverse", "as_is"],
            "mean_abs_rank_ic": [0.20, 0.10, 0.30],
            "mean_normalized_mutual_info": [0.04, 0.02, 0.05],
            "mean_coverage_ratio": [0.90, 0.80, 0.95],
            "family_id": ["fam_a", "fam_b", "fam_a"],
        }
    )

    portfolio = build_external_factor_portfolio(
        candidates,
        account_profile=personal_trader_profile(account_equity=1_000_000),
    )

    review = portfolio.filter(pl.col("candidate_status") == "review_candidate")
    blocked = portfolio.filter(pl.col("candidate_status") == "blocked")
    assert review.height == 2
    assert blocked.height == 1
    assert round(review["target_notional"].sum(), 6) == 25_000.0
    assert blocked.row(0, named=True)["target_notional"] == 0.0
    assert "gate_b_not_passed" in blocked.row(0, named=True)["failed_reasons"]


def test_external_factor_portfolio_audit_blocks_missing_portfolio():
    evidence = evaluate_external_factor_portfolio(
        pl.DataFrame(),
        account_profile=personal_trader_profile(account_equity=1_000_000),
    )

    assert not evidence.approved
    assert "missing_external_factor_portfolio" in evidence.failed_reasons


def test_external_factor_portfolio_audit_approves_reviewable_budget():
    evidence = evaluate_external_factor_portfolio(
        pl.DataFrame(
            {
                "candidate_status": ["review_candidate"],
                "target_notional": [25_000.0],
            }
        ),
        account_profile=personal_trader_profile(account_equity=1_000_000),
    )

    assert evidence.approved
    assert evidence.review_candidate_count == 1
    assert evidence.target_notional == 25_000.0


def test_external_portfolio_cost_capacity_audit_uses_personal_limits():
    cost_audit, capacity_audit = audit_external_portfolio_cost_capacity(
        pl.DataFrame(
            {
                "candidate_status": ["review_candidate"],
                "target_notional": [25_000.0],
            }
        ),
        account_profile=personal_trader_profile(account_equity=1_000_000),
    )

    assert cost_audit.passed
    assert cost_audit.total_cost_bps == 35.0
    assert capacity_audit.passed
    assert capacity_audit.max_symbol_notional == 25_000.0
