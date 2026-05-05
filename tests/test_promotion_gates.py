from __future__ import annotations

from multi_layer_trading_lab.risk.promotion import (
    PaperGateEvidence,
    ResearchGateEvidence,
    evaluate_paper_to_live,
    evaluate_research_to_paper,
)


def test_research_to_paper_requires_sample_and_audits() -> None:
    decision = evaluate_research_to_paper(
        ResearchGateEvidence(
            trade_count=12,
            distinct_trade_dates=5,
            no_lookahead_audit_passed=False,
            cost_model_applied=True,
            capacity_check_passed=False,
        )
    )

    assert decision.approved is False
    assert decision.failed_reasons == (
        "insufficient_research_trades",
        "insufficient_research_trade_dates",
        "lookahead_audit_not_passed",
        "capacity_check_not_passed",
    )


def test_research_to_paper_approves_complete_evidence() -> None:
    decision = evaluate_research_to_paper(
        ResearchGateEvidence(
            trade_count=120,
            distinct_trade_dates=30,
            no_lookahead_audit_passed=True,
            cost_model_applied=True,
            capacity_check_passed=True,
        )
    )

    assert decision.approved is True
    assert decision.failed_reasons == ()


def test_paper_to_live_requires_clean_execution_evidence() -> None:
    decision = evaluate_paper_to_live(
        PaperGateEvidence(
            paper_sessions=20,
            order_reject_rate=0.05,
            reconciliation_clean=False,
            slippage_within_assumption=True,
            manual_live_enable=False,
        )
    )

    assert decision.approved is False
    assert decision.failed_reasons == (
        "order_reject_rate_too_high",
        "reconciliation_not_clean",
        "manual_live_enable_missing",
    )
