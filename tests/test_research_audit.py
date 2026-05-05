from __future__ import annotations

from datetime import date

import polars as pl

from multi_layer_trading_lab.backtest.engine import BacktestMetrics
from multi_layer_trading_lab.research.audit import (
    ResearchAuditInput,
    build_research_gate_evidence,
    evaluate_order_add_research_gate,
    run_research_promotion_audit,
)
from multi_layer_trading_lab.research.cost_capacity import CapacityAuditResult, CostAuditResult
from multi_layer_trading_lab.risk.profile import personal_trader_profile
from multi_layer_trading_lab.risk.promotion import PromotionGateConfig


def test_research_audit_builds_gate_evidence_from_signals_and_backtest() -> None:
    signals = pl.DataFrame(
        {
            "signal_id": ["s1", "s2", "s3"],
            "trade_date": [date(2026, 4, 1), date(2026, 4, 1), date(2026, 4, 2)],
        }
    )
    metrics = BacktestMetrics(
        total_pnl=100.0,
        turnover=10_000.0,
        max_drawdown=50.0,
        hit_ratio=0.6,
        fills=3,
        rejected=0,
    )

    evidence = build_research_gate_evidence(
        ResearchAuditInput(
            signal_events=signals,
            backtest_metrics=metrics,
            no_lookahead_audit_passed=True,
            cost_model_applied=True,
            capacity_check_passed=False,
        )
    )

    assert evidence.trade_count == 3
    assert evidence.distinct_trade_dates == 2
    assert evidence.no_lookahead_audit_passed is True
    assert evidence.cost_model_applied is True
    assert evidence.capacity_check_passed is False


def test_research_audit_blocks_paper_when_capacity_check_missing() -> None:
    signals = pl.DataFrame(
        {
            "signal_id": ["s1", "s2"],
            "trade_date": [date(2026, 4, 1), date(2026, 4, 2)],
        }
    )

    result = run_research_promotion_audit(
        ResearchAuditInput(
            signal_events=signals,
            no_lookahead_audit_passed=True,
            cost_model_applied=True,
            capacity_check_passed=False,
        ),
        config=PromotionGateConfig(min_research_trades=2, min_research_trade_dates=2),
    )

    assert result.decision.approved is False
    assert result.decision.failed_reasons == ("capacity_check_not_passed",)


def test_research_audit_uses_cost_and_capacity_audit_results() -> None:
    signals = pl.DataFrame(
        {
            "signal_id": ["s1", "s2"],
            "trade_date": [date(2026, 4, 1), date(2026, 4, 2)],
        }
    )

    result = run_research_promotion_audit(
        ResearchAuditInput(
            signal_events=signals,
            no_lookahead_audit_passed=True,
            cost_audit=CostAuditResult(
                passed=True,
                total_notional=10_000,
                estimated_fees=10,
                estimated_slippage=5,
                total_cost=15,
                total_cost_bps=15,
            ),
            capacity_audit=CapacityAuditResult(
                passed=True,
                max_fill_notional=5_000,
                max_symbol_notional=10_000,
            ),
        ),
        config=PromotionGateConfig(min_research_trades=2, min_research_trade_dates=2),
    )

    assert result.evidence.cost_model_applied is True
    assert result.evidence.capacity_check_passed is True
    assert result.decision.approved is True


def test_order_add_research_gate_blocks_negative_best_sweep() -> None:
    sweep = pl.DataFrame(
        {
            "min_order_add_volume": [200_000],
            "min_large_order_ratio": [0.03],
            "trade_count": [20],
            "avg_net_ret": [-0.002],
            "total_net_ret": [-0.04],
        }
    )

    result = evaluate_order_add_research_gate(sweep, min_trade_count=30)

    assert result.approved is False
    assert result.best_params["min_order_add_volume"] == 200_000
    assert result.failed_reasons == (
        "insufficient_order_add_trades",
        "avg_net_ret_not_positive",
        "total_net_ret_not_positive",
    )


def test_order_add_research_gate_approves_positive_best_sweep() -> None:
    sweep = pl.DataFrame(
        {
            "min_order_add_volume": [200_000, 400_000],
            "min_large_order_ratio": [0.03, 0.05],
            "trade_count": [50, 10],
            "avg_net_ret": [0.001, 0.003],
            "total_net_ret": [0.05, 0.03],
        }
    )

    result = evaluate_order_add_research_gate(sweep, min_trade_count=30)

    assert result.approved is True
    assert result.best_params["min_order_add_volume"] == 200_000
    assert result.failed_reasons == ()


def test_order_add_research_gate_blocks_personal_capacity_breach() -> None:
    sweep = pl.DataFrame(
        {
            "min_order_add_volume": [200_000],
            "min_large_order_ratio": [0.03],
            "trade_count": [50],
            "avg_net_ret": [0.001],
            "total_net_ret": [0.05],
        }
    )

    result = evaluate_order_add_research_gate(
        sweep,
        min_trade_count=30,
        account_profile=personal_trader_profile(account_equity=1_000_000),
    )

    assert result.approved is False
    assert result.best_params["estimated_single_trade_notional"] == 200_000
    assert result.best_params["estimated_strategy_notional"] == 10_000_000
    assert result.failed_reasons == (
        "order_add_single_trade_exceeds_personal_limit",
        "order_add_strategy_notional_exceeds_personal_limit",
    )


def test_order_add_research_gate_uses_planned_notional_for_capacity() -> None:
    sweep = pl.DataFrame(
        {
            "min_order_add_volume": [200_000],
            "min_large_order_ratio": [0.03],
            "planned_notional": [4_000.0],
            "trade_count": [50],
            "avg_net_ret": [0.001],
            "total_net_ret": [0.05],
        }
    )

    result = evaluate_order_add_research_gate(
        sweep,
        min_trade_count=30,
        account_profile=personal_trader_profile(account_equity=1_000_000),
    )

    assert result.approved is True
    assert result.best_params["estimated_single_trade_notional"] == 4_000
    assert result.best_params["estimated_strategy_notional"] == 200_000
