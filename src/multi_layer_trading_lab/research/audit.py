from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime

import polars as pl

from multi_layer_trading_lab.backtest.engine import BacktestMetrics
from multi_layer_trading_lab.research.cost_capacity import CapacityAuditResult, CostAuditResult
from multi_layer_trading_lab.risk.profile import PersonalAccountProfile
from multi_layer_trading_lab.risk.promotion import (
    PromotionDecision,
    PromotionGateConfig,
    ResearchGateEvidence,
    evaluate_research_to_paper,
)


@dataclass(frozen=True, slots=True)
class ResearchAuditInput:
    signal_events: pl.DataFrame
    backtest_metrics: BacktestMetrics | None = None
    no_lookahead_audit_passed: bool = False
    cost_model_applied: bool = False
    capacity_check_passed: bool = False
    cost_audit: CostAuditResult | None = None
    capacity_audit: CapacityAuditResult | None = None


@dataclass(frozen=True, slots=True)
class ResearchAuditResult:
    evidence: ResearchGateEvidence
    decision: PromotionDecision


@dataclass(frozen=True, slots=True)
class OrderAddResearchGateResult:
    approved: bool
    best_params: dict[str, float | int] = field(default_factory=dict)
    failed_reasons: tuple[str, ...] = ()


def _distinct_trade_dates(signal_events: pl.DataFrame) -> int:
    if signal_events.is_empty():
        return 0
    if "trade_date" in signal_events.columns:
        return signal_events.select(pl.col("trade_date")).unique().height
    if "event_ts" in signal_events.columns:
        dates = []
        for value in signal_events["event_ts"].to_list():
            if isinstance(value, datetime):
                dates.append(value.date())
            elif isinstance(value, date):
                dates.append(value)
        return len(set(dates))
    return 0


def build_research_gate_evidence(audit_input: ResearchAuditInput) -> ResearchGateEvidence:
    trade_count = (
        audit_input.backtest_metrics.fills
        if audit_input.backtest_metrics is not None
        else audit_input.signal_events.height
    )
    cost_model_applied = (
        audit_input.cost_audit.passed
        if audit_input.cost_audit is not None
        else audit_input.cost_model_applied
    )
    capacity_check_passed = (
        audit_input.capacity_audit.passed
        if audit_input.capacity_audit is not None
        else audit_input.capacity_check_passed
    )
    return ResearchGateEvidence(
        trade_count=trade_count,
        distinct_trade_dates=_distinct_trade_dates(audit_input.signal_events),
        no_lookahead_audit_passed=audit_input.no_lookahead_audit_passed,
        cost_model_applied=cost_model_applied,
        capacity_check_passed=capacity_check_passed,
    )


def run_research_promotion_audit(
    audit_input: ResearchAuditInput,
    config: PromotionGateConfig | None = None,
) -> ResearchAuditResult:
    evidence = build_research_gate_evidence(audit_input)
    decision = evaluate_research_to_paper(evidence, config=config)
    return ResearchAuditResult(evidence=evidence, decision=decision)


def evaluate_order_add_research_gate(
    threshold_sweep: pl.DataFrame,
    *,
    min_trade_count: int = 30,
    min_avg_net_ret: float = 0.0,
    min_total_net_ret: float = 0.0,
    account_profile: PersonalAccountProfile | None = None,
) -> OrderAddResearchGateResult:
    if threshold_sweep.is_empty():
        return OrderAddResearchGateResult(
            approved=False,
            failed_reasons=("empty_threshold_sweep",),
        )

    ranked = threshold_sweep.sort(
        ["avg_net_ret", "total_net_ret", "trade_count"],
        descending=[True, True, True],
    )
    eligible = ranked.filter(pl.col("trade_count") >= min_trade_count)
    selected = eligible if not eligible.is_empty() else ranked
    best = selected.row(0, named=True)
    failed: list[str] = []
    if int(best["trade_count"]) < min_trade_count:
        failed.append("insufficient_order_add_trades")
    if float(best["avg_net_ret"]) <= min_avg_net_ret:
        failed.append("avg_net_ret_not_positive")
    if float(best["total_net_ret"]) <= min_total_net_ret:
        failed.append("total_net_ret_not_positive")
    if account_profile is not None:
        best_single_trade_notional = float(
            best.get("planned_notional", best["min_order_add_volume"])
        )
        estimated_strategy_notional = best_single_trade_notional * int(best["trade_count"])
        if best_single_trade_notional > account_profile.max_single_name_notional:
            failed.append("order_add_single_trade_exceeds_personal_limit")
        if estimated_strategy_notional > account_profile.max_strategy_notional:
            failed.append("order_add_strategy_notional_exceeds_personal_limit")

    return OrderAddResearchGateResult(
        approved=not failed,
        best_params={
            "min_order_add_volume": int(best["min_order_add_volume"]),
            "min_large_order_ratio": float(best["min_large_order_ratio"]),
            "trade_count": int(best["trade_count"]),
            "avg_net_ret": float(best["avg_net_ret"]),
            "total_net_ret": float(best["total_net_ret"]),
            "estimated_single_trade_notional": float(
                best.get("planned_notional", best["min_order_add_volume"])
            ),
            "estimated_strategy_notional": float(
                best.get("planned_notional", best["min_order_add_volume"])
            )
            * int(best["trade_count"]),
        },
        failed_reasons=tuple(failed),
    )
