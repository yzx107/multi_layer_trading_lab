from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class ResearchGateEvidence:
    trade_count: int
    distinct_trade_dates: int
    no_lookahead_audit_passed: bool
    cost_model_applied: bool
    capacity_check_passed: bool


@dataclass(frozen=True, slots=True)
class PaperGateEvidence:
    paper_sessions: int
    order_reject_rate: float
    reconciliation_clean: bool
    slippage_within_assumption: bool
    manual_live_enable: bool


@dataclass(frozen=True, slots=True)
class PromotionGateConfig:
    min_research_trades: int = 80
    min_research_trade_dates: int = 20
    min_paper_sessions: int = 20
    max_order_reject_rate: float = 0.02


@dataclass(frozen=True, slots=True)
class PromotionDecision:
    approved: bool
    failed_reasons: tuple[str, ...] = field(default_factory=tuple)


def evaluate_research_to_paper(
    evidence: ResearchGateEvidence,
    config: PromotionGateConfig | None = None,
) -> PromotionDecision:
    cfg = config or PromotionGateConfig()
    failed: list[str] = []

    if evidence.trade_count < cfg.min_research_trades:
        failed.append("insufficient_research_trades")
    if evidence.distinct_trade_dates < cfg.min_research_trade_dates:
        failed.append("insufficient_research_trade_dates")
    if not evidence.no_lookahead_audit_passed:
        failed.append("lookahead_audit_not_passed")
    if not evidence.cost_model_applied:
        failed.append("cost_model_missing")
    if not evidence.capacity_check_passed:
        failed.append("capacity_check_not_passed")

    return PromotionDecision(approved=not failed, failed_reasons=tuple(failed))


def evaluate_paper_to_live(
    evidence: PaperGateEvidence,
    config: PromotionGateConfig | None = None,
) -> PromotionDecision:
    cfg = config or PromotionGateConfig()
    failed: list[str] = []

    if evidence.paper_sessions < cfg.min_paper_sessions:
        failed.append("insufficient_paper_sessions")
    if evidence.order_reject_rate > cfg.max_order_reject_rate:
        failed.append("order_reject_rate_too_high")
    if not evidence.reconciliation_clean:
        failed.append("reconciliation_not_clean")
    if not evidence.slippage_within_assumption:
        failed.append("slippage_outside_assumption")
    if not evidence.manual_live_enable:
        failed.append("manual_live_enable_missing")

    return PromotionDecision(approved=not failed, failed_reasons=tuple(failed))
