from __future__ import annotations

from dataclasses import dataclass

from multi_layer_trading_lab.execution.reconciliation import ReconciliationResult
from multi_layer_trading_lab.risk.promotion import (
    PaperGateEvidence,
    PromotionDecision,
    PromotionGateConfig,
    evaluate_paper_to_live,
)


@dataclass(frozen=True, slots=True)
class PaperAuditInput:
    paper_sessions: int
    local_records: list[dict[str, object]]
    reconciliation: ReconciliationResult
    max_allowed_slippage: float
    manual_live_enable: bool = False


@dataclass(frozen=True, slots=True)
class PaperAuditResult:
    evidence: PaperGateEvidence
    decision: PromotionDecision


def _order_reject_rate(local_records: list[dict[str, object]]) -> float:
    if not local_records:
        return 1.0
    rejected = sum(1 for record in local_records if str(record.get("status")) == "rejected")
    return rejected / len(local_records)


def _slippage_within_assumption(
    local_records: list[dict[str, object]],
    max_allowed_slippage: float,
) -> bool:
    if not local_records:
        return False
    slippages = [abs(float(record.get("slippage") or 0.0)) for record in local_records]
    return max(slippages, default=0.0) <= max_allowed_slippage


def build_paper_gate_evidence(audit_input: PaperAuditInput) -> PaperGateEvidence:
    return PaperGateEvidence(
        paper_sessions=audit_input.paper_sessions,
        order_reject_rate=_order_reject_rate(audit_input.local_records),
        reconciliation_clean=audit_input.reconciliation.clean,
        slippage_within_assumption=_slippage_within_assumption(
            audit_input.local_records,
            audit_input.max_allowed_slippage,
        ),
        manual_live_enable=audit_input.manual_live_enable,
    )


def run_paper_promotion_audit(
    audit_input: PaperAuditInput,
    config: PromotionGateConfig | None = None,
) -> PaperAuditResult:
    evidence = build_paper_gate_evidence(audit_input)
    return PaperAuditResult(evidence=evidence, decision=evaluate_paper_to_live(evidence, config))
