from __future__ import annotations

from multi_layer_trading_lab.execution.paper_audit import (
    PaperAuditInput,
    build_paper_gate_evidence,
    run_paper_promotion_audit,
)
from multi_layer_trading_lab.execution.reconciliation import ReconciliationResult
from multi_layer_trading_lab.risk.promotion import PromotionGateConfig


def test_paper_audit_builds_evidence_from_logs_and_reconciliation() -> None:
    evidence = build_paper_gate_evidence(
        PaperAuditInput(
            paper_sessions=20,
            local_records=[
                {"status": "filled", "slippage": 0.01},
                {"status": "rejected", "slippage": 0.00},
            ],
            reconciliation=ReconciliationResult(matched_orders=1),
            max_allowed_slippage=0.05,
            manual_live_enable=True,
        )
    )

    assert evidence.paper_sessions == 20
    assert evidence.order_reject_rate == 0.5
    assert evidence.reconciliation_clean is True
    assert evidence.slippage_within_assumption is True
    assert evidence.manual_live_enable is True


def test_paper_audit_blocks_live_when_reject_rate_is_high() -> None:
    result = run_paper_promotion_audit(
        PaperAuditInput(
            paper_sessions=20,
            local_records=[
                {"status": "filled", "slippage": 0.01},
                {"status": "rejected", "slippage": 0.00},
            ],
            reconciliation=ReconciliationResult(matched_orders=1),
            max_allowed_slippage=0.05,
            manual_live_enable=True,
        ),
        config=PromotionGateConfig(max_order_reject_rate=0.1),
    )

    assert result.decision.approved is False
    assert result.decision.failed_reasons == ("order_reject_rate_too_high",)
