from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from multi_layer_trading_lab.adapters.futu.reports import (
    futu_order_reports_to_execution_reports,
    load_futu_order_report_rows,
)
from multi_layer_trading_lab.execution.paper_audit import (
    PaperAuditInput,
    PaperAuditResult,
    run_paper_promotion_audit,
)
from multi_layer_trading_lab.execution.reconciliation import (
    load_execution_log,
    reconcile_execution_reports,
)


@dataclass(frozen=True, slots=True)
class PaperEvidenceInput:
    execution_log_path: Path
    broker_report_path: Path
    paper_sessions: int
    max_allowed_slippage: float = 0.05
    manual_live_enable: bool = False
    price_tolerance: float = 0.01


@dataclass(frozen=True, slots=True)
class PaperEvidenceResult:
    audit: PaperAuditResult | None
    execution_log_rows: int
    broker_report_rows: int
    failed_reasons: tuple[str, ...]

    @property
    def ready(self) -> bool:
        return self.audit is not None and not self.failed_reasons


def build_paper_evidence(input_data: PaperEvidenceInput) -> PaperEvidenceResult:
    failed: list[str] = []
    if not input_data.execution_log_path.exists():
        failed.append("missing_execution_log")
    if not input_data.broker_report_path.exists():
        failed.append("missing_broker_report")
    if failed:
        return PaperEvidenceResult(
            audit=None,
            execution_log_rows=0,
            broker_report_rows=0,
            failed_reasons=tuple(failed),
        )

    local_records = load_execution_log(input_data.execution_log_path)
    broker_rows = load_futu_order_report_rows(input_data.broker_report_path)
    if not local_records:
        failed.append("empty_execution_log")
    if not broker_rows:
        failed.append("empty_broker_report")
    if any(record.get("dry_run") is True for record in local_records):
        failed.append("dry_run_execution_log_not_real_paper")
    if any(row.get("dry_run") is True for row in broker_rows):
        failed.append("dry_run_broker_report_not_real_paper")

    broker_reports = futu_order_reports_to_execution_reports(broker_rows)
    reconciliation = reconcile_execution_reports(
        local_records,
        broker_reports,
        price_tolerance=input_data.price_tolerance,
    )
    audit = run_paper_promotion_audit(
        PaperAuditInput(
            paper_sessions=input_data.paper_sessions,
            local_records=local_records,
            reconciliation=reconciliation,
            max_allowed_slippage=input_data.max_allowed_slippage,
            manual_live_enable=input_data.manual_live_enable,
        )
    )
    if failed:
        return PaperEvidenceResult(
            audit=audit,
            execution_log_rows=len(local_records),
            broker_report_rows=len(broker_rows),
            failed_reasons=tuple(failed),
        )
    return PaperEvidenceResult(
        audit=audit,
        execution_log_rows=len(local_records),
        broker_report_rows=len(broker_rows),
        failed_reasons=(),
    )
