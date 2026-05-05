from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from multi_layer_trading_lab.execution.opend_tickets import load_opend_paper_tickets


@dataclass(frozen=True, slots=True)
class DryRunEvidenceResult:
    execution_log_path: Path
    broker_report_path: Path
    order_count: int
    failed_reasons: tuple[str, ...]

    @property
    def ready(self) -> bool:
        return not self.failed_reasons


def build_dry_run_execution_evidence(
    ticket_path: Path,
    *,
    execution_log_path: Path,
    broker_report_path: Path,
) -> DryRunEvidenceResult:
    tickets = load_opend_paper_tickets(ticket_path)
    failed: list[str] = []
    if not tickets:
        failed.append("missing_opend_paper_tickets")
    if failed:
        return DryRunEvidenceResult(execution_log_path, broker_report_path, 0, tuple(failed))

    now = datetime.now(UTC).isoformat()
    execution_rows: list[dict[str, object]] = []
    broker_rows: list[dict[str, object]] = []
    for ticket in tickets:
        payload = ticket.get("web_normal_order_payload")
        risk = ticket.get("risk")
        if not isinstance(payload, dict) or not isinstance(risk, dict):
            failed.append("invalid_opend_ticket_payload")
            continue
        if ticket.get("real") is not False or ticket.get("submit_real") is not False:
            failed.append("opend_ticket_not_dry_run")
            continue
        order_id = str(ticket.get("ticket_id") or f"dry-run-{len(execution_rows) + 1:03d}")
        quantity = float(payload.get("shares") or 0)
        fill_price = float(payload.get("limit_price") or risk.get("reference_price") or 0)
        execution_rows.append(
            {
                "order_id": order_id,
                "status": "filled",
                "quantity": quantity,
                "fill_price": fill_price,
                "slippage": 0.0,
                "symbol": payload.get("symbol"),
                "side": str(payload.get("side") or "").lower(),
                "source": "opend_ticket_dry_run",
                "dry_run": True,
                "created_at": now,
            }
        )
        broker_rows.append(
            {
                "local_order_id": order_id,
                "order_id": f"dry-{order_id}",
                "order_status": "FILLED_ALL",
                "dealt_qty": quantity,
                "dealt_avg_price": fill_price,
                "source": "simulated_opend_dry_run_report",
                "dry_run": True,
            }
        )

    if failed:
        return DryRunEvidenceResult(execution_log_path, broker_report_path, 0, tuple(failed))

    execution_log_path.parent.mkdir(parents=True, exist_ok=True)
    broker_report_path.parent.mkdir(parents=True, exist_ok=True)
    with execution_log_path.open("w", encoding="utf-8") as handle:
        for row in execution_rows:
            handle.write(json.dumps(row, ensure_ascii=True, sort_keys=True) + "\n")
    broker_report_path.write_text(
        json.dumps(broker_rows, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return DryRunEvidenceResult(
        execution_log_path=execution_log_path,
        broker_report_path=broker_report_path,
        order_count=len(execution_rows),
        failed_reasons=(),
    )
