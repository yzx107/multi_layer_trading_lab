from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from multi_layer_trading_lab.adapters.futu.reports import load_futu_order_report_rows
from multi_layer_trading_lab.execution.opend_tickets import load_opend_paper_tickets

STATUS_MAP = {
    "FILLED_ALL": "filled",
    "FILLED_PART": "partial_filled",
    "SUBMITTED": "submitted",
    "CANCELLED_ALL": "cancelled",
    "CANCELLED_PART": "cancelled",
    "FAILED": "rejected",
}


@dataclass(frozen=True, slots=True)
class PaperExecutionLogResult:
    execution_log_path: Path
    rows: int
    failed_reasons: tuple[str, ...]

    @property
    def ready(self) -> bool:
        return not self.failed_reasons


def build_paper_execution_log_from_futu_report(
    ticket_path: Path,
    broker_report_path: Path,
    *,
    execution_log_path: Path,
) -> PaperExecutionLogResult:
    tickets = load_opend_paper_tickets(ticket_path)
    broker_rows = (
        load_futu_order_report_rows(broker_report_path) if broker_report_path.exists() else []
    )
    failed: list[str] = []
    if not tickets:
        failed.append("missing_opend_paper_tickets")
    if not broker_rows:
        failed.append("missing_futu_broker_report")
    if any(row.get("dry_run") is True for row in broker_rows):
        failed.append("dry_run_broker_report_not_real_paper")
    ticket_by_id = {str(ticket.get("ticket_id")): ticket for ticket in tickets}
    rows: list[dict[str, object]] = []
    now = datetime.now(UTC).isoformat()
    for broker in broker_rows:
        local_order_id = str(broker.get("local_order_id") or broker.get("remark") or "")
        if not local_order_id:
            failed.append("broker_report_missing_local_order_id")
            continue
        ticket = ticket_by_id.get(local_order_id)
        if ticket is None:
            failed.append(f"broker_report_unmatched_ticket:{local_order_id}")
            continue
        payload = ticket.get("web_normal_order_payload")
        if not isinstance(payload, dict):
            failed.append(f"invalid_ticket_payload:{local_order_id}")
            continue
        raw_status = str(broker.get("order_status") or broker.get("status") or "")
        status = STATUS_MAP.get(raw_status, raw_status.lower())
        quantity = float(broker.get("dealt_qty") or broker.get("filled_quantity") or 0.0)
        price_value = broker.get("dealt_avg_price") or broker.get("fill_price")
        fill_price = float(price_value) if price_value is not None else None
        broker_time = (
            broker.get("updated_time")
            or broker.get("dealt_time")
            or broker.get("created_time")
            or broker.get("create_time")
            or broker.get("trade_date")
        )
        rows.append(
            {
                "order_id": local_order_id,
                "broker_order_id": str(broker.get("order_id") or ""),
                "status": status,
                "quantity": quantity,
                "fill_price": fill_price,
                "slippage": 0.0,
                "symbol": payload.get("symbol"),
                "side": str(payload.get("side") or "").lower(),
                "source": "futu_opend_paper_report",
                "dry_run": False,
                "created_at": now,
                "broker_time": broker_time,
                "trade_date": _trade_date_from_broker_time(broker_time),
            }
        )

    if failed:
        return PaperExecutionLogResult(
            execution_log_path=execution_log_path,
            rows=0,
            failed_reasons=tuple(failed),
        )

    execution_log_path.parent.mkdir(parents=True, exist_ok=True)
    with execution_log_path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=True, sort_keys=True) + "\n")
    return PaperExecutionLogResult(
        execution_log_path=execution_log_path,
        rows=len(rows),
        failed_reasons=(),
    )


def _trade_date_from_broker_time(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if len(text) >= 10 and text[4] in {"-", "/"}:
        return text[:10].replace("/", "-")
    if len(text) == 8 and text.isdigit():
        return f"{text[:4]}-{text[4:6]}-{text[6:]}"
    return None
