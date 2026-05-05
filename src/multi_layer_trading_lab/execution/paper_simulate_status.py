from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from multi_layer_trading_lab.adapters.futu.reports import (
    extract_futu_order_report_rows_from_ticket_responses,
)


@dataclass(frozen=True, slots=True)
class PaperSimulateStatus:
    response_path: Path
    response_rows: int
    paper_rows: int
    dry_run_rows: int
    submitted_rows: int
    order_report_rows: int
    failed_reasons: tuple[str, ...]

    @property
    def ready_for_session_collection(self) -> bool:
        return not self.failed_reasons

    def to_dict(self) -> dict[str, object]:
        return {
            "response_path": str(self.response_path),
            "response_rows": self.response_rows,
            "paper_rows": self.paper_rows,
            "dry_run_rows": self.dry_run_rows,
            "submitted_rows": self.submitted_rows,
            "order_report_rows": self.order_report_rows,
            "ready_for_session_collection": self.ready_for_session_collection,
            "failed_reasons": list(self.failed_reasons),
        }


def inspect_paper_simulate_responses(response_path: Path) -> PaperSimulateStatus:
    failed: list[str] = []
    rows: list[dict[str, object]] = []
    if not response_path.exists():
        failed.append("missing_opend_ticket_response")
    else:
        rows = _load_jsonl(response_path)
        if not rows:
            failed.append("empty_opend_ticket_response")

    paper_rows = sum(1 for row in rows if row.get("paper") is True)
    dry_run_rows = sum(1 for row in rows if row.get("dry_run") is True)
    submitted_rows = sum(1 for row in rows if _response_submitted(row))
    order_report_rows = (
        len(extract_futu_order_report_rows_from_ticket_responses(response_path))
        if rows
        else 0
    )

    if rows and paper_rows == 0:
        failed.append("missing_paper_simulate_responses")
    if dry_run_rows:
        failed.append("dry_run_response_rows_present")
    if rows and submitted_rows == 0:
        failed.append("missing_submitted_responses")
    if rows and order_report_rows == 0:
        failed.append("missing_futu_order_report_rows")

    return PaperSimulateStatus(
        response_path=response_path,
        response_rows=len(rows),
        paper_rows=paper_rows,
        dry_run_rows=dry_run_rows,
        submitted_rows=submitted_rows,
        order_report_rows=order_report_rows,
        failed_reasons=tuple(dict.fromkeys(failed)),
    )


def write_paper_simulate_status(
    *,
    response_path: Path,
    output_path: Path,
) -> PaperSimulateStatus:
    status = inspect_paper_simulate_responses(response_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(status.to_dict(), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return status


def _load_jsonl(path: Path) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            stripped = line.strip()
            if not stripped:
                continue
            payload = json.loads(stripped)
            if isinstance(payload, dict):
                rows.append(payload)
    return rows


def _response_submitted(event: dict[str, object]) -> bool:
    response = event.get("response")
    return isinstance(response, dict) and response.get("submitted") is True
