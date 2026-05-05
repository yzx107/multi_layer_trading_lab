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
    response_error_rows: int
    response_error_types: dict[str, int]
    response_error_samples: tuple[str, ...]
    blocked_by_kill_switch: bool
    next_required_action: str
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
            "response_error_rows": self.response_error_rows,
            "response_error_types": self.response_error_types,
            "response_error_samples": list(self.response_error_samples),
            "blocked_by_kill_switch": self.blocked_by_kill_switch,
            "next_required_action": self.next_required_action,
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
    response_errors = [_response_error(row) for row in rows]
    response_errors = [error for error in response_errors if error]
    response_error_types = _response_error_type_counts(response_errors)
    blocked_by_kill_switch = any("kill switch" in error.lower() for error in response_errors)
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
    if response_errors:
        failed.append("paper_simulate_submit_errors_present")
    if blocked_by_kill_switch:
        failed.append("opend_kill_switch_enabled")

    return PaperSimulateStatus(
        response_path=response_path,
        response_rows=len(rows),
        paper_rows=paper_rows,
        dry_run_rows=dry_run_rows,
        submitted_rows=submitted_rows,
        order_report_rows=order_report_rows,
        response_error_rows=len(response_errors),
        response_error_types=response_error_types,
        response_error_samples=tuple(response_errors[:3]),
        blocked_by_kill_switch=blocked_by_kill_switch,
        next_required_action=_next_required_action(
            rows=rows,
            dry_run_rows=dry_run_rows,
            submitted_rows=submitted_rows,
            blocked_by_kill_switch=blocked_by_kill_switch,
        ),
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


def _response_error(event: dict[str, object]) -> str | None:
    response = event.get("response")
    if not isinstance(response, dict):
        return None
    error = response.get("error")
    if error in {None, ""}:
        return None
    return str(error)


def _response_error_type_counts(errors: list[str]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for error in errors:
        error_type = _response_error_type(error)
        counts[error_type] = counts.get(error_type, 0) + 1
    return counts


def _response_error_type(error: str) -> str:
    parts = error.split(":", 3)
    if len(parts) >= 3:
        return ":".join(parts[:3])
    return parts[0] if parts else "unknown_error"


def _next_required_action(
    *,
    rows: list[dict[str, object]],
    dry_run_rows: int,
    submitted_rows: int,
    blocked_by_kill_switch: bool,
) -> str:
    if not rows:
        return "submit_paper_simulate_tickets"
    if dry_run_rows:
        return "rerun_with_submit_paper_simulate"
    if blocked_by_kill_switch:
        return "clear_opend_kill_switch_then_resubmit_paper_simulate"
    if submitted_rows == 0:
        return "resubmit_paper_simulate_tickets"
    return "collect_session_evidence"
