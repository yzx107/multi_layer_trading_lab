from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class PaperBlockerReport:
    runtime_status_path: Path | None
    paper_simulate_status_path: Path | None
    paper_calendar_path: Path | None
    paper_progress_path: Path | None
    ready_for_next_session: bool
    next_required_action: str | None
    sessions_remaining: int | None
    failed_reasons: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "runtime_status_path": str(self.runtime_status_path)
            if self.runtime_status_path
            else None,
            "paper_simulate_status_path": str(self.paper_simulate_status_path)
            if self.paper_simulate_status_path
            else None,
            "paper_calendar_path": str(self.paper_calendar_path)
            if self.paper_calendar_path
            else None,
            "paper_progress_path": str(self.paper_progress_path)
            if self.paper_progress_path
            else None,
            "ready_for_next_session": self.ready_for_next_session,
            "next_required_action": self.next_required_action,
            "sessions_remaining": self.sessions_remaining,
            "failed_reasons": list(self.failed_reasons),
        }


def build_paper_blocker_report(
    *,
    runtime_status_path: Path | None = None,
    paper_simulate_status_path: Path | None = None,
    paper_calendar_path: Path | None = None,
    paper_progress_path: Path | None = None,
) -> PaperBlockerReport:
    failed: list[str] = []
    runtime = _load_optional_json(runtime_status_path)
    paper_status = _load_optional_json(paper_simulate_status_path)
    calendar = _load_optional_json(paper_calendar_path)
    progress = _load_optional_json(paper_progress_path)

    if runtime_status_path is not None and runtime is None:
        failed.append("missing_opend_runtime_status")
    if paper_simulate_status_path is not None and paper_status is None:
        failed.append("missing_paper_simulate_status")
    if paper_calendar_path is not None and calendar is None:
        failed.append("missing_paper_session_calendar")
    if paper_progress_path is not None and progress is None:
        failed.append("missing_paper_progress")

    if runtime is not None and runtime.get("ready_for_order_submission") is not True:
        failed.extend(str(reason) for reason in runtime.get("failed_reasons", []))
    if paper_status is not None and paper_status.get("ready_for_session_collection") is not True:
        failed.extend(str(reason) for reason in paper_status.get("failed_reasons", []))
    next_action = (
        str(calendar.get("next_required_action"))
        if calendar is not None and calendar.get("next_required_action")
        else None
    )
    if next_action and next_action != "collect_today_paper_session":
        failed.append(f"paper_calendar_action:{next_action}")
    sessions_remaining = _optional_int(progress.get("sessions_remaining")) if progress else None
    if progress is not None and progress.get("ready_for_live_review") is not True:
        failed.extend(str(reason) for reason in progress.get("failed_reasons", []))

    failed = tuple(dict.fromkeys(reason for reason in failed if reason))
    return PaperBlockerReport(
        runtime_status_path=runtime_status_path,
        paper_simulate_status_path=paper_simulate_status_path,
        paper_calendar_path=paper_calendar_path,
        paper_progress_path=paper_progress_path,
        ready_for_next_session=not failed,
        next_required_action=next_action,
        sessions_remaining=sessions_remaining,
        failed_reasons=failed,
    )


def write_paper_blocker_report(
    *,
    output_path: Path,
    runtime_status_path: Path | None = None,
    paper_simulate_status_path: Path | None = None,
    paper_calendar_path: Path | None = None,
    paper_progress_path: Path | None = None,
) -> PaperBlockerReport:
    report = build_paper_blocker_report(
        runtime_status_path=runtime_status_path,
        paper_simulate_status_path=paper_simulate_status_path,
        paper_calendar_path=paper_calendar_path,
        paper_progress_path=paper_progress_path,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(report.to_dict(), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return report


def _load_optional_json(path: Path | None) -> dict[str, object] | None:
    if path is None or not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else None


def _optional_int(value: object) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
