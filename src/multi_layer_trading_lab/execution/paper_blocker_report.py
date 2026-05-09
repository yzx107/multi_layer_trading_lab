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
    ready_for_live_review: bool | None
    next_required_action: str | None
    next_required_evidence: tuple[str, ...]
    next_collect_date: str | None
    sessions_remaining: int | None
    failed_reasons: tuple[str, ...]
    next_session_failed_reasons: tuple[str, ...]
    blocker_details: dict[str, object]

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
            "ready_for_live_review": self.ready_for_live_review,
            "next_required_action": self.next_required_action,
            "next_required_evidence": list(self.next_required_evidence),
            "next_collect_date": self.next_collect_date,
            "sessions_remaining": self.sessions_remaining,
            "failed_reasons": list(self.failed_reasons),
            "next_session_failed_reasons": list(self.next_session_failed_reasons),
            "blocker_details": self.blocker_details,
        }


def build_paper_blocker_report(
    *,
    runtime_status_path: Path | None = None,
    paper_simulate_status_path: Path | None = None,
    paper_calendar_path: Path | None = None,
    paper_progress_path: Path | None = None,
) -> PaperBlockerReport:
    failed: list[str] = []
    next_session_failed: list[str] = []
    runtime = _load_optional_json(runtime_status_path)
    paper_status = _load_optional_json(paper_simulate_status_path)
    calendar = _load_optional_json(paper_calendar_path)
    progress = _load_optional_json(paper_progress_path)
    paper_status_stale = _is_older_than(
        paper_simulate_status_path,
        runtime_status_path,
    )

    if runtime_status_path is not None and runtime is None:
        failed.append("missing_opend_runtime_status")
        next_session_failed.append("missing_opend_runtime_status")
    if paper_simulate_status_path is not None and paper_status is None:
        failed.append("missing_paper_simulate_status")
        next_session_failed.append("missing_paper_simulate_status")
    if paper_status_stale:
        failed.append("stale_paper_simulate_status")
        next_session_failed.append("stale_paper_simulate_status")
    if paper_calendar_path is not None and calendar is None:
        failed.append("missing_paper_session_calendar")
        next_session_failed.append("missing_paper_session_calendar")
    if paper_progress_path is not None and progress is None:
        failed.append("missing_paper_progress")

    if runtime is not None and runtime.get("ready_for_order_submission") is not True:
        reasons = [str(reason) for reason in runtime.get("failed_reasons", [])]
        failed.extend(reasons)
        next_session_failed.extend(reasons)
    if runtime is not None and runtime.get("kill_switch") is True:
        failed.append("opend_kill_switch_enabled")
        next_session_failed.append("opend_kill_switch_enabled")
    if (
        paper_status is not None
        and not paper_status_stale
        and paper_status.get("ready_for_session_collection") is not True
    ):
        reasons = [str(reason) for reason in paper_status.get("failed_reasons", [])]
        failed.extend(reasons)
        next_session_failed.extend(reasons)
    calendar_next_action = (
        str(calendar.get("next_required_action"))
        if calendar is not None and calendar.get("next_required_action")
        else None
    )
    next_collect_date = _optional_str(calendar.get("next_collect_date")) if calendar else None
    paper_next_action = (
        str(paper_status.get("next_required_action"))
        if (
            paper_status is not None
            and not paper_status_stale
            and paper_status.get("next_required_action")
        )
        else None
    )
    next_action = _resolve_next_required_action(
        failed_reasons=failed,
        paper_next_action=paper_next_action,
        calendar_next_action=calendar_next_action,
    )
    if calendar_next_action and calendar_next_action != "collect_today_paper_session":
        reason = f"paper_calendar_action:{calendar_next_action}"
        failed.append(reason)
        next_session_failed.append(reason)
    sessions_remaining = _optional_int(progress.get("sessions_remaining")) if progress else None
    next_required_evidence = _next_required_evidence(progress)
    ready_for_live_review = (
        progress.get("ready_for_live_review") is True if progress is not None else None
    )
    if progress is not None and progress.get("ready_for_live_review") is not True:
        progress_reasons = [str(reason) for reason in progress.get("failed_reasons", [])]
        failed.extend(progress_reasons)
        next_session_failed.extend(
            reason
            for reason in progress_reasons
            if reason not in _PROGRESS_COMPLETION_ONLY_REASONS
        )

    failed = tuple(dict.fromkeys(reason for reason in failed if reason))
    next_session_failed_tuple = tuple(
        dict.fromkeys(reason for reason in next_session_failed if reason)
    )
    blocker_details = _blocker_details(runtime, failed)
    return PaperBlockerReport(
        runtime_status_path=runtime_status_path,
        paper_simulate_status_path=paper_simulate_status_path,
        paper_calendar_path=paper_calendar_path,
        paper_progress_path=paper_progress_path,
        ready_for_next_session=not next_session_failed_tuple,
        ready_for_live_review=ready_for_live_review,
        next_required_action=next_action,
        next_required_evidence=next_required_evidence,
        next_collect_date=next_collect_date,
        sessions_remaining=sessions_remaining,
        failed_reasons=failed,
        next_session_failed_reasons=next_session_failed_tuple,
        blocker_details=blocker_details,
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


_PROGRESS_COMPLETION_ONLY_REASONS = {
    "insufficient_inferred_sessions",
    "insufficient_inferred_paper_sessions",
    "insufficient_profitable_paper_sessions",
    "missing_profitability_broker_report_rows",
    "missing_profitability_evidence",
    "missing_profitability_execution_log_rows",
    "missing_profitability_inferred_session_count",
    "missing_profitability_paper_sessions",
    "missing_profitability_session_dates",
    "net_pnl_not_positive",
    "paper_sessions_remaining",
    "profitability_broker_report_rows_mismatch",
    "profitability_execution_log_rows_mismatch",
    "profitability_inferred_session_count_mismatch",
    "profitability_session_count_internal_mismatch",
    "profitability_session_count_mismatch",
    "profitability_session_dates_mismatch",
}


def _is_older_than(path: Path | None, reference_path: Path | None) -> bool:
    if path is None or reference_path is None:
        return False
    if not path.exists() or not reference_path.exists():
        return False
    return path.stat().st_mtime < reference_path.stat().st_mtime


def _optional_int(value: object) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if text else None


def _next_required_evidence(progress: dict[str, object] | None) -> tuple[str, ...]:
    if progress is None:
        return ()
    raw = progress.get("next_required_evidence")
    if not isinstance(raw, list):
        return ()
    return tuple(dict.fromkeys(str(item) for item in raw if str(item)))


def _blocker_details(
    runtime: dict[str, object] | None,
    failed_reasons: tuple[str, ...],
) -> dict[str, object]:
    details: dict[str, object] = {}
    kill_switch_enabled = runtime is not None and runtime.get("kill_switch") is True
    if "opend_kill_switch_enabled" in failed_reasons or kill_switch_enabled:
        kill_switch_file = runtime.get("kill_switch_file") if runtime else None
        details["opend_kill_switch"] = {
            "enabled": True,
            "kill_switch_file": str(kill_switch_file) if kill_switch_file else "",
            "requires_manual_operator_authorization": True,
            "automation_allowed": False,
            "next_safe_action": (
                "operator_must_explicitly_clear_kill_switch_before_resubmit"
            ),
        }
    return details


def _resolve_next_required_action(
    *,
    failed_reasons: list[str],
    paper_next_action: str | None,
    calendar_next_action: str | None,
) -> str | None:
    if "opend_kill_switch_enabled" in failed_reasons:
        return "clear_opend_kill_switch_then_resubmit_paper_simulate"
    if paper_next_action and paper_next_action != "collect_session_evidence":
        return paper_next_action
    return calendar_next_action or paper_next_action
