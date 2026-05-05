from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

from multi_layer_trading_lab.execution.session_ledger import (
    PaperSessionLedger,
    build_paper_session_ledger,
)


@dataclass(frozen=True, slots=True)
class PaperProgress:
    execution_log_path: Path
    broker_report_path: Path
    target_sessions: int
    inferred_session_count: int
    sessions_remaining: int
    dry_run_rows: int
    execution_log_rows: int
    broker_report_rows: int
    session_dates: tuple[str, ...]
    profitability_ready: bool | None
    net_pnl: float | None
    max_drawdown: float | None
    cash_drawdown: float | None
    reconciled: bool | None
    ready_for_live_review: bool
    next_required_evidence: tuple[str, ...]
    failed_reasons: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "execution_log_path": str(self.execution_log_path),
            "broker_report_path": str(self.broker_report_path),
            "target_sessions": self.target_sessions,
            "inferred_session_count": self.inferred_session_count,
            "sessions_remaining": self.sessions_remaining,
            "dry_run_rows": self.dry_run_rows,
            "execution_log_rows": self.execution_log_rows,
            "broker_report_rows": self.broker_report_rows,
            "session_dates": list(self.session_dates),
            "profitability_ready": self.profitability_ready,
            "net_pnl": self.net_pnl,
            "max_drawdown": self.max_drawdown,
            "cash_drawdown": self.cash_drawdown,
            "reconciled": self.reconciled,
            "ready_for_live_review": self.ready_for_live_review,
            "next_required_evidence": list(self.next_required_evidence),
            "failed_reasons": list(self.failed_reasons),
        }


@dataclass(frozen=True, slots=True)
class PaperSessionCalendar:
    execution_log_path: Path
    broker_report_path: Path
    as_of_date: str
    target_sessions: int
    inferred_session_count: int
    sessions_remaining: int
    has_session_today: bool
    last_session_date: str | None
    next_required_action: str
    failed_reasons: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "execution_log_path": str(self.execution_log_path),
            "broker_report_path": str(self.broker_report_path),
            "as_of_date": self.as_of_date,
            "target_sessions": self.target_sessions,
            "inferred_session_count": self.inferred_session_count,
            "sessions_remaining": self.sessions_remaining,
            "has_session_today": self.has_session_today,
            "last_session_date": self.last_session_date,
            "next_required_action": self.next_required_action,
            "failed_reasons": list(self.failed_reasons),
        }


def build_paper_progress(
    *,
    execution_log_path: Path,
    broker_report_path: Path,
    profitability_evidence_path: Path | None = None,
    target_sessions: int = 20,
) -> PaperProgress:
    ledger = build_paper_session_ledger(
        execution_log_path=execution_log_path,
        broker_report_path=broker_report_path,
    )
    failed = list(ledger.failed_reasons)
    sessions_remaining = max(0, target_sessions - ledger.inferred_session_count)
    if sessions_remaining:
        failed.append("paper_sessions_remaining")

    profitability = _load_profitability(profitability_evidence_path)
    profitability_ready = None
    net_pnl = None
    max_drawdown = None
    cash_drawdown = None
    reconciled = None
    if profitability is None:
        failed.append("missing_profitability_evidence")
    else:
        profitability_ready = profitability.get("ready") is True
        net_pnl = _optional_float(profitability.get("net_pnl"))
        max_drawdown = _optional_float(profitability.get("max_drawdown"))
        cash_drawdown = _optional_float(profitability.get("cash_drawdown"))
        reconciled = profitability.get("reconciled") is True
        failed.extend(_profitability_consistency_failures(profitability, ledger))
        if not profitability_ready:
            failed.extend(str(reason) for reason in profitability.get("failed_reasons", []))

    failed = list(dict.fromkeys(failed))
    ready = (
        ledger.inferred_session_count >= target_sessions
        and ledger.dry_run_rows == 0
        and profitability_ready is True
        and not failed
    )
    next_required_evidence = _next_required_paper_evidence(
        ledger=ledger,
        sessions_remaining=sessions_remaining,
        profitability_present=profitability is not None,
        profitability_ready=profitability_ready,
        net_pnl=net_pnl,
        reconciled=reconciled,
        failed=failed,
    )
    return PaperProgress(
        execution_log_path=execution_log_path,
        broker_report_path=broker_report_path,
        target_sessions=target_sessions,
        inferred_session_count=ledger.inferred_session_count,
        sessions_remaining=sessions_remaining,
        dry_run_rows=ledger.dry_run_rows,
        execution_log_rows=ledger.execution_log_rows,
        broker_report_rows=ledger.broker_report_rows,
        session_dates=ledger.session_dates,
        profitability_ready=profitability_ready,
        net_pnl=net_pnl,
        max_drawdown=max_drawdown,
        cash_drawdown=cash_drawdown,
        reconciled=reconciled,
        ready_for_live_review=ready,
        next_required_evidence=next_required_evidence,
        failed_reasons=tuple(failed),
    )


def build_paper_session_calendar(
    *,
    execution_log_path: Path,
    broker_report_path: Path,
    as_of_date: date | str | None = None,
    target_sessions: int = 20,
) -> PaperSessionCalendar:
    ledger = build_paper_session_ledger(
        execution_log_path=execution_log_path,
        broker_report_path=broker_report_path,
    )
    resolved_as_of_date = _coerce_date(as_of_date)
    as_of = resolved_as_of_date.isoformat()
    sessions_remaining = max(0, target_sessions - ledger.inferred_session_count)
    session_dates = tuple(date_text for date_text in ledger.session_dates if date_text <= as_of)
    has_session_today = as_of in session_dates
    last_session_date = session_dates[-1] if session_dates else None
    failed = [
        reason
        for reason in ledger.failed_reasons
        if reason
        not in {
            "insufficient_inferred_sessions",
        }
    ]
    if sessions_remaining == 0:
        next_action = "target_complete"
    elif has_session_today:
        next_action = "wait_next_trade_date"
    else:
        next_action = "collect_today_paper_session"
    return PaperSessionCalendar(
        execution_log_path=execution_log_path,
        broker_report_path=broker_report_path,
        as_of_date=as_of,
        target_sessions=target_sessions,
        inferred_session_count=ledger.inferred_session_count,
        sessions_remaining=sessions_remaining,
        has_session_today=has_session_today,
        last_session_date=last_session_date,
        next_required_action=next_action,
        failed_reasons=tuple(dict.fromkeys(failed)),
    )


def write_paper_session_calendar(
    *,
    execution_log_path: Path,
    broker_report_path: Path,
    output_path: Path,
    as_of_date: date | str | None = None,
    target_sessions: int = 20,
) -> PaperSessionCalendar:
    calendar = build_paper_session_calendar(
        execution_log_path=execution_log_path,
        broker_report_path=broker_report_path,
        as_of_date=as_of_date,
        target_sessions=target_sessions,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(calendar.to_dict(), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return calendar


def write_paper_progress(
    *,
    execution_log_path: Path,
    broker_report_path: Path,
    output_path: Path,
    profitability_evidence_path: Path | None = None,
    target_sessions: int = 20,
) -> PaperProgress:
    progress = build_paper_progress(
        execution_log_path=execution_log_path,
        broker_report_path=broker_report_path,
        profitability_evidence_path=profitability_evidence_path,
        target_sessions=target_sessions,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(progress.to_dict(), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return progress


def _load_profitability(path: Path | None) -> dict[str, object] | None:
    if path is None or not path.exists():
        return None
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else None


def _optional_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _profitability_consistency_failures(
    profitability: dict[str, object],
    ledger: PaperSessionLedger,
) -> list[str]:
    failed: list[str] = []
    paper_sessions = _optional_int(profitability.get("paper_sessions"))
    inferred_sessions = _optional_int(profitability.get("inferred_session_count"))
    if paper_sessions is None:
        failed.append("missing_profitability_paper_sessions")
    elif paper_sessions != ledger.inferred_session_count:
        failed.append("profitability_session_count_mismatch")
    if inferred_sessions is None:
        failed.append("missing_profitability_inferred_session_count")
    elif inferred_sessions != ledger.inferred_session_count:
        failed.append("profitability_inferred_session_count_mismatch")
    if (
        paper_sessions is not None
        and inferred_sessions is not None
        and paper_sessions != inferred_sessions
    ):
        failed.append("profitability_session_count_internal_mismatch")

    session_dates = profitability.get("session_dates")
    if not isinstance(session_dates, list):
        failed.append("missing_profitability_session_dates")
    else:
        profitability_dates = tuple(sorted(str(item) for item in session_dates))
        if profitability_dates != ledger.session_dates:
            failed.append("profitability_session_dates_mismatch")

    execution_rows = _optional_int(profitability.get("execution_log_rows"))
    broker_rows = _optional_int(profitability.get("broker_report_rows"))
    if execution_rows is None:
        failed.append("missing_profitability_execution_log_rows")
    elif execution_rows != ledger.execution_log_rows:
        failed.append("profitability_execution_log_rows_mismatch")
    if broker_rows is None:
        failed.append("missing_profitability_broker_report_rows")
    elif broker_rows != ledger.broker_report_rows:
        failed.append("profitability_broker_report_rows_mismatch")
    return failed


def _optional_int(value: object) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _next_required_paper_evidence(
    *,
    ledger: PaperSessionLedger,
    sessions_remaining: int,
    profitability_present: bool,
    profitability_ready: bool | None,
    net_pnl: float | None,
    reconciled: bool | None,
    failed: list[str],
) -> tuple[str, ...]:
    actions: list[str] = []
    if "missing_execution_log" in failed:
        actions.append("create_or_import_paper_execution_log")
    if "missing_broker_report" in failed:
        actions.append("export_futu_broker_report")
    if "empty_execution_log" in failed:
        actions.append("append_non_dry_run_paper_execution_rows")
    if "empty_broker_report" in failed:
        actions.append("append_matching_futu_broker_report_rows")
    if ledger.dry_run_rows:
        actions.append("remove_dry_run_rows_from_paper_evidence")
    if _has_session_date_gap(failed):
        actions.append("reconcile_execution_and_broker_session_dates")
    if sessions_remaining:
        actions.append(f"collect_{sessions_remaining}_broker_reconciled_paper_sessions")
    if not profitability_present:
        actions.append("build_profitability_evidence")
    elif _has_profitability_consistency_gap(failed):
        actions.append("refresh_profitability_evidence_from_latest_ledger")
    if reconciled is False:
        actions.append("reconcile_profitability_evidence_to_broker")
    if net_pnl is not None and net_pnl <= 0:
        actions.append("continue_until_positive_reconciled_net_pnl")
    if profitability_ready is False and "net_pnl_not_positive" not in failed:
        actions.append("resolve_profitability_evidence_failures")
    return tuple(dict.fromkeys(actions))


def _has_session_date_gap(failed: list[str]) -> bool:
    return any(
        reason in failed
        for reason in (
            "missing_execution_session_dates",
            "missing_broker_session_dates",
            "missing_broker_backed_session_dates",
            "execution_session_dates_missing_broker_report",
            "broker_session_dates_missing_execution_log",
        )
    )


def _has_profitability_consistency_gap(failed: list[str]) -> bool:
    return any(
        reason.startswith("profitability_") or reason.startswith("missing_profitability_")
        for reason in failed
    )


def _coerce_date(value: date | str | None) -> date:
    if value is None:
        return date.today()
    if isinstance(value, date):
        return value
    text = value.strip()
    if not text:
        return date.today()
    return datetime.fromisoformat(text).date()
