from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

from multi_layer_trading_lab.execution.session_ledger import build_paper_session_ledger


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
    profitability_ready: bool | None
    net_pnl: float | None
    max_drawdown: float | None
    cash_drawdown: float | None
    reconciled: bool | None
    ready_for_live_review: bool
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
            "profitability_ready": self.profitability_ready,
            "net_pnl": self.net_pnl,
            "max_drawdown": self.max_drawdown,
            "cash_drawdown": self.cash_drawdown,
            "reconciled": self.reconciled,
            "ready_for_live_review": self.ready_for_live_review,
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
        if not profitability_ready:
            failed.extend(str(reason) for reason in profitability.get("failed_reasons", []))

    failed = list(dict.fromkeys(failed))
    ready = (
        ledger.inferred_session_count >= target_sessions
        and ledger.dry_run_rows == 0
        and profitability_ready is True
        and not failed
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
        profitability_ready=profitability_ready,
        net_pnl=net_pnl,
        max_drawdown=max_drawdown,
        cash_drawdown=cash_drawdown,
        reconciled=reconciled,
        ready_for_live_review=ready,
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


def _coerce_date(value: date | str | None) -> date:
    if value is None:
        return date.today()
    if isinstance(value, date):
        return value
    text = value.strip()
    if not text:
        return date.today()
    return datetime.fromisoformat(text).date()
