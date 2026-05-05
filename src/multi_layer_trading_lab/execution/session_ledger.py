from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

from multi_layer_trading_lab.adapters.futu.reports import load_futu_order_report_rows
from multi_layer_trading_lab.execution.reconciliation import load_execution_log

DATE_FIELDS = (
    "trade_date",
    "created_at",
    "updated_at",
    "updated_time",
    "create_time",
    "created_time",
    "order_time",
    "dealt_time",
    "time",
)


@dataclass(frozen=True, slots=True)
class PaperSessionLedger:
    execution_log_path: Path
    broker_report_path: Path
    execution_log_rows: int
    broker_report_rows: int
    inferred_session_count: int
    session_dates: tuple[str, ...]
    dry_run_rows: int
    failed_reasons: tuple[str, ...]

    @property
    def ready_for_profitability_evidence(self) -> bool:
        return not self.failed_reasons and self.inferred_session_count >= 20

    def to_dict(self) -> dict[str, object]:
        return {
            "execution_log_path": str(self.execution_log_path),
            "broker_report_path": str(self.broker_report_path),
            "execution_log_rows": self.execution_log_rows,
            "broker_report_rows": self.broker_report_rows,
            "inferred_session_count": self.inferred_session_count,
            "session_dates": list(self.session_dates),
            "dry_run_rows": self.dry_run_rows,
            "ready_for_profitability_evidence": self.ready_for_profitability_evidence,
            "failed_reasons": list(self.failed_reasons),
        }


def build_paper_session_ledger(
    *,
    execution_log_path: Path,
    broker_report_path: Path,
) -> PaperSessionLedger:
    failed: list[str] = []
    local_records = []
    broker_rows = []
    if not execution_log_path.exists():
        failed.append("missing_execution_log")
    else:
        local_records = load_execution_log(execution_log_path)
    if not broker_report_path.exists():
        failed.append("missing_broker_report")
    else:
        broker_rows = load_futu_order_report_rows(broker_report_path)

    if execution_log_path.exists() and not local_records:
        failed.append("empty_execution_log")
    if broker_report_path.exists() and not broker_rows:
        failed.append("empty_broker_report")

    all_rows = [*local_records, *broker_rows]
    dry_run_rows = sum(1 for row in all_rows if row.get("dry_run") is True)
    if dry_run_rows:
        failed.append("dry_run_rows_present")

    dates = sorted(
        {
            parsed.isoformat()
            for row in all_rows
            if (parsed := _row_date(row)) is not None
        }
    )
    if all_rows and not dates:
        failed.append("missing_session_dates")
    if dates and len(dates) < 20:
        failed.append("insufficient_inferred_sessions")

    return PaperSessionLedger(
        execution_log_path=execution_log_path,
        broker_report_path=broker_report_path,
        execution_log_rows=len(local_records),
        broker_report_rows=len(broker_rows),
        inferred_session_count=len(dates),
        session_dates=tuple(dates),
        dry_run_rows=dry_run_rows,
        failed_reasons=tuple(dict.fromkeys(failed)),
    )


def write_paper_session_ledger(
    *,
    execution_log_path: Path,
    broker_report_path: Path,
    output_path: Path,
) -> PaperSessionLedger:
    ledger = build_paper_session_ledger(
        execution_log_path=execution_log_path,
        broker_report_path=broker_report_path,
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(ledger.to_dict(), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return ledger


def _row_date(row: dict[str, object]) -> date | None:
    for field in DATE_FIELDS:
        value = row.get(field)
        parsed = _parse_date(value)
        if parsed is not None:
            return parsed
    return None


def _parse_date(value: Any) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if not text:
        return None
    if len(text) == 8 and text.isdigit():
        return datetime.strptime(text, "%Y%m%d").date()
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).date()
    except ValueError:
        pass
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S", "%Y-%m-%d", "%Y/%m/%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None
