from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from multi_layer_trading_lab.adapters.futu.reports import load_futu_order_report_rows
from multi_layer_trading_lab.execution.reconciliation import load_execution_log


@dataclass(frozen=True, slots=True)
class CombinedPaperEvidenceResult:
    execution_log_path: Path
    broker_report_path: Path
    execution_log_rows: int
    broker_report_rows: int
    failed_reasons: tuple[str, ...]

    @property
    def ready(self) -> bool:
        return not self.failed_reasons


def combine_paper_evidence_files(
    *,
    execution_log_paths: tuple[Path, ...],
    broker_report_paths: tuple[Path, ...],
    output_execution_log_path: Path,
    output_broker_report_path: Path,
) -> CombinedPaperEvidenceResult:
    failed: list[str] = []
    execution_rows: list[dict[str, object]] = []
    broker_rows: list[dict[str, object]] = []

    for path in execution_log_paths:
        if not path.exists():
            failed.append(f"missing_execution_log:{path}")
            continue
        rows = load_execution_log(path)
        if not rows:
            failed.append(f"empty_execution_log:{path}")
        execution_rows.extend(rows)

    for path in broker_report_paths:
        if not path.exists():
            failed.append(f"missing_broker_report:{path}")
            continue
        rows = load_futu_order_report_rows(path)
        if not rows:
            failed.append(f"empty_broker_report:{path}")
        broker_rows.extend(rows)

    if any(row.get("dry_run") is True for row in execution_rows):
        failed.append("dry_run_execution_log_rows_present")
    if any(row.get("dry_run") is True for row in broker_rows):
        failed.append("dry_run_broker_report_rows_present")

    execution_ids = _duplicate_ids(
        str(row.get("order_id") or "") for row in execution_rows
    )
    broker_ids = _duplicate_ids(
        str(row.get("local_order_id") or row.get("remark") or row.get("order_id") or "")
        for row in broker_rows
    )
    failed.extend(f"duplicate_execution_order_id:{order_id}" for order_id in execution_ids)
    failed.extend(f"duplicate_broker_order_id:{order_id}" for order_id in broker_ids)

    if not execution_rows:
        failed.append("missing_combined_execution_rows")
    if not broker_rows:
        failed.append("missing_combined_broker_rows")

    failed = list(dict.fromkeys(failed))
    if failed:
        return CombinedPaperEvidenceResult(
            execution_log_path=output_execution_log_path,
            broker_report_path=output_broker_report_path,
            execution_log_rows=len(execution_rows),
            broker_report_rows=len(broker_rows),
            failed_reasons=tuple(failed),
        )

    output_execution_log_path.parent.mkdir(parents=True, exist_ok=True)
    with output_execution_log_path.open("w", encoding="utf-8") as handle:
        for row in sorted(execution_rows, key=_execution_sort_key):
            handle.write(json.dumps(row, ensure_ascii=True, sort_keys=True) + "\n")

    output_broker_report_path.parent.mkdir(parents=True, exist_ok=True)
    output_broker_report_path.write_text(
        json.dumps(sorted(broker_rows, key=_broker_sort_key), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    return CombinedPaperEvidenceResult(
        execution_log_path=output_execution_log_path,
        broker_report_path=output_broker_report_path,
        execution_log_rows=len(execution_rows),
        broker_report_rows=len(broker_rows),
        failed_reasons=(),
    )


def _duplicate_ids(values) -> tuple[str, ...]:
    seen: set[str] = set()
    duplicates: set[str] = set()
    for value in values:
        if not value:
            continue
        if value in seen:
            duplicates.add(value)
        seen.add(value)
    return tuple(sorted(duplicates))


def _execution_sort_key(row: dict[str, object]) -> tuple[str, str]:
    return (
        str(row.get("trade_date") or row.get("broker_time") or row.get("created_at") or ""),
        str(row.get("order_id") or ""),
    )


def _broker_sort_key(row: dict[str, object]) -> tuple[str, str]:
    return (
        str(row.get("updated_time") or row.get("dealt_time") or row.get("create_time") or ""),
        str(row.get("local_order_id") or row.get("remark") or row.get("order_id") or ""),
    )
