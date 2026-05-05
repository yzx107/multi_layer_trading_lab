from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True, slots=True)
class HshareVerifiedEvidence:
    ready: bool
    year: int | None
    status: str
    selected_date_count: int
    completed_count: int
    failed_count: int
    orders_rows: int
    trades_rows: int
    is_partial: bool
    summary_path: Path
    failed_reasons: tuple[str, ...]


def _read_summary(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def evaluate_hshare_verified_summary(
    summary_path: Path,
    *,
    min_selected_dates: int = 20,
    require_complete: bool = True,
    allow_partial: bool = False,
) -> HshareVerifiedEvidence:
    if not summary_path.exists():
        return HshareVerifiedEvidence(
            ready=False,
            year=None,
            status="missing",
            selected_date_count=0,
            completed_count=0,
            failed_count=0,
            orders_rows=0,
            trades_rows=0,
            is_partial=True,
            summary_path=summary_path,
            failed_reasons=("missing_hshare_verified_summary",),
        )

    payload = _read_summary(summary_path)
    selection = payload.get("selection", {})
    tables = payload.get("tables", {})
    orders = tables.get("verified_orders", {})
    trades = tables.get("verified_trades", {})
    status = str(payload.get("status", "unknown"))
    selected_date_count = int(selection.get("selected_date_count") or 0)
    completed_count = int(payload.get("completed_count") or 0)
    failed_count = int(payload.get("failed_count") or 0)
    is_partial = bool(selection.get("is_partial", True))
    orders_rows = int(orders.get("rows") or 0)
    trades_rows = int(trades.get("rows") or 0)

    failed: list[str] = []
    if require_complete and status != "completed":
        failed.append("hshare_verified_not_completed")
    if failed_count > 0:
        failed.append("hshare_verified_has_failed_tasks")
    if selected_date_count < min_selected_dates:
        failed.append("insufficient_hshare_verified_dates")
    if is_partial and not allow_partial:
        failed.append("hshare_verified_partial_selection")
    if orders_rows <= 0:
        failed.append("hshare_verified_orders_empty")
    if trades_rows <= 0:
        failed.append("hshare_verified_trades_empty")

    return HshareVerifiedEvidence(
        ready=not failed,
        year=int(payload["year"]) if payload.get("year") is not None else None,
        status=status,
        selected_date_count=selected_date_count,
        completed_count=completed_count,
        failed_count=failed_count,
        orders_rows=orders_rows,
        trades_rows=trades_rows,
        is_partial=is_partial,
        summary_path=summary_path,
        failed_reasons=tuple(failed),
    )
