from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True, slots=True)
class BrokerExecutionReport:
    order_id: str
    status: str
    filled_quantity: float = 0.0
    fill_price: float | None = None
    broker_order_id: str | None = None


@dataclass(frozen=True, slots=True)
class ReconciliationBreak:
    order_id: str
    reason: str
    local_value: str | None = None
    broker_value: str | None = None


@dataclass(frozen=True, slots=True)
class ReconciliationResult:
    matched_orders: int
    breaks: tuple[ReconciliationBreak, ...] = field(default_factory=tuple)

    @property
    def clean(self) -> bool:
        return not self.breaks


def load_execution_log(path: Path) -> list[dict[str, object]]:
    if not path.exists():
        return []
    rows: list[dict[str, object]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            stripped = line.strip()
            if stripped:
                rows.append(json.loads(stripped))
    return rows


def reconcile_execution_reports(
    local_records: list[dict[str, object]],
    broker_reports: list[BrokerExecutionReport],
    *,
    price_tolerance: float = 0.01,
) -> ReconciliationResult:
    local_by_order = {str(record["order_id"]): record for record in local_records}
    broker_by_order = {report.order_id: report for report in broker_reports}
    breaks: list[ReconciliationBreak] = []

    for order_id, local in local_by_order.items():
        broker = broker_by_order.get(order_id)
        if broker is None:
            breaks.append(ReconciliationBreak(order_id=order_id, reason="missing_broker_report"))
            continue
        if str(local.get("status")) != broker.status:
            breaks.append(
                ReconciliationBreak(
                    order_id=order_id,
                    reason="status_mismatch",
                    local_value=str(local.get("status")),
                    broker_value=broker.status,
                )
            )
        local_qty = float(local.get("quantity") or 0.0)
        if abs(local_qty - broker.filled_quantity) > 1e-9:
            breaks.append(
                ReconciliationBreak(
                    order_id=order_id,
                    reason="quantity_mismatch",
                    local_value=str(local_qty),
                    broker_value=str(broker.filled_quantity),
                )
            )
        local_price = local.get("fill_price")
        if local_price is not None and broker.fill_price is not None:
            if abs(float(local_price) - broker.fill_price) > price_tolerance:
                breaks.append(
                    ReconciliationBreak(
                        order_id=order_id,
                        reason="fill_price_mismatch",
                        local_value=str(local_price),
                        broker_value=str(broker.fill_price),
                    )
                )

    for order_id in broker_by_order:
        if order_id not in local_by_order:
            breaks.append(ReconciliationBreak(order_id=order_id, reason="missing_local_record"))

    matched = len(set(local_by_order).intersection(broker_by_order))
    return ReconciliationResult(matched_orders=matched, breaks=tuple(breaks))
