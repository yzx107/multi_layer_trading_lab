from __future__ import annotations

import json
from pathlib import Path

import polars as pl

from multi_layer_trading_lab.execution.reconciliation import BrokerExecutionReport

FUTU_STATUS_MAP = {
    "FILLED_ALL": "filled",
    "FILLED_PART": "partial_filled",
    "SUBMITTED": "submitted",
    "CANCELLED_ALL": "cancelled",
    "FAILED": "rejected",
}


def futu_order_report_to_execution_report(row: dict[str, object]) -> BrokerExecutionReport:
    order_id = str(row.get("local_order_id") or row.get("order_id") or "")
    broker_order_id = row.get("order_id")
    raw_status = str(row.get("order_status") or row.get("status") or "")
    status = FUTU_STATUS_MAP.get(raw_status, raw_status.lower())
    filled_quantity = float(row.get("dealt_qty") or row.get("filled_quantity") or 0.0)
    price_value = row.get("dealt_avg_price") or row.get("fill_price")
    fill_price = float(price_value) if price_value is not None else None
    return BrokerExecutionReport(
        order_id=order_id,
        status=status,
        filled_quantity=filled_quantity,
        fill_price=fill_price,
        broker_order_id=str(broker_order_id) if broker_order_id is not None else None,
    )


def futu_order_reports_to_execution_reports(
    rows: list[dict[str, object]],
) -> list[BrokerExecutionReport]:
    return [futu_order_report_to_execution_report(row) for row in rows]


def load_futu_order_report_rows(path: Path) -> list[dict[str, object]]:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pl.read_csv(path).to_dicts()
    if suffix == ".jsonl":
        rows: list[dict[str, object]] = []
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                stripped = line.strip()
                if stripped:
                    rows.append(json.loads(stripped))
        return rows
    if suffix == ".json":
        payload = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(payload, list):
            return payload
        if isinstance(payload, dict) and isinstance(payload.get("rows"), list):
            return payload["rows"]
    raise ValueError(f"unsupported Futu report format: {suffix}")


def extract_futu_order_report_rows_from_web_log(path: Path) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            stripped = line.strip()
            if stripped:
                rows.extend(_extract_rows_from_web_event(json.loads(stripped)))
    return _latest_by_order_id(rows)


def extract_futu_order_report_rows_from_ticket_responses(
    path: Path,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            stripped = line.strip()
            if not stripped:
                continue
            event = json.loads(stripped)
            rows.extend(_extract_rows_from_ticket_response_event(event))
    return _latest_by_order_id(rows)


def write_futu_order_report_rows(path: Path, rows: list[dict[str, object]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(rows, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _extract_rows_from_web_event(event: dict[str, object]) -> list[dict[str, object]]:
    event_type = str(event.get("event") or "")
    if event_type in {"order_response", "order_query"}:
        return _rows_from_payload(event.get("data") or event.get("payload"))
    if event_type == "order_timeline":
        rows: list[dict[str, object]] = []
        timeline = event.get("timeline")
        if isinstance(timeline, list):
            for item in timeline:
                if isinstance(item, dict):
                    rows.extend(_rows_from_payload(item.get("data") or item.get("payload")))
        return rows
    if event_type == "web_normal_order_response":
        rows = _rows_from_payload(event.get("response"))
        timeline = event.get("timeline")
        if isinstance(timeline, list):
            for item in timeline:
                rows.extend(_rows_from_payload(item))
        return rows
    return []


def _extract_rows_from_ticket_response_event(event: dict[str, object]) -> list[dict[str, object]]:
    ticket_id = str(event.get("ticket_id") or "")
    response = event.get("response")
    if not isinstance(response, dict):
        return []

    rows: list[dict[str, object]] = []
    for key in ["order_response", "order_query", "payload", "data"]:
        rows.extend(_rows_from_payload(response.get(key)))
    timeline = response.get("timeline")
    if isinstance(timeline, list):
        for item in timeline:
            rows.extend(_rows_from_payload(item))
    if rows:
        normalized_rows = []
        for row in rows:
            normalized = dict(row)
            if ticket_id:
                normalized.setdefault("local_order_id", ticket_id)
                normalized.setdefault("remark", ticket_id)
            if event.get("dry_run") is True:
                normalized["dry_run"] = True
            normalized_rows.append(normalized)
        return normalized_rows

    intent = response.get("intent")
    request = event.get("request")
    if isinstance(intent, dict) or isinstance(request, dict):
        source = intent if isinstance(intent, dict) else request
        assert isinstance(source, dict)
        return [
            {
                "local_order_id": ticket_id,
                "order_id": ticket_id,
                "order_status": "DRY_RUN" if event.get("dry_run") is True else "SUBMITTED",
                "dealt_qty": source.get("quantity") or source.get("shares") or 0,
                "dealt_avg_price": source.get("limit_price") or source.get("price"),
                "remark": ticket_id,
                "dry_run": event.get("dry_run") is True,
            }
        ]
    return []


def _rows_from_payload(payload: object) -> list[dict[str, object]]:
    if isinstance(payload, dict):
        return [payload]
    if isinstance(payload, list):
        rows: list[dict[str, object]] = []
        for item in payload:
            if isinstance(item, dict):
                rows.append(item)
            elif isinstance(item, list):
                rows.extend(row for row in item if isinstance(row, dict))
        return rows
    return []


def _latest_by_order_id(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    latest: dict[str, dict[str, object]] = {}
    for row in rows:
        order_id = str(row.get("order_id") or "")
        if not order_id:
            continue
        normalized = dict(row)
        remark = str(normalized.get("remark") or "")
        if remark:
            normalized.setdefault("local_order_id", remark)
        latest[order_id] = normalized
    return list(latest.values())
