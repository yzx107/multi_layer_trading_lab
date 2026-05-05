from __future__ import annotations

import json
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import polars as pl

IFIND_EVENT_COLUMN_ALIASES = {
    "事件ID": "id",
    "事件编号": "id",
    "证券代码": "symbol",
    "股票代码": "symbol",
    "同花顺代码": "symbol",
    "代码": "symbol",
    "公告日期": "date",
    "事件日期": "date",
    "日期": "date",
    "发布时间": "datetime",
    "事件时间": "datetime",
    "事件类型": "type",
    "类型": "type",
    "标题": "title",
    "事件标题": "title",
    "公告标题": "title",
    "重要性": "importance",
    "重要程度": "importance",
}

IFIND_REPORT_QUERY_ENDPOINT = "https://quantapi.51ifind.com/api/v1/report_query"


@dataclass(slots=True)
class IFindClient:
    username: str | None = None
    password: str | None = None
    access_token: str | None = None
    refresh_token: str | None = None
    use_real: bool = False
    events_endpoint: str | None = None
    event_provider: Callable[..., Iterable[dict[str, Any]]] | None = None

    def fetch_events(
        self,
        symbols: list[str],
        start: date,
        end: date,
        event_types: list[str] | None = None,
    ) -> pl.DataFrame:
        if self.use_real:
            return self._fetch_real_events(symbols, start, end, event_types)
        requested_types = event_types or ["earnings", "company_notice"]
        rows: list[dict[str, object]] = []
        for idx, symbol in enumerate(symbols):
            event_date = start if idx % 2 == 0 else min(end, start)
            event_type = requested_types[idx % len(requested_types)]
            rows.append(
                {
                    "event_id": f"ifind-{symbol}-{event_date.isoformat()}-{event_type}",
                    "security_id": self._security_id_from_symbol(symbol),
                    "symbol": symbol,
                    "market": self._market_from_symbol(symbol),
                    "event_date": event_date,
                    "event_ts": datetime.combine(event_date, datetime.min.time(), tzinfo=UTC),
                    "event_type": event_type,
                    "event_title": f"{symbol} {event_type}",
                    "event_source": "ifind",
                    "importance": 3,
                    "data_source": "ifind_stub",
                    "source_dataset": "ifind_events",
                    "source_run_id": "demo-ifind-events",
                    "ingested_at": datetime(2026, 4, 1, 18, 0, tzinfo=UTC),
                }
            )
        return pl.DataFrame(rows)

    def _fetch_real_events(
        self,
        symbols: list[str],
        start: date,
        end: date,
        event_types: list[str] | None,
    ) -> pl.DataFrame:
        ingested_at = datetime.now(UTC)
        raw_events = list(self._load_raw_events(symbols, start, end, event_types))
        rows = [self._normalize_event(row, ingested_at) for row in raw_events]
        return pl.DataFrame(rows) if rows else self._empty_events()

    def load_events_file(
        self,
        path: Path,
        *,
        source_run_id: str | None = None,
    ) -> pl.DataFrame:
        ingested_at = datetime.now(UTC)
        raw_events = list(_load_ifind_event_rows(path))
        rows = [self._normalize_event(row, ingested_at) for row in raw_events]
        frame = pl.DataFrame(rows) if rows else self._empty_events()
        run_id = source_run_id or f"ifind-file-{path.stem}-{ingested_at:%Y%m%dT%H%M%S}"
        return frame.with_columns(
            pl.lit("ifind_real_file").alias("data_source"),
            pl.lit(str(path)).alias("source_dataset"),
            pl.lit(run_id).alias("source_run_id"),
        )

    @staticmethod
    def write_events_template(path: Path) -> Path:
        template = pl.DataFrame(
            {
                "id": ["evt-00700-20260401-company-notice"],
                "symbol": ["00700.HK"],
                "date": ["20260401"],
                "type": ["company_notice"],
                "title": ["00700.HK company notice"],
                "importance": [3],
            }
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        template.write_csv(path)
        return path

    def _load_raw_events(
        self,
        symbols: list[str],
        start: date,
        end: date,
        event_types: list[str] | None,
    ) -> Iterable[dict[str, Any]]:
        if self.event_provider is not None:
            return self.event_provider(
                symbols=symbols,
                start=start,
                end=end,
                event_types=event_types,
            )
        if not self.access_token:
            raise ValueError("IFIND_ACCESS_TOKEN is required for real iFind event access")
        try:
            import requests
        except ImportError as exc:
            raise RuntimeError("requests is required for real iFind HTTP access") from exc
        if self.events_endpoint:
            response = requests.post(
                self.events_endpoint,
                json={
                    "symbols": symbols,
                    "start": start.isoformat(),
                    "end": end.isoformat(),
                    "event_types": event_types or [],
                },
                headers={
                    "Authorization": f"Bearer {self.access_token}",
                    "X-IFind-Refresh-Token": self.refresh_token or "",
                },
                timeout=30,
            )
        else:
            response = requests.post(
                IFIND_REPORT_QUERY_ENDPOINT,
                json={
                    "codes": ",".join(symbols),
                    "functionpara": {"reportType": "901"},
                    "beginrDate": start.isoformat(),
                    "endrDate": end.isoformat(),
                    "outputpara": (
                        "reportDate:Y,thscode:Y,secName:Y,ctime:Y,"
                        "reportTitle:Y,pdfURL:Y,seq:Y"
                    ),
                },
                headers={
                    "Content-Type": "application/json",
                    "access_token": self.access_token,
                },
                timeout=30,
            )
        response.raise_for_status()
        payload = response.json()
        events = _extract_ifind_event_payload_rows(payload)
        if not isinstance(events, list):
            raise ValueError("iFind events response must be a list or contain events/data list")
        return events

    def _normalize_event(self, row: dict[str, Any], ingested_at: datetime) -> dict[str, object]:
        symbol = str(row.get("symbol") or row.get("thscode") or row.get("code") or "")
        if not symbol:
            raise ValueError("iFind event row missing symbol")
        event_date = self._coerce_date(
            row.get("event_date")
            or row.get("date")
            or row.get("ann_date")
            or row.get("reportDate")
            or row.get("report_date")
        )
        event_ts = self._coerce_datetime(
            row.get("event_ts") or row.get("datetime") or row.get("ctime"),
            event_date,
        )
        event_type = str(
            row.get("event_type") or row.get("type") or row.get("reportType") or "ifind_report"
        )
        event_id = str(
            row.get("event_id")
            or row.get("id")
            or row.get("seq")
            or f"ifind-{symbol}-{event_date.isoformat()}-{event_type}"
        )
        return {
            "event_id": event_id,
            "security_id": str(row.get("security_id") or self._security_id_from_symbol(symbol)),
            "symbol": symbol,
            "market": str(row.get("market") or self._market_from_symbol(symbol)),
            "event_date": event_date,
            "event_ts": event_ts,
            "event_type": event_type,
            "event_title": str(
                row.get("event_title") or row.get("title") or row.get("reportTitle") or event_type
            ),
            "event_source": "ifind",
            "importance": int(row.get("importance") or 3),
            "data_source": "ifind_real",
            "source_dataset": str(
                row.get("source_dataset") or row.get("sourceDataset") or "ifind_events"
            ),
            "source_run_id": str(
                row.get("source_run_id") or f"ifind-events-{ingested_at:%Y%m%dT%H%M%S}"
            ),
            "ingested_at": ingested_at,
        }

    @staticmethod
    def _coerce_date(value: object) -> date:
        if isinstance(value, date) and not isinstance(value, datetime):
            return value
        if isinstance(value, datetime):
            return value.date()
        if value is None:
            raise ValueError("iFind event row missing event_date")
        text = str(value)
        if len(text) == 8 and text.isdigit():
            return datetime.strptime(text, "%Y%m%d").date()
        return datetime.fromisoformat(text).date()

    @staticmethod
    def _coerce_datetime(value: object, fallback_date: date) -> datetime:
        if isinstance(value, datetime):
            return value if value.tzinfo else value.replace(tzinfo=UTC)
        if value is None:
            return datetime.combine(fallback_date, datetime.min.time(), tzinfo=UTC)
        text = str(value)
        if len(text) == 8 and text.isdigit():
            parsed_date = datetime.strptime(text, "%Y%m%d").date()
            return datetime.combine(parsed_date, datetime.min.time(), tzinfo=UTC)
        parsed = datetime.fromisoformat(text)
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)

    def _empty_events(self) -> pl.DataFrame:
        return pl.DataFrame(
            schema={
                "event_id": pl.Utf8,
                "security_id": pl.Utf8,
                "symbol": pl.Utf8,
                "market": pl.Utf8,
                "event_date": pl.Date,
                "event_ts": pl.Datetime(time_zone="UTC"),
                "event_type": pl.Utf8,
                "event_title": pl.Utf8,
                "event_source": pl.Utf8,
                "importance": pl.Int64,
                "data_source": pl.Utf8,
                "source_dataset": pl.Utf8,
                "source_run_id": pl.Utf8,
                "ingested_at": pl.Datetime(time_zone="UTC"),
            }
        )

    @staticmethod
    def _market_from_symbol(symbol: str) -> str:
        suffix = symbol.split(".")[-1]
        return {"HK": "HK", "US": "US", "SH": "CN", "SZ": "CN"}.get(suffix, suffix)

    @classmethod
    def _security_id_from_symbol(cls, symbol: str) -> str:
        ticker, _ = symbol.split(".")
        return f"{cls._market_from_symbol(symbol)}.{ticker}"


def _load_ifind_event_rows(path: Path) -> Iterable[dict[str, Any]]:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return _normalize_ifind_export_columns(pl.read_csv(path).to_dicts())
    if suffix in {".json", ".jsonl", ".ndjson"}:
        text = path.read_text(encoding="utf-8").strip()
        if not text:
            return []
        if suffix == ".json":
            payload = json.loads(text)
            if isinstance(payload, dict):
                rows = payload.get("events") or payload.get("data") or []
            else:
                rows = payload
            if not isinstance(rows, list):
                raise ValueError("iFind event JSON must be a list or contain events/data list")
            return _normalize_ifind_export_columns(rows)
        return _normalize_ifind_export_columns(
            [json.loads(line) for line in text.splitlines() if line.strip()]
        )
    raise ValueError("iFind event file must be .csv, .json, .jsonl, or .ndjson")


def _extract_ifind_event_payload_rows(payload: object) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return _normalize_ifind_export_columns(payload)
    if not isinstance(payload, dict):
        raise ValueError("iFind events response must be a JSON object or list")
    rows = payload.get("events") or payload.get("data")
    if isinstance(rows, list):
        return _normalize_ifind_export_columns(rows)
    tables = payload.get("tables")
    if isinstance(tables, list):
        extracted: list[dict[str, Any]] = []
        for table_payload in tables:
            extracted.extend(_extract_ifind_table_rows(table_payload))
        return _normalize_ifind_export_columns(extracted)
    return []


def _extract_ifind_table_rows(payload: object) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    table = payload.get("table", payload)
    if isinstance(table, list):
        return [row for row in table if isinstance(row, dict)]
    if not isinstance(table, dict):
        return []
    lengths = [
        len(value)
        for value in table.values()
        if isinstance(value, list)
    ]
    if not lengths:
        return [table]
    row_count = max(lengths)
    rows: list[dict[str, Any]] = []
    for idx in range(row_count):
        row: dict[str, Any] = {}
        for key, value in table.items():
            if isinstance(value, list):
                row[key] = value[idx] if idx < len(value) else None
            else:
                row[key] = value
        rows.append(row)
    return rows


def _normalize_ifind_export_columns(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for row in rows:
        normalized.append(
            {
                IFIND_EVENT_COLUMN_ALIASES.get(str(key).strip(), str(key).strip()): value
                for key, value in row.items()
            }
        )
    return normalized
