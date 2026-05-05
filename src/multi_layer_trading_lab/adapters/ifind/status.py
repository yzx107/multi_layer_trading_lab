from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

from multi_layer_trading_lab.adapters.ifind.token import inspect_ifind_tokens
from multi_layer_trading_lab.storage import ParquetStore


@dataclass(frozen=True, slots=True)
class IFindConnectionPlan:
    ready_for_real_http_fetch: bool
    ready_for_real_file_mode: bool
    endpoint_configured: bool
    endpoint_host: str | None
    official_report_query_available: bool
    access_token_present: bool
    refresh_token_present: bool
    refresh_token_expires_at: datetime | None
    refresh_token_valid_now: bool | None
    lake_counts: dict[str, int]
    failed_reasons: tuple[str, ...]
    next_actions: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "ready_for_real_http_fetch": self.ready_for_real_http_fetch,
            "ready_for_real_file_mode": self.ready_for_real_file_mode,
            "endpoint_configured": self.endpoint_configured,
            "endpoint_host": self.endpoint_host,
            "official_report_query_available": self.official_report_query_available,
            "access_token_present": self.access_token_present,
            "refresh_token_present": self.refresh_token_present,
            "refresh_token_expires_at": (
                self.refresh_token_expires_at.isoformat()
                if self.refresh_token_expires_at
                else None
            ),
            "refresh_token_valid_now": self.refresh_token_valid_now,
            "lake_counts": self.lake_counts,
            "failed_reasons": list(self.failed_reasons),
            "next_actions": list(self.next_actions),
        }


def build_ifind_connection_plan(
    *,
    lake_root: Path,
    access_token: str | None,
    refresh_token: str | None,
    events_endpoint: str | None,
) -> IFindConnectionPlan:
    token_status = inspect_ifind_tokens(
        access_token=access_token,
        refresh_token=refresh_token,
    )
    endpoint = events_endpoint.strip() if events_endpoint else ""
    endpoint_host = _endpoint_host(endpoint) if endpoint else None
    lake_counts = ifind_lake_counts(lake_root)
    real_rows = lake_counts["ifind_real_rows"] + lake_counts["ifind_real_file_rows"]

    failed: list[str] = []
    if not token_status.has_access_token:
        failed.append("missing_ifind_access_token")
    if not token_status.has_refresh_token:
        failed.append("missing_ifind_refresh_token")
    if token_status.refresh_token_valid_now is False:
        failed.append("expired_ifind_refresh_token")
    if real_rows <= 0:
        failed.append("missing_real_ifind_lake_rows")

    ready_for_http = (
        token_status.token_pair_present
        and token_status.refresh_token_valid_now is not False
    )
    ready_for_file_mode = real_rows > 0
    next_actions = _next_actions(
        ready_for_http=ready_for_http,
        ready_for_file_mode=ready_for_file_mode,
        real_rows=real_rows,
    )
    return IFindConnectionPlan(
        ready_for_real_http_fetch=ready_for_http,
        ready_for_real_file_mode=ready_for_file_mode,
        endpoint_configured=bool(endpoint),
        endpoint_host=endpoint_host,
        official_report_query_available=True,
        access_token_present=token_status.has_access_token,
        refresh_token_present=token_status.has_refresh_token,
        refresh_token_expires_at=token_status.refresh_token_expires_at,
        refresh_token_valid_now=token_status.refresh_token_valid_now,
        lake_counts=lake_counts,
        failed_reasons=tuple(failed),
        next_actions=next_actions,
    )


def ifind_lake_counts(lake_root: Path) -> dict[str, int]:
    try:
        events = ParquetStore(lake_root).read("ifind_events")
    except FileNotFoundError:
        return {
            "ifind_total_rows": 0,
            "ifind_real_rows": 0,
            "ifind_real_file_rows": 0,
            "ifind_stub_rows": 0,
        }
    if "data_source" not in events.columns:
        return {
            "ifind_total_rows": events.height,
            "ifind_real_rows": 0,
            "ifind_real_file_rows": 0,
            "ifind_stub_rows": 0,
        }
    return {
        "ifind_total_rows": events.height,
        "ifind_real_rows": events.filter(events["data_source"] == "ifind_real").height,
        "ifind_real_file_rows": events.filter(
            events["data_source"] == "ifind_real_file"
        ).height,
        "ifind_stub_rows": events.filter(events["data_source"] == "ifind_stub").height,
    }


def _endpoint_host(endpoint: str) -> str | None:
    parsed = urlparse(endpoint)
    return parsed.netloc or None


def _next_actions(
    *,
    ready_for_http: bool,
    ready_for_file_mode: bool,
    real_rows: int,
) -> tuple[str, ...]:
    actions: list[str] = []
    if ready_for_http:
        actions.append(
            "Run fetch-ifind-to-lake --use-real for a small report_query symbol/date smoke."
        )
    if real_rows <= 0:
        actions.append(
            "Or export an iFind terminal event file and run validate/import-ifind-events-file."
        )
    if ready_for_file_mode:
        actions.append("Run data-adapter-status --ifind-adapter-status real_file_adapter.")
    return tuple(actions)
