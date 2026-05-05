from __future__ import annotations

from dataclasses import dataclass

from multi_layer_trading_lab.adapters.readiness import DataSourceReadiness


@dataclass(frozen=True, slots=True)
class SourceAdapterStatus:
    source: str
    credential_ready: bool
    adapter_status: str
    live_data_ready: bool
    failed_reasons: tuple[str, ...]


def build_source_adapter_status(
    readiness: DataSourceReadiness,
    *,
    adapter_status: str,
) -> SourceAdapterStatus:
    failed = list(readiness.failed_reasons)
    live_adapter_statuses = {"real_adapter", "real_file_adapter"}
    live_data_ready = readiness.ready and adapter_status in live_adapter_statuses
    if readiness.ready and adapter_status not in live_adapter_statuses:
        failed.append(f"{readiness.source}_{adapter_status}")
    return SourceAdapterStatus(
        source=readiness.source,
        credential_ready=readiness.ready,
        adapter_status=adapter_status,
        live_data_ready=live_data_ready,
        failed_reasons=tuple(failed),
    )
