from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from multi_layer_trading_lab.execution.session_ledger import (
    PaperSessionLedger,
    build_paper_session_ledger,
)


@dataclass(frozen=True, slots=True)
class ObjectiveAuditInput:
    readiness_manifest_path: Path
    output_path: Path
    research_input_manifest_path: Path | None = None
    profitability_evidence_path: Path | None = None
    ifind_validation_report_path: Path | None = None
    ifind_ingestion_status_path: Path | None = None
    opend_quote_snapshot_path: Path | None = None
    opend_ticket_response_path: Path | None = None
    opend_runtime_status_path: Path | None = None
    opend_account_status_path: Path | None = None
    paper_simulate_status_path: Path | None = None
    execution_log_path: Path | None = None
    broker_report_path: Path | None = None
    paper_blocker_report_path: Path | None = None
    paper_progress_path: Path | None = None


def _load_json(path: Path) -> dict[str, object]:
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def _is_older_than(path: Path | None, reference_path: Path | None) -> bool:
    if path is None or reference_path is None:
        return False
    if not path.exists() or not reference_path.exists():
        return False
    return path.stat().st_mtime < reference_path.stat().st_mtime


def _status(approved: bool) -> str:
    return "passed" if approved else "blocked"


def _profitability_ready(
    path: Path | None,
    reference_paths: tuple[Path | None, ...] = (),
) -> tuple[bool, dict[str, object] | None, list[str]]:
    if path is None:
        return False, None, ["missing_profitability_evidence_path"]
    if not path.exists():
        return False, None, ["missing_profitability_evidence"]
    payload = dict(_load_json(path))
    failed: list[str] = []
    stale_reference_paths = [
        str(reference_path)
        for reference_path in reference_paths
        if _is_older_than(path, reference_path)
    ]
    if stale_reference_paths:
        payload["profitability_evidence_stale"] = True
        payload["stale_reference_paths"] = stale_reference_paths
        failed.append("stale_profitability_evidence")
    if payload.get("ready") is False:
        failed.extend(str(reason) for reason in payload.get("failed_reasons", []))
    if float(payload.get("paper_sessions", 0) or 0) < 20:
        failed.append("insufficient_profitable_paper_sessions")
    if float(payload.get("net_pnl", 0.0) or 0.0) <= 0:
        failed.append("net_pnl_not_positive")
    if float(payload.get("max_drawdown", 0.0) or 0.0) < -float(
        payload.get("max_allowed_drawdown", 10_000.0) or 10_000.0
    ):
        failed.append("drawdown_breached")
    if not bool(payload.get("reconciled", False)):
        failed.append("profitability_not_reconciled_to_broker")
    failed = list(dict.fromkeys(failed))
    return not failed, payload, failed


def _paper_progress_ready(
    path: Path | None,
    reference_paths: tuple[Path | None, ...] = (),
) -> tuple[bool, dict[str, object] | None, list[str]]:
    if path is None:
        return True, None, []
    if not path.exists():
        return False, None, ["missing_paper_progress"]
    payload = dict(_load_json(path))
    failed: list[str] = []
    stale_reference_paths = [
        str(reference_path)
        for reference_path in reference_paths
        if _is_older_than(path, reference_path)
    ]
    if stale_reference_paths:
        payload["paper_progress_stale"] = True
        payload["stale_reference_paths"] = stale_reference_paths
        failed.append("stale_paper_progress")
    if payload.get("ready_for_live_review") is not True:
        reasons = [
            str(reason)
            for reason in payload.get("failed_reasons", [])
            if str(reason)
        ]
        failed.extend(reasons or ["paper_progress_not_ready_for_live_review"])
    return not failed, payload, list(dict.fromkeys(failed))


def _research_input_manifest_reuse(
    path: Path | None,
) -> tuple[dict[str, object] | None, list[str], list[str]]:
    if path is None:
        return None, [], []
    if not path.exists():
        evidence = {"path": str(path), "exists": False}
        return evidence, ["missing_research_input_manifest"], [
            "missing_research_input_manifest"
        ]

    payload = dict(_load_json(path))
    hshare_failed: list[str] = []
    factor_failed: list[str] = []
    external_repos = _dict_payload(payload.get("external_repos"))
    research_inputs = _dict_payload(payload.get("research_inputs"))

    hshare_repo = _dict_payload(external_repos.get("hshare_lab_v2"))
    if not hshare_repo:
        hshare_failed.append("missing_hshare_lab_reuse_check")
    else:
        if hshare_repo.get("ready") is not True:
            hshare_failed.append("hshare_lab_v2_not_ready")
        if hshare_repo.get("missing_paths"):
            hshare_failed.append("hshare_lab_v2_missing_paths")

    hshare_stage = _dict_payload(research_inputs.get("hshare_stage"))
    hshare_verified = _dict_payload(research_inputs.get("hshare_verified"))
    if not hshare_stage:
        hshare_failed.append("missing_hshare_stage_reuse_input")
    else:
        if hshare_stage.get("exists") is not True:
            hshare_failed.append("hshare_stage_reuse_input_missing")
        if _optional_int(hshare_stage.get("file_count")) == 0:
            hshare_failed.append("hshare_stage_reuse_input_empty")
    if not hshare_verified:
        hshare_failed.append("missing_hshare_verified_reuse_input")
    else:
        if hshare_verified.get("exists") is not True:
            hshare_failed.append("hshare_verified_reuse_input_missing")
        if _optional_int(hshare_verified.get("manifest_count")) == 0:
            hshare_failed.append("hshare_verified_manifest_empty")
        if _optional_int(hshare_verified.get("parquet_count")) == 0:
            hshare_failed.append("hshare_verified_parquet_empty")

    factor_repo = _dict_payload(external_repos.get("hk_factor_autoresearch"))
    if not factor_repo:
        factor_failed.append("missing_factor_factory_reuse_check")
    else:
        if factor_repo.get("ready") is not True:
            factor_failed.append("hk_factor_autoresearch_not_ready")
        if factor_repo.get("missing_paths"):
            factor_failed.append("hk_factor_autoresearch_missing_paths")

    factor_registry = _dict_payload(research_inputs.get("factor_registry"))
    factor_runs = _dict_payload(research_inputs.get("factor_runs"))
    if not factor_registry:
        factor_failed.append("missing_factor_registry_reuse_input")
    else:
        if factor_registry.get("exists") is not True:
            factor_failed.append("factor_registry_reuse_input_missing")
        if _optional_int(factor_registry.get("file_count")) == 0:
            factor_failed.append("factor_registry_reuse_input_empty")
    if not factor_runs:
        factor_failed.append("missing_factor_runs_reuse_input")
    else:
        if factor_runs.get("exists") is not True:
            factor_failed.append("factor_runs_reuse_input_missing")
        if _optional_int(factor_runs.get("summary_count")) == 0:
            factor_failed.append("factor_runs_reuse_input_empty")

    return payload, list(dict.fromkeys(hshare_failed)), list(dict.fromkeys(factor_failed))


def _freshness_failed_reasons(
    data_freshness: dict[str, dict[str, object]],
    datasets: tuple[str, ...],
) -> list[str]:
    failed: list[str] = []
    for dataset in datasets:
        item = data_freshness.get(dataset)
        if not item:
            continue
        status = item.get("status")
        if status != "fresh":
            failed.append(f"{dataset}_not_fresh")
            if status:
                failed.append(f"{dataset}_{status}")
        if _optional_int(item.get("rows")) == 0:
            failed.append(f"{dataset}_empty")
    return list(dict.fromkeys(failed))


def _opend_runtime_ready(
    quote_snapshot_path: Path | None,
    ticket_response_path: Path | None,
    runtime_status_path: Path | None = None,
    account_status_path: Path | None = None,
    paper_simulate_status_path: Path | None = None,
    paper_blocker_report_path: Path | None = None,
    paper_progress_path: Path | None = None,
) -> tuple[bool, dict[str, object], list[str]]:
    failed: list[str] = []
    evidence: dict[str, object] = {
        "quote_snapshot_path": str(quote_snapshot_path) if quote_snapshot_path else None,
        "ticket_response_path": str(ticket_response_path) if ticket_response_path else None,
        "runtime_status_path": str(runtime_status_path) if runtime_status_path else None,
        "account_status_path": str(account_status_path) if account_status_path else None,
        "paper_simulate_status_path": (
            str(paper_simulate_status_path) if paper_simulate_status_path else None
        ),
        "paper_blocker_report_path": (
            str(paper_blocker_report_path) if paper_blocker_report_path else None
        ),
    }
    if runtime_status_path is None or not runtime_status_path.exists():
        failed.append("missing_opend_runtime_status")
    else:
        runtime_status = _load_json(runtime_status_path)
        evidence["runtime_status"] = runtime_status
        if runtime_status.get("ready_for_order_submission") is not True:
            failed.extend(str(reason) for reason in runtime_status.get("failed_reasons", []))
            if runtime_status.get("kill_switch") is True:
                failed.append("opend_kill_switch_enabled")

    if account_status_path is None or not account_status_path.exists():
        failed.append("missing_opend_account_status")
    else:
        account_status = _load_json(account_status_path)
        evidence["account_status"] = _sanitize_opend_account_status(account_status)
        if account_status.get("ready_for_paper_simulate") is not True:
            failed.extend(str(reason) for reason in account_status.get("failed_reasons", []))
            failed.append("opend_account_not_ready_for_paper_simulate")

    if quote_snapshot_path is None or not quote_snapshot_path.exists():
        failed.append("missing_opend_quote_snapshot")
    else:
        quote_payload = _load_json(quote_snapshot_path)
        quote = (
            quote_payload.get("quote")
            if isinstance(quote_payload.get("quote"), dict)
            else quote_payload
        )
        evidence["quote_snapshot"] = quote
        if not isinstance(quote, dict):
            failed.append("invalid_opend_quote_snapshot")
        else:
            if not quote.get("symbol"):
                failed.append("opend_quote_missing_symbol")
            if not quote.get("lot_size"):
                failed.append("opend_quote_missing_lot_size")
            price_values = [
                quote.get("last_price"),
                quote.get("price"),
                quote.get("best_ask"),
                quote.get("ask"),
                quote.get("best_bid"),
                quote.get("bid"),
            ]
            if not any(_positive_float(value) for value in price_values):
                failed.append("opend_quote_missing_reference_price")

    if ticket_response_path is None or not ticket_response_path.exists():
        failed.append("missing_opend_ticket_response")
    else:
        response_rows = _load_jsonl(ticket_response_path)
        evidence["ticket_response_rows"] = len(response_rows)
        if not response_rows:
            failed.append("empty_opend_ticket_response")
        submitted_rows = sum(1 for row in response_rows if _ticket_response_submitted(row))
        failed_rows = sum(1 for row in response_rows if _ticket_response_failed(row))
        evidence["submitted_ticket_response_rows"] = submitted_rows
        evidence["failed_ticket_response_rows"] = failed_rows
        if response_rows and submitted_rows == 0:
            failed.append("missing_submitted_opend_ticket_response")

    paper_status_stale = _is_older_than(paper_simulate_status_path, runtime_status_path)
    if paper_status_stale:
        failed.append("stale_paper_simulate_status")

    if paper_simulate_status_path is not None and not paper_simulate_status_path.exists():
        failed.append("missing_paper_simulate_status")
    elif paper_simulate_status_path is not None and paper_simulate_status_path.exists():
        paper_status = _load_json(paper_simulate_status_path)
        evidence["paper_simulate_status"] = paper_status
        evidence["paper_simulate_status_stale"] = paper_status_stale
        if (
            not paper_status_stale
            and paper_status.get("ready_for_session_collection") is not True
        ):
            failed.extend(str(reason) for reason in paper_status.get("failed_reasons", []))

    blocker_ready, blocker_payload, blocker_failed = _paper_blocker_ready(
        paper_blocker_report_path,
        reference_paths=(
            runtime_status_path,
            paper_simulate_status_path,
            paper_progress_path,
        ),
    )
    if blocker_payload is not None:
        evidence["paper_blocker_report"] = blocker_payload
    failed.extend(blocker_failed)

    return not failed and blocker_ready, evidence, list(dict.fromkeys(failed))


def _paper_blocker_ready(
    path: Path | None,
    reference_paths: tuple[Path | None, ...] = (),
) -> tuple[bool, dict[str, object] | None, list[str]]:
    if path is None:
        return True, None, []
    if not path.exists():
        return False, None, ["missing_paper_blocker_report"]
    payload = dict(_load_json(path))
    failed: list[str] = []
    stale_reference_paths = [
        str(reference_path)
        for reference_path in (
            *reference_paths,
            *_paper_blocker_internal_reference_paths(payload),
        )
        if _is_older_than(path, reference_path)
    ]
    if stale_reference_paths:
        payload["paper_blocker_report_stale"] = True
        payload["stale_reference_paths"] = list(dict.fromkeys(stale_reference_paths))
        failed.append("stale_paper_blocker_report")
    if payload.get("ready_for_next_session") is not True:
        reasons = [
            str(reason)
            for reason in payload.get("next_session_failed_reasons", [])
            if str(reason)
        ]
        failed.extend(reasons or ["paper_next_session_not_ready"])
    return not failed, payload, list(dict.fromkeys(failed))


def _paper_blocker_internal_reference_paths(
    payload: dict[str, object],
) -> tuple[Path, ...]:
    paths: list[Path] = []
    for key in (
        "runtime_status_path",
        "paper_simulate_status_path",
        "paper_calendar_path",
        "paper_progress_path",
    ):
        value = payload.get(key)
        if isinstance(value, str) and value:
            paths.append(Path(value))
    return tuple(paths)


def _positive_float(value: object) -> bool:
    try:
        return float(str(value)) > 0
    except (TypeError, ValueError):
        return False


def _load_jsonl(path: Path) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            stripped = line.strip()
            if not stripped:
                continue
            payload = json.loads(stripped)
            if isinstance(payload, dict):
                rows.append(payload)
    return rows


def _ticket_response_submitted(row: dict[str, object]) -> bool:
    response = row.get("response")
    return isinstance(response, dict) and response.get("submitted") is True


def _ticket_response_failed(row: dict[str, object]) -> bool:
    response = row.get("response")
    return isinstance(response, dict) and response.get("ok") is False


def _sanitize_opend_account_status(payload: dict[str, object]) -> dict[str, object]:
    sanitized = dict(payload)
    if "configured_acc_id" in sanitized:
        sanitized["configured_acc_id"] = _redact_identifier(sanitized.get("configured_acc_id"))
    accounts = sanitized.get("accounts")
    if isinstance(accounts, list):
        sanitized["accounts"] = [
            _sanitize_opend_account(account)
            for account in accounts
            if isinstance(account, dict)
        ]
    return sanitized


def _sanitize_opend_account(account: dict[str, object]) -> dict[str, object]:
    sanitized = dict(account)
    for key in ["acc_id", "card_num", "uni_card_num"]:
        if key in sanitized:
            sanitized[key] = _redact_identifier(sanitized.get(key))
    return sanitized


def _redact_identifier(value: object) -> str | None:
    if value in {None, ""}:
        return None
    text = str(value)
    if text.upper() == "N/A":
        return "N/A"
    if len(text) <= 4:
        return "***"
    return f"***{text[-4:]}"


def _ifind_validation_failed_reasons(payload: dict[str, object] | None) -> list[str]:
    if payload is None:
        return []
    if payload.get("valid") is not False:
        return []
    reasons = [str(reason) for reason in payload.get("failed_reasons", [])]
    return reasons or ["ifind_validation_report_invalid"]


def _ifind_failed_reasons(
    *,
    data_source: dict[str, object],
    source_adapter: dict[str, object],
    validation_payload: dict[str, object] | None,
) -> list[str]:
    reasons: list[str] = []
    if data_source.get("ready") is not True:
        reasons.extend(str(reason) for reason in data_source.get("failed_reasons", []))
    if source_adapter.get("live_data_ready") is not True:
        reasons.extend(str(reason) for reason in source_adapter.get("failed_reasons", []))
        adapter_status = source_adapter.get("adapter_status")
        if adapter_status:
            reasons.append(f"ifind_{adapter_status}")
    reasons.extend(_ifind_validation_failed_reasons(validation_payload))
    return list(dict.fromkeys(reasons))


def _ifind_validation_payload(path: Path | None) -> dict[str, object] | None:
    if path is None:
        return None
    if not path.exists():
        return {
            "valid": False,
            "report_path": str(path),
            "failed_reasons": ["missing_ifind_validation_report"],
        }
    return _load_json(path)


def _readiness_failed_reasons(evidence: object, default_reason: str) -> list[str]:
    if not isinstance(evidence, dict):
        return [default_reason]
    reasons = [str(reason) for reason in evidence.get("failed_reasons", [])]
    return list(dict.fromkeys(reasons or [default_reason]))


def _paper_ledger_evidence(
    *,
    execution_log_path: Path | None,
    broker_report_path: Path | None,
) -> tuple[PaperSessionLedger | None, list[str]]:
    if execution_log_path is None and broker_report_path is None:
        return None, []
    if execution_log_path is None:
        return None, ["missing_execution_log_path"]
    if broker_report_path is None:
        return None, ["missing_broker_report_path"]
    ledger = build_paper_session_ledger(
        execution_log_path=execution_log_path,
        broker_report_path=broker_report_path,
    )
    failed = list(ledger.failed_reasons)
    if ledger.inferred_session_count < 20:
        failed.append("insufficient_broker_backed_paper_sessions")
    if ledger.dry_run_rows:
        failed.append("dry_run_rows_present")
    return ledger, list(dict.fromkeys(failed))


def _profitability_ledger_failures(
    profitability: dict[str, object] | None,
    ledger: PaperSessionLedger | None,
) -> list[str]:
    if profitability is None or ledger is None:
        return []
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


def _dict_payload(value: object) -> dict[str, object]:
    return value if isinstance(value, dict) else {}


def build_objective_audit(input_data: ObjectiveAuditInput) -> dict[str, object]:
    readiness = _load_json(input_data.readiness_manifest_path)
    ifind_validation_payload = _ifind_validation_payload(input_data.ifind_validation_report_path)
    ifind_ingestion_payload = _load_json(input_data.ifind_ingestion_status_path) if (
        input_data.ifind_ingestion_status_path is not None
        and input_data.ifind_ingestion_status_path.exists()
    ) else None
    profitability_ready, profitability_payload, profitability_failed = _profitability_ready(
        input_data.profitability_evidence_path,
        reference_paths=(input_data.execution_log_path, input_data.broker_report_path),
    )
    paper_progress_ready, paper_progress_payload, paper_progress_failed = _paper_progress_ready(
        input_data.paper_progress_path,
        reference_paths=(
            input_data.profitability_evidence_path,
            input_data.execution_log_path,
            input_data.broker_report_path,
        ),
    )
    profitability_failed.extend(paper_progress_failed)
    profitability_failed = list(dict.fromkeys(profitability_failed))
    profitability_ready = profitability_ready and paper_progress_ready
    opend_runtime_ready, opend_runtime_evidence, opend_runtime_failed = _opend_runtime_ready(
        input_data.opend_quote_snapshot_path,
        input_data.opend_ticket_response_path,
        input_data.opend_runtime_status_path,
        input_data.opend_account_status_path,
        input_data.paper_simulate_status_path,
        input_data.paper_blocker_report_path,
        input_data.paper_progress_path,
    )
    paper_ledger, paper_ledger_failed = _paper_ledger_evidence(
        execution_log_path=input_data.execution_log_path,
        broker_report_path=input_data.broker_report_path,
    )
    profitability_failed.extend(
        _profitability_ledger_failures(profitability_payload, paper_ledger)
    )
    profitability_failed = list(dict.fromkeys(profitability_failed))
    if _has_any(
        profitability_failed,
        [
            "missing_profitability_broker_report_rows",
            "missing_profitability_execution_log_rows",
            "missing_profitability_inferred_session_count",
            "missing_profitability_paper_sessions",
            "missing_profitability_session_dates",
            "profitability_broker_report_rows_mismatch",
            "profitability_execution_log_rows_mismatch",
            "profitability_inferred_session_count_mismatch",
            "profitability_session_count_mismatch",
            "profitability_session_dates_mismatch",
        ],
    ):
        profitability_ready = False
    data_sources = {
        str(source.get("source")): source
        for source in readiness.get("data_sources", [])
        if isinstance(source, dict)
    }
    source_adapters = {
        str(source.get("source")): source
        for source in readiness.get("source_adapters", [])
        if isinstance(source, dict)
    }
    data_freshness = {
        str(item.get("dataset")): item
        for item in readiness.get("data_freshness", [])
        if isinstance(item, dict)
    }
    (
        research_input_manifest_payload,
        hshare_reuse_failed,
        factor_reuse_failed,
    ) = _research_input_manifest_reuse(input_data.research_input_manifest_path)

    intraday_l2_payload = data_freshness.get("intraday_l2_features", {})
    intraday_l2_fresh = intraday_l2_payload.get("status") == "fresh"
    intraday_l2_rows = _optional_int(intraday_l2_payload.get("rows")) or 0
    intraday_l2_rows_ready = intraday_l2_rows >= 20
    hshare_verified_payload = _dict_payload(readiness.get("hshare_verified"))
    hk_l2_ready = (
        intraday_l2_fresh
        and intraday_l2_rows_ready
        and hshare_verified_payload.get("ready") is True
        and not hshare_reuse_failed
    )
    hk_l2_failed: list[str] = []
    if not intraday_l2_fresh:
        hk_l2_failed.append("intraday_l2_features_not_fresh")
    if not intraday_l2_rows_ready:
        hk_l2_failed.append("insufficient_intraday_l2_feature_rows")
    if hshare_verified_payload.get("ready") is not True:
        hk_l2_failed.extend(
            _readiness_failed_reasons(
                hshare_verified_payload,
                "hshare_verified_not_ready",
            )
        )
    hk_l2_failed.extend(hshare_reuse_failed)
    hk_l2_failed = list(dict.fromkeys(hk_l2_failed))
    tushare_data_failed = _freshness_failed_reasons(
        data_freshness,
        ("security_master", "daily_features"),
    )
    tushare_failed = (
        []
        if data_sources.get("tushare", {}).get("ready") is True
        else _readiness_failed_reasons(
            data_sources.get("tushare", {}),
            "tushare_source_not_ready",
        )
    )
    if source_adapters.get("tushare", {}).get("live_data_ready") is not True:
        tushare_failed.extend(
            _readiness_failed_reasons(
                source_adapters.get("tushare", {}),
                "tushare_adapter_not_live_data_ready",
            )
        )
    tushare_failed.extend(tushare_data_failed)
    tushare_failed = list(dict.fromkeys(tushare_failed))
    tushare_ready = not tushare_failed
    ifind_ready = (
        data_sources.get("ifind", {}).get("ready") is True
        and source_adapters.get("ifind", {}).get("live_data_ready") is True
    )
    opend_ready = readiness.get("execution", {}).get("opend_ready") is True and opend_runtime_ready
    personal_risk_ready = bool(readiness.get("account_risk_budget"))
    research_to_paper = _dict_payload(readiness.get("research_to_paper"))
    external_factor_portfolio = _dict_payload(readiness.get("external_factor_portfolio"))
    research_failed = (
        []
        if research_to_paper.get("approved") is True
        else _readiness_failed_reasons(
            research_to_paper,
            "research_to_paper_not_approved",
        )
    )
    if external_factor_portfolio and external_factor_portfolio.get("approved") is not True:
        research_failed.extend(
            _readiness_failed_reasons(
                external_factor_portfolio,
                "external_factor_portfolio_not_approved",
            )
        )
    research_failed.extend(factor_reuse_failed)
    research_failed = list(dict.fromkeys(research_failed))
    research_ready = not research_failed
    paper_to_live_evidence = readiness.get("paper_to_live", {})
    paper_ready = (
        isinstance(paper_to_live_evidence, dict)
        and paper_to_live_evidence.get("approved") is True
    )
    paper_failed = (
        []
        if paper_ready
        else _readiness_failed_reasons(
            paper_to_live_evidence,
            "paper_to_live_not_approved",
        )
    )
    paper_failed.extend(paper_ledger_failed)
    paper_failed = list(dict.fromkeys(paper_failed))
    paper_direct_evidence_ready = paper_ledger is None or not paper_ledger_failed
    paper_ready = paper_ready and paper_direct_evidence_ready
    go_live_ready = readiness.get("go_live_approved") is True

    checks = [
        {
            "requirement": "wall_street_style_research_and_gate_process",
            "status": _status(research_ready),
            "evidence": {
                "research_to_paper": research_to_paper,
                "external_factor_portfolio": external_factor_portfolio,
                "research_input_manifest": research_input_manifest_payload,
            },
            "failed_reasons": research_failed,
        },
        {
            "requirement": "hk_l2_data_reuse",
            "status": _status(hk_l2_ready),
            "evidence": {
                "intraday_l2_features": intraday_l2_payload,
                "hshare_verified": hshare_verified_payload,
                "research_input_manifest": research_input_manifest_payload,
            },
            "failed_reasons": hk_l2_failed,
        },
        {
            "requirement": "tushare_real_data_adapter",
            "status": _status(tushare_ready),
            "evidence": {
                "data_source": data_sources.get("tushare", {}),
                "source_adapter": source_adapters.get("tushare", {}),
                "freshness": {
                    "security_master": data_freshness.get("security_master", {}),
                    "daily_features": data_freshness.get("daily_features", {}),
                },
            },
            "failed_reasons": tushare_failed,
        },
        {
            "requirement": "ifind_real_data_adapter",
            "status": _status(ifind_ready),
            "evidence": {
                "data_source": data_sources.get("ifind", {}),
                "source_adapter": source_adapters.get("ifind", {}),
                "validation_report": ifind_validation_payload,
                "ingestion_status": ifind_ingestion_payload,
            },
            "failed_reasons": _ifind_failed_reasons(
                data_source=data_sources.get("ifind", {}),
                source_adapter=source_adapters.get("ifind", {}),
                validation_payload=ifind_validation_payload,
            ),
        },
        {
            "requirement": "opend_execution_gate",
            "status": _status(opend_ready),
            "evidence": {
                "readiness": readiness.get("execution", {}),
                "runtime": opend_runtime_evidence,
            },
            "failed_reasons": opend_runtime_failed,
        },
        {
            "requirement": "million_scale_personal_account_risk",
            "status": _status(personal_risk_ready),
            "evidence": readiness.get("account_risk_budget", {}),
        },
        {
            "requirement": "paper_to_live_execution_evidence",
            "status": _status(paper_ready),
            "evidence": _paper_to_live_evidence_payload(
                paper_to_live_evidence,
                paper_ledger,
            ),
            "failed_reasons": paper_failed,
        },
        {
            "requirement": "profitable_reconciled_paper_or_live_evidence",
            "status": _status(profitability_ready),
            "evidence": _profitability_evidence_payload(
                profitability_payload,
                paper_progress_payload,
            ),
            "failed_reasons": profitability_failed,
        },
    ]
    blocked = [
        check["requirement"]
        for check in checks
        if check["status"] != "passed"
    ]
    success_criteria = _success_criteria()
    audit = {
        "objective_achieved": go_live_ready and not blocked,
        "objective": (
            "Build a Wall-Street-style personal trading research and execution platform "
            "for HK L2, Tushare, iFind, and OpenD on Mac mini/MacBook Air hardware, "
            "sized for a million-level personal account, with broker-reconciled "
            "paper/live evidence of positive profitability before considering it complete."
        ),
        "success_criteria": success_criteria,
        "readiness_manifest_path": str(input_data.readiness_manifest_path),
        "research_input_manifest_path": (
            str(input_data.research_input_manifest_path)
            if input_data.research_input_manifest_path is not None
            else None
        ),
        "profitability_evidence_path": (
            str(input_data.profitability_evidence_path)
            if input_data.profitability_evidence_path is not None
            else None
        ),
        "ifind_validation_report_path": (
            str(input_data.ifind_validation_report_path)
            if input_data.ifind_validation_report_path is not None
            else None
        ),
        "ifind_ingestion_status_path": (
            str(input_data.ifind_ingestion_status_path)
            if input_data.ifind_ingestion_status_path is not None
            else None
        ),
        "opend_quote_snapshot_path": (
            str(input_data.opend_quote_snapshot_path)
            if input_data.opend_quote_snapshot_path is not None
            else None
        ),
        "opend_ticket_response_path": (
            str(input_data.opend_ticket_response_path)
            if input_data.opend_ticket_response_path is not None
            else None
        ),
        "opend_runtime_status_path": (
            str(input_data.opend_runtime_status_path)
            if input_data.opend_runtime_status_path is not None
            else None
        ),
        "opend_account_status_path": (
            str(input_data.opend_account_status_path)
            if input_data.opend_account_status_path is not None
            else None
        ),
        "paper_simulate_status_path": (
            str(input_data.paper_simulate_status_path)
            if input_data.paper_simulate_status_path is not None
            else None
        ),
        "paper_blocker_report_path": (
            str(input_data.paper_blocker_report_path)
            if input_data.paper_blocker_report_path is not None
            else None
        ),
        "paper_progress_path": (
            str(input_data.paper_progress_path)
            if input_data.paper_progress_path is not None
            else None
        ),
        "execution_log_path": (
            str(input_data.execution_log_path)
            if input_data.execution_log_path is not None
            else None
        ),
        "broker_report_path": (
            str(input_data.broker_report_path)
            if input_data.broker_report_path is not None
            else None
        ),
        "checks": checks,
        "prompt_to_artifact_checklist": _prompt_to_artifact_checklist(
            checks=checks,
            success_criteria=success_criteria,
            readiness_manifest_path=input_data.readiness_manifest_path,
            research_input_manifest_path=input_data.research_input_manifest_path,
            profitability_evidence_path=input_data.profitability_evidence_path,
            ifind_validation_report_path=input_data.ifind_validation_report_path,
            ifind_ingestion_status_path=input_data.ifind_ingestion_status_path,
            opend_quote_snapshot_path=input_data.opend_quote_snapshot_path,
            opend_ticket_response_path=input_data.opend_ticket_response_path,
            opend_runtime_status_path=input_data.opend_runtime_status_path,
            opend_account_status_path=input_data.opend_account_status_path,
            paper_simulate_status_path=input_data.paper_simulate_status_path,
            paper_blocker_report_path=input_data.paper_blocker_report_path,
            paper_progress_path=input_data.paper_progress_path,
            execution_log_path=input_data.execution_log_path,
            broker_report_path=input_data.broker_report_path,
        ),
        "blocked_requirements": blocked,
        "completion_decision": {
            "status": "achieved" if go_live_ready and not blocked else "not_achieved",
            "reason": (
                "all objective requirements have direct evidence"
                if go_live_ready and not blocked
                else "one or more objective requirements are blocked or weakly evidenced"
            ),
            "blocked_requirements": blocked,
        },
    }
    input_data.output_path.parent.mkdir(parents=True, exist_ok=True)
    input_data.output_path.write_text(
        json.dumps(audit, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return audit


def _paper_to_live_evidence_payload(
    readiness_evidence: object,
    paper_ledger: PaperSessionLedger | None,
) -> dict[str, object]:
    payload: dict[str, object] = (
        dict(readiness_evidence) if isinstance(readiness_evidence, dict) else {}
    )
    if paper_ledger is not None:
        payload["paper_session_ledger"] = paper_ledger.to_dict()
    return payload


def _profitability_evidence_payload(
    profitability_evidence: dict[str, object] | None,
    paper_progress: dict[str, object] | None,
) -> dict[str, object] | None:
    if profitability_evidence is None and paper_progress is None:
        return None
    payload = dict(profitability_evidence or {})
    if paper_progress is not None:
        payload["paper_progress"] = paper_progress
    return payload


def render_objective_audit_report(audit: dict[str, object]) -> str:
    decision = audit.get("completion_decision", {})
    status = (
        decision.get("status")
        if isinstance(decision, dict)
        else "not_achieved"
    )
    blocked = audit.get("blocked_requirements", [])
    lines = [
        "# Objective Completion Audit",
        "",
        f"Status: {status}",
        "",
        "## Objective",
        "",
        str(audit.get("objective", "")),
        "",
        "## Completion Decision",
        "",
        f"- objective_achieved: {str(audit.get('objective_achieved')).lower()}",
        f"- reason: {decision.get('reason') if isinstance(decision, dict) else ''}",
        f"- blocked_requirements: {_format_list(blocked)}",
        "",
        "## Prompt To Artifact Checklist",
        "",
        "| Requirement | Status | Next Action | Verification | Artifacts | Failed Reasons |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    checklist = audit.get("prompt_to_artifact_checklist", [])
    if isinstance(checklist, list):
        for item in checklist:
            if not isinstance(item, dict):
                continue
            lines.append(
                "| "
                + " | ".join(
                    [
                        _md_cell(item.get("requirement")),
                        _md_cell(item.get("status")),
                        _md_cell(item.get("next_required_action")),
                        _md_cell(item.get("verification_command")),
                        _md_cell(item.get("artifacts")),
                        _md_cell(item.get("failed_reasons")),
                    ]
                )
                + " |"
            )
    lines.extend(
        [
            "",
            "## Next Required Evidence",
            "",
        ]
    )
    for requirement in blocked if isinstance(blocked, list) else []:
        lines.append(f"- {requirement}: {_next_required_evidence(str(requirement))}")
    if not blocked:
        lines.append("- None. All objective requirements have direct evidence.")
    return "\n".join(lines) + "\n"


def write_objective_audit_report(audit_path: Path, output_path: Path) -> Path:
    audit = _load_json(audit_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(render_objective_audit_report(audit), encoding="utf-8")
    return output_path


def _success_criteria() -> list[dict[str, object]]:
    return [
        {
            "id": "wall_street_style_research_and_gate_process",
            "requirement": "Use institutional-style research gates instead of ad hoc scripts.",
            "minimum_evidence": [
                "research_to_paper approved",
                "lookahead, cost, capacity, and research gates represented in readiness manifest",
                "external factor factory portfolio approved when present",
                "research input manifest confirms hk_factor_autoresearch reuse when supplied",
            ],
        },
        {
            "id": "hk_l2_data_reuse",
            "requirement": (
                "Reuse existing HK L2 and Hshare Lab verified data rather than rebuild it."
            ),
            "minimum_evidence": [
                "intraday_l2_features fresh",
                "intraday_l2_features has at least 20 rows",
                "hshare_verified ready",
                "research input manifest confirms Hshare Lab verified reuse when supplied",
            ],
        },
        {
            "id": "tushare_real_data_adapter",
            "requirement": "Use a real Tushare adapter for live reference data.",
            "minimum_evidence": [
                "tushare credential ready",
                "tushare source adapter live_data_ready",
                "security_master and daily_features are fresh when present in readiness",
            ],
        },
        {
            "id": "ifind_real_data_adapter",
            "requirement": "Use real iFind endpoint data or real iFind terminal export data.",
            "minimum_evidence": [
                "ifind credential ready",
                "ifind source adapter live_data_ready",
                "ifind_events contains ifind_real or ifind_real_file rows when file mode is used",
            ],
        },
        {
            "id": "opend_execution_gate",
            "requirement": "Connect to OpenD through safe dry-run/paper/live-gated execution.",
            "minimum_evidence": [
                "OpenD readiness approved for the requested mode",
                "HK stock SIMULATE account available",
                "real quote snapshot captured",
                "submitted ticket response JSONL captured",
            ],
        },
        {
            "id": "million_scale_personal_account_risk",
            "requirement": "Respect a million-level personal account risk budget.",
            "minimum_evidence": [
                "account risk budget in readiness manifest",
                "single-name and strategy notional limits present",
            ],
        },
        {
            "id": "paper_to_live_execution_evidence",
            "requirement": "Require broker-reconciled paper evidence before live promotion.",
            "minimum_evidence": [
                "paper_to_live approved",
                "broker report reconciled to local execution log",
            ],
        },
        {
            "id": "profitable_reconciled_paper_or_live_evidence",
            "requirement": "Demonstrate positive PnL with broker reconciliation before completion.",
            "minimum_evidence": [
                "profitability evidence ready",
                "paper sessions >= 20",
                "net PnL > 0",
                "drawdown within limit",
                "reconciled true",
            ],
        },
    ]


def _prompt_to_artifact_checklist(
    *,
    checks: list[dict[str, object]],
    success_criteria: list[dict[str, object]],
    readiness_manifest_path: Path,
    research_input_manifest_path: Path | None,
    profitability_evidence_path: Path | None,
    ifind_validation_report_path: Path | None,
    ifind_ingestion_status_path: Path | None,
    opend_quote_snapshot_path: Path | None,
    opend_ticket_response_path: Path | None,
    opend_runtime_status_path: Path | None,
    opend_account_status_path: Path | None,
    paper_simulate_status_path: Path | None,
    paper_blocker_report_path: Path | None,
    paper_progress_path: Path | None,
    execution_log_path: Path | None,
    broker_report_path: Path | None,
) -> list[dict[str, object]]:
    check_by_requirement = {str(check["requirement"]): check for check in checks}
    artifacts = {
        "wall_street_style_research_and_gate_process": [
            str(readiness_manifest_path),
            str(research_input_manifest_path) if research_input_manifest_path else None,
            "src/multi_layer_trading_lab/reports/readiness.py",
            "src/multi_layer_trading_lab/research/audit.py",
            "src/multi_layer_trading_lab/research/factor_factory.py",
        ],
        "hk_l2_data_reuse": [
            str(readiness_manifest_path),
            str(research_input_manifest_path) if research_input_manifest_path else None,
            "EXTERNAL_REUSE_MAP.md",
            "src/multi_layer_trading_lab/research/hshare_verified.py",
        ],
        "tushare_real_data_adapter": [
            str(readiness_manifest_path),
            "src/multi_layer_trading_lab/adapters/tushare/client.py",
            "data/lake/security_master",
            "data/lake/daily_features",
        ],
        "ifind_real_data_adapter": [
            str(readiness_manifest_path),
            str(ifind_validation_report_path) if ifind_validation_report_path else None,
            str(ifind_ingestion_status_path) if ifind_ingestion_status_path else None,
            "src/multi_layer_trading_lab/adapters/ifind/client.py",
            "data/lake/ifind_events",
        ],
        "opend_execution_gate": [
            str(readiness_manifest_path),
            str(opend_runtime_status_path) if opend_runtime_status_path else None,
            str(opend_account_status_path) if opend_account_status_path else None,
            str(opend_quote_snapshot_path) if opend_quote_snapshot_path else None,
            str(opend_ticket_response_path) if opend_ticket_response_path else None,
            str(paper_simulate_status_path) if paper_simulate_status_path else None,
            str(paper_blocker_report_path) if paper_blocker_report_path else None,
            "src/multi_layer_trading_lab/execution/opend_tickets.py",
            "src/multi_layer_trading_lab/execution/paper_blocker_report.py",
        ],
        "million_scale_personal_account_risk": [
            str(readiness_manifest_path),
            "configs/personal_trading.yaml",
            "src/multi_layer_trading_lab/risk/profile.py",
        ],
        "paper_to_live_execution_evidence": [
            str(readiness_manifest_path),
            str(execution_log_path) if execution_log_path else None,
            str(broker_report_path) if broker_report_path else None,
            "src/multi_layer_trading_lab/execution/paper_audit.py",
            "src/multi_layer_trading_lab/execution/reconciliation.py",
            "src/multi_layer_trading_lab/execution/session_ledger.py",
        ],
        "profitable_reconciled_paper_or_live_evidence": [
            str(profitability_evidence_path) if profitability_evidence_path else None,
            str(paper_progress_path) if paper_progress_path else None,
            "src/multi_layer_trading_lab/execution/profitability_evidence.py",
            "src/multi_layer_trading_lab/execution/paper_progress.py",
        ],
    }
    commands = {
        "wall_street_style_research_and_gate_process": (
            "research-input-manifest / factor-factory-summary / research-audit / "
            "go-live-readiness"
        ),
        "hk_l2_data_reuse": "research-input-manifest / hshare-verified-audit / go-live-readiness",
        "tushare_real_data_adapter": "fetch-tushare-to-lake --use-real / data-adapter-status",
        "ifind_real_data_adapter": (
            "fetch-ifind-to-lake --use-real or import-ifind-events-file"
        ),
        "opend_execution_gate": (
            "fetch-opend-runtime-status / fetch-opend-quote-snapshot / "
            "export-opend-paper-tickets / "
            "submit-opend-paper-tickets"
        ),
        "million_scale_personal_account_risk": "risk-precheck --account-equity 1000000",
        "paper_to_live_execution_evidence": (
            "paper-session-ledger / paper-audit / build-paper-session-evidence-bundle"
        ),
        "profitable_reconciled_paper_or_live_evidence": (
            "profitability-evidence / paper-progress"
        ),
    }
    checklist: list[dict[str, object]] = []
    for criterion in success_criteria:
        requirement_id = str(criterion["id"])
        check = check_by_requirement.get(requirement_id, {})
        checklist.append(
            {
                "requirement": requirement_id,
                "prompt_requirement": criterion["requirement"],
                "minimum_evidence": criterion["minimum_evidence"],
                "artifacts": [
                    artifact for artifact in artifacts.get(requirement_id, []) if artifact
                ],
                "verification_command": commands.get(requirement_id),
                "status": check.get("status", "blocked"),
                "evidence": check.get("evidence"),
                "failed_reasons": check.get("failed_reasons", []),
                "next_required_action": _next_required_action(requirement_id, check),
            }
        )
    return checklist


def _next_required_action(requirement: str, check: dict[str, object]) -> str:
    if check.get("status") == "passed":
        return "none"
    failed = [
        str(reason)
        for reason in check.get("failed_reasons", [])
        if str(reason)
    ]
    if requirement == "opend_execution_gate":
        blocker_action = _paper_blocker_next_action(check.get("evidence"))
        if blocker_action:
            return blocker_action
        if "stale_paper_blocker_report" in failed:
            return "refresh_paper_blocker_report"
        paper_action = _opend_paper_simulate_next_action(check.get("evidence"))
        if paper_action:
            return paper_action
        if "stale_paper_simulate_status" in failed:
            return "regenerate_paper_simulate_status_from_latest_responses"
        if "opend_kill_switch_enabled" in failed:
            return "clear_opend_kill_switch_then_resubmit_paper_simulate"
        if _has_any(
            failed,
            [
                "missing_opend_account_status",
                "missing_hk_stock_simulate_account",
                "opend_account_not_ready_for_paper_simulate",
            ],
        ):
            return "refresh_or_select_hk_stock_simulate_account"
        if "missing_opend_runtime_status" in failed:
            return "fetch_opend_runtime_status"
        if _has_any(
            failed,
            [
                "missing_opend_quote_snapshot",
                "invalid_opend_quote_snapshot",
                "opend_quote_missing_symbol",
                "opend_quote_missing_lot_size",
                "opend_quote_missing_reference_price",
            ],
        ):
            return "fetch_opend_quote_snapshot"
        if _has_any(
            failed,
            [
                "missing_opend_ticket_response",
                "empty_opend_ticket_response",
                "missing_submitted_opend_ticket_response",
                "missing_submitted_responses",
                "paper_simulate_submit_errors_present",
            ],
        ):
            return "submit_opend_paper_tickets_with_simulate_enabled"
        return "refresh_opend_execution_evidence"
    if requirement == "paper_to_live_execution_evidence":
        if _has_any(
            failed,
            [
                "insufficient_broker_backed_paper_sessions",
                "insufficient_inferred_sessions",
            ],
        ):
            remaining = _remaining_broker_backed_sessions(check.get("evidence"))
            if remaining is not None and remaining > 0:
                return f"collect_{remaining}_remaining_broker_reconciled_paper_sessions"
        return "collect_broker_reconciled_paper_sessions"
    if requirement == "profitable_reconciled_paper_or_live_evidence":
        if "profitability_not_reconciled_to_broker" in failed:
            return "reconcile_profitability_evidence_to_broker"
        if _has_any(
            failed,
            [
                "missing_profitability_broker_report_rows",
                "missing_profitability_execution_log_rows",
                "missing_profitability_inferred_session_count",
                "missing_profitability_paper_sessions",
                "missing_profitability_session_dates",
                "profitability_broker_report_rows_mismatch",
                "profitability_execution_log_rows_mismatch",
                "profitability_inferred_session_count_mismatch",
                "profitability_session_count_mismatch",
                "profitability_session_dates_mismatch",
                "stale_profitability_evidence",
            ],
        ):
            return "refresh_profitability_evidence"
        if "stale_paper_progress" in failed:
            return "refresh_paper_progress"
        if _has_any(
            failed,
            [
                "insufficient_inferred_paper_sessions",
                "insufficient_profitable_paper_sessions",
                "insufficient_inferred_sessions",
                "paper_sessions_exceed_inferred_sessions",
                "paper_sessions_remaining",
            ],
        ):
            remaining = _remaining_paper_sessions(check.get("evidence"))
            if remaining is not None and remaining > 0:
                return f"collect_{remaining}_remaining_broker_reconciled_paper_sessions"
            return "collect_remaining_broker_reconciled_paper_sessions"
        if _has_any(failed, ["net_pnl_not_positive", "drawdown_breached"]):
            return "continue_research_and_paper_iteration_until_profitable"
        return "refresh_profitability_evidence"
    if requirement == "ifind_real_data_adapter":
        return "refresh_or_import_real_ifind_events"
    if requirement == "tushare_real_data_adapter":
        if _has_any(
            failed,
            [
                "security_master_not_fresh",
                "security_master_stub",
                "security_master_empty",
                "daily_features_not_fresh",
                "daily_features_stub",
                "daily_features_empty",
            ],
        ):
            return "refresh_tushare_real_lake_datasets"
        return "refresh_tushare_real_adapter_status"
    if requirement == "hk_l2_data_reuse":
        if _has_any(
            failed,
            [
                "missing_research_input_manifest",
                "missing_hshare_lab_reuse_check",
                "missing_hshare_stage_reuse_input",
                "missing_hshare_verified_reuse_input",
                "hshare_lab_v2_missing_paths",
                "hshare_lab_v2_not_ready",
                "hshare_stage_reuse_input_empty",
                "hshare_stage_reuse_input_missing",
                "hshare_verified_manifest_empty",
                "hshare_verified_parquet_empty",
                "hshare_verified_reuse_input_missing",
            ],
        ):
            return "refresh_research_input_manifest"
        return "refresh_hshare_verified_and_l2_freshness"
    if requirement == "wall_street_style_research_and_gate_process":
        if _has_any(
            failed,
            [
                "external_factor_portfolio_not_approved",
                "factor_registry_reuse_input_empty",
                "factor_registry_reuse_input_missing",
                "factor_runs_reuse_input_empty",
                "factor_runs_reuse_input_missing",
                "hk_factor_autoresearch_missing_paths",
                "hk_factor_autoresearch_not_ready",
                "missing_factor_factory_reuse_check",
                "missing_factor_registry_reuse_input",
                "missing_factor_runs_reuse_input",
                "missing_research_input_manifest",
            ],
        ):
            return "refresh_factor_factory_reuse_evidence"
        return "rerun_research_audit_and_readiness_gate"
    if requirement == "million_scale_personal_account_risk":
        return "rerun_million_scale_risk_precheck"
    return "inspect_blocked_requirement"


def _opend_paper_simulate_next_action(evidence: object) -> str | None:
    if not isinstance(evidence, dict):
        return None
    runtime = evidence.get("runtime")
    if not isinstance(runtime, dict):
        return None
    if runtime.get("paper_simulate_status_stale") is True:
        return None
    paper_status = runtime.get("paper_simulate_status")
    if not isinstance(paper_status, dict):
        return None
    action = paper_status.get("next_required_action")
    return str(action) if action else None


def _paper_blocker_next_action(evidence: object) -> str | None:
    if not isinstance(evidence, dict):
        return None
    runtime = evidence.get("runtime")
    if not isinstance(runtime, dict):
        return None
    blocker = runtime.get("paper_blocker_report")
    if not isinstance(blocker, dict):
        return None
    if blocker.get("paper_blocker_report_stale") is True:
        return None
    action = blocker.get("next_required_action")
    return str(action) if action else None


def _has_any(failed: list[str], reasons: list[str]) -> bool:
    return any(reason in failed for reason in reasons)


def _remaining_paper_sessions(evidence: object, target_sessions: int = 20) -> int | None:
    if not isinstance(evidence, dict):
        return None
    progress = evidence.get("paper_progress")
    if isinstance(progress, dict):
        try:
            return max(int(progress.get("sessions_remaining", 0) or 0), 0)
        except (TypeError, ValueError):
            pass
    try:
        target = int(evidence.get("target_sessions", target_sessions) or target_sessions)
        sessions = int(evidence.get("paper_sessions", 0) or 0)
    except (TypeError, ValueError):
        return None
    return max(target - sessions, 0)


def _remaining_broker_backed_sessions(
    evidence: object,
    target_sessions: int = 20,
) -> int | None:
    if not isinstance(evidence, dict):
        return None
    ledger = evidence.get("paper_session_ledger")
    if not isinstance(ledger, dict):
        return None
    try:
        target = int(evidence.get("target_sessions", target_sessions) or target_sessions)
        sessions = int(ledger.get("inferred_session_count", 0) or 0)
    except (TypeError, ValueError):
        return None
    return max(target - sessions, 0)


def _format_list(value: object) -> str:
    if isinstance(value, list):
        return ", ".join(str(item) for item in value) if value else "none"
    return str(value)


def _md_cell(value: object) -> str:
    if isinstance(value, list):
        text = "<br>".join(str(item) for item in value) if value else ""
    elif value is None:
        text = ""
    else:
        text = str(value)
    return text.replace("|", "\\|").replace("\n", "<br>")


def _next_required_evidence(requirement: str) -> str:
    guidance = {
        "ifind_real_data_adapter": (
            "Import a real iFind terminal export with import-ifind-events-file, or configure "
            "IFIND_EVENTS_ENDPOINT and run fetch-ifind-to-lake --use-real."
        ),
        "paper_to_live_execution_evidence": (
            "Run real OpenD paper sessions, collect Futu broker reports, and pass paper-audit "
            "or build-paper-session-evidence-bundle with non-dry-run evidence."
        ),
        "profitable_reconciled_paper_or_live_evidence": (
            "Generate profitability-evidence from broker-reconciled paper/live execution logs "
            "with at least 20 sessions, positive net PnL, and drawdown within limits."
        ),
        "opend_execution_gate": (
            "Capture an OpenD quote snapshot and at least one submitted ticket response "
            "JSONL row from the execution API, with a ready HK stock SIMULATE account."
        ),
        "hk_l2_data_reuse": (
            "Refresh Hshare verified evidence and intraday_l2_features freshness in "
            "go-live-readiness, and refresh research-input-manifest when external reuse "
            "paths are missing."
        ),
        "tushare_real_data_adapter": (
            "Run fetch-tushare-to-lake --use-real and confirm data-adapter-status "
            "reports live data with non-stub security_master and daily_features rows."
        ),
        "wall_street_style_research_and_gate_process": (
            "Pass research-audit and go-live-readiness research_to_paper gates, with "
            "factor factory reuse evidence from research-input-manifest."
        ),
        "million_scale_personal_account_risk": (
            "Include account_risk_budget from risk-precheck/go-live-readiness."
        ),
    }
    return guidance.get(requirement, "Provide direct evidence for this requirement.")
