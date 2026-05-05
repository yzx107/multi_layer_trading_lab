from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class ObjectiveAuditInput:
    readiness_manifest_path: Path
    output_path: Path
    profitability_evidence_path: Path | None = None
    ifind_validation_report_path: Path | None = None
    ifind_ingestion_status_path: Path | None = None
    opend_quote_snapshot_path: Path | None = None
    opend_ticket_response_path: Path | None = None
    opend_runtime_status_path: Path | None = None
    paper_simulate_status_path: Path | None = None


def _load_json(path: Path) -> dict[str, object]:
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def _status(approved: bool) -> str:
    return "passed" if approved else "blocked"


def _profitability_ready(path: Path | None) -> tuple[bool, dict[str, object] | None, list[str]]:
    if path is None:
        return False, None, ["missing_profitability_evidence_path"]
    if not path.exists():
        return False, None, ["missing_profitability_evidence"]
    payload = _load_json(path)
    failed: list[str] = []
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


def _opend_runtime_ready(
    quote_snapshot_path: Path | None,
    ticket_response_path: Path | None,
    runtime_status_path: Path | None = None,
    paper_simulate_status_path: Path | None = None,
) -> tuple[bool, dict[str, object], list[str]]:
    failed: list[str] = []
    evidence: dict[str, object] = {
        "quote_snapshot_path": str(quote_snapshot_path) if quote_snapshot_path else None,
        "ticket_response_path": str(ticket_response_path) if ticket_response_path else None,
        "runtime_status_path": str(runtime_status_path) if runtime_status_path else None,
        "paper_simulate_status_path": (
            str(paper_simulate_status_path) if paper_simulate_status_path else None
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

    if paper_simulate_status_path is not None and paper_simulate_status_path.exists():
        paper_status = _load_json(paper_simulate_status_path)
        evidence["paper_simulate_status"] = paper_status
        if paper_status.get("ready_for_session_collection") is not True:
            failed.extend(str(reason) for reason in paper_status.get("failed_reasons", []))

    return not failed, evidence, list(dict.fromkeys(failed))


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


def build_objective_audit(input_data: ObjectiveAuditInput) -> dict[str, object]:
    readiness = _load_json(input_data.readiness_manifest_path)
    ifind_validation_payload = _ifind_validation_payload(input_data.ifind_validation_report_path)
    ifind_ingestion_payload = _load_json(input_data.ifind_ingestion_status_path) if (
        input_data.ifind_ingestion_status_path is not None
        and input_data.ifind_ingestion_status_path.exists()
    ) else None
    profitability_ready, profitability_payload, profitability_failed = _profitability_ready(
        input_data.profitability_evidence_path
    )
    opend_runtime_ready, opend_runtime_evidence, opend_runtime_failed = _opend_runtime_ready(
        input_data.opend_quote_snapshot_path,
        input_data.opend_ticket_response_path,
        input_data.opend_runtime_status_path,
        input_data.paper_simulate_status_path,
    )
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

    hk_l2_ready = (
        data_freshness.get("intraday_l2_features", {}).get("status") == "fresh"
        and readiness.get("hshare_verified", {}).get("ready") is True
    )
    tushare_ready = (
        data_sources.get("tushare", {}).get("ready") is True
        and source_adapters.get("tushare", {}).get("live_data_ready") is True
    )
    ifind_ready = (
        data_sources.get("ifind", {}).get("ready") is True
        and source_adapters.get("ifind", {}).get("live_data_ready") is True
    )
    opend_ready = readiness.get("execution", {}).get("opend_ready") is True and opend_runtime_ready
    personal_risk_ready = bool(readiness.get("account_risk_budget"))
    research_ready = readiness.get("research_to_paper", {}).get("approved") is True
    paper_ready = readiness.get("paper_to_live", {}).get("approved") is True
    go_live_ready = readiness.get("go_live_approved") is True

    checks = [
        {
            "requirement": "wall_street_style_research_and_gate_process",
            "status": _status(research_ready),
            "evidence": readiness.get("research_to_paper", {}),
        },
        {
            "requirement": "hk_l2_data_reuse",
            "status": _status(hk_l2_ready),
            "evidence": {
                "intraday_l2_features": data_freshness.get("intraday_l2_features", {}),
                "hshare_verified": readiness.get("hshare_verified", {}),
            },
        },
        {
            "requirement": "tushare_real_data_adapter",
            "status": _status(tushare_ready),
            "evidence": {
                "data_source": data_sources.get("tushare", {}),
                "source_adapter": source_adapters.get("tushare", {}),
            },
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
            "evidence": readiness.get("paper_to_live", {}),
        },
        {
            "requirement": "profitable_reconciled_paper_or_live_evidence",
            "status": _status(profitability_ready),
            "evidence": profitability_payload,
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
        "paper_simulate_status_path": (
            str(input_data.paper_simulate_status_path)
            if input_data.paper_simulate_status_path is not None
            else None
        ),
        "checks": checks,
        "prompt_to_artifact_checklist": _prompt_to_artifact_checklist(
            checks=checks,
            success_criteria=success_criteria,
            readiness_manifest_path=input_data.readiness_manifest_path,
            profitability_evidence_path=input_data.profitability_evidence_path,
            ifind_validation_report_path=input_data.ifind_validation_report_path,
            ifind_ingestion_status_path=input_data.ifind_ingestion_status_path,
            opend_quote_snapshot_path=input_data.opend_quote_snapshot_path,
            opend_ticket_response_path=input_data.opend_ticket_response_path,
            opend_runtime_status_path=input_data.opend_runtime_status_path,
            paper_simulate_status_path=input_data.paper_simulate_status_path,
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
        "| Requirement | Status | Verification | Artifacts | Failed Reasons |",
        "| --- | --- | --- | --- | --- |",
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
            ],
        },
        {
            "id": "hk_l2_data_reuse",
            "requirement": (
                "Reuse existing HK L2 and Hshare Lab verified data rather than rebuild it."
            ),
            "minimum_evidence": [
                "intraday_l2_features fresh",
                "hshare_verified ready",
            ],
        },
        {
            "id": "tushare_real_data_adapter",
            "requirement": "Use a real Tushare adapter for live reference data.",
            "minimum_evidence": [
                "tushare credential ready",
                "tushare source adapter live_data_ready",
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
                "real quote snapshot captured",
                "ticket response JSONL captured",
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
    profitability_evidence_path: Path | None,
    ifind_validation_report_path: Path | None,
    ifind_ingestion_status_path: Path | None,
    opend_quote_snapshot_path: Path | None,
    opend_ticket_response_path: Path | None,
    opend_runtime_status_path: Path | None,
    paper_simulate_status_path: Path | None,
) -> list[dict[str, object]]:
    check_by_requirement = {str(check["requirement"]): check for check in checks}
    artifacts = {
        "wall_street_style_research_and_gate_process": [
            str(readiness_manifest_path),
            "src/multi_layer_trading_lab/reports/readiness.py",
            "src/multi_layer_trading_lab/research/audit.py",
        ],
        "hk_l2_data_reuse": [
            str(readiness_manifest_path),
            "EXTERNAL_REUSE_MAP.md",
            "src/multi_layer_trading_lab/research/hshare_verified.py",
        ],
        "tushare_real_data_adapter": [
            str(readiness_manifest_path),
            "src/multi_layer_trading_lab/adapters/tushare/client.py",
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
            str(opend_quote_snapshot_path) if opend_quote_snapshot_path else None,
            str(opend_ticket_response_path) if opend_ticket_response_path else None,
            str(paper_simulate_status_path) if paper_simulate_status_path else None,
            "src/multi_layer_trading_lab/execution/opend_tickets.py",
        ],
        "million_scale_personal_account_risk": [
            str(readiness_manifest_path),
            "configs/personal_trading.yaml",
            "src/multi_layer_trading_lab/risk/profile.py",
        ],
        "paper_to_live_execution_evidence": [
            str(readiness_manifest_path),
            "src/multi_layer_trading_lab/execution/paper_audit.py",
            "src/multi_layer_trading_lab/execution/reconciliation.py",
        ],
        "profitable_reconciled_paper_or_live_evidence": [
            str(profitability_evidence_path) if profitability_evidence_path else None,
            "src/multi_layer_trading_lab/execution/profitability_evidence.py",
        ],
    }
    commands = {
        "wall_street_style_research_and_gate_process": "research-audit / go-live-readiness",
        "hk_l2_data_reuse": "hshare-verified-audit / go-live-readiness",
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
        "paper_to_live_execution_evidence": "paper-audit / build-paper-session-evidence-bundle",
        "profitable_reconciled_paper_or_live_evidence": "profitability-evidence",
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
            }
        )
    return checklist


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
            "Capture an OpenD quote snapshot and ticket response JSONL from the execution API."
        ),
        "hk_l2_data_reuse": (
            "Refresh Hshare verified evidence and intraday_l2_features freshness in "
            "go-live-readiness."
        ),
        "tushare_real_data_adapter": (
            "Run fetch-tushare-to-lake --use-real and confirm data-adapter-status "
            "reports live data."
        ),
        "wall_street_style_research_and_gate_process": (
            "Pass research-audit and go-live-readiness research_to_paper gates."
        ),
        "million_scale_personal_account_risk": (
            "Include account_risk_budget from risk-precheck/go-live-readiness."
        ),
    }
    return guidance.get(requirement, "Provide direct evidence for this requirement.")
