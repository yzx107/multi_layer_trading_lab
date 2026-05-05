from __future__ import annotations

import json
import os

from multi_layer_trading_lab.reports.objective_audit import (
    ObjectiveAuditInput,
    build_objective_audit,
    render_objective_audit_report,
)


def _write_ready_opend_account_status(path) -> None:
    path.write_text(
        json.dumps(
            {
                "configured_acc_id": 9100000000000001,
                "accounts": [
                    {
                        "acc_id": 9100000000000001,
                        "card_num": "TESTCARD0001",
                        "uni_card_num": "TESTUNI0001",
                        "trd_env": "REAL",
                    },
                    {
                        "acc_id": 92000002,
                        "card_num": "N/A",
                        "uni_card_num": "N/A",
                        "trd_env": "SIMULATE",
                        "sim_acc_type": "STOCK",
                        "trdmarket_auth": ["HK"],
                    },
                ],
                "ready_for_paper_simulate": True,
                "simulate_account_count": 1,
                "hk_stock_simulate_account_count": 1,
                "failed_reasons": [],
            }
        ),
        encoding="utf-8",
    )


def test_objective_audit_blocks_without_profitability_and_live_adapters(tmp_path) -> None:
    readiness = tmp_path / "readiness.json"
    ifind_validation = tmp_path / "ifind_validation.json"
    ifind_ingestion = tmp_path / "ifind_ingestion.json"
    output = tmp_path / "audit.json"
    readiness.write_text(
        json.dumps(
            {
                "go_live_approved": False,
                "account_risk_budget": {"account_equity": 1_000_000},
                "data_sources": [
                    {"source": "tushare", "ready": True},
                    {"source": "ifind", "ready": True},
                ],
                "source_adapters": [
                    {
                        "source": "tushare",
                        "adapter_status": "partial_real_adapter",
                        "live_data_ready": False,
                    },
                    {
                        "source": "ifind",
                        "adapter_status": "stub_adapter",
                        "live_data_ready": False,
                    },
                ],
                "data_freshness": [
                    {"dataset": "intraday_l2_features", "status": "fresh", "rows": 100}
                ],
                "hshare_verified": {"ready": True},
                "execution": {"opend_ready": False},
                "research_to_paper": {"approved": True},
                "paper_to_live": {"approved": False},
            }
        ),
        encoding="utf-8",
    )
    ifind_validation.write_text(
        json.dumps(
            {
                "valid": False,
                "rows": 0,
                "failed_reasons": ["ifind_file_validate_failed:ValueError"],
            }
        ),
        encoding="utf-8",
    )
    ifind_ingestion.write_text(
        json.dumps(
            {
                "ready": False,
                "ifind_stub_rows": 2,
                "ifind_real_rows": 0,
                "ifind_real_file_rows": 0,
                "failed_reasons": ["missing_real_ifind_lake_rows"],
            }
        ),
        encoding="utf-8",
    )

    audit = build_objective_audit(
        ObjectiveAuditInput(
            readiness_manifest_path=readiness,
            output_path=output,
            ifind_validation_report_path=ifind_validation,
            ifind_ingestion_status_path=ifind_ingestion,
        )
    )

    assert audit["objective_achieved"] is False
    assert "tushare_real_data_adapter" in audit["blocked_requirements"]
    assert "ifind_real_data_adapter" in audit["blocked_requirements"]
    assert "opend_execution_gate" in audit["blocked_requirements"]
    assert "paper_to_live_execution_evidence" in audit["blocked_requirements"]
    assert "profitable_reconciled_paper_or_live_evidence" in audit["blocked_requirements"]
    assert audit["completion_decision"]["status"] == "not_achieved"
    assert len(audit["prompt_to_artifact_checklist"]) == 8
    ifind_item = [
        item
        for item in audit["prompt_to_artifact_checklist"]
        if item["requirement"] == "ifind_real_data_adapter"
    ][0]
    assert "import-ifind-events-file" in ifind_item["verification_command"]
    assert str(ifind_validation) in ifind_item["artifacts"]
    assert str(ifind_ingestion) in ifind_item["artifacts"]
    assert "data/lake/ifind_events" in ifind_item["artifacts"]
    ifind_check = [
        check
        for check in audit["checks"]
        if check["requirement"] == "ifind_real_data_adapter"
    ][0]
    assert "ifind_stub_adapter" in ifind_check["failed_reasons"]
    assert "ifind_file_validate_failed:ValueError" in ifind_check["failed_reasons"]
    assert ifind_check["evidence"]["ingestion_status"]["ifind_stub_rows"] == 2
    assert output.exists()


def test_objective_audit_marks_missing_ifind_validation_report(tmp_path) -> None:
    readiness = tmp_path / "readiness.json"
    ifind_validation = tmp_path / "missing_ifind_validation.json"
    output = tmp_path / "audit.json"
    readiness.write_text(
        json.dumps(
            {
                "go_live_approved": False,
                "account_risk_budget": {"account_equity": 1_000_000},
                "data_sources": [
                    {"source": "tushare", "ready": True},
                    {"source": "ifind", "ready": True},
                ],
                "source_adapters": [
                    {
                        "source": "tushare",
                        "adapter_status": "real_adapter",
                        "live_data_ready": True,
                    },
                    {"source": "ifind", "adapter_status": "real_adapter", "live_data_ready": True},
                ],
                "data_freshness": [
                    {"dataset": "intraday_l2_features", "status": "fresh", "rows": 100}
                ],
                "hshare_verified": {"ready": True},
                "execution": {"opend_ready": False},
                "research_to_paper": {"approved": True},
                "paper_to_live": {"approved": False},
            }
        ),
        encoding="utf-8",
    )

    audit = build_objective_audit(
        ObjectiveAuditInput(
            readiness_manifest_path=readiness,
            output_path=output,
            ifind_validation_report_path=ifind_validation,
        )
    )
    ifind_check = [
        check
        for check in audit["checks"]
        if check["requirement"] == "ifind_real_data_adapter"
    ][0]

    assert "missing_ifind_validation_report" in ifind_check["failed_reasons"]
    assert ifind_check["evidence"]["validation_report"]["report_path"] == str(ifind_validation)


def test_objective_audit_requires_positive_reconciled_profitability(tmp_path) -> None:
    readiness = tmp_path / "readiness.json"
    profitability = tmp_path / "profitability.json"
    quote = tmp_path / "quote.json"
    responses = tmp_path / "responses.jsonl"
    runtime = tmp_path / "runtime.json"
    account = tmp_path / "account.json"
    output = tmp_path / "audit.json"
    readiness.write_text(
        json.dumps(
            {
                "go_live_approved": True,
                "account_risk_budget": {"account_equity": 1_000_000},
                "data_sources": [
                    {"source": "tushare", "ready": True},
                    {"source": "ifind", "ready": True},
                ],
                "source_adapters": [
                    {
                        "source": "tushare",
                        "adapter_status": "real_adapter",
                        "live_data_ready": True,
                    },
                    {"source": "ifind", "adapter_status": "real_adapter", "live_data_ready": True},
                ],
                "data_freshness": [
                    {"dataset": "intraday_l2_features", "status": "fresh", "rows": 100}
                ],
                "hshare_verified": {"ready": True},
                "execution": {"opend_ready": True},
                "research_to_paper": {"approved": True},
                "paper_to_live": {"approved": True},
            }
        ),
        encoding="utf-8",
    )
    profitability.write_text(
        json.dumps(
            {
                "paper_sessions": 20,
                "net_pnl": 1200.0,
                "max_drawdown": -3000.0,
                "max_allowed_drawdown": 10_000.0,
                "reconciled": True,
            }
        ),
        encoding="utf-8",
    )
    quote.write_text(
        json.dumps({"quote": {"symbol": "HK.00700", "lot_size": 100, "last_price": 320.0}}),
        encoding="utf-8",
    )
    responses.write_text(
        '{"ticket_id":"paper-001","response":{"submitted":true}}\n',
        encoding="utf-8",
    )
    runtime.write_text(
        json.dumps({"ready_for_order_submission": True, "failed_reasons": []}),
        encoding="utf-8",
    )
    _write_ready_opend_account_status(account)

    audit = build_objective_audit(
        ObjectiveAuditInput(
            readiness_manifest_path=readiness,
            output_path=output,
            profitability_evidence_path=profitability,
            opend_quote_snapshot_path=quote,
            opend_ticket_response_path=responses,
            opend_runtime_status_path=runtime,
            opend_account_status_path=account,
        )
    )

    assert audit["objective_achieved"] is True
    assert audit["blocked_requirements"] == []
    assert audit["completion_decision"]["status"] == "achieved"
    assert all(item["status"] == "passed" for item in audit["prompt_to_artifact_checklist"])
    opend_check = [
        check for check in audit["checks"] if check["requirement"] == "opend_execution_gate"
    ][0]
    account_status = opend_check["evidence"]["runtime"]["account_status"]
    assert account_status["configured_acc_id"] == "***0001"
    assert account_status["accounts"][0]["card_num"] == "***0001"
    assert account_status["accounts"][1]["acc_id"] == "***0002"


def test_objective_audit_propagates_profitability_failed_reasons(tmp_path) -> None:
    readiness = tmp_path / "readiness.json"
    profitability = tmp_path / "profitability.json"
    quote = tmp_path / "quote.json"
    responses = tmp_path / "responses.jsonl"
    runtime = tmp_path / "runtime.json"
    account = tmp_path / "account.json"
    output = tmp_path / "audit.json"
    readiness.write_text(
        json.dumps(
            {
                "go_live_approved": True,
                "account_risk_budget": {"account_equity": 1_000_000},
                "data_sources": [
                    {"source": "tushare", "ready": True},
                    {"source": "ifind", "ready": True},
                ],
                "source_adapters": [
                    {
                        "source": "tushare",
                        "adapter_status": "real_adapter",
                        "live_data_ready": True,
                    },
                    {"source": "ifind", "adapter_status": "real_adapter", "live_data_ready": True},
                ],
                "data_freshness": [
                    {"dataset": "intraday_l2_features", "status": "fresh", "rows": 100}
                ],
                "hshare_verified": {"ready": True},
                "execution": {"opend_ready": True},
                "research_to_paper": {"approved": True},
                "paper_to_live": {"approved": True},
            }
        ),
        encoding="utf-8",
    )
    profitability.write_text(
        json.dumps(
            {
                "ready": False,
                "paper_sessions": 3,
                "net_pnl": 0.0,
                "max_drawdown": -10.0,
                "max_allowed_drawdown": 10_000.0,
                "reconciled": True,
                "failed_reasons": [
                    "dry_run_execution_log_not_real_paper",
                    "net_pnl_not_positive",
                ],
            }
        ),
        encoding="utf-8",
    )
    quote.write_text(
        json.dumps({"quote": {"symbol": "HK.00700", "lot_size": 100, "last_price": 320.0}}),
        encoding="utf-8",
    )
    responses.write_text(
        '{"ticket_id":"paper-001","response":{"submitted":true}}\n',
        encoding="utf-8",
    )
    runtime.write_text(
        json.dumps({"ready_for_order_submission": True, "failed_reasons": []}),
        encoding="utf-8",
    )
    _write_ready_opend_account_status(account)

    audit = build_objective_audit(
        ObjectiveAuditInput(
            readiness_manifest_path=readiness,
            output_path=output,
            profitability_evidence_path=profitability,
            opend_quote_snapshot_path=quote,
            opend_ticket_response_path=responses,
            opend_runtime_status_path=runtime,
            opend_account_status_path=account,
        )
    )
    profit_check = [
        check
        for check in audit["checks"]
        if check["requirement"] == "profitable_reconciled_paper_or_live_evidence"
    ][0]

    assert audit["objective_achieved"] is False
    assert "dry_run_execution_log_not_real_paper" in profit_check["failed_reasons"]
    assert profit_check["failed_reasons"].count("net_pnl_not_positive") == 1
    checklist_item = [
        item
        for item in audit["prompt_to_artifact_checklist"]
        if item["requirement"] == "profitable_reconciled_paper_or_live_evidence"
    ][0]
    assert (
        checklist_item["next_required_action"]
        == "collect_17_remaining_broker_reconciled_paper_sessions"
    )


def test_objective_audit_report_renders_blockers_and_next_evidence(tmp_path) -> None:
    readiness = tmp_path / "readiness.json"
    output = tmp_path / "audit.json"
    readiness.write_text(
        json.dumps(
            {
                "go_live_approved": False,
                "account_risk_budget": {"account_equity": 1_000_000},
                "data_sources": [
                    {"source": "tushare", "ready": True},
                    {"source": "ifind", "ready": True},
                ],
                "source_adapters": [
                    {
                        "source": "tushare",
                        "adapter_status": "real_adapter",
                        "live_data_ready": True,
                    },
                    {"source": "ifind", "adapter_status": "stub_adapter", "live_data_ready": False},
                ],
                "data_freshness": [
                    {"dataset": "intraday_l2_features", "status": "fresh", "rows": 100}
                ],
                "hshare_verified": {"ready": True},
                "execution": {"opend_ready": True},
                "research_to_paper": {"approved": True},
                "paper_to_live": {"approved": False},
            }
        ),
        encoding="utf-8",
    )

    audit = build_objective_audit(
        ObjectiveAuditInput(readiness_manifest_path=readiness, output_path=output)
    )
    report = render_objective_audit_report(audit)

    assert "# Objective Completion Audit" in report
    assert "Status: not_achieved" in report
    assert "| Requirement | Status | Next Action |" in report
    assert "ifind_real_data_adapter" in report
    assert "refresh_or_import_real_ifind_events" in report
    assert "import-ifind-events-file" in report
    assert "profitable_reconciled_paper_or_live_evidence" in report
    paper_check = [
        check
        for check in audit["checks"]
        if check["requirement"] == "paper_to_live_execution_evidence"
    ][0]
    assert "paper_to_live_not_approved" in paper_check["failed_reasons"]


def test_objective_audit_uses_paper_session_ledger_for_paper_to_live(
    tmp_path,
) -> None:
    readiness = tmp_path / "readiness.json"
    execution_log = tmp_path / "execution.jsonl"
    broker_report = tmp_path / "broker.json"
    output = tmp_path / "audit.json"
    readiness.write_text(
        json.dumps(
            {
                "go_live_approved": False,
                "account_risk_budget": {"account_equity": 1_000_000},
                "data_sources": [
                    {"source": "tushare", "ready": True},
                    {"source": "ifind", "ready": True},
                ],
                "source_adapters": [
                    {
                        "source": "tushare",
                        "adapter_status": "real_adapter",
                        "live_data_ready": True,
                    },
                    {"source": "ifind", "adapter_status": "real_adapter", "live_data_ready": True},
                ],
                "data_freshness": [
                    {"dataset": "intraday_l2_features", "status": "fresh", "rows": 100}
                ],
                "hshare_verified": {"ready": True},
                "execution": {"opend_ready": False},
                "research_to_paper": {"approved": True},
                "paper_to_live": {"approved": False, "failed_reasons": ["not_evaluated"]},
            }
        ),
        encoding="utf-8",
    )
    execution_log.write_text(
        json.dumps({"order_id": "ord-1", "trade_date": "2026-05-05", "dry_run": False})
        + "\n",
        encoding="utf-8",
    )
    broker_report.write_text(
        json.dumps([{"local_order_id": "ord-1", "updated_time": "2026-05-05 10:00:00"}]),
        encoding="utf-8",
    )

    audit = build_objective_audit(
        ObjectiveAuditInput(
            readiness_manifest_path=readiness,
            output_path=output,
            execution_log_path=execution_log,
            broker_report_path=broker_report,
        )
    )
    paper_check = [
        check
        for check in audit["checks"]
        if check["requirement"] == "paper_to_live_execution_evidence"
    ][0]
    checklist_item = [
        item
        for item in audit["prompt_to_artifact_checklist"]
        if item["requirement"] == "paper_to_live_execution_evidence"
    ][0]

    assert paper_check["status"] == "blocked"
    assert "not_evaluated" in paper_check["failed_reasons"]
    assert "insufficient_broker_backed_paper_sessions" in paper_check["failed_reasons"]
    assert (
        paper_check["evidence"]["paper_session_ledger"]["inferred_session_count"]
        == 1
    )
    assert str(execution_log) in checklist_item["artifacts"]
    assert str(broker_report) in checklist_item["artifacts"]
    assert (
        checklist_item["next_required_action"]
        == "collect_19_remaining_broker_reconciled_paper_sessions"
    )


def test_objective_audit_uses_paper_blocker_report_for_opend_next_action(
    tmp_path,
) -> None:
    readiness = tmp_path / "readiness.json"
    profitability = tmp_path / "profitability.json"
    quote = tmp_path / "quote.json"
    responses = tmp_path / "responses.jsonl"
    runtime = tmp_path / "runtime.json"
    account = tmp_path / "account.json"
    blocker = tmp_path / "paper_blocker.json"
    output = tmp_path / "audit.json"
    readiness.write_text(
        json.dumps(
            {
                "go_live_approved": True,
                "account_risk_budget": {"account_equity": 1_000_000},
                "data_sources": [
                    {"source": "tushare", "ready": True},
                    {"source": "ifind", "ready": True},
                ],
                "source_adapters": [
                    {
                        "source": "tushare",
                        "adapter_status": "real_adapter",
                        "live_data_ready": True,
                    },
                    {"source": "ifind", "adapter_status": "real_adapter", "live_data_ready": True},
                ],
                "data_freshness": [
                    {"dataset": "intraday_l2_features", "status": "fresh", "rows": 100}
                ],
                "hshare_verified": {"ready": True},
                "execution": {"opend_ready": True},
                "research_to_paper": {"approved": True},
                "paper_to_live": {"approved": True},
            }
        ),
        encoding="utf-8",
    )
    profitability.write_text(
        json.dumps(
            {
                "paper_sessions": 20,
                "net_pnl": 1200.0,
                "max_drawdown": -3000.0,
                "max_allowed_drawdown": 10_000.0,
                "reconciled": True,
            }
        ),
        encoding="utf-8",
    )
    quote.write_text(
        json.dumps({"quote": {"symbol": "HK.00700", "lot_size": 100, "last_price": 320.0}}),
        encoding="utf-8",
    )
    responses.write_text(
        '{"ticket_id":"paper-001","response":{"submitted":true}}\n',
        encoding="utf-8",
    )
    runtime.write_text(
        json.dumps({"ready_for_order_submission": True, "failed_reasons": []}),
        encoding="utf-8",
    )
    _write_ready_opend_account_status(account)
    blocker.write_text(
        json.dumps(
            {
                "ready_for_next_session": False,
                "next_required_action": "wait_next_trade_date",
                "next_session_failed_reasons": [
                    "paper_calendar_action:wait_next_trade_date"
                ],
            }
        ),
        encoding="utf-8",
    )

    audit = build_objective_audit(
        ObjectiveAuditInput(
            readiness_manifest_path=readiness,
            output_path=output,
            profitability_evidence_path=profitability,
            opend_quote_snapshot_path=quote,
            opend_ticket_response_path=responses,
            opend_runtime_status_path=runtime,
            opend_account_status_path=account,
            paper_blocker_report_path=blocker,
        )
    )
    opend_check = [
        check for check in audit["checks"] if check["requirement"] == "opend_execution_gate"
    ][0]
    checklist_item = [
        item
        for item in audit["prompt_to_artifact_checklist"]
        if item["requirement"] == "opend_execution_gate"
    ][0]

    assert audit["objective_achieved"] is False
    assert "opend_execution_gate" in audit["blocked_requirements"]
    assert "paper_calendar_action:wait_next_trade_date" in opend_check["failed_reasons"]
    assert (
        opend_check["evidence"]["runtime"]["paper_blocker_report"][
            "ready_for_next_session"
        ]
        is False
    )
    assert str(blocker) in checklist_item["artifacts"]
    assert checklist_item["next_required_action"] == "wait_next_trade_date"


def test_objective_audit_blocks_stale_paper_blocker_report(tmp_path) -> None:
    readiness = tmp_path / "readiness.json"
    profitability = tmp_path / "profitability.json"
    quote = tmp_path / "quote.json"
    responses = tmp_path / "responses.jsonl"
    runtime = tmp_path / "runtime.json"
    account = tmp_path / "account.json"
    blocker = tmp_path / "paper_blocker.json"
    output = tmp_path / "audit.json"
    readiness.write_text(
        json.dumps(
            {
                "go_live_approved": True,
                "account_risk_budget": {"account_equity": 1_000_000},
                "data_sources": [
                    {"source": "tushare", "ready": True},
                    {"source": "ifind", "ready": True},
                ],
                "source_adapters": [
                    {
                        "source": "tushare",
                        "adapter_status": "real_adapter",
                        "live_data_ready": True,
                    },
                    {"source": "ifind", "adapter_status": "real_adapter", "live_data_ready": True},
                ],
                "data_freshness": [
                    {"dataset": "intraday_l2_features", "status": "fresh", "rows": 100}
                ],
                "hshare_verified": {"ready": True},
                "execution": {"opend_ready": True},
                "research_to_paper": {"approved": True},
                "paper_to_live": {"approved": True},
            }
        ),
        encoding="utf-8",
    )
    profitability.write_text(
        json.dumps(
            {
                "paper_sessions": 20,
                "net_pnl": 1200.0,
                "max_drawdown": -3000.0,
                "max_allowed_drawdown": 10_000.0,
                "reconciled": True,
            }
        ),
        encoding="utf-8",
    )
    quote.write_text(
        json.dumps({"quote": {"symbol": "HK.00700", "lot_size": 100, "last_price": 320.0}}),
        encoding="utf-8",
    )
    responses.write_text(
        '{"ticket_id":"paper-001","response":{"submitted":true}}\n',
        encoding="utf-8",
    )
    runtime.write_text(
        json.dumps({"ready_for_order_submission": True, "failed_reasons": []}),
        encoding="utf-8",
    )
    _write_ready_opend_account_status(account)
    blocker.write_text(
        json.dumps(
            {
                "runtime_status_path": str(runtime),
                "ready_for_next_session": True,
                "next_required_action": "collect_today_paper_session",
                "next_session_failed_reasons": [],
            }
        ),
        encoding="utf-8",
    )
    os.utime(blocker, (1000, 1000))
    os.utime(runtime, (2000, 2000))

    audit = build_objective_audit(
        ObjectiveAuditInput(
            readiness_manifest_path=readiness,
            output_path=output,
            profitability_evidence_path=profitability,
            opend_quote_snapshot_path=quote,
            opend_ticket_response_path=responses,
            opend_runtime_status_path=runtime,
            opend_account_status_path=account,
            paper_blocker_report_path=blocker,
        )
    )
    opend_check = [
        check for check in audit["checks"] if check["requirement"] == "opend_execution_gate"
    ][0]
    checklist_item = [
        item
        for item in audit["prompt_to_artifact_checklist"]
        if item["requirement"] == "opend_execution_gate"
    ][0]

    assert audit["objective_achieved"] is False
    assert "stale_paper_blocker_report" in opend_check["failed_reasons"]
    assert (
        opend_check["evidence"]["runtime"]["paper_blocker_report"][
            "paper_blocker_report_stale"
        ]
        is True
    )
    assert (
        str(runtime)
        in opend_check["evidence"]["runtime"]["paper_blocker_report"][
            "stale_reference_paths"
        ]
    )
    assert checklist_item["next_required_action"] == "refresh_paper_blocker_report"


def test_objective_audit_uses_paper_progress_for_profitability_next_action(
    tmp_path,
) -> None:
    readiness = tmp_path / "readiness.json"
    profitability = tmp_path / "profitability.json"
    progress = tmp_path / "paper_progress.json"
    quote = tmp_path / "quote.json"
    responses = tmp_path / "responses.jsonl"
    runtime = tmp_path / "runtime.json"
    account = tmp_path / "account.json"
    output = tmp_path / "audit.json"
    readiness.write_text(
        json.dumps(
            {
                "go_live_approved": True,
                "account_risk_budget": {"account_equity": 1_000_000},
                "data_sources": [
                    {"source": "tushare", "ready": True},
                    {"source": "ifind", "ready": True},
                ],
                "source_adapters": [
                    {
                        "source": "tushare",
                        "adapter_status": "real_adapter",
                        "live_data_ready": True,
                    },
                    {"source": "ifind", "adapter_status": "real_adapter", "live_data_ready": True},
                ],
                "data_freshness": [
                    {"dataset": "intraday_l2_features", "status": "fresh", "rows": 100}
                ],
                "hshare_verified": {"ready": True},
                "execution": {"opend_ready": True},
                "research_to_paper": {"approved": True},
                "paper_to_live": {"approved": True},
            }
        ),
        encoding="utf-8",
    )
    profitability.write_text(
        json.dumps(
            {
                "ready": True,
                "paper_sessions": 20,
                "inferred_session_count": 20,
                "session_dates": [f"2026-04-{day:02d}" for day in range(1, 21)],
                "execution_log_rows": 20,
                "broker_report_rows": 20,
                "net_pnl": 1200.0,
                "max_drawdown": -3000.0,
                "max_allowed_drawdown": 10_000.0,
                "reconciled": True,
                "failed_reasons": [],
            }
        ),
        encoding="utf-8",
    )
    progress.write_text(
        json.dumps(
            {
                "ready_for_live_review": False,
                "sessions_remaining": 3,
                "failed_reasons": ["paper_sessions_remaining"],
            }
        ),
        encoding="utf-8",
    )
    quote.write_text(
        json.dumps({"quote": {"symbol": "HK.00700", "lot_size": 100, "last_price": 320.0}}),
        encoding="utf-8",
    )
    responses.write_text(
        '{"ticket_id":"paper-001","response":{"submitted":true}}\n',
        encoding="utf-8",
    )
    runtime.write_text(
        json.dumps({"ready_for_order_submission": True, "failed_reasons": []}),
        encoding="utf-8",
    )
    _write_ready_opend_account_status(account)

    audit = build_objective_audit(
        ObjectiveAuditInput(
            readiness_manifest_path=readiness,
            output_path=output,
            profitability_evidence_path=profitability,
            paper_progress_path=progress,
            opend_quote_snapshot_path=quote,
            opend_ticket_response_path=responses,
            opend_runtime_status_path=runtime,
            opend_account_status_path=account,
        )
    )
    profit_check = [
        check
        for check in audit["checks"]
        if check["requirement"] == "profitable_reconciled_paper_or_live_evidence"
    ][0]
    checklist_item = [
        item
        for item in audit["prompt_to_artifact_checklist"]
        if item["requirement"] == "profitable_reconciled_paper_or_live_evidence"
    ][0]

    assert audit["objective_achieved"] is False
    assert "paper_sessions_remaining" in profit_check["failed_reasons"]
    assert profit_check["evidence"]["paper_progress"]["sessions_remaining"] == 3
    assert str(progress) in checklist_item["artifacts"]
    assert (
        checklist_item["next_required_action"]
        == "collect_3_remaining_broker_reconciled_paper_sessions"
    )


def test_objective_audit_blocks_stale_paper_progress(tmp_path) -> None:
    readiness = tmp_path / "readiness.json"
    profitability = tmp_path / "profitability.json"
    progress = tmp_path / "paper_progress.json"
    execution_log = tmp_path / "execution.jsonl"
    broker_report = tmp_path / "broker.json"
    quote = tmp_path / "quote.json"
    responses = tmp_path / "responses.jsonl"
    runtime = tmp_path / "runtime.json"
    account = tmp_path / "account.json"
    output = tmp_path / "audit.json"
    readiness.write_text(
        json.dumps(
            {
                "go_live_approved": True,
                "account_risk_budget": {"account_equity": 1_000_000},
                "data_sources": [
                    {"source": "tushare", "ready": True},
                    {"source": "ifind", "ready": True},
                ],
                "source_adapters": [
                    {
                        "source": "tushare",
                        "adapter_status": "real_adapter",
                        "live_data_ready": True,
                    },
                    {"source": "ifind", "adapter_status": "real_adapter", "live_data_ready": True},
                ],
                "data_freshness": [
                    {"dataset": "intraday_l2_features", "status": "fresh", "rows": 100}
                ],
                "hshare_verified": {"ready": True},
                "execution": {"opend_ready": True},
                "research_to_paper": {"approved": True},
                "paper_to_live": {"approved": True},
            }
        ),
        encoding="utf-8",
    )
    profitability.write_text(
        json.dumps(
            {
                "ready": True,
                "paper_sessions": 20,
                "inferred_session_count": 20,
                "session_dates": [f"2026-04-{day:02d}" for day in range(1, 21)],
                "execution_log_rows": 20,
                "broker_report_rows": 20,
                "net_pnl": 1200.0,
                "max_drawdown": -3000.0,
                "max_allowed_drawdown": 10_000.0,
                "reconciled": True,
                "failed_reasons": [],
            }
        ),
        encoding="utf-8",
    )
    progress.write_text(
        json.dumps(
            {
                "ready_for_live_review": True,
                "sessions_remaining": 0,
                "failed_reasons": [],
            }
        ),
        encoding="utf-8",
    )
    execution_log.write_text(
        "\n".join(
            json.dumps(
                {
                    "order_id": f"ord-{day}",
                    "trade_date": f"2026-04-{day:02d}",
                    "dry_run": False,
                }
            )
            for day in range(1, 21)
        )
        + "\n",
        encoding="utf-8",
    )
    broker_report.write_text(
        json.dumps(
            [
                {
                    "local_order_id": f"ord-{day}",
                    "updated_time": f"2026-04-{day:02d} 10:00:00",
                }
                for day in range(1, 21)
            ]
        ),
        encoding="utf-8",
    )
    quote.write_text(
        json.dumps({"quote": {"symbol": "HK.00700", "lot_size": 100, "last_price": 320.0}}),
        encoding="utf-8",
    )
    responses.write_text(
        '{"ticket_id":"paper-001","response":{"submitted":true}}\n',
        encoding="utf-8",
    )
    runtime.write_text(
        json.dumps({"ready_for_order_submission": True, "failed_reasons": []}),
        encoding="utf-8",
    )
    _write_ready_opend_account_status(account)
    os.utime(progress, (1000, 1000))
    os.utime(execution_log, (1500, 1500))
    os.utime(broker_report, (1500, 1500))
    os.utime(profitability, (2000, 2000))

    audit = build_objective_audit(
        ObjectiveAuditInput(
            readiness_manifest_path=readiness,
            output_path=output,
            profitability_evidence_path=profitability,
            paper_progress_path=progress,
            execution_log_path=execution_log,
            broker_report_path=broker_report,
            opend_quote_snapshot_path=quote,
            opend_ticket_response_path=responses,
            opend_runtime_status_path=runtime,
            opend_account_status_path=account,
        )
    )
    profit_check = [
        check
        for check in audit["checks"]
        if check["requirement"] == "profitable_reconciled_paper_or_live_evidence"
    ][0]
    checklist_item = [
        item
        for item in audit["prompt_to_artifact_checklist"]
        if item["requirement"] == "profitable_reconciled_paper_or_live_evidence"
    ][0]

    assert audit["objective_achieved"] is False
    assert "stale_paper_progress" in profit_check["failed_reasons"]
    assert profit_check["evidence"]["paper_progress"]["paper_progress_stale"] is True
    assert str(profitability) in profit_check["evidence"]["paper_progress"]["stale_reference_paths"]
    assert checklist_item["next_required_action"] == "refresh_paper_progress"


def test_objective_audit_blocks_profitability_ledger_mismatch(tmp_path) -> None:
    readiness = tmp_path / "readiness.json"
    profitability = tmp_path / "profitability.json"
    execution_log = tmp_path / "execution.jsonl"
    broker_report = tmp_path / "broker.json"
    quote = tmp_path / "quote.json"
    responses = tmp_path / "responses.jsonl"
    runtime = tmp_path / "runtime.json"
    account = tmp_path / "account.json"
    output = tmp_path / "audit.json"
    readiness.write_text(
        json.dumps(
            {
                "go_live_approved": True,
                "account_risk_budget": {"account_equity": 1_000_000},
                "data_sources": [
                    {"source": "tushare", "ready": True},
                    {"source": "ifind", "ready": True},
                ],
                "source_adapters": [
                    {
                        "source": "tushare",
                        "adapter_status": "real_adapter",
                        "live_data_ready": True,
                    },
                    {"source": "ifind", "adapter_status": "real_adapter", "live_data_ready": True},
                ],
                "data_freshness": [
                    {"dataset": "intraday_l2_features", "status": "fresh", "rows": 100}
                ],
                "hshare_verified": {"ready": True},
                "execution": {"opend_ready": True},
                "research_to_paper": {"approved": True},
                "paper_to_live": {"approved": True},
            }
        ),
        encoding="utf-8",
    )
    profitability.write_text(
        json.dumps(
            {
                "ready": True,
                "paper_sessions": 20,
                "inferred_session_count": 20,
                "session_dates": [f"2026-04-{day:02d}" for day in range(1, 21)],
                "execution_log_rows": 20,
                "broker_report_rows": 20,
                "net_pnl": 1200.0,
                "max_drawdown": -3000.0,
                "max_allowed_drawdown": 10_000.0,
                "reconciled": True,
                "failed_reasons": [],
            }
        ),
        encoding="utf-8",
    )
    execution_log.write_text(
        json.dumps({"order_id": "ord-1", "trade_date": "2026-05-05", "dry_run": False})
        + "\n",
        encoding="utf-8",
    )
    broker_report.write_text(
        json.dumps([{"local_order_id": "ord-1", "updated_time": "2026-05-05 10:00:00"}]),
        encoding="utf-8",
    )
    quote.write_text(
        json.dumps({"quote": {"symbol": "HK.00700", "lot_size": 100, "last_price": 320.0}}),
        encoding="utf-8",
    )
    responses.write_text(
        '{"ticket_id":"paper-001","response":{"submitted":true}}\n',
        encoding="utf-8",
    )
    runtime.write_text(
        json.dumps({"ready_for_order_submission": True, "failed_reasons": []}),
        encoding="utf-8",
    )
    _write_ready_opend_account_status(account)

    audit = build_objective_audit(
        ObjectiveAuditInput(
            readiness_manifest_path=readiness,
            output_path=output,
            profitability_evidence_path=profitability,
            execution_log_path=execution_log,
            broker_report_path=broker_report,
            opend_quote_snapshot_path=quote,
            opend_ticket_response_path=responses,
            opend_runtime_status_path=runtime,
            opend_account_status_path=account,
        )
    )
    profit_check = [
        check
        for check in audit["checks"]
        if check["requirement"] == "profitable_reconciled_paper_or_live_evidence"
    ][0]
    checklist_item = [
        item
        for item in audit["prompt_to_artifact_checklist"]
        if item["requirement"] == "profitable_reconciled_paper_or_live_evidence"
    ][0]

    assert audit["objective_achieved"] is False
    assert "profitability_session_count_mismatch" in profit_check["failed_reasons"]
    assert "profitability_session_dates_mismatch" in profit_check["failed_reasons"]
    assert "profitability_execution_log_rows_mismatch" in profit_check["failed_reasons"]
    assert "profitability_broker_report_rows_mismatch" in profit_check["failed_reasons"]
    assert checklist_item["next_required_action"] == "refresh_profitability_evidence"


def test_objective_audit_blocks_stale_profitability_evidence(tmp_path) -> None:
    readiness = tmp_path / "readiness.json"
    profitability = tmp_path / "profitability.json"
    execution_log = tmp_path / "execution.jsonl"
    broker_report = tmp_path / "broker.json"
    quote = tmp_path / "quote.json"
    responses = tmp_path / "responses.jsonl"
    runtime = tmp_path / "runtime.json"
    account = tmp_path / "account.json"
    output = tmp_path / "audit.json"
    readiness.write_text(
        json.dumps(
            {
                "go_live_approved": True,
                "account_risk_budget": {"account_equity": 1_000_000},
                "data_sources": [
                    {"source": "tushare", "ready": True},
                    {"source": "ifind", "ready": True},
                ],
                "source_adapters": [
                    {
                        "source": "tushare",
                        "adapter_status": "real_adapter",
                        "live_data_ready": True,
                    },
                    {"source": "ifind", "adapter_status": "real_adapter", "live_data_ready": True},
                ],
                "data_freshness": [
                    {"dataset": "intraday_l2_features", "status": "fresh", "rows": 100}
                ],
                "hshare_verified": {"ready": True},
                "execution": {"opend_ready": True},
                "research_to_paper": {"approved": True},
                "paper_to_live": {"approved": True},
            }
        ),
        encoding="utf-8",
    )
    rows = [
        {
            "order_id": f"ord-{day}",
            "trade_date": f"2026-04-{day:02d}",
            "dry_run": False,
        }
        for day in range(1, 21)
    ]
    broker_rows = [
        {
            "local_order_id": f"ord-{day}",
            "updated_time": f"2026-04-{day:02d} 10:00:00",
        }
        for day in range(1, 21)
    ]
    profitability.write_text(
        json.dumps(
            {
                "ready": True,
                "paper_sessions": 20,
                "inferred_session_count": 20,
                "session_dates": [f"2026-04-{day:02d}" for day in range(1, 21)],
                "execution_log_rows": 20,
                "broker_report_rows": 20,
                "net_pnl": 1200.0,
                "max_drawdown": -3000.0,
                "max_allowed_drawdown": 10_000.0,
                "reconciled": True,
                "failed_reasons": [],
            }
        ),
        encoding="utf-8",
    )
    execution_log.write_text(
        "\n".join(json.dumps(row) for row in rows) + "\n",
        encoding="utf-8",
    )
    broker_report.write_text(json.dumps(broker_rows), encoding="utf-8")
    quote.write_text(
        json.dumps({"quote": {"symbol": "HK.00700", "lot_size": 100, "last_price": 320.0}}),
        encoding="utf-8",
    )
    responses.write_text(
        '{"ticket_id":"paper-001","response":{"submitted":true}}\n',
        encoding="utf-8",
    )
    runtime.write_text(
        json.dumps({"ready_for_order_submission": True, "failed_reasons": []}),
        encoding="utf-8",
    )
    _write_ready_opend_account_status(account)
    os.utime(profitability, (1000, 1000))
    os.utime(execution_log, (2000, 2000))

    audit = build_objective_audit(
        ObjectiveAuditInput(
            readiness_manifest_path=readiness,
            output_path=output,
            profitability_evidence_path=profitability,
            execution_log_path=execution_log,
            broker_report_path=broker_report,
            opend_quote_snapshot_path=quote,
            opend_ticket_response_path=responses,
            opend_runtime_status_path=runtime,
            opend_account_status_path=account,
        )
    )
    profit_check = [
        check
        for check in audit["checks"]
        if check["requirement"] == "profitable_reconciled_paper_or_live_evidence"
    ][0]
    checklist_item = [
        item
        for item in audit["prompt_to_artifact_checklist"]
        if item["requirement"] == "profitable_reconciled_paper_or_live_evidence"
    ][0]

    assert audit["objective_achieved"] is False
    assert "stale_profitability_evidence" in profit_check["failed_reasons"]
    assert profit_check["evidence"]["profitability_evidence_stale"] is True
    assert str(execution_log) in profit_check["evidence"]["stale_reference_paths"]
    assert checklist_item["next_required_action"] == "refresh_profitability_evidence"


def test_objective_audit_requires_opend_runtime_evidence(tmp_path) -> None:
    readiness = tmp_path / "readiness.json"
    profitability = tmp_path / "profitability.json"
    output = tmp_path / "audit.json"
    readiness.write_text(
        json.dumps(
            {
                "go_live_approved": True,
                "account_risk_budget": {"account_equity": 1_000_000},
                "data_sources": [
                    {"source": "tushare", "ready": True},
                    {"source": "ifind", "ready": True},
                ],
                "source_adapters": [
                    {
                        "source": "tushare",
                        "adapter_status": "real_adapter",
                        "live_data_ready": True,
                    },
                    {"source": "ifind", "adapter_status": "real_adapter", "live_data_ready": True},
                ],
                "data_freshness": [
                    {"dataset": "intraday_l2_features", "status": "fresh", "rows": 100}
                ],
                "hshare_verified": {"ready": True},
                "execution": {"opend_ready": True},
                "research_to_paper": {"approved": True},
                "paper_to_live": {"approved": True},
            }
        ),
        encoding="utf-8",
    )
    profitability.write_text(
        json.dumps(
            {
                "paper_sessions": 20,
                "net_pnl": 1200.0,
                "max_drawdown": -3000.0,
                "max_allowed_drawdown": 10_000.0,
                "reconciled": True,
            }
        ),
        encoding="utf-8",
    )

    audit = build_objective_audit(
        ObjectiveAuditInput(
            readiness_manifest_path=readiness,
            output_path=output,
            profitability_evidence_path=profitability,
        )
    )
    opend_check = [
        check for check in audit["checks"] if check["requirement"] == "opend_execution_gate"
    ][0]

    assert audit["objective_achieved"] is False
    assert "opend_execution_gate" in audit["blocked_requirements"]
    assert "missing_opend_runtime_status" in opend_check["failed_reasons"]
    assert "missing_opend_account_status" in opend_check["failed_reasons"]
    assert "missing_opend_quote_snapshot" in opend_check["failed_reasons"]
    assert "missing_opend_ticket_response" in opend_check["failed_reasons"]


def test_objective_audit_blocks_opend_kill_switch_runtime_status(tmp_path) -> None:
    readiness = tmp_path / "readiness.json"
    profitability = tmp_path / "profitability.json"
    quote = tmp_path / "quote.json"
    responses = tmp_path / "responses.jsonl"
    runtime = tmp_path / "runtime.json"
    account = tmp_path / "account.json"
    output = tmp_path / "audit.json"
    readiness.write_text(
        json.dumps(
            {
                "go_live_approved": True,
                "account_risk_budget": {"account_equity": 1_000_000},
                "data_sources": [
                    {"source": "tushare", "ready": True},
                    {"source": "ifind", "ready": True},
                ],
                "source_adapters": [
                    {
                        "source": "tushare",
                        "adapter_status": "real_adapter",
                        "live_data_ready": True,
                    },
                    {"source": "ifind", "adapter_status": "real_adapter", "live_data_ready": True},
                ],
                "data_freshness": [
                    {"dataset": "intraday_l2_features", "status": "fresh", "rows": 100}
                ],
                "hshare_verified": {"ready": True},
                "execution": {"opend_ready": True},
                "research_to_paper": {"approved": True},
                "paper_to_live": {"approved": True},
            }
        ),
        encoding="utf-8",
    )
    profitability.write_text(
        json.dumps(
            {
                "paper_sessions": 20,
                "net_pnl": 1200.0,
                "max_drawdown": -3000.0,
                "max_allowed_drawdown": 10_000.0,
                "reconciled": True,
            }
        ),
        encoding="utf-8",
    )
    quote.write_text(
        json.dumps({"quote": {"symbol": "HK.00700", "lot_size": 100, "last_price": 320.0}}),
        encoding="utf-8",
    )
    responses.write_text(
        '{"ticket_id":"paper-001","response":{"submitted":false}}\n',
        encoding="utf-8",
    )
    runtime.write_text(
        json.dumps(
            {
                "ready_for_order_submission": False,
                "kill_switch": True,
                "failed_reasons": ["opend_kill_switch_enabled"],
            }
        ),
        encoding="utf-8",
    )
    _write_ready_opend_account_status(account)

    audit = build_objective_audit(
        ObjectiveAuditInput(
            readiness_manifest_path=readiness,
            output_path=output,
            profitability_evidence_path=profitability,
            opend_quote_snapshot_path=quote,
            opend_ticket_response_path=responses,
            opend_runtime_status_path=runtime,
            opend_account_status_path=account,
        )
    )
    opend_check = [
        check for check in audit["checks"] if check["requirement"] == "opend_execution_gate"
    ][0]

    assert audit["objective_achieved"] is False
    assert "opend_execution_gate" in audit["blocked_requirements"]
    assert "opend_kill_switch_enabled" in opend_check["failed_reasons"]
    assert opend_check["evidence"]["runtime"]["runtime_status"]["kill_switch"] is True


def test_objective_audit_requires_submitted_opend_ticket_response(tmp_path) -> None:
    readiness = tmp_path / "readiness.json"
    profitability = tmp_path / "profitability.json"
    quote = tmp_path / "quote.json"
    responses = tmp_path / "responses.jsonl"
    runtime = tmp_path / "runtime.json"
    account = tmp_path / "account.json"
    output = tmp_path / "audit.json"
    readiness.write_text(
        json.dumps(
            {
                "go_live_approved": True,
                "account_risk_budget": {"account_equity": 1_000_000},
                "data_sources": [
                    {"source": "tushare", "ready": True},
                    {"source": "ifind", "ready": True},
                ],
                "source_adapters": [
                    {
                        "source": "tushare",
                        "adapter_status": "real_adapter",
                        "live_data_ready": True,
                    },
                    {"source": "ifind", "adapter_status": "real_adapter", "live_data_ready": True},
                ],
                "data_freshness": [
                    {"dataset": "intraday_l2_features", "status": "fresh", "rows": 100}
                ],
                "hshare_verified": {"ready": True},
                "execution": {"opend_ready": True},
                "research_to_paper": {"approved": True},
                "paper_to_live": {"approved": True},
            }
        ),
        encoding="utf-8",
    )
    profitability.write_text(
        json.dumps(
            {
                "paper_sessions": 20,
                "net_pnl": 1200.0,
                "max_drawdown": -3000.0,
                "max_allowed_drawdown": 10_000.0,
                "reconciled": True,
            }
        ),
        encoding="utf-8",
    )
    quote.write_text(
        json.dumps({"quote": {"symbol": "HK.00700", "lot_size": 100, "last_price": 320.0}}),
        encoding="utf-8",
    )
    responses.write_text(
        '{"ticket_id":"paper-001","response":{"submitted":false}}\n',
        encoding="utf-8",
    )
    runtime.write_text(
        json.dumps({"ready_for_order_submission": True, "failed_reasons": []}),
        encoding="utf-8",
    )
    _write_ready_opend_account_status(account)

    audit = build_objective_audit(
        ObjectiveAuditInput(
            readiness_manifest_path=readiness,
            output_path=output,
            profitability_evidence_path=profitability,
            opend_quote_snapshot_path=quote,
            opend_ticket_response_path=responses,
            opend_runtime_status_path=runtime,
            opend_account_status_path=account,
        )
    )
    opend_check = [
        check for check in audit["checks"] if check["requirement"] == "opend_execution_gate"
    ][0]

    assert audit["objective_achieved"] is False
    assert "opend_execution_gate" in audit["blocked_requirements"]
    assert "missing_submitted_opend_ticket_response" in opend_check["failed_reasons"]
    assert opend_check["evidence"]["runtime"]["submitted_ticket_response_rows"] == 0
    assert opend_check["evidence"]["runtime"]["failed_ticket_response_rows"] == 0


def test_objective_audit_requires_ready_paper_simulate_account(tmp_path) -> None:
    readiness = tmp_path / "readiness.json"
    profitability = tmp_path / "profitability.json"
    quote = tmp_path / "quote.json"
    responses = tmp_path / "responses.jsonl"
    runtime = tmp_path / "runtime.json"
    account = tmp_path / "account.json"
    output = tmp_path / "audit.json"
    readiness.write_text(
        json.dumps(
            {
                "go_live_approved": True,
                "account_risk_budget": {"account_equity": 1_000_000},
                "data_sources": [
                    {"source": "tushare", "ready": True},
                    {"source": "ifind", "ready": True},
                ],
                "source_adapters": [
                    {
                        "source": "tushare",
                        "adapter_status": "real_adapter",
                        "live_data_ready": True,
                    },
                    {"source": "ifind", "adapter_status": "real_adapter", "live_data_ready": True},
                ],
                "data_freshness": [
                    {"dataset": "intraday_l2_features", "status": "fresh", "rows": 100}
                ],
                "hshare_verified": {"ready": True},
                "execution": {"opend_ready": True},
                "research_to_paper": {"approved": True},
                "paper_to_live": {"approved": True},
            }
        ),
        encoding="utf-8",
    )
    profitability.write_text(
        json.dumps(
            {
                "paper_sessions": 20,
                "net_pnl": 1200.0,
                "max_drawdown": -3000.0,
                "max_allowed_drawdown": 10_000.0,
                "reconciled": True,
            }
        ),
        encoding="utf-8",
    )
    quote.write_text(
        json.dumps({"quote": {"symbol": "HK.00700", "lot_size": 100, "last_price": 320.0}}),
        encoding="utf-8",
    )
    responses.write_text(
        '{"ticket_id":"paper-001","response":{"submitted":true}}\n',
        encoding="utf-8",
    )
    runtime.write_text(
        json.dumps({"ready_for_order_submission": True, "failed_reasons": []}),
        encoding="utf-8",
    )
    account.write_text(
        json.dumps(
            {
                "ready_for_paper_simulate": False,
                "simulate_account_count": 1,
                "hk_stock_simulate_account_count": 0,
                "failed_reasons": ["missing_hk_stock_simulate_account"],
            }
        ),
        encoding="utf-8",
    )

    audit = build_objective_audit(
        ObjectiveAuditInput(
            readiness_manifest_path=readiness,
            output_path=output,
            profitability_evidence_path=profitability,
            opend_quote_snapshot_path=quote,
            opend_ticket_response_path=responses,
            opend_runtime_status_path=runtime,
            opend_account_status_path=account,
        )
    )
    opend_check = [
        check for check in audit["checks"] if check["requirement"] == "opend_execution_gate"
    ][0]

    assert audit["objective_achieved"] is False
    assert "opend_execution_gate" in audit["blocked_requirements"]
    assert "missing_hk_stock_simulate_account" in opend_check["failed_reasons"]
    assert "opend_account_not_ready_for_paper_simulate" in opend_check["failed_reasons"]
    assert opend_check["evidence"]["runtime"]["account_status"]["ready_for_paper_simulate"] is False


def test_objective_audit_uses_paper_simulate_status_failed_reasons(tmp_path) -> None:
    readiness = tmp_path / "readiness.json"
    profitability = tmp_path / "profitability.json"
    quote = tmp_path / "quote.json"
    responses = tmp_path / "responses.jsonl"
    runtime = tmp_path / "runtime.json"
    account = tmp_path / "account.json"
    paper_status = tmp_path / "paper_status.json"
    output = tmp_path / "audit.json"
    readiness.write_text(
        json.dumps(
            {
                "go_live_approved": True,
                "account_risk_budget": {"account_equity": 1_000_000},
                "data_sources": [
                    {"source": "tushare", "ready": True},
                    {"source": "ifind", "ready": True},
                ],
                "source_adapters": [
                    {
                        "source": "tushare",
                        "adapter_status": "real_adapter",
                        "live_data_ready": True,
                    },
                    {"source": "ifind", "adapter_status": "real_adapter", "live_data_ready": True},
                ],
                "data_freshness": [
                    {"dataset": "intraday_l2_features", "status": "fresh", "rows": 100}
                ],
                "hshare_verified": {"ready": True},
                "execution": {"opend_ready": True},
                "research_to_paper": {"approved": True},
                "paper_to_live": {"approved": True},
            }
        ),
        encoding="utf-8",
    )
    profitability.write_text(
        json.dumps(
            {
                "paper_sessions": 20,
                "net_pnl": 1200.0,
                "max_drawdown": -3000.0,
                "max_allowed_drawdown": 10_000.0,
                "reconciled": True,
            }
        ),
        encoding="utf-8",
    )
    quote.write_text(
        json.dumps({"quote": {"symbol": "HK.00700", "lot_size": 100, "last_price": 320.0}}),
        encoding="utf-8",
    )
    responses.write_text(
        '{"ticket_id":"paper-001","response":{"submitted":false}}\n',
        encoding="utf-8",
    )
    runtime.write_text(
        json.dumps({"ready_for_order_submission": True, "failed_reasons": []}),
        encoding="utf-8",
    )
    _write_ready_opend_account_status(account)
    paper_status.write_text(
        json.dumps(
            {
                "ready_for_session_collection": False,
                "failed_reasons": ["missing_submitted_responses"],
            }
        ),
        encoding="utf-8",
    )

    audit = build_objective_audit(
        ObjectiveAuditInput(
            readiness_manifest_path=readiness,
            output_path=output,
            profitability_evidence_path=profitability,
            opend_quote_snapshot_path=quote,
            opend_ticket_response_path=responses,
            opend_runtime_status_path=runtime,
            opend_account_status_path=account,
            paper_simulate_status_path=paper_status,
        )
    )
    opend_check = [
        check for check in audit["checks"] if check["requirement"] == "opend_execution_gate"
    ][0]

    assert audit["objective_achieved"] is False
    assert "missing_submitted_responses" in opend_check["failed_reasons"]


def test_objective_audit_marks_stale_paper_simulate_status(tmp_path) -> None:
    readiness = tmp_path / "readiness.json"
    profitability = tmp_path / "profitability.json"
    quote = tmp_path / "quote.json"
    responses = tmp_path / "responses.jsonl"
    runtime = tmp_path / "runtime.json"
    account = tmp_path / "account.json"
    paper_status = tmp_path / "paper_status.json"
    output = tmp_path / "audit.json"
    readiness.write_text(
        json.dumps(
            {
                "go_live_approved": True,
                "account_risk_budget": {"account_equity": 1_000_000},
                "data_sources": [
                    {"source": "tushare", "ready": True},
                    {"source": "ifind", "ready": True},
                ],
                "source_adapters": [
                    {
                        "source": "tushare",
                        "adapter_status": "real_adapter",
                        "live_data_ready": True,
                    },
                    {"source": "ifind", "adapter_status": "real_adapter", "live_data_ready": True},
                ],
                "data_freshness": [
                    {"dataset": "intraday_l2_features", "status": "fresh", "rows": 100}
                ],
                "hshare_verified": {"ready": True},
                "execution": {"opend_ready": True},
                "research_to_paper": {"approved": True},
                "paper_to_live": {"approved": True},
            }
        ),
        encoding="utf-8",
    )
    profitability.write_text(
        json.dumps(
            {
                "paper_sessions": 20,
                "net_pnl": 1200.0,
                "max_drawdown": -3000.0,
                "max_allowed_drawdown": 10_000.0,
                "reconciled": True,
            }
        ),
        encoding="utf-8",
    )
    quote.write_text(
        json.dumps({"quote": {"symbol": "HK.00700", "lot_size": 100, "last_price": 320.0}}),
        encoding="utf-8",
    )
    responses.write_text(
        '{"ticket_id":"paper-001","response":{"submitted":true}}\n',
        encoding="utf-8",
    )
    runtime.write_text(
        json.dumps({"ready_for_order_submission": True, "failed_reasons": []}),
        encoding="utf-8",
    )
    _write_ready_opend_account_status(account)
    paper_status.write_text(
        json.dumps(
            {
                "ready_for_session_collection": False,
                "failed_reasons": ["missing_submitted_responses"],
            }
        ),
        encoding="utf-8",
    )
    os.utime(paper_status, (1000, 1000))
    os.utime(runtime, (2000, 2000))

    audit = build_objective_audit(
        ObjectiveAuditInput(
            readiness_manifest_path=readiness,
            output_path=output,
            profitability_evidence_path=profitability,
            opend_quote_snapshot_path=quote,
            opend_ticket_response_path=responses,
            opend_runtime_status_path=runtime,
            opend_account_status_path=account,
            paper_simulate_status_path=paper_status,
        )
    )
    opend_check = [
        check for check in audit["checks"] if check["requirement"] == "opend_execution_gate"
    ][0]

    assert audit["objective_achieved"] is False
    assert "stale_paper_simulate_status" in opend_check["failed_reasons"]
    assert "missing_submitted_responses" not in opend_check["failed_reasons"]
    assert opend_check["evidence"]["runtime"]["paper_simulate_status_stale"] is True
    checklist_item = [
        item
        for item in audit["prompt_to_artifact_checklist"]
        if item["requirement"] == "opend_execution_gate"
    ][0]
    assert (
        checklist_item["next_required_action"]
        == "regenerate_paper_simulate_status_from_latest_responses"
    )
