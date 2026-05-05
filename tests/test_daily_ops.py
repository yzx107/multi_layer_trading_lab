import subprocess
from pathlib import Path

from multi_layer_trading_lab.runtime.daily_ops import (
    DailyOpsPlan,
    build_daily_ops_commands,
    run_daily_ops_plan,
)


def test_build_daily_ops_commands_runs_report_then_readiness() -> None:
    commands = build_daily_ops_commands(
        DailyOpsPlan(
            python_executable=".venv/bin/python",
            lake_root=Path("data/lake"),
            report_path=Path("data/logs/ops.md"),
            readiness_path=Path("data/logs/readiness.json"),
        )
    )

    assert commands[0][:4] == [
        ".venv/bin/python",
        "-m",
        "multi_layer_trading_lab.cli",
        "ifind-ingestion-status",
    ]
    assert "--output-path" in commands[0]
    assert "data/logs/ifind_ingestion_status.json" in commands[0]
    assert commands[1][:4] == [
        ".venv/bin/python",
        "-m",
        "multi_layer_trading_lab.cli",
        "ifind-connection-plan",
    ]
    assert "--output-path" in commands[1]
    assert "data/logs/ifind_connection_plan.json" in commands[1]
    assert commands[2][:4] == [
        ".venv/bin/python",
        "-m",
        "multi_layer_trading_lab.cli",
        "research-input-manifest",
    ]
    assert commands[3][:4] == [
        ".venv/bin/python",
        "-m",
        "multi_layer_trading_lab.cli",
        "factor-factory-summary",
    ]
    assert commands[4][:4] == [
        ".venv/bin/python",
        "-m",
        "multi_layer_trading_lab.cli",
        "external-factor-portfolio",
    ]
    assert commands[5][:4] == [
        ".venv/bin/python",
        "-m",
        "multi_layer_trading_lab.cli",
        "build-external-research-signals",
    ]
    assert commands[6][:4] == [
        ".venv/bin/python",
        "-m",
        "multi_layer_trading_lab.cli",
        "paper-session-plan",
    ]
    assert "--quote-snapshot-path" in commands[6]
    assert "data/logs/opend_quote_snapshot.json" in commands[6]
    assert commands[7][:4] == [
        ".venv/bin/python",
        "-m",
        "multi_layer_trading_lab.cli",
        "ops-report",
    ]
    assert commands[8][:4] == [
        ".venv/bin/python",
        "-m",
        "multi_layer_trading_lab.cli",
        "go-live-readiness",
    ]
    assert commands[9][:4] == [
        ".venv/bin/python",
        "-m",
        "multi_layer_trading_lab.cli",
        "objective-audit",
    ]
    assert commands[10][:4] == [
        ".venv/bin/python",
        "-m",
        "multi_layer_trading_lab.cli",
        "objective-audit-report",
    ]
    assert "--include-research-audit" in commands[7]
    assert "data/logs/ops.md" in commands[7]
    assert "--signal-dataset" in commands[7]
    assert "external_research_signal_events" in commands[8]
    assert "data/logs/readiness.json" in commands[8]
    assert "--account-equity" in commands[8]
    assert "1000000.00" in commands[8]
    assert "--readiness-manifest-path" in commands[9]
    assert "data/logs/readiness.json" in commands[9]
    assert "--research-input-manifest-path" in commands[9]
    assert "data/logs/research_input_manifest.json" in commands[9]
    assert "--ifind-ingestion-status-path" in commands[9]
    assert "data/logs/ifind_ingestion_status.json" in commands[9]
    assert "--opend-account-status-path" in commands[9]
    assert "data/logs/opend_account_status.json" in commands[9]
    assert "--audit-path" in commands[10]
    assert "data/logs/objective_audit.json" in commands[10]


def test_run_daily_ops_plan_writes_diagnostics_after_runtime_submission_block(
    tmp_path,
    monkeypatch,
) -> None:
    commands_seen: list[str] = []

    def fake_run(command, check, capture_output, text):
        del check, capture_output, text
        command_name = command[3]
        commands_seen.append(command_name)
        returncode = 1 if command_name == "fetch-opend-runtime-status" else 0
        return subprocess.CompletedProcess(
            command,
            returncode,
            stdout=f"{command_name}\n",
            stderr="",
        )

    monkeypatch.setattr("multi_layer_trading_lab.runtime.daily_ops.subprocess.run", fake_run)

    results = run_daily_ops_plan(
        DailyOpsPlan(
            python_executable=".venv/bin/python",
            lake_root=tmp_path / "lake",
            report_path=tmp_path / "ops.md",
            readiness_path=tmp_path / "readiness.json",
            objective_audit_path=tmp_path / "objective.json",
            objective_audit_report_path=tmp_path / "objective.md",
            paper_blocker_report_path=tmp_path / "blocker.json",
            export_opend_ticket_path=tmp_path / "tickets.jsonl",
            opend_ticket_response_path=tmp_path / "responses.jsonl",
            submit_opend_paper_simulate_tickets=True,
        )
    )

    assert "fetch-opend-runtime-status" in commands_seen
    assert "fetch-opend-account-status" not in commands_seen
    assert "export-opend-paper-tickets" not in commands_seen
    assert "submit-opend-paper-tickets" not in commands_seen
    assert "paper-blocker-report" in commands_seen
    assert "paper-operator-handoff" in commands_seen
    assert "ops-report" in commands_seen
    assert "go-live-readiness" in commands_seen
    assert "objective-audit" in commands_seen
    assert any(result.returncode == 1 for result in results)


def test_run_daily_ops_plan_writes_diagnostics_after_account_submission_block(
    tmp_path,
    monkeypatch,
) -> None:
    commands_seen: list[str] = []

    def fake_run(command, check, capture_output, text):
        del check, capture_output, text
        command_name = command[3]
        commands_seen.append(command_name)
        returncode = 1 if command_name == "fetch-opend-account-status" else 0
        return subprocess.CompletedProcess(
            command,
            returncode,
            stdout=f"{command_name}\n",
            stderr="",
        )

    monkeypatch.setattr("multi_layer_trading_lab.runtime.daily_ops.subprocess.run", fake_run)

    results = run_daily_ops_plan(
        DailyOpsPlan(
            python_executable=".venv/bin/python",
            lake_root=tmp_path / "lake",
            report_path=tmp_path / "ops.md",
            readiness_path=tmp_path / "readiness.json",
            objective_audit_path=tmp_path / "objective.json",
            objective_audit_report_path=tmp_path / "objective.md",
            paper_blocker_report_path=tmp_path / "blocker.json",
            export_opend_ticket_path=tmp_path / "tickets.jsonl",
            opend_ticket_response_path=tmp_path / "responses.jsonl",
            submit_opend_paper_simulate_tickets=True,
        )
    )

    assert "fetch-opend-runtime-status" in commands_seen
    assert "fetch-opend-account-status" in commands_seen
    assert "export-opend-paper-tickets" not in commands_seen
    assert "submit-opend-paper-tickets" not in commands_seen
    assert "paper-blocker-report" in commands_seen
    assert "paper-operator-handoff" in commands_seen
    assert "ops-report" in commands_seen
    assert "objective-audit" in commands_seen
    assert any(result.returncode == 1 for result in results)


def test_build_daily_ops_commands_can_ingest_ifind_events_file() -> None:
    commands = build_daily_ops_commands(
        DailyOpsPlan(
            python_executable=".venv/bin/python",
            lake_root=Path("data/lake"),
            report_path=Path("data/logs/ops.md"),
            readiness_path=Path("data/logs/readiness.json"),
            ifind_events_file_path=Path("data/inbox/ifind.csv"),
            ifind_validation_report_path=Path("data/logs/ifind_validation.json"),
            ifind_source_run_id="manual-ifind-export",
            ifind_overwrite=True,
        )
    )

    assert commands[0][:4] == [
        ".venv/bin/python",
        "-m",
        "multi_layer_trading_lab.cli",
        "validate-ifind-events-file",
    ]
    assert "data/inbox/ifind.csv" in commands[0]
    assert "--output-path" in commands[0]
    assert "data/logs/ifind_validation.json" in commands[0]
    assert "--source-run-id" in commands[0]
    assert "manual-ifind-export" in commands[0]
    assert commands[1][:4] == [
        ".venv/bin/python",
        "-m",
        "multi_layer_trading_lab.cli",
        "import-ifind-events-file",
    ]
    assert "--lake-root" in commands[1]
    assert "data/lake" in commands[1]
    assert "--overwrite" in commands[1]
    assert commands[2][3] == "ifind-ingestion-status"
    assert commands[3][3] == "ifind-connection-plan"
    objective_audit_command = commands[-2]
    assert objective_audit_command[3] == "objective-audit"
    assert "--ifind-validation-report-path" in objective_audit_command
    assert "data/logs/ifind_validation.json" in objective_audit_command


def test_build_daily_ops_commands_can_allow_lot_round_up_after_review() -> None:
    commands = build_daily_ops_commands(
        DailyOpsPlan(
            python_executable=".venv/bin/python",
            lake_root=Path("data/lake"),
            report_path=Path("data/logs/ops.md"),
            readiness_path=Path("data/logs/readiness.json"),
            allow_lot_round_up=True,
        )
    )

    paper_plan_command = commands[6]
    assert paper_plan_command[3] == "paper-session-plan"
    assert "--quote-snapshot-path" in paper_plan_command
    assert "--allow-lot-round-up" in paper_plan_command


def test_build_daily_ops_commands_can_export_opend_tickets_after_review() -> None:
    commands = build_daily_ops_commands(
        DailyOpsPlan(
            python_executable=".venv/bin/python",
            lake_root=Path("data/lake"),
            report_path=Path("data/logs/ops.md"),
            readiness_path=Path("data/logs/readiness.json"),
            allow_lot_round_up=True,
            export_opend_ticket_path=Path("data/logs/tickets.jsonl"),
        )
    )

    assert commands[7][:4] == [
        ".venv/bin/python",
        "-m",
        "multi_layer_trading_lab.cli",
        "export-opend-paper-tickets",
    ]
    assert "--plan-path" in commands[7]
    assert "data/logs/paper_session_plan.json" in commands[7]
    assert "--output-path" in commands[7]
    assert "data/logs/tickets.jsonl" in commands[7]
    assert "--quote-snapshot-path" in commands[7]
    assert "data/logs/opend_quote_snapshot.json" in commands[7]
    assert commands[8][3] == "ops-report"


def test_build_daily_ops_commands_can_submit_exported_dry_run_tickets() -> None:
    commands = build_daily_ops_commands(
        DailyOpsPlan(
            python_executable=".venv/bin/python",
            lake_root=Path("data/lake"),
            report_path=Path("data/logs/ops.md"),
            readiness_path=Path("data/logs/readiness.json"),
            allow_lot_round_up=True,
            export_opend_ticket_path=Path("data/logs/tickets.jsonl"),
            opend_ticket_response_path=Path("data/logs/responses.jsonl"),
            submit_opend_dry_run_tickets=True,
            submit_opend_max_attempts=5,
            submit_opend_retry_delay_seconds=0.25,
        )
    )

    assert commands[7][3] == "export-opend-paper-tickets"
    assert commands[8][:4] == [
        ".venv/bin/python",
        "-m",
        "multi_layer_trading_lab.cli",
        "submit-opend-paper-tickets",
    ]
    assert "--ticket-path" in commands[8]
    assert "data/logs/tickets.jsonl" in commands[8]
    assert "--output-path" in commands[8]
    assert "data/logs/responses.jsonl" in commands[8]
    assert "--max-attempts" in commands[8]
    assert "5" in commands[8]
    assert "--retry-delay-seconds" in commands[8]
    assert "0.250" in commands[8]
    assert commands[9][3] == "ops-report"


def test_build_daily_ops_commands_can_submit_exported_paper_simulate_tickets() -> None:
    commands = build_daily_ops_commands(
        DailyOpsPlan(
            python_executable=".venv/bin/python",
            lake_root=Path("data/lake"),
            report_path=Path("data/logs/ops.md"),
            readiness_path=Path("data/logs/readiness.json"),
            allow_lot_round_up=True,
            export_opend_ticket_path=Path("data/logs/tickets.jsonl"),
            opend_ticket_response_path=Path("data/logs/responses.jsonl"),
            submit_opend_paper_simulate_tickets=True,
        )
    )

    assert commands[7][:4] == [
        ".venv/bin/python",
        "-m",
        "multi_layer_trading_lab.cli",
        "fetch-opend-runtime-status",
    ]
    assert "--require-order-submission-ready" in commands[7]
    assert commands[8][:4] == [
        ".venv/bin/python",
        "-m",
        "multi_layer_trading_lab.cli",
        "fetch-opend-account-status",
    ]
    assert "--require-paper-simulate-ready" in commands[8]
    assert commands[9][3] == "export-opend-paper-tickets"
    assert commands[10][:4] == [
        ".venv/bin/python",
        "-m",
        "multi_layer_trading_lab.cli",
        "submit-opend-paper-tickets",
    ]
    assert "--submit-paper-simulate" in commands[10]
    assert "--opend-runtime-status-path" in commands[10]
    assert "data/logs/opend_runtime_status.json" in commands[10]
    assert "--allow-failed-resubmit" in commands[10]
    assert commands[11][:4] == [
        ".venv/bin/python",
        "-m",
        "multi_layer_trading_lab.cli",
        "paper-simulate-status",
    ]
    assert "--response-path" in commands[11]
    assert "data/logs/responses.jsonl" in commands[11]
    assert commands[12][3] == "paper-blocker-report"
    assert commands[13][3] == "paper-operator-handoff"
    assert "--paper-blocker-report-path" in commands[13]
    assert "data/logs/paper_blocker_report.json" in commands[13]
    assert commands[14][3] == "ops-report"


def test_build_daily_ops_commands_can_require_calendar_collect_before_simulate_submit() -> None:
    commands = build_daily_ops_commands(
        DailyOpsPlan(
            python_executable=".venv/bin/python",
            lake_root=Path("data/lake"),
            report_path=Path("data/logs/ops.md"),
            readiness_path=Path("data/logs/readiness.json"),
            allow_lot_round_up=True,
            export_opend_ticket_path=Path("data/logs/tickets.jsonl"),
            opend_ticket_response_path=Path("data/logs/responses.jsonl"),
            submit_opend_paper_simulate_tickets=True,
            require_paper_session_calendar_collect=True,
            execution_log_path=Path("data/logs/execution_log.paper_combined.jsonl"),
            broker_report_path=Path("data/logs/futu_order_report.paper_combined.json"),
            paper_session_calendar_path=Path("data/logs/paper_session_calendar.json"),
        )
    )

    assert commands[7][3] == "fetch-opend-runtime-status"
    assert "--require-order-submission-ready" in commands[7]
    assert commands[8][3] == "fetch-opend-account-status"
    assert "--require-paper-simulate-ready" in commands[8]
    assert commands[9][3] == "export-opend-paper-tickets"
    assert commands[10][3] == "paper-session-calendar"
    assert "--require-collect-today" in commands[10]
    assert "--execution-log-path" in commands[10]
    assert "data/logs/execution_log.paper_combined.jsonl" in commands[10]
    assert commands[11][3] == "submit-opend-paper-tickets"
    assert "--allow-failed-resubmit" in commands[11]
    assert commands[12][3] == "paper-simulate-status"


def test_build_daily_ops_commands_can_submit_and_build_paper_simulate_evidence() -> None:
    commands = build_daily_ops_commands(
        DailyOpsPlan(
            python_executable=".venv/bin/python",
            lake_root=Path("data/lake"),
            report_path=Path("data/logs/ops.md"),
            readiness_path=Path("data/logs/readiness.json"),
            allow_lot_round_up=True,
            export_opend_ticket_path=Path("data/logs/tickets.jsonl"),
            opend_ticket_response_path=Path("data/logs/responses.jsonl"),
            paper_simulate_status_path=Path("data/logs/sim_status.json"),
            submit_opend_paper_simulate_tickets=True,
            execution_log_path=Path("data/logs/execution.jsonl"),
            broker_report_path=Path("data/logs/futu.json"),
            profitability_evidence_path=Path("data/logs/profitability.json"),
            paper_blocker_report_path=Path("data/logs/blockers.json"),
            paper_sessions=20,
        )
    )

    assert commands[7][3] == "fetch-opend-runtime-status"
    assert "--require-order-submission-ready" in commands[7]
    assert commands[8][3] == "fetch-opend-account-status"
    assert "--require-paper-simulate-ready" in commands[8]
    assert commands[9][3] == "export-opend-paper-tickets"
    assert commands[10][3] == "submit-opend-paper-tickets"
    assert "--allow-failed-resubmit" in commands[10]
    assert commands[11][3] == "paper-simulate-status"
    assert "--output-path" in commands[11]
    assert "data/logs/sim_status.json" in commands[11]
    assert commands[12][3] == "build-paper-session-evidence-bundle"
    assert "--ticket-path" in commands[12]
    assert "data/logs/tickets.jsonl" in commands[12]
    assert "--response-path" in commands[12]
    assert "data/logs/responses.jsonl" in commands[12]
    assert "--profitability-evidence-path" in commands[12]
    assert "data/logs/profitability.json" in commands[12]
    assert commands[13][3] == "paper-session-calendar"
    assert "--execution-log-path" in commands[13]
    assert "data/logs/execution.jsonl" in commands[13]
    assert "--broker-report-path" in commands[13]
    assert "data/logs/futu.json" in commands[13]
    assert commands[14][3] == "paper-progress"
    assert commands[15][3] == "paper-blocker-report"
    assert "--output-path" in commands[15]
    assert "data/logs/blockers.json" in commands[15]
    assert "--paper-simulate-status-path" in commands[15]
    assert "data/logs/sim_status.json" in commands[15]


def test_build_daily_ops_commands_can_include_real_paper_evidence_paths() -> None:
    commands = build_daily_ops_commands(
        DailyOpsPlan(
            python_executable=".venv/bin/python",
            lake_root=Path("data/lake"),
            report_path=Path("data/logs/ops.md"),
            readiness_path=Path("data/logs/readiness.json"),
            execution_log_path=Path("data/logs/execution.jsonl"),
            broker_report_path=Path("data/logs/futu.json"),
            paper_sessions=20,
            manual_live_enable=True,
        )
    )

    assert commands[7][:4] == [
        ".venv/bin/python",
        "-m",
        "multi_layer_trading_lab.cli",
        "paper-evidence",
    ]
    assert "--execution-log-path" in commands[7]
    assert "data/logs/execution.jsonl" in commands[7]
    assert "--broker-report-path" in commands[7]
    assert "data/logs/futu.json" in commands[7]
    assert commands[8][3] == "paper-session-calendar"
    assert "--execution-log-path" in commands[8]
    assert "data/logs/execution.jsonl" in commands[8]
    assert "--broker-report-path" in commands[8]
    assert "data/logs/futu.json" in commands[8]
    assert commands[9][3] == "paper-progress"
    assert "--execution-log-path" in commands[9]
    assert "data/logs/execution.jsonl" in commands[9]
    assert "--broker-report-path" in commands[9]
    assert "data/logs/futu.json" in commands[9]
    assert commands[10][3] == "paper-blocker-report"
    assert commands[11][3] == "paper-operator-handoff"
    assert "--paper-blocker-report-path" in commands[11]
    assert "data/logs/paper_blocker_report.json" in commands[11]
    assert "--futu-report-path" in commands[13]
    assert "data/logs/futu.json" in commands[13]
    assert "--paper-sessions" in commands[13]
    assert "20" in commands[13]
    assert "--manual-live-enable" in commands[13]
    assert commands[14][3] == "objective-audit"
    assert "--research-input-manifest-path" in commands[14]
    assert "data/logs/research_input_manifest.json" in commands[14]
    assert "--paper-blocker-report-path" in commands[14]
    assert "data/logs/paper_blocker_report.json" in commands[14]
    assert "--paper-progress-path" in commands[14]
    assert "data/logs/paper_progress.json" in commands[14]
    assert "--execution-log-path" in commands[14]
    assert "data/logs/execution.jsonl" in commands[14]
    assert "--broker-report-path" in commands[14]
    assert "data/logs/futu.json" in commands[14]
    assert commands[15][3] == "objective-audit-report"


def test_build_daily_ops_commands_can_build_full_paper_evidence_bundle() -> None:
    commands = build_daily_ops_commands(
        DailyOpsPlan(
            python_executable=".venv/bin/python",
            lake_root=Path("data/lake"),
            report_path=Path("data/logs/ops.md"),
            readiness_path=Path("data/logs/readiness.json"),
            ticket_path=Path("data/logs/tickets.jsonl"),
            response_path=Path("data/logs/responses.jsonl"),
            mark_prices_path=Path("data/logs/marks.json"),
            execution_log_path=Path("data/logs/execution.jsonl"),
            broker_report_path=Path("data/logs/futu.json"),
            profitability_evidence_path=Path("data/logs/profitability.json"),
            paper_sessions=20,
            manual_live_enable=True,
        )
    )

    assert commands[7][:4] == [
        ".venv/bin/python",
        "-m",
        "multi_layer_trading_lab.cli",
        "build-paper-session-evidence-bundle",
    ]
    assert "--ticket-path" in commands[7]
    assert "data/logs/tickets.jsonl" in commands[7]
    assert "--response-path" in commands[7]
    assert "data/logs/responses.jsonl" in commands[7]
    assert "--mark-prices-path" in commands[7]
    assert "data/logs/marks.json" in commands[7]
    assert "--profitability-evidence-path" in commands[7]
    assert "data/logs/profitability.json" in commands[7]
    assert "--manual-live-enable" in commands[7]
    assert commands[8][3] == "paper-session-calendar"
    assert commands[9][3] == "paper-progress"
    assert "--profitability-evidence-path" in commands[9]
    assert "data/logs/profitability.json" in commands[9]
    assert commands[10][3] == "paper-blocker-report"
    assert commands[11][3] == "paper-operator-handoff"
    assert "--paper-blocker-report-path" in commands[11]
    assert "data/logs/paper_blocker_report.json" in commands[11]
    assert commands[14][3] == "objective-audit"
    assert "--research-input-manifest-path" in commands[14]
    assert "data/logs/research_input_manifest.json" in commands[14]
    assert "--profitability-evidence-path" in commands[14]
    assert "data/logs/profitability.json" in commands[14]
    assert "--paper-blocker-report-path" in commands[14]
    assert "data/logs/paper_blocker_report.json" in commands[14]
    assert "--paper-progress-path" in commands[14]
    assert "data/logs/paper_progress.json" in commands[14]
    assert "--execution-log-path" in commands[14]
    assert "data/logs/execution.jsonl" in commands[14]
    assert "--broker-report-path" in commands[14]
    assert "data/logs/futu.json" in commands[14]


def test_build_daily_ops_commands_can_auto_build_mark_prices_from_quote() -> None:
    commands = build_daily_ops_commands(
        DailyOpsPlan(
            python_executable=".venv/bin/python",
            lake_root=Path("data/lake"),
            report_path=Path("data/logs/ops.md"),
            readiness_path=Path("data/logs/readiness.json"),
            ticket_path=Path("data/logs/tickets.jsonl"),
            response_path=Path("data/logs/responses.jsonl"),
            execution_log_path=Path("data/logs/execution.jsonl"),
            broker_report_path=Path("data/logs/futu.json"),
            opend_quote_snapshot_path=Path("data/logs/quote.json"),
            build_mark_prices_from_opend_quote=True,
            paper_sessions=20,
        )
    )

    assert commands[7][:4] == [
        ".venv/bin/python",
        "-m",
        "multi_layer_trading_lab.cli",
        "build-mark-prices-from-opend-quote",
    ]
    assert "--quote-snapshot-path" in commands[7]
    assert "data/logs/quote.json" in commands[7]
    assert "--output-path" in commands[7]
    assert "data/logs/mark_prices.json" in commands[7]
    assert commands[8][3] == "build-paper-session-evidence-bundle"
    assert "--mark-prices-path" in commands[8]
    assert "data/logs/mark_prices.json" in commands[8]
    assert commands[9][3] == "paper-session-calendar"
    assert commands[10][3] == "paper-progress"
