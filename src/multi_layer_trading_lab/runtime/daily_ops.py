from __future__ import annotations

import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class DailyOpsPlan:
    python_executable: str
    lake_root: Path
    report_path: Path
    readiness_path: Path
    objective_audit_path: Path = Path("data/logs/objective_audit.json")
    objective_audit_report_path: Path = Path("data/logs/objective_audit.md")
    profitability_evidence_path: Path | None = Path("data/logs/profitability_evidence.json")
    paper_progress_path: Path | None = Path("data/logs/paper_progress.json")
    paper_blocker_report_path: Path | None = Path("data/logs/paper_blocker_report.json")
    paper_operator_handoff_path: Path | None = Path("data/logs/paper_operator_handoff.json")
    paper_session_calendar_path: Path | None = Path("data/logs/paper_session_calendar.json")
    opend_quote_snapshot_path: Path | None = Path("data/logs/opend_quote_snapshot.json")
    opend_runtime_status_path: Path | None = Path("data/logs/opend_runtime_status.json")
    opend_account_status_path: Path | None = Path("data/logs/opend_account_status.json")
    opend_ticket_response_path: Path | None = Path("data/logs/opend_paper_ticket_responses.jsonl")
    paper_simulate_status_path: Path | None = Path("data/logs/paper_simulate_status.json")
    ifind_events_file_path: Path | None = None
    ifind_validation_report_path: Path = Path("data/logs/ifind_events_validation.json")
    ifind_ingestion_status_path: Path = Path("data/logs/ifind_ingestion_status.json")
    ifind_connection_plan_path: Path = Path("data/logs/ifind_connection_plan.json")
    ifind_source_run_id: str | None = None
    ifind_overwrite: bool = False
    research_input_manifest_path: Path = Path("data/logs/research_input_manifest.json")
    paper_session_plan_path: Path = Path("data/logs/paper_session_plan.json")
    allow_lot_round_up: bool = False
    export_opend_ticket_path: Path | None = None
    submit_opend_dry_run_tickets: bool = False
    submit_opend_paper_simulate_tickets: bool = False
    require_paper_session_calendar_collect: bool = False
    submit_opend_allow_failed_resubmit: bool = True
    submit_opend_max_attempts: int = 3
    submit_opend_retry_delay_seconds: float = 0.5
    ticket_path: Path | None = None
    response_path: Path | None = None
    mark_prices_path: Path | None = None
    build_mark_prices_from_opend_quote: bool = False
    execution_log_path: Path | None = None
    broker_report_path: Path | None = None
    account_equity: float = 1_000_000.0
    paper_sessions: int = 0
    opend_mode: str = "paper"
    opend_env: str = "SIMULATE"
    manual_live_enable: bool = False


def build_daily_ops_commands(plan: DailyOpsPlan) -> list[list[str]]:
    base = [plan.python_executable, "-m", "multi_layer_trading_lab.cli"]
    account_equity = f"{plan.account_equity:.2f}"
    resolved_mark_prices_path = plan.mark_prices_path
    if plan.build_mark_prices_from_opend_quote:
        resolved_mark_prices_path = plan.mark_prices_path or Path(
            "data/logs/mark_prices.json"
        )
    commands = []
    if plan.ifind_events_file_path is not None:
        validate_ifind_command = [
            *base,
            "validate-ifind-events-file",
            str(plan.ifind_events_file_path),
            "--output-path",
            str(plan.ifind_validation_report_path),
        ]
        import_ifind_command = [
            *base,
            "import-ifind-events-file",
            str(plan.ifind_events_file_path),
            "--lake-root",
            str(plan.lake_root),
        ]
        if plan.ifind_source_run_id:
            validate_ifind_command.extend(["--source-run-id", plan.ifind_source_run_id])
            import_ifind_command.extend(["--source-run-id", plan.ifind_source_run_id])
        if plan.ifind_overwrite:
            import_ifind_command.append("--overwrite")
        commands.extend([validate_ifind_command, import_ifind_command])

    paper_session_plan_command = [
        *base,
        "paper-session-plan",
        "--output-path",
        str(plan.paper_session_plan_path),
        "--lake-root",
        str(plan.lake_root),
        "--account-equity",
        account_equity,
        "--opend-mode",
        "paper",
        "--opend-env",
        "SIMULATE",
    ]
    if plan.opend_quote_snapshot_path is not None:
        paper_session_plan_command.extend(
            ["--quote-snapshot-path", str(plan.opend_quote_snapshot_path)]
        )
    if plan.allow_lot_round_up:
        paper_session_plan_command.append("--allow-lot-round-up")

    commands.extend([
        [
            *base,
            "ifind-ingestion-status",
            "--lake-root",
            str(plan.lake_root),
            "--output-path",
            str(plan.ifind_ingestion_status_path),
        ],
        [
            *base,
            "ifind-connection-plan",
            "--lake-root",
            str(plan.lake_root),
            "--output-path",
            str(plan.ifind_connection_plan_path),
        ],
        [
            *base,
            "research-input-manifest",
            "--output-path",
            str(plan.research_input_manifest_path),
        ],
        [
            *base,
            "factor-factory-summary",
            "--lake-root",
            str(plan.lake_root),
        ],
        [
            *base,
            "external-factor-portfolio",
            "--lake-root",
            str(plan.lake_root),
            "--account-equity",
            account_equity,
        ],
        [
            *base,
            "build-external-research-signals",
            "--lake-root",
            str(plan.lake_root),
        ],
        paper_session_plan_command,
    ])
    if plan.export_opend_ticket_path is not None:
        if plan.submit_opend_paper_simulate_tickets and plan.opend_account_status_path is not None:
            if plan.opend_runtime_status_path is not None:
                commands.append(
                    [
                        *base,
                        "fetch-opend-runtime-status",
                        "--output-path",
                        str(plan.opend_runtime_status_path),
                        "--require-order-submission-ready",
                    ]
                )
            commands.append(
                [
                    *base,
                    "fetch-opend-account-status",
                    "--output-path",
                    str(plan.opend_account_status_path),
                    "--require-paper-simulate-ready",
                ]
            )
        export_ticket_command = [
            *base,
            "export-opend-paper-tickets",
            "--plan-path",
            str(plan.paper_session_plan_path),
            "--output-path",
            str(plan.export_opend_ticket_path),
        ]
        if plan.opend_quote_snapshot_path is not None:
            export_ticket_command.extend(
                ["--quote-snapshot-path", str(plan.opend_quote_snapshot_path)]
            )
        commands.append(export_ticket_command)
    if plan.submit_opend_dry_run_tickets or plan.submit_opend_paper_simulate_tickets:
        submit_ticket_path = plan.export_opend_ticket_path or plan.ticket_path
        submit_response_path = plan.response_path or plan.opend_ticket_response_path
        if submit_ticket_path is not None and submit_response_path is not None:
            if (
                plan.submit_opend_paper_simulate_tickets
                and plan.require_paper_session_calendar_collect
                and plan.execution_log_path is not None
                and plan.broker_report_path is not None
                and plan.paper_session_calendar_path is not None
            ):
                commands.append(
                    [
                        *base,
                        "paper-session-calendar",
                        "--execution-log-path",
                        str(plan.execution_log_path),
                        "--broker-report-path",
                        str(plan.broker_report_path),
                        "--output-path",
                        str(plan.paper_session_calendar_path),
                        "--target-sessions",
                        "20",
                        "--require-collect-today",
                    ]
                )
            submit_command = [
                *base,
                "submit-opend-paper-tickets",
                "--ticket-path",
                str(submit_ticket_path),
                "--output-path",
                str(submit_response_path),
                "--max-attempts",
                str(plan.submit_opend_max_attempts),
                "--retry-delay-seconds",
                f"{plan.submit_opend_retry_delay_seconds:.3f}",
            ]
            if plan.submit_opend_paper_simulate_tickets:
                submit_command.append("--submit-paper-simulate")
                if plan.opend_runtime_status_path is not None:
                    submit_command.extend(
                        [
                            "--opend-runtime-status-path",
                            str(plan.opend_runtime_status_path),
                        ]
                    )
                if plan.submit_opend_allow_failed_resubmit:
                    submit_command.append("--allow-failed-resubmit")
            commands.append(submit_command)
            if (
                plan.submit_opend_paper_simulate_tickets
                and plan.paper_simulate_status_path is not None
            ):
                commands.append(
                    [
                        *base,
                        "paper-simulate-status",
                        "--response-path",
                        str(submit_response_path),
                        "--output-path",
                        str(plan.paper_simulate_status_path),
                    ]
                )
    evidence_ticket_path = plan.ticket_path or plan.export_opend_ticket_path
    evidence_response_path = plan.response_path or plan.opend_ticket_response_path
    if (
        evidence_ticket_path is not None
        and plan.execution_log_path is not None
        and plan.broker_report_path is not None
    ):
        if plan.build_mark_prices_from_opend_quote:
            commands.append(
                [
                    *base,
                    "build-mark-prices-from-opend-quote",
                    "--quote-snapshot-path",
                    str(
                        plan.opend_quote_snapshot_path
                        or Path("data/logs/opend_quote_snapshot.json")
                    ),
                    "--output-path",
                    str(resolved_mark_prices_path),
                ]
            )
        paper_evidence_command = [
            *base,
            "build-paper-session-evidence-bundle",
            "--ticket-path",
            str(evidence_ticket_path),
            "--broker-report-path",
            str(plan.broker_report_path),
            "--execution-log-path",
            str(plan.execution_log_path),
            "--profitability-evidence-path",
            str(
                plan.profitability_evidence_path
                or Path("data/logs/profitability_evidence.json")
            ),
            "--paper-sessions",
            str(plan.paper_sessions),
        ]
        if evidence_response_path is not None:
            paper_evidence_command.extend(["--response-path", str(evidence_response_path)])
        else:
            paper_evidence_command.extend(["--response-path", ""])
        if resolved_mark_prices_path is not None:
            paper_evidence_command.extend(["--mark-prices-path", str(resolved_mark_prices_path)])
        if plan.manual_live_enable:
            paper_evidence_command.append("--manual-live-enable")
        commands.append(paper_evidence_command)
    elif plan.execution_log_path is not None and plan.broker_report_path is not None:
        paper_evidence_command = [
            *base,
            "paper-evidence",
            "--execution-log-path",
            str(plan.execution_log_path),
            "--broker-report-path",
            str(plan.broker_report_path),
            "--paper-sessions",
            str(plan.paper_sessions),
        ]
        if plan.manual_live_enable:
            paper_evidence_command.append("--manual-live-enable")
        commands.append(paper_evidence_command)

    if (
        plan.execution_log_path is not None
        and plan.broker_report_path is not None
        and plan.paper_session_calendar_path is not None
    ):
        commands.append(
            [
                *base,
                "paper-session-calendar",
                "--execution-log-path",
                str(plan.execution_log_path),
                "--broker-report-path",
                str(plan.broker_report_path),
                "--output-path",
                str(plan.paper_session_calendar_path),
                "--target-sessions",
                "20",
            ]
        )

    if (
        plan.execution_log_path is not None
        and plan.broker_report_path is not None
        and plan.profitability_evidence_path is not None
        and plan.paper_progress_path is not None
    ):
        commands.append(
            [
                *base,
                "paper-progress",
                "--execution-log-path",
                str(plan.execution_log_path),
                "--broker-report-path",
                str(plan.broker_report_path),
                "--profitability-evidence-path",
                str(plan.profitability_evidence_path),
                "--output-path",
                str(plan.paper_progress_path),
                "--target-sessions",
                "20",
            ]
        )

    if plan.paper_blocker_report_path is not None and (
        plan.submit_opend_paper_simulate_tickets
        or (plan.execution_log_path is not None and plan.broker_report_path is not None)
    ):
        blocker_command = [
            *base,
            "paper-blocker-report",
            "--output-path",
            str(plan.paper_blocker_report_path),
        ]
        if plan.opend_runtime_status_path is not None:
            blocker_command.extend(
                ["--opend-runtime-status-path", str(plan.opend_runtime_status_path)]
            )
        if plan.paper_simulate_status_path is not None:
            blocker_command.extend(
                ["--paper-simulate-status-path", str(plan.paper_simulate_status_path)]
            )
        if plan.paper_session_calendar_path is not None:
            blocker_command.extend(
                ["--paper-session-calendar-path", str(plan.paper_session_calendar_path)]
            )
        if plan.paper_progress_path is not None:
            blocker_command.extend(["--paper-progress-path", str(plan.paper_progress_path)])
        commands.append(blocker_command)
        if plan.paper_operator_handoff_path is not None:
            commands.append(
                [
                    *base,
                    "paper-operator-handoff",
                    "--paper-blocker-report-path",
                    str(plan.paper_blocker_report_path),
                    "--output-path",
                    str(plan.paper_operator_handoff_path),
                ]
            )

    ops_command = [
        *base,
        "ops-report",
        "--output-path",
        str(plan.report_path),
        "--lake-root",
        str(plan.lake_root),
        "--account-equity",
        account_equity,
        "--opend-mode",
        plan.opend_mode,
        "--opend-env",
        plan.opend_env,
        "--include-research-audit",
        "--signal-dataset",
        "external_research_signal_events",
        "--use-external-portfolio-audits",
        "--use-external-lookahead-audit",
    ]
    commands.append(ops_command)

    readiness_command = [
        *base,
        "go-live-readiness",
        "--output-path",
        str(plan.readiness_path),
        "--lake-root",
        str(plan.lake_root),
        "--account-equity",
        account_equity,
        "--opend-mode",
        plan.opend_mode,
        "--opend-env",
        plan.opend_env,
        "--signal-dataset",
        "external_research_signal_events",
        "--use-external-portfolio-audits",
        "--use-external-lookahead-audit",
    ]
    if plan.execution_log_path is not None and plan.broker_report_path is not None:
        readiness_command.extend(
            [
                "--execution-log-path",
                str(plan.execution_log_path),
                "--futu-report-path",
                str(plan.broker_report_path),
                "--paper-sessions",
                str(plan.paper_sessions),
            ]
        )
        if plan.manual_live_enable:
            readiness_command.append("--manual-live-enable")
    commands.append(readiness_command)

    objective_audit_command = [
        *base,
        "objective-audit",
        "--readiness-manifest-path",
        str(plan.readiness_path),
        "--output-path",
        str(plan.objective_audit_path),
        "--research-input-manifest-path",
        str(plan.research_input_manifest_path),
    ]
    if plan.profitability_evidence_path is not None:
        objective_audit_command.extend(
            ["--profitability-evidence-path", str(plan.profitability_evidence_path)]
        )
    if plan.ifind_events_file_path is not None:
        objective_audit_command.extend(
            ["--ifind-validation-report-path", str(plan.ifind_validation_report_path)]
        )
    objective_audit_command.extend(
        ["--ifind-ingestion-status-path", str(plan.ifind_ingestion_status_path)]
    )
    if plan.opend_quote_snapshot_path is not None:
        objective_audit_command.extend(
            ["--opend-quote-snapshot-path", str(plan.opend_quote_snapshot_path)]
        )
    if plan.opend_runtime_status_path is not None:
        objective_audit_command.extend(
            ["--opend-runtime-status-path", str(plan.opend_runtime_status_path)]
        )
    if plan.opend_account_status_path is not None:
        objective_audit_command.extend(
            ["--opend-account-status-path", str(plan.opend_account_status_path)]
        )
    if plan.opend_ticket_response_path is not None:
        objective_audit_command.extend(
            ["--opend-ticket-response-path", str(plan.opend_ticket_response_path)]
        )
    if plan.paper_simulate_status_path is not None:
        objective_audit_command.extend(
            ["--paper-simulate-status-path", str(plan.paper_simulate_status_path)]
        )
    if plan.paper_blocker_report_path is not None:
        objective_audit_command.extend(
            ["--paper-blocker-report-path", str(plan.paper_blocker_report_path)]
        )
    if plan.paper_operator_handoff_path is not None:
        objective_audit_command.extend(
            ["--paper-operator-handoff-path", str(plan.paper_operator_handoff_path)]
        )
    if plan.paper_progress_path is not None:
        objective_audit_command.extend(
            ["--paper-progress-path", str(plan.paper_progress_path)]
        )
    if plan.execution_log_path is not None:
        objective_audit_command.extend(
            ["--execution-log-path", str(plan.execution_log_path)]
        )
    if plan.broker_report_path is not None:
        objective_audit_command.extend(
            ["--broker-report-path", str(plan.broker_report_path)]
        )
    commands.append(objective_audit_command)
    commands.append(
        [
            *base,
            "objective-audit-report",
            "--audit-path",
            str(plan.objective_audit_path),
            "--output-path",
            str(plan.objective_audit_report_path),
        ]
    )
    return commands


def run_daily_ops_plan(plan: DailyOpsPlan) -> list[subprocess.CompletedProcess[str]]:
    plan.report_path.parent.mkdir(parents=True, exist_ok=True)
    plan.readiness_path.parent.mkdir(parents=True, exist_ok=True)
    plan.objective_audit_path.parent.mkdir(parents=True, exist_ok=True)
    plan.objective_audit_report_path.parent.mkdir(parents=True, exist_ok=True)
    plan.ifind_validation_report_path.parent.mkdir(parents=True, exist_ok=True)
    plan.ifind_ingestion_status_path.parent.mkdir(parents=True, exist_ok=True)
    plan.research_input_manifest_path.parent.mkdir(parents=True, exist_ok=True)
    plan.paper_session_plan_path.parent.mkdir(parents=True, exist_ok=True)
    if plan.execution_log_path is not None:
        plan.execution_log_path.parent.mkdir(parents=True, exist_ok=True)
    if plan.broker_report_path is not None:
        plan.broker_report_path.parent.mkdir(parents=True, exist_ok=True)
    if plan.ticket_path is not None:
        plan.ticket_path.parent.mkdir(parents=True, exist_ok=True)
    if plan.export_opend_ticket_path is not None:
        plan.export_opend_ticket_path.parent.mkdir(parents=True, exist_ok=True)
    if plan.response_path is not None:
        plan.response_path.parent.mkdir(parents=True, exist_ok=True)
    if plan.paper_simulate_status_path is not None:
        plan.paper_simulate_status_path.parent.mkdir(parents=True, exist_ok=True)
    if (
        plan.submit_opend_dry_run_tickets or plan.submit_opend_paper_simulate_tickets
    ) and plan.opend_ticket_response_path is not None:
        plan.opend_ticket_response_path.parent.mkdir(parents=True, exist_ok=True)
    if plan.mark_prices_path is not None:
        plan.mark_prices_path.parent.mkdir(parents=True, exist_ok=True)
    if plan.opend_account_status_path is not None:
        plan.opend_account_status_path.parent.mkdir(parents=True, exist_ok=True)
    if plan.opend_runtime_status_path is not None:
        plan.opend_runtime_status_path.parent.mkdir(parents=True, exist_ok=True)
    if plan.paper_progress_path is not None:
        plan.paper_progress_path.parent.mkdir(parents=True, exist_ok=True)
    if plan.paper_blocker_report_path is not None:
        plan.paper_blocker_report_path.parent.mkdir(parents=True, exist_ok=True)
    if plan.paper_operator_handoff_path is not None:
        plan.paper_operator_handoff_path.parent.mkdir(parents=True, exist_ok=True)
    if plan.paper_session_calendar_path is not None:
        plan.paper_session_calendar_path.parent.mkdir(parents=True, exist_ok=True)
    results = []
    blocked_submission = False
    for command in build_daily_ops_commands(plan):
        command_name = _command_name(command)
        if blocked_submission and not _run_after_submission_block(command_name):
            continue
        result = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
        )
        results.append(result)
        if result.returncode != 0 and _blocks_submission(command):
            blocked_submission = True
            continue
        if result.returncode != 0:
            raise subprocess.CalledProcessError(
                result.returncode,
                command,
                output=result.stdout,
                stderr=result.stderr,
            )
    return results


def _command_name(command: list[str]) -> str:
    return command[3] if len(command) > 3 else ""


def _blocks_submission(command: list[str]) -> bool:
    name = _command_name(command)
    if name == "fetch-opend-runtime-status":
        return "--require-order-submission-ready" in command
    if name == "paper-session-calendar":
        return "--require-collect-today" in command
    if name == "fetch-opend-account-status":
        return True
    return False


def _run_after_submission_block(command_name: str) -> bool:
    return command_name in {
        "paper-blocker-report",
        "paper-operator-handoff",
        "ops-report",
        "go-live-readiness",
        "objective-audit",
        "objective-audit-report",
    }


def default_plan(
    *,
    lake_root: Path = Path("data/lake"),
    report_path: Path = Path("data/logs/ops_daily_report.md"),
    readiness_path: Path = Path("data/logs/go_live_readiness.json"),
    objective_audit_path: Path = Path("data/logs/objective_audit.json"),
    objective_audit_report_path: Path = Path("data/logs/objective_audit.md"),
    profitability_evidence_path: Path | None = Path("data/logs/profitability_evidence.json"),
    paper_progress_path: Path | None = Path("data/logs/paper_progress.json"),
    paper_blocker_report_path: Path | None = Path("data/logs/paper_blocker_report.json"),
    paper_operator_handoff_path: Path | None = Path("data/logs/paper_operator_handoff.json"),
    paper_session_calendar_path: Path | None = Path("data/logs/paper_session_calendar.json"),
    opend_quote_snapshot_path: Path | None = Path("data/logs/opend_quote_snapshot.json"),
    opend_runtime_status_path: Path | None = Path("data/logs/opend_runtime_status.json"),
    opend_account_status_path: Path | None = Path("data/logs/opend_account_status.json"),
    opend_ticket_response_path: Path | None = Path("data/logs/opend_paper_ticket_responses.jsonl"),
    paper_simulate_status_path: Path | None = Path("data/logs/paper_simulate_status.json"),
    ifind_events_file_path: Path | None = None,
    ifind_validation_report_path: Path = Path("data/logs/ifind_events_validation.json"),
    ifind_ingestion_status_path: Path = Path("data/logs/ifind_ingestion_status.json"),
    ifind_connection_plan_path: Path = Path("data/logs/ifind_connection_plan.json"),
    ifind_source_run_id: str | None = None,
    ifind_overwrite: bool = False,
    research_input_manifest_path: Path = Path("data/logs/research_input_manifest.json"),
    paper_session_plan_path: Path = Path("data/logs/paper_session_plan.json"),
    allow_lot_round_up: bool = False,
    export_opend_ticket_path: Path | None = None,
    submit_opend_dry_run_tickets: bool = False,
    submit_opend_paper_simulate_tickets: bool = False,
    require_paper_session_calendar_collect: bool = False,
    submit_opend_allow_failed_resubmit: bool = True,
    submit_opend_max_attempts: int = 3,
    submit_opend_retry_delay_seconds: float = 0.5,
    ticket_path: Path | None = None,
    response_path: Path | None = None,
    mark_prices_path: Path | None = None,
    build_mark_prices_from_opend_quote: bool = False,
    execution_log_path: Path | None = None,
    broker_report_path: Path | None = None,
    account_equity: float = 1_000_000.0,
    paper_sessions: int = 0,
    opend_mode: str = "paper",
    opend_env: str = "SIMULATE",
    manual_live_enable: bool = False,
) -> DailyOpsPlan:
    return DailyOpsPlan(
        python_executable=sys.executable,
        lake_root=lake_root,
        report_path=report_path,
        readiness_path=readiness_path,
        objective_audit_path=objective_audit_path,
        objective_audit_report_path=objective_audit_report_path,
        profitability_evidence_path=profitability_evidence_path,
        paper_progress_path=paper_progress_path,
        paper_blocker_report_path=paper_blocker_report_path,
        paper_operator_handoff_path=paper_operator_handoff_path,
        paper_session_calendar_path=paper_session_calendar_path,
        opend_quote_snapshot_path=opend_quote_snapshot_path,
        opend_runtime_status_path=opend_runtime_status_path,
        opend_account_status_path=opend_account_status_path,
        opend_ticket_response_path=opend_ticket_response_path,
        paper_simulate_status_path=paper_simulate_status_path,
        ifind_events_file_path=ifind_events_file_path,
        ifind_validation_report_path=ifind_validation_report_path,
        ifind_ingestion_status_path=ifind_ingestion_status_path,
        ifind_connection_plan_path=ifind_connection_plan_path,
        ifind_source_run_id=ifind_source_run_id,
        ifind_overwrite=ifind_overwrite,
        research_input_manifest_path=research_input_manifest_path,
        paper_session_plan_path=paper_session_plan_path,
        allow_lot_round_up=allow_lot_round_up,
        export_opend_ticket_path=export_opend_ticket_path,
        submit_opend_dry_run_tickets=submit_opend_dry_run_tickets,
        submit_opend_paper_simulate_tickets=submit_opend_paper_simulate_tickets,
        require_paper_session_calendar_collect=require_paper_session_calendar_collect,
        submit_opend_allow_failed_resubmit=submit_opend_allow_failed_resubmit,
        submit_opend_max_attempts=submit_opend_max_attempts,
        submit_opend_retry_delay_seconds=submit_opend_retry_delay_seconds,
        ticket_path=ticket_path,
        response_path=response_path,
        mark_prices_path=mark_prices_path,
        build_mark_prices_from_opend_quote=build_mark_prices_from_opend_quote,
        execution_log_path=execution_log_path,
        broker_report_path=broker_report_path,
        account_equity=account_equity,
        paper_sessions=paper_sessions,
        opend_mode=opend_mode,
        opend_env=opend_env,
        manual_live_enable=manual_live_enable,
    )
