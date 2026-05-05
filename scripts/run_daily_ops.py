import argparse
import sys
from pathlib import Path

from multi_layer_trading_lab.runtime.daily_ops import default_plan, run_daily_ops_plan


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the daily local ops checks.")
    parser.add_argument("--lake-root", default="data/lake")
    parser.add_argument("--report-path", default="data/logs/ops_daily_report.md")
    parser.add_argument("--readiness-path", default="data/logs/go_live_readiness.json")
    parser.add_argument("--objective-audit-path", default="data/logs/objective_audit.json")
    parser.add_argument("--objective-audit-report-path", default="data/logs/objective_audit.md")
    parser.add_argument(
        "--profitability-evidence-path",
        default="data/logs/profitability_evidence.json",
    )
    parser.add_argument("--paper-progress-path", default="data/logs/paper_progress.json")
    parser.add_argument(
        "--paper-blocker-report-path",
        default="data/logs/paper_blocker_report.json",
    )
    parser.add_argument(
        "--paper-session-calendar-path",
        default="data/logs/paper_session_calendar.json",
    )
    parser.add_argument(
        "--opend-quote-snapshot-path",
        default="data/logs/opend_quote_snapshot.json",
    )
    parser.add_argument(
        "--opend-runtime-status-path",
        default="data/logs/opend_runtime_status.json",
    )
    parser.add_argument(
        "--opend-account-status-path",
        default="data/logs/opend_account_status.json",
    )
    parser.add_argument(
        "--opend-ticket-response-path",
        default="data/logs/opend_paper_ticket_responses.jsonl",
    )
    parser.add_argument(
        "--paper-simulate-status-path",
        default="data/logs/paper_simulate_status.json",
    )
    parser.add_argument("--ifind-events-file-path")
    parser.add_argument(
        "--ifind-validation-report-path",
        default="data/logs/ifind_events_validation.json",
    )
    parser.add_argument(
        "--ifind-ingestion-status-path",
        default="data/logs/ifind_ingestion_status.json",
    )
    parser.add_argument(
        "--ifind-connection-plan-path",
        default="data/logs/ifind_connection_plan.json",
    )
    parser.add_argument("--ifind-source-run-id")
    parser.add_argument("--ifind-overwrite", action="store_true")
    parser.add_argument(
        "--research-input-manifest-path",
        default="data/logs/research_input_manifest.json",
    )
    parser.add_argument("--paper-session-plan-path", default="data/logs/paper_session_plan.json")
    parser.add_argument("--allow-lot-round-up", action="store_true")
    parser.add_argument("--export-opend-ticket-path")
    parser.add_argument("--submit-opend-dry-run-tickets", action="store_true")
    parser.add_argument("--submit-opend-paper-simulate-tickets", action="store_true")
    parser.add_argument("--require-paper-session-calendar-collect", action="store_true")
    parser.add_argument(
        "--no-submit-opend-allow-failed-resubmit",
        action="store_false",
        dest="submit_opend_allow_failed_resubmit",
        help=(
            "Do not automatically retry tickets that only have failed OpenD responses "
            "in the output JSONL."
        ),
    )
    parser.add_argument("--submit-opend-max-attempts", type=int, default=3)
    parser.add_argument("--submit-opend-retry-delay-seconds", type=float, default=0.5)
    parser.add_argument("--ticket-path")
    parser.add_argument("--response-path")
    parser.add_argument("--mark-prices-path")
    parser.add_argument("--build-mark-prices-from-opend-quote", action="store_true")
    parser.add_argument("--execution-log-path")
    parser.add_argument("--broker-report-path")
    parser.add_argument("--paper-sessions", type=int, default=0)
    parser.add_argument("--account-equity", type=float, default=1_000_000.0)
    parser.add_argument("--opend-mode", default="paper")
    parser.add_argument("--opend-env", default="SIMULATE")
    parser.add_argument("--manual-live-enable", action="store_true")
    args = parser.parse_args()

    plan = default_plan(
        lake_root=Path(args.lake_root),
        report_path=Path(args.report_path),
        readiness_path=Path(args.readiness_path),
        objective_audit_path=Path(args.objective_audit_path),
        objective_audit_report_path=Path(args.objective_audit_report_path),
        profitability_evidence_path=Path(args.profitability_evidence_path),
        paper_progress_path=Path(args.paper_progress_path),
        paper_blocker_report_path=Path(args.paper_blocker_report_path),
        paper_session_calendar_path=Path(args.paper_session_calendar_path),
        opend_quote_snapshot_path=Path(args.opend_quote_snapshot_path),
        opend_runtime_status_path=Path(args.opend_runtime_status_path),
        opend_account_status_path=Path(args.opend_account_status_path),
        opend_ticket_response_path=Path(args.opend_ticket_response_path),
        paper_simulate_status_path=Path(args.paper_simulate_status_path),
        ifind_events_file_path=(
            Path(args.ifind_events_file_path) if args.ifind_events_file_path else None
        ),
        ifind_validation_report_path=Path(args.ifind_validation_report_path),
        ifind_ingestion_status_path=Path(args.ifind_ingestion_status_path),
        ifind_connection_plan_path=Path(args.ifind_connection_plan_path),
        ifind_source_run_id=args.ifind_source_run_id,
        ifind_overwrite=args.ifind_overwrite,
        research_input_manifest_path=Path(args.research_input_manifest_path),
        paper_session_plan_path=Path(args.paper_session_plan_path),
        allow_lot_round_up=args.allow_lot_round_up,
        export_opend_ticket_path=(
            Path(args.export_opend_ticket_path) if args.export_opend_ticket_path else None
        ),
        submit_opend_dry_run_tickets=args.submit_opend_dry_run_tickets,
        submit_opend_paper_simulate_tickets=args.submit_opend_paper_simulate_tickets,
        require_paper_session_calendar_collect=args.require_paper_session_calendar_collect,
        submit_opend_allow_failed_resubmit=args.submit_opend_allow_failed_resubmit,
        submit_opend_max_attempts=args.submit_opend_max_attempts,
        submit_opend_retry_delay_seconds=args.submit_opend_retry_delay_seconds,
        ticket_path=Path(args.ticket_path) if args.ticket_path else None,
        response_path=Path(args.response_path) if args.response_path else None,
        mark_prices_path=Path(args.mark_prices_path) if args.mark_prices_path else None,
        build_mark_prices_from_opend_quote=args.build_mark_prices_from_opend_quote,
        execution_log_path=Path(args.execution_log_path) if args.execution_log_path else None,
        broker_report_path=Path(args.broker_report_path) if args.broker_report_path else None,
        account_equity=args.account_equity,
        paper_sessions=args.paper_sessions,
        opend_mode=args.opend_mode,
        opend_env=args.opend_env,
        manual_live_enable=args.manual_live_enable,
    )
    failed = False
    for result in run_daily_ops_plan(plan):
        print(result.stdout, end="")
        if result.stderr:
            print(result.stderr, end="", file=sys.stderr)
        if result.returncode != 0:
            failed = True
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
