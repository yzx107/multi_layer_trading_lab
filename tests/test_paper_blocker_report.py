from __future__ import annotations

import json
import os

from multi_layer_trading_lab.execution.paper_blocker_report import build_paper_blocker_report


def test_paper_blocker_report_aggregates_runtime_and_session_blockers(tmp_path) -> None:
    runtime = tmp_path / "runtime.json"
    paper_status = tmp_path / "paper_status.json"
    calendar = tmp_path / "calendar.json"
    progress = tmp_path / "progress.json"
    runtime.write_text(
        json.dumps(
            {
                "kill_switch": True,
                "kill_switch_file": "/tmp/futu-opend-execution.KILL",
                "ready_for_order_submission": False,
                "failed_reasons": ["opend_kill_switch_enabled"],
            }
        ),
        encoding="utf-8",
    )
    paper_status.write_text(
        json.dumps(
            {
                "ready_for_session_collection": False,
                "next_required_action": "resubmit_paper_simulate_tickets",
                "failed_reasons": ["missing_submitted_responses"],
            }
        ),
        encoding="utf-8",
    )
    calendar.write_text(
        json.dumps({"next_required_action": "collect_today_paper_session"}),
        encoding="utf-8",
    )
    progress.write_text(
        json.dumps(
            {
                "ready_for_live_review": False,
                "sessions_remaining": 19,
                "failed_reasons": ["paper_sessions_remaining"],
            }
        ),
        encoding="utf-8",
    )

    report = build_paper_blocker_report(
        runtime_status_path=runtime,
        paper_simulate_status_path=paper_status,
        paper_calendar_path=calendar,
        paper_progress_path=progress,
    )

    assert report.ready_for_next_session is False
    assert report.ready_for_live_review is False
    assert (
        report.next_required_action
        == "clear_opend_kill_switch_then_resubmit_paper_simulate"
    )
    assert report.sessions_remaining == 19
    assert "opend_kill_switch_enabled" in report.failed_reasons
    assert "opend_kill_switch_enabled" in report.next_session_failed_reasons
    assert report.blocker_details["opend_kill_switch"] == {
        "enabled": True,
        "kill_switch_file": "/tmp/futu-opend-execution.KILL",
        "requires_manual_operator_authorization": True,
        "automation_allowed": False,
        "next_safe_action": "operator_must_explicitly_clear_kill_switch_before_resubmit",
    }
    assert "missing_submitted_responses" in report.failed_reasons
    assert "paper_sessions_remaining" in report.failed_reasons
    assert "paper_sessions_remaining" not in report.next_session_failed_reasons


def test_paper_blocker_report_separates_next_session_from_live_review(tmp_path) -> None:
    runtime = tmp_path / "runtime.json"
    paper_status = tmp_path / "paper_status.json"
    calendar = tmp_path / "calendar.json"
    progress = tmp_path / "progress.json"
    runtime.write_text(
        json.dumps({"ready_for_order_submission": True, "failed_reasons": []}),
        encoding="utf-8",
    )
    paper_status.write_text(
        json.dumps({"ready_for_session_collection": True}),
        encoding="utf-8",
    )
    calendar.write_text(
        json.dumps({"next_required_action": "collect_today_paper_session"}),
        encoding="utf-8",
    )
    progress.write_text(
        json.dumps(
            {
                "ready_for_live_review": False,
                "sessions_remaining": 19,
                "failed_reasons": [
                    "paper_sessions_remaining",
                    "insufficient_inferred_paper_sessions",
                    "net_pnl_not_positive",
                ],
            }
        ),
        encoding="utf-8",
    )

    report = build_paper_blocker_report(
        runtime_status_path=runtime,
        paper_simulate_status_path=paper_status,
        paper_calendar_path=calendar,
        paper_progress_path=progress,
    )

    assert report.ready_for_next_session is True
    assert report.ready_for_live_review is False
    assert report.next_required_action == "collect_today_paper_session"
    assert report.sessions_remaining == 19
    assert report.next_session_failed_reasons == ()
    assert "paper_sessions_remaining" in report.failed_reasons
    assert "net_pnl_not_positive" in report.failed_reasons


def test_paper_blocker_report_treats_runtime_kill_switch_as_blocker(tmp_path) -> None:
    runtime = tmp_path / "runtime.json"
    runtime.write_text(
        json.dumps(
            {
                "kill_switch": True,
                "kill_switch_file": "/tmp/futu-opend-execution.KILL",
                "ready_for_order_submission": False,
                "failed_reasons": [],
            }
        ),
        encoding="utf-8",
    )

    report = build_paper_blocker_report(runtime_status_path=runtime)

    assert report.ready_for_next_session is False
    assert (
        report.next_required_action
        == "clear_opend_kill_switch_then_resubmit_paper_simulate"
    )
    assert "opend_kill_switch_enabled" in report.failed_reasons
    assert report.blocker_details["opend_kill_switch"]["automation_allowed"] is False


def test_paper_blocker_report_keeps_profitability_mismatch_completion_only(
    tmp_path,
) -> None:
    runtime = tmp_path / "runtime.json"
    paper_status = tmp_path / "paper_status.json"
    calendar = tmp_path / "calendar.json"
    progress = tmp_path / "progress.json"
    runtime.write_text(
        json.dumps({"ready_for_order_submission": True, "failed_reasons": []}),
        encoding="utf-8",
    )
    paper_status.write_text(
        json.dumps({"ready_for_session_collection": True}),
        encoding="utf-8",
    )
    calendar.write_text(
        json.dumps({"next_required_action": "collect_today_paper_session"}),
        encoding="utf-8",
    )
    progress.write_text(
        json.dumps(
            {
                "ready_for_live_review": False,
                "sessions_remaining": 0,
                "failed_reasons": [
                    "profitability_session_count_mismatch",
                    "profitability_session_dates_mismatch",
                    "profitability_execution_log_rows_mismatch",
                    "profitability_broker_report_rows_mismatch",
                ],
            }
        ),
        encoding="utf-8",
    )

    report = build_paper_blocker_report(
        runtime_status_path=runtime,
        paper_simulate_status_path=paper_status,
        paper_calendar_path=calendar,
        paper_progress_path=progress,
    )

    assert report.ready_for_next_session is True
    assert report.ready_for_live_review is False
    assert report.next_required_action == "collect_today_paper_session"
    assert report.next_session_failed_reasons == ()
    assert "profitability_session_count_mismatch" in report.failed_reasons


def test_paper_blocker_report_marks_stale_paper_simulate_status(tmp_path) -> None:
    runtime = tmp_path / "runtime.json"
    paper_status = tmp_path / "paper_status.json"
    runtime.write_text(
        json.dumps(
            {
                "ready_for_order_submission": False,
                "failed_reasons": ["opend_kill_switch_enabled"],
            }
        ),
        encoding="utf-8",
    )
    paper_status.write_text(
        json.dumps(
            {
                "ready_for_session_collection": False,
                "next_required_action": "resubmit_paper_simulate_tickets",
                "failed_reasons": ["missing_submitted_responses"],
            }
        ),
        encoding="utf-8",
    )
    os.utime(paper_status, (1000, 1000))
    os.utime(runtime, (2000, 2000))

    report = build_paper_blocker_report(
        runtime_status_path=runtime,
        paper_simulate_status_path=paper_status,
    )

    assert report.ready_for_next_session is False
    assert (
        report.next_required_action
        == "clear_opend_kill_switch_then_resubmit_paper_simulate"
    )
    assert "opend_kill_switch_enabled" in report.failed_reasons
    assert "stale_paper_simulate_status" in report.failed_reasons
    assert "missing_submitted_responses" not in report.failed_reasons
