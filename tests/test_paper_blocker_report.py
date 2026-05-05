from __future__ import annotations

import json

from multi_layer_trading_lab.execution.paper_blocker_report import build_paper_blocker_report


def test_paper_blocker_report_aggregates_runtime_and_session_blockers(tmp_path) -> None:
    runtime = tmp_path / "runtime.json"
    paper_status = tmp_path / "paper_status.json"
    calendar = tmp_path / "calendar.json"
    progress = tmp_path / "progress.json"
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
    assert (
        report.next_required_action
        == "clear_opend_kill_switch_then_resubmit_paper_simulate"
    )
    assert report.sessions_remaining == 19
    assert "opend_kill_switch_enabled" in report.failed_reasons
    assert "missing_submitted_responses" in report.failed_reasons
    assert "paper_sessions_remaining" in report.failed_reasons
