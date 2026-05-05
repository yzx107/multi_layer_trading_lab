from __future__ import annotations

import json

from multi_layer_trading_lab.execution.paper_progress import (
    build_paper_progress,
    build_paper_session_calendar,
)


def test_paper_progress_reports_remaining_sessions_and_pnl(tmp_path) -> None:
    execution_log = tmp_path / "execution.jsonl"
    broker_report = tmp_path / "broker.json"
    profitability = tmp_path / "profitability.json"
    execution_log.write_text(
        json.dumps(
            {
                "order_id": "ord-1",
                "trade_date": "2026-04-01",
                "dry_run": False,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    broker_report.write_text(
        json.dumps(
            [
                {
                    "local_order_id": "ord-1",
                    "updated_time": "2026-04-01 10:00:00",
                }
            ]
        ),
        encoding="utf-8",
    )
    profitability.write_text(
        json.dumps(
            {
                "ready": False,
                "paper_sessions": 1,
                "inferred_session_count": 1,
                "session_dates": ["2026-04-01"],
                "execution_log_rows": 1,
                "broker_report_rows": 1,
                "net_pnl": -25.0,
                "max_drawdown": -100.0,
                "cash_drawdown": -800.0,
                "reconciled": True,
                "failed_reasons": ["net_pnl_not_positive"],
            }
        ),
        encoding="utf-8",
    )

    progress = build_paper_progress(
        execution_log_path=execution_log,
        broker_report_path=broker_report,
        profitability_evidence_path=profitability,
        target_sessions=20,
    )

    assert progress.ready_for_live_review is False
    assert progress.inferred_session_count == 1
    assert progress.sessions_remaining == 19
    assert progress.net_pnl == -25.0
    assert progress.max_drawdown == -100.0
    assert progress.cash_drawdown == -800.0
    assert progress.reconciled is True
    assert "paper_sessions_remaining" in progress.failed_reasons
    assert "net_pnl_not_positive" in progress.failed_reasons


def test_paper_progress_can_be_ready_after_twenty_profitable_sessions(tmp_path) -> None:
    execution_log = tmp_path / "execution.jsonl"
    broker_report = tmp_path / "broker.json"
    profitability = tmp_path / "profitability.json"
    execution_rows = []
    broker_rows = []
    for day in range(1, 21):
        order_id = f"ord-{day}"
        trade_date = f"2026-04-{day:02d}"
        execution_rows.append(
            {
                "order_id": order_id,
                "trade_date": trade_date,
                "dry_run": False,
            }
        )
        broker_rows.append(
            {
                "local_order_id": order_id,
                "updated_time": f"{trade_date} 10:00:00",
            }
        )
    execution_log.write_text(
        "\n".join(json.dumps(row) for row in execution_rows) + "\n",
        encoding="utf-8",
    )
    broker_report.write_text(json.dumps(broker_rows), encoding="utf-8")
    profitability.write_text(
        json.dumps(
            {
                "ready": True,
                "paper_sessions": 20,
                "inferred_session_count": 20,
                "session_dates": [f"2026-04-{day:02d}" for day in range(1, 21)],
                "execution_log_rows": 20,
                "broker_report_rows": 20,
                "net_pnl": 100.0,
                "max_drawdown": -50.0,
                "reconciled": True,
                "failed_reasons": [],
            }
        ),
        encoding="utf-8",
    )

    progress = build_paper_progress(
        execution_log_path=execution_log,
        broker_report_path=broker_report,
        profitability_evidence_path=profitability,
        target_sessions=20,
    )

    assert progress.ready_for_live_review is True
    assert progress.sessions_remaining == 0
    assert progress.failed_reasons == ()


def test_paper_progress_blocks_stale_profitability_session_count(tmp_path) -> None:
    execution_log = tmp_path / "execution.jsonl"
    broker_report = tmp_path / "broker.json"
    profitability = tmp_path / "profitability.json"
    execution_rows = []
    broker_rows = []
    for day in range(1, 21):
        trade_date = f"2026-04-{day:02d}"
        execution_rows.append(
            {"order_id": f"ord-{day}", "trade_date": trade_date, "dry_run": False}
        )
        broker_rows.append(
            {"local_order_id": f"ord-{day}", "updated_time": f"{trade_date} 10:00:00"}
        )
    execution_log.write_text(
        "\n".join(json.dumps(row) for row in execution_rows) + "\n",
        encoding="utf-8",
    )
    broker_report.write_text(json.dumps(broker_rows), encoding="utf-8")
    profitability.write_text(
        json.dumps(
            {
                "ready": True,
                "paper_sessions": 19,
                "inferred_session_count": 19,
                "session_dates": [f"2026-04-{day:02d}" for day in range(1, 20)],
                "execution_log_rows": 19,
                "broker_report_rows": 19,
                "net_pnl": 100.0,
                "max_drawdown": -50.0,
                "reconciled": True,
                "failed_reasons": [],
            }
        ),
        encoding="utf-8",
    )

    progress = build_paper_progress(
        execution_log_path=execution_log,
        broker_report_path=broker_report,
        profitability_evidence_path=profitability,
        target_sessions=20,
    )

    assert progress.ready_for_live_review is False
    assert progress.sessions_remaining == 0
    assert "profitability_session_count_mismatch" in progress.failed_reasons
    assert "profitability_session_dates_mismatch" in progress.failed_reasons
    assert "profitability_execution_log_rows_mismatch" in progress.failed_reasons
    assert "profitability_broker_report_rows_mismatch" in progress.failed_reasons


def test_paper_session_calendar_waits_after_today_session(tmp_path) -> None:
    execution_log = tmp_path / "execution.jsonl"
    broker_report = tmp_path / "broker.json"
    execution_log.write_text(
        json.dumps({"order_id": "ord-1", "trade_date": "2026-05-05", "dry_run": False})
        + "\n",
        encoding="utf-8",
    )
    broker_report.write_text(
        json.dumps([{"local_order_id": "ord-1", "updated_time": "2026-05-05 10:00:00"}]),
        encoding="utf-8",
    )

    calendar = build_paper_session_calendar(
        execution_log_path=execution_log,
        broker_report_path=broker_report,
        as_of_date="2026-05-05",
        target_sessions=20,
    )

    assert calendar.has_session_today is True
    assert calendar.last_session_date == "2026-05-05"
    assert calendar.sessions_remaining == 19
    assert calendar.next_required_action == "wait_next_trade_date"


def test_paper_session_calendar_collects_when_today_missing(tmp_path) -> None:
    execution_log = tmp_path / "execution.jsonl"
    broker_report = tmp_path / "broker.json"
    execution_log.write_text(
        json.dumps({"order_id": "ord-1", "trade_date": "2026-05-04", "dry_run": False})
        + "\n",
        encoding="utf-8",
    )
    broker_report.write_text(
        json.dumps([{"local_order_id": "ord-1", "updated_time": "2026-05-04 10:00:00"}]),
        encoding="utf-8",
    )

    calendar = build_paper_session_calendar(
        execution_log_path=execution_log,
        broker_report_path=broker_report,
        as_of_date="2026-05-05",
        target_sessions=20,
    )

    assert calendar.has_session_today is False
    assert calendar.last_session_date == "2026-05-04"
    assert calendar.next_required_action == "collect_today_paper_session"


def test_paper_session_calendar_completes_after_target_sessions(tmp_path) -> None:
    execution_log = tmp_path / "execution.jsonl"
    broker_report = tmp_path / "broker.json"
    execution_rows = []
    broker_rows = []
    for day in range(1, 21):
        trade_date = f"2026-04-{day:02d}"
        execution_rows.append(
            {"order_id": f"ord-{day}", "trade_date": trade_date, "dry_run": False}
        )
        broker_rows.append(
            {"local_order_id": f"ord-{day}", "updated_time": f"{trade_date} 10:00:00"}
        )
    execution_log.write_text(
        "\n".join(json.dumps(row) for row in execution_rows) + "\n",
        encoding="utf-8",
    )
    broker_report.write_text(json.dumps(broker_rows), encoding="utf-8")

    calendar = build_paper_session_calendar(
        execution_log_path=execution_log,
        broker_report_path=broker_report,
        as_of_date="2026-05-05",
        target_sessions=20,
    )

    assert calendar.sessions_remaining == 0
    assert calendar.next_required_action == "target_complete"
