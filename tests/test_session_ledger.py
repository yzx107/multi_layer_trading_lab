from __future__ import annotations

import json

from multi_layer_trading_lab.execution.session_ledger import build_paper_session_ledger


def test_session_ledger_counts_only_broker_backed_session_dates(tmp_path) -> None:
    execution_log = tmp_path / "execution.jsonl"
    broker_report = tmp_path / "broker.json"
    execution_rows = [
        {"order_id": f"ord-{day}", "trade_date": f"2026-04-{day:02d}", "dry_run": False}
        for day in range(1, 21)
    ]
    broker_rows = [
        {"local_order_id": f"ord-{day}", "updated_time": f"2026-04-{day:02d} 10:00:00"}
        for day in range(1, 20)
    ]
    execution_log.write_text(
        "\n".join(json.dumps(row) for row in execution_rows) + "\n",
        encoding="utf-8",
    )
    broker_report.write_text(json.dumps(broker_rows), encoding="utf-8")

    ledger = build_paper_session_ledger(
        execution_log_path=execution_log,
        broker_report_path=broker_report,
    )

    assert ledger.inferred_session_count == 19
    assert ledger.session_dates[-1] == "2026-04-19"
    assert ledger.execution_session_dates[-1] == "2026-04-20"
    assert ledger.broker_session_dates[-1] == "2026-04-19"
    assert "execution_session_dates_missing_broker_report" in ledger.failed_reasons
    assert "insufficient_inferred_sessions" in ledger.failed_reasons


def test_session_ledger_blocks_broker_report_without_dates(tmp_path) -> None:
    execution_log = tmp_path / "execution.jsonl"
    broker_report = tmp_path / "broker.json"
    execution_log.write_text(
        json.dumps({"order_id": "ord-1", "trade_date": "2026-04-01", "dry_run": False})
        + "\n",
        encoding="utf-8",
    )
    broker_report.write_text(
        json.dumps([{"local_order_id": "ord-1", "order_status": "FILLED_ALL"}]),
        encoding="utf-8",
    )

    ledger = build_paper_session_ledger(
        execution_log_path=execution_log,
        broker_report_path=broker_report,
    )

    assert ledger.inferred_session_count == 0
    assert ledger.session_dates == ()
    assert "missing_broker_session_dates" in ledger.failed_reasons
    assert "missing_broker_backed_session_dates" in ledger.failed_reasons
