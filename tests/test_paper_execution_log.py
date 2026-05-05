from __future__ import annotations

import json

from multi_layer_trading_lab.execution.paper_evidence import (
    PaperEvidenceInput,
    build_paper_evidence,
)
from multi_layer_trading_lab.execution.paper_execution_log import (
    build_paper_execution_log_from_futu_report,
)


def test_build_paper_execution_log_from_futu_report(tmp_path) -> None:
    tickets = tmp_path / "tickets.jsonl"
    broker_report = tmp_path / "futu.json"
    execution_log = tmp_path / "execution.jsonl"
    tickets.write_text(
        json.dumps(
            {
                "ticket_id": "paper-001",
                "web_normal_order_payload": {
                    "symbol": "HK.00001",
                    "side": "BUY",
                    "shares": 100,
                    "limit_price": 8.0,
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )
    broker_report.write_text(
        json.dumps(
            [
                {
                    "local_order_id": "paper-001",
                    "order_id": "futu-1",
                    "order_status": "FILLED_ALL",
                    "dealt_qty": 100,
                    "dealt_avg_price": 8.0,
                }
            ]
        ),
        encoding="utf-8",
    )

    result = build_paper_execution_log_from_futu_report(
        tickets,
        broker_report,
        execution_log_path=execution_log,
    )

    assert result.ready is True
    rows = [json.loads(line) for line in execution_log.read_text(encoding="utf-8").splitlines()]
    assert rows[0]["order_id"] == "paper-001"
    assert rows[0]["broker_order_id"] == "futu-1"
    assert rows[0]["status"] == "filled"
    assert rows[0]["dry_run"] is False


def test_real_paper_execution_log_can_enter_paper_evidence(tmp_path) -> None:
    tickets = tmp_path / "tickets.jsonl"
    broker_report = tmp_path / "futu.json"
    execution_log = tmp_path / "execution.jsonl"
    tickets.write_text(
        json.dumps(
            {
                "ticket_id": "paper-001",
                "web_normal_order_payload": {
                    "symbol": "HK.00001",
                    "side": "BUY",
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )
    broker_report.write_text(
        json.dumps(
            [
                {
                    "local_order_id": "paper-001",
                    "order_id": "futu-1",
                    "order_status": "FILLED_ALL",
                    "dealt_qty": 100,
                    "dealt_avg_price": 8.0,
                }
            ]
        ),
        encoding="utf-8",
    )
    build_paper_execution_log_from_futu_report(
        tickets,
        broker_report,
        execution_log_path=execution_log,
    )

    evidence = build_paper_evidence(
        PaperEvidenceInput(
            execution_log_path=execution_log,
            broker_report_path=broker_report,
            paper_sessions=20,
            manual_live_enable=True,
        )
    )

    assert evidence.ready is True
    assert "dry_run_execution_log_not_real_paper" not in evidence.failed_reasons
    assert "dry_run_broker_report_not_real_paper" not in evidence.failed_reasons


def test_build_paper_execution_log_blocks_dry_run_broker_report(tmp_path) -> None:
    tickets = tmp_path / "tickets.jsonl"
    broker_report = tmp_path / "futu.json"
    execution_log = tmp_path / "execution.jsonl"
    tickets.write_text(
        json.dumps(
            {
                "ticket_id": "paper-001",
                "web_normal_order_payload": {"symbol": "HK.00001", "side": "BUY"},
            }
        )
        + "\n",
        encoding="utf-8",
    )
    broker_report.write_text(
        json.dumps(
            [
                {
                    "local_order_id": "paper-001",
                    "order_id": "dry-paper-001",
                    "order_status": "FILLED_ALL",
                    "dealt_qty": 100,
                    "dealt_avg_price": 8.0,
                    "dry_run": True,
                }
            ]
        ),
        encoding="utf-8",
    )

    result = build_paper_execution_log_from_futu_report(
        tickets,
        broker_report,
        execution_log_path=execution_log,
    )

    assert result.ready is False
    assert "dry_run_broker_report_not_real_paper" in result.failed_reasons
    assert not execution_log.exists()
