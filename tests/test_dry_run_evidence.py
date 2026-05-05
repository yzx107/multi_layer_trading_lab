from __future__ import annotations

import json

from multi_layer_trading_lab.execution.dry_run_evidence import (
    build_dry_run_execution_evidence,
)
from multi_layer_trading_lab.execution.paper_evidence import (
    PaperEvidenceInput,
    build_paper_evidence,
)


def test_build_dry_run_execution_evidence_from_opend_tickets(tmp_path) -> None:
    tickets = tmp_path / "tickets.jsonl"
    execution_log = tmp_path / "execution.jsonl"
    broker_report = tmp_path / "broker.json"
    ticket = {
        "ticket_id": "paper-001",
        "real": False,
        "submit_real": False,
        "web_normal_order_payload": {
            "symbol": "HK.00001",
            "side": "BUY",
            "shares": 100,
            "limit_price": 8.0,
        },
        "risk": {"reference_price": 8.0},
    }
    tickets.write_text(json.dumps(ticket) + "\n", encoding="utf-8")

    result = build_dry_run_execution_evidence(
        tickets,
        execution_log_path=execution_log,
        broker_report_path=broker_report,
    )

    assert result.ready is True
    assert result.order_count == 1
    local = [json.loads(line) for line in execution_log.read_text(encoding="utf-8").splitlines()]
    broker = json.loads(broker_report.read_text(encoding="utf-8"))
    assert local[0]["order_id"] == "paper-001"
    assert local[0]["dry_run"] is True
    assert broker[0]["local_order_id"] == "paper-001"
    assert broker[0]["dry_run"] is True


def test_dry_run_evidence_can_exercise_paper_evidence_shape(tmp_path) -> None:
    tickets = tmp_path / "tickets.jsonl"
    execution_log = tmp_path / "execution.jsonl"
    broker_report = tmp_path / "broker.json"
    tickets.write_text(
        json.dumps(
            {
                "ticket_id": "paper-001",
                "real": False,
                "submit_real": False,
                "web_normal_order_payload": {
                    "symbol": "HK.00001",
                    "side": "BUY",
                    "shares": 100,
                    "limit_price": 8.0,
                },
                "risk": {"reference_price": 8.0},
            }
        )
        + "\n",
        encoding="utf-8",
    )
    build_dry_run_execution_evidence(
        tickets,
        execution_log_path=execution_log,
        broker_report_path=broker_report,
    )

    evidence = build_paper_evidence(
        PaperEvidenceInput(
            execution_log_path=execution_log,
            broker_report_path=broker_report,
            paper_sessions=1,
        )
    )

    assert evidence.ready is False
    assert evidence.execution_log_rows == 1
    assert evidence.broker_report_rows == 1
    assert "dry_run_execution_log_not_real_paper" in evidence.failed_reasons
    assert "dry_run_broker_report_not_real_paper" in evidence.failed_reasons
    assert evidence.audit is not None
    assert evidence.audit.evidence.reconciliation_clean is True
