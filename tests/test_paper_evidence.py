import json

from multi_layer_trading_lab.execution.paper_evidence import (
    PaperEvidenceInput,
    build_paper_evidence,
)


def test_paper_evidence_blocks_missing_files(tmp_path):
    result = build_paper_evidence(
        PaperEvidenceInput(
            execution_log_path=tmp_path / "missing.jsonl",
            broker_report_path=tmp_path / "missing.json",
            paper_sessions=20,
        )
    )

    assert not result.ready
    assert "missing_execution_log" in result.failed_reasons
    assert "missing_broker_report" in result.failed_reasons


def test_paper_evidence_builds_audit_from_execution_and_broker_reports(tmp_path):
    execution_log = tmp_path / "execution.jsonl"
    broker_report = tmp_path / "futu.json"
    execution_log.write_text(
        json.dumps(
            {
                "order_id": "ord-1",
                "status": "filled",
                "quantity": 100,
                "fill_price": 10.0,
                "slippage": 0.01,
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
                    "order_status": "FILLED_ALL",
                    "dealt_qty": 100,
                    "dealt_avg_price": 10.0,
                }
            ]
        ),
        encoding="utf-8",
    )

    result = build_paper_evidence(
        PaperEvidenceInput(
            execution_log_path=execution_log,
            broker_report_path=broker_report,
            paper_sessions=20,
            manual_live_enable=True,
        )
    )

    assert result.ready
    assert result.execution_log_rows == 1
    assert result.broker_report_rows == 1
    assert result.audit is not None
    assert result.audit.evidence.reconciliation_clean
