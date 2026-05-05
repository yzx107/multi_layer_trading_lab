from __future__ import annotations

import json

from multi_layer_trading_lab.execution.paper_evidence_bundle import (
    combine_paper_evidence_files,
)


def test_combine_paper_evidence_files_writes_sorted_outputs(tmp_path) -> None:
    execution_a = tmp_path / "execution_a.jsonl"
    execution_b = tmp_path / "execution_b.jsonl"
    broker_a = tmp_path / "broker_a.json"
    broker_b = tmp_path / "broker_b.json"
    output_execution = tmp_path / "combined_execution.jsonl"
    output_broker = tmp_path / "combined_broker.json"

    execution_b.write_text(
        json.dumps(
            {
                "order_id": "ord-2",
                "trade_date": "2026-04-02",
                "dry_run": False,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    execution_a.write_text(
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
    broker_b.write_text(
        json.dumps(
            [
                {
                    "local_order_id": "ord-2",
                    "order_status": "FILLED_ALL",
                    "updated_time": "2026-04-02 10:00:00",
                }
            ]
        ),
        encoding="utf-8",
    )
    broker_a.write_text(
        json.dumps(
            [
                {
                    "local_order_id": "ord-1",
                    "order_status": "FILLED_ALL",
                    "updated_time": "2026-04-01 10:00:00",
                }
            ]
        ),
        encoding="utf-8",
    )

    result = combine_paper_evidence_files(
        execution_log_paths=(execution_b, execution_a),
        broker_report_paths=(broker_b, broker_a),
        output_execution_log_path=output_execution,
        output_broker_report_path=output_broker,
    )

    assert result.ready is True
    assert result.execution_log_rows == 2
    assert result.broker_report_rows == 2
    execution_rows = [
        json.loads(line) for line in output_execution.read_text(encoding="utf-8").splitlines()
    ]
    broker_rows = json.loads(output_broker.read_text(encoding="utf-8"))
    assert [row["order_id"] for row in execution_rows] == ["ord-1", "ord-2"]
    assert [row["local_order_id"] for row in broker_rows] == ["ord-1", "ord-2"]


def test_combine_paper_evidence_files_blocks_dry_run_and_duplicates(tmp_path) -> None:
    execution = tmp_path / "execution.jsonl"
    broker = tmp_path / "broker.json"
    execution.write_text(
        "\n".join(
            [
                json.dumps({"order_id": "ord-1", "dry_run": False}),
                json.dumps({"order_id": "ord-1", "dry_run": True}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    broker.write_text(
        json.dumps(
            [
                {"local_order_id": "ord-1", "dry_run": False},
                {"local_order_id": "ord-1", "dry_run": True},
            ]
        ),
        encoding="utf-8",
    )

    result = combine_paper_evidence_files(
        execution_log_paths=(execution,),
        broker_report_paths=(broker,),
        output_execution_log_path=tmp_path / "out.jsonl",
        output_broker_report_path=tmp_path / "out.json",
    )

    assert result.ready is False
    assert "dry_run_execution_log_rows_present" in result.failed_reasons
    assert "dry_run_broker_report_rows_present" in result.failed_reasons
    assert "duplicate_execution_order_id:ord-1" in result.failed_reasons
    assert "duplicate_broker_order_id:ord-1" in result.failed_reasons


def test_combine_paper_evidence_files_blocks_unmatched_order_ids(tmp_path) -> None:
    execution = tmp_path / "execution.jsonl"
    broker = tmp_path / "broker.json"
    execution.write_text(
        json.dumps({"order_id": "local-only", "trade_date": "2026-04-01", "dry_run": False})
        + "\n",
        encoding="utf-8",
    )
    broker.write_text(
        json.dumps(
            [
                {
                    "local_order_id": "broker-only",
                    "updated_time": "2026-04-01 10:00:00",
                    "dry_run": False,
                }
            ]
        ),
        encoding="utf-8",
    )

    result = combine_paper_evidence_files(
        execution_log_paths=(execution,),
        broker_report_paths=(broker,),
        output_execution_log_path=tmp_path / "out.jsonl",
        output_broker_report_path=tmp_path / "out.json",
    )

    assert result.ready is False
    assert "execution_order_missing_broker_report:local-only" in result.failed_reasons
    assert "broker_order_missing_execution_log:broker-only" in result.failed_reasons
