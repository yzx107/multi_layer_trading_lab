from __future__ import annotations

import json

from multi_layer_trading_lab.execution.reconciliation import (
    BrokerExecutionReport,
    load_execution_log,
    reconcile_execution_reports,
)


def test_reconciliation_passes_matching_local_and_broker_records() -> None:
    local = [
        {
            "order_id": "ord-1",
            "status": "filled",
            "quantity": 100,
            "fill_price": 320.1,
        }
    ]
    broker = [
        BrokerExecutionReport(
            order_id="ord-1",
            status="filled",
            filled_quantity=100,
            fill_price=320.1,
        )
    ]

    result = reconcile_execution_reports(local, broker)

    assert result.clean is True
    assert result.matched_orders == 1


def test_reconciliation_detects_status_quantity_and_price_breaks() -> None:
    local = [
        {
            "order_id": "ord-1",
            "status": "filled",
            "quantity": 100,
            "fill_price": 320.1,
        }
    ]
    broker = [
        BrokerExecutionReport(
            order_id="ord-1",
            status="cancelled",
            filled_quantity=80,
            fill_price=321.0,
        )
    ]

    result = reconcile_execution_reports(local, broker)

    assert result.clean is False
    assert {item.reason for item in result.breaks} == {
        "status_mismatch",
        "quantity_mismatch",
        "fill_price_mismatch",
    }


def test_load_execution_log_reads_jsonl_records(tmp_path) -> None:
    path = tmp_path / "execution_log.jsonl"
    path.write_text(json.dumps({"order_id": "ord-1"}) + "\n", encoding="utf-8")

    assert load_execution_log(path) == [{"order_id": "ord-1"}]
