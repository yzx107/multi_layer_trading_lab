from __future__ import annotations

from multi_layer_trading_lab.adapters.futu.reports import (
    extract_futu_order_report_rows_from_ticket_responses,
    extract_futu_order_report_rows_from_web_log,
    futu_order_report_to_execution_report,
    load_futu_order_report_rows,
)


def test_futu_order_report_maps_common_paper_fields() -> None:
    report = futu_order_report_to_execution_report(
        {
            "local_order_id": "ord-1",
            "order_id": "futu-123",
            "order_status": "FILLED_ALL",
            "dealt_qty": 100,
            "dealt_avg_price": 320.5,
        }
    )

    assert report.order_id == "ord-1"
    assert report.broker_order_id == "futu-123"
    assert report.status == "filled"
    assert report.filled_quantity == 100
    assert report.fill_price == 320.5


def test_load_futu_order_report_rows_reads_json_list(tmp_path) -> None:
    path = tmp_path / "futu.json"
    path.write_text('[{"order_id": "ord-1", "order_status": "FILLED_ALL"}]', encoding="utf-8")

    assert load_futu_order_report_rows(path) == [
        {"order_id": "ord-1", "order_status": "FILLED_ALL"}
    ]


def test_extract_futu_order_report_rows_from_web_log_uses_latest_timeline(tmp_path) -> None:
    path = tmp_path / "web.jsonl"
    path.write_text(
        "\n".join(
            [
                '{"event":"order_response","data":[{"order_id":"futu-1","order_status":"SUBMITTING","dealt_qty":0,"remark":"paper-001"}]}',
                '{"event":"order_query","data":[{"order_id":"futu-1","order_status":"FILLED_ALL","dealt_qty":100,"dealt_avg_price":8.0,"remark":"paper-001"}]}',
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    rows = extract_futu_order_report_rows_from_web_log(path)

    assert rows == [
        {
            "order_id": "futu-1",
            "order_status": "FILLED_ALL",
            "dealt_qty": 100,
            "dealt_avg_price": 8.0,
            "remark": "paper-001",
            "local_order_id": "paper-001",
        }
    ]


def test_extract_futu_order_report_rows_from_web_normal_response(tmp_path) -> None:
    path = tmp_path / "web.jsonl"
    path.write_text(
        '{"event":"web_normal_order_response","timeline":[[{"order_id":"futu-1","order_status":"FILLED_ALL","dealt_qty":100,"dealt_avg_price":8.0,"remark":"paper-001"}]]}\n',
        encoding="utf-8",
    )

    rows = extract_futu_order_report_rows_from_web_log(path)

    assert rows[0]["local_order_id"] == "paper-001"
    assert rows[0]["order_status"] == "FILLED_ALL"


def test_extract_futu_order_report_rows_from_ticket_responses_keeps_dry_run_flag(
    tmp_path,
) -> None:
    path = tmp_path / "responses.jsonl"
    path.write_text(
        '{"event":"mttl_opend_paper_ticket_response","ticket_id":"paper-001","dry_run":true,"request":{"symbol":"HK.00700","shares":100,"limit_price":8.0},"response":{"submitted":false,"intent":{"quantity":100,"limit_price":"8.0"}}}\n',
        encoding="utf-8",
    )

    rows = extract_futu_order_report_rows_from_ticket_responses(path)

    assert rows == [
        {
            "local_order_id": "paper-001",
            "order_id": "paper-001",
            "order_status": "DRY_RUN",
            "dealt_qty": 100,
            "dealt_avg_price": "8.0",
            "remark": "paper-001",
            "dry_run": True,
        }
    ]


def test_extract_futu_order_report_rows_from_ticket_response_timeline(tmp_path) -> None:
    path = tmp_path / "responses.jsonl"
    path.write_text(
        '{"event":"mttl_opend_paper_ticket_response","ticket_id":"paper-001","dry_run":false,"response":{"timeline":[[{"order_id":"futu-1","order_status":"FILLED_ALL","dealt_qty":100,"dealt_avg_price":8.0}]]}}\n',
        encoding="utf-8",
    )

    rows = extract_futu_order_report_rows_from_ticket_responses(path)

    assert rows[0]["local_order_id"] == "paper-001"
    assert rows[0]["remark"] == "paper-001"
    assert rows[0]["order_status"] == "FILLED_ALL"
