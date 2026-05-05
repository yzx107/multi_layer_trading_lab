from __future__ import annotations

import json

from multi_layer_trading_lab.execution.profitability_evidence import (
    ProfitabilityEvidenceInput,
    build_mark_prices_from_opend_quote_snapshot,
    build_profitability_evidence,
)


def test_profitability_evidence_builds_positive_reconciled_result(tmp_path) -> None:
    execution_log = tmp_path / "execution.jsonl"
    broker_report = tmp_path / "futu.json"
    output = tmp_path / "profitability.json"
    execution_rows = []
    broker_rows = []
    for day in range(1, 21):
        order_id = f"buy-{day}" if day == 1 else f"sell-{day}"
        trade_date = f"2026-04-{day:02d}"
        execution_rows.append(
            {
                "order_id": order_id,
                "status": "filled",
                "symbol": "HK.00001",
                "side": "buy" if day == 1 else "sell",
                "quantity": 100 if day == 1 else 10 if day == 20 else 5,
                "fill_price": 8.0 if day == 1 else 8.5,
                "fees": 2.0 if day in {1, 20} else 0.0,
                "trade_date": trade_date,
                "dry_run": False,
            }
        )
        broker_rows.append(
                {
                    "local_order_id": order_id,
                    "order_status": "FILLED_ALL",
                    "dealt_qty": 100 if day == 1 else 10 if day == 20 else 5,
                    "dealt_avg_price": 8.0 if day == 1 else 8.5,
                    "updated_time": f"{trade_date} 10:00:00",
                }
        )
    execution_log.write_text(
        "\n".join(json.dumps(row) for row in execution_rows) + "\n",
        encoding="utf-8",
    )
    broker_report.write_text(json.dumps(broker_rows), encoding="utf-8")

    evidence = build_profitability_evidence(
        ProfitabilityEvidenceInput(
            execution_log_path=execution_log,
            broker_report_path=broker_report,
            output_path=output,
            paper_sessions=20,
        )
    )

    assert evidence["ready"] is True
    assert evidence["reconciled"] is True
    assert evidence["paper_sessions"] == 20
    assert evidence["requested_paper_sessions"] == 20
    assert evidence["inferred_session_count"] == 20
    assert evidence["net_pnl"] == 46.0
    attribution = evidence["symbol_attribution"]["HK.00001"]
    assert attribution["quantity"] == 0.0
    assert attribution["market_value"] == 0.0
    assert attribution["mark_price"] is None
    assert attribution["net_pnl"] == 46.0
    assert json.loads(output.read_text(encoding="utf-8"))["ready"] is True


def test_profitability_evidence_blocks_open_position_without_mark(tmp_path) -> None:
    execution_log = tmp_path / "execution.jsonl"
    broker_report = tmp_path / "futu.json"
    output = tmp_path / "profitability.json"
    execution_log.write_text(
        json.dumps(
            {
                "order_id": "buy-1",
                "status": "filled",
                "symbol": "HK.00001",
                "side": "buy",
                "quantity": 100,
                "fill_price": 8.0,
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
                    "local_order_id": "buy-1",
                    "order_status": "FILLED_ALL",
                    "dealt_qty": 100,
                    "dealt_avg_price": 8.0,
                }
            ]
        ),
        encoding="utf-8",
    )

    evidence = build_profitability_evidence(
        ProfitabilityEvidenceInput(
            execution_log_path=execution_log,
            broker_report_path=broker_report,
            output_path=output,
            paper_sessions=1,
        )
    )

    assert evidence["ready"] is False
    assert "insufficient_inferred_paper_sessions" in evidence["failed_reasons"]
    assert "missing_mark_price:HK.00001" in evidence["failed_reasons"]
    assert "net_pnl_not_positive" in evidence["failed_reasons"]


def test_profitability_evidence_uses_marked_equity_for_drawdown(tmp_path) -> None:
    execution_log = tmp_path / "execution.jsonl"
    broker_report = tmp_path / "futu.json"
    marks = tmp_path / "marks.json"
    output = tmp_path / "profitability.json"
    execution_log.write_text(
        json.dumps(
            {
                "order_id": "buy-1",
                "status": "filled",
                "symbol": "HK.00001",
                "side": "buy",
                "quantity": 500,
                "fill_price": 65.2,
                "trade_date": "2026-05-04",
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
                    "local_order_id": "buy-1",
                    "order_status": "FILLED_ALL",
                    "dealt_qty": 500,
                    "dealt_avg_price": 65.2,
                    "updated_time": "2026-05-04 14:50:00",
                }
            ]
        ),
        encoding="utf-8",
    )
    marks.write_text(json.dumps({"HK.00001": 65.15}), encoding="utf-8")

    evidence = build_profitability_evidence(
        ProfitabilityEvidenceInput(
            execution_log_path=execution_log,
            broker_report_path=broker_report,
            output_path=output,
            paper_sessions=1,
            mark_prices_path=marks,
            max_allowed_drawdown=10_000.0,
        )
    )

    assert evidence["ready"] is False
    assert evidence["cash_drawdown"] == -32600.0
    assert round(evidence["max_drawdown"], 6) == -25.0
    attribution = evidence["symbol_attribution"]["HK.00001"]
    assert attribution["quantity"] == 500.0
    assert attribution["avg_cost"] == 65.2
    assert attribution["latest_trade_price"] == 65.2
    assert attribution["mark_price"] == 65.15
    assert round(attribution["market_value"], 6) == 32575.0
    assert round(attribution["net_pnl"], 6) == -25.0
    assert "drawdown_breached" not in evidence["failed_reasons"]
    assert "net_pnl_not_positive" in evidence["failed_reasons"]


def test_profitability_evidence_blocks_hand_filled_session_count(tmp_path) -> None:
    execution_log = tmp_path / "execution.jsonl"
    broker_report = tmp_path / "futu.json"
    output = tmp_path / "profitability.json"
    execution_log.write_text(
        "\n".join(
            [
                json.dumps(
                    {
                        "order_id": "buy-1",
                        "status": "filled",
                        "symbol": "HK.00001",
                        "side": "buy",
                        "quantity": 100,
                        "fill_price": 8.0,
                        "trade_date": "2026-04-01",
                        "dry_run": False,
                    }
                ),
                json.dumps(
                    {
                        "order_id": "sell-1",
                        "status": "filled",
                        "symbol": "HK.00001",
                        "side": "sell",
                        "quantity": 100,
                        "fill_price": 8.5,
                        "trade_date": "2026-04-01",
                        "dry_run": False,
                    }
                ),
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    broker_report.write_text(
        json.dumps(
            [
                {
                    "local_order_id": "buy-1",
                    "order_status": "FILLED_ALL",
                    "dealt_qty": 100,
                    "dealt_avg_price": 8.0,
                    "updated_time": "2026-04-01 10:00:00",
                },
                {
                    "local_order_id": "sell-1",
                    "order_status": "FILLED_ALL",
                    "dealt_qty": 100,
                    "dealt_avg_price": 8.5,
                    "updated_time": "2026-04-01 10:05:00",
                },
            ]
        ),
        encoding="utf-8",
    )

    evidence = build_profitability_evidence(
        ProfitabilityEvidenceInput(
            execution_log_path=execution_log,
            broker_report_path=broker_report,
            output_path=output,
            paper_sessions=20,
        )
    )

    assert evidence["ready"] is False
    assert evidence["paper_sessions"] == 1
    assert evidence["requested_paper_sessions"] == 20
    assert evidence["inferred_session_count"] == 1
    assert "insufficient_inferred_paper_sessions" in evidence["failed_reasons"]
    assert "paper_sessions_exceed_inferred_sessions" in evidence["failed_reasons"]


def test_build_mark_prices_from_opend_quote_snapshot(tmp_path) -> None:
    quote = tmp_path / "quote.json"
    output = tmp_path / "marks.json"
    quote.write_text(
        json.dumps(
            {
                "quote": {
                    "symbol": "HK.00001",
                    "lot_size": 500,
                    "last_price": 65.05,
                }
            }
        ),
        encoding="utf-8",
    )

    marks = build_mark_prices_from_opend_quote_snapshot(quote, output)

    assert marks == {"HK.00001": 65.05}
    assert json.loads(output.read_text(encoding="utf-8")) == {"HK.00001": 65.05}
