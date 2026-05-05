from __future__ import annotations

import json

from multi_layer_trading_lab.execution.paper_simulate_status import (
    inspect_paper_simulate_responses,
    write_paper_simulate_status,
)


def test_paper_simulate_status_blocks_missing_response(tmp_path) -> None:
    status = inspect_paper_simulate_responses(tmp_path / "missing.jsonl")

    assert status.ready_for_session_collection is False
    assert "missing_opend_ticket_response" in status.failed_reasons


def test_paper_simulate_status_blocks_dry_run_response(tmp_path) -> None:
    response_path = tmp_path / "responses.jsonl"
    response_path.write_text(
        json.dumps(
            {
                "event": "mttl_opend_paper_ticket_response",
                "ticket_id": "paper-001",
                "dry_run": True,
                "paper": False,
                "response": {
                    "submitted": False,
                    "intent": {"quantity": 100, "limit_price": "8.0"},
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )

    status = inspect_paper_simulate_responses(response_path)

    assert status.ready_for_session_collection is False
    assert status.dry_run_rows == 1
    assert status.order_report_rows == 1
    assert status.next_required_action == "rerun_with_submit_paper_simulate"
    assert "missing_paper_simulate_responses" in status.failed_reasons
    assert "dry_run_response_rows_present" in status.failed_reasons


def test_paper_simulate_status_extracts_kill_switch_submit_errors(tmp_path) -> None:
    response_path = tmp_path / "responses.jsonl"
    response_path.write_text(
        json.dumps(
            {
                "event": "mttl_opend_paper_ticket_response",
                "ticket_id": "paper-001",
                "dry_run": False,
                "paper": True,
                "response": {
                    "ok": False,
                    "submitted": False,
                    "error": "HTTPError:400:ExecutionValidationError:Kill switch enabled",
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )

    status = inspect_paper_simulate_responses(response_path)

    assert status.ready_for_session_collection is False
    assert status.paper_rows == 1
    assert status.submitted_rows == 0
    assert status.response_error_rows == 1
    assert status.response_error_types == {"HTTPError:400:ExecutionValidationError": 1}
    assert status.blocked_by_kill_switch is True
    assert (
        status.next_required_action
        == "clear_opend_kill_switch_then_resubmit_paper_simulate"
    )
    assert "paper_simulate_submit_errors_present" in status.failed_reasons
    assert "opend_kill_switch_enabled" in status.failed_reasons


def test_paper_simulate_status_accepts_submitted_paper_timeline(tmp_path) -> None:
    response_path = tmp_path / "responses.jsonl"
    response_path.write_text(
        json.dumps(
            {
                "event": "mttl_opend_paper_ticket_response",
                "ticket_id": "paper-001",
                "dry_run": False,
                "paper": True,
                "response": {
                    "submitted": True,
                    "timeline": [
                        [
                            {
                                "order_id": "futu-1",
                                "order_status": "SUBMITTED",
                                "dealt_qty": 0,
                            }
                        ]
                    ],
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )

    status = write_paper_simulate_status(
        response_path=response_path,
        output_path=tmp_path / "status.json",
    )

    assert status.ready_for_session_collection is True
    assert status.paper_rows == 1
    assert status.submitted_rows == 1
    assert status.order_report_rows == 1
    assert status.next_required_action == "collect_session_evidence"
    payload = json.loads((tmp_path / "status.json").read_text(encoding="utf-8"))
    assert payload["ready_for_session_collection"] is True
    assert payload["next_required_action"] == "collect_session_evidence"
