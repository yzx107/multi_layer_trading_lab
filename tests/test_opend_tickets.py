from __future__ import annotations

import json
from io import BytesIO
from urllib.error import HTTPError

from multi_layer_trading_lab.execution.opend_tickets import (
    export_opend_paper_tickets,
    fetch_opend_account_status,
    fetch_opend_quote_snapshot,
    submit_opend_paper_tickets,
)


class FakeResponse:
    def __init__(self, payload: dict[str, object]) -> None:
        self._payload = payload

    def __enter__(self) -> FakeResponse:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def read(self) -> bytes:
        return BytesIO(json.dumps(self._payload).encode("utf-8")).read()


def _paper_plan() -> dict[str, object]:
    return {
        "session_id": "paper_001",
        "ready_for_paper": True,
        "orders": [
            {
                "strategy_id": "external_factor::factor_a",
                "factor_name": "factor_a",
                "side": "buy",
                "target_notional": 25_000.0,
                "max_single_name_notional": 80_000.0,
                "status": "planned_not_submitted",
            }
        ],
    }


def test_export_opend_paper_tickets_blocks_without_symbol_or_price(tmp_path) -> None:
    result = export_opend_paper_tickets(
        _paper_plan(),
        tmp_path / "tickets.jsonl",
        symbol=None,
        reference_price=None,
    )

    assert result.ready is False
    assert "missing_paper_ticket_symbol" in result.failed_reasons
    assert "missing_paper_ticket_reference_price" in result.failed_reasons
    assert not (tmp_path / "tickets.jsonl").exists()


def test_export_opend_paper_tickets_writes_dry_run_payload(tmp_path) -> None:
    output = tmp_path / "tickets.jsonl"
    result = export_opend_paper_tickets(
        _paper_plan(),
        output,
        symbol="00700.HK",
        reference_price=100.0,
        lot_size=100,
    )

    assert result.ready is True
    rows = [json.loads(line) for line in output.read_text(encoding="utf-8").splitlines()]
    assert len(rows) == 1
    payload = rows[0]["web_normal_order_payload"]
    assert payload["symbol"] == "HK.00700"
    assert payload["side"] == "BUY"
    assert payload["quantity_mode"] == "SHARES"
    assert payload["shares"] == 200
    assert payload["remark"] == "paper_001-001"
    assert payload["real"] is False
    assert rows[0]["dry_run"] is True
    assert rows[0]["submit_real"] is False


def test_export_opend_paper_tickets_can_use_quote_snapshot(tmp_path) -> None:
    quote_snapshot = {
        "quote": {
            "symbol": "HK.00001",
            "lot_size": 500,
            "best_bid": "7.99",
            "best_ask": "8.01",
            "last_price": "8.00",
        }
    }
    output = tmp_path / "tickets.jsonl"

    result = export_opend_paper_tickets(
        _paper_plan(),
        output,
        symbol=None,
        reference_price=None,
        quote_snapshot=quote_snapshot,
    )

    assert result.ready is True
    ticket = json.loads(output.read_text(encoding="utf-8").splitlines()[0])
    payload = ticket["web_normal_order_payload"]
    assert payload["symbol"] == "HK.00001"
    assert payload["limit_price"] == 8.0
    assert payload["shares"] == 3000
    assert ticket["risk"]["lot_size"] == 500


def test_export_opend_paper_tickets_blocks_invalid_quote_snapshot(tmp_path) -> None:
    result = export_opend_paper_tickets(
        _paper_plan(),
        tmp_path / "tickets.jsonl",
        symbol=None,
        reference_price=None,
        quote_snapshot={"quote": {"symbol": "HK.00001", "lot_size": 100}},
    )

    assert result.ready is False
    assert "quote_snapshot_missing_reference_price" in result.failed_reasons


def test_export_opend_paper_tickets_blocks_non_executable_plan_order(tmp_path) -> None:
    plan = _paper_plan()
    plan["orders"][0]["status"] = "blocked_below_one_lot"

    result = export_opend_paper_tickets(
        plan,
        tmp_path / "tickets.jsonl",
        symbol="00700.HK",
        reference_price=100.0,
        lot_size=100,
    )

    assert result.ready is False
    assert "paper_plan_order_not_executable:factor_a" in result.failed_reasons


def test_fetch_opend_quote_snapshot_reads_and_validates_quote_payload() -> None:
    calls = []

    def fake_urlopen(url: str, timeout: float):
        calls.append((url, timeout))
        return FakeResponse(
            {
                "quote": {
                    "symbol": "HK.00700",
                    "lot_size": 100,
                    "last_price": "320.0",
                }
            }
        )

    snapshot = fetch_opend_quote_snapshot(
        base_url="http://127.0.0.1:8766/",
        symbol="00700",
        timeout_seconds=3.0,
        urlopen_fn=fake_urlopen,
    )

    assert snapshot["quote"]["symbol"] == "HK.00700"
    assert calls == [("http://127.0.0.1:8766/api/quote?symbol=HK.00700", 3.0)]


def test_fetch_opend_account_status_detects_hk_stock_sim_account() -> None:
    def fake_urlopen(url: str, timeout: float):
        assert url == "http://127.0.0.1:8766/api/accounts"
        assert timeout == 3.0
        return FakeResponse(
            {
                "configured_acc_id": 281756479117805085,
                "accounts": [
                    {
                        "acc_id": 281756479117805085,
                        "trd_env": "REAL",
                        "sim_acc_type": "N/A",
                        "trdmarket_auth": ["HK"],
                    },
                    {
                        "acc_id": 15091974,
                        "trd_env": "SIMULATE",
                        "sim_acc_type": "STOCK",
                        "trdmarket_auth": ["HK"],
                    },
                ],
            }
        )

    status = fetch_opend_account_status(
        base_url="http://127.0.0.1:8766",
        timeout_seconds=3.0,
        urlopen_fn=fake_urlopen,
    )

    assert status["ready_for_paper_simulate"] is True
    assert status["simulate_account_count"] == 1
    assert status["hk_stock_simulate_account_count"] == 1
    assert status["failed_reasons"] == []
    assert status["configured_acc_id"] == "***5085"
    assert status["accounts"][0]["acc_id"] == "***5085"
    assert status["accounts"][1]["acc_id"] == "***1974"


def test_fetch_opend_account_status_blocks_without_hk_stock_sim_account() -> None:
    def fake_urlopen(url: str, timeout: float):
        return FakeResponse(
            {
                "accounts": [
                    {
                        "acc_id": 1,
                        "trd_env": "SIMULATE",
                        "sim_acc_type": "OPTION",
                        "trdmarket_auth": ["HK"],
                    }
                ]
            }
        )

    status = fetch_opend_account_status(
        base_url="http://127.0.0.1:8766",
        urlopen_fn=fake_urlopen,
    )

    assert status["ready_for_paper_simulate"] is False
    assert status["failed_reasons"] == ["missing_hk_stock_simulate_account"]
    assert status["accounts"][0]["acc_id"] == "***"


def test_submit_opend_paper_tickets_posts_dry_run_payload(tmp_path) -> None:
    ticket_path = tmp_path / "tickets.jsonl"
    output_path = tmp_path / "responses.jsonl"
    export_opend_paper_tickets(
        _paper_plan(),
        ticket_path,
        symbol="00700.HK",
        reference_price=100.0,
        lot_size=100,
    )
    calls = []

    def fake_urlopen(request, timeout: float):
        calls.append((request.full_url, json.loads(request.data.decode("utf-8")), timeout))
        return FakeResponse({"submitted": False, "intent": {"quantity": 200}})

    result = submit_opend_paper_tickets(
        ticket_path,
        output_path,
        base_url="http://127.0.0.1:8766",
        timeout_seconds=3.0,
        urlopen_fn=fake_urlopen,
    )

    assert result.ready is True
    assert result.submitted_count == 1
    assert calls[0][0] == "http://127.0.0.1:8766/api/normal/order"
    assert calls[0][1]["real"] is False
    assert calls[0][1]["paper"] is False
    assert calls[0][1]["remark"] == "paper_001-001"
    rows = [json.loads(line) for line in output_path.read_text(encoding="utf-8").splitlines()]
    assert rows[0]["ticket_id"] == "paper_001-001"
    assert rows[0]["dry_run"] is True
    assert rows[0]["paper"] is False
    assert rows[0]["response"]["submitted"] is False


def test_submit_opend_paper_tickets_can_submit_simulate_payload(tmp_path) -> None:
    ticket_path = tmp_path / "tickets.jsonl"
    output_path = tmp_path / "responses.jsonl"
    export_opend_paper_tickets(
        _paper_plan(),
        ticket_path,
        symbol="00700.HK",
        reference_price=100.0,
        lot_size=100,
    )
    calls = []

    def fake_urlopen(request, timeout: float):
        calls.append((request.full_url, json.loads(request.data.decode("utf-8")), timeout))
        return FakeResponse(
            {
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
            }
        )

    result = submit_opend_paper_tickets(
        ticket_path,
        output_path,
        base_url="http://127.0.0.1:8766",
        submit_paper_simulate=True,
        urlopen_fn=fake_urlopen,
    )

    assert result.ready is True
    assert calls[0][1]["real"] is False
    assert calls[0][1]["paper"] is True
    rows = [json.loads(line) for line in output_path.read_text(encoding="utf-8").splitlines()]
    assert rows[0]["dry_run"] is False
    assert rows[0]["paper"] is True
    assert rows[0]["response"]["submitted"] is True


def test_submit_opend_paper_tickets_blocks_duplicate_response_by_default(
    tmp_path,
) -> None:
    ticket_path = tmp_path / "tickets.jsonl"
    output_path = tmp_path / "responses.jsonl"
    export_opend_paper_tickets(
        _paper_plan(),
        ticket_path,
        symbol="00700.HK",
        reference_price=100.0,
        lot_size=100,
    )
    output_path.write_text(
        json.dumps(
            {
                "event": "mttl_opend_paper_ticket_response",
                "ticket_id": "paper_001-001",
                "dry_run": False,
                "paper": True,
                "response": {"submitted": True},
            }
        )
        + "\n",
        encoding="utf-8",
    )
    calls = []

    def fake_urlopen(request, timeout: float):
        calls.append(request.full_url)
        return FakeResponse({"submitted": True})

    result = submit_opend_paper_tickets(
        ticket_path,
        output_path,
        base_url="http://127.0.0.1:8766",
        submit_paper_simulate=True,
        urlopen_fn=fake_urlopen,
    )

    assert result.ready is False
    assert result.submitted_count == 0
    assert calls == []
    assert result.failed_reasons == ("ticket_already_submitted:paper_001-001",)


def test_submit_opend_paper_tickets_can_allow_resubmit(tmp_path) -> None:
    ticket_path = tmp_path / "tickets.jsonl"
    output_path = tmp_path / "responses.jsonl"
    export_opend_paper_tickets(
        _paper_plan(),
        ticket_path,
        symbol="00700.HK",
        reference_price=100.0,
        lot_size=100,
    )
    output_path.write_text(
        json.dumps({"ticket_id": "paper_001-001", "response": {"submitted": True}})
        + "\n",
        encoding="utf-8",
    )
    calls = []

    def fake_urlopen(request, timeout: float):
        calls.append(request.full_url)
        return FakeResponse({"submitted": True})

    result = submit_opend_paper_tickets(
        ticket_path,
        output_path,
        base_url="http://127.0.0.1:8766",
        submit_paper_simulate=True,
        allow_resubmit=True,
        urlopen_fn=fake_urlopen,
    )

    assert result.ready is True
    assert calls == ["http://127.0.0.1:8766/api/normal/order"]


def test_submit_opend_paper_tickets_can_retry_failed_response_only(tmp_path) -> None:
    ticket_path = tmp_path / "tickets.jsonl"
    output_path = tmp_path / "responses.jsonl"
    export_opend_paper_tickets(
        _paper_plan(),
        ticket_path,
        symbol="00700.HK",
        reference_price=100.0,
        lot_size=100,
    )
    output_path.write_text(
        json.dumps(
            {
                "ticket_id": "paper_001-001",
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
    calls = []

    def fake_urlopen(request, timeout: float):
        calls.append(request.full_url)
        return FakeResponse({"submitted": True})

    result = submit_opend_paper_tickets(
        ticket_path,
        output_path,
        base_url="http://127.0.0.1:8766",
        submit_paper_simulate=True,
        allow_failed_resubmit=True,
        urlopen_fn=fake_urlopen,
    )

    assert result.ready is True
    assert calls == ["http://127.0.0.1:8766/api/normal/order"]


def test_submit_opend_paper_tickets_does_not_retry_success_with_failed_resubmit_flag(
    tmp_path,
) -> None:
    ticket_path = tmp_path / "tickets.jsonl"
    output_path = tmp_path / "responses.jsonl"
    export_opend_paper_tickets(
        _paper_plan(),
        ticket_path,
        symbol="00700.HK",
        reference_price=100.0,
        lot_size=100,
    )
    output_path.write_text(
        json.dumps({"ticket_id": "paper_001-001", "response": {"submitted": True}})
        + "\n",
        encoding="utf-8",
    )
    calls = []

    def fake_urlopen(request, timeout: float):
        calls.append(request.full_url)
        return FakeResponse({"submitted": True})

    result = submit_opend_paper_tickets(
        ticket_path,
        output_path,
        base_url="http://127.0.0.1:8766",
        submit_paper_simulate=True,
        allow_failed_resubmit=True,
        urlopen_fn=fake_urlopen,
    )

    assert result.ready is False
    assert calls == []
    assert result.failed_reasons == ("ticket_already_submitted:paper_001-001",)


def test_fetch_opend_runtime_status_blocks_kill_switch() -> None:
    from multi_layer_trading_lab.execution.opend_tickets import fetch_opend_runtime_status

    def fake_urlopen(endpoint: str, timeout: float):
        assert endpoint == "http://127.0.0.1:8766/api/health"
        return FakeResponse(
            {
                "ok": True,
                "status": "READY",
                "kill_switch": True,
                "kill_switch_file": "/tmp/futu-opend-execution.KILL",
            }
        )

    status = fetch_opend_runtime_status(
        base_url="http://127.0.0.1:8766",
        urlopen_fn=fake_urlopen,
    )

    assert status["ready_for_order_submission"] is False
    assert status["failed_reasons"] == ["opend_kill_switch_enabled"]


def test_submit_opend_paper_tickets_retries_transient_web_error(tmp_path) -> None:
    ticket_path = tmp_path / "tickets.jsonl"
    output_path = tmp_path / "responses.jsonl"
    export_opend_paper_tickets(
        _paper_plan(),
        ticket_path,
        symbol="00700.HK",
        reference_price=100.0,
        lot_size=100,
    )
    calls = []

    def flaky_urlopen(request, timeout: float):
        calls.append((request.full_url, timeout))
        if len(calls) == 1:
            raise TimeoutError("web ui still starting")
        return FakeResponse({"submitted": False, "dry_run": True, "ok": True})

    result = submit_opend_paper_tickets(
        ticket_path,
        output_path,
        base_url="http://127.0.0.1:8766",
        max_attempts=2,
        retry_delay_seconds=0.0,
        urlopen_fn=flaky_urlopen,
    )

    assert result.ready is True
    assert len(calls) == 2
    rows = [json.loads(line) for line in output_path.read_text(encoding="utf-8").splitlines()]
    assert rows[0]["response"]["dry_run"] is True


def test_submit_opend_paper_tickets_reports_web_error_body(tmp_path) -> None:
    ticket_path = tmp_path / "tickets.jsonl"
    output_path = tmp_path / "responses.jsonl"
    export_opend_paper_tickets(
        _paper_plan(),
        ticket_path,
        symbol="00700.HK",
        reference_price=100.0,
        lot_size=100,
    )

    def failing_urlopen(request, timeout: float):
        raise HTTPError(
            request.full_url,
            400,
            "Bad Request",
            {},
            BytesIO(
                json.dumps(
                    {
                        "ok": False,
                        "error_type": "BrokerResponseError",
                        "error": "place_order failed: account not available",
                    }
                ).encode("utf-8")
            ),
        )

    result = submit_opend_paper_tickets(
        ticket_path,
        output_path,
        base_url="http://127.0.0.1:8766",
        max_attempts=1,
        urlopen_fn=failing_urlopen,
    )

    assert result.ready is False
    assert result.failed_reasons == (
        "opend_ticket_submit_failed:paper_001-001:"
        "HTTPError:400:BrokerResponseError:place_order failed: account not available",
    )
    assert result.output_path == output_path
    assert result.submitted_count == 0
    assert result.response_count == 1
    rows = [json.loads(line) for line in output_path.read_text(encoding="utf-8").splitlines()]
    assert rows[0]["paper"] is False
    assert rows[0]["response"]["ok"] is False
    assert rows[0]["response"]["submitted"] is False
    assert (
        rows[0]["response"]["error"]
        == "HTTPError:400:BrokerResponseError:place_order failed: account not available"
    )


def test_submit_opend_paper_tickets_blocks_real_ticket(tmp_path) -> None:
    ticket_path = tmp_path / "tickets.jsonl"
    ticket_path.write_text(
        json.dumps(
            {
                "ticket_id": "paper-001",
                "dry_run": False,
                "real": True,
                "submit_real": True,
                "web_normal_order_payload": {"symbol": "HK.00700", "real": True},
            }
        )
        + "\n",
        encoding="utf-8",
    )

    result = submit_opend_paper_tickets(
        ticket_path,
        tmp_path / "responses.jsonl",
        base_url="http://127.0.0.1:8766",
    )

    assert result.ready is False
    assert "ticket_not_dry_run:paper-001" in result.failed_reasons
    assert "ticket_requests_real_submission:paper-001" in result.failed_reasons


def test_submit_opend_paper_tickets_blocks_real_ticket_in_simulate_mode(tmp_path) -> None:
    ticket_path = tmp_path / "tickets.jsonl"
    ticket_path.write_text(
        json.dumps(
            {
                "ticket_id": "paper-001",
                "dry_run": True,
                "real": True,
                "submit_real": True,
                "web_normal_order_payload": {"symbol": "HK.00700", "real": True},
            }
        )
        + "\n",
        encoding="utf-8",
    )

    result = submit_opend_paper_tickets(
        ticket_path,
        tmp_path / "responses.jsonl",
        base_url="http://127.0.0.1:8766",
        submit_paper_simulate=True,
    )

    assert result.ready is False
    assert "ticket_requests_real_submission:paper-001" in result.failed_reasons
