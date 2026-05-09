from __future__ import annotations

import base64
import json
from datetime import UTC, datetime, timedelta
from unittest.mock import patch
from zipfile import ZipFile

import polars as pl
from typer.testing import CliRunner

from multi_layer_trading_lab.adapters.ifind.token import IFindAccessTokenRefreshStatus
from multi_layer_trading_lab.cli import app


def _ifind_refresh_token(expiry: str) -> str:
    header = base64.urlsafe_b64encode(b'{"sign_time":"2026-05-03 14:32:46"}').decode().rstrip("=")
    payload = base64.urlsafe_b64encode(
        json.dumps(
            {
                "uid": "test-user",
                "user": {
                    "refreshTokenExpiredTime": expiry,
                    "userId": "test-user",
                },
            }
        ).encode()
    ).decode().rstrip("=")
    return f"{header}.{payload}.signature"


def test_risk_precheck_prints_personal_account_limits() -> None:
    result = CliRunner().invoke(app, ["risk-precheck", "--account-equity", "1000000"])

    assert result.exit_code == 0
    assert "account_equity=1000000.00" in result.output
    assert "max_single_name_notional=80000.00" in result.output
    assert "max_strategy_notional=200000.00" in result.output
    assert "max_daily_drawdown=10000.00" in result.output
    assert "default_kelly_scale=0.125" in result.output


def test_opend_precheck_reports_live_blockers() -> None:
    result = CliRunner().invoke(app, ["opend-precheck", "--mode", "live"])

    assert result.exit_code == 0
    assert "opend_ready=false" in result.output
    assert "live_requires_real_env" in result.output
    assert "manual_live_enable_missing" in result.output


def test_fetch_opend_runtime_status_cli_reports_kill_switch(tmp_path) -> None:
    output = tmp_path / "runtime.json"

    with patch("multi_layer_trading_lab.cli.read_opend_runtime_status") as mocked:
        mocked.return_value = {
            "ok": True,
            "kill_switch": True,
            "ready_for_order_submission": False,
            "failed_reasons": ["opend_kill_switch_enabled"],
        }
        result = CliRunner().invoke(
            app,
            [
                "fetch-opend-runtime-status",
                "--output-path",
                str(output),
            ],
        )

    assert result.exit_code == 0
    assert "ready_for_order_submission=false" in result.output
    assert "kill_switch=true" in result.output
    assert "opend_kill_switch_enabled" in result.output
    assert json.loads(output.read_text(encoding="utf-8"))["failed_reasons"] == [
        "opend_kill_switch_enabled"
    ]


def test_fetch_opend_runtime_status_cli_can_require_order_submission_ready(tmp_path) -> None:
    output = tmp_path / "runtime.json"

    with patch("multi_layer_trading_lab.cli.read_opend_runtime_status") as mocked:
        mocked.return_value = {
            "ok": True,
            "kill_switch": True,
            "ready_for_order_submission": False,
            "failed_reasons": ["opend_kill_switch_enabled"],
        }
        result = CliRunner().invoke(
            app,
            [
                "fetch-opend-runtime-status",
                "--output-path",
                str(output),
                "--require-order-submission-ready",
            ],
        )

    assert result.exit_code == 1
    assert "ready_for_order_submission=false" in result.output
    assert "status=blocked" in result.output
    assert "opend_runtime_not_ready_for_order_submission" in result.output


def test_submit_opend_paper_tickets_cli_blocks_duplicate_response(tmp_path) -> None:
    ticket_path = tmp_path / "tickets.jsonl"
    output_path = tmp_path / "responses.jsonl"
    ticket_path.write_text(
        json.dumps(
            {
                "ticket_id": "paper-001",
                "dry_run": True,
                "real": False,
                "submit_real": False,
                "web_normal_order_payload": {
                    "symbol": "HK.00001",
                    "side": "BUY",
                    "shares": 500,
                    "limit_price": 65.0,
                    "real": False,
                },
            }
        )
        + "\n",
        encoding="utf-8",
    )
    output_path.write_text(
        json.dumps({"ticket_id": "paper-001", "response": {"submitted": True}})
        + "\n",
        encoding="utf-8",
    )

    with patch("multi_layer_trading_lab.execution.opend_tickets.urlopen") as mocked:
        result = CliRunner().invoke(
            app,
            [
                "submit-opend-paper-tickets",
                "--ticket-path",
                str(ticket_path),
                "--output-path",
                str(output_path),
                "--submit-paper-simulate",
            ],
        )

    assert result.exit_code == 0
    assert "status=blocked" in result.output
    assert "ticket_already_submitted:paper-001" in result.output
    mocked.assert_not_called()


def test_submit_opend_paper_tickets_cli_forwards_failed_resubmit_flag(tmp_path) -> None:
    ticket_path = tmp_path / "tickets.jsonl"
    output_path = tmp_path / "responses.jsonl"

    class Result:
        failed_reasons: tuple[str, ...] = ()
        submitted_count = 1
        response_count = 1

        def __init__(self, path):
            self.output_path = path

    with patch("multi_layer_trading_lab.cli.post_opend_paper_tickets") as mocked:
        mocked.return_value = Result(output_path)
        result = CliRunner().invoke(
            app,
            [
                "submit-opend-paper-tickets",
                "--ticket-path",
                str(ticket_path),
                "--output-path",
                str(output_path),
                "--submit-paper-simulate",
                "--opend-runtime-status-path",
                "data/logs/runtime.json",
                "--allow-failed-resubmit",
            ],
        )

    assert result.exit_code == 0
    assert "status=ready" in result.output
    assert mocked.call_args.kwargs["allow_failed_resubmit"] is True
    assert str(mocked.call_args.kwargs["opend_runtime_status_path"]) == "data/logs/runtime.json"


def test_reconcile_futu_report_cli_reports_clean_match(tmp_path) -> None:
    execution_log = tmp_path / "execution_log.jsonl"
    futu_report = tmp_path / "futu.json"
    execution_log.write_text(
        json.dumps(
            {
                "order_id": "ord-1",
                "status": "filled",
                "quantity": 100,
                "fill_price": 320.5,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    futu_report.write_text(
        json.dumps(
            [
                {
                    "local_order_id": "ord-1",
                    "order_id": "futu-1",
                    "order_status": "FILLED_ALL",
                    "dealt_qty": 100,
                    "dealt_avg_price": 320.5,
                }
            ]
        ),
        encoding="utf-8",
    )

    result = CliRunner().invoke(
        app,
        ["reconcile-futu-report", str(execution_log), str(futu_report)],
    )

    assert result.exit_code == 0
    assert "reconciliation_clean=true" in result.output
    assert "matched_orders=1" in result.output


def test_paper_audit_cli_blocks_without_manual_live_enable(tmp_path) -> None:
    execution_log = tmp_path / "execution_log.jsonl"
    futu_report = tmp_path / "futu.json"
    execution_log.write_text(
        json.dumps(
            {
                "order_id": "ord-1",
                "status": "filled",
                "quantity": 100,
                "fill_price": 320.5,
                "slippage": 0.01,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    futu_report.write_text(
        json.dumps(
            [
                {
                    "local_order_id": "ord-1",
                    "order_id": "futu-1",
                    "order_status": "FILLED_ALL",
                    "dealt_qty": 100,
                    "dealt_avg_price": 320.5,
                }
            ]
        ),
        encoding="utf-8",
    )

    result = CliRunner().invoke(
        app,
        [
            "paper-audit",
            str(execution_log),
            str(futu_report),
            "--paper-sessions",
            "20",
        ],
    )

    assert result.exit_code == 0
    assert "paper_to_live_approved=false" in result.output
    assert "reconciliation_clean=true" in result.output
    assert "manual_live_enable_missing" in result.output


def test_data_source_precheck_reports_missing_credentials() -> None:
    result = CliRunner().invoke(
        app,
        [
            "data-source-precheck",
            "--tushare-token",
            "",
            "--ifind-username",
            "",
            "--ifind-password",
            "",
        ],
    )

    assert result.exit_code == 0
    assert "tushare_ready=false" in result.output
    assert "missing_tushare_token" in result.output
    assert "ifind_ready=false" in result.output
    assert "missing_ifind_password" in result.output


def test_fetch_tushare_to_lake_blocks_without_token_unless_stub_allowed(tmp_path) -> None:
    lake_root = tmp_path / "lake"
    blocked = CliRunner().invoke(
        app,
        ["fetch-tushare-to-lake", "--lake-root", str(lake_root), "--token", ""],
    )
    assert blocked.exit_code == 0
    assert "status=blocked" in blocked.output
    assert not (lake_root / "daily_bars" / "part-000.parquet").exists()

    allowed = CliRunner().invoke(
        app,
        ["fetch-tushare-to-lake", "--lake-root", str(lake_root), "--token", "", "--allow-stub"],
    )
    assert allowed.exit_code == 0
    assert "status=stub_adapter" in allowed.output
    assert "daily_features_rows=" in allowed.output
    assert (lake_root / "daily_bars" / "part-000.parquet").exists()
    assert (lake_root / "daily_features" / "part-000.parquet").exists()


def test_fetch_tushare_to_lake_blocks_real_token_until_real_adapter_exists(tmp_path) -> None:
    lake_root = tmp_path / "lake"
    result = CliRunner().invoke(
        app,
        ["fetch-tushare-to-lake", "--lake-root", str(lake_root), "--token", "token"],
    )

    assert result.exit_code == 0
    assert "status=blocked" in result.output
    assert "real_tushare_adapter_requires_use_real" in result.output
    assert not (lake_root / "daily_bars" / "part-000.parquet").exists()


def test_fetch_tushare_to_lake_blocks_cleanly_on_real_fetch_error(tmp_path, monkeypatch) -> None:
    lake_root = tmp_path / "lake"

    def raise_fetch_error(*args, **kwargs):
        del args, kwargs
        raise Exception("daily unavailable")

    monkeypatch.setattr(
        "multi_layer_trading_lab.adapters.tushare.client.TushareClient.fetch_daily_bars",
        raise_fetch_error,
    )

    result = CliRunner().invoke(
        app,
        [
            "fetch-tushare-to-lake",
            "--lake-root",
            str(lake_root),
            "--token",
            "token",
            "--symbols",
            "600519.SH",
            "--use-real",
        ],
    )

    assert result.exit_code == 0
    assert "status=blocked" in result.output
    assert "tushare_fetch_failed:Exception" in result.output
    assert not (lake_root / "security_master" / "part-000.parquet").exists()


def test_fetch_tushare_to_lake_derives_master_when_stock_basic_rate_limited(
    tmp_path,
    monkeypatch,
) -> None:
    lake_root = tmp_path / "lake"
    trade_date = datetime(2026, 4, 1).date()
    ingested_at = datetime(2026, 4, 1, 8, 0, tzinfo=UTC)

    def fake_daily(*args, **kwargs):
        del args, kwargs
        return pl.DataFrame(
            {
                "security_id": ["CN.600519", "CN.600519"],
                "symbol": ["600519.SH", "600519.SH"],
                "market": ["CN", "CN"],
                "trade_date": [trade_date, trade_date + timedelta(days=1)],
                "open": [100.0, 101.0],
                "high": [101.0, 102.0],
                "low": [99.0, 100.0],
                "close": [100.5, 101.5],
                "adj_close": [100.5, 101.5],
                "volume": [1000.0, 1100.0],
                "turnover": [100000.0, 111650.0],
                "turnover_rate": [None, None],
                "event_tag": [None, None],
                "data_source": ["tushare_pro", "tushare_pro"],
                "source_dataset": ["daily", "daily"],
                "source_run_id": ["test-daily", "test-daily"],
                "ingested_at": [ingested_at, ingested_at],
            }
        )

    def raise_rate_limit(*args, **kwargs):
        del args, kwargs
        raise Exception("rate limit")

    monkeypatch.setattr(
        "multi_layer_trading_lab.adapters.tushare.client.TushareClient.fetch_daily_bars",
        fake_daily,
    )
    monkeypatch.setattr(
        "multi_layer_trading_lab.adapters.tushare.client.TushareClient.fetch_trade_calendar",
        raise_rate_limit,
    )
    monkeypatch.setattr(
        "multi_layer_trading_lab.adapters.tushare.client.TushareClient.fetch_security_master",
        raise_rate_limit,
    )

    result = CliRunner().invoke(
        app,
        [
            "fetch-tushare-to-lake",
            "--lake-root",
            str(lake_root),
            "--token",
            "token",
            "--symbols",
            "600519.SH",
            "--use-real",
        ],
    )

    assert result.exit_code == 0
    assert "status=real_adapter" in result.output
    assert "security_master_fallback=derived_from_daily_bars" in result.output
    assert "trade_calendar_fallback=derived_from_daily_bars" in result.output
    master = pl.read_parquet(lake_root / "security_master" / "part-000.parquet")
    assert master["data_source"].to_list() == ["tushare_pro"]
    assert master["source_dataset"].to_list() == ["daily_symbol_universe"]
    calendar = pl.read_parquet(lake_root / "trade_calendar" / "part-000.parquet")
    assert calendar["source_dataset"].to_list() == ["daily_trade_dates", "daily_trade_dates"]
    assert (lake_root / "daily_features" / "part-000.parquet").exists()


def test_fetch_ifind_to_lake_blocks_without_credentials_unless_stub_allowed(tmp_path) -> None:
    lake_root = tmp_path / "lake"
    blocked = CliRunner().invoke(
        app,
        [
            "fetch-ifind-to-lake",
            "--lake-root",
            str(lake_root),
            "--username",
            "",
            "--password",
            "",
        ],
    )
    assert blocked.exit_code == 0
    assert "status=blocked" in blocked.output
    assert not (lake_root / "ifind_events" / "part-000.parquet").exists()

    allowed = CliRunner().invoke(
        app,
        [
            "fetch-ifind-to-lake",
            "--lake-root",
            str(lake_root),
            "--username",
            "",
            "--password",
            "",
            "--allow-stub",
        ],
    )
    assert allowed.exit_code == 0
    assert "status=stub_adapter" in allowed.output
    assert (lake_root / "ifind_events" / "part-000.parquet").exists()


def test_ifind_token_status_reports_refresh_expiry_without_printing_secret() -> None:
    refresh_token = _ifind_refresh_token("2026-05-29 11:32:56")
    result = CliRunner().invoke(
        app,
        [
            "ifind-token-status",
            "--access-token",
            "access-secret",
            "--refresh-token",
            refresh_token,
        ],
    )

    assert result.exit_code == 0
    assert "ifind_access_token_present=true" in result.output
    assert "ifind_refresh_token_present=true" in result.output
    assert "ifind_refresh_token_expires_at=2026-05-29T11:32:56+00:00" in result.output
    assert "access-secret" not in result.output
    assert refresh_token not in result.output


def test_ifind_refresh_access_token_smoke_writes_safe_status(
    tmp_path,
    monkeypatch,
) -> None:
    status_path = tmp_path / "ifind_refresh.json"
    refresh_token = _ifind_refresh_token("2026-05-29 11:32:56")

    def fake_refresh(**kwargs):
        assert kwargs["refresh_token"] == refresh_token
        return IFindAccessTokenRefreshStatus(
            requested_url=kwargs["url"],
            access_token_received=True,
            access_token_expires_at=datetime(2026, 5, 10, 11, 0, tzinfo=UTC),
        )

    monkeypatch.setattr(
        "multi_layer_trading_lab.cli.refresh_ifind_access_token_status",
        fake_refresh,
    )

    result = CliRunner().invoke(
        app,
        [
            "ifind-refresh-access-token-smoke",
            "--refresh-token",
            refresh_token,
            "--output-path",
            str(status_path),
        ],
    )

    assert result.exit_code == 0
    assert "ifind_access_token_refresh_ok=true" in result.output
    assert refresh_token not in result.output
    report_text = status_path.read_text(encoding="utf-8")
    report = json.loads(report_text)
    assert report["access_token_received"] is True
    assert report["access_token_expires_at"] == "2026-05-10T11:00:00+00:00"
    assert refresh_token not in report_text


def test_fetch_ifind_to_lake_requires_explicit_real_mode_for_real_token(tmp_path) -> None:
    lake_root = tmp_path / "lake"
    result = CliRunner().invoke(
        app,
        [
            "fetch-ifind-to-lake",
            "--lake-root",
            str(lake_root),
            "--access-token",
            "access",
            "--refresh-token",
            "refresh",
        ],
    )

    assert result.exit_code == 0
    assert "status=blocked" in result.output
    assert "real_ifind_adapter_requires_use_real" in result.output
    assert not (lake_root / "ifind_events" / "part-000.parquet").exists()


def test_fetch_ifind_to_lake_real_mode_uses_official_report_query(
    tmp_path,
    monkeypatch,
) -> None:
    lake_root = tmp_path / "lake"
    validation_path = tmp_path / "ifind_validation.json"

    class FakeResponse:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            return {
                "tables": [
                    {
                        "table": {
                            "seq": ["seq-1"],
                            "thscode": ["00700.HK"],
                            "reportDate": ["2026-04-01"],
                            "ctime": ["2026-04-01 08:00:00"],
                            "reportTitle": ["Tencent notice"],
                        }
                    }
                ]
            }

    def fake_post(url, json, headers, timeout):
        assert url == "https://quantapi.51ifind.com/api/v1/report_query"
        assert json["codes"] == "00700.HK"
        assert json["beginrDate"] == "2026-04-01"
        assert json["endrDate"] == "2026-04-08"
        assert headers["access_token"] == "access"
        assert timeout == 30
        return FakeResponse()

    monkeypatch.setattr("requests.post", fake_post)

    result = CliRunner().invoke(
        app,
        [
            "fetch-ifind-to-lake",
            "--lake-root",
            str(lake_root),
            "--symbols",
            "00700.HK",
            "--start",
            "2026-04-01",
            "--end",
            "2026-04-08",
            "--access-token",
            "access",
            "--refresh-token",
            "refresh",
            "--validation-report-path",
            str(validation_path),
            "--use-real",
        ],
    )

    assert result.exit_code == 0
    assert "status=real_adapter" in result.output
    assert "ifind_events_rows=1" in result.output
    frame = pl.read_parquet(lake_root / "ifind_events" / "part-000.parquet")
    assert frame["event_id"][0] == "seq-1"
    assert frame["symbol"][0] == "00700.HK"
    assert frame["event_title"][0] == "Tencent notice"
    validation = json.loads(validation_path.read_text(encoding="utf-8"))
    assert validation["valid"] is True
    assert validation["rows"] == 1


def test_fetch_ifind_to_lake_real_endpoint_writes_events(tmp_path, monkeypatch) -> None:
    lake_root = tmp_path / "lake"
    validation_path = tmp_path / "ifind_validation.json"

    class FakeResponse:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            return {
                "events": [
                    {
                        "id": "evt-1",
                        "symbol": "00700.HK",
                        "date": "20260401",
                        "type": "company_notice",
                        "title": "notice",
                    }
                ]
            }

    def fake_post(url, json, headers, timeout):
        assert url == "http://127.0.0.1:9999/ifind/events"
        assert json["symbols"] == ["00700.HK"]
        assert headers["Authorization"] == "Bearer access"
        assert timeout == 30
        return FakeResponse()

    monkeypatch.setattr("requests.post", fake_post)

    result = CliRunner().invoke(
        app,
        [
            "fetch-ifind-to-lake",
            "--lake-root",
            str(lake_root),
            "--symbols",
            "00700.HK",
            "--access-token",
            "access",
            "--refresh-token",
            "refresh",
            "--events-endpoint",
            "http://127.0.0.1:9999/ifind/events",
            "--validation-report-path",
            str(validation_path),
            "--use-real",
        ],
    )

    assert result.exit_code == 0
    assert "status=real_adapter" in result.output
    assert "ifind_events_rows=1" in result.output
    assert "validation_report=" in result.output
    assert (lake_root / "ifind_events" / "part-000.parquet").exists()
    validation = json.loads(validation_path.read_text(encoding="utf-8"))
    assert validation["valid"] is True
    assert validation["rows"] == 1


def test_fetch_ifind_to_lake_blocks_cleanly_on_real_fetch_error(tmp_path, monkeypatch) -> None:
    lake_root = tmp_path / "lake"

    def fake_post(*args, **kwargs):
        del args, kwargs
        raise TimeoutError("endpoint timeout")

    monkeypatch.setattr("requests.post", fake_post)

    result = CliRunner().invoke(
        app,
        [
            "fetch-ifind-to-lake",
            "--lake-root",
            str(lake_root),
            "--access-token",
            "access",
            "--refresh-token",
            "refresh",
            "--events-endpoint",
            "http://127.0.0.1:9999/ifind/events",
            "--use-real",
        ],
    )

    assert result.exit_code == 0
    assert "status=blocked" in result.output
    assert "ifind_fetch_failed:TimeoutError" in result.output
    assert not (lake_root / "ifind_events" / "part-000.parquet").exists()


def test_import_ifind_events_file_writes_real_file_events(tmp_path) -> None:
    lake_root = tmp_path / "lake"
    export_path = tmp_path / "ifind_events.json"
    export_path.write_text(
        json.dumps(
            {
                "events": [
                    {
                        "id": "evt-1",
                        "symbol": "00700.HK",
                        "date": "20260401",
                        "type": "company_notice",
                        "title": "notice",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    result = CliRunner().invoke(
        app,
        [
            "import-ifind-events-file",
            str(export_path),
            "--lake-root",
            str(lake_root),
            "--source-run-id",
            "manual-ifind-export",
        ],
    )

    assert result.exit_code == 0
    assert "status=real_file_adapter" in result.output
    assert "ifind_events_rows=1" in result.output
    dataset_path = lake_root / "ifind_events" / "part-000.parquet"
    assert dataset_path.exists()
    frame = pl.read_parquet(dataset_path)
    assert frame["data_source"][0] == "ifind_real_file"
    assert frame["source_run_id"][0] == "manual-ifind-export"


def test_import_ifind_events_file_blocks_duplicate_event_ids(tmp_path) -> None:
    lake_root = tmp_path / "lake"
    export_path = tmp_path / "ifind_events.json"
    export_path.write_text(
        json.dumps(
            {
                "events": [
                    {
                        "id": "evt-1",
                        "symbol": "00700.HK",
                        "date": "20260401",
                        "type": "company_notice",
                        "title": "notice",
                    },
                    {
                        "id": "evt-1",
                        "symbol": "00700.HK",
                        "date": "20260402",
                        "type": "company_notice",
                        "title": "notice 2",
                    },
                ]
            }
        ),
        encoding="utf-8",
    )

    result = CliRunner().invoke(
        app,
        [
            "import-ifind-events-file",
            str(export_path),
            "--lake-root",
            str(lake_root),
        ],
    )

    assert result.exit_code == 0
    assert "status=blocked" in result.output
    assert "duplicate_key_rows=1" in result.output
    assert "failed_reasons=duplicate_key_rows" in result.output
    assert not (lake_root / "ifind_events" / "part-000.parquet").exists()


def test_import_ifind_events_file_blocks_existing_dataset_without_overwrite(tmp_path) -> None:
    lake_root = tmp_path / "lake"
    dataset_path = lake_root / "ifind_events"
    dataset_path.mkdir(parents=True)
    pl.DataFrame({"existing": [1]}).write_parquet(dataset_path / "part-000.parquet")
    export_path = tmp_path / "ifind_events.json"
    export_path.write_text(
        json.dumps(
            {
                "events": [
                    {
                        "id": "evt-1",
                        "symbol": "00700.HK",
                        "date": "20260401",
                        "type": "company_notice",
                        "title": "notice",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    result = CliRunner().invoke(
        app,
        [
            "import-ifind-events-file",
            str(export_path),
            "--lake-root",
            str(lake_root),
        ],
    )

    assert result.exit_code == 0
    assert "status=blocked" in result.output
    assert "ifind_events_dataset_exists" in result.output
    assert pl.read_parquet(dataset_path / "part-000.parquet").columns == ["existing"]


def test_import_ifind_events_file_can_overwrite_existing_dataset(tmp_path) -> None:
    lake_root = tmp_path / "lake"
    dataset_path = lake_root / "ifind_events"
    dataset_path.mkdir(parents=True)
    pl.DataFrame({"existing": [1]}).write_parquet(dataset_path / "part-000.parquet")
    export_path = tmp_path / "ifind_events.json"
    export_path.write_text(
        json.dumps(
            {
                "events": [
                    {
                        "id": "evt-1",
                        "symbol": "00700.HK",
                        "date": "20260401",
                        "type": "company_notice",
                        "title": "notice",
                    }
                ]
            }
        ),
        encoding="utf-8",
    )

    result = CliRunner().invoke(
        app,
        [
            "import-ifind-events-file",
            str(export_path),
            "--lake-root",
            str(lake_root),
            "--overwrite",
        ],
    )

    assert result.exit_code == 0
    assert "status=real_file_adapter" in result.output
    frame = pl.read_parquet(dataset_path / "part-000.parquet")
    assert frame["data_source"][0] == "ifind_real_file"


def test_write_ifind_events_template_cli_writes_importable_csv(tmp_path) -> None:
    output_path = tmp_path / "template.csv"

    result = CliRunner().invoke(
        app,
        [
            "write-ifind-events-template",
            "--output-path",
            str(output_path),
        ],
    )

    assert result.exit_code == 0
    assert "ifind_events_template=" in result.output
    assert output_path.exists()
    frame = pl.read_csv(output_path)
    assert frame.columns == ["id", "symbol", "date", "type", "title", "importance"]
    assert frame["symbol"][0] == "00700.HK"


def test_validate_ifind_events_file_cli_reports_contract_pass(tmp_path) -> None:
    export_path = tmp_path / "ifind_events.csv"
    report_path = tmp_path / "ifind_validation.json"
    pl.DataFrame(
        {
            "事件ID": ["evt-1"],
            "证券代码": ["00700.HK"],
            "公告日期": ["20260401"],
            "事件类型": ["company_notice"],
            "公告标题": ["notice"],
        }
    ).write_csv(export_path)

    result = CliRunner().invoke(
        app,
        [
            "validate-ifind-events-file",
            str(export_path),
            "--source-run-id",
            "manual-ifind-export",
            "--output-path",
            str(report_path),
        ],
    )

    assert result.exit_code == 0
    assert "validation_report=" in result.output
    assert "ifind_events_file_valid=true" in result.output
    assert "ifind_events_rows=1" in result.output
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["valid"] is True
    assert report["rows"] == 1
    assert report["source_run_id"] == "manual-ifind-export"


def test_validate_ifind_events_file_cli_blocks_bad_file(tmp_path) -> None:
    export_path = tmp_path / "bad_ifind_events.csv"
    report_path = tmp_path / "bad_ifind_validation.json"
    pl.DataFrame({"事件ID": ["evt-1"], "公告日期": ["20260401"]}).write_csv(export_path)

    result = CliRunner().invoke(
        app,
        [
            "validate-ifind-events-file",
            str(export_path),
            "--output-path",
            str(report_path),
        ],
    )

    assert result.exit_code == 0
    assert "status=blocked" in result.output
    assert "ifind_file_validate_failed:ValueError" in result.output
    assert "missing symbol" in result.output
    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert report["valid"] is False
    assert "ifind_file_validate_failed:ValueError" in report["failed_reasons"]


def test_data_adapter_status_blocks_ifind_real_file_adapter_without_lake_rows(
    tmp_path,
) -> None:
    result = CliRunner().invoke(
        app,
        [
            "data-adapter-status",
            "--lake-root",
            str(tmp_path / "lake"),
            "--ifind-access-token",
            "access",
            "--ifind-refresh-token",
            "refresh",
            "--ifind-adapter-status",
            "real_file_adapter",
        ],
    )

    assert result.exit_code == 0
    assert "ifind_adapter_status=real_file_adapter_missing_lake_data" in result.output
    assert "ifind_live_data_ready=false" in result.output
    assert "ifind_real_file_adapter_missing_lake_data" in result.output


def test_data_adapter_status_accepts_ifind_real_file_adapter_with_lake_rows(
    tmp_path,
) -> None:
    lake_root = tmp_path / "lake"
    path = lake_root / "ifind_events"
    path.mkdir(parents=True)
    pl.DataFrame({"symbol": ["00700.HK"], "data_source": ["ifind_real_file"]}).write_parquet(
        path / "part-000.parquet"
    )

    result = CliRunner().invoke(
        app,
        [
            "data-adapter-status",
            "--lake-root",
            str(lake_root),
            "--ifind-access-token",
            "access",
            "--ifind-refresh-token",
            "refresh",
            "--ifind-adapter-status",
            "real_file_adapter",
        ],
    )

    assert result.exit_code == 0
    assert "ifind_adapter_status=real_file_adapter" in result.output
    assert "ifind_live_data_ready=true" in result.output


def test_ifind_ingestion_status_reports_stub_lake_rows(tmp_path) -> None:
    lake_root = tmp_path / "lake"
    status_path = tmp_path / "ifind_status.json"
    path = lake_root / "ifind_events"
    path.mkdir(parents=True)
    pl.DataFrame({"symbol": ["00700.HK"], "data_source": ["ifind_stub"]}).write_parquet(
        path / "part-000.parquet"
    )

    result = CliRunner().invoke(
        app,
        [
            "ifind-ingestion-status",
            "--lake-root",
            str(lake_root),
            "--output-path",
            str(status_path),
            "--ifind-access-token",
            "access",
            "--ifind-refresh-token",
            _ifind_refresh_token("2026-05-29 11:32:56"),
        ],
    )

    assert result.exit_code == 0
    assert "ifind_ingestion_ready=false" in result.output
    assert "ifind_stub_rows=1" in result.output
    assert "missing_real_ifind_lake_rows" in result.output
    report = json.loads(status_path.read_text(encoding="utf-8"))
    assert report["access_token_present"] is True
    assert report["refresh_token_valid_now"] is True
    assert report["ifind_stub_rows"] == 1
    assert report["ifind_real_rows"] == 0
    assert report["ifind_real_file_rows"] == 0


def test_ifind_ingestion_status_accepts_real_file_rows(tmp_path) -> None:
    lake_root = tmp_path / "lake"
    status_path = tmp_path / "ifind_status.json"
    path = lake_root / "ifind_events"
    path.mkdir(parents=True)
    pl.DataFrame({"symbol": ["00700.HK"], "data_source": ["ifind_real_file"]}).write_parquet(
        path / "part-000.parquet"
    )

    result = CliRunner().invoke(
        app,
        [
            "ifind-ingestion-status",
            "--lake-root",
            str(lake_root),
            "--output-path",
            str(status_path),
            "--ifind-access-token",
            "access",
            "--ifind-refresh-token",
            _ifind_refresh_token("2026-05-29 11:32:56"),
            "--events-endpoint",
            "http://127.0.0.1:9999/ifind/events",
        ],
    )

    assert result.exit_code == 0
    assert "ifind_ingestion_ready=true" in result.output
    assert "ifind_real_file_rows=1" in result.output
    report = json.loads(status_path.read_text(encoding="utf-8"))
    assert report["ready"] is True
    assert report["failed_reasons"] == []


def test_ifind_connection_plan_reports_safe_next_actions(tmp_path) -> None:
    lake_root = tmp_path / "lake"
    plan_path = tmp_path / "ifind_connection_plan.json"
    path = lake_root / "ifind_events"
    path.mkdir(parents=True)
    pl.DataFrame({"symbol": ["00700.HK"], "data_source": ["ifind_stub"]}).write_parquet(
        path / "part-000.parquet"
    )
    refresh_token = _ifind_refresh_token("2026-05-29 11:32:56")

    result = CliRunner().invoke(
        app,
        [
            "ifind-connection-plan",
            "--lake-root",
            str(lake_root),
            "--output-path",
            str(plan_path),
            "--access-token",
            "secret-access-token",
            "--refresh-token",
            refresh_token,
        ],
    )

    assert result.exit_code == 0
    assert "ready_for_real_http_fetch=true" in result.output
    assert "ready_for_real_file_mode=false" in result.output
    assert "missing_real_ifind_lake_rows" in result.output
    assert "secret-access-token" not in result.output
    assert refresh_token not in result.output
    report_text = plan_path.read_text(encoding="utf-8")
    report = json.loads(report_text)
    assert report["access_token_present"] is True
    assert report["refresh_token_present"] is True
    assert report["refresh_token_expires_at"] == "2026-05-29T11:32:56+00:00"
    assert report["official_report_query_available"] is True
    assert report["lake_counts"]["ifind_stub_rows"] == 1
    assert "secret-access-token" not in report_text
    assert refresh_token not in report_text


def test_ifind_connection_plan_detects_http_smoke_ready(tmp_path) -> None:
    plan_path = tmp_path / "ifind_connection_plan.json"

    result = CliRunner().invoke(
        app,
        [
            "ifind-connection-plan",
            "--lake-root",
            str(tmp_path / "lake"),
            "--output-path",
            str(plan_path),
            "--access-token",
            "secret-access-token",
            "--refresh-token",
            _ifind_refresh_token("2026-05-29 11:32:56"),
            "--events-endpoint",
            "http://127.0.0.1:9999/ifind/events",
        ],
    )

    assert result.exit_code == 0
    assert "ready_for_real_http_fetch=true" in result.output
    report = json.loads(plan_path.read_text(encoding="utf-8"))
    assert report["endpoint_host"] == "127.0.0.1:9999"
    assert report["ready_for_real_http_fetch"] is True
    assert report["ready_for_real_file_mode"] is False
    assert "missing_real_ifind_lake_rows" in report["failed_reasons"]


def test_fetch_opend_quote_snapshot_cli_blocks_when_web_api_unavailable(tmp_path) -> None:
    result = CliRunner().invoke(
        app,
        [
            "fetch-opend-quote-snapshot",
            "00700",
            "--output-path",
            str(tmp_path / "quote.json"),
            "--base-url",
            "http://127.0.0.1:1",
            "--timeout-seconds",
            "0.01",
        ],
    )

    assert result.exit_code == 0
    assert "status=blocked" in result.output
    assert "opend_quote_snapshot_failed" in result.output
    assert not (tmp_path / "quote.json").exists()


def test_fetch_opend_account_status_cli_blocks_when_web_api_unavailable(tmp_path) -> None:
    result = CliRunner().invoke(
        app,
        [
            "fetch-opend-account-status",
            "--output-path",
            str(tmp_path / "account_status.json"),
            "--base-url",
            "http://127.0.0.1:1",
            "--timeout-seconds",
            "0.01",
        ],
    )

    assert result.exit_code == 0
    assert "status=blocked" in result.output
    assert "opend_account_status_failed" in result.output
    assert not (tmp_path / "account_status.json").exists()


def test_fetch_opend_account_status_cli_can_require_paper_simulate_ready(
    tmp_path,
) -> None:
    output = tmp_path / "account_status.json"
    with patch("multi_layer_trading_lab.cli.read_opend_account_status") as mocked:
        mocked.return_value = {
            "ready_for_paper_simulate": False,
            "simulate_account_count": 1,
            "hk_stock_simulate_account_count": 0,
            "failed_reasons": ["missing_hk_stock_simulate_account"],
        }
        result = CliRunner().invoke(
            app,
            [
                "fetch-opend-account-status",
                "--output-path",
                str(output),
                "--require-paper-simulate-ready",
            ],
        )

    assert result.exit_code == 1
    assert output.exists()
    assert "ready_for_paper_simulate=false" in result.output
    assert "missing_hk_stock_simulate_account" in result.output
    assert "opend_account_not_ready_for_paper_simulate" in result.output


def test_extract_futu_web_report_cli_writes_broker_report(tmp_path) -> None:
    web_log = tmp_path / "web.jsonl"
    output = tmp_path / "futu.json"
    web_log.write_text(
        '{"event":"order_query","data":[{"order_id":"futu-1","order_status":"FILLED_ALL","dealt_qty":100,"dealt_avg_price":8.0,"remark":"paper-001"}]}\n',
        encoding="utf-8",
    )

    result = CliRunner().invoke(
        app,
        ["extract-futu-web-report", str(web_log), "--output-path", str(output)],
    )

    assert result.exit_code == 0
    assert "report_rows=1" in result.output
    rows = json.loads(output.read_text(encoding="utf-8"))
    assert rows[0]["local_order_id"] == "paper-001"


def test_extract_futu_ticket_response_report_cli_writes_broker_report(tmp_path) -> None:
    responses = tmp_path / "responses.jsonl"
    output = tmp_path / "futu.json"
    responses.write_text(
        '{"event":"mttl_opend_paper_ticket_response","ticket_id":"paper-001","dry_run":true,"response":{"submitted":false,"intent":{"quantity":100,"limit_price":"8.0"}}}\n',
        encoding="utf-8",
    )

    result = CliRunner().invoke(
        app,
        [
            "extract-futu-ticket-response-report",
            "--response-path",
            str(responses),
            "--output-path",
            str(output),
        ],
    )

    assert result.exit_code == 0
    assert "report_rows=1" in result.output
    rows = json.loads(output.read_text(encoding="utf-8"))
    assert rows[0]["local_order_id"] == "paper-001"
    assert rows[0]["dry_run"] is True


def test_extract_futu_ticket_response_report_cli_blocks_missing_input(tmp_path) -> None:
    result = CliRunner().invoke(
        app,
        [
            "extract-futu-ticket-response-report",
            "--response-path",
            str(tmp_path / "missing.jsonl"),
            "--output-path",
            str(tmp_path / "futu.json"),
        ],
    )

    assert result.exit_code == 0
    assert "status=blocked" in result.output
    assert "missing_opend_ticket_response_path" in result.output


def test_build_paper_execution_log_cli_writes_local_log(tmp_path) -> None:
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
                    "order_id": "futu-1",
                    "order_status": "FILLED_ALL",
                    "dealt_qty": 100,
                    "dealt_avg_price": 8.0,
                }
            ]
        ),
        encoding="utf-8",
    )

    result = CliRunner().invoke(
        app,
        [
            "build-paper-execution-log",
            "--ticket-path",
            str(tickets),
            "--broker-report-path",
            str(broker_report),
            "--execution-log-path",
            str(execution_log),
        ],
    )

    assert result.exit_code == 0
    assert "status=ready" in result.output
    assert "execution_log_rows=1" in result.output
    rows = [json.loads(line) for line in execution_log.read_text(encoding="utf-8").splitlines()]
    assert rows[0]["dry_run"] is False


def test_build_paper_session_evidence_bundle_blocks_dry_run_responses(tmp_path) -> None:
    tickets = tmp_path / "tickets.jsonl"
    responses = tmp_path / "responses.jsonl"
    broker_report = tmp_path / "futu.json"
    execution_log = tmp_path / "execution.jsonl"
    profitability = tmp_path / "profitability.json"
    tickets.write_text(
        json.dumps(
            {
                "ticket_id": "paper-001",
                "dry_run": True,
                "real": False,
                "submit_real": False,
                "web_normal_order_payload": {"symbol": "HK.00001", "side": "BUY"},
            }
        )
        + "\n",
        encoding="utf-8",
    )
    responses.write_text(
        '{"event":"mttl_opend_paper_ticket_response","ticket_id":"paper-001","dry_run":true,"response":{"submitted":false,"intent":{"quantity":100,"limit_price":"8.0"}}}\n',
        encoding="utf-8",
    )

    result = CliRunner().invoke(
        app,
        [
            "build-paper-session-evidence-bundle",
            "--ticket-path",
            str(tickets),
            "--response-path",
            str(responses),
            "--broker-report-path",
            str(broker_report),
            "--execution-log-path",
            str(execution_log),
            "--profitability-evidence-path",
            str(profitability),
        ],
    )

    assert result.exit_code == 0
    assert "status=blocked" in result.output
    assert "dry_run_broker_report_not_real_paper" in result.output
    assert broker_report.exists()
    assert not execution_log.exists()


def test_build_paper_session_evidence_bundle_from_real_broker_report(tmp_path) -> None:
    tickets = tmp_path / "tickets.jsonl"
    broker_report = tmp_path / "futu.json"
    execution_log = tmp_path / "execution.jsonl"
    profitability = tmp_path / "profitability.json"
    tickets_rows = []
    broker_rows = []
    for day in range(1, 21):
        order_id = f"buy-{day}" if day == 1 else f"sell-{day}"
        trade_date = f"2026-04-{day:02d}"
        tickets_rows.append(
            {
                "ticket_id": order_id,
                "web_normal_order_payload": {
                    "symbol": "HK.00001",
                    "side": "BUY" if day == 1 else "SELL",
                },
            }
        )
        broker_rows.append(
            {
                "local_order_id": order_id,
                "order_id": f"futu-{order_id}",
                "order_status": "FILLED_ALL",
                "dealt_qty": 100 if day == 1 else 10 if day == 20 else 5,
                "dealt_avg_price": 8.0 if day == 1 else 8.5,
                "updated_time": f"{trade_date} 10:00:00",
            }
        )
    tickets.write_text(
        "\n".join(json.dumps(row) for row in tickets_rows) + "\n",
        encoding="utf-8",
    )
    broker_report.write_text(json.dumps(broker_rows), encoding="utf-8")

    result = CliRunner().invoke(
        app,
        [
            "build-paper-session-evidence-bundle",
            "--ticket-path",
            str(tickets),
            "--response-path",
            "",
            "--broker-report-path",
            str(broker_report),
            "--execution-log-path",
            str(execution_log),
            "--profitability-evidence-path",
            str(profitability),
            "--paper-sessions",
            "20",
        ],
    )

    assert result.exit_code == 0
    assert "status=ready" in result.output
    assert "paper_evidence_ready=true" in result.output
    assert "profitability_evidence_ready=true" in result.output
    report = json.loads(profitability.read_text(encoding="utf-8"))
    assert report["net_pnl"] == 50.0
    assert report["inferred_session_count"] == 20


def test_ops_report_writes_daily_report(tmp_path) -> None:
    output_path = tmp_path / "ops.md"
    lake_root = tmp_path / "lake"
    result = CliRunner().invoke(
        app,
        ["ops-report", "--output-path", str(output_path), "--lake-root", str(lake_root)],
    )

    assert result.exit_code == 0
    assert output_path.exists()
    assert "ops_report=" in result.output
    content = output_path.read_text(encoding="utf-8")
    assert "Account Risk Budget" in content
    assert "security_master: missing rows=0" in content


def test_ops_report_can_include_research_and_paper_gate_decisions(tmp_path) -> None:
    output_path = tmp_path / "ops.md"
    lake_root = tmp_path / "lake"
    execution_log = tmp_path / "execution_log.jsonl"
    futu_report = tmp_path / "futu.json"
    execution_log.write_text(
        json.dumps(
            {
                "order_id": "ord-1",
                "status": "filled",
                "quantity": 100,
                "fill_price": 320.5,
                "slippage": 0.01,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    futu_report.write_text(
        json.dumps(
            [
                {
                    "local_order_id": "ord-1",
                    "order_id": "futu-1",
                    "order_status": "FILLED_ALL",
                    "dealt_qty": 100,
                    "dealt_avg_price": 320.5,
                }
            ]
        ),
        encoding="utf-8",
    )

    result = CliRunner().invoke(
        app,
        [
            "ops-report",
            "--output-path",
            str(output_path),
            "--lake-root",
            str(lake_root),
            "--include-research-audit",
            "--execution-log-path",
            str(execution_log),
            "--futu-report-path",
            str(futu_report),
            "--paper-sessions",
            "20",
        ],
    )

    assert result.exit_code == 0
    content = output_path.read_text(encoding="utf-8")
    assert "research_to_paper: BLOCKED" in content
    assert "paper_to_live: BLOCKED" in content


def test_research_audit_blocks_empty_lake(tmp_path) -> None:
    result = CliRunner().invoke(app, ["research-audit", "--lake-root", str(tmp_path / "lake")])

    assert result.exit_code == 0
    assert "research_to_paper_approved=false" in result.output
    assert "trade_count=0" in result.output
    assert "insufficient_research_trades" in result.output


def test_profile_l2_sample_cli_outputs_mapping(tmp_path) -> None:
    path = tmp_path / "sample.parquet"
    mapping_path = tmp_path / "mapping.json"
    pl.DataFrame({"code": ["00700.HK"], "event_time": ["2026-04-01 09:30:00"]}).write_parquet(
        path
    )

    result = CliRunner().invoke(
        app,
        ["profile-l2-sample", str(path), "--mapping-output", str(mapping_path)],
    )

    assert result.exit_code == 0
    assert "map.symbol=code" in result.output
    assert "map.ts=event_time" in result.output
    assert "mapping_template=" in result.output
    assert mapping_path.exists()


def test_profile_l2_zip_cli_profiles_realistic_order_add_member(tmp_path) -> None:
    zip_path = tmp_path / "20250123.zip"
    with ZipFile(zip_path, "w") as archive:
        archive.writestr(
            "20250123\\OrderAdd\\00005.csv",
            "SeqNum,OrderId,OrderType,Ext,Time,Price,Volume,Level\n"
            "0,13337603,1,110,92006,78.900,9600,0\n",
        )

    result = CliRunner().invoke(app, ["profile-l2-zip", str(zip_path)])

    assert result.exit_code == 0
    assert "member_count=1" in result.output
    assert "sample_rows=1" in result.output
    assert "map.symbol=symbol" in result.output
    assert "map.last_px=Price" in result.output


def test_import_l2_zip_order_add_cli_writes_raw_order_add_dataset(tmp_path) -> None:
    zip_path = tmp_path / "20250123.zip"
    lake_root = tmp_path / "lake"
    with ZipFile(zip_path, "w") as archive:
        archive.writestr(
            "20250123\\OrderAdd\\00005.csv",
            "SeqNum,OrderId,OrderType,Ext,Time,Price,Volume,Level,BrokerNo,VolumePre\n"
            "0,13337603,1,110,92006,78.900,9600,0,,0\n",
        )

    result = CliRunner().invoke(
        app,
        [
            "import-l2-zip-order-add",
            str(zip_path),
            "--lake-root",
            str(lake_root),
        ],
    )

    assert result.exit_code == 0
    assert "rows=1" in result.output
    assert (lake_root / "raw_l2_order_add" / "part-000.parquet").exists()


def test_import_l2_zip_order_add_batch_cli_writes_concat_dataset(tmp_path) -> None:
    first_zip = tmp_path / "20250123.zip"
    second_zip = tmp_path / "20250124.zip"
    lake_root = tmp_path / "lake"
    for zip_path in [first_zip, second_zip]:
        trade_date = zip_path.stem
        with ZipFile(zip_path, "w") as archive:
            archive.writestr(
                f"{trade_date}\\OrderAdd\\00005.csv",
                "SeqNum,OrderId,OrderType,Ext,Time,Price,Volume,Level,BrokerNo,VolumePre\n"
                "0,13337603,1,110,92006,78.900,9600,0,,0\n",
            )

    result = CliRunner().invoke(
        app,
        [
            "import-l2-zip-order-add-batch",
            f"{first_zip},{second_zip}",
            "--symbols",
            "00005.HK",
            "--lake-root",
            str(lake_root),
        ],
    )

    assert result.exit_code == 0
    assert "zip_count=2" in result.output
    assert "rows=2" in result.output
    assert (lake_root / "raw_l2_order_add" / "part-000.parquet").exists()


def test_discover_l2_zips_cli_lists_existing_range(tmp_path) -> None:
    root = tmp_path / "ticks"
    year = root / "2025"
    year.mkdir(parents=True)
    (year / "20250123.zip").write_bytes(b"placeholder")

    result = CliRunner().invoke(
        app,
        [
            "discover-l2-zips",
            "--raw-root",
            str(root),
            "--start",
            "2025-01-22",
            "--end",
            "2025-01-24",
        ],
    )

    assert result.exit_code == 0
    assert "zip_count=1" in result.output
    assert "20250123.zip" in result.output


def test_check_l2_order_add_coverage_cli_writes_csv(tmp_path) -> None:
    root = tmp_path / "ticks"
    year = root / "2025"
    output_path = tmp_path / "coverage.csv"
    year.mkdir(parents=True)
    zip_path = year / "20250123.zip"
    with ZipFile(zip_path, "w") as archive:
        archive.writestr(
            "20250123\\OrderAdd\\00005.csv",
            "SeqNum,OrderId,OrderType,Ext,Time,Price,Volume,Level\n"
            "0,13337603,1,110,92006,78.900,9600,0\n",
        )

    result = CliRunner().invoke(
        app,
        [
            "check-l2-order-add-coverage",
            "--raw-root",
            str(root),
            "--start",
            "2025-01-23",
            "--end",
            "2025-01-23",
            "--symbols",
            "00005.HK,00006.HK",
            "--output-path",
            str(output_path),
        ],
    )

    assert result.exit_code == 0
    assert "available_rows=1" in result.output
    assert "missing_rows=1" in result.output
    assert output_path.exists()


def test_import_l2_zip_order_add_range_cli_writes_dataset(tmp_path) -> None:
    root = tmp_path / "ticks"
    year = root / "2025"
    year.mkdir(parents=True)
    zip_path = year / "20250123.zip"
    lake_root = tmp_path / "lake"
    with ZipFile(zip_path, "w") as archive:
        archive.writestr(
            "20250123\\OrderAdd\\00005.csv",
            "SeqNum,OrderId,OrderType,Ext,Time,Price,Volume,Level,BrokerNo,VolumePre\n"
            "0,13337603,1,110,92006,78.900,9600,0,,0\n",
        )

    result = CliRunner().invoke(
        app,
        [
            "import-l2-zip-order-add-range",
            "--raw-root",
            str(root),
            "--start",
            "2025-01-23",
            "--end",
            "2025-01-24",
            "--symbols",
            "00005.HK",
            "--lake-root",
            str(lake_root),
        ],
    )

    assert result.exit_code == 0
    assert "zip_count=1" in result.output
    assert "rows=1" in result.output
    assert (lake_root / "raw_l2_order_add" / "part-000.parquet").exists()


def test_build_l2_order_add_features_from_lake_cli_writes_features(tmp_path) -> None:
    lake_root = tmp_path / "lake"
    raw_path = lake_root / "raw_l2_order_add"
    raw_path.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["00001.HK", "00001.HK"],
            "ts": ["2025-01-23 09:20:06", "2025-01-23 09:20:30"],
            "trade_date": ["20250123", "20250123"],
            "seq_num": [0, 1],
            "order_id": [1, 2],
            "order_type": [1, 1],
            "ext": [110, 110],
            "price": [78.9, 79.0],
            "volume": [9_600, 20_000],
            "level": [0, 1],
            "broker_no": [None, None],
            "volume_pre": [0, 0],
        }
    ).with_columns(pl.col("ts").str.strptime(pl.Datetime)).write_parquet(
        raw_path / "part-000.parquet"
    )

    result = CliRunner().invoke(
        app,
        ["build-l2-order-add-features-from-lake", "--lake-root", str(lake_root)],
    )

    assert result.exit_code == 0
    assert "features_rows=1" in result.output
    assert (lake_root / "l2_order_add_features" / "part-000.parquet").exists()


def test_build_hshare_verified_l2_features_cli_writes_intraday_features(tmp_path) -> None:
    verified_root = tmp_path / "verified"
    date_dir = verified_root / "verified_orders" / "year=2026" / "date=2026-04-01"
    date_dir.mkdir(parents=True)
    base = datetime(2026, 4, 1, 1, 20, tzinfo=UTC)
    pl.DataFrame(
        {
            "date": [datetime(2026, 4, 1).date()] * 25,
            "instrument_key": ["00001"] * 25,
            "SendTime": [base + timedelta(minutes=idx) for idx in range(25)],
            "Price": [61.0 + idx * 0.01 for idx in range(25)],
            "Volume": [500 + idx for idx in range(25)],
        }
    ).write_parquet(date_dir / "part-00000.parquet")
    lake_root = tmp_path / "lake"

    result = CliRunner().invoke(
        app,
        [
            "build-hshare-verified-l2-features",
            "--verified-root",
            str(verified_root),
            "--dates",
            "2026-04-01",
            "--symbols",
            "00001.HK",
            "--lake-root",
            str(lake_root),
        ],
    )

    assert result.exit_code == 0
    assert "features_rows=25" in result.output
    features = pl.read_parquet(lake_root / "intraday_l2_features" / "part-000.parquet")
    assert features["data_source"].to_list() == ["hshare_verified"] * 25
    assert features["source_dataset"].to_list() == ["verified_orders"] * 25
    assert features["security_id"].to_list()[0] == "HK.00001"


def test_build_order_add_signals_from_lake_cli_writes_candidates(tmp_path) -> None:
    lake_root = tmp_path / "lake"
    features_path = lake_root / "l2_order_add_features"
    features_path.mkdir(parents=True)
    pl.DataFrame(
        {
            "security_id": ["HK.00001"],
            "symbol": ["00001.HK"],
            "market": ["HK"],
            "trade_date": [datetime(2025, 1, 23).date()],
            "bar_start_ts": [datetime(2025, 1, 23, 9, 20)],
            "order_add_count": [10],
            "order_add_volume": [50_000],
            "large_order_ratio": [0.4],
        }
    ).write_parquet(features_path / "part-000.parquet")

    result = CliRunner().invoke(
        app,
        ["build-order-add-signals-from-lake", "--lake-root", str(lake_root)],
    )

    assert result.exit_code == 0
    assert "candidate_rows=1" in result.output
    assert (lake_root / "order_add_signal_candidates" / "part-000.parquet").exists()


def test_backtest_order_add_signals_from_lake_cli_writes_summary(tmp_path) -> None:
    lake_root = tmp_path / "lake"
    features_path = lake_root / "l2_order_add_features"
    candidates_path = lake_root / "order_add_signal_candidates"
    features_path.mkdir(parents=True)
    candidates_path.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["00001.HK", "00001.HK"],
            "bar_start_ts": [datetime(2025, 1, 23, 9, 20), datetime(2025, 1, 23, 9, 21)],
            "order_add_price_mean": [100.0, 101.0],
        }
    ).write_parquet(features_path / "part-000.parquet")
    pl.DataFrame(
        {
            "security_id": ["HK.00001"],
            "symbol": ["00001.HK"],
            "event_ts": [datetime(2025, 1, 23, 9, 20, tzinfo=UTC)],
            "score": [1.0],
        }
    ).write_parquet(candidates_path / "part-000.parquet")

    result = CliRunner().invoke(
        app,
        ["backtest-order-add-signals-from-lake", "--lake-root", str(lake_root)],
    )

    assert result.exit_code == 0
    assert "trade_count=1" in result.output
    assert (lake_root / "order_add_backtest_summary" / "part-000.parquet").exists()


def test_sweep_order_add_thresholds_from_lake_cli_writes_sweep(tmp_path) -> None:
    lake_root = tmp_path / "lake"
    features_path = lake_root / "l2_order_add_features"
    features_path.mkdir(parents=True)
    pl.DataFrame(
        {
            "security_id": ["HK.00001", "HK.00001"],
            "symbol": ["00001.HK", "00001.HK"],
            "market": ["HK", "HK"],
            "trade_date": [datetime(2025, 1, 23).date(), datetime(2025, 1, 23).date()],
            "bar_start_ts": [datetime(2025, 1, 23, 9, 20), datetime(2025, 1, 23, 9, 21)],
            "order_add_price_mean": [100.0, 101.0],
            "order_add_count": [10, 10],
            "order_add_volume": [50_000, 10_000],
            "large_order_ratio": [0.5, 0.0],
        }
    ).write_parquet(features_path / "part-000.parquet")

    result = CliRunner().invoke(
        app,
        [
            "sweep-order-add-thresholds-from-lake",
            "--lake-root",
            str(lake_root),
            "--volume-thresholds",
            "10000,50000",
            "--large-order-ratio-thresholds",
            "0.0,0.5",
            "--planned-notional",
            "8000",
        ],
    )

    assert result.exit_code == 0
    assert "sweep_rows=4" in result.output
    output = lake_root / "order_add_threshold_sweep" / "part-000.parquet"
    assert output.exists()
    assert pl.read_parquet(output)["planned_notional"].to_list() == [8_000.0] * 4


def test_order_add_research_gate_cli_blocks_negative_sweep(tmp_path) -> None:
    lake_root = tmp_path / "lake"
    sweep_path = lake_root / "order_add_threshold_sweep"
    sweep_path.mkdir(parents=True)
    pl.DataFrame(
        {
            "min_order_add_volume": [200_000],
            "min_large_order_ratio": [0.03],
            "trade_count": [20],
            "avg_net_ret": [-0.002],
            "total_net_ret": [-0.04],
        }
    ).write_parquet(sweep_path / "part-000.parquet")

    result = CliRunner().invoke(
        app,
        ["order-add-research-gate", "--lake-root", str(lake_root), "--min-trade-count", "30"],
    )

    assert result.exit_code == 0
    assert "order_add_research_approved=false" in result.output
    assert "avg_net_ret_not_positive" in result.output
    assert "estimated_single_trade_notional=200000.00" in result.output


def test_import_l2_sample_cli_writes_normalized_lake_dataset(tmp_path) -> None:
    sample_path = tmp_path / "sample.parquet"
    mapping_path = tmp_path / "mapping.json"
    lake_root = tmp_path / "lake"
    pl.DataFrame(
        {
            "code": ["00700.HK"],
            "event_time": ["2026-04-01 09:30:00"],
            "bid1": [320.0],
            "ask1": [320.2],
            "bidvol1": [1000],
            "askvol1": [900],
            "price": [320.1],
            "volume": [500],
            "bs_flag": ["BUY"],
            "is_cancel": [False],
        }
    ).write_parquet(sample_path)
    profile_result = CliRunner().invoke(
        app,
        ["profile-l2-sample", str(sample_path), "--mapping-output", str(mapping_path)],
    )
    assert profile_result.exit_code == 0

    result = CliRunner().invoke(
        app,
        [
            "import-l2-sample",
            str(sample_path),
            str(mapping_path),
            "--lake-root",
            str(lake_root),
        ],
    )

    assert result.exit_code == 0
    assert "normalized_rows=1" in result.output
    assert (lake_root / "raw_l2_ticks" / "part-000.parquet").exists()


def test_build_l2_features_from_lake_cli_writes_feature_dataset(tmp_path) -> None:
    sample_path = tmp_path / "sample.parquet"
    mapping_path = tmp_path / "mapping.json"
    lake_root = tmp_path / "lake"
    pl.DataFrame(
        {
            "code": ["00700.HK", "00700.HK"],
            "event_time": ["2026-04-01 09:30:00", "2026-04-01 09:30:30"],
            "bid1": [320.0, 320.1],
            "ask1": [320.2, 320.3],
            "bidvol1": [1000, 1100],
            "askvol1": [900, 1000],
            "price": [320.1, 320.2],
            "volume": [500, 600],
            "bs_flag": ["BUY", "SELL"],
            "is_cancel": [False, True],
        }
    ).write_parquet(sample_path)
    CliRunner().invoke(
        app,
        ["profile-l2-sample", str(sample_path), "--mapping-output", str(mapping_path)],
    )
    CliRunner().invoke(
        app,
        ["import-l2-sample", str(sample_path), str(mapping_path), "--lake-root", str(lake_root)],
    )

    result = CliRunner().invoke(app, ["build-l2-features-from-lake", "--lake-root", str(lake_root)])

    assert result.exit_code == 0
    assert "features_rows=1" in result.output
    assert (lake_root / "intraday_l2_features" / "part-000.parquet").exists()


def test_go_live_readiness_blocks_without_required_evidence(tmp_path) -> None:
    output_path = tmp_path / "readiness.json"

    result = CliRunner().invoke(
        app,
        [
            "go-live-readiness",
            "--output-path",
            str(output_path),
            "--lake-root",
            str(tmp_path / "lake"),
            "--tushare-token",
            "",
            "--ifind-username",
            "",
            "--ifind-password",
            "",
        ],
    )

    assert result.exit_code == 0
    assert "go_live_approved=false" in result.output
    manifest = json.loads(output_path.read_text(encoding="utf-8"))
    assert manifest["go_live_approved"] is False
    assert manifest["account_risk_budget"]["account_equity"] == 1_000_000
    assert manifest["data_freshness"][0]["status"] == "missing"
    assert "missing_tushare_token" in manifest["data_sources"][0]["failed_reasons"]
    assert (
        "missing_external_factor_portfolio"
        in manifest["external_factor_portfolio"]["failed_reasons"]
    )
    assert "not_evaluated" in manifest["paper_to_live"]["failed_reasons"]


def test_go_live_readiness_approves_when_all_gates_have_evidence(tmp_path) -> None:
    output_path = tmp_path / "readiness.json"
    lake_root = tmp_path / "lake"
    execution_log = tmp_path / "execution_log.jsonl"
    futu_report = tmp_path / "futu.json"
    signal_rows = [
        {
            "symbol": "00700.HK",
            "trade_date": f"2026-04-{day:02d}",
            "signal": 1,
        }
        for day in range(1, 21)
        for _ in range(4)
    ]
    datasets = {
        "security_master": pl.DataFrame({"symbol": ["00700.HK"], "market": ["HK"]}),
        "daily_features": pl.DataFrame({"symbol": ["00700.HK"], "feature": [1.0]}),
        "intraday_l2_features": pl.DataFrame({"symbol": ["00700.HK"], "imbalance": [0.1]}),
        "ifind_events": pl.DataFrame(
            {"symbol": ["00700.HK"], "data_source": ["ifind_real_file"]}
        ),
        "signal_events": pl.DataFrame(signal_rows),
        "external_factor_portfolio": pl.DataFrame(
            {
                "factor_name": ["factor_a"],
                "candidate_status": ["review_candidate"],
                "target_notional": [25_000.0],
            }
        ),
    }
    for dataset, frame in datasets.items():
        path = lake_root / dataset
        path.mkdir(parents=True)
        frame.write_parquet(path / "part-000.parquet")
    execution_log.write_text(
        "\n".join(
            json.dumps(
                {
                    "order_id": f"ord-{idx}",
                    "status": "filled",
                    "quantity": 100,
                    "fill_price": 320.5,
                    "slippage": 0.01,
                }
            )
            for idx in range(20)
        )
        + "\n",
        encoding="utf-8",
    )
    futu_report.write_text(
        json.dumps(
            [
                {
                    "local_order_id": f"ord-{idx}",
                    "order_id": f"futu-{idx}",
                    "order_status": "FILLED_ALL",
                    "dealt_qty": 100,
                    "dealt_avg_price": 320.5,
                }
                for idx in range(20)
            ]
        ),
        encoding="utf-8",
    )

    result = CliRunner().invoke(
        app,
        [
            "go-live-readiness",
            "--output-path",
            str(output_path),
            "--lake-root",
            str(lake_root),
            "--tushare-token",
            "token",
            "--ifind-username",
            "user",
            "--ifind-password",
            "password",
            "--tushare-adapter-status",
            "real_adapter",
            "--ifind-adapter-status",
            "real_adapter",
            "--opend-env",
            "REAL",
            "--opend-mode",
            "live",
            "--unlock-password-set",
            "--manual-live-enable",
            "--no-lookahead-audit-passed",
            "--cost-model-applied",
            "--capacity-check-passed",
            "--execution-log-path",
            str(execution_log),
            "--futu-report-path",
            str(futu_report),
            "--paper-sessions",
            "20",
        ],
    )

    assert result.exit_code == 0
    assert "go_live_approved=true" in result.output
    manifest = json.loads(output_path.read_text(encoding="utf-8"))
    assert manifest["go_live_approved"] is True
    assert manifest["research_to_paper"]["approved"] is True
    assert manifest["external_factor_portfolio"]["approved"] is True
    assert manifest["paper_to_live"]["approved"] is True
    assert manifest["execution"]["opend_ready"] is True
    assert all(adapter["live_data_ready"] for adapter in manifest["source_adapters"])
    assert manifest["account_risk_budget"]["max_strategy_notional"] == 200_000


def test_go_live_readiness_blocks_ifind_real_adapter_without_real_lake_rows(
    tmp_path,
) -> None:
    output_path = tmp_path / "readiness.json"
    lake_root = tmp_path / "lake"
    path = lake_root / "ifind_events"
    path.mkdir(parents=True)
    pl.DataFrame({"symbol": ["00700.HK"], "data_source": ["ifind_stub"]}).write_parquet(
        path / "part-000.parquet"
    )

    result = CliRunner().invoke(
        app,
        [
            "go-live-readiness",
            "--output-path",
            str(output_path),
            "--lake-root",
            str(lake_root),
            "--tushare-token",
            "token",
            "--ifind-access-token",
            "access",
            "--ifind-refresh-token",
            "refresh",
            "--ifind-adapter-status",
            "real_file_adapter",
        ],
    )

    assert result.exit_code == 0
    manifest = json.loads(output_path.read_text(encoding="utf-8"))
    ifind_adapter = next(
        adapter for adapter in manifest["source_adapters"] if adapter["source"] == "ifind"
    )
    assert ifind_adapter["live_data_ready"] is False
    assert (
        "ifind_real_file_adapter_missing_lake_data"
        in ifind_adapter["failed_reasons"]
    )


def test_objective_audit_cli_writes_completion_gap_report(tmp_path) -> None:
    readiness = tmp_path / "readiness.json"
    output = tmp_path / "objective_audit.json"
    readiness.write_text(
        json.dumps(
            {
                "go_live_approved": False,
                "account_risk_budget": {"account_equity": 1_000_000},
                "data_sources": [],
                "source_adapters": [],
                "data_freshness": [],
                "execution": {"opend_ready": False},
                "research_to_paper": {"approved": False},
                "paper_to_live": {"approved": False},
            }
        ),
        encoding="utf-8",
    )

    result = CliRunner().invoke(
        app,
        [
            "objective-audit",
            "--readiness-manifest-path",
            str(readiness),
            "--output-path",
            str(output),
        ],
    )

    assert result.exit_code == 0
    assert "objective_achieved=false" in result.output
    assert "profitable_reconciled_paper_or_live_evidence" in result.output
    audit = json.loads(output.read_text(encoding="utf-8"))
    assert audit["objective_achieved"] is False


def test_objective_audit_cli_accepts_paper_ledger_paths(tmp_path) -> None:
    readiness = tmp_path / "readiness.json"
    execution_log = tmp_path / "execution.jsonl"
    broker_report = tmp_path / "broker.json"
    blocker = tmp_path / "paper_blocker.json"
    handoff = tmp_path / "paper_handoff.json"
    progress = tmp_path / "paper_progress.json"
    output = tmp_path / "objective_audit.json"
    readiness.write_text(
        json.dumps(
            {
                "go_live_approved": False,
                "account_risk_budget": {"account_equity": 1_000_000},
                "data_sources": [],
                "source_adapters": [],
                "data_freshness": [],
                "execution": {"opend_ready": False},
                "research_to_paper": {"approved": False},
                "paper_to_live": {"approved": False},
            }
        ),
        encoding="utf-8",
    )
    execution_log.write_text(
        json.dumps({"order_id": "ord-1", "trade_date": "2026-05-05", "dry_run": False})
        + "\n",
        encoding="utf-8",
    )
    broker_report.write_text(
        json.dumps([{"local_order_id": "ord-1", "updated_time": "2026-05-05 10:00:00"}]),
        encoding="utf-8",
    )
    blocker.write_text(
        json.dumps(
            {
                "ready_for_next_session": False,
                "next_session_failed_reasons": ["opend_kill_switch_enabled"],
                "next_required_action": (
                    "clear_opend_kill_switch_then_resubmit_paper_simulate"
                ),
            }
        ),
        encoding="utf-8",
    )
    handoff.write_text(
        json.dumps(
            {
                "paper_blocker_report_path": str(blocker),
                "status": "manual_operator_authorization_required",
                "manual_authorization_required": True,
                "remediation_automation_allowed": False,
                "order_submission_allowed": False,
            }
        ),
        encoding="utf-8",
    )
    progress.write_text(
        json.dumps(
            {
                "ready_for_live_review": False,
                "sessions_remaining": 19,
                "failed_reasons": ["paper_sessions_remaining"],
            }
        ),
        encoding="utf-8",
    )

    result = CliRunner().invoke(
        app,
        [
            "objective-audit",
            "--readiness-manifest-path",
            str(readiness),
            "--output-path",
            str(output),
            "--execution-log-path",
            str(execution_log),
            "--broker-report-path",
            str(broker_report),
            "--paper-blocker-report-path",
            str(blocker),
            "--paper-operator-handoff-path",
            str(handoff),
            "--paper-progress-path",
            str(progress),
        ],
    )

    assert result.exit_code == 0
    audit = json.loads(output.read_text(encoding="utf-8"))
    paper_check = [
        check
        for check in audit["checks"]
        if check["requirement"] == "paper_to_live_execution_evidence"
    ][0]
    assert (
        paper_check["evidence"]["paper_session_ledger"]["inferred_session_count"]
        == 1
    )
    opend_check = [
        check for check in audit["checks"] if check["requirement"] == "opend_execution_gate"
    ][0]
    assert (
        opend_check["evidence"]["runtime"]["paper_blocker_report"][
            "next_required_action"
        ]
        == "clear_opend_kill_switch_then_resubmit_paper_simulate"
    )
    assert (
        opend_check["evidence"]["runtime"]["paper_operator_handoff"]["status"]
        == "manual_operator_authorization_required"
    )
    profit_check = [
        check
        for check in audit["checks"]
        if check["requirement"] == "profitable_reconciled_paper_or_live_evidence"
    ][0]
    assert profit_check["evidence"]["paper_progress"]["sessions_remaining"] == 19


def test_objective_audit_report_cli_writes_markdown(tmp_path) -> None:
    audit_path = tmp_path / "objective_audit.json"
    output_path = tmp_path / "objective_audit.md"
    audit_path.write_text(
        json.dumps(
            {
                "objective_achieved": False,
                "objective": "Build the platform.",
                "blocked_requirements": ["ifind_real_data_adapter"],
                "completion_decision": {
                    "status": "not_achieved",
                    "reason": "missing evidence",
                    "blocked_requirements": ["ifind_real_data_adapter"],
                },
                "prompt_to_artifact_checklist": [
                    {
                        "requirement": "ifind_real_data_adapter",
                        "status": "blocked",
                        "verification_command": "import-ifind-events-file",
                        "artifacts": ["data/lake/ifind_events"],
                        "failed_reasons": ["ifind_stub_adapter"],
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    result = CliRunner().invoke(
        app,
        [
            "objective-audit-report",
            "--audit-path",
            str(audit_path),
            "--output-path",
            str(output_path),
        ],
    )

    assert result.exit_code == 0
    assert "objective_audit_report=" in result.output
    content = output_path.read_text(encoding="utf-8")
    assert "Objective Completion Audit" in content
    assert "ifind_real_data_adapter" in content
    assert "Import a real iFind terminal export" in content


def test_profitability_evidence_cli_writes_pnl_report(tmp_path) -> None:
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

    result = CliRunner().invoke(
        app,
        [
            "profitability-evidence",
            "--execution-log-path",
            str(execution_log),
            "--broker-report-path",
            str(broker_report),
            "--output-path",
            str(output),
            "--paper-sessions",
            "20",
        ],
    )

    assert result.exit_code == 0
    assert "profitability_evidence_ready=true" in result.output
    assert "net_pnl=50.00" in result.output
    assert json.loads(output.read_text(encoding="utf-8"))["ready"] is True


def test_paper_session_ledger_cli_counts_broker_backed_sessions(tmp_path) -> None:
    execution_log = tmp_path / "execution.jsonl"
    broker_report = tmp_path / "futu.json"
    output = tmp_path / "paper_session_ledger.json"
    execution_rows = []
    broker_rows = []
    for day in range(1, 21):
        order_id = f"ord-{day}"
        trade_date = f"2026-04-{day:02d}"
        execution_rows.append(
            {
                "order_id": order_id,
                "status": "filled",
                "quantity": 100,
                "fill_price": 8.0,
                "trade_date": trade_date,
                "dry_run": False,
            }
        )
        broker_rows.append(
            {
                "local_order_id": order_id,
                "order_status": "FILLED_ALL",
                "dealt_qty": 100,
                "dealt_avg_price": 8.0,
                "updated_time": f"{trade_date} 10:00:00",
            }
        )
    execution_log.write_text(
        "\n".join(json.dumps(row) for row in execution_rows) + "\n",
        encoding="utf-8",
    )
    broker_report.write_text(json.dumps(broker_rows), encoding="utf-8")

    result = CliRunner().invoke(
        app,
        [
            "paper-session-ledger",
            "--execution-log-path",
            str(execution_log),
            "--broker-report-path",
            str(broker_report),
            "--output-path",
            str(output),
        ],
    )

    assert result.exit_code == 0
    assert "ready_for_profitability_evidence=true" in result.output
    assert "inferred_session_count=20" in result.output
    report = json.loads(output.read_text(encoding="utf-8"))
    assert report["inferred_session_count"] == 20
    assert report["ready_for_profitability_evidence"] is True
    assert report["failed_reasons"] == []


def test_paper_session_ledger_cli_blocks_dry_run_and_sparse_dates(tmp_path) -> None:
    execution_log = tmp_path / "execution.jsonl"
    broker_report = tmp_path / "futu.json"
    output = tmp_path / "paper_session_ledger.json"
    execution_log.write_text(
        json.dumps(
            {
                "order_id": "ord-1",
                "status": "filled",
                "trade_date": "2026-04-01",
                "dry_run": True,
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
                    "updated_time": "2026-04-01 10:00:00",
                    "dry_run": True,
                }
            ]
        ),
        encoding="utf-8",
    )

    result = CliRunner().invoke(
        app,
        [
            "paper-session-ledger",
            "--execution-log-path",
            str(execution_log),
            "--broker-report-path",
            str(broker_report),
            "--output-path",
            str(output),
        ],
    )

    assert result.exit_code == 0
    assert "ready_for_profitability_evidence=false" in result.output
    assert "dry_run_rows_present" in result.output
    assert "insufficient_inferred_sessions" in result.output
    report = json.loads(output.read_text(encoding="utf-8"))
    assert report["dry_run_rows"] == 2


def test_paper_progress_cli_writes_progress_report(tmp_path) -> None:
    execution_log = tmp_path / "execution.jsonl"
    broker_report = tmp_path / "broker.json"
    profitability = tmp_path / "profitability.json"
    output = tmp_path / "paper_progress.json"
    execution_log.write_text(
        json.dumps({"order_id": "ord-1", "trade_date": "2026-04-01", "dry_run": False})
        + "\n",
        encoding="utf-8",
    )
    broker_report.write_text(
        json.dumps([{"local_order_id": "ord-1", "updated_time": "2026-04-01 10:00:00"}]),
        encoding="utf-8",
    )
    profitability.write_text(
        json.dumps(
            {
                "ready": False,
                "net_pnl": -25.0,
                "max_drawdown": -100.0,
                "cash_drawdown": -800.0,
                "reconciled": True,
                "failed_reasons": ["net_pnl_not_positive"],
            }
        ),
        encoding="utf-8",
    )

    result = CliRunner().invoke(
        app,
        [
            "paper-progress",
            "--execution-log-path",
            str(execution_log),
            "--broker-report-path",
            str(broker_report),
            "--profitability-evidence-path",
            str(profitability),
            "--output-path",
            str(output),
        ],
    )

    assert result.exit_code == 0
    assert "ready_for_live_review=false" in result.output
    assert "sessions_remaining=19" in result.output
    assert "net_pnl=-25.00" in result.output
    assert "cash_drawdown=-800.00" in result.output
    assert (
        "next_required_evidence=collect_19_broker_reconciled_paper_sessions,"
        "refresh_profitability_evidence_from_latest_ledger,"
        "continue_until_positive_reconciled_net_pnl"
    ) in result.output
    report = json.loads(output.read_text(encoding="utf-8"))
    assert report["sessions_remaining"] == 19
    assert report["cash_drawdown"] == -800.0
    assert report["session_dates"] == ["2026-04-01"]
    assert report["next_required_evidence"] == [
        "collect_19_broker_reconciled_paper_sessions",
        "refresh_profitability_evidence_from_latest_ledger",
        "continue_until_positive_reconciled_net_pnl",
    ]


def test_paper_session_calendar_cli_writes_next_action(tmp_path) -> None:
    execution_log = tmp_path / "execution.jsonl"
    broker_report = tmp_path / "broker.json"
    output = tmp_path / "paper_session_calendar.json"
    execution_log.write_text(
        json.dumps({"order_id": "ord-1", "trade_date": "2026-05-04", "dry_run": False})
        + "\n",
        encoding="utf-8",
    )
    broker_report.write_text(
        json.dumps([{"local_order_id": "ord-1", "updated_time": "2026-05-04 10:00:00"}]),
        encoding="utf-8",
    )

    result = CliRunner().invoke(
        app,
        [
            "paper-session-calendar",
            "--execution-log-path",
            str(execution_log),
            "--broker-report-path",
            str(broker_report),
            "--output-path",
            str(output),
            "--as-of-date",
            "2026-05-05",
        ],
    )

    assert result.exit_code == 0
    assert "next_required_action=collect_today_paper_session" in result.output
    assert "has_session_today=false" in result.output
    assert "is_weekday=true" in result.output
    assert "is_market_holiday=false" in result.output
    assert "is_trading_day=true" in result.output
    assert "next_collect_date=2026-05-05" in result.output
    report = json.loads(output.read_text(encoding="utf-8"))
    assert report["as_of_date"] == "2026-05-05"
    assert report["is_weekday"] is True
    assert report["is_market_holiday"] is False
    assert report["is_trading_day"] is True
    assert report["next_collect_date"] == "2026-05-05"
    assert report["sessions_remaining"] == 19


def test_paper_session_calendar_cli_waits_on_weekend(tmp_path) -> None:
    execution_log = tmp_path / "execution.jsonl"
    broker_report = tmp_path / "broker.json"
    output = tmp_path / "paper_session_calendar.json"
    execution_log.write_text(
        json.dumps({"order_id": "ord-1", "trade_date": "2026-05-04", "dry_run": False})
        + "\n",
        encoding="utf-8",
    )
    broker_report.write_text(
        json.dumps([{"local_order_id": "ord-1", "updated_time": "2026-05-04 10:00:00"}]),
        encoding="utf-8",
    )

    result = CliRunner().invoke(
        app,
        [
            "paper-session-calendar",
            "--execution-log-path",
            str(execution_log),
            "--broker-report-path",
            str(broker_report),
            "--output-path",
            str(output),
            "--as-of-date",
            "2026-05-09",
            "--require-collect-today",
        ],
    )

    assert result.exit_code == 1
    assert "next_required_action=wait_next_trade_date" in result.output
    assert "is_weekday=false" in result.output
    assert "is_trading_day=false" in result.output
    assert "next_collect_date=2026-05-11" in result.output
    assert "paper_session_calendar_not_collect_today:wait_next_trade_date" in result.output
    report = json.loads(output.read_text(encoding="utf-8"))
    assert report["as_of_date"] == "2026-05-09"
    assert report["is_weekday"] is False
    assert report["is_trading_day"] is False
    assert report["next_collect_date"] == "2026-05-11"


def test_paper_session_calendar_cli_waits_on_market_holiday(tmp_path) -> None:
    execution_log = tmp_path / "execution.jsonl"
    broker_report = tmp_path / "broker.json"
    output = tmp_path / "paper_session_calendar.json"
    execution_log.write_text(
        json.dumps({"order_id": "ord-1", "trade_date": "2026-05-04", "dry_run": False})
        + "\n",
        encoding="utf-8",
    )
    broker_report.write_text(
        json.dumps([{"local_order_id": "ord-1", "updated_time": "2026-05-04 10:00:00"}]),
        encoding="utf-8",
    )

    result = CliRunner().invoke(
        app,
        [
            "paper-session-calendar",
            "--execution-log-path",
            str(execution_log),
            "--broker-report-path",
            str(broker_report),
            "--output-path",
            str(output),
            "--as-of-date",
            "2026-05-06",
            "--market-holiday-dates",
            "2026-05-06",
            "--require-collect-today",
        ],
    )

    assert result.exit_code == 1
    assert "next_required_action=wait_next_trade_date" in result.output
    assert "is_weekday=true" in result.output
    assert "is_market_holiday=true" in result.output
    assert "is_trading_day=false" in result.output
    assert "next_collect_date=2026-05-07" in result.output
    assert "paper_session_calendar_not_collect_today:wait_next_trade_date" in result.output
    report = json.loads(output.read_text(encoding="utf-8"))
    assert report["as_of_date"] == "2026-05-06"
    assert report["is_market_holiday"] is True
    assert report["is_trading_day"] is False
    assert report["next_collect_date"] == "2026-05-07"


def test_paper_session_calendar_cli_reads_market_holiday_calendar_file(tmp_path) -> None:
    execution_log = tmp_path / "execution.jsonl"
    broker_report = tmp_path / "broker.json"
    holidays = tmp_path / "market_holidays.json"
    output = tmp_path / "paper_session_calendar.json"
    execution_log.write_text(
        json.dumps({"order_id": "ord-1", "trade_date": "2026-05-04", "dry_run": False})
        + "\n",
        encoding="utf-8",
    )
    broker_report.write_text(
        json.dumps([{"local_order_id": "ord-1", "updated_time": "2026-05-04 10:00:00"}]),
        encoding="utf-8",
    )
    holidays.write_text(
        json.dumps({"market_holidays": ["2026-05-06"]}),
        encoding="utf-8",
    )

    result = CliRunner().invoke(
        app,
        [
            "paper-session-calendar",
            "--execution-log-path",
            str(execution_log),
            "--broker-report-path",
            str(broker_report),
            "--output-path",
            str(output),
            "--as-of-date",
            "2026-05-06",
            "--market-holiday-calendar-path",
            str(holidays),
        ],
    )

    assert result.exit_code == 0
    assert "is_market_holiday=true" in result.output
    assert "is_trading_day=false" in result.output
    assert "next_collect_date=2026-05-07" in result.output
    report = json.loads(output.read_text(encoding="utf-8"))
    assert report["is_market_holiday"] is True


def test_paper_session_calendar_cli_blocks_when_today_already_collected(tmp_path) -> None:
    execution_log = tmp_path / "execution.jsonl"
    broker_report = tmp_path / "broker.json"
    output = tmp_path / "paper_session_calendar.json"
    execution_log.write_text(
        json.dumps({"order_id": "ord-1", "trade_date": "2026-05-05", "dry_run": False})
        + "\n",
        encoding="utf-8",
    )
    broker_report.write_text(
        json.dumps([{"local_order_id": "ord-1", "updated_time": "2026-05-05 10:00:00"}]),
        encoding="utf-8",
    )

    result = CliRunner().invoke(
        app,
        [
            "paper-session-calendar",
            "--execution-log-path",
            str(execution_log),
            "--broker-report-path",
            str(broker_report),
            "--output-path",
            str(output),
            "--as-of-date",
            "2026-05-05",
            "--require-collect-today",
        ],
    )

    assert result.exit_code == 1
    assert "next_required_action=wait_next_trade_date" in result.output
    assert "status=blocked" in result.output
    assert "paper_session_calendar_not_collect_today:wait_next_trade_date" in result.output


def test_paper_simulate_status_cli_blocks_dry_run_response(tmp_path) -> None:
    response_path = tmp_path / "responses.jsonl"
    output = tmp_path / "paper_simulate_status.json"
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

    result = CliRunner().invoke(
        app,
        [
            "paper-simulate-status",
            "--response-path",
            str(response_path),
            "--output-path",
            str(output),
        ],
    )

    assert result.exit_code == 0
    assert "ready_for_session_collection=false" in result.output
    assert "dry_run_rows=1" in result.output
    assert "missing_paper_simulate_responses" in result.output
    report = json.loads(output.read_text(encoding="utf-8"))
    assert report["ready_for_session_collection"] is False


def test_paper_blocker_report_cli_writes_aggregated_blockers(tmp_path) -> None:
    runtime = tmp_path / "runtime.json"
    paper_status = tmp_path / "paper_status.json"
    calendar = tmp_path / "calendar.json"
    progress = tmp_path / "progress.json"
    output = tmp_path / "blockers.json"
    runtime.write_text(
        json.dumps(
            {
                "kill_switch": True,
                "kill_switch_file": "/tmp/futu-opend-execution.KILL",
                "ready_for_order_submission": False,
                "failed_reasons": ["opend_kill_switch_enabled"],
            }
        ),
        encoding="utf-8",
    )
    paper_status.write_text(
        json.dumps(
            {
                "ready_for_session_collection": False,
                "failed_reasons": ["missing_submitted_responses"],
            }
        ),
        encoding="utf-8",
    )
    progress.write_text(
        json.dumps(
            {
                "ready_for_live_review": False,
                "sessions_remaining": 19,
                "next_required_evidence": [
                    "collect_19_broker_reconciled_paper_sessions",
                    "continue_until_positive_reconciled_net_pnl",
                ],
                "failed_reasons": ["paper_sessions_remaining"],
            }
        ),
        encoding="utf-8",
    )
    calendar.write_text(
        json.dumps(
            {
                "next_required_action": "wait_next_trade_date",
                "next_collect_date": "2026-05-11",
            }
        ),
        encoding="utf-8",
    )

    result = CliRunner().invoke(
        app,
        [
            "paper-blocker-report",
            "--output-path",
            str(output),
            "--opend-runtime-status-path",
            str(runtime),
            "--paper-simulate-status-path",
            str(paper_status),
            "--paper-session-calendar-path",
            str(calendar),
            "--paper-progress-path",
            str(progress),
        ],
    )

    assert result.exit_code == 0
    assert "ready_for_next_session=false" in result.output
    assert (
        "next_session_failed_reasons=opend_kill_switch_enabled,missing_submitted_responses"
        in result.output
    )
    assert "opend_kill_switch_enabled" in result.output
    assert "missing_submitted_responses" in result.output
    assert (
        "next_required_evidence=collect_19_broker_reconciled_paper_sessions,"
        "continue_until_positive_reconciled_net_pnl"
    ) in result.output
    assert "next_collect_date=2026-05-11" in result.output
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["failed_reasons"] == [
        "opend_kill_switch_enabled",
        "missing_submitted_responses",
        "paper_calendar_action:wait_next_trade_date",
        "paper_sessions_remaining",
    ]
    assert payload["next_session_failed_reasons"] == [
        "opend_kill_switch_enabled",
        "missing_submitted_responses",
        "paper_calendar_action:wait_next_trade_date",
    ]
    assert payload["next_collect_date"] == "2026-05-11"
    assert payload["next_required_evidence"] == [
        "collect_19_broker_reconciled_paper_sessions",
        "continue_until_positive_reconciled_net_pnl",
    ]
    assert payload["blocker_details"]["opend_kill_switch"] == {
        "enabled": True,
        "kill_switch_file": "/tmp/futu-opend-execution.KILL",
        "requires_manual_operator_authorization": True,
        "automation_allowed": False,
        "next_safe_action": "operator_must_explicitly_clear_kill_switch_before_resubmit",
    }


def test_paper_operator_handoff_cli_writes_manual_kill_switch_handoff(tmp_path) -> None:
    blocker = tmp_path / "paper_blocker.json"
    output = tmp_path / "handoff.json"
    blocker.write_text(
        json.dumps(
            {
                "ready_for_next_session": False,
                "next_required_action": (
                    "clear_opend_kill_switch_then_resubmit_paper_simulate"
                ),
                "failed_reasons": ["opend_kill_switch_enabled"],
                "next_session_failed_reasons": ["opend_kill_switch_enabled"],
                "next_required_evidence": [
                    "collect_19_broker_reconciled_paper_sessions",
                    "continue_until_positive_reconciled_net_pnl",
                ],
                "next_collect_date": "2026-05-11",
                "blocker_details": {
                    "opend_kill_switch": {
                        "enabled": True,
                        "kill_switch_file": "/tmp/futu-opend-execution.KILL",
                        "requires_manual_operator_authorization": True,
                        "automation_allowed": False,
                        "next_safe_action": (
                            "operator_must_explicitly_clear_kill_switch_before_resubmit"
                        ),
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    result = CliRunner().invoke(
        app,
        [
            "paper-operator-handoff",
            "--paper-blocker-report-path",
            str(blocker),
            "--output-path",
            str(output),
        ],
    )

    assert result.exit_code == 0
    assert "status=manual_operator_authorization_required" in result.output
    assert "manual_authorization_required=true" in result.output
    assert "remediation_automation_allowed=false" in result.output
    assert "order_submission_allowed=false" in result.output
    assert "opend_kill_switch_enabled" in result.output
    assert (
        "next_required_evidence=collect_19_broker_reconciled_paper_sessions,"
        "continue_until_positive_reconciled_net_pnl"
    ) in result.output
    assert "next_collect_date=2026-05-11" in result.output
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["status"] == "manual_operator_authorization_required"
    assert payload["manual_authorization_required"] is True
    assert payload["remediation_automation_allowed"] is False
    assert payload["order_submission_allowed"] is False
    assert payload["next_required_evidence"] == [
        "collect_19_broker_reconciled_paper_sessions",
        "continue_until_positive_reconciled_net_pnl",
    ]
    assert payload["next_collect_date"] == "2026-05-11"
    assert "do_not_clear_kill_switch_from_automation" in payload["prohibited_actions"]


def test_combine_paper_evidence_cli_writes_combined_files(tmp_path) -> None:
    execution = tmp_path / "execution.jsonl"
    broker = tmp_path / "broker.json"
    output_execution = tmp_path / "combined.jsonl"
    output_broker = tmp_path / "combined.json"
    execution.write_text(
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
    broker.write_text(
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

    result = CliRunner().invoke(
        app,
        [
            "combine-paper-evidence",
            str(execution),
            str(broker),
            "--output-execution-log-path",
            str(output_execution),
            "--output-broker-report-path",
            str(output_broker),
        ],
    )

    assert result.exit_code == 0
    assert "status=ready" in result.output
    assert "execution_log_rows=1" in result.output
    assert json.loads(output_broker.read_text(encoding="utf-8"))[0]["local_order_id"] == "ord-1"


def test_build_mark_prices_from_opend_quote_cli_writes_marks(tmp_path) -> None:
    quote = tmp_path / "quote.json"
    output = tmp_path / "marks.json"
    quote.write_text(
        json.dumps({"quote": {"symbol": "HK.00001", "last_price": 65.05, "lot_size": 500}}),
        encoding="utf-8",
    )

    result = CliRunner().invoke(
        app,
        [
            "build-mark-prices-from-opend-quote",
            "--quote-snapshot-path",
            str(quote),
            "--output-path",
            str(output),
        ],
    )

    assert result.exit_code == 0
    assert "status=ready" in result.output
    assert "mark_price_count=1" in result.output
    assert json.loads(output.read_text(encoding="utf-8")) == {"HK.00001": 65.05}


def test_build_mark_prices_from_opend_quote_cli_blocks_bad_quote(tmp_path) -> None:
    quote = tmp_path / "quote.json"
    output = tmp_path / "marks.json"
    quote.write_text(json.dumps({"quote": {"symbol": "HK.00001"}}), encoding="utf-8")

    result = CliRunner().invoke(
        app,
        [
            "build-mark-prices-from-opend-quote",
            "--quote-snapshot-path",
            str(quote),
            "--output-path",
            str(output),
        ],
    )

    assert result.exit_code == 0
    assert "status=blocked" in result.output
    assert "mark_prices_from_opend_quote_failed:ValueError" in result.output
    assert not output.exists()
