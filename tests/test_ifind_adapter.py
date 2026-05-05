from __future__ import annotations

from datetime import date

import polars as pl

from multi_layer_trading_lab.adapters.ifind import IFindClient
from multi_layer_trading_lab.contracts import IFIND_EVENTS_CONTRACT, validate_dataset


def test_ifind_event_stub_matches_contract() -> None:
    frame = IFindClient().fetch_events(
        symbols=["00700.HK", "AAPL.US"],
        start=date(2026, 4, 1),
        end=date(2026, 4, 3),
    )

    result = validate_dataset(frame, IFIND_EVENTS_CONTRACT)

    assert result.passed
    assert frame.height == 2
    assert set(frame["event_source"].to_list()) == {"ifind"}


def test_ifind_real_provider_normalizes_events_to_contract() -> None:
    def provider(**kwargs):
        assert kwargs["symbols"] == ["00700.HK"]
        return [
            {
                "id": "evt-1",
                "symbol": "00700.HK",
                "date": "20260401",
                "type": "company_notice",
                "title": "notice",
                "importance": 4,
            }
        ]

    frame = IFindClient(use_real=True, event_provider=provider).fetch_events(
        symbols=["00700.HK"],
        start=date(2026, 4, 1),
        end=date(2026, 4, 3),
    )

    result = validate_dataset(frame, IFIND_EVENTS_CONTRACT)

    assert result.passed
    assert frame.height == 1
    assert frame["event_id"][0] == "evt-1"
    assert frame["data_source"][0] == "ifind_real"


def test_ifind_events_file_import_normalizes_real_export(tmp_path) -> None:
    export_path = tmp_path / "ifind_events.csv"
    pl.DataFrame(
        {
            "id": ["evt-1"],
            "thscode": ["00700.HK"],
            "ann_date": ["20260401"],
            "type": ["company_notice"],
            "title": ["notice"],
            "importance": [4],
        }
    ).write_csv(export_path)

    frame = IFindClient(use_real=True).load_events_file(
        export_path,
        source_run_id="manual-ifind-export",
    )
    result = validate_dataset(frame, IFIND_EVENTS_CONTRACT)

    assert result.passed
    assert frame.height == 1
    assert frame["event_id"][0] == "evt-1"
    assert frame["symbol"][0] == "00700.HK"
    assert frame["data_source"][0] == "ifind_real_file"
    assert frame["source_dataset"][0] == str(export_path)
    assert frame["source_run_id"][0] == "manual-ifind-export"


def test_ifind_events_template_round_trips_through_import(tmp_path) -> None:
    template_path = tmp_path / "ifind_events_template.csv"
    IFindClient.write_events_template(template_path)

    frame = IFindClient(use_real=True).load_events_file(
        template_path,
        source_run_id="template-smoke",
    )
    result = validate_dataset(frame, IFIND_EVENTS_CONTRACT)

    assert result.passed
    assert frame.height == 1
    assert frame["symbol"][0] == "00700.HK"
    assert frame["data_source"][0] == "ifind_real_file"


def test_ifind_events_file_import_accepts_common_chinese_headers(tmp_path) -> None:
    export_path = tmp_path / "ifind_cn_export.csv"
    pl.DataFrame(
        {
            "事件ID": ["evt-cn-1"],
            "证券代码": ["00700.HK"],
            "公告日期": ["20260401"],
            "事件类型": ["company_notice"],
            "公告标题": ["公告"],
            "重要性": [4],
        }
    ).write_csv(export_path)

    frame = IFindClient(use_real=True).load_events_file(
        export_path,
        source_run_id="ifind-cn-export",
    )
    result = validate_dataset(frame, IFIND_EVENTS_CONTRACT)

    assert result.passed
    assert frame.height == 1
    assert frame["event_id"][0] == "evt-cn-1"
    assert frame["symbol"][0] == "00700.HK"
    assert frame["event_title"][0] == "公告"
