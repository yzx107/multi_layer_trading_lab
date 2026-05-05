from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from zipfile import ZipFile

import polars as pl

from multi_layer_trading_lab.adapters.l2_loader.profile import (
    build_order_add_coverage,
    discover_l2_zip_paths,
    find_order_add_member,
    list_l2_zip_members,
    load_l2_mapping,
    normalize_order_add_zip_batch,
    normalize_order_add_zip_member,
    profile_l2_file,
    read_l2_zip_member_sample,
    resolve_l2_zip_member,
    suggest_l2_mapping,
    write_l2_mapping_template,
)


def test_suggest_l2_mapping_recognizes_common_vendor_names() -> None:
    mapping = suggest_l2_mapping(
        ["code", "event_time", "bid1", "ask1", "bidvol1", "askvol1", "price", "volume"]
    )

    assert mapping["symbol"] == "code"
    assert mapping["ts"] == "event_time"
    assert mapping["bid_px_1"] == "bid1"
    assert mapping["ask_sz_1"] == "askvol1"


def test_profile_l2_file_reports_rows_and_missing_targets(tmp_path: Path) -> None:
    path = tmp_path / "sample.parquet"
    pl.DataFrame(
        {
            "code": ["00700.HK"],
            "event_time": [datetime(2026, 4, 1, 9, 30)],
            "bid1": [320.0],
            "ask1": [320.2],
            "bidvol1": [1000],
            "askvol1": [900],
            "price": [320.1],
            "volume": [500],
        }
    ).write_parquet(path)

    profile = profile_l2_file(path)

    assert profile.rows == 1
    assert profile.suggested_mapping["symbol"] == "code"
    assert "side" in profile.missing_targets
    assert "cancel_flag" in profile.missing_targets


def test_l2_mapping_template_round_trips_suggested_columns(tmp_path: Path) -> None:
    sample_path = tmp_path / "sample.parquet"
    pl.DataFrame(
        {
            "code": ["00700.HK"],
            "event_time": [datetime(2026, 4, 1, 9, 30)],
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

    profile = profile_l2_file(sample_path)
    mapping_path = write_l2_mapping_template(profile, tmp_path / "mapping.json")
    mapping = load_l2_mapping(mapping_path)

    assert mapping.symbol == "code"
    assert mapping.ts == "event_time"
    assert mapping.side == "bs_flag"
    assert mapping.cancel_flag == "is_cancel"


def test_l2_zip_member_sample_adds_symbol_and_trade_date(tmp_path: Path) -> None:
    zip_path = tmp_path / "20250123.zip"
    member = "20250123\\OrderAdd\\00005.csv"
    with ZipFile(zip_path, "w") as archive:
        archive.writestr(
            member,
            "SeqNum,OrderId,OrderType,Ext,Time,Price,Volume,Level\n"
            "0,13337603,1,110,92006,78.900,9600,0\n",
        )

    members = list_l2_zip_members(zip_path, category="OrderAdd")
    sample = read_l2_zip_member_sample(zip_path, members[0])

    assert members == (member,)
    assert sample.height == 1
    assert sample["symbol"].to_list() == ["00005.HK"]
    assert sample["trade_date"].to_list() == ["20250123"]
    assert suggest_l2_mapping(sample.columns)["last_px"] == "Price"
    assert resolve_l2_zip_member(zip_path, "20250123/OrderAdd/00005.csv") == member
    assert resolve_l2_zip_member(zip_path, "20250123\\\\OrderAdd\\\\00005.csv") == member
    assert find_order_add_member(zip_path, "00005.HK") == member
    assert find_order_add_member(zip_path, "5") == member


def test_normalize_order_add_zip_member_parses_timestamp(tmp_path: Path) -> None:
    zip_path = tmp_path / "20250123.zip"
    member = "20250123\\OrderAdd\\00005.csv"
    with ZipFile(zip_path, "w") as archive:
        archive.writestr(
            member,
            "SeqNum,OrderId,OrderType,Ext,Time,Price,Volume,Level,BrokerNo,VolumePre\n"
            "0,13337603,1,110,92006,78.900,9600,0,,0\n",
        )

    normalized = normalize_order_add_zip_member(zip_path, member)

    assert normalized.columns == [
        "symbol",
        "ts",
        "trade_date",
        "seq_num",
        "order_id",
        "order_type",
        "ext",
        "price",
        "volume",
        "level",
        "broker_no",
        "volume_pre",
    ]
    assert normalized["symbol"].to_list() == ["00005.HK"]
    assert normalized["ts"].to_list()[0] == datetime(2025, 1, 23, 9, 20, 6)


def test_normalize_order_add_zip_batch_concats_dates_and_symbols(tmp_path: Path) -> None:
    first_zip = tmp_path / "20250123.zip"
    second_zip = tmp_path / "20250124.zip"
    for zip_path in [first_zip, second_zip]:
        trade_date = zip_path.stem
        with ZipFile(zip_path, "w") as archive:
            archive.writestr(
                f"{trade_date}\\OrderAdd\\00005.csv",
                "SeqNum,OrderId,OrderType,Ext,Time,Price,Volume,Level,BrokerNo,VolumePre\n"
                "0,13337603,1,110,92006,78.900,9600,0,,0\n",
            )

    normalized = normalize_order_add_zip_batch(
        [first_zip, second_zip],
        ["00005.HK"],
    )

    assert normalized.height == 2
    assert normalized["trade_date"].to_list() == ["20250123", "20250124"]


def test_discover_l2_zip_paths_returns_existing_dates_only(tmp_path: Path) -> None:
    root = tmp_path / "ticks"
    year = root / "2025"
    year.mkdir(parents=True)
    first = year / "20250123.zip"
    second = year / "20250124.zip"
    first.write_bytes(b"placeholder")
    second.write_bytes(b"placeholder")

    paths = discover_l2_zip_paths(root, date(2025, 1, 22), date(2025, 1, 25))

    assert paths == (first, second)


def test_build_order_add_coverage_marks_missing_members(tmp_path: Path) -> None:
    zip_path = tmp_path / "20250123.zip"
    with ZipFile(zip_path, "w") as archive:
        archive.writestr(
            "20250123\\OrderAdd\\00005.csv",
            "SeqNum,OrderId,OrderType,Ext,Time,Price,Volume,Level\n"
            "0,13337603,1,110,92006,78.900,9600,0\n",
        )

    coverage = build_order_add_coverage([zip_path], ["00005.HK", "00006.HK"])

    assert coverage.height == 2
    assert coverage.filter(pl.col("symbol") == "00005.HK")["available"].to_list() == [True]
    assert coverage.filter(pl.col("symbol") == "00006.HK")["available"].to_list() == [False]
