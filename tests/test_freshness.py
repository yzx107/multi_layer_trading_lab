from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import polars as pl

from multi_layer_trading_lab.storage.freshness import (
    build_freshness_status,
    inspect_parquet_dataset,
)


def test_inspect_parquet_dataset_reports_missing(tmp_path: Path) -> None:
    result = inspect_parquet_dataset(tmp_path, "daily_features")

    assert result.status == "missing"
    assert result.rows == 0


def test_inspect_parquet_dataset_reports_fresh_rows(tmp_path: Path) -> None:
    dataset_path = tmp_path / "daily_features"
    dataset_path.mkdir()
    pl.DataFrame({"x": [1, 2]}).write_parquet(dataset_path / "part-000.parquet")

    result = inspect_parquet_dataset(
        tmp_path,
        "daily_features",
        now=datetime.now(UTC),
        max_age=timedelta(days=1),
    )

    assert result.status == "fresh"
    assert result.rows == 2
    assert result.latest_modified_at is not None


def test_build_freshness_status_returns_text_summary(tmp_path: Path) -> None:
    dataset_path = tmp_path / "security_master"
    dataset_path.mkdir()
    pl.DataFrame({"security_id": ["HK.00700"]}).write_parquet(dataset_path / "part-000.parquet")

    status = build_freshness_status(tmp_path, ["security_master", "ifind_events"])

    assert status["security_master"].startswith("fresh rows=1")
    assert status["ifind_events"].startswith("missing rows=0")


def test_inspect_parquet_dataset_marks_stub_data_source(tmp_path: Path) -> None:
    dataset_path = tmp_path / "ifind_events"
    dataset_path.mkdir()
    pl.DataFrame({"data_source": ["ifind_stub"], "x": [1]}).write_parquet(
        dataset_path / "part-000.parquet"
    )

    result = inspect_parquet_dataset(tmp_path, "ifind_events")

    assert result.status == "stub"
