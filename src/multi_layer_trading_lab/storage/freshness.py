from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

import polars as pl


@dataclass(frozen=True, slots=True)
class DatasetFreshness:
    dataset: str
    status: str
    rows: int = 0
    latest_modified_at: datetime | None = None
    path: Path | None = None

    def as_status_text(self) -> str:
        modified = (
            self.latest_modified_at.isoformat()
            if self.latest_modified_at is not None
            else "none"
        )
        return f"{self.status} rows={self.rows} modified_at={modified}"


def inspect_parquet_dataset(
    lake_root: Path,
    dataset: str,
    *,
    max_age: timedelta = timedelta(days=1),
    now: datetime | None = None,
) -> DatasetFreshness:
    current_time = now or datetime.now(UTC)
    dataset_path = lake_root / dataset
    files = sorted(dataset_path.glob("*.parquet")) if dataset_path.exists() else []
    if not files:
        return DatasetFreshness(dataset=dataset, status="missing", path=dataset_path)

    latest_file = max(files, key=lambda path: path.stat().st_mtime)
    latest_modified_at = datetime.fromtimestamp(latest_file.stat().st_mtime, tz=UTC)
    frames = [pl.read_parquet(file) for file in files]
    rows = sum(frame.height for frame in frames)

    if rows == 0:
        status = "empty"
    elif any(
        "data_source" in frame.columns
        and frame.select(pl.col("data_source").cast(pl.Utf8).str.ends_with("_stub").any()).item()
        for frame in frames
    ):
        status = "stub"
    elif current_time - latest_modified_at > max_age:
        status = "stale"
    else:
        status = "fresh"

    return DatasetFreshness(
        dataset=dataset,
        status=status,
        rows=rows,
        latest_modified_at=latest_modified_at,
        path=dataset_path,
    )


def build_freshness_status(
    lake_root: Path,
    datasets: list[str],
    *,
    max_age: timedelta = timedelta(days=1),
    now: datetime | None = None,
) -> dict[str, str]:
    return {
        dataset: inspect_parquet_dataset(
            lake_root,
            dataset,
            max_age=max_age,
            now=now,
        ).as_status_text()
        for dataset in datasets
    }
