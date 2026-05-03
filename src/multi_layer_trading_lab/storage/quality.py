from __future__ import annotations

from dataclasses import dataclass

import polars as pl


@dataclass
class DatasetQualityReport:
    dataset: str
    row_count: int
    duplicate_rows: int
    null_ratio_max: float
    quality_status: str

    def as_dict(self) -> dict[str, object]:
        return {
            "dataset": self.dataset,
            "row_count": self.row_count,
            "duplicate_rows": self.duplicate_rows,
            "null_ratio_max": self.null_ratio_max,
            "quality_status": self.quality_status,
        }


def build_quality_report(
    dataset: str,
    frame: pl.DataFrame,
    primary_key: tuple[str, ...] = (),
    critical_columns: tuple[str, ...] = (),
) -> DatasetQualityReport:
    if frame.is_empty():
        return DatasetQualityReport(dataset, 0, 0, 1.0, "empty")

    duplicate_rows = 0
    if primary_key and all(column in frame.columns for column in primary_key):
        duplicate_rows = frame.height - frame.unique(list(primary_key)).height

    columns_to_check = [column for column in critical_columns if column in frame.columns]
    if not columns_to_check:
        columns_to_check = frame.columns
    null_ratio_max = max(
        (
            float(frame.select(pl.col(column).is_null().mean()).item())
            for column in columns_to_check
        ),
        default=0.0,
    )
    quality_status = "ok" if duplicate_rows == 0 and null_ratio_max < 0.9 else "review"
    return DatasetQualityReport(
        dataset,
        frame.height,
        duplicate_rows,
        null_ratio_max,
        quality_status,
    )


def reports_to_frame(reports: list[DatasetQualityReport]) -> pl.DataFrame:
    return pl.DataFrame([report.as_dict() for report in reports])
