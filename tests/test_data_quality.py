from datetime import date

import polars as pl

from multi_layer_trading_lab.storage.quality import build_quality_report


def test_quality_report_detects_duplicates():
    frame = pl.DataFrame(
        {
            "security_id": ["HK.00700", "HK.00700"],
            "as_of_date": [date(2026, 4, 1), date(2026, 4, 1)],
            "feature_set_version": ["daily_v2", "daily_v2"],
        }
    )
    report = build_quality_report(
        "daily_features",
        frame,
        ("security_id", "as_of_date", "feature_set_version"),
    )
    assert report.duplicate_rows == 1
    assert report.quality_status == "review"
