from datetime import UTC, datetime

import polars as pl

from multi_layer_trading_lab.contracts import DAILY_FEATURES_CONTRACT, validate_dataset
from multi_layer_trading_lab.storage.registry import FeatureSetSpec, build_feature_registry


def test_contract_validation_detects_missing_required_column():
    frame = pl.DataFrame(
        {
            "security_id": ["HK.00700"],
            "market": ["HK"],
            "close": [100.0],
            "feature_set_version": ["daily_v2"],
            "data_source": ["tushare_stub"],
            "source_dataset": ["daily_bars"],
            "computed_at": [datetime.now(UTC)],
        }
    )
    result = validate_dataset(frame, DAILY_FEATURES_CONTRACT)
    assert not result.passed
    assert "as_of_date" in result.missing_columns


def test_feature_registry_builder_returns_expected_columns():
    frame = build_feature_registry(
        [
            FeatureSetSpec(
                feature_set_version="daily_v2",
                feature_domain="daily",
                description="Daily features",
                feature_columns=("ret_1d", "ret_5d"),
                source_tables=("daily_bars",),
            )
        ]
    )
    assert "feature_set_version" in frame.columns
    assert frame.height == 1
