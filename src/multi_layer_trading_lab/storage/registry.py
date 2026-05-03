from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime

import polars as pl


@dataclass(frozen=True)
class FeatureSetSpec:
    feature_set_version: str
    feature_domain: str
    description: str
    feature_columns: tuple[str, ...]
    source_tables: tuple[str, ...]
    refresh_frequency: str = "daily"
    quality_status: str = "validated"
    owner: str = "research"


def build_feature_registry(specs: list[FeatureSetSpec]) -> pl.DataFrame:
    timestamp = datetime.now(UTC)
    return pl.DataFrame(
        [
            {
                "feature_set_version": spec.feature_set_version,
                "feature_domain": spec.feature_domain,
                "owner": spec.owner,
                "description": spec.description,
                "feature_columns": json.dumps(list(spec.feature_columns)),
                "source_tables": json.dumps(list(spec.source_tables)),
                "refresh_frequency": spec.refresh_frequency,
                "quality_status": spec.quality_status,
                "created_at": timestamp,
                "updated_at": timestamp,
            }
            for spec in specs
        ]
    )
