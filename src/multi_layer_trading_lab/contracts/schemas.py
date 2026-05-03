from __future__ import annotations

from dataclasses import dataclass, field

import polars as pl


@dataclass(frozen=True)
class ColumnSpec:
    name: str
    kind: str
    required: bool = True


@dataclass(frozen=True)
class DatasetContract:
    name: str
    columns: tuple[ColumnSpec, ...]
    primary_key: tuple[str, ...] = ()

    @property
    def required_columns(self) -> tuple[str, ...]:
        return tuple(column.name for column in self.columns if column.required)


@dataclass
class ContractValidationResult:
    dataset: str
    passed: bool
    missing_columns: list[str] = field(default_factory=list)
    type_mismatches: list[str] = field(default_factory=list)
    duplicate_key_rows: int = 0

    def as_dict(self) -> dict[str, object]:
        return {
            "dataset": self.dataset,
            "passed": self.passed,
            "missing_columns": ",".join(self.missing_columns),
            "type_mismatches": ",".join(self.type_mismatches),
            "duplicate_key_rows": self.duplicate_key_rows,
        }


def _dtype_matches(dtype: pl.DataType, expected_kind: str) -> bool:
    if expected_kind == "string":
        return dtype == pl.Utf8 or dtype == pl.String
    if expected_kind == "date":
        return dtype == pl.Date
    if expected_kind == "datetime":
        return dtype.base_type() == pl.Datetime
    if expected_kind == "bool":
        return dtype == pl.Boolean
    if expected_kind == "int":
        return dtype.is_integer()
    if expected_kind == "float":
        return dtype.is_float() or dtype.is_integer()
    return True


def validate_dataset(frame: pl.DataFrame, contract: DatasetContract) -> ContractValidationResult:
    missing = [column for column in contract.required_columns if column not in frame.columns]
    type_mismatches: list[str] = []
    for column in contract.columns:
        if column.name not in frame.columns:
            continue
        dtype = frame.schema[column.name]
        if not _dtype_matches(dtype, column.kind):
            type_mismatches.append(f"{column.name}:{dtype}->{column.kind}")

    duplicate_key_rows = 0
    if contract.primary_key and all(column in frame.columns for column in contract.primary_key):
        duplicate_key_rows = frame.height - frame.unique(list(contract.primary_key)).height

    passed = not missing and not type_mismatches and duplicate_key_rows == 0
    return ContractValidationResult(
        dataset=contract.name,
        passed=passed,
        missing_columns=missing,
        type_mismatches=type_mismatches,
        duplicate_key_rows=duplicate_key_rows,
    )


SECURITY_MASTER_CONTRACT = DatasetContract(
    name="security_master",
    columns=(
        ColumnSpec("security_id", "string"),
        ColumnSpec("ticker", "string"),
        ColumnSpec("market", "string"),
        ColumnSpec("exchange", "string"),
        ColumnSpec("asset_type", "string"),
        ColumnSpec("currency", "string"),
        ColumnSpec("active_flag", "bool"),
        ColumnSpec("data_source", "string"),
        ColumnSpec("source_dataset", "string"),
        ColumnSpec("ingested_at", "datetime"),
    ),
    primary_key=("security_id",),
)

DAILY_FEATURES_CONTRACT = DatasetContract(
    name="daily_features",
    columns=(
        ColumnSpec("security_id", "string"),
        ColumnSpec("market", "string"),
        ColumnSpec("as_of_date", "date"),
        ColumnSpec("close", "float"),
        ColumnSpec("ret_1d", "float", required=False),
        ColumnSpec("ret_5d", "float", required=False),
        ColumnSpec("ret_20d", "float", required=False),
        ColumnSpec("realized_vol_5d", "float", required=False),
        ColumnSpec("realized_vol_20d", "float", required=False),
        ColumnSpec("turnover_rate", "float", required=False),
        ColumnSpec("volume_ratio_5d", "float", required=False),
        ColumnSpec("feature_set_version", "string"),
        ColumnSpec("data_source", "string"),
        ColumnSpec("source_dataset", "string"),
        ColumnSpec("computed_at", "datetime"),
    ),
    primary_key=("security_id", "as_of_date", "feature_set_version"),
)

INTRADAY_L2_FEATURES_CONTRACT = DatasetContract(
    name="intraday_l2_features",
    columns=(
        ColumnSpec("security_id", "string"),
        ColumnSpec("market", "string"),
        ColumnSpec("trade_date", "date"),
        ColumnSpec("bucket_size", "string"),
        ColumnSpec("bar_start_ts", "datetime"),
        ColumnSpec("bar_end_ts", "datetime"),
        ColumnSpec("mid_price_close", "float", required=False),
        ColumnSpec("bid_ask_imbalance", "float", required=False),
        ColumnSpec("trade_imbalance", "float", required=False),
        ColumnSpec("cancel_rate_proxy", "float", required=False),
        ColumnSpec("feature_set_version", "string"),
        ColumnSpec("data_source", "string"),
        ColumnSpec("source_dataset", "string"),
        ColumnSpec("computed_at", "datetime"),
    ),
    primary_key=("security_id", "bar_start_ts", "bucket_size", "feature_set_version"),
)

SIGNAL_EVENTS_CONTRACT = DatasetContract(
    name="signal_events",
    columns=(
        ColumnSpec("signal_id", "string"),
        ColumnSpec("strategy_id", "string"),
        ColumnSpec("security_id", "string"),
        ColumnSpec("market", "string"),
        ColumnSpec("trade_date", "date"),
        ColumnSpec("event_ts", "datetime"),
        ColumnSpec("signal_type", "string"),
        ColumnSpec("side", "string"),
        ColumnSpec("data_source", "string"),
        ColumnSpec("created_at", "datetime"),
    ),
    primary_key=("signal_id",),
)

FEATURE_REGISTRY_CONTRACT = DatasetContract(
    name="feature_registry",
    columns=(
        ColumnSpec("feature_set_version", "string"),
        ColumnSpec("feature_domain", "string"),
        ColumnSpec("description", "string"),
        ColumnSpec("feature_columns", "string"),
        ColumnSpec("source_tables", "string"),
        ColumnSpec("created_at", "datetime"),
        ColumnSpec("updated_at", "datetime"),
    ),
    primary_key=("feature_set_version",),
)


def available_contracts() -> dict[str, DatasetContract]:
    return {
        SECURITY_MASTER_CONTRACT.name: SECURITY_MASTER_CONTRACT,
        DAILY_FEATURES_CONTRACT.name: DAILY_FEATURES_CONTRACT,
        INTRADAY_L2_FEATURES_CONTRACT.name: INTRADAY_L2_FEATURES_CONTRACT,
        SIGNAL_EVENTS_CONTRACT.name: SIGNAL_EVENTS_CONTRACT,
        FEATURE_REGISTRY_CONTRACT.name: FEATURE_REGISTRY_CONTRACT,
    }
