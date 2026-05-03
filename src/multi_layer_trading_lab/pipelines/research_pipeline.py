from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl

from multi_layer_trading_lab.adapters.l2_loader.loader import L2Loader
from multi_layer_trading_lab.adapters.tushare.client import TushareClient
from multi_layer_trading_lab.contracts import available_contracts, validate_dataset
from multi_layer_trading_lab.features.daily.basic import build_daily_features
from multi_layer_trading_lab.features.intraday.basic import (
    build_intraday_bar_features,
    summarize_open_window,
)
from multi_layer_trading_lab.features.l2.basic import (
    build_l2_bucket_features,
    summarize_l2_bucket_features,
)
from multi_layer_trading_lab.labels import (
    add_horizon_labels,
    extract_event_outcomes,
    normalize_symbol_frame,
)
from multi_layer_trading_lab.models import (
    attach_setup_posteriors,
    batch_scan_lead_lag,
    summarize_setup_posteriors,
)
from multi_layer_trading_lab.pipelines.demo_pipeline import make_sample_l2_file
from multi_layer_trading_lab.reports import render_research_summary
from multi_layer_trading_lab.settings import settings
from multi_layer_trading_lab.storage import (
    DuckDBCatalog,
    FeatureSetSpec,
    ParquetStore,
    build_feature_registry,
    build_quality_report,
    reports_to_frame,
)


def _fetch_sample_daily_bars(client: TushareClient) -> pl.DataFrame:
    symbols = ["00700.HK", "AAPL.US", "600519.SH"]
    return pl.concat(
        [
            client.fetch_daily_bars(symbol, date(2026, 3, 10), date(2026, 4, 8))
            for symbol in symbols
        ],
        how="diagonal_relaxed",
    )


def _build_signal_contract_frame(panel: pl.DataFrame) -> pl.DataFrame:
    candidates = panel.filter(pl.col("setup_flag") == 1)
    if candidates.is_empty():
        return pl.DataFrame(
            schema={
                "signal_id": pl.String,
                "strategy_id": pl.String,
                "security_id": pl.String,
                "market": pl.String,
                "trade_date": pl.Date,
                "event_ts": pl.Datetime(time_zone="UTC"),
                "signal_type": pl.String,
                "side": pl.String,
                "data_source": pl.String,
                "created_at": pl.Datetime(time_zone="UTC"),
            }
        )
    return candidates.select(
        [
            pl.concat_str(
                [
                    pl.col("security_id"),
                    pl.lit("-"),
                    pl.col("trade_date").cast(pl.Utf8),
                    pl.lit("-open_strength"),
                ]
            ).alias("signal_id"),
            pl.lit("open_strength_setup").alias("strategy_id"),
            pl.col("security_id"),
            pl.col("market"),
            pl.col("trade_date"),
            pl.col("computed_at").alias("event_ts"),
            pl.lit("entry").alias("signal_type"),
            pl.lit("buy").alias("side"),
            pl.lit("research_workflow").alias("data_source"),
            pl.col("computed_at").alias("created_at"),
        ]
    )


def run_research_workflow(data_root: Path | None = None) -> dict[str, object]:
    root = data_root or settings.data_root
    store = ParquetStore(root / "lake")
    catalog = DuckDBCatalog(root / "catalog" / "research.duckdb")
    client = TushareClient(token=settings.tushare_token)

    security_master = pl.concat(
        [
            client.fetch_security_master("HK"),
            client.fetch_security_master("US"),
            client.fetch_security_master("CN"),
        ],
        how="diagonal_relaxed",
    )
    security_master = normalize_symbol_frame(
        security_master,
        symbol_col="symbol",
        output_col="symbol",
    )

    daily_bars = _fetch_sample_daily_bars(client)
    daily_features = build_daily_features(daily_bars)
    labeled_daily = add_horizon_labels(
        daily_features,
        price_col="close",
        horizons=[1, 3],
        group_cols=("security_id",),
    )
    posterior_base = labeled_daily.filter(pl.col("market") == "HK").with_columns(
        [
            (
                (pl.col("ret_1d") > 0)
                & (pl.col("volume_ratio_5d") > 1)
            )
            .cast(pl.Int8)
            .alias("setup_flag"),
            pl.when(pl.col("event_tag").is_not_null())
            .then(pl.lit("event_setup"))
            .when(pl.col("ret_1d") > 0)
            .then(pl.lit("momentum_setup"))
            .otherwise(pl.lit("baseline"))
            .alias("setup_id"),
        ]
    )

    minute_bars = client.fetch_minute_bars("00700.HK", date(2026, 4, 1), minutes=90)
    intraday_bar_features = build_intraday_bar_features(minute_bars)
    intraday_summary = summarize_open_window(intraday_bar_features)

    l2_root = root / "raw" / "l2"
    make_sample_l2_file(l2_root, symbol="00700.HK", trade_date="2026-04-01")
    loader = L2Loader(l2_root)
    l2_frame = loader.load_trade_date("2026-04-01")
    l2_agg = loader.aggregate(l2_frame, "1m")
    l2_bucket_features = build_l2_bucket_features(l2_agg)
    l2_session_summary = summarize_l2_bucket_features(l2_bucket_features)

    hk_panel = (
        posterior_base
        .join(intraday_summary, on=["symbol", "trade_date"], how="left")
        .join(l2_session_summary, on=["security_id", "symbol", "market", "trade_date"], how="left")
        .with_columns(
            [
                (
                    (pl.col("ret_1d") > 0)
                    & (pl.col("volume_ratio_5d") > 1)
                    & (pl.col("bid_ask_imbalance_mean") > 0)
                )
                .cast(pl.Int8)
                .alias("setup_flag"),
                pl.when(pl.col("setup_id") == "event_setup")
                .then(pl.lit("event_setup"))
                .when(pl.col("open_30m_return") > 0)
                .then(pl.lit("open_strength"))
                .otherwise(pl.col("setup_id"))
                .alias("setup_id"),
            ]
        )
    )

    posterior_panel = attach_setup_posteriors(
        posterior_base,
        label_col="label_up_1b",
        group_cols=("setup_id",),
    )
    posterior_summary = summarize_setup_posteriors(
        posterior_panel.filter(pl.col("setup_flag") == 1),
        label_col="label_up_1b",
        group_cols=("setup_id",),
    )

    event_outcomes = extract_event_outcomes(
        hk_panel.with_columns(pl.col("setup_flag").cast(pl.Boolean).alias("setup_event")),
        event_col="setup_event",
        price_col="close",
        horizon=3,
        upper_barrier=0.03,
        lower_barrier=-0.02,
        group_cols=("security_id",),
    )

    daily_lead_lag = batch_scan_lead_lag(
        labeled_daily.select(["ret_1d", "volume_ratio_5d", "ret_5d", "turnover_rate"]).drop_nulls(),
        column_pairs=[
            ("volume_ratio_5d", "ret_1d"),
            ("ret_5d", "ret_1d"),
            ("turnover_rate", "ret_1d"),
        ],
        max_lag=3,
    ).with_columns(pl.lit("daily").alias("domain"))
    l2_lead_lag = batch_scan_lead_lag(
        l2_bucket_features.select(
            ["mid_ret", "bid_ask_imbalance", "trade_imbalance", "spread_bps_avg"]
        ).drop_nulls(),
        column_pairs=[
            ("bid_ask_imbalance", "mid_ret"),
            ("trade_imbalance", "mid_ret"),
            ("spread_bps_avg", "mid_ret"),
        ],
        max_lag=3,
    ).with_columns(pl.lit("l2").alias("domain"))
    lead_lag_summary = pl.concat([daily_lead_lag, l2_lead_lag], how="diagonal_relaxed")

    feature_registry = build_feature_registry(
        [
            FeatureSetSpec(
                feature_set_version="daily_v2",
                feature_domain="daily",
                description="Daily research features with contract-aligned metadata.",
                feature_columns=tuple(
                    column for column in daily_features.columns if column not in {"source_run_id"}
                ),
                source_tables=("security_master", "daily_bars"),
            ),
            FeatureSetSpec(
                feature_set_version="intraday_v2",
                feature_domain="intraday",
                description="Minute-bar intraday features and open-window summaries.",
                feature_columns=tuple(column for column in intraday_bar_features.columns),
                source_tables=("minute_bars",),
                refresh_frequency="intraday_batch",
            ),
            FeatureSetSpec(
                feature_set_version="l2_v2",
                feature_domain="l2",
                description="Bucketed HK L2 microstructure features.",
                feature_columns=tuple(column for column in l2_bucket_features.columns),
                source_tables=("l2_tick_aggregated",),
                refresh_frequency="intraday_batch",
            ),
        ]
    )

    signal_events = _build_signal_contract_frame(posterior_panel)
    contracts = available_contracts()
    validations = [
        validate_dataset(security_master, contracts["security_master"]),
        validate_dataset(daily_features, contracts["daily_features"]),
        validate_dataset(l2_bucket_features, contracts["intraday_l2_features"]),
        validate_dataset(signal_events, contracts["signal_events"]),
        validate_dataset(feature_registry, contracts["feature_registry"]),
    ]
    validation_summary = pl.DataFrame([validation.as_dict() for validation in validations])

    quality_summary = reports_to_frame(
        [
            build_quality_report(
                "security_master",
                security_master,
                ("security_id",),
                critical_columns=contracts["security_master"].required_columns,
            ),
            build_quality_report(
                "daily_features",
                daily_features,
                ("security_id", "as_of_date", "feature_set_version"),
                critical_columns=contracts["daily_features"].required_columns,
            ),
            build_quality_report(
                "intraday_l2_features",
                l2_bucket_features,
                ("security_id", "bar_start_ts", "bucket_size", "feature_set_version"),
                critical_columns=contracts["intraday_l2_features"].required_columns,
            ),
            build_quality_report(
                "signal_events",
                signal_events,
                ("signal_id",),
                critical_columns=contracts["signal_events"].required_columns,
            ),
            build_quality_report(
                "feature_registry",
                feature_registry,
                ("feature_set_version",),
                critical_columns=contracts["feature_registry"].required_columns,
            ),
        ]
    )

    outputs: dict[str, object] = {
        "security_master": security_master,
        "daily_features": daily_features,
        "labeled_daily_features": labeled_daily,
        "intraday_bar_features": intraday_bar_features,
        "intraday_summary": intraday_summary,
        "intraday_l2_features": l2_bucket_features,
        "l2_session_summary": l2_session_summary,
        "posterior_panel": posterior_panel,
        "posterior_summary": posterior_summary,
        "lead_lag_summary": lead_lag_summary,
        "event_outcomes": event_outcomes,
        "feature_registry": feature_registry,
        "signal_events": signal_events,
        "validation_summary": validation_summary,
        "quality_summary": quality_summary,
    }

    for dataset, frame in outputs.items():
        if isinstance(frame, pl.DataFrame):
            path = store.write(dataset, frame)
            catalog.register_parquet(dataset, path)

    report_path = render_research_summary(
        root / "reports" / "research_summary.md",
        validation_summary=validation_summary,
        quality_summary=quality_summary,
        posterior_summary=posterior_summary,
        lead_lag_summary=lead_lag_summary,
    )
    outputs["research_report_path"] = report_path
    return outputs
