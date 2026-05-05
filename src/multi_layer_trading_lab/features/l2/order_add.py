from __future__ import annotations

from datetime import UTC, datetime

import polars as pl


def build_order_add_bucket_features(
    order_add_events: pl.DataFrame,
    bucket_size: str = "1m",
    large_order_volume: int = 10_000,
) -> pl.DataFrame:
    if order_add_events.is_empty():
        return pl.DataFrame()

    computed_at = datetime.now(UTC)
    bucket_every = {"1s": "1s", "5s": "5s", "30s": "30s", "1m": "1m"}[bucket_size]
    return (
        order_add_events.sort(["symbol", "ts"])
        .with_columns(
            [
                pl.col("symbol").str.replace(r"\.HK$", "").alias("ticker"),
                pl.lit("HK").alias("market"),
                pl.col("ts").dt.date().alias("trade_date"),
                pl.col("volume").cast(pl.Int64),
                pl.col("price").cast(pl.Float64),
                (pl.col("volume") >= large_order_volume).alias("large_order_flag"),
            ]
        )
        .group_by_dynamic("ts", every=bucket_every, group_by="symbol", closed="left")
        .agg(
            [
                pl.col("ticker").last().alias("ticker"),
                pl.col("market").last().alias("market"),
                pl.col("trade_date").last().alias("trade_date"),
                pl.len().alias("order_add_count"),
                pl.col("volume").sum().alias("order_add_volume"),
                pl.col("price").mean().alias("order_add_price_mean"),
                pl.col("price").min().alias("order_add_price_min"),
                pl.col("price").max().alias("order_add_price_max"),
                pl.col("level").mean().alias("order_add_level_mean"),
                pl.col("large_order_flag").mean().alias("large_order_ratio"),
                pl.col("ts").min().alias("event_ts_min"),
                pl.col("ts").max().alias("event_ts_max"),
            ]
        )
        .with_columns(
            [
                pl.concat_str([pl.lit("HK."), pl.col("ticker")]).alias("security_id"),
                pl.lit(bucket_size).alias("bucket_size"),
                pl.col("ts").alias("bar_start_ts"),
                (pl.col("ts") + pl.duration(minutes=1)).alias("bar_end_ts"),
                (pl.col("ts").dt.hour() == 9).alias("preopen_stat_flag"),
                pl.lit("order_add_v1").alias("feature_set_version"),
                pl.lit("hk_l2_local").alias("data_source"),
                pl.lit("raw_l2_order_add").alias("source_dataset"),
                pl.lit(computed_at).alias("computed_at"),
                pl.lit(computed_at).alias("ingested_at"),
            ]
        )
        .select(
            [
                "security_id",
                "symbol",
                "market",
                "trade_date",
                "bucket_size",
                "bar_start_ts",
                "bar_end_ts",
                "order_add_count",
                "order_add_volume",
                "order_add_price_mean",
                "order_add_price_min",
                "order_add_price_max",
                "order_add_level_mean",
                "large_order_ratio",
                "preopen_stat_flag",
                "feature_set_version",
                "data_source",
                "source_dataset",
                "event_ts_min",
                "event_ts_max",
                "computed_at",
                "ingested_at",
            ]
        )
        .sort(["symbol", "bar_start_ts"])
    )
