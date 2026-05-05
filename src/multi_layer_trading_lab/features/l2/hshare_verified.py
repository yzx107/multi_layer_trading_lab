from __future__ import annotations

from datetime import UTC, datetime

import polars as pl

_BUCKET_EVERY = {
    "1s": "1s",
    "5s": "5s",
    "30s": "30s",
    "1m": "1m",
}
_BUCKET_DURATION = {
    "1s": pl.duration(seconds=1),
    "5s": pl.duration(seconds=5),
    "30s": pl.duration(seconds=30),
    "1m": pl.duration(minutes=1),
}


def build_hshare_verified_order_features(
    orders: pl.DataFrame,
    *,
    bucket_size: str = "1m",
    source_run_id: str = "hshare-verified-orders",
) -> pl.DataFrame:
    if bucket_size not in _BUCKET_EVERY:
        raise ValueError(f"unsupported bucket_size={bucket_size}")
    if orders.is_empty():
        return pl.DataFrame()

    computed_at = datetime.now(UTC)
    return (
        orders.drop_nulls(["instrument_key", "SendTime", "Price", "Volume"])
        .sort(["instrument_key", "SendTime"])
        .with_columns(
            [
                pl.col("instrument_key").cast(pl.Utf8),
                pl.col("Price").cast(pl.Float64),
                pl.col("Volume").cast(pl.Int64),
                pl.concat_str([pl.col("instrument_key"), pl.lit(".HK")]).alias("symbol"),
            ]
        )
        .group_by_dynamic(
            "SendTime",
            every=_BUCKET_EVERY[bucket_size],
            group_by="instrument_key",
            closed="left",
        )
        .agg(
            [
                pl.col("symbol").last().alias("symbol"),
                pl.col("date").last().alias("trade_date"),
                pl.col("Price").last().alias("mid_price_close"),
                pl.col("Price").mean().alias("order_price_mean"),
                pl.len().alias("event_count"),
                pl.col("Volume").sum().alias("order_volume"),
                (pl.col("Price") * pl.col("Volume")).sum().alias("trade_value"),
                pl.col("SendTime").min().alias("event_ts_min"),
                pl.col("SendTime").max().alias("event_ts_max"),
            ]
        )
        .rename({"SendTime": "bar_start_ts"})
        .with_columns(
            [
                pl.concat_str([pl.lit("HK."), pl.col("instrument_key")]).alias("security_id"),
                pl.lit("HK").alias("market"),
                pl.lit(bucket_size).alias("bucket_size"),
                (pl.col("bar_start_ts") + _BUCKET_DURATION[bucket_size]).alias("bar_end_ts"),
                pl.lit(None, dtype=pl.Float64).alias("bid_ask_imbalance"),
                pl.col("order_volume").cast(pl.Float64).alias("trade_imbalance"),
                pl.lit(None, dtype=pl.Float64).alias("cancel_rate_proxy"),
                pl.lit("hshare_verified_orders_v1").alias("feature_set_version"),
                pl.lit("hshare_verified").alias("data_source"),
                pl.lit("verified_orders").alias("source_dataset"),
                pl.lit(source_run_id).alias("source_run_id"),
                pl.lit(computed_at).alias("computed_at"),
                pl.lit(computed_at).alias("ingested_at"),
            ]
        )
        .with_columns(
            pl.col("mid_price_close").pct_change().over("instrument_key").alias("mid_ret")
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
                "mid_price_close",
                "mid_ret",
                "bid_ask_imbalance",
                "trade_imbalance",
                "cancel_rate_proxy",
                "event_count",
                "trade_value",
                "order_price_mean",
                "order_volume",
                "feature_set_version",
                "data_source",
                "source_dataset",
                "source_run_id",
                "event_ts_min",
                "event_ts_max",
                "computed_at",
                "ingested_at",
            ]
        )
        .sort(["symbol", "bar_start_ts"])
    )
