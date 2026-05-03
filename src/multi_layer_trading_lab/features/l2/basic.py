from __future__ import annotations

from datetime import UTC, datetime

import polars as pl


def build_l2_features(aggregated_l2: pl.DataFrame) -> pl.DataFrame:
    with_clock = aggregated_l2.with_columns(
        [
            pl.col("ts").dt.date().alias("trade_date"),
            pl.col("ts").dt.hour().alias("hour"),
            pl.col("ts").dt.minute().alias("minute"),
        ]
    )
    return with_clock.group_by(["symbol", "trade_date"]).agg(
        [
            pl.col("bid_ask_imbalance").mean().alias("bid_ask_imbalance_mean"),
            pl.col("trade_direction_imbalance").sum().alias("trade_direction_imbalance_sum"),
            pl.col("cancel_rate").mean().alias("cancel_rate_mean"),
            pl.col("spread_bps").mean().alias("spread_bps_mean"),
            pl.col("depth_summary").mean().alias("depth_summary_mean"),
            pl.col("bid_ask_imbalance")
            .filter(pl.col("minute") < 40)
            .mean()
            .alias("pre_open_imbalance"),
        ]
    )


def build_l2_bucket_features(aggregated_l2: pl.DataFrame, bucket_size: str = "1m") -> pl.DataFrame:
    computed_at = datetime.now(UTC)
    return (
        aggregated_l2.sort(["symbol", "ts"])
        .with_columns(
            [
                pl.col("symbol").str.replace(r"\.HK$", "").alias("ticker"),
                pl.lit("HK").alias("market"),
                pl.col("ts").dt.date().alias("trade_date"),
                pl.lit(bucket_size).alias("bucket_size"),
                pl.col("ts").alias("bar_start_ts"),
                pl.col("ts").alias("event_ts_min"),
                pl.col("ts").alias("event_ts_max"),
                pl.col("mid_close").shift(1).over("symbol").alias("mid_price_open"),
                pl.col("mid_close").alias("mid_price_close"),
                pl.col("mid_close").pct_change().over("symbol").alias("mid_ret"),
                pl.col("spread_bps").alias("spread_bps_avg"),
                pl.col("trade_direction_imbalance").alias("trade_imbalance"),
                pl.col("cancel_rate").alias("cancel_rate_proxy"),
                (pl.col("depth_summary") * 0.55).alias("depth_slope_bid"),
                (pl.col("depth_summary") * 0.45).alias("depth_slope_ask"),
                pl.col("depth_summary").alias("depth_total_top5"),
                (pl.col("ts").dt.hour() == 9).alias("preopen_stat_flag"),
                pl.lit("l2_v2").alias("feature_set_version"),
                pl.lit("hk_l2_local").alias("data_source"),
                pl.lit("l2_tick_aggregated").alias("source_dataset"),
                pl.lit("demo-l2-buckets").alias("source_run_id"),
                pl.lit(computed_at).alias("computed_at"),
            ]
        )
        .with_columns(
            [
                pl.concat_str([pl.lit("HK."), pl.col("ticker")]).alias("security_id"),
                (pl.col("bar_start_ts") + pl.duration(minutes=1)).alias("bar_end_ts"),
                (pl.col("mid_price_open").fill_null(pl.col("mid_price_close"))).alias("mid_price_open"),
                (pl.col("trade_imbalance").abs() * pl.col("mid_price_close")).alias("trade_value"),
                pl.col("trade_imbalance").abs().cast(pl.Int64).alias("event_count"),
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
                "mid_price_open",
                "mid_price_close",
                "mid_ret",
                "spread_bps_avg",
                "bid_ask_imbalance",
                "trade_imbalance",
                "cancel_rate_proxy",
                "depth_slope_bid",
                "depth_slope_ask",
                "depth_total_top5",
                "preopen_stat_flag",
                "event_count",
                "trade_value",
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
    )


def summarize_l2_bucket_features(bucket_features: pl.DataFrame) -> pl.DataFrame:
    return bucket_features.group_by(["security_id", "symbol", "market", "trade_date"]).agg(
        [
            pl.col("bid_ask_imbalance").mean().alias("bid_ask_imbalance_mean"),
            pl.col("trade_imbalance").sum().alias("trade_direction_imbalance_sum"),
            pl.col("cancel_rate_proxy").mean().alias("cancel_rate_mean"),
            pl.col("spread_bps_avg").mean().alias("spread_bps_mean"),
            pl.col("depth_total_top5").mean().alias("depth_summary_mean"),
            pl.col("bid_ask_imbalance").filter(pl.col("preopen_stat_flag")).mean().alias("pre_open_imbalance"),
        ]
    )
