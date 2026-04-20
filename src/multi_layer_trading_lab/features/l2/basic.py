from __future__ import annotations

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
            pl.col("bid_ask_imbalance").filter(pl.col("minute") < 40).mean().alias("pre_open_imbalance"),
        ]
    )
