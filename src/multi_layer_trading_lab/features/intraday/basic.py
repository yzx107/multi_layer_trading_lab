from __future__ import annotations

import polars as pl


def build_intraday_bar_features(minute_bars: pl.DataFrame) -> pl.DataFrame:
    base = minute_bars.sort(["symbol", "ts"]).with_columns(
        [
            pl.col("close").first().over(["symbol", pl.col("ts").dt.date()]).alias("open_ref"),
            pl.col("volume").rolling_mean(15).over("symbol").alias("volume_mean_15"),
            pl.col("close").pct_change().over("symbol").alias("ret_1m"),
        ]
    )
    return base.with_columns(
        [
            ((pl.col("close") / pl.col("open_ref")) - 1).alias("open_session_return"),
            (pl.col("volume") / pl.col("volume_mean_15")).alias("minute_volume_ratio"),
            pl.col("ret_1m").rolling_std(15).over("symbol").alias("minute_volatility_15"),
            ((pl.col("open") / pl.col("close").shift(1).over("symbol")) - 1).alias("gap_feature"),
            (pl.col("ret_1m") > 0).cast(pl.Int8).alias("continuation_flag"),
            (pl.col("ret_1m") < 0).cast(pl.Int8).alias("reversal_flag"),
        ]
    )


def summarize_open_window(features: pl.DataFrame) -> pl.DataFrame:
    with_mins = features.with_columns(
        ((pl.col("ts") - pl.col("ts").min().over(["symbol", pl.col("ts").dt.date()])).dt.total_minutes()).alias("minute_from_open")
    )
    return with_mins.group_by(["symbol", pl.col("ts").dt.date().alias("trade_date")]).agg(
        [
            pl.col("open_session_return").filter(pl.col("minute_from_open") <= 5).last().alias("open_5m_return"),
            pl.col("open_session_return").filter(pl.col("minute_from_open") <= 15).last().alias("open_15m_return"),
            pl.col("open_session_return").filter(pl.col("minute_from_open") <= 30).last().alias("open_30m_return"),
            pl.col("minute_volume_ratio").mean().alias("avg_minute_volume_ratio"),
            pl.col("minute_volatility_15").mean().alias("avg_minute_volatility"),
        ]
    )
