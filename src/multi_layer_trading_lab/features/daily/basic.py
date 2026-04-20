from __future__ import annotations

import polars as pl


def build_daily_features(daily_bars: pl.DataFrame) -> pl.DataFrame:
    return (
        daily_bars.sort(["symbol", "trade_date"])
        .with_columns(
            [
                pl.col("close").pct_change().over("symbol").alias("ret_1d"),
                (pl.col("close") / pl.col("close").shift(5).over("symbol") - 1).alias("ret_5d"),
                (pl.col("volume") / pl.col("volume").rolling_mean(5).over("symbol")).alias("volume_ratio"),
                ((pl.col("high") - pl.col("low")) / pl.col("close")).alias("realized_range"),
                (pl.col("turnover") / pl.col("volume")).alias("avg_trade_value"),
                pl.lit(None, dtype=pl.Float64).alias("us_overnight_mapping"),
                pl.lit(None, dtype=pl.Float64).alias("southbound_flow_proxy"),
            ]
        )
        .with_columns(
            pl.col("ret_1d").rolling_std(5).over("symbol").alias("volatility_5d")
        )
    )
