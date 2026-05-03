from __future__ import annotations

from datetime import UTC, datetime

import polars as pl


def build_daily_features(daily_bars: pl.DataFrame) -> pl.DataFrame:
    computed_at = datetime.now(UTC)
    return (
        daily_bars.sort(["symbol", "trade_date"])
        .with_columns(
            [
                pl.col("close").pct_change().over("symbol").alias("ret_1d"),
                (pl.col("close") / pl.col("close").shift(5).over("symbol") - 1).alias("ret_5d"),
                (pl.col("close") / pl.col("close").shift(20).over("symbol") - 1).alias("ret_20d"),
                (pl.col("volume") / pl.col("volume").rolling_mean(5).over("symbol")).alias(
                    "volume_ratio_5d"
                ),
                ((pl.col("high") - pl.col("low")) / pl.col("close")).alias("realized_range"),
                (pl.col("turnover") / pl.col("volume")).alias("avg_trade_value"),
                ((pl.col("open") / pl.col("close").shift(1).over("symbol")) - 1).alias(
                    "gap_from_prev_close"
                ),
                pl.lit(None, dtype=pl.Float64).alias("us_overnight_mapping"),
                pl.lit(None, dtype=pl.Float64).alias("southbound_flow_proxy"),
                pl.lit(None, dtype=pl.Float64).alias("northbound_flow_proxy"),
                pl.col("trade_date").alias("as_of_date"),
                pl.lit("daily_v2").alias("feature_set_version"),
                pl.col("data_source"),
                pl.col("source_dataset"),
                pl.col("source_run_id"),
                pl.lit(computed_at).alias("computed_at"),
                pl.col("ingested_at"),
            ]
        )
        .with_columns(
            [
                pl.col("ret_1d").rolling_std(5).over("symbol").alias("volatility_5d"),
                pl.col("ret_1d").rolling_std(5).over("symbol").alias("realized_vol_5d"),
                pl.col("ret_1d").rolling_std(20).over("symbol").alias("realized_vol_20d"),
                pl.col("volume_ratio_5d").alias("volume_ratio"),
                pl.col("us_overnight_mapping").alias("us_overnight_lead_proxy"),
            ]
        )
    )
