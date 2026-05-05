from __future__ import annotations

from datetime import UTC, datetime

import polars as pl


def build_order_add_signal_candidates(
    order_add_features: pl.DataFrame,
    *,
    min_order_add_volume: int = 20_000,
    min_large_order_ratio: float = 0.25,
) -> pl.DataFrame:
    if order_add_features.is_empty():
        return pl.DataFrame(
            schema={
                "security_id": pl.String,
                "symbol": pl.String,
                "market": pl.String,
                "trade_date": pl.Date,
                "event_ts": pl.Datetime(time_zone="UTC"),
                "strategy_id": pl.String,
                "side": pl.String,
                "score": pl.Float64,
                "order_add_volume": pl.Int64,
                "large_order_ratio": pl.Float64,
                "order_add_count": pl.UInt32,
            }
        )

    return (
        order_add_features.filter(
            (pl.col("order_add_volume") >= min_order_add_volume)
            & (pl.col("large_order_ratio") >= min_large_order_ratio)
        )
        .with_columns(
            [
                (
                    (pl.col("order_add_volume") / float(min_order_add_volume))
                    * pl.col("large_order_ratio")
                ).alias("score"),
                pl.col("bar_start_ts").dt.replace_time_zone("UTC").alias("event_ts"),
                pl.lit("order_add_pressure").alias("strategy_id"),
                pl.lit("buy").alias("side"),
            ]
        )
        .select(
            [
                "security_id",
                "symbol",
                "market",
                "trade_date",
                "event_ts",
                "strategy_id",
                "side",
                "score",
                "order_add_volume",
                "large_order_ratio",
                "order_add_count",
            ]
        )
        .sort(["symbol", "event_ts"])
    )


def order_add_candidates_to_signal_events(candidates: pl.DataFrame) -> pl.DataFrame:
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

    created_at = datetime.now(UTC)
    return candidates.select(
        [
            pl.concat_str(
                [
                    pl.col("security_id"),
                    pl.lit("-"),
                    pl.col("event_ts").dt.strftime("%Y%m%d%H%M%S"),
                    pl.lit("-order_add_pressure"),
                ]
            ).alias("signal_id"),
            pl.col("strategy_id"),
            pl.col("security_id"),
            pl.col("market"),
            pl.col("trade_date"),
            pl.col("event_ts"),
            pl.lit("entry").alias("signal_type"),
            pl.col("side"),
            pl.lit("l2_order_add_features").alias("data_source"),
            pl.lit(created_at).alias("created_at"),
        ]
    )
