from __future__ import annotations

from datetime import UTC, datetime

import polars as pl

from multi_layer_trading_lab.models.bayes import update_hit_rate
from multi_layer_trading_lab.models.risk import kelly_fraction


def build_demo_signals(daily_features: pl.DataFrame, l2_features: pl.DataFrame) -> pl.DataFrame:
    merged = daily_features.join(l2_features, on=["symbol", "trade_date"], how="inner")
    merged = merged.with_columns(
        (
            (pl.col("ret_1d") > 0)
            & (pl.col("volume_ratio") > 1)
            & (pl.col("bid_ask_imbalance_mean") > 0)
        ).cast(pl.Int8).alias("setup_hit")
    )
    posterior = update_hit_rate(merged["setup_hit"].to_list())
    budget = kelly_fraction(posterior.mean, payoff_ratio=1.5)
    ts = datetime.now(UTC)
    return merged.filter(pl.col("setup_hit") == 1).select(
        [
            pl.col("symbol"),
            pl.col("trade_date"),
            pl.lit("open_strength_setup").alias("signal_name"),
            pl.lit("BUY").alias("side"),
            pl.lit(posterior.mean).alias("posterior_mean"),
            pl.lit(budget["quarter_kelly"]).alias("risk_budget"),
            pl.lit(ts).alias("generated_at"),
            pl.lit("daily+l2_demo").alias("lineage"),
        ]
    )
