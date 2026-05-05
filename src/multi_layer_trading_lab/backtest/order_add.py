from __future__ import annotations

from dataclasses import dataclass

import polars as pl

from multi_layer_trading_lab.signals.order_add import build_order_add_signal_candidates


@dataclass(frozen=True, slots=True)
class OrderAddBacktestResult:
    trades: pl.DataFrame
    summary: pl.DataFrame


def _empty_summary() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "trade_count": [0],
            "win_rate": [0.0],
            "avg_gross_ret": [0.0],
            "avg_net_ret": [0.0],
            "total_net_ret": [0.0],
        }
    )


def backtest_order_add_candidates(
    candidates: pl.DataFrame,
    order_add_features: pl.DataFrame,
    *,
    horizon_buckets: int = 1,
    cost_bps: float = 15.0,
) -> OrderAddBacktestResult:
    if candidates.is_empty() or order_add_features.is_empty():
        return OrderAddBacktestResult(trades=pl.DataFrame(), summary=_empty_summary())

    feature_panel = (
        order_add_features.sort(["symbol", "bar_start_ts"])
        .with_columns(
            [
                pl.col("order_add_price_mean").shift(-horizon_buckets).over("symbol").alias(
                    "exit_price"
                ),
                pl.col("bar_start_ts").shift(-horizon_buckets).over("symbol").alias("exit_ts"),
            ]
        )
        .select(
            [
                "symbol",
                pl.col("bar_start_ts").alias("entry_ts"),
                pl.col("order_add_price_mean").alias("entry_price"),
                "exit_ts",
                "exit_price",
            ]
        )
    )
    prepared_candidates = candidates.with_columns(
        pl.col("event_ts").dt.replace_time_zone(None).alias("entry_ts")
    )
    trades = (
        prepared_candidates.join(feature_panel, on=["symbol", "entry_ts"], how="inner")
        .drop_nulls(["entry_price", "exit_price"])
        .with_columns(
            [
                ((pl.col("exit_price") / pl.col("entry_price")) - 1.0).alias("gross_ret"),
                (((pl.col("exit_price") / pl.col("entry_price")) - 1.0) - cost_bps / 10_000.0)
                .alias("net_ret"),
            ]
        )
    )
    if trades.is_empty():
        summary = _empty_summary()
    else:
        summary = trades.select(
            [
                pl.len().alias("trade_count"),
                (pl.col("net_ret") > 0).mean().alias("win_rate"),
                pl.col("gross_ret").mean().alias("avg_gross_ret"),
                pl.col("net_ret").mean().alias("avg_net_ret"),
                pl.col("net_ret").sum().alias("total_net_ret"),
            ]
        )
    return OrderAddBacktestResult(trades=trades, summary=summary)


def sweep_order_add_thresholds(
    order_add_features: pl.DataFrame,
    *,
    volume_thresholds: list[int],
    large_order_ratio_thresholds: list[float],
    horizon_buckets: int = 1,
    cost_bps: float = 15.0,
    planned_notional: float | None = None,
) -> pl.DataFrame:
    rows = []
    for volume_threshold in volume_thresholds:
        for ratio_threshold in large_order_ratio_thresholds:
            candidates = build_order_add_signal_candidates(
                order_add_features,
                min_order_add_volume=volume_threshold,
                min_large_order_ratio=ratio_threshold,
            )
            result = backtest_order_add_candidates(
                candidates,
                order_add_features,
                horizon_buckets=horizon_buckets,
                cost_bps=cost_bps,
            )
            summary = result.summary.row(0, named=True)
            rows.append(
                {
                    "min_order_add_volume": volume_threshold,
                    "min_large_order_ratio": ratio_threshold,
                    "planned_notional": float(planned_notional)
                    if planned_notional is not None
                    else float(volume_threshold),
                    "candidate_count": candidates.height,
                    **summary,
                }
            )
    if not rows:
        return pl.DataFrame()
    return pl.DataFrame(rows).sort(
        ["avg_net_ret", "total_net_ret", "trade_count"],
        descending=[True, True, True],
    )
