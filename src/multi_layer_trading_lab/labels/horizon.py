from __future__ import annotations

from collections.abc import Iterable

import polars as pl


def _infer_order_col(frame: pl.DataFrame, group_cols: list[str]) -> str:
    for candidate in ("ts", "trade_date", "date", "timestamp"):
        if candidate in frame.columns and candidate not in group_cols:
            return candidate
    raise ValueError("unable to infer an order column; provide a time-like column such as ts")


def _normalize_horizons(horizons: int | Iterable[int]) -> list[int]:
    if isinstance(horizons, int):
        return [horizons]
    values = sorted({int(horizon) for horizon in horizons if int(horizon) > 0})
    if not values:
        raise ValueError("horizons must contain at least one positive integer")
    return values


def _side_multiplier_expr(side_col: str | None) -> pl.Expr:
    if side_col is None:
        return pl.lit(1.0)
    normalized = pl.col(side_col).cast(pl.Utf8).str.to_uppercase()
    return (
        pl.when(normalized.is_in(["SELL", "SHORT", "-1"]))
        .then(pl.lit(-1.0))
        .otherwise(pl.lit(1.0))
    )


def add_horizon_labels(
    frame: pl.DataFrame,
    *,
    price_col: str,
    horizons: int | Iterable[int],
    group_cols: tuple[str, ...] = ("symbol",),
    threshold: float = 0.0,
    side_col: str | None = None,
) -> pl.DataFrame:
    """Add fixed-horizon forward returns and binary outcome labels."""

    group_by = list(group_cols)
    order_col = _infer_order_col(frame, group_by)
    result = frame.sort([*group_by, order_col])
    multiplier = _side_multiplier_expr(side_col)

    derived_cols: list[pl.Expr] = []
    for horizon in _normalize_horizons(horizons):
        forward_price = pl.col(price_col).shift(-horizon).over(group_by)
        forward_return = ((forward_price / pl.col(price_col)) - 1.0) * multiplier
        derived_cols.extend(
            [
                forward_return.alias(f"forward_return_{horizon}b"),
                (forward_return > threshold).cast(pl.Int8).alias(f"label_up_{horizon}b"),
            ]
        )
    return result.with_columns(derived_cols)


def extract_event_outcomes(
    frame: pl.DataFrame,
    *,
    event_col: str,
    price_col: str,
    horizon: int,
    upper_barrier: float,
    lower_barrier: float,
    group_cols: tuple[str, ...] = ("symbol",),
    side_col: str | None = None,
) -> pl.DataFrame:
    """Create event-level labels using barrier-or-time-exit logic on future closes."""

    group_by = list(group_cols)
    order_col = _infer_order_col(frame, group_by)
    sorted_frame = frame.sort([*group_by, order_col]).with_row_index("_row_id")

    rows: list[dict[str, object]] = []
    for group in sorted_frame.partition_by(group_by, maintain_order=True):
        records = list(group.iter_rows(named=True))
        prices = [float(record[price_col]) for record in records]

        for idx, record in enumerate(records):
            if not bool(record[event_col]):
                continue

            side_value = str(record.get(side_col, "BUY")).upper() if side_col else "BUY"
            multiplier = -1.0 if side_value in {"SELL", "SHORT", "-1"} else 1.0
            window = prices[idx + 1 : idx + 1 + horizon]
            if not window:
                continue

            signed_returns = [
                (((future_price / prices[idx]) - 1.0) * multiplier) for future_price in window
            ]

            outcome = "time_exit"
            realized_return = signed_returns[-1]
            exit_bar = len(signed_returns)

            for step, value in enumerate(signed_returns, start=1):
                if value >= upper_barrier:
                    outcome = "take_profit"
                    realized_return = upper_barrier
                    exit_bar = step
                    break
                if value <= lower_barrier:
                    outcome = "stop_loss"
                    realized_return = lower_barrier
                    exit_bar = step
                    break

            event_row = {column: record[column] for column in group_by if column in record}
            event_row[order_col] = record[order_col]
            event_row.update(
                {
                    "event_row_id": record["_row_id"],
                    "entry_price": prices[idx],
                    "horizon_bars": horizon,
                    "upper_barrier": upper_barrier,
                    "lower_barrier": lower_barrier,
                    "exit_bar": exit_bar,
                    "realized_return": realized_return,
                    "event_outcome": outcome,
                    "label": int(realized_return > 0),
                }
            )
            if side_col and side_col in record:
                event_row[side_col] = record[side_col]
            rows.append(event_row)

    if not rows:
        return pl.DataFrame(
            schema={
                **{column: frame.schema[column] for column in group_by if column in frame.columns},
                order_col: frame.schema[order_col],
                "event_row_id": pl.UInt32,
                "entry_price": pl.Float64,
                "horizon_bars": pl.Int64,
                "upper_barrier": pl.Float64,
                "lower_barrier": pl.Float64,
                "exit_bar": pl.Int64,
                "realized_return": pl.Float64,
                "event_outcome": pl.String,
                "label": pl.Int64,
            }
        )
    return pl.DataFrame(rows)
