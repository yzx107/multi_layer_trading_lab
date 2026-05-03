from __future__ import annotations

import math
from collections import Counter

import polars as pl


def scan_lead_lag(
    frame: pl.DataFrame,
    leader_col: str,
    follower_col: str,
    max_lag: int = 5,
) -> pl.DataFrame:
    rows: list[dict] = []
    for lag in range(1, max_lag + 1):
        shifted = frame.select(
            pl.corr(pl.col(leader_col).shift(lag), pl.col(follower_col)).alias("corr")
        )
        rows.append({"lag": lag, "score": shifted.item(0, 0)})
    return pl.DataFrame(rows).sort("score", descending=True)


def transfer_entropy_placeholder(
    frame: pl.DataFrame,
    source_col: str,
    target_col: str,
    bins: int = 4,
) -> float:
    """A simple placeholder score, not a causal proof."""
    source = frame[source_col].fill_null(0).to_list()
    target = frame[target_col].fill_null(0).to_list()
    if len(source) < 3 or len(target) < 3:
        return 0.0
    joint = 0.0
    for idx in range(1, len(source)):
        joint += abs(source[idx - 1] - target[idx]) / (1 + bins)
    return 1 / (1 + math.exp(-joint / len(source)))


def _discretize(values: list[float], bins: int) -> list[int]:
    if not values:
        return []
    low = min(values)
    high = max(values)
    if math.isclose(low, high):
        return [0] * len(values)
    step = (high - low) / bins
    return [min(bins - 1, int((value - low) / step)) for value in values]


def estimate_transfer_entropy(
    frame: pl.DataFrame,
    source_col: str,
    target_col: str,
    *,
    bins: int = 4,
    lag: int = 1,
) -> float:
    """A simple TE-style estimate for ranking candidates, not causal proof."""

    source = frame[source_col].fill_null(0).cast(pl.Float64).to_list()
    target = frame[target_col].fill_null(0).cast(pl.Float64).to_list()
    if len(source) <= lag + 1 or len(target) <= lag + 1:
        return 0.0

    source_bins = _discretize(source, bins)
    target_bins = _discretize(target, bins)

    triples = [
        (target_bins[idx], target_bins[idx - lag], source_bins[idx - lag])
        for idx in range(lag, len(target_bins))
    ]
    if not triples:
        return 0.0

    joint_counts = Counter(triples)
    cond_counts = Counter((y_prev, x_prev) for _, y_prev, x_prev in triples)
    target_cond_counts = Counter((y_t, y_prev) for y_t, y_prev, _ in triples)
    prev_counts = Counter(y_prev for _, y_prev, _ in triples)
    total = len(triples)
    score = 0.0

    for (y_t, y_prev, x_prev), count in joint_counts.items():
        p_joint = count / total
        p_y_given_prev_x = count / cond_counts[(y_prev, x_prev)]
        p_y_given_prev = target_cond_counts[(y_t, y_prev)] / prev_counts[y_prev]
        score += p_joint * math.log(max(p_y_given_prev_x / max(p_y_given_prev, 1e-12), 1e-12))
    return max(score, 0.0)


def batch_scan_lead_lag(
    frame: pl.DataFrame,
    *,
    column_pairs: list[tuple[str, str]] | None = None,
    candidate_cols: list[str] | None = None,
    max_lag: int = 5,
    bins: int = 4,
) -> pl.DataFrame:
    if column_pairs is None:
        columns = candidate_cols or [
            column
            for column, dtype in frame.schema.items()
            if dtype.is_numeric() and column not in {"trade_date"}
        ]
        column_pairs = [
            (leader, follower)
            for leader in columns
            for follower in columns
            if leader != follower
        ]

    rows: list[dict[str, object]] = []
    for leader_col, follower_col in column_pairs:
        lag_scores = scan_lead_lag(frame, leader_col, follower_col, max_lag=max_lag)
        best = lag_scores.row(0, named=True)
        te = estimate_transfer_entropy(
            frame,
            leader_col,
            follower_col,
            bins=bins,
            lag=int(best["lag"]),
        )
        rows.append(
            {
                "leader_col": leader_col,
                "follower_col": follower_col,
                "best_lag": int(best["lag"]),
                "corr_score": float(best["score"]) if best["score"] is not None else 0.0,
                "transfer_entropy": te,
            }
        )
    if not rows:
        return pl.DataFrame(
            schema={
                "leader_col": pl.String,
                "follower_col": pl.String,
                "best_lag": pl.Int64,
                "corr_score": pl.Float64,
                "transfer_entropy": pl.Float64,
            }
        )
    return pl.DataFrame(rows).sort(["transfer_entropy", "corr_score"], descending=True)
