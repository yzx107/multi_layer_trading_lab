from __future__ import annotations

import math

import polars as pl


def scan_lead_lag(frame: pl.DataFrame, leader_col: str, follower_col: str, max_lag: int = 5) -> pl.DataFrame:
    rows: list[dict] = []
    for lag in range(1, max_lag + 1):
        shifted = frame.select(
            pl.corr(pl.col(leader_col).shift(lag), pl.col(follower_col)).alias("corr")
        )
        rows.append({"lag": lag, "score": shifted.item(0, 0)})
    return pl.DataFrame(rows).sort("score", descending=True)


def transfer_entropy_placeholder(frame: pl.DataFrame, source_col: str, target_col: str, bins: int = 4) -> float:
    """A simple placeholder score, not a causal proof."""
    source = frame[source_col].fill_null(0).to_list()
    target = frame[target_col].fill_null(0).to_list()
    if len(source) < 3 or len(target) < 3:
        return 0.0
    joint = 0.0
    for idx in range(1, len(source)):
        joint += abs(source[idx - 1] - target[idx]) / (1 + bins)
    return 1 / (1 + math.exp(-joint / len(source)))
