from __future__ import annotations

from dataclasses import dataclass

import polars as pl


@dataclass
class BetaPosterior:
    alpha: float
    beta: float

    @property
    def mean(self) -> float:
        return self.alpha / (self.alpha + self.beta)

    @property
    def interval(self) -> tuple[float, float]:
        total = self.alpha + self.beta
        spread = 1.96 * ((self.alpha * self.beta) / ((total**2) * (total + 1))) ** 0.5
        return max(0.0, self.mean - spread), min(1.0, self.mean + spread)


def update_hit_rate(
    samples: list[int],
    prior_alpha: float = 1.0,
    prior_beta: float = 1.0,
) -> BetaPosterior:
    wins = sum(samples)
    losses = len(samples) - wins
    return BetaPosterior(alpha=prior_alpha + wins, beta=prior_beta + losses)


def rolling_posterior(frame: pl.DataFrame, label_col: str, window: int = 20) -> pl.DataFrame:
    values = frame[label_col].fill_null(0).cast(pl.Int64).to_list()
    rows = []
    for idx in range(len(values)):
        sample = values[max(0, idx - window + 1) : idx + 1]
        posterior = update_hit_rate(sample)
        low, high = posterior.interval
        rows.append({"idx": idx, "posterior_mean": posterior.mean, "ci_low": low, "ci_high": high})
    return pl.DataFrame(rows)


def attach_setup_posteriors(
    frame: pl.DataFrame,
    *,
    label_col: str,
    group_cols: tuple[str, ...] = ("setup_id",),
    prior_alpha: float = 1.0,
    prior_beta: float = 1.0,
) -> pl.DataFrame:
    if frame.is_empty():
        return frame

    group_columns = [column for column in group_cols if column in frame.columns]
    order_col = None
    if "trade_date" in frame.columns:
        order_col = "trade_date"
    elif "ts" in frame.columns:
        order_col = "ts"
    sort_cols = [*group_columns, order_col] if order_col else group_columns
    working = frame.sort(sort_cols) if sort_cols else frame

    rows: list[dict[str, float | int]] = []
    if group_columns:
        groups = working.partition_by(group_columns, as_dict=True, maintain_order=True)
        for _, group in groups.items():
            samples = group[label_col].fill_null(0).cast(pl.Int64).to_list()
            for idx in range(len(samples)):
                posterior = update_hit_rate(
                    samples[: idx + 1],
                    prior_alpha=prior_alpha,
                    prior_beta=prior_beta,
                )
                low, high = posterior.interval
                rows.append(
                    {
                        "posterior_mean": posterior.mean,
                        "posterior_ci_low": low,
                        "posterior_ci_high": high,
                        "posterior_alpha": posterior.alpha,
                        "posterior_beta": posterior.beta,
                        "posterior_sample_count": idx + 1,
                    }
                )
    else:
        samples = working[label_col].fill_null(0).cast(pl.Int64).to_list()
        for idx in range(len(samples)):
            posterior = update_hit_rate(
                samples[: idx + 1],
                prior_alpha=prior_alpha,
                prior_beta=prior_beta,
            )
            low, high = posterior.interval
            rows.append(
                {
                    "posterior_mean": posterior.mean,
                    "posterior_ci_low": low,
                    "posterior_ci_high": high,
                    "posterior_alpha": posterior.alpha,
                    "posterior_beta": posterior.beta,
                    "posterior_sample_count": idx + 1,
                }
            )
    return working.hstack(pl.DataFrame(rows))


def summarize_setup_posteriors(
    frame: pl.DataFrame,
    *,
    label_col: str,
    group_cols: tuple[str, ...] = ("setup_id",),
    prior_alpha: float = 1.0,
    prior_beta: float = 1.0,
) -> pl.DataFrame:
    group_columns = [column for column in group_cols if column in frame.columns]
    if not group_columns:
        posterior = update_hit_rate(
            frame[label_col].fill_null(0).cast(pl.Int64).to_list(),
            prior_alpha=prior_alpha,
            prior_beta=prior_beta,
        )
        low, high = posterior.interval
        return pl.DataFrame(
            [
                {
                    "posterior_mean": posterior.mean,
                    "posterior_ci_low": low,
                    "posterior_ci_high": high,
                    "sample_count": frame.height,
                }
            ]
        )

    rows: list[dict[str, object]] = []
    for key, group in frame.partition_by(group_columns, as_dict=True, maintain_order=True).items():
        posterior = update_hit_rate(
            group[label_col].fill_null(0).cast(pl.Int64).to_list(),
            prior_alpha=prior_alpha,
            prior_beta=prior_beta,
        )
        low, high = posterior.interval
        values = key if isinstance(key, tuple) else (key,)
        row = {column: value for column, value in zip(group_columns, values, strict=True)}
        row.update(
            {
                "posterior_mean": posterior.mean,
                "posterior_ci_low": low,
                "posterior_ci_high": high,
                "sample_count": group.height,
            }
        )
        rows.append(row)
    return pl.DataFrame(rows)
