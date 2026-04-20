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


def update_hit_rate(samples: list[int], prior_alpha: float = 1.0, prior_beta: float = 1.0) -> BetaPosterior:
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
