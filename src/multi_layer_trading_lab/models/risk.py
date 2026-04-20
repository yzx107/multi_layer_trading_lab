from __future__ import annotations


def kelly_fraction(win_rate: float, payoff_ratio: float) -> dict[str, float]:
    full = max(0.0, win_rate - (1 - win_rate) / max(payoff_ratio, 1e-9))
    return {
        "full_kelly": full,
        "half_kelly": full / 2,
        "quarter_kelly": full / 4,
        "eighth_kelly": full / 8,
    }
