from multi_layer_trading_lab.models.bayes import update_hit_rate
from multi_layer_trading_lab.models.risk import kelly_fraction


def test_bayes_posterior_mean_between_zero_and_one():
    posterior = update_hit_rate([1, 0, 1, 1, 0])
    assert 0 < posterior.mean < 1


def test_kelly_budget_monotonic():
    fractions = kelly_fraction(0.55, 1.5)
    assert fractions["full_kelly"] >= fractions["half_kelly"] >= fractions["quarter_kelly"] >= 0
