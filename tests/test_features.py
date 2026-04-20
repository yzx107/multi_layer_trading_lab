from datetime import date

from multi_layer_trading_lab.adapters.tushare.client import TushareClient
from multi_layer_trading_lab.features.daily.basic import build_daily_features


def test_build_daily_features_has_expected_columns():
    bars = TushareClient().fetch_daily_bars("00700.HK", date(2026, 3, 20), date(2026, 4, 2))
    features = build_daily_features(bars)
    assert "ret_1d" in features.columns
    assert "volatility_5d" in features.columns
