from __future__ import annotations

from multi_layer_trading_lab.adapters.futu.client import (
    FutuMode,
    FutuOpenDConfig,
    check_opend_readiness,
)


def test_opend_paper_readiness_accepts_simulate_env() -> None:
    readiness = check_opend_readiness(
        FutuOpenDConfig(mode=FutuMode.PAPER, env="SIMULATE")
    )

    assert readiness.ready is True
    assert readiness.failed_reasons == ()


def test_opend_live_requires_real_env_unlock_and_manual_enable() -> None:
    readiness = check_opend_readiness(FutuOpenDConfig(mode=FutuMode.LIVE, env="SIMULATE"))

    assert readiness.ready is False
    assert readiness.failed_reasons == (
        "live_requires_real_env",
        "live_requires_unlock_password",
        "manual_live_enable_missing",
    )
