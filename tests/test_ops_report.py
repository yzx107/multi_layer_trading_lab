from __future__ import annotations

from datetime import date
from pathlib import Path

from multi_layer_trading_lab.adapters.futu.client import FutuReadiness
from multi_layer_trading_lab.adapters.readiness import DataSourceReadiness
from multi_layer_trading_lab.reports.ops import OpsDailyReportInput, render_ops_daily_report
from multi_layer_trading_lab.risk.profile import personal_trader_profile


def test_ops_daily_report_renders_risk_and_readiness(tmp_path: Path) -> None:
    path = render_ops_daily_report(
        tmp_path / "ops.md",
        OpsDailyReportInput(
            report_date=date(2026, 5, 3),
            account_profile=personal_trader_profile(1_000_000),
            opend_readiness=FutuReadiness(
                ready=False,
                failed_reasons=("manual_live_enable_missing",),
            ),
            data_source_readiness=(
                DataSourceReadiness(
                    source="ifind",
                    ready=False,
                    failed_reasons=("missing_ifind_password",),
                ),
            ),
            data_freshness={"hk_l2": "fresh", "tushare": "stale"},
        ),
    )

    content = path.read_text(encoding="utf-8")

    assert "max_single_name_notional=80000.00" in content
    assert "futu_opend: BLOCKED reasons=manual_live_enable_missing" in content
    assert "ifind: BLOCKED reasons=missing_ifind_password" in content
    assert "hk_l2: fresh" in content
    assert "tushare: stale" in content
