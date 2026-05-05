from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

from multi_layer_trading_lab.adapters.futu.client import FutuReadiness
from multi_layer_trading_lab.adapters.readiness import DataSourceReadiness
from multi_layer_trading_lab.risk.profile import PersonalAccountProfile
from multi_layer_trading_lab.risk.promotion import PromotionDecision


@dataclass(frozen=True, slots=True)
class OpsDailyReportInput:
    report_date: date
    account_profile: PersonalAccountProfile
    opend_readiness: FutuReadiness
    research_to_paper: PromotionDecision | None = None
    paper_to_live: PromotionDecision | None = None
    data_freshness: dict[str, str] | None = None
    data_source_readiness: tuple[DataSourceReadiness, ...] = ()


def _status_line(name: str, ok: bool, reasons: tuple[str, ...] = ()) -> str:
    status = "PASS" if ok else "BLOCKED"
    suffix = "" if ok else f" reasons={','.join(reasons)}"
    return f"- {name}: {status}{suffix}"


def render_ops_daily_report(path: Path, report: OpsDailyReportInput) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    profile = report.account_profile
    freshness = report.data_freshness or {}

    lines = [
        "# Ops Daily Report",
        "",
        f"date={report.report_date.isoformat()}",
        "",
        "## Account Risk Budget",
        f"- account_equity={profile.account_equity:.2f}",
        f"- max_single_name_notional={profile.max_single_name_notional:.2f}",
        f"- max_strategy_notional={profile.max_strategy_notional:.2f}",
        f"- max_daily_drawdown={profile.max_daily_drawdown:.2f}",
        f"- max_gross_notional={profile.max_gross_notional:.2f}",
        f"- max_open_slippage_bps={profile.max_open_slippage_bps:.2f}",
        f"- default_kelly_scale={profile.default_kelly_scale:.3f}",
        "",
        "## Execution Readiness",
        _status_line(
            "futu_opend",
            report.opend_readiness.ready,
            report.opend_readiness.failed_reasons,
        ),
        "",
        "## Data Source Readiness",
    ]

    if report.data_source_readiness:
        for readiness in report.data_source_readiness:
            lines.append(
                _status_line(
                    readiness.source,
                    readiness.ready,
                    readiness.failed_reasons,
                )
            )
    else:
        lines.append("- no data source checks supplied")

    lines.extend(
        [
            "",
            "## Promotion Gates",
        ]
    )

    if report.research_to_paper is None:
        lines.append("- research_to_paper: NOT_EVALUATED")
    else:
        lines.append(
            _status_line(
                "research_to_paper",
                report.research_to_paper.approved,
                report.research_to_paper.failed_reasons,
            )
        )

    if report.paper_to_live is None:
        lines.append("- paper_to_live: NOT_EVALUATED")
    else:
        lines.append(
            _status_line(
                "paper_to_live",
                report.paper_to_live.approved,
                report.paper_to_live.failed_reasons,
            )
        )

    lines.extend(
        [
            "",
            "## Data Freshness",
        ]
    )
    if freshness:
        for dataset, status in sorted(freshness.items()):
            lines.append(f"- {dataset}: {status}")
    else:
        lines.append("- no freshness inputs supplied")

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path
