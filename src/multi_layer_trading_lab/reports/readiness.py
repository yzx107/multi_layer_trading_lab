from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from multi_layer_trading_lab.adapters.futu.client import FutuReadiness
from multi_layer_trading_lab.adapters.readiness import DataSourceReadiness
from multi_layer_trading_lab.adapters.source_status import SourceAdapterStatus
from multi_layer_trading_lab.research.external_portfolio import ExternalPortfolioEvidence
from multi_layer_trading_lab.research.hshare_verified import HshareVerifiedEvidence
from multi_layer_trading_lab.risk.profile import PersonalAccountProfile
from multi_layer_trading_lab.risk.promotion import (
    PaperGateEvidence,
    PromotionDecision,
    ResearchGateEvidence,
)
from multi_layer_trading_lab.storage.freshness import DatasetFreshness


@dataclass(frozen=True, slots=True)
class GoLiveReadinessInput:
    account_profile: PersonalAccountProfile
    data_sources: tuple[DataSourceReadiness, ...]
    data_freshness: tuple[DatasetFreshness, ...]
    opend_readiness: FutuReadiness
    research_evidence: ResearchGateEvidence
    research_decision: PromotionDecision
    source_adapters: tuple[SourceAdapterStatus, ...] = ()
    hshare_verified_evidence: HshareVerifiedEvidence | None = None
    external_portfolio_evidence: ExternalPortfolioEvidence | None = None
    paper_evidence: PaperGateEvidence | None = None
    paper_decision: PromotionDecision | None = None


def _format_dt(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


def _decision_payload(decision: PromotionDecision | None) -> dict[str, object]:
    if decision is None:
        return {
            "approved": False,
            "failed_reasons": ["not_evaluated"],
        }
    return {
        "approved": decision.approved,
        "failed_reasons": list(decision.failed_reasons),
    }


def build_go_live_readiness_manifest(
    readiness: GoLiveReadinessInput,
) -> dict[str, object]:
    data_sources_ready = all(source.ready for source in readiness.data_sources)
    live_data_adapters_ready = all(
        adapter.live_data_ready for adapter in readiness.source_adapters
    )
    data_fresh = all(item.status == "fresh" for item in readiness.data_freshness)
    research_approved = readiness.research_decision.approved
    hshare_verified_ready = (
        readiness.hshare_verified_evidence.ready
        if readiness.hshare_verified_evidence is not None
        else False
    )
    external_portfolio_approved = (
        readiness.external_portfolio_evidence.approved
        if readiness.external_portfolio_evidence is not None
        else False
    )
    paper_approved = (
        readiness.paper_decision.approved if readiness.paper_decision is not None else False
    )
    opend_ready = readiness.opend_readiness.ready
    approved = all(
        [
            data_sources_ready,
            live_data_adapters_ready,
            data_fresh,
            research_approved,
            hshare_verified_ready,
            external_portfolio_approved,
            paper_approved,
            opend_ready,
        ]
    )

    return {
        "go_live_approved": approved,
        "account_risk_budget": {
            "account_equity": readiness.account_profile.account_equity,
            "max_single_name_notional": readiness.account_profile.max_single_name_notional,
            "max_strategy_notional": readiness.account_profile.max_strategy_notional,
            "max_daily_drawdown": readiness.account_profile.max_daily_drawdown,
            "max_gross_notional": readiness.account_profile.max_gross_notional,
            "max_open_slippage_bps": readiness.account_profile.max_open_slippage_bps,
            "default_kelly_scale": readiness.account_profile.default_kelly_scale,
        },
        "data_sources": [
            {
                "source": source.source,
                "ready": source.ready,
                "failed_reasons": list(source.failed_reasons),
            }
            for source in readiness.data_sources
        ],
        "source_adapters": [
            {
                "source": adapter.source,
                "credential_ready": adapter.credential_ready,
                "adapter_status": adapter.adapter_status,
                "live_data_ready": adapter.live_data_ready,
                "failed_reasons": list(adapter.failed_reasons),
            }
            for adapter in readiness.source_adapters
        ],
        "data_freshness": [
            {
                "dataset": item.dataset,
                "status": item.status,
                "rows": item.rows,
                "modified_at": _format_dt(item.latest_modified_at),
                "path": str(item.path) if item.path is not None else None,
            }
            for item in readiness.data_freshness
        ],
        "execution": {
            "opend_ready": opend_ready,
            "failed_reasons": list(readiness.opend_readiness.failed_reasons),
        },
        "research_to_paper": {
            **_decision_payload(readiness.research_decision),
            "evidence": {
                "trade_count": readiness.research_evidence.trade_count,
                "distinct_trade_dates": readiness.research_evidence.distinct_trade_dates,
                "no_lookahead_audit_passed": (
                    readiness.research_evidence.no_lookahead_audit_passed
                ),
                "cost_model_applied": readiness.research_evidence.cost_model_applied,
                "capacity_check_passed": readiness.research_evidence.capacity_check_passed,
            },
        },
        "hshare_verified": {
            "ready": hshare_verified_ready,
            "failed_reasons": (
                ["not_evaluated"]
                if readiness.hshare_verified_evidence is None
                else list(readiness.hshare_verified_evidence.failed_reasons)
            ),
            "evidence": None
            if readiness.hshare_verified_evidence is None
            else {
                "year": readiness.hshare_verified_evidence.year,
                "status": readiness.hshare_verified_evidence.status,
                "selected_date_count": (
                    readiness.hshare_verified_evidence.selected_date_count
                ),
                "completed_count": readiness.hshare_verified_evidence.completed_count,
                "failed_count": readiness.hshare_verified_evidence.failed_count,
                "orders_rows": readiness.hshare_verified_evidence.orders_rows,
                "trades_rows": readiness.hshare_verified_evidence.trades_rows,
                "is_partial": readiness.hshare_verified_evidence.is_partial,
                "summary_path": str(readiness.hshare_verified_evidence.summary_path),
            },
        },
        "external_factor_portfolio": {
            "approved": external_portfolio_approved,
            "failed_reasons": (
                ["not_evaluated"]
                if readiness.external_portfolio_evidence is None
                else list(readiness.external_portfolio_evidence.failed_reasons)
            ),
            "evidence": None
            if readiness.external_portfolio_evidence is None
            else {
                "candidate_count": readiness.external_portfolio_evidence.candidate_count,
                "review_candidate_count": (
                    readiness.external_portfolio_evidence.review_candidate_count
                ),
                "target_notional": readiness.external_portfolio_evidence.target_notional,
                "max_single_candidate_notional": (
                    readiness.external_portfolio_evidence.max_single_candidate_notional
                ),
            },
        },
        "paper_to_live": {
            **_decision_payload(readiness.paper_decision),
            "evidence": None
            if readiness.paper_evidence is None
            else {
                "paper_sessions": readiness.paper_evidence.paper_sessions,
                "order_reject_rate": readiness.paper_evidence.order_reject_rate,
                "reconciliation_clean": readiness.paper_evidence.reconciliation_clean,
                "slippage_within_assumption": (
                    readiness.paper_evidence.slippage_within_assumption
                ),
                "manual_live_enable": readiness.paper_evidence.manual_live_enable,
            },
        },
    }
