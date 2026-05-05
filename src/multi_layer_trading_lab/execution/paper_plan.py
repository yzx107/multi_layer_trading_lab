from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

from multi_layer_trading_lab.execution.opend_tickets import resolve_quote_snapshot
from multi_layer_trading_lab.risk.profile import PersonalAccountProfile


@dataclass(frozen=True, slots=True)
class PaperSessionPlanInput:
    portfolio: pl.DataFrame
    account_profile: PersonalAccountProfile
    session_id: str
    execution_log_path: Path
    broker_report_path: Path
    opend_env: str = "SIMULATE"
    opend_mode: str = "paper"
    quote_snapshot: dict[str, object] | None = None
    allow_lot_round_up: bool = False


def build_paper_session_plan(plan_input: PaperSessionPlanInput) -> dict[str, object]:
    portfolio = plan_input.portfolio
    reviewable = (
        portfolio.filter(pl.col("candidate_status") == "review_candidate")
        if not portfolio.is_empty() and "candidate_status" in portfolio.columns
        else pl.DataFrame()
    )
    orders: list[dict[str, object]] = []
    symbol, reference_price, lot_size, quote_failed = resolve_quote_snapshot(
        plan_input.quote_snapshot,
        symbol=None,
        reference_price=None,
        lot_size=None,
    )
    min_lot_notional = (
        float(reference_price) * int(lot_size)
        if reference_price is not None and lot_size is not None
        else None
    )
    if not reviewable.is_empty():
        for row in reviewable.iter_rows(named=True):
            notional = float(row.get("target_notional") or 0.0)
            if notional <= 0:
                continue
            status = "planned_not_submitted"
            failed_reasons: list[str] = []
            sizing_method = "requested_notional"
            requested_notional = notional
            if min_lot_notional is not None and notional < min_lot_notional:
                if (
                    plan_input.allow_lot_round_up
                    and min_lot_notional <= plan_input.account_profile.max_single_name_notional
                ):
                    notional = min_lot_notional
                    sizing_method = "lot_round_up"
                    failed_reasons.append("target_notional_rounded_up_to_one_lot")
                else:
                    status = "blocked_below_one_lot"
                    failed_reasons.append("target_notional_below_one_lot")
            direction = str(row.get("direction_hint") or "")
            orders.append(
                {
                    "strategy_id": f"external_factor::{row['factor_name']}",
                    "factor_name": row["factor_name"],
                    "side": "sell" if "inverse" in direction else "buy",
                    "target_notional": notional,
                    "requested_notional": requested_notional,
                    "sizing_method": sizing_method,
                    "max_single_name_notional": plan_input.account_profile.max_single_name_notional,
                    "quote_symbol": symbol,
                    "reference_price": reference_price,
                    "lot_size": lot_size,
                    "min_lot_notional": min_lot_notional,
                    "mode": plan_input.opend_mode,
                    "env": plan_input.opend_env,
                    "status": status,
                    "failed_reasons": failed_reasons,
                }
            )

    total_notional = sum(float(order["target_notional"]) for order in orders)
    requested_total_notional = sum(float(order["requested_notional"]) for order in orders)
    executable_orders = [
        order for order in orders if order["status"] == "planned_not_submitted"
    ]
    below_lot_orders = [
        order for order in orders if order["status"] == "blocked_below_one_lot"
    ]
    rounded_orders = [
        order for order in orders if order["sizing_method"] == "lot_round_up"
    ]
    failed: list[str] = []
    if not orders:
        failed.append("no_reviewable_paper_orders")
    failed.extend(quote_failed)
    if orders and all(order["status"] != "planned_not_submitted" for order in orders):
        failed.append("no_executable_lot_sized_orders")
    if total_notional > plan_input.account_profile.max_strategy_notional:
        failed.append("planned_notional_exceeds_strategy_limit")
    if any(
        float(order["target_notional"]) > plan_input.account_profile.max_single_name_notional
        for order in orders
    ):
        failed.append("planned_order_exceeds_single_name_limit")
    if plan_input.opend_mode != "paper":
        failed.append("paper_plan_requires_paper_mode")
    if plan_input.opend_env != "SIMULATE":
        failed.append("paper_plan_requires_simulate_env")
    if (
        rounded_orders
        and total_notional > plan_input.account_profile.max_strategy_notional
        and "planned_notional_exceeds_strategy_limit" not in failed
    ):
        failed.append("planned_notional_exceeds_strategy_limit")

    return {
        "session_id": plan_input.session_id,
        "created_at": datetime.now(UTC).isoformat(),
        "ready_for_paper": not failed,
        "failed_reasons": failed,
        "opend": {
            "mode": plan_input.opend_mode,
            "env": plan_input.opend_env,
            "quote_symbol": symbol,
            "reference_price": reference_price,
            "lot_size": lot_size,
            "min_lot_notional": min_lot_notional,
        },
        "lot_sizing": {
            "allow_lot_round_up": plan_input.allow_lot_round_up,
            "min_lot_notional": min_lot_notional,
            "requested_total_notional": requested_total_notional,
            "planned_total_notional": total_notional,
            "executable_order_count": len(executable_orders),
            "below_lot_order_count": len(below_lot_orders),
            "rounded_order_count": len(rounded_orders),
            "required_notional_for_one_lot": min_lot_notional,
            "suggested_actions": _lot_sizing_suggestions(
                has_quote=min_lot_notional is not None,
                below_lot_order_count=len(below_lot_orders),
                allow_lot_round_up=plan_input.allow_lot_round_up,
                min_lot_notional=min_lot_notional,
                max_single_name_notional=plan_input.account_profile.max_single_name_notional,
            ),
        },
        "account_risk_budget": {
            "account_equity": plan_input.account_profile.account_equity,
            "max_single_name_notional": plan_input.account_profile.max_single_name_notional,
            "max_strategy_notional": plan_input.account_profile.max_strategy_notional,
            "default_kelly_scale": plan_input.account_profile.default_kelly_scale,
        },
        "paper_evidence_paths": {
            "execution_log_path": str(plan_input.execution_log_path),
            "broker_report_path": str(plan_input.broker_report_path),
        },
        "planned_order_count": len(orders),
        "planned_total_notional": total_notional,
        "orders": orders,
    }


def _lot_sizing_suggestions(
    *,
    has_quote: bool,
    below_lot_order_count: int,
    allow_lot_round_up: bool,
    min_lot_notional: float | None,
    max_single_name_notional: float,
) -> list[str]:
    if not has_quote:
        return ["provide_opend_quote_snapshot_for_lot_sizing"]
    if below_lot_order_count <= 0:
        return []
    if min_lot_notional is not None and min_lot_notional > max_single_name_notional:
        return ["choose_lower_lot_notional_symbol_or_raise_single_name_limit"]
    if not allow_lot_round_up:
        return ["enable_allow_lot_round_up_after_review_or_raise_target_notional"]
    return []
