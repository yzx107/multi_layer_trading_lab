from __future__ import annotations

from dataclasses import dataclass, field

from multi_layer_trading_lab.backtest.types import Fill
from multi_layer_trading_lab.execution.adapters import CostModel
from multi_layer_trading_lab.risk.profile import PersonalAccountProfile


@dataclass(frozen=True, slots=True)
class CostAuditResult:
    passed: bool
    total_notional: float
    estimated_fees: float
    estimated_slippage: float
    total_cost: float
    total_cost_bps: float
    failed_reasons: tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True, slots=True)
class CapacityAuditResult:
    passed: bool
    max_fill_notional: float
    max_symbol_notional: float
    failed_reasons: tuple[str, ...] = field(default_factory=tuple)


def audit_trade_costs(
    fills: list[Fill],
    *,
    cost_model: CostModel | None = None,
    max_total_cost_bps: float = 35.0,
) -> CostAuditResult:
    model = cost_model or CostModel()
    total_notional = sum(fill.price * fill.quantity for fill in fills)
    estimated_fees = sum(
        model.estimate_fees(
            fill.price * fill.quantity,
            apply_stamp_duty=fill.symbol.endswith((".HK", "-HK")),
        )
        for fill in fills
    )
    estimated_slippage = sum(abs(fill.slippage) * fill.quantity for fill in fills)
    total_cost = estimated_fees + estimated_slippage
    total_cost_bps = total_cost / total_notional * 10_000 if total_notional else 0.0

    failed: list[str] = []
    if not fills:
        failed.append("no_fills")
    if total_cost_bps > max_total_cost_bps:
        failed.append("cost_too_high")

    return CostAuditResult(
        passed=not failed,
        total_notional=total_notional,
        estimated_fees=estimated_fees,
        estimated_slippage=estimated_slippage,
        total_cost=total_cost,
        total_cost_bps=total_cost_bps,
        failed_reasons=tuple(failed),
    )


def audit_personal_capacity(
    fills: list[Fill],
    *,
    profile: PersonalAccountProfile,
) -> CapacityAuditResult:
    symbol_notional: dict[str, float] = {}
    max_fill_notional = 0.0
    for fill in fills:
        notional = fill.price * fill.quantity
        max_fill_notional = max(max_fill_notional, notional)
        symbol_notional[fill.symbol] = symbol_notional.get(fill.symbol, 0.0) + notional

    max_symbol_notional = max(symbol_notional.values(), default=0.0)
    failed: list[str] = []
    if not fills:
        failed.append("no_fills")
    if max_fill_notional > profile.max_single_name_notional:
        failed.append("single_fill_exceeds_single_name_limit")
    if max_symbol_notional > profile.max_single_name_notional:
        failed.append("symbol_notional_exceeds_single_name_limit")
    if sum(symbol_notional.values()) > profile.max_gross_notional:
        failed.append("gross_notional_exceeds_account_limit")

    return CapacityAuditResult(
        passed=not failed,
        max_fill_notional=max_fill_notional,
        max_symbol_notional=max_symbol_notional,
        failed_reasons=tuple(failed),
    )
