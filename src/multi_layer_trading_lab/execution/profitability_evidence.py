from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from multi_layer_trading_lab.adapters.futu.reports import (
    futu_order_reports_to_execution_reports,
    load_futu_order_report_rows,
)
from multi_layer_trading_lab.execution.reconciliation import (
    load_execution_log,
    reconcile_execution_reports,
)
from multi_layer_trading_lab.execution.session_ledger import build_paper_session_ledger


@dataclass(frozen=True, slots=True)
class ProfitabilityEvidenceInput:
    execution_log_path: Path
    broker_report_path: Path
    output_path: Path
    paper_sessions: int
    mark_prices_path: Path | None = None
    max_allowed_drawdown: float = 10_000.0
    price_tolerance: float = 0.01


def _load_mark_prices(path: Path | None) -> dict[str, float]:
    if path is None or not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(payload, dict) and isinstance(payload.get("marks"), list):
        payload = payload["marks"]
    if isinstance(payload, list):
        return {
            str(item["symbol"]): float(item["mark_price"])
            for item in payload
            if isinstance(item, dict) and item.get("symbol") and item.get("mark_price") is not None
        }
    if isinstance(payload, dict):
        return {str(symbol): float(price) for symbol, price in payload.items()}
    return {}


def build_mark_prices_from_opend_quote_snapshot(
    quote_snapshot_path: Path,
    output_path: Path,
) -> dict[str, float]:
    payload = json.loads(quote_snapshot_path.read_text(encoding="utf-8"))
    quote = payload.get("quote") if isinstance(payload, dict) and "quote" in payload else payload
    if not isinstance(quote, dict):
        raise ValueError("OpenD quote snapshot must be a JSON object")
    symbol = quote.get("symbol")
    if not symbol:
        raise ValueError("OpenD quote snapshot missing symbol")
    mark_price = _first_positive_float(
        quote.get("last_price"),
        quote.get("price"),
        quote.get("best_bid"),
        quote.get("bid"),
        quote.get("best_ask"),
        quote.get("ask"),
    )
    if mark_price is None:
        raise ValueError("OpenD quote snapshot missing positive mark price")
    marks = {str(symbol): mark_price}
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(marks, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return marks


def _first_positive_float(*values: object) -> float | None:
    for value in values:
        if value is None:
            continue
        try:
            parsed = float(str(value))
        except ValueError:
            continue
        if parsed > 0:
            return parsed
    return None


def _signed_cash_flow(record: dict[str, object]) -> float:
    side = str(record.get("side") or "").lower()
    quantity = float(record.get("quantity") or 0.0)
    fill_price = float(record.get("fill_price") or 0.0)
    fees = float(record.get("fees") or record.get("commission") or 0.0)
    notional = quantity * fill_price
    if side == "buy":
        return -notional - fees
    if side == "sell":
        return notional - fees
    return -fees


def build_profitability_evidence(
    input_data: ProfitabilityEvidenceInput,
) -> dict[str, object]:
    failed: list[str] = []
    local_records = load_execution_log(input_data.execution_log_path)
    broker_rows = (
        load_futu_order_report_rows(input_data.broker_report_path)
        if input_data.broker_report_path.exists()
        else []
    )
    if not local_records:
        failed.append("missing_execution_log")
    if not broker_rows:
        failed.append("missing_broker_report")
    if any(record.get("dry_run") is True for record in local_records):
        failed.append("dry_run_execution_log_not_real_paper")
    if any(row.get("dry_run") is True for row in broker_rows):
        failed.append("dry_run_broker_report_not_real_paper")
    session_ledger = build_paper_session_ledger(
        execution_log_path=input_data.execution_log_path,
        broker_report_path=input_data.broker_report_path,
    )
    if session_ledger.inferred_session_count < 20:
        failed.append("insufficient_inferred_paper_sessions")
    if input_data.paper_sessions > session_ledger.inferred_session_count:
        failed.append("paper_sessions_exceed_inferred_sessions")
    failed.extend(
        reason
        for reason in session_ledger.failed_reasons
        if reason
        not in {
            "dry_run_rows_present",
            "insufficient_inferred_sessions",
            "missing_execution_log",
            "missing_broker_report",
        }
    )

    reconciliation = reconcile_execution_reports(
        local_records,
        futu_order_reports_to_execution_reports(broker_rows),
        price_tolerance=input_data.price_tolerance,
    )
    if not reconciliation.clean:
        failed.append("broker_reconciliation_not_clean")

    marks = _load_mark_prices(input_data.mark_prices_path)
    cash_pnl = 0.0
    positions: dict[str, float] = {}
    avg_cost: dict[str, float] = {}
    open_position_market_value = 0.0
    peak = 0.0
    trough_drawdown = 0.0
    cash_peak = 0.0
    cash_trough_drawdown = 0.0
    running_cash = 0.0
    latest_prices: dict[str, float] = {}
    latest_trade_prices: dict[str, float] = {}
    symbol_cash_pnl: dict[str, float] = {}
    for record in local_records:
        if str(record.get("status")) not in {"filled", "partial_filled"}:
            continue
        symbol = str(record.get("symbol") or "")
        side = str(record.get("side") or "").lower()
        quantity = float(record.get("quantity") or 0.0)
        fill_price = float(record.get("fill_price") or 0.0)
        cash_flow = _signed_cash_flow(record)
        cash_pnl += cash_flow
        running_cash += cash_flow
        cash_peak = max(cash_peak, running_cash)
        cash_trough_drawdown = min(cash_trough_drawdown, running_cash - cash_peak)
        if not symbol or side not in {"buy", "sell"}:
            continue
        symbol_cash_pnl[symbol] = symbol_cash_pnl.get(symbol, 0.0) + cash_flow
        latest_prices[symbol] = fill_price
        latest_trade_prices[symbol] = fill_price
        signed_qty = quantity if side == "buy" else -quantity
        previous_qty = positions.get(symbol, 0.0)
        new_qty = previous_qty + signed_qty
        if side == "buy" and new_qty:
            previous_cost = avg_cost.get(symbol, 0.0) * previous_qty
            avg_cost[symbol] = (previous_cost + quantity * fill_price) / new_qty
        positions[symbol] = new_qty
        equity = running_cash + _marked_position_value(positions, latest_prices)
        peak = max(peak, equity)
        trough_drawdown = min(trough_drawdown, equity - peak)

    for symbol, quantity in positions.items():
        if abs(quantity) < 1e-9:
            continue
        mark = marks.get(symbol)
        if mark is None:
            failed.append(f"missing_mark_price:{symbol}")
            continue
        latest_prices[symbol] = mark
        open_position_market_value += quantity * mark

    net_pnl = cash_pnl + open_position_market_value
    symbol_attribution = _build_symbol_attribution(
        symbol_cash_pnl=symbol_cash_pnl,
        positions=positions,
        avg_cost=avg_cost,
        marks=marks,
        latest_trade_prices=latest_trade_prices,
    )
    if latest_prices:
        final_equity = cash_pnl + _marked_position_value(positions, latest_prices)
        peak = max(peak, final_equity)
        trough_drawdown = min(trough_drawdown, final_equity - peak)
    if net_pnl <= 0:
        failed.append("net_pnl_not_positive")
    if trough_drawdown < -abs(input_data.max_allowed_drawdown):
        failed.append("drawdown_breached")

    evidence = {
        "ready": not failed,
        "paper_sessions": input_data.paper_sessions,
        "inferred_session_count": session_ledger.inferred_session_count,
        "session_dates": session_ledger.session_dates,
        "execution_log_rows": len(local_records),
        "broker_report_rows": len(broker_rows),
        "reconciled": reconciliation.clean,
        "matched_orders": reconciliation.matched_orders,
        "cash_pnl": cash_pnl,
        "open_position_market_value": open_position_market_value,
        "net_pnl": net_pnl,
        "max_drawdown": trough_drawdown,
        "cash_drawdown": cash_trough_drawdown,
        "max_allowed_drawdown": input_data.max_allowed_drawdown,
        "positions": positions,
        "symbol_attribution": symbol_attribution,
        "failed_reasons": tuple(dict.fromkeys(failed)),
    }
    input_data.output_path.parent.mkdir(parents=True, exist_ok=True)
    input_data.output_path.write_text(
        json.dumps(evidence, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return evidence


def _marked_position_value(
    positions: dict[str, float],
    prices: dict[str, float],
) -> float:
    return sum(quantity * prices.get(symbol, 0.0) for symbol, quantity in positions.items())


def _build_symbol_attribution(
    *,
    symbol_cash_pnl: dict[str, float],
    positions: dict[str, float],
    avg_cost: dict[str, float],
    marks: dict[str, float],
    latest_trade_prices: dict[str, float],
) -> dict[str, dict[str, float | None]]:
    attribution: dict[str, dict[str, float | None]] = {}
    symbols = set(symbol_cash_pnl) | set(positions)
    for symbol in sorted(symbols):
        quantity = positions.get(symbol, 0.0)
        mark_price = marks.get(symbol)
        market_value = quantity * mark_price if mark_price is not None else None
        cash_pnl = symbol_cash_pnl.get(symbol, 0.0)
        net_pnl = cash_pnl + market_value if market_value is not None else None
        attribution[symbol] = {
            "cash_pnl": cash_pnl,
            "quantity": quantity,
            "avg_cost": avg_cost.get(symbol),
            "latest_trade_price": latest_trade_prices.get(symbol),
            "mark_price": mark_price,
            "market_value": market_value,
            "net_pnl": net_pnl,
        }
    return attribution
