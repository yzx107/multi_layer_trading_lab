from __future__ import annotations

import json
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


@dataclass(frozen=True, slots=True)
class OpenDPaperTicketExportResult:
    output_path: Path | None
    ticket_count: int
    failed_reasons: tuple[str, ...]

    @property
    def ready(self) -> bool:
        return not self.failed_reasons and self.output_path is not None


@dataclass(frozen=True, slots=True)
class OpenDPaperTicketSubmitResult:
    output_path: Path | None
    submitted_count: int
    response_count: int
    failed_reasons: tuple[str, ...]

    @property
    def ready(self) -> bool:
        return not self.failed_reasons and self.output_path is not None


def load_opend_paper_tickets(path: Path) -> list[dict[str, object]]:
    if not path.exists():
        return []
    rows: list[dict[str, object]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            stripped = line.strip()
            if stripped:
                rows.append(json.loads(stripped))
    return rows


def _normalize_hk_symbol(symbol: str) -> str:
    normalized = symbol.strip().upper()
    if not normalized:
        raise ValueError("symbol must not be empty")
    if normalized.startswith("HK."):
        return normalized
    if normalized.endswith(".HK"):
        return f"HK.{normalized[:-3].zfill(5)}"
    return f"HK.{normalized.zfill(5)}"


def _round_down_to_lot(quantity: int, lot_size: int) -> int:
    if lot_size <= 0:
        raise ValueError("lot_size must be positive")
    return quantity - (quantity % lot_size)


def resolve_quote_snapshot(
    quote_snapshot: dict[str, object] | None,
    *,
    symbol: str | None,
    reference_price: float | None,
    lot_size: int | None,
) -> tuple[str | None, float | None, int | None, tuple[str, ...]]:
    if quote_snapshot is None:
        return symbol, reference_price, lot_size, ()

    payload = quote_snapshot.get("quote") if "quote" in quote_snapshot else quote_snapshot
    if not isinstance(payload, dict):
        return symbol, reference_price, lot_size, ("invalid_quote_snapshot",)

    resolved_symbol = symbol or _optional_str(payload.get("symbol"))
    resolved_lot_size = lot_size or _optional_int(payload.get("lot_size"))
    resolved_price = reference_price or _first_positive_float(
        payload.get("last_price"),
        payload.get("price"),
        payload.get("best_ask"),
        payload.get("ask"),
        payload.get("best_bid"),
        payload.get("bid"),
    )
    failed: list[str] = []
    if not resolved_symbol:
        failed.append("quote_snapshot_missing_symbol")
    if not resolved_lot_size or resolved_lot_size <= 0:
        failed.append("quote_snapshot_missing_lot_size")
    if not resolved_price or resolved_price <= 0:
        failed.append("quote_snapshot_missing_reference_price")
    return resolved_symbol, resolved_price, resolved_lot_size, tuple(failed)


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _optional_int(value: object) -> int | None:
    if value is None:
        return None
    try:
        return int(float(str(value)))
    except ValueError:
        return None


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


def fetch_opend_quote_snapshot(
    *,
    base_url: str,
    symbol: str,
    timeout_seconds: float = 8.0,
    urlopen_fn: Callable[..., object] = urlopen,
) -> dict[str, object]:
    endpoint = (
        f"{base_url.rstrip('/')}/api/quote?"
        f"{urlencode({'symbol': _normalize_hk_symbol(symbol)})}"
    )
    with urlopen_fn(endpoint, timeout=timeout_seconds) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("OpenD quote response must be a JSON object")
    _, _, _, failed = resolve_quote_snapshot(
        payload,
        symbol=None,
        reference_price=None,
        lot_size=None,
    )
    if failed:
        raise ValueError(f"invalid OpenD quote response: {','.join(failed)}")
    return payload


def fetch_opend_account_status(
    *,
    base_url: str,
    timeout_seconds: float = 8.0,
    urlopen_fn: Callable[..., object] = urlopen,
) -> dict[str, object]:
    endpoint = f"{base_url.rstrip('/')}/api/accounts"
    with urlopen_fn(endpoint, timeout=timeout_seconds) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("OpenD accounts response must be a JSON object")
    accounts = payload.get("accounts")
    if not isinstance(accounts, list):
        raise ValueError("OpenD accounts response missing accounts list")
    sim_accounts = [
        account
        for account in accounts
        if isinstance(account, dict)
        and str(account.get("trd_env") or "").upper() == "SIMULATE"
    ]
    hk_stock_sim_accounts = [
        account
        for account in sim_accounts
        if _account_has_hk_auth(account)
        and str(account.get("sim_acc_type") or "").upper() in {"STOCK", "N/A", ""}
    ]
    return {
        **payload,
        "simulate_account_count": len(sim_accounts),
        "hk_stock_simulate_account_count": len(hk_stock_sim_accounts),
        "ready_for_paper_simulate": bool(hk_stock_sim_accounts),
        "failed_reasons": []
        if hk_stock_sim_accounts
        else ["missing_hk_stock_simulate_account"],
    }


def fetch_opend_runtime_status(
    *,
    base_url: str,
    timeout_seconds: float = 8.0,
    urlopen_fn: Callable[..., object] = urlopen,
) -> dict[str, object]:
    endpoint = f"{base_url.rstrip('/')}/api/health"
    with urlopen_fn(endpoint, timeout=timeout_seconds) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("OpenD health response must be a JSON object")
    kill_switch = payload.get("kill_switch") is True
    ok = payload.get("ok") is True
    failed: list[str] = []
    if not ok:
        failed.append("opend_health_not_ok")
    if kill_switch:
        failed.append("opend_kill_switch_enabled")
    return {
        **payload,
        "ready_for_order_submission": ok and not kill_switch,
        "failed_reasons": failed,
    }


def _account_has_hk_auth(account: dict[str, object]) -> bool:
    auth = account.get("trdmarket_auth")
    if not isinstance(auth, list):
        return False
    return "HK" in {str(item).upper() for item in auth}


def submit_opend_paper_tickets(
    ticket_path: Path,
    output_path: Path,
    *,
    base_url: str,
    submit_paper_simulate: bool = False,
    allow_resubmit: bool = False,
    timeout_seconds: float = 8.0,
    max_attempts: int = 3,
    retry_delay_seconds: float = 0.5,
    urlopen_fn: Callable[..., object] = urlopen,
) -> OpenDPaperTicketSubmitResult:
    tickets = load_opend_paper_tickets(ticket_path)
    failed: list[str] = []
    if not tickets:
        failed.append("missing_opend_paper_tickets")
    ticket_ids = {str(ticket.get("ticket_id") or "") for ticket in tickets}
    if output_path.exists() and not allow_resubmit:
        existing_ticket_ids = {
            str(response.get("ticket_id") or "")
            for response in load_opend_paper_ticket_responses(output_path)
        }
        for ticket_id in sorted(ticket_ids & existing_ticket_ids):
            if ticket_id:
                failed.append(f"ticket_already_submitted:{ticket_id}")
    responses: list[dict[str, object]] = []
    if failed:
        return OpenDPaperTicketSubmitResult(
            output_path=None,
            submitted_count=0,
            response_count=0,
            failed_reasons=tuple(failed),
        )
    for ticket in tickets:
        ticket_id = str(ticket.get("ticket_id") or "")
        ticket_failed = False
        if ticket.get("dry_run") is not True and not submit_paper_simulate:
            failed.append(f"ticket_not_dry_run:{ticket_id}")
            ticket_failed = True
        if ticket.get("real") is True or ticket.get("submit_real") is True:
            failed.append(f"ticket_requests_real_submission:{ticket_id}")
            ticket_failed = True
        if ticket_failed:
            continue
        payload = ticket.get("web_normal_order_payload")
        if not isinstance(payload, dict):
            failed.append(f"invalid_ticket_payload:{ticket_id}")
            continue
        request_payload = dict(payload)
        request_payload["real"] = False
        request_payload["paper"] = bool(submit_paper_simulate)
        endpoint = f"{base_url.rstrip('/')}/api/normal/order"
        body = json.dumps(request_payload).encode("utf-8")
        response_payload, error = _post_json_with_retries(
            endpoint=endpoint,
            body=body,
            timeout_seconds=timeout_seconds,
            max_attempts=max_attempts,
            retry_delay_seconds=retry_delay_seconds,
            urlopen_fn=urlopen_fn,
        )
        if error is not None:
            failed.append(f"opend_ticket_submit_failed:{ticket_id}:{error}")
            responses.append(
                {
                    "event": "mttl_opend_paper_ticket_response",
                    "created_at": datetime.now(UTC).isoformat(),
                    "ticket_id": ticket_id,
                    "dry_run": not submit_paper_simulate,
                    "paper": bool(submit_paper_simulate),
                    "request": request_payload,
                    "response": {
                        "ok": False,
                        "submitted": False,
                        "error": error,
                    },
                }
            )
            continue
        responses.append(
            {
                "event": "mttl_opend_paper_ticket_response",
                "created_at": datetime.now(UTC).isoformat(),
                "ticket_id": ticket_id,
                "dry_run": not submit_paper_simulate,
                "paper": bool(submit_paper_simulate),
                "request": request_payload,
                "response": response_payload,
            }
        )

    if failed:
        if responses:
            _write_opend_paper_ticket_responses(output_path, responses)
        return OpenDPaperTicketSubmitResult(
            output_path=output_path if responses else None,
            submitted_count=_submitted_response_count(responses),
            response_count=len(responses),
            failed_reasons=tuple(failed),
        )
    _write_opend_paper_ticket_responses(output_path, responses)
    return OpenDPaperTicketSubmitResult(
        output_path=output_path,
        submitted_count=_submitted_response_count(responses),
        response_count=len(responses),
        failed_reasons=(),
    )


def _submitted_response_count(responses: list[dict[str, object]]) -> int:
    return sum(
        1
        for response in responses
        if isinstance(response.get("response"), dict)
        and response["response"].get("ok") is not False
    )


def _write_opend_paper_ticket_responses(
    output_path: Path,
    responses: list[dict[str, object]],
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        for response in responses:
            handle.write(json.dumps(response, ensure_ascii=True, sort_keys=True) + "\n")


def load_opend_paper_ticket_responses(path: Path) -> list[dict[str, object]]:
    if not path.exists():
        return []
    rows: list[dict[str, object]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            stripped = line.strip()
            if stripped:
                payload = json.loads(stripped)
                if isinstance(payload, dict):
                    rows.append(payload)
    return rows


def _post_json_with_retries(
    *,
    endpoint: str,
    body: bytes,
    timeout_seconds: float,
    max_attempts: int,
    retry_delay_seconds: float,
    urlopen_fn: Callable[..., object],
) -> tuple[dict[str, object], str | None]:
    attempts = max(1, max_attempts)
    last_error = "UnknownError"
    for attempt in range(1, attempts + 1):
        try:
            request = Request(
                endpoint,
                data=body,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urlopen_fn(request, timeout=timeout_seconds) as response:
                payload = json.loads(response.read().decode("utf-8"))
            if not isinstance(payload, dict):
                return {}, "ValueError"
            return payload, None
        except HTTPError as exc:
            last_error = _http_error_summary(exc)
            if attempt < attempts and retry_delay_seconds > 0:
                time.sleep(retry_delay_seconds)
        except Exception as exc:
            last_error = type(exc).__name__
            if attempt < attempts and retry_delay_seconds > 0:
                time.sleep(retry_delay_seconds)
    return {}, last_error


def _http_error_summary(exc: HTTPError) -> str:
    try:
        payload = json.loads(exc.read().decode("utf-8"))
    except Exception:
        return f"HTTPError:{exc.code}"
    if not isinstance(payload, dict):
        return f"HTTPError:{exc.code}"
    error_type = str(payload.get("error_type") or "HTTPError")
    message = str(payload.get("error") or exc.reason or exc.code)
    return f"HTTPError:{exc.code}:{error_type}:{message}"


def build_opend_paper_tickets(
    paper_plan: dict[str, object],
    *,
    symbol: str | None,
    reference_price: float | None,
    lot_size: int = 100,
    order_type: str = "NORMAL",
) -> tuple[list[dict[str, object]], tuple[str, ...]]:
    failed: list[str] = []
    if not paper_plan.get("ready_for_paper"):
        failed.append("paper_plan_not_ready")
    if not symbol:
        failed.append("missing_paper_ticket_symbol")
    if reference_price is None or reference_price <= 0:
        failed.append("missing_paper_ticket_reference_price")
    if lot_size <= 0:
        failed.append("invalid_lot_size")
    if failed:
        return [], tuple(failed)

    broker_symbol = _normalize_hk_symbol(str(symbol))
    assert reference_price is not None
    orders = paper_plan.get("orders") or []
    if not isinstance(orders, list):
        return [], ("paper_plan_orders_not_list",)

    created_at = datetime.now(UTC).isoformat()
    tickets: list[dict[str, object]] = []
    for idx, order in enumerate(orders, start=1):
        if not isinstance(order, dict):
            failed.append("paper_plan_order_not_object")
            continue
        if str(order.get("status") or "") != "planned_not_submitted":
            failed.append(f"paper_plan_order_not_executable:{order.get('factor_name') or idx}")
            continue
        notional = float(order.get("target_notional") or 0.0)
        if notional <= 0:
            continue
        shares = _round_down_to_lot(int(notional / reference_price), lot_size)
        if shares <= 0:
            failed.append("paper_ticket_notional_below_one_lot")
            continue
        side = str(order.get("side") or "buy").upper()
        if side not in {"BUY", "SELL"}:
            failed.append(f"unsupported_paper_ticket_side:{side}")
            continue
        ticket_id = f"{paper_plan.get('session_id') or 'paper'}-{idx:03d}"
        tickets.append(
            {
                "event": "mttl_paper_order_ticket",
                "created_at": created_at,
                "session_id": paper_plan.get("session_id"),
                "ticket_id": ticket_id,
                "dry_run": True,
                "real": False,
                "submit_real": False,
                "opend_repo": "/Users/yxin/AI_Workstation/futu-opend-execution",
                "web_normal_order_payload": {
                    "symbol": broker_symbol,
                    "side": side,
                    "order_type": order_type.upper(),
                    "quantity_mode": "SHARES",
                    "shares": shares,
                    "limit_price": round(reference_price, 4),
                    "max_notional": round(
                        float(order.get("max_single_name_notional") or notional),
                        2,
                    ),
                    "remark": ticket_id[:64],
                    "real": False,
                },
                "source_order": order,
                "risk": {
                    "target_notional": notional,
                    "reference_price": reference_price,
                    "lot_size": lot_size,
                    "rounded_shares": shares,
                    "estimated_notional": round(shares * reference_price, 2),
                    "planned_status": "exported_for_opend_dry_run",
                },
            }
        )

    if not tickets and not failed:
        failed.append("no_exportable_paper_tickets")
    return tickets, tuple(failed)


def export_opend_paper_tickets(
    paper_plan: dict[str, object],
    output_path: Path,
    *,
    symbol: str | None,
    reference_price: float | None,
    lot_size: int | None = None,
    order_type: str = "NORMAL",
    quote_snapshot: dict[str, object] | None = None,
) -> OpenDPaperTicketExportResult:
    symbol, reference_price, resolved_lot_size, quote_failed = resolve_quote_snapshot(
        quote_snapshot,
        symbol=symbol,
        reference_price=reference_price,
        lot_size=lot_size,
    )
    if quote_failed:
        return OpenDPaperTicketExportResult(
            output_path=None,
            ticket_count=0,
            failed_reasons=quote_failed,
        )
    tickets, failed = build_opend_paper_tickets(
        paper_plan,
        symbol=symbol,
        reference_price=reference_price,
        lot_size=resolved_lot_size or 0,
        order_type=order_type,
    )
    if failed:
        return OpenDPaperTicketExportResult(
            output_path=None,
            ticket_count=0,
            failed_reasons=failed,
        )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as handle:
        for ticket in tickets:
            handle.write(json.dumps(ticket, ensure_ascii=True, sort_keys=True) + "\n")
    return OpenDPaperTicketExportResult(
        output_path=output_path,
        ticket_count=len(tickets),
        failed_reasons=(),
    )
