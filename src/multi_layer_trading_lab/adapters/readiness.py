from __future__ import annotations

from dataclasses import dataclass

from multi_layer_trading_lab.adapters.ifind.token import inspect_ifind_tokens


@dataclass(frozen=True, slots=True)
class DataSourceReadiness:
    source: str
    ready: bool
    failed_reasons: tuple[str, ...] = ()


def check_tushare_readiness(token: str | None) -> DataSourceReadiness:
    failed: list[str] = []
    if not token or not token.strip():
        failed.append("missing_tushare_token")
    return DataSourceReadiness(
        source="tushare",
        ready=not failed,
        failed_reasons=tuple(failed),
    )


def check_ifind_readiness(
    username: str | None,
    password: str | None,
    access_token: str | None = None,
    refresh_token: str | None = None,
) -> DataSourceReadiness:
    failed: list[str] = []
    token_status = inspect_ifind_tokens(
        access_token=access_token,
        refresh_token=refresh_token,
    )
    has_token_pair = token_status.token_pair_present
    has_user_password = bool(username and username.strip() and password and password.strip())
    if has_token_pair and token_status.refresh_token_valid_now is False:
        failed.append("expired_ifind_refresh_token")
    if not failed and (has_token_pair or has_user_password):
        return DataSourceReadiness(source="ifind", ready=True)
    if not access_token or not access_token.strip():
        failed.append("missing_ifind_access_token")
    if not refresh_token or not refresh_token.strip():
        failed.append("missing_ifind_refresh_token")
    if not username or not username.strip():
        failed.append("missing_ifind_username")
    if not password or not password.strip():
        failed.append("missing_ifind_password")
    return DataSourceReadiness(
        source="ifind",
        ready=not failed,
        failed_reasons=tuple(failed),
    )
