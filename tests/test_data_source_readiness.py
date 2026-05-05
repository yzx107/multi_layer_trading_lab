from __future__ import annotations

import base64
import json

from multi_layer_trading_lab.adapters.readiness import (
    check_ifind_readiness,
    check_tushare_readiness,
)


def _ifind_refresh_token(expiry: str) -> str:
    header = base64.urlsafe_b64encode(b'{"sign_time":"2026-05-03 14:32:46"}').decode().rstrip("=")
    payload = base64.urlsafe_b64encode(
        json.dumps({"user": {"refreshTokenExpiredTime": expiry}}).encode()
    ).decode().rstrip("=")
    return f"{header}.{payload}.signature"


def test_tushare_readiness_requires_token() -> None:
    readiness = check_tushare_readiness("")

    assert readiness.ready is False
    assert readiness.failed_reasons == ("missing_tushare_token",)


def test_ifind_readiness_requires_token_or_username_password() -> None:
    readiness = check_ifind_readiness(username="user", password=None)

    assert readiness.ready is False
    assert "missing_ifind_password" in readiness.failed_reasons
    assert "missing_ifind_access_token" in readiness.failed_reasons


def test_data_source_readiness_passes_when_credentials_present() -> None:
    assert check_tushare_readiness("token").ready is True
    assert check_ifind_readiness("user", "password").ready is True
    assert check_ifind_readiness(None, None, "access", "refresh").ready is True


def test_ifind_readiness_blocks_expired_parseable_refresh_token() -> None:
    readiness = check_ifind_readiness(
        None,
        None,
        "access",
        _ifind_refresh_token("2020-01-01 00:00:00"),
    )

    assert readiness.ready is False
    assert "expired_ifind_refresh_token" in readiness.failed_reasons
