from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from urllib.request import Request, urlopen


@dataclass(frozen=True, slots=True)
class IFindTokenStatus:
    has_access_token: bool
    has_refresh_token: bool
    refresh_token_expires_at: datetime | None
    refresh_token_valid_now: bool | None
    parse_error: str | None = None

    @property
    def token_pair_present(self) -> bool:
        return self.has_access_token and self.has_refresh_token


@dataclass(frozen=True, slots=True)
class IFindAccessTokenRefreshStatus:
    requested_url: str
    access_token_received: bool
    access_token_expires_at: datetime | None
    failed_reason: str | None = None

    @property
    def ok(self) -> bool:
        return self.access_token_received and self.failed_reason is None

    def to_dict(self) -> dict[str, object]:
        return {
            "requested_url": self.requested_url,
            "access_token_received": self.access_token_received,
            "access_token_expires_at": (
                self.access_token_expires_at.isoformat()
                if self.access_token_expires_at
                else None
            ),
            "failed_reason": self.failed_reason,
        }


def inspect_ifind_tokens(
    *,
    access_token: str | None,
    refresh_token: str | None,
    now: datetime | None = None,
) -> IFindTokenStatus:
    has_access = bool(access_token and access_token.strip())
    has_refresh = bool(refresh_token and refresh_token.strip())
    if not has_refresh:
        return IFindTokenStatus(
            has_access_token=has_access,
            has_refresh_token=False,
            refresh_token_expires_at=None,
            refresh_token_valid_now=None,
        )
    try:
        payload = _decode_refresh_payload(refresh_token or "")
        expires_at = _coerce_ifind_datetime(
            _get_nested(payload, ["user", "refreshTokenExpiredTime"])
            or payload.get("refreshTokenExpiredTime")
        )
    except Exception as exc:
        return IFindTokenStatus(
            has_access_token=has_access,
            has_refresh_token=True,
            refresh_token_expires_at=None,
            refresh_token_valid_now=None,
            parse_error=type(exc).__name__,
        )
    resolved_now = now or datetime.now(UTC)
    return IFindTokenStatus(
        has_access_token=has_access,
        has_refresh_token=True,
        refresh_token_expires_at=expires_at,
        refresh_token_valid_now=expires_at > resolved_now,
    )


def refresh_ifind_access_token_status(
    *,
    refresh_token: str | None,
    url: str = "https://quantapi.51ifind.com/api/v1/get_access_token",
    timeout_seconds: float = 15.0,
    urlopen_fn: Any = urlopen,
) -> IFindAccessTokenRefreshStatus:
    if not refresh_token or not refresh_token.strip():
        return IFindAccessTokenRefreshStatus(
            requested_url=url,
            access_token_received=False,
            access_token_expires_at=None,
            failed_reason="missing_ifind_refresh_token",
        )
    request = Request(
        url,
        data=b"{}",
        headers={
            "Content-Type": "application/json",
            "refresh_token": refresh_token,
        },
        method="POST",
    )
    try:
        with urlopen_fn(request, timeout=timeout_seconds) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except Exception as exc:
        return IFindAccessTokenRefreshStatus(
            requested_url=url,
            access_token_received=False,
            access_token_expires_at=None,
            failed_reason=f"ifind_access_token_refresh_failed:{type(exc).__name__}",
        )
    data = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(data, dict):
        return IFindAccessTokenRefreshStatus(
            requested_url=url,
            access_token_received=False,
            access_token_expires_at=None,
            failed_reason="ifind_access_token_response_missing_data",
        )
    token = data.get("access_token") or data.get("accessToken")
    expires_at = _optional_ifind_datetime(
        data.get("accessTokenExpiredTime")
        or data.get("access_token_expired_time")
        or data.get("expire_time")
        or data.get("expires_at")
    )
    return IFindAccessTokenRefreshStatus(
        requested_url=url,
        access_token_received=bool(token),
        access_token_expires_at=expires_at,
        failed_reason=None if token else "ifind_access_token_missing",
    )


def _decode_refresh_payload(refresh_token: str) -> dict[str, Any]:
    parts = refresh_token.split(".")
    if len(parts) < 2:
        raise ValueError("refresh token does not contain a payload segment")
    payload_segment = parts[1]
    payload_bytes = _base64url_decode(payload_segment)
    payload = json.loads(payload_bytes.decode("utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("refresh token payload must be a JSON object")
    return payload


def _base64url_decode(value: str) -> bytes:
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode(value + padding)


def _get_nested(payload: dict[str, Any], path: list[str]) -> Any:
    current: Any = payload
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _coerce_ifind_datetime(value: object) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    if value is None:
        raise ValueError("refresh token expiry is missing")
    text = str(value).strip()
    parsed = datetime.strptime(text, "%Y-%m-%d %H:%M:%S")
    return parsed.replace(tzinfo=UTC)


def _optional_ifind_datetime(value: object) -> datetime | None:
    if value is None:
        return None
    try:
        return _coerce_ifind_datetime(value)
    except Exception:
        return None
