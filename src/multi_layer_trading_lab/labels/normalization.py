from __future__ import annotations

import re

import polars as pl

_SUFFIX_ALIASES = {
    "HK": "HK",
    "HKG": "HK",
    "US": "US",
    "NYSE": "US",
    "NASDAQ": "US",
    "NQ": "US",
    "SH": "SH",
    "SS": "SH",
    "SSE": "SH",
    "SZ": "SZ",
    "SZSE": "SZ",
}


def _infer_numeric_market(code: str, default_market: str | None = None) -> str:
    if default_market:
        market = default_market.upper()
        if market == "CN":
            return "SH" if code.startswith(("5", "6", "9")) else "SZ"
        return _SUFFIX_ALIASES.get(market, market)
    if len(code) <= 5:
        return "HK"
    return "SH" if code.startswith(("5", "6", "9")) else "SZ"


def normalize_symbol(symbol: str, default_market: str | None = None) -> str:
    """Normalize loose ticker strings into a canonical repo-level symbol format."""

    cleaned = re.sub(r"[\s/_-]+", ".", symbol.strip().upper())
    cleaned = re.sub(r"[^A-Z0-9.]", "", cleaned).strip(".")
    if not cleaned:
        raise ValueError("symbol must not be empty")

    if "." in cleaned:
        base, suffix = cleaned.rsplit(".", 1)
        suffix = _SUFFIX_ALIASES.get(suffix, suffix)
        if base.isdigit():
            if suffix == "HK":
                base = base.zfill(5)
            elif suffix in {"SH", "SZ"}:
                base = base.zfill(6)
        return f"{base}.{suffix}"

    if cleaned.isdigit():
        suffix = _infer_numeric_market(cleaned, default_market=default_market)
        width = 5 if suffix == "HK" else 6
        return f"{cleaned.zfill(width)}.{suffix}"

    suffix = _SUFFIX_ALIASES.get((default_market or "US").upper(), (default_market or "US").upper())
    return f"{cleaned}.{suffix}"


def normalize_symbol_frame(
    frame: pl.DataFrame,
    symbol_col: str = "symbol",
    output_col: str | None = None,
    default_market: str | None = None,
) -> pl.DataFrame:
    target_col = output_col or symbol_col
    return frame.with_columns(
        pl.col(symbol_col)
        .map_elements(
            lambda value: normalize_symbol(value, default_market=default_market)
            if value is not None
            else None,
            return_dtype=pl.Utf8,
        )
        .alias(target_col)
    )
