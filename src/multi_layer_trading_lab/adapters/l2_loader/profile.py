from __future__ import annotations

import json
import zipfile
from dataclasses import dataclass, field
from datetime import date, timedelta
from io import BytesIO
from pathlib import Path

import polars as pl

from multi_layer_trading_lab.adapters.l2_loader.loader import L2ColumnMapping

L2_MAPPING_TARGETS = tuple(
    target for target in L2ColumnMapping.__dataclass_fields__ if target != "extra_columns"
)


COMMON_L2_COLUMN_HINTS = {
    "symbol": ("symbol", "code", "ticker", "security_id", "stock_code"),
    "ts": ("ts", "timestamp", "time", "datetime", "event_time", "transact_time"),
    "bid_px_1": ("bid_px_1", "bid1", "bid_price1", "bid_price_1", "bid_px1"),
    "ask_px_1": ("ask_px_1", "ask1", "ask_price1", "ask_price_1", "ask_px1"),
    "bid_sz_1": ("bid_sz_1", "bidvol1", "bid_volume1", "bid_size1", "bid_qty1"),
    "ask_sz_1": ("ask_sz_1", "askvol1", "ask_volume1", "ask_size1", "ask_qty1"),
    "last_px": ("last_px", "price", "last_price", "trade_price", "match_price"),
    "last_sz": ("last_sz", "volume", "trade_volume", "last_volume", "match_qty"),
    "side": ("side", "bs_flag", "trade_side", "buy_sell", "direction"),
    "cancel_flag": ("cancel_flag", "is_cancel", "cancel", "cancelled", "order_cancel"),
}


@dataclass(frozen=True, slots=True)
class L2FileProfile:
    path: Path
    rows: int
    columns: tuple[str, ...]
    dtypes: dict[str, str]
    suggested_mapping: dict[str, str] = field(default_factory=dict)
    missing_targets: tuple[str, ...] = ()

    def as_text(self) -> str:
        lines = [
            f"path={self.path}",
            f"rows={self.rows}",
            f"columns={','.join(self.columns)}",
        ]
        for target, source in sorted(self.suggested_mapping.items()):
            lines.append(f"map.{target}={source}")
        if self.missing_targets:
            lines.append(f"missing_targets={','.join(self.missing_targets)}")
        return "\n".join(lines)


def read_l2_sample(path: Path, limit: int = 1000) -> pl.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".parquet":
        return pl.read_parquet(path).head(limit)
    if suffix == ".csv":
        return pl.read_csv(path, infer_schema_length=min(limit, 1000)).head(limit)
    raise ValueError(f"unsupported L2 sample format: {suffix}")


def list_l2_zip_members(path: Path, category: str | None = None) -> tuple[str, ...]:
    with zipfile.ZipFile(path) as archive:
        members = []
        for name in archive.namelist():
            if name.endswith("/") or not name.lower().endswith(".csv"):
                continue
            if category and f"\\{category}\\" not in name:
                continue
            members.append(name)
    return tuple(sorted(members))


def resolve_l2_zip_member(path: Path, member: str) -> str:
    requested = member.replace("\\\\", "\\").replace("/", "\\")
    with zipfile.ZipFile(path) as archive:
        members = archive.namelist()
    for candidate in members:
        if candidate == requested:
            return candidate
        if candidate.replace("/", "\\") == requested:
            return candidate
    raise KeyError(f"L2 zip member not found: {member}")


def find_order_add_member(path: Path, symbol: str) -> str:
    ticker = symbol.upper().replace(".HK", "").zfill(5)
    suffix = f"\\OrderAdd\\{ticker}.csv"
    for member in list_l2_zip_members(path, category="OrderAdd"):
        if member.endswith(suffix):
            return member
    raise KeyError(f"OrderAdd member not found for symbol: {symbol}")


def read_l2_zip_member_sample(path: Path, member: str, limit: int = 1000) -> pl.DataFrame:
    resolved_member = resolve_l2_zip_member(path, member)
    with zipfile.ZipFile(path) as archive:
        payload = archive.read(resolved_member)
    frame = pl.read_csv(BytesIO(payload), infer_schema_length=min(limit, 1000)).head(limit)
    normalized_member = resolved_member.replace("\\", "/")
    symbol = f"{Path(normalized_member).stem}.HK"
    trade_date = path.stem
    return frame.with_columns(
        pl.lit(symbol).alias("symbol"),
        pl.lit(trade_date).alias("trade_date"),
    )


def normalize_order_add_zip_member(path: Path, member: str, limit: int = 100_000) -> pl.DataFrame:
    sample = read_l2_zip_member_sample(path, member, limit=limit)
    required = {"SeqNum", "OrderId", "OrderType", "Ext", "Time", "Price", "Volume", "Level"}
    missing = sorted(required.difference(sample.columns))
    if missing:
        raise ValueError(f"missing OrderAdd columns: {','.join(missing)}")

    time_text = pl.col("Time").cast(pl.String).str.zfill(6)
    return sample.select(
        [
            pl.col("symbol").cast(pl.String),
            (pl.col("trade_date").cast(pl.String) + time_text)
            .str.strptime(pl.Datetime, "%Y%m%d%H%M%S", strict=False)
            .alias("ts"),
            pl.col("trade_date").cast(pl.String),
            pl.col("SeqNum").cast(pl.Int64).alias("seq_num"),
            pl.col("OrderId").cast(pl.Int64).alias("order_id"),
            pl.col("OrderType").cast(pl.Int64).alias("order_type"),
            pl.col("Ext").cast(pl.Int64).alias("ext"),
            pl.col("Price").cast(pl.Float64).alias("price"),
            pl.col("Volume").cast(pl.Int64).alias("volume"),
            pl.col("Level").cast(pl.Int64).alias("level"),
            pl.col("BrokerNo").cast(pl.String).alias("broker_no")
            if "BrokerNo" in sample.columns
            else pl.lit(None).cast(pl.String).alias("broker_no"),
            pl.col("VolumePre").cast(pl.Int64).alias("volume_pre")
            if "VolumePre" in sample.columns
            else pl.lit(None).cast(pl.Int64).alias("volume_pre"),
        ]
    )


def normalize_order_add_zip_batch(
    paths: list[Path],
    symbols: list[str],
    limit_per_member: int = 100_000,
) -> pl.DataFrame:
    frames = []
    for path in paths:
        for symbol in symbols:
            member = find_order_add_member(path, symbol)
            frames.append(normalize_order_add_zip_member(path, member, limit=limit_per_member))
    if not frames:
        return pl.DataFrame()
    return pl.concat(frames, how="diagonal_relaxed")


def discover_l2_zip_paths(
    root: Path,
    start: date,
    end: date,
) -> tuple[Path, ...]:
    if end < start:
        raise ValueError("end date cannot be before start date")
    paths = []
    current = start
    while current <= end:
        candidate = root / f"{current.year}" / f"{current:%Y%m%d}.zip"
        if candidate.exists():
            paths.append(candidate)
        current += timedelta(days=1)
    return tuple(paths)


def build_order_add_coverage(paths: list[Path], symbols: list[str]) -> pl.DataFrame:
    rows = []
    for path in paths:
        for symbol in symbols:
            try:
                member = find_order_add_member(path, symbol)
                available = True
                failed_reason = ""
            except KeyError as exc:
                member = ""
                available = False
                failed_reason = str(exc)
            rows.append(
                {
                    "zip_path": str(path),
                    "trade_date": path.stem,
                    "symbol": symbol,
                    "available": available,
                    "member": member,
                    "failed_reason": failed_reason,
                }
            )
    return pl.DataFrame(rows)


def suggest_l2_mapping(columns: list[str]) -> dict[str, str]:
    normalized_lookup = {column.lower(): column for column in columns}
    suggested: dict[str, str] = {}
    for target, candidates in COMMON_L2_COLUMN_HINTS.items():
        for candidate in candidates:
            if candidate.lower() in normalized_lookup:
                suggested[target] = normalized_lookup[candidate.lower()]
                break
    return suggested


def profile_l2_file(path: Path, limit: int = 1000) -> L2FileProfile:
    sample = read_l2_sample(path, limit=limit)
    return profile_l2_file_from_frame(path, sample)


def profile_l2_file_from_frame(path: Path, sample: pl.DataFrame) -> L2FileProfile:
    suggested = suggest_l2_mapping(sample.columns)
    missing = tuple(target for target in L2_MAPPING_TARGETS if target not in suggested)
    return L2FileProfile(
        path=path,
        rows=sample.height,
        columns=tuple(sample.columns),
        dtypes={column: str(dtype) for column, dtype in sample.schema.items()},
        suggested_mapping=suggested,
        missing_targets=missing,
    )


def write_l2_mapping_template(profile: L2FileProfile, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "source_path": str(profile.path),
        "mapping": profile.suggested_mapping,
        "missing_targets": list(profile.missing_targets),
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def load_l2_mapping(path: Path) -> L2ColumnMapping:
    payload = json.loads(path.read_text(encoding="utf-8"))
    mapping = payload.get("mapping", payload)
    allowed = set(L2_MAPPING_TARGETS)
    kwargs = {target: source for target, source in mapping.items() if target in allowed}
    return L2ColumnMapping(**kwargs)
