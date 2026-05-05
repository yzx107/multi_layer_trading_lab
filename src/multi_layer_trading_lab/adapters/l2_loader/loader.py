from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import polars as pl

L2_SCHEMA = {
    "symbol": pl.String,
    "ts": pl.Datetime,
    "bid_px_1": pl.Float64,
    "ask_px_1": pl.Float64,
    "bid_sz_1": pl.Int64,
    "ask_sz_1": pl.Int64,
    "last_px": pl.Float64,
    "last_sz": pl.Int64,
    "side": pl.String,
    "cancel_flag": pl.Boolean,
}


@dataclass(frozen=True, slots=True)
class L2ColumnMapping:
    symbol: str = "symbol"
    ts: str = "ts"
    bid_px_1: str = "bid_px_1"
    ask_px_1: str = "ask_px_1"
    bid_sz_1: str = "bid_sz_1"
    ask_sz_1: str = "ask_sz_1"
    last_px: str = "last_px"
    last_sz: str = "last_sz"
    side: str = "side"
    cancel_flag: str = "cancel_flag"
    extra_columns: dict[str, str] = field(default_factory=dict)

    def as_rename_map(self) -> dict[str, str]:
        mapping = {
            self.symbol: "symbol",
            self.ts: "ts",
            self.bid_px_1: "bid_px_1",
            self.ask_px_1: "ask_px_1",
            self.bid_sz_1: "bid_sz_1",
            self.ask_sz_1: "ask_sz_1",
            self.last_px: "last_px",
            self.last_sz: "last_sz",
            self.side: "side",
            self.cancel_flag: "cancel_flag",
        }
        mapping.update(self.extra_columns)
        return mapping


class L2Loader:
    def __init__(self, root: Path):
        self.root = root

    def file_for_date(self, trade_date: str) -> Path:
        return self.root / f"l2_{trade_date}.parquet"

    def load_trade_date(self, trade_date: str) -> pl.DataFrame:
        path = self.file_for_date(trade_date)
        if not path.exists():
            raise FileNotFoundError(path)
        return pl.read_parquet(path)

    def normalize_raw_frame(
        self,
        frame: pl.DataFrame,
        mapping: L2ColumnMapping | None = None,
    ) -> pl.DataFrame:
        column_mapping = mapping or L2ColumnMapping()
        rename_map = column_mapping.as_rename_map()
        required_raw_columns = set(rename_map)
        missing = sorted(required_raw_columns.difference(frame.columns))
        if missing:
            raise ValueError(f"missing L2 columns: {','.join(missing)}")

        normalized = frame.rename(rename_map).select(list(L2_SCHEMA))
        ts_dtype = normalized.schema["ts"]
        ts_expr = (
            pl.col("ts").str.strptime(pl.Datetime, strict=False)
            if ts_dtype == pl.String
            else pl.col("ts").cast(pl.Datetime)
        )
        return normalized.with_columns(
            [
                pl.col("symbol").cast(pl.String),
                ts_expr,
                pl.col("bid_px_1").cast(pl.Float64),
                pl.col("ask_px_1").cast(pl.Float64),
                pl.col("bid_sz_1").cast(pl.Int64),
                pl.col("ask_sz_1").cast(pl.Int64),
                pl.col("last_px").cast(pl.Float64),
                pl.col("last_sz").cast(pl.Int64),
                pl.col("side").cast(pl.String).str.to_uppercase(),
                pl.col("cancel_flag").cast(pl.Boolean),
            ]
        )

    def aggregate(self, frame: pl.DataFrame, bucket: str = "1m") -> pl.DataFrame:
        bucket_every = {"1s": "1s", "5s": "5s", "30s": "30s", "1m": "1m"}[bucket]
        mid = (pl.col("bid_px_1") + pl.col("ask_px_1")) / 2
        signed_volume = (
            pl.when(pl.col("side") == "BUY")
            .then(pl.col("last_sz"))
            .otherwise(-pl.col("last_sz"))
        )
        return (
            frame.with_columns(
                [
                    mid.alias("mid_px"),
                    (pl.col("bid_sz_1") - pl.col("ask_sz_1")).alias("imbalance_raw"),
                    signed_volume.alias("signed_volume"),
                    pl.col("cancel_flag").cast(pl.Int64).alias("cancel_int"),
                ]
            )
            .group_by_dynamic("ts", every=bucket_every, group_by="symbol", closed="left")
            .agg(
                [
                    pl.col("mid_px").last().alias("mid_close"),
                    pl.col("last_px").last().alias("last_px"),
                    pl.col("imbalance_raw").mean().alias("bid_ask_imbalance"),
                    pl.col("signed_volume").sum().alias("trade_direction_imbalance"),
                    pl.col("cancel_int").mean().alias("cancel_rate"),
                    ((pl.col("ask_px_1") - pl.col("bid_px_1")) / pl.col("mid_px"))
                    .mean()
                    .alias("spread_bps"),
                    (pl.col("bid_sz_1") + pl.col("ask_sz_1")).mean().alias("depth_summary"),
                ]
            )
            .sort(["symbol", "ts"])
        )
