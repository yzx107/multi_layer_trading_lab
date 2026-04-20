from __future__ import annotations

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

    def aggregate(self, frame: pl.DataFrame, bucket: str = "1m") -> pl.DataFrame:
        bucket_every = {"1s": "1s", "5s": "5s", "30s": "30s", "1m": "1m"}[bucket]
        mid = (pl.col("bid_px_1") + pl.col("ask_px_1")) / 2
        signed_volume = pl.when(pl.col("side") == "BUY").then(pl.col("last_sz")).otherwise(-pl.col("last_sz"))
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
                    ((pl.col("ask_px_1") - pl.col("bid_px_1")) / pl.col("mid_px")).mean().alias("spread_bps"),
                    (pl.col("bid_sz_1") + pl.col("ask_sz_1")).mean().alias("depth_summary"),
                ]
            )
            .sort(["symbol", "ts"])
        )
