from __future__ import annotations

from pathlib import Path

import duckdb
import polars as pl


class ParquetStore:
    def __init__(self, root: Path):
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)

    def dataset_path(self, dataset: str) -> Path:
        path = self.root / dataset
        path.mkdir(parents=True, exist_ok=True)
        return path

    def write(
        self,
        dataset: str,
        frame: pl.DataFrame,
        partition_cols: list[str] | None = None,
    ) -> Path:
        path = self.dataset_path(dataset) / "part-000.parquet"
        if partition_cols:
            frame.write_parquet(path, use_pyarrow=True)
        else:
            frame.write_parquet(path)
        return path

    def read(self, dataset: str) -> pl.DataFrame:
        path = self.dataset_path(dataset)
        files = sorted(path.glob("*.parquet"))
        if not files:
            return pl.DataFrame()
        return pl.concat([pl.read_parquet(file) for file in files], how="diagonal_relaxed")


class DuckDBCatalog:
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    def connect(self) -> duckdb.DuckDBPyConnection:
        return duckdb.connect(str(self.db_path))

    def register_parquet(self, table_name: str, parquet_path: Path) -> None:
        with self.connect() as conn:
            parquet_uri = parquet_path.as_posix()
            query = (
                f"create or replace view {table_name} "
                f"as select * from read_parquet('{parquet_uri}')"
            )
            conn.execute(query)
