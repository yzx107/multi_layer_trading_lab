from __future__ import annotations

import json
from pathlib import Path

from multi_layer_trading_lab.backtest.types import ExecutionLogRecord


class ExecutionLogWriter:
    """Append execution events as jsonl for easy local inspection and replay."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def append(self, record: ExecutionLogRecord) -> None:
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record.as_dict(), ensure_ascii=True))
            handle.write("\n")

