from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import polars as pl

from multi_layer_trading_lab.backtest.engine import EventDrivenBacktester
from multi_layer_trading_lab.backtest.types import ExecutionMode, OrderType, Side, SignalEvent
from multi_layer_trading_lab.execution.adapters import DryRunBrokerAdapter, PaperBrokerAdapter
from multi_layer_trading_lab.execution.interfaces import MarketDataAdapter, Quote
from multi_layer_trading_lab.execution.logging import ExecutionLogWriter
from multi_layer_trading_lab.execution.order_manager import OrderManager
from multi_layer_trading_lab.adapters.l2_loader.loader import L2Loader
from multi_layer_trading_lab.adapters.tushare.client import TushareClient
from multi_layer_trading_lab.features.daily.basic import build_daily_features
from multi_layer_trading_lab.features.intraday.basic import build_intraday_bar_features, summarize_open_window
from multi_layer_trading_lab.features.l2.basic import build_l2_features
from multi_layer_trading_lab.risk.manager import RiskLimits, RiskManager
from multi_layer_trading_lab.settings import settings
from multi_layer_trading_lab.signals.demo import build_demo_signals
from multi_layer_trading_lab.storage.parquet_store import DuckDBCatalog, ParquetStore


def make_sample_l2_file(root: Path, symbol: str = "00700.HK", trade_date: str = "2026-04-01") -> Path:
    root.mkdir(parents=True, exist_ok=True)
    start = datetime(2026, 4, 1, 9, 20, 0)
    rows = []
    for idx in range(240):
        ts = start + timedelta(seconds=15 * idx)
        rows.append(
            {
                "symbol": symbol,
                "ts": ts,
                "bid_px_1": 100.0 + idx * 0.01,
                "ask_px_1": 100.02 + idx * 0.01,
                "bid_sz_1": 1000 + idx,
                "ask_sz_1": 900 + idx,
                "last_px": 100.01 + idx * 0.01,
                "last_sz": 100 + (idx % 10),
                "side": "BUY" if idx % 3 else "SELL",
                "cancel_flag": idx % 11 == 0,
            }
        )
    path = root / f"l2_{trade_date}.parquet"
    pl.DataFrame(rows).write_parquet(path)
    return path


def run_data_pipeline(data_root: Path | None = None) -> dict[str, pl.DataFrame]:
    root = data_root or settings.data_root
    store = ParquetStore(root / "lake")
    catalog = DuckDBCatalog(root / "catalog" / "research.duckdb")
    tushare = TushareClient(token=settings.tushare_token)

    security_master = tushare.fetch_security_master("HK")
    daily_bars = tushare.fetch_daily_bars("00700.HK", date(2026, 3, 20), date(2026, 4, 2))
    minute_bars = tushare.fetch_minute_bars("00700.HK", date(2026, 4, 1), minutes=60)
    daily_features = build_daily_features(daily_bars)
    intraday_bar_features = build_intraday_bar_features(minute_bars)
    intraday_summary = summarize_open_window(intraday_bar_features)

    l2_root = root / "raw" / "l2"
    make_sample_l2_file(l2_root)
    loader = L2Loader(l2_root)
    l2_frame = loader.load_trade_date("2026-04-01")
    l2_agg = loader.aggregate(l2_frame, "1m")
    l2_features = build_l2_features(l2_agg)

    signals = build_demo_signals(daily_features, l2_features)

    outputs = {
        "security_master": security_master,
        "daily_bars": daily_bars,
        "minute_bars": minute_bars,
        "daily_features": daily_features,
        "intraday_summary": intraday_summary,
        "intraday_l2_features": l2_features,
        "signal_events": signals,
    }
    for dataset, frame in outputs.items():
        path = store.write(dataset, frame)
        catalog.register_parquet(dataset, path)
    return outputs


class FrameMarketDataAdapter(MarketDataAdapter):
    def __init__(self, minute_bars: pl.DataFrame):
        self.quotes: dict[str, Quote] = {}
        latest = minute_bars.sort(["symbol", "ts"]).group_by("symbol").tail(1)
        for row in latest.iter_rows(named=True):
            bid = float(row["close"]) - 0.02
            ask = float(row["close"]) + 0.02
            self.quotes[row["symbol"]] = Quote(
                symbol=row["symbol"],
                timestamp=row["ts"],
                bid=bid,
                ask=ask,
                last=float(row["close"]),
                bid_size=float(row["volume"]),
                ask_size=float(row["volume"]),
            )

    def get_quote(self, symbol: str, timestamp: datetime | None = None) -> Quote | None:
        return self.quotes.get(symbol)


def signal_frame_to_events(signals: pl.DataFrame, quantity: int = 100) -> list[SignalEvent]:
    events: list[SignalEvent] = []
    for row in signals.iter_rows(named=True):
        trade_date = row["trade_date"]
        ts = datetime.combine(trade_date, datetime.min.time(), tzinfo=timezone.utc).replace(
            hour=9, minute=35
        )
        side = Side(row["side"])
        events.append(
            SignalEvent(
                signal_id=f"{row['symbol']}-{trade_date.isoformat()}-{row['signal_name']}",
                timestamp=ts,
                symbol=row["symbol"],
                side=side,
                quantity=quantity,
                order_type=OrderType.MARKET,
                strategy_id=row["signal_name"],
                alpha_name=row["signal_name"],
                metadata={
                    "posterior_mean": row["posterior_mean"],
                    "risk_budget": row["risk_budget"],
                    "lineage": row["lineage"],
                },
            )
        )
    return events


def run_demo_stack(
    data_root: Path | None = None,
    execution_mode: str = "dry_run",
    execution_log_path: Path | None = None,
) -> dict[str, object]:
    outputs = run_data_pipeline(data_root)
    market_data = FrameMarketDataAdapter(outputs["minute_bars"])
    log_path = execution_log_path or (data_root or settings.data_root) / "logs" / "execution_log.jsonl"
    risk_manager = RiskManager(
        limits=RiskLimits(
            max_position_notional=2_000_000.0,
            max_strategy_notional=2_000_000.0,
            max_daily_drawdown=50_000.0,
            max_open_slippage_bps=100.0,
        )
    )
    broker = DryRunBrokerAdapter() if execution_mode == ExecutionMode.DRY_RUN.value else PaperBrokerAdapter()
    order_manager = OrderManager(
        broker=broker,
        risk_manager=risk_manager,
        execution_log_writer=ExecutionLogWriter(log_path),
    )
    signal_events = signal_frame_to_events(outputs["signal_events"])
    backtest = EventDrivenBacktester(market_data=market_data, order_manager=order_manager)
    result = backtest.run(signal_events)
    return {
        **outputs,
        "signal_event_objects": signal_events,
        "backtest_result": result,
        "execution_log_path": log_path,
    }
