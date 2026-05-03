import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from multi_layer_trading_lab.pipelines.demo_pipeline import run_demo_stack


def main() -> None:
    outputs = run_demo_stack(Path("data"), execution_mode="dry_run")
    for name in [
        "security_master",
        "daily_bars",
        "minute_bars",
        "daily_features",
        "intraday_summary",
        "intraday_l2_features",
        "signal_events",
    ]:
        frame = outputs[name]
        print(f"{name}: rows={frame.height}")
    metrics = outputs["backtest_result"].metrics
    print(
        "backtest_metrics:",
        {
            "fills": metrics.fills,
            "rejected": metrics.rejected,
            "pnl": round(metrics.total_pnl, 4),
            "turnover": round(metrics.turnover, 4),
            "max_drawdown": round(metrics.max_drawdown, 4),
        },
    )
    print(f"execution_log: {outputs['execution_log_path']}")


if __name__ == "__main__":
    main()
