from __future__ import annotations

from pathlib import Path

import typer

from multi_layer_trading_lab.pipelines.demo_pipeline import run_data_pipeline, run_demo_stack

app = typer.Typer(help="Multi-layer trading lab CLI")


@app.command()
def init_master(data_root: str = "data") -> None:
    outputs = run_data_pipeline(Path(data_root))
    typer.echo(f"security_master rows={outputs['security_master'].height}")


@app.command()
def fetch_history(data_root: str = "data") -> None:
    outputs = run_data_pipeline(Path(data_root))
    typer.echo(f"daily_bars rows={outputs['daily_bars'].height}")


@app.command()
def import_l2(data_root: str = "data") -> None:
    outputs = run_data_pipeline(Path(data_root))
    typer.echo(f"intraday_l2_features rows={outputs['intraday_l2_features'].height}")


@app.command()
def generate_features(data_root: str = "data") -> None:
    outputs = run_data_pipeline(Path(data_root))
    typer.echo(
        f"daily_features rows={outputs['daily_features'].height}, "
        f"intraday_l2_features rows={outputs['intraday_l2_features'].height}"
    )


@app.command()
def demo_backtest(data_root: str = "data", mode: str = "dry_run") -> None:
    outputs = run_demo_stack(Path(data_root), execution_mode=mode)
    result = outputs["backtest_result"]
    metrics = result.metrics
    typer.echo(
        "backtest "
        f"fills={metrics.fills} rejected={metrics.rejected} "
        f"pnl={metrics.total_pnl:.2f} turnover={metrics.turnover:.2f} "
        f"max_dd={metrics.max_drawdown:.2f}"
    )


@app.command()
def dry_run_signals(data_root: str = "data") -> None:
    outputs = run_demo_stack(Path(data_root), execution_mode="dry_run")
    typer.echo(
        f"signals={len(outputs['signal_event_objects'])} "
        f"log={outputs['execution_log_path']}"
    )


if __name__ == "__main__":
    app()
