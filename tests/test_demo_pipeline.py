from pathlib import Path

from multi_layer_trading_lab.pipelines.demo_pipeline import run_demo_stack, signal_frame_to_events


def test_signal_frame_bridge_produces_events(tmp_path: Path):
    outputs = run_demo_stack(tmp_path, execution_mode="dry_run")
    events = signal_frame_to_events(outputs["signal_events"])
    assert len(events) == outputs["signal_events"].height


def test_demo_stack_runs_end_to_end(tmp_path: Path):
    outputs = run_demo_stack(tmp_path, execution_mode="dry_run")
    metrics = outputs["backtest_result"].metrics
    assert outputs["execution_log_path"].exists()
    assert metrics is not None
    assert metrics.fills >= 0
