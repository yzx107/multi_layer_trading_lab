from pathlib import Path

from multi_layer_trading_lab.pipelines.research_pipeline import run_research_workflow


def test_research_workflow_runs_end_to_end(tmp_path: Path):
    outputs = run_research_workflow(tmp_path)
    assert outputs["daily_features"].height > 0
    assert outputs["intraday_l2_features"].height > 0
    assert outputs["l2_order_add_features"].height > 0
    assert outputs["order_add_signal_candidates"].height > 0
    assert outputs["validation_summary"].height >= 3
    assert outputs["research_report_path"].exists()

    registry = outputs["feature_registry"]
    assert "order_add_v1" in registry["feature_set_version"].to_list()
    assert "l2_order_add" in outputs["lead_lag_summary"]["domain"].to_list()
    assert "order_add_pressure" in outputs["signal_events"]["strategy_id"].to_list()
