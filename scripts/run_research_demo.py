import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from multi_layer_trading_lab.pipelines.research_pipeline import run_research_workflow


def main() -> None:
    outputs = run_research_workflow(Path("data"))
    print(
        {
            "daily_features": outputs["daily_features"].height,
            "intraday_l2_features": outputs["intraday_l2_features"].height,
            "signal_events": outputs["signal_events"].height,
            "report": str(outputs["research_report_path"]),
        }
    )


if __name__ == "__main__":
    main()
