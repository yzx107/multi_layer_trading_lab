import plistlib
from pathlib import Path

from multi_layer_trading_lab.runtime.launchd import (
    LaunchdDailyOpsConfig,
    build_launchd_daily_ops_plist,
    write_launchd_daily_ops_plist,
)


def test_build_launchd_daily_ops_plist_contains_schedule_and_paths() -> None:
    config = LaunchdDailyOpsConfig(
        label="com.yxin.mttl.daily-ops",
        project_root=Path("/repo"),
        python_executable=Path("/repo/.venv/bin/python"),
        script_path=Path("/repo/scripts/run_daily_ops.py"),
        lake_root=Path("/repo/data/lake"),
        report_path=Path("/repo/data/logs/ops.md"),
        readiness_path=Path("/repo/data/logs/readiness.json"),
        objective_audit_path=Path("/repo/data/logs/objective.json"),
        objective_audit_report_path=Path("/repo/data/logs/objective.md"),
        paper_progress_path=Path("/repo/data/logs/paper_progress.json"),
        stdout_path=Path("/repo/data/logs/out.log"),
        stderr_path=Path("/repo/data/logs/err.log"),
        hour=17,
        minute=45,
    )

    plist = build_launchd_daily_ops_plist(config)

    assert plist["Label"] == "com.yxin.mttl.daily-ops"
    assert plist["WorkingDirectory"] == "/repo"
    assert plist["StartCalendarInterval"] == {"Hour": 17, "Minute": 45}
    assert plist["ProgramArguments"][:2] == [
        "/repo/.venv/bin/python",
        "/repo/scripts/run_daily_ops.py",
    ]
    assert "--readiness-path" in plist["ProgramArguments"]
    assert "/repo/data/logs/readiness.json" in plist["ProgramArguments"]
    assert "--paper-progress-path" in plist["ProgramArguments"]
    assert "/repo/data/logs/paper_progress.json" in plist["ProgramArguments"]
    assert "--submit-opend-paper-simulate-tickets" not in plist["ProgramArguments"]
    assert "--submit-opend-dry-run-tickets" not in plist["ProgramArguments"]


def test_write_launchd_daily_ops_plist_writes_valid_plist(tmp_path) -> None:
    output_path = tmp_path / "com.yxin.mttl.daily-ops.plist"
    config = LaunchdDailyOpsConfig(
        label="com.yxin.mttl.daily-ops",
        project_root=Path("/repo"),
        python_executable=Path("/repo/.venv/bin/python"),
        script_path=Path("/repo/scripts/run_daily_ops.py"),
        lake_root=Path("/repo/data/lake"),
        report_path=Path("/repo/data/logs/ops.md"),
        readiness_path=Path("/repo/data/logs/readiness.json"),
        objective_audit_path=Path("/repo/data/logs/objective.json"),
        objective_audit_report_path=Path("/repo/data/logs/objective.md"),
        paper_progress_path=Path("/repo/data/logs/paper_progress.json"),
        stdout_path=Path("/repo/data/logs/out.log"),
        stderr_path=Path("/repo/data/logs/err.log"),
    )

    write_launchd_daily_ops_plist(output_path, config)

    loaded = plistlib.loads(output_path.read_bytes())
    assert loaded["Label"] == "com.yxin.mttl.daily-ops"
    assert loaded["ProgramArguments"][0] == "/repo/.venv/bin/python"
