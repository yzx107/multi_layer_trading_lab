import argparse
from pathlib import Path

from multi_layer_trading_lab.runtime.launchd import (
    LaunchdDailyOpsConfig,
    write_launchd_daily_ops_plist,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Render a launchd plist for daily ops.")
    parser.add_argument("--output-path", default="configs/launchd/com.yxin.mttl.daily-ops.plist")
    parser.add_argument("--label", default="com.yxin.mttl.daily-ops")
    parser.add_argument("--project-root", default=str(Path.cwd()))
    parser.add_argument("--python-executable", default=".venv/bin/python")
    parser.add_argument("--script-path", default="scripts/run_daily_ops.py")
    parser.add_argument("--lake-root", default="data/lake")
    parser.add_argument("--report-path", default="data/logs/ops_daily_report.md")
    parser.add_argument("--readiness-path", default="data/logs/go_live_readiness.json")
    parser.add_argument("--objective-audit-path", default="data/logs/objective_audit.json")
    parser.add_argument("--objective-audit-report-path", default="data/logs/objective_audit.md")
    parser.add_argument("--paper-progress-path", default="data/logs/paper_progress.json")
    parser.add_argument("--stdout-path", default="data/logs/launchd_daily_ops.out.log")
    parser.add_argument("--stderr-path", default="data/logs/launchd_daily_ops.err.log")
    parser.add_argument("--hour", type=int, default=18)
    parser.add_argument("--minute", type=int, default=30)
    parser.add_argument("--account-equity", type=float, default=1_000_000.0)
    parser.add_argument("--opend-mode", default="paper")
    parser.add_argument("--opend-env", default="SIMULATE")
    args = parser.parse_args()

    project_root = Path(args.project_root).resolve()
    config = LaunchdDailyOpsConfig(
        label=args.label,
        project_root=project_root,
        python_executable=(project_root / args.python_executable).resolve(),
        script_path=(project_root / args.script_path).resolve(),
        lake_root=(project_root / args.lake_root).resolve(),
        report_path=(project_root / args.report_path).resolve(),
        readiness_path=(project_root / args.readiness_path).resolve(),
        objective_audit_path=(project_root / args.objective_audit_path).resolve(),
        objective_audit_report_path=(project_root / args.objective_audit_report_path).resolve(),
        paper_progress_path=(project_root / args.paper_progress_path).resolve(),
        stdout_path=(project_root / args.stdout_path).resolve(),
        stderr_path=(project_root / args.stderr_path).resolve(),
        hour=args.hour,
        minute=args.minute,
        account_equity=args.account_equity,
        opend_mode=args.opend_mode,
        opend_env=args.opend_env,
    )
    output = write_launchd_daily_ops_plist(Path(args.output_path), config)
    print(f"launchd_plist={output}")


if __name__ == "__main__":
    main()
