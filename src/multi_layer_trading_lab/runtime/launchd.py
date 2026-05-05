from __future__ import annotations

import plistlib
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class LaunchdDailyOpsConfig:
    label: str
    project_root: Path
    python_executable: Path
    script_path: Path
    lake_root: Path
    report_path: Path
    readiness_path: Path
    objective_audit_path: Path
    objective_audit_report_path: Path
    paper_progress_path: Path
    stdout_path: Path
    stderr_path: Path
    hour: int = 18
    minute: int = 30
    account_equity: float = 1_000_000.0
    opend_mode: str = "paper"
    opend_env: str = "SIMULATE"


def build_launchd_daily_ops_plist(config: LaunchdDailyOpsConfig) -> dict[str, object]:
    return {
        "Label": config.label,
        "ProgramArguments": [
            str(config.python_executable),
            str(config.script_path),
            "--lake-root",
            str(config.lake_root),
            "--report-path",
            str(config.report_path),
            "--readiness-path",
            str(config.readiness_path),
            "--objective-audit-path",
            str(config.objective_audit_path),
            "--objective-audit-report-path",
            str(config.objective_audit_report_path),
            "--paper-progress-path",
            str(config.paper_progress_path),
            "--account-equity",
            f"{config.account_equity:.2f}",
            "--opend-mode",
            config.opend_mode,
            "--opend-env",
            config.opend_env,
        ],
        "WorkingDirectory": str(config.project_root),
        "StartCalendarInterval": {
            "Hour": config.hour,
            "Minute": config.minute,
        },
        "StandardOutPath": str(config.stdout_path),
        "StandardErrorPath": str(config.stderr_path),
        "RunAtLoad": False,
    }


def write_launchd_daily_ops_plist(path: Path, config: LaunchdDailyOpsConfig) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(
        plistlib.dumps(
            build_launchd_daily_ops_plist(config),
            sort_keys=False,
        )
    )
    return path
