from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from multi_layer_trading_lab.adapters.external_repos import (
    ExternalRepoConfig,
    check_external_repos,
)


@dataclass(frozen=True, slots=True)
class ResearchInputManifestConfig:
    external_repos: ExternalRepoConfig
    max_sample_paths: int = 8


def _path_payload(path: Path) -> dict[str, Any]:
    return {
        "path": str(path),
        "exists": path.exists(),
        "is_dir": path.is_dir(),
        "is_file": path.is_file(),
    }


def _sample_files(root: Path, patterns: tuple[str, ...], max_sample_paths: int) -> list[str]:
    if not root.exists():
        return []
    files: list[Path] = []
    for pattern in patterns:
        files.extend(path for path in root.rglob(pattern) if path.is_file())
        if len(files) >= max_sample_paths:
            break
    return [str(path) for path in sorted(files)[:max_sample_paths]]


def _count_files(root: Path, patterns: tuple[str, ...]) -> int:
    if not root.exists():
        return 0
    count = 0
    for pattern in patterns:
        count += sum(1 for path in root.rglob(pattern) if path.is_file())
    return count


def build_research_input_manifest(config: ResearchInputManifestConfig) -> dict[str, Any]:
    precheck = check_external_repos(config.external_repos)
    hshare_stage = config.external_repos.hshare_data_root / "candidate_cleaned"
    hshare_verified = config.external_repos.hshare_data_root / "verified"
    factor_registry = config.external_repos.factor_factory_root / "registry"
    factor_runs = config.external_repos.factor_factory_root / "runs"

    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "ready": precheck.ready,
        "external_repos": {
            "hshare_lab_v2": {
                "ready": precheck.hshare_lab.ready,
                "root": str(precheck.hshare_lab.root),
                "missing_paths": [str(path) for path in precheck.hshare_lab.missing_paths],
            },
            "hk_factor_autoresearch": {
                "ready": precheck.factor_factory.ready,
                "root": str(precheck.factor_factory.root),
                "missing_paths": [str(path) for path in precheck.factor_factory.missing_paths],
            },
            "ashare_lab_tushare": {
                "ready": precheck.tushare_repo.ready,
                "root": str(precheck.tushare_repo.root),
                "missing_paths": [str(path) for path in precheck.tushare_repo.missing_paths],
            },
            "futu_opend_execution": {
                "ready": precheck.opend_execution.ready,
                "root": str(precheck.opend_execution.root),
                "missing_paths": [str(path) for path in precheck.opend_execution.missing_paths],
            },
        },
        "research_inputs": {
            "hshare_stage": {
                **_path_payload(hshare_stage),
                "role": "structural exploration and coverage checks only",
                "file_count": _count_files(hshare_stage, ("*.parquet",)),
                "sample_files": _sample_files(
                    hshare_stage,
                    ("*.parquet",),
                    config.max_sample_paths,
                ),
            },
            "hshare_verified": {
                **_path_payload(hshare_verified),
                "role": "default HK L2 research input",
                "manifest_count": _count_files(
                    hshare_verified / "manifests",
                    ("*.json", "*.jsonl"),
                ),
                "parquet_count": _count_files(hshare_verified, ("*.parquet",)),
                "sample_files": _sample_files(
                    hshare_verified,
                    ("*.parquet", "*.json", "*.jsonl"),
                    config.max_sample_paths,
                ),
            },
            "factor_registry": {
                **_path_payload(factor_registry),
                "role": "factor candidates, families, gate logs and promotion evidence",
                "file_count": _count_files(factor_registry, ("*.tsv", "*.json", "*.md")),
                "sample_files": _sample_files(
                    factor_registry,
                    ("*.tsv", "*.json", "*.md"),
                    config.max_sample_paths,
                ),
            },
            "factor_runs": {
                **_path_payload(factor_runs),
                "role": "fixed harness outputs for approved or reviewable factor experiments",
                "summary_count": _count_files(factor_runs, ("*summary.json",)),
                "sample_files": _sample_files(
                    factor_runs,
                    ("*summary.json", "factor_output.parquet"),
                    config.max_sample_paths,
                ),
            },
            "tushare_reference_repo": {
                **_path_payload(config.external_repos.tushare_repo_root),
                "role": "Tushare connectivity reference and smoke script",
            },
            "opend_execution_repo": {
                **_path_payload(config.external_repos.opend_execution_root),
                "role": "Futu OpenD execution prototype and safety model reference",
            },
        },
        "current_repo_scope": {
            "owns": [
                "research input manifest",
                "Bayesian posterior and lead-lag / transfer entropy orchestration",
                "Kelly sizing and personal account risk gates",
                "OpenD dry-run / paper / live readiness and reconciliation",
                "daily ops and go-live readiness reporting",
            ],
            "does_not_own": [
                "H-share raw L2 cleaning and DQA",
                "H-share verified layer semantics",
                "ordinary factor factory harness and promotion gates",
            ],
        },
    }
