from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ExternalRepoConfig:
    hshare_lab_root: Path
    factor_factory_root: Path
    hshare_data_root: Path
    tushare_repo_root: Path
    opend_execution_root: Path


@dataclass(frozen=True)
class ExternalRepoCheck:
    name: str
    ready: bool
    root: Path
    missing_paths: tuple[Path, ...]
    evidence_paths: tuple[Path, ...]


@dataclass(frozen=True)
class ExternalRepoPrecheck:
    hshare_lab: ExternalRepoCheck
    factor_factory: ExternalRepoCheck
    tushare_repo: ExternalRepoCheck
    opend_execution: ExternalRepoCheck
    reusable_inputs: dict[str, Path]

    @property
    def ready(self) -> bool:
        return (
            self.hshare_lab.ready
            and self.factor_factory.ready
            and self.tushare_repo.ready
            and self.opend_execution.ready
        )

    @property
    def failed_reasons(self) -> tuple[str, ...]:
        reasons: list[str] = []
        for check in (
            self.hshare_lab,
            self.factor_factory,
            self.tushare_repo,
            self.opend_execution,
        ):
            for path in check.missing_paths:
                reasons.append(f"{check.name}:missing:{path}")
        return tuple(reasons)


def default_external_repo_config() -> ExternalRepoConfig:
    return ExternalRepoConfig(
        hshare_lab_root=Path("/Users/yxin/AI_Workstation/Hshare_Lab_v2"),
        factor_factory_root=Path("/Users/yxin/AI_Workstation/hk_factor_autoresearch"),
        hshare_data_root=Path("/Volumes/Data/港股Tick数据"),
        tushare_repo_root=Path("/Users/yxin/AI_Workstation/Ashare_Lab"),
        opend_execution_root=Path("/Users/yxin/AI_Workstation/futu-opend-execution"),
    )


def check_external_repos(config: ExternalRepoConfig) -> ExternalRepoPrecheck:
    hshare_required = (
        config.hshare_lab_root / "DATA_CONTRACT.md",
        config.hshare_lab_root / "STAGE_SCHEMA.md",
        config.hshare_lab_root / "Scripts" / "build_stage_parquet.py",
        config.hshare_lab_root / "Scripts" / "build_verified_layer.py",
        config.hshare_data_root / "candidate_cleaned",
        config.hshare_data_root / "verified",
    )
    factor_required = (
        config.factor_factory_root / "README.md",
        config.factor_factory_root / "factor_contracts" / "schema.md",
        config.factor_factory_root / "harness" / "run_verified_factor.py",
        config.factor_factory_root / "harness" / "run_gate_b.py",
        config.factor_factory_root / "registry",
    )
    tushare_required = (
        config.tushare_repo_root / "README.md",
        config.tushare_repo_root / "requirements.txt",
        config.tushare_repo_root / "test_tushare.py",
    )
    opend_required = (
        config.opend_execution_root / "README.md",
        config.opend_execution_root / "setup.py",
        config.opend_execution_root / "src",
    )

    hshare_missing = tuple(path for path in hshare_required if not path.exists())
    factor_missing = tuple(path for path in factor_required if not path.exists())
    tushare_missing = tuple(path for path in tushare_required if not path.exists())
    opend_missing = tuple(path for path in opend_required if not path.exists())

    return ExternalRepoPrecheck(
        hshare_lab=ExternalRepoCheck(
            name="hshare_lab_v2",
            ready=not hshare_missing,
            root=config.hshare_lab_root,
            missing_paths=hshare_missing,
            evidence_paths=tuple(path for path in hshare_required if path.exists()),
        ),
        factor_factory=ExternalRepoCheck(
            name="hk_factor_autoresearch",
            ready=not factor_missing,
            root=config.factor_factory_root,
            missing_paths=factor_missing,
            evidence_paths=tuple(path for path in factor_required if path.exists()),
        ),
        tushare_repo=ExternalRepoCheck(
            name="ashare_lab_tushare",
            ready=not tushare_missing,
            root=config.tushare_repo_root,
            missing_paths=tushare_missing,
            evidence_paths=tuple(path for path in tushare_required if path.exists()),
        ),
        opend_execution=ExternalRepoCheck(
            name="futu_opend_execution",
            ready=not opend_missing,
            root=config.opend_execution_root,
            missing_paths=opend_missing,
            evidence_paths=tuple(path for path in opend_required if path.exists()),
        ),
        reusable_inputs={
            "hshare_stage": config.hshare_data_root / "candidate_cleaned",
            "hshare_verified": config.hshare_data_root / "verified",
            "factor_runs": config.factor_factory_root / "runs",
            "factor_registry": config.factor_factory_root / "registry",
            "tushare_reference_repo": config.tushare_repo_root,
            "opend_execution_repo": config.opend_execution_root,
        },
    )
