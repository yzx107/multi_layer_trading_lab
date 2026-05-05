from pathlib import Path

from multi_layer_trading_lab.adapters.external_repos import ExternalRepoConfig
from multi_layer_trading_lab.research.input_manifest import (
    ResearchInputManifestConfig,
    build_research_input_manifest,
)


def _touch(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("", encoding="utf-8")


def test_research_input_manifest_reuses_external_artifacts(tmp_path: Path):
    hshare = tmp_path / "Hshare_Lab_v2"
    factor = tmp_path / "hk_factor_autoresearch"
    tushare = tmp_path / "Ashare_Lab"
    opend = tmp_path / "futu-opend-execution"
    data = tmp_path / "hshare_data"
    for path in (
        hshare / "DATA_CONTRACT.md",
        hshare / "STAGE_SCHEMA.md",
        hshare / "Scripts" / "build_stage_parquet.py",
        hshare / "Scripts" / "build_verified_layer.py",
        data / "candidate_cleaned" / "orders" / "date=2026-01-05" / "orders.parquet",
        data / "verified" / "manifests" / "year=2026" / "summary.json",
        factor / "README.md",
        factor / "factor_contracts" / "schema.md",
        factor / "harness" / "run_verified_factor.py",
        factor / "harness" / "run_gate_b.py",
        factor / "registry" / "factor_families.tsv",
        factor / "runs" / "run_001" / "gate_b_summary.json",
        tushare / "README.md",
        tushare / "requirements.txt",
        tushare / "test_tushare.py",
        opend / "README.md",
        opend / "setup.py",
        opend / "src" / ".keep",
    ):
        _touch(path)

    manifest = build_research_input_manifest(
        ResearchInputManifestConfig(
            external_repos=ExternalRepoConfig(
                hshare_lab_root=hshare,
                factor_factory_root=factor,
                hshare_data_root=data,
                tushare_repo_root=tushare,
                opend_execution_root=opend,
            ),
        )
    )

    assert manifest["ready"] is True
    assert manifest["research_inputs"]["hshare_stage"]["file_count"] == 1
    assert manifest["research_inputs"]["hshare_verified"]["manifest_count"] == 1
    assert manifest["research_inputs"]["factor_registry"]["file_count"] == 1
    assert manifest["research_inputs"]["factor_runs"]["summary_count"] == 1
    assert manifest["external_repos"]["ashare_lab_tushare"]["ready"] is True
    assert manifest["external_repos"]["futu_opend_execution"]["ready"] is True
    assert "H-share raw L2 cleaning and DQA" in manifest["current_repo_scope"]["does_not_own"]
