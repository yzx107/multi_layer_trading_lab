from pathlib import Path

from multi_layer_trading_lab.adapters.external_repos import (
    ExternalRepoConfig,
    check_external_repos,
)


def test_external_repo_precheck_reports_ready_inputs(tmp_path: Path):
    hshare_root = tmp_path / "Hshare_Lab_v2"
    factor_root = tmp_path / "hk_factor_autoresearch"
    tushare_root = tmp_path / "Ashare_Lab"
    opend_root = tmp_path / "futu-opend-execution"
    data_root = tmp_path / "hshare_data"
    for path in (
        hshare_root / "Scripts",
        factor_root / "factor_contracts",
        factor_root / "harness",
        factor_root / "registry",
        tushare_root,
        opend_root / "src",
        data_root / "candidate_cleaned",
        data_root / "verified",
    ):
        path.mkdir(parents=True)
    for path in (
        hshare_root / "DATA_CONTRACT.md",
        hshare_root / "STAGE_SCHEMA.md",
        hshare_root / "Scripts" / "build_stage_parquet.py",
        hshare_root / "Scripts" / "build_verified_layer.py",
        factor_root / "README.md",
        factor_root / "factor_contracts" / "schema.md",
        factor_root / "harness" / "run_verified_factor.py",
        factor_root / "harness" / "run_gate_b.py",
        tushare_root / "README.md",
        tushare_root / "requirements.txt",
        tushare_root / "test_tushare.py",
        opend_root / "README.md",
        opend_root / "setup.py",
    ):
        path.write_text("", encoding="utf-8")

    result = check_external_repos(
        ExternalRepoConfig(
            hshare_lab_root=hshare_root,
            factor_factory_root=factor_root,
            hshare_data_root=data_root,
            tushare_repo_root=tushare_root,
            opend_execution_root=opend_root,
        )
    )

    assert result.ready
    assert result.reusable_inputs["hshare_verified"] == data_root / "verified"
    assert result.reusable_inputs["factor_registry"] == factor_root / "registry"
    assert result.reusable_inputs["tushare_reference_repo"] == tushare_root
    assert result.reusable_inputs["opend_execution_repo"] == opend_root


def test_external_repo_precheck_reports_missing_contracts(tmp_path: Path):
    result = check_external_repos(
        ExternalRepoConfig(
            hshare_lab_root=tmp_path / "missing_hshare",
            factor_factory_root=tmp_path / "missing_factor",
            hshare_data_root=tmp_path / "missing_data",
            tushare_repo_root=tmp_path / "missing_tushare",
            opend_execution_root=tmp_path / "missing_opend",
        )
    )

    assert not result.ready
    assert result.failed_reasons
    assert any("hshare_lab_v2:missing" in reason for reason in result.failed_reasons)
    assert any("hk_factor_autoresearch:missing" in reason for reason in result.failed_reasons)
    assert any("ashare_lab_tushare:missing" in reason for reason in result.failed_reasons)
    assert any("futu_opend_execution:missing" in reason for reason in result.failed_reasons)
