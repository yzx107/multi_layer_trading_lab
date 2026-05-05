from pathlib import Path

from multi_layer_trading_lab.research.factor_factory import load_factor_factory_summary


def test_load_factor_factory_summary_joins_gate_b_to_families(tmp_path: Path):
    registry = tmp_path / "registry"
    registry.mkdir()
    (registry / "gate_b_log.tsv").write_text(
        "\t".join(
            [
                "gate_b_id",
                "created_at",
                "pre_eval_id",
                "experiment_id",
                "factor_name",
                "decision",
                "direction_hint",
                "mean_abs_rank_ic",
                "mean_normalized_mutual_info",
                "mean_coverage_ratio",
                "sign_consistency",
                "summary_path",
                "notes",
            ]
        )
        + "\n"
        + "g1\t2026-01-01T00:00:00+00:00\tp1\te1\tfactor_a\tpass\tas_is\t"
        "0.12\t0.03\t0.9\t0.7\t/runs/g1/gate_b_summary.json\tnotes\n",
        encoding="utf-8",
    )
    (registry / "factor_families.tsv").write_text(
        "\t".join(
            [
                "family_id",
                "family_name",
                "mechanism",
                "research_unit",
                "horizon_scope",
                "current_members",
                "expected_regime",
                "forbidden_semantic_assumptions",
                "benchmark_group",
                "status",
                "notes",
            ]
        )
        + "\n"
        + "fam_a\tFamily A\tMechanism\tdate_x_symbol\t1d\tfactor_a,factor_b\t"
        "regime\tno_queue_semantics\tbaseline\tactive\tnotes\n",
        encoding="utf-8",
    )

    summary = load_factor_factory_summary(registry)

    assert summary.candidates.height == 1
    row = summary.candidates.row(0, named=True)
    assert row["factor_name"] == "factor_a"
    assert row["candidate_status"] == "gate_b_passed_review_required"
    assert row["family_id"] == "fam_a"
    assert summary.family_summary.row(0, named=True)["gate_b_pass_count"] == 1
