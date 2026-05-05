from pathlib import Path

import polars as pl

from multi_layer_trading_lab.research.lookahead import audit_factor_factory_lookahead_lineage


def test_factor_factory_lookahead_lineage_passes_with_gate_b_and_pre_eval(tmp_path: Path):
    registry = tmp_path / "registry"
    registry.mkdir()
    gate_summary = tmp_path / "gate_b_summary.json"
    pre_summary = tmp_path / "pre_eval_summary.json"
    gate_summary.write_text("{}", encoding="utf-8")
    pre_summary.write_text("{}", encoding="utf-8")
    (registry / "gate_b_log.tsv").write_text(
        "gate_b_id\tcreated_at\tpre_eval_id\texperiment_id\tfactor_name\tdecision\t"
        "direction_hint\tmean_abs_rank_ic\tmean_normalized_mutual_info\tmean_coverage_ratio\t"
        "sign_consistency\tsummary_path\tnotes\n"
        f"g1\t2026-01-02T00:00:00+00:00\tp1\te1\tfactor_a\tpass\tas_is\t"
        f"0.1\t0.02\t0.9\t0.7\t{gate_summary}\tnotes\n",
        encoding="utf-8",
    )
    (registry / "pre_eval_log.tsv").write_text(
        "pre_eval_id\tcreated_at\texperiment_id\tfactor_name\tscore_column\tlabel_name\t"
        "evaluated_dates\tjoined_rows\tmean_rank_ic\tmean_abs_rank_ic\t"
        "mean_top_bottom_spread\tsummary_path\tnotes\n"
        f"p1\t2026-01-01T00:00:00+00:00\te1\tfactor_a\tfactor_score\t"
        f"forward_return_1d_close_like\t2026-01-05\t100\t0.1\t0.1\t0.01\t{pre_summary}\tnotes\n",
        encoding="utf-8",
    )
    portfolio = pl.DataFrame(
        {
            "factor_name": ["factor_a"],
            "candidate_status": ["review_candidate"],
        }
    )

    evidence = audit_factor_factory_lookahead_lineage(portfolio, registry_root=registry)

    assert evidence.passed
    assert evidence.reviewed_factor_count == 1
    assert evidence.matched_pre_eval_count == 1


def test_factor_factory_lookahead_lineage_blocks_missing_registry(tmp_path: Path):
    evidence = audit_factor_factory_lookahead_lineage(
        pl.DataFrame({"factor_name": ["factor_a"], "candidate_status": ["review_candidate"]}),
        registry_root=tmp_path / "missing",
    )

    assert not evidence.passed
    assert "missing_gate_b_log" in evidence.failed_reasons
