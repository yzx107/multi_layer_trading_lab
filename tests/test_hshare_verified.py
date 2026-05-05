import json
from pathlib import Path

from multi_layer_trading_lab.research.hshare_verified import evaluate_hshare_verified_summary


def _write_summary(path: Path, *, selected_dates: int, partial: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "year": 2025,
                "status": "completed",
                "completed_count": selected_dates * 2,
                "failed_count": 0,
                "selection": {
                    "selected_date_count": selected_dates,
                    "is_partial": partial,
                },
                "tables": {
                    "verified_orders": {"rows": 1000},
                    "verified_trades": {"rows": 500},
                },
            }
        ),
        encoding="utf-8",
    )


def test_hshare_verified_evidence_approves_complete_summary(tmp_path: Path):
    summary_path = tmp_path / "verified" / "manifests" / "year=2025" / "summary.json"
    _write_summary(summary_path, selected_dates=246, partial=False)

    evidence = evaluate_hshare_verified_summary(summary_path)

    assert evidence.ready
    assert evidence.selected_date_count == 246
    assert evidence.orders_rows == 1000


def test_hshare_verified_evidence_blocks_partial_or_sparse_summary(tmp_path: Path):
    summary_path = tmp_path / "verified" / "manifests" / "year=2026" / "summary.json"
    _write_summary(summary_path, selected_dates=3, partial=True)

    evidence = evaluate_hshare_verified_summary(summary_path)

    assert not evidence.ready
    assert "insufficient_hshare_verified_dates" in evidence.failed_reasons
    assert "hshare_verified_partial_selection" in evidence.failed_reasons
