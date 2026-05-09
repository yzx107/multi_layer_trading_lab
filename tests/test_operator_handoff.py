from __future__ import annotations

import json

from multi_layer_trading_lab.execution.operator_handoff import (
    build_paper_operator_handoff,
)


def test_paper_operator_handoff_requires_manual_kill_switch_authorization(
    tmp_path,
) -> None:
    blocker = tmp_path / "paper_blocker.json"
    blocker.write_text(
        json.dumps(
            {
                "ready_for_next_session": False,
                "next_required_action": (
                    "clear_opend_kill_switch_then_resubmit_paper_simulate"
                ),
                "failed_reasons": [
                    "opend_kill_switch_enabled",
                    "missing_submitted_responses",
                ],
                "next_session_failed_reasons": ["opend_kill_switch_enabled"],
                "next_required_evidence": [
                    "collect_19_broker_reconciled_paper_sessions",
                    "continue_until_positive_reconciled_net_pnl",
                ],
                "blocker_details": {
                    "opend_kill_switch": {
                        "enabled": True,
                        "kill_switch_file": "/tmp/futu-opend-execution.KILL",
                        "requires_manual_operator_authorization": True,
                        "automation_allowed": False,
                        "next_safe_action": (
                            "operator_must_explicitly_clear_kill_switch_before_resubmit"
                        ),
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    handoff = build_paper_operator_handoff(blocker)

    assert handoff.status == "manual_operator_authorization_required"
    assert handoff.manual_authorization_required is True
    assert handoff.remediation_automation_allowed is False
    assert handoff.order_submission_allowed is False
    assert (
        handoff.next_safe_action
        == "operator_must_explicitly_clear_kill_switch_before_resubmit"
    )
    assert handoff.next_required_evidence == (
        "collect_19_broker_reconciled_paper_sessions",
        "continue_until_positive_reconciled_net_pnl",
    )
    assert handoff.to_dict()["next_required_evidence"] == [
        "collect_19_broker_reconciled_paper_sessions",
        "continue_until_positive_reconciled_net_pnl",
    ]
    assert "do_not_clear_kill_switch_from_automation" in handoff.prohibited_actions
    assert "do_not_submit_paper_or_live_orders_from_handoff" in handoff.prohibited_actions
    assert "opend_kill_switch_enabled" in handoff.failed_reasons


def test_paper_operator_handoff_blocks_missing_report(tmp_path) -> None:
    missing = tmp_path / "missing.json"

    handoff = build_paper_operator_handoff(missing)

    assert handoff.status == "blocked_missing_paper_blocker_report"
    assert handoff.next_required_action == "run_paper_blocker_report"
    assert handoff.order_submission_allowed is False
    assert handoff.failed_reasons == ("missing_paper_blocker_report",)


def test_paper_operator_handoff_allows_normal_gate_when_no_manual_blocker(
    tmp_path,
) -> None:
    blocker = tmp_path / "paper_blocker.json"
    blocker.write_text(
        json.dumps(
            {
                "ready_for_next_session": True,
                "next_required_action": "collect_today_paper_session",
                "failed_reasons": [],
                "next_session_failed_reasons": [],
                "blocker_details": {},
            }
        ),
        encoding="utf-8",
    )

    handoff = build_paper_operator_handoff(blocker)

    assert handoff.status == "no_manual_operator_blocker"
    assert handoff.manual_authorization_required is False
    assert handoff.order_submission_allowed is False
    assert handoff.next_safe_action == "continue_with_normal_paper_session_gate"
