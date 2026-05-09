from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class PaperOperatorHandoff:
    paper_blocker_report_path: Path
    status: str
    manual_authorization_required: bool
    remediation_automation_allowed: bool
    order_submission_allowed: bool
    next_required_action: str | None
    next_required_evidence: tuple[str, ...]
    next_collect_date: str | None
    next_safe_action: str | None
    failed_reasons: tuple[str, ...]
    operator_actions: tuple[str, ...]
    prohibited_actions: tuple[str, ...]
    verification_commands: tuple[str, ...]
    blocker_details: dict[str, object]

    def to_dict(self) -> dict[str, object]:
        return {
            "paper_blocker_report_path": str(self.paper_blocker_report_path),
            "status": self.status,
            "manual_authorization_required": self.manual_authorization_required,
            "remediation_automation_allowed": self.remediation_automation_allowed,
            "order_submission_allowed": self.order_submission_allowed,
            "next_required_action": self.next_required_action,
            "next_required_evidence": list(self.next_required_evidence),
            "next_collect_date": self.next_collect_date,
            "next_safe_action": self.next_safe_action,
            "failed_reasons": list(self.failed_reasons),
            "operator_actions": list(self.operator_actions),
            "prohibited_actions": list(self.prohibited_actions),
            "verification_commands": list(self.verification_commands),
            "blocker_details": self.blocker_details,
        }


def build_paper_operator_handoff(
    paper_blocker_report_path: Path,
) -> PaperOperatorHandoff:
    if not paper_blocker_report_path.exists():
        return _missing_blocker_report_handoff(paper_blocker_report_path)
    try:
        payload = json.loads(paper_blocker_report_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return _invalid_blocker_report_handoff(paper_blocker_report_path)
    if not isinstance(payload, dict):
        return _invalid_blocker_report_handoff(paper_blocker_report_path)

    failed_reasons = _failed_reasons(payload)
    blocker_details = (
        payload.get("blocker_details") if isinstance(payload.get("blocker_details"), dict) else {}
    )
    kill_switch = _kill_switch_details(blocker_details)
    next_required_action = _optional_str(payload.get("next_required_action"))
    next_required_evidence = _next_required_evidence(payload)
    next_collect_date = _optional_str(payload.get("next_collect_date"))
    if kill_switch is not None and kill_switch.get("enabled") is True:
        next_safe_action = _optional_str(kill_switch.get("next_safe_action")) or (
            "operator_must_explicitly_clear_kill_switch_before_resubmit"
        )
        return PaperOperatorHandoff(
            paper_blocker_report_path=paper_blocker_report_path,
            status="manual_operator_authorization_required",
            manual_authorization_required=True,
            remediation_automation_allowed=False,
            order_submission_allowed=False,
            next_required_action=next_required_action,
            next_required_evidence=next_required_evidence,
            next_collect_date=next_collect_date,
            next_safe_action=next_safe_action,
            failed_reasons=failed_reasons,
            operator_actions=(
                "review_paper_blocker_report",
                "confirm_no_paper_or_live_order_submission_is_running",
                (
                    "clear_kill_switch_outside_automation_only_after_explicit_"
                    "operator_authorization"
                ),
                "rerun_fetch_opend_runtime_status",
                "rerun_paper_blocker_report",
            ),
            prohibited_actions=(
                "do_not_clear_kill_switch_from_automation",
                "do_not_submit_paper_or_live_orders_from_handoff",
                "do_not_treat_handoff_as_execution_evidence",
            ),
            verification_commands=(
                (
                    ".venv/bin/python -m multi_layer_trading_lab.cli "
                    "fetch-opend-runtime-status --output-path "
                    "data/logs/opend_runtime_status.json "
                    "--require-order-submission-ready"
                ),
                (
                    ".venv/bin/python -m multi_layer_trading_lab.cli "
                    "paper-blocker-report --output-path "
                    "data/logs/paper_blocker_report.json"
                ),
            ),
            blocker_details=dict(blocker_details),
        )

    if payload.get("ready_for_next_session") is True:
        return PaperOperatorHandoff(
            paper_blocker_report_path=paper_blocker_report_path,
            status="no_manual_operator_blocker",
            manual_authorization_required=False,
            remediation_automation_allowed=False,
            order_submission_allowed=False,
            next_required_action=next_required_action,
            next_required_evidence=next_required_evidence,
            next_collect_date=next_collect_date,
            next_safe_action="continue_with_normal_paper_session_gate",
            failed_reasons=failed_reasons,
            operator_actions=("continue_with_normal_paper_session_gate",),
            prohibited_actions=("do_not_treat_handoff_as_execution_evidence",),
            verification_commands=(),
            blocker_details=dict(blocker_details),
        )

    return PaperOperatorHandoff(
        paper_blocker_report_path=paper_blocker_report_path,
        status="blocked_review_required",
        manual_authorization_required=False,
        remediation_automation_allowed=False,
        order_submission_allowed=False,
        next_required_action=next_required_action,
        next_required_evidence=next_required_evidence,
        next_collect_date=next_collect_date,
        next_safe_action=next_required_action,
        failed_reasons=failed_reasons,
        operator_actions=(
            "review_failed_reasons",
            "resolve_next_required_action",
            "rerun_paper_blocker_report",
        ),
        prohibited_actions=(
            "do_not_submit_paper_or_live_orders_from_handoff",
            "do_not_treat_handoff_as_execution_evidence",
        ),
        verification_commands=(
            (
                ".venv/bin/python -m multi_layer_trading_lab.cli "
                "paper-blocker-report --output-path "
                "data/logs/paper_blocker_report.json"
            ),
        ),
        blocker_details=dict(blocker_details),
    )


def write_paper_operator_handoff(
    *,
    paper_blocker_report_path: Path,
    output_path: Path,
) -> PaperOperatorHandoff:
    handoff = build_paper_operator_handoff(paper_blocker_report_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(handoff.to_dict(), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return handoff


def _missing_blocker_report_handoff(path: Path) -> PaperOperatorHandoff:
    return PaperOperatorHandoff(
        paper_blocker_report_path=path,
        status="blocked_missing_paper_blocker_report",
        manual_authorization_required=False,
        remediation_automation_allowed=False,
        order_submission_allowed=False,
        next_required_action="run_paper_blocker_report",
        next_required_evidence=(),
        next_collect_date=None,
        next_safe_action="run_paper_blocker_report",
        failed_reasons=("missing_paper_blocker_report",),
        operator_actions=("run_paper_blocker_report",),
        prohibited_actions=("do_not_submit_paper_or_live_orders_from_handoff",),
        verification_commands=(
            (
                ".venv/bin/python -m multi_layer_trading_lab.cli "
                "paper-blocker-report --output-path "
                "data/logs/paper_blocker_report.json"
            ),
        ),
        blocker_details={},
    )


def _invalid_blocker_report_handoff(path: Path) -> PaperOperatorHandoff:
    return PaperOperatorHandoff(
        paper_blocker_report_path=path,
        status="blocked_invalid_paper_blocker_report",
        manual_authorization_required=False,
        remediation_automation_allowed=False,
        order_submission_allowed=False,
        next_required_action="regenerate_paper_blocker_report",
        next_required_evidence=(),
        next_collect_date=None,
        next_safe_action="regenerate_paper_blocker_report",
        failed_reasons=("invalid_paper_blocker_report",),
        operator_actions=("regenerate_paper_blocker_report",),
        prohibited_actions=("do_not_submit_paper_or_live_orders_from_handoff",),
        verification_commands=(
            (
                ".venv/bin/python -m multi_layer_trading_lab.cli "
                "paper-blocker-report --output-path "
                "data/logs/paper_blocker_report.json"
            ),
        ),
        blocker_details={},
    )


def _failed_reasons(payload: dict[str, object]) -> tuple[str, ...]:
    reasons: list[str] = []
    for key in ("failed_reasons", "next_session_failed_reasons"):
        value = payload.get(key)
        if isinstance(value, list):
            reasons.extend(str(reason) for reason in value if str(reason))
    return tuple(dict.fromkeys(reasons))


def _next_required_evidence(payload: dict[str, object]) -> tuple[str, ...]:
    value = payload.get("next_required_evidence")
    if not isinstance(value, list):
        return ()
    return tuple(dict.fromkeys(str(item) for item in value if str(item)))


def _kill_switch_details(blocker_details: object) -> dict[str, object] | None:
    if not isinstance(blocker_details, dict):
        return None
    kill_switch = blocker_details.get("opend_kill_switch")
    return kill_switch if isinstance(kill_switch, dict) else None


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if text else None
