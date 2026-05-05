import polars as pl

from multi_layer_trading_lab.execution.paper_plan import (
    PaperSessionPlanInput,
    build_paper_session_plan,
)
from multi_layer_trading_lab.risk.profile import personal_trader_profile


def test_build_paper_session_plan_from_external_portfolio(tmp_path):
    plan = build_paper_session_plan(
        PaperSessionPlanInput(
            portfolio=pl.DataFrame(
                {
                    "factor_name": ["factor_a", "factor_b"],
                    "direction_hint": ["as_is_candidate", "inverse_candidate"],
                    "candidate_status": ["review_candidate", "blocked"],
                    "target_notional": [25_000.0, 0.0],
                }
            ),
            account_profile=personal_trader_profile(account_equity=1_000_000),
            session_id="paper_001",
            execution_log_path=tmp_path / "execution.jsonl",
            broker_report_path=tmp_path / "broker.json",
        )
    )

    assert plan["ready_for_paper"] is True
    assert plan["planned_order_count"] == 1
    assert plan["planned_total_notional"] == 25_000.0
    assert plan["orders"][0]["side"] == "buy"


def test_build_paper_session_plan_requires_simulate_paper_mode(tmp_path):
    plan = build_paper_session_plan(
        PaperSessionPlanInput(
            portfolio=pl.DataFrame(
                {
                    "factor_name": ["factor_a"],
                    "direction_hint": ["as_is_candidate"],
                    "candidate_status": ["review_candidate"],
                    "target_notional": [25_000.0],
                }
            ),
            account_profile=personal_trader_profile(account_equity=1_000_000),
            session_id="paper_001",
            execution_log_path=tmp_path / "execution.jsonl",
            broker_report_path=tmp_path / "broker.json",
            opend_env="REAL",
            opend_mode="live",
        )
    )

    assert plan["ready_for_paper"] is False
    assert "paper_plan_requires_paper_mode" in plan["failed_reasons"]
    assert "paper_plan_requires_simulate_env" in plan["failed_reasons"]


def test_build_paper_session_plan_blocks_orders_below_one_lot(tmp_path):
    plan = build_paper_session_plan(
        PaperSessionPlanInput(
            portfolio=pl.DataFrame(
                {
                    "factor_name": ["factor_a"],
                    "direction_hint": ["as_is_candidate"],
                    "candidate_status": ["review_candidate"],
                    "target_notional": [25_000.0],
                }
            ),
            account_profile=personal_trader_profile(account_equity=1_000_000),
            session_id="paper_001",
            execution_log_path=tmp_path / "execution.jsonl",
            broker_report_path=tmp_path / "broker.json",
            quote_snapshot={
                "quote": {
                    "symbol": "HK.00700",
                    "lot_size": 100,
                    "last_price": 467.8,
                }
            },
        )
    )

    assert plan["ready_for_paper"] is False
    assert "no_executable_lot_sized_orders" in plan["failed_reasons"]
    assert plan["orders"][0]["status"] == "blocked_below_one_lot"
    assert plan["orders"][0]["min_lot_notional"] == 46780.0
    assert plan["lot_sizing"]["below_lot_order_count"] == 1
    assert plan["lot_sizing"]["suggested_actions"] == [
        "enable_allow_lot_round_up_after_review_or_raise_target_notional"
    ]


def test_build_paper_session_plan_allows_orders_above_one_lot(tmp_path):
    plan = build_paper_session_plan(
        PaperSessionPlanInput(
            portfolio=pl.DataFrame(
                {
                    "factor_name": ["factor_a"],
                    "direction_hint": ["as_is_candidate"],
                    "candidate_status": ["review_candidate"],
                    "target_notional": [25_000.0],
                }
            ),
            account_profile=personal_trader_profile(account_equity=1_000_000),
            session_id="paper_001",
            execution_log_path=tmp_path / "execution.jsonl",
            broker_report_path=tmp_path / "broker.json",
            quote_snapshot={
                "quote": {
                    "symbol": "HK.00001",
                    "lot_size": 500,
                    "last_price": 8.0,
                }
            },
        )
    )

    assert plan["ready_for_paper"] is True
    assert plan["orders"][0]["status"] == "planned_not_submitted"
    assert plan["opend"]["min_lot_notional"] == 4000.0


def test_build_paper_session_plan_can_explicitly_round_up_to_one_lot(tmp_path):
    plan = build_paper_session_plan(
        PaperSessionPlanInput(
            portfolio=pl.DataFrame(
                {
                    "factor_name": ["factor_a"],
                    "direction_hint": ["as_is_candidate"],
                    "candidate_status": ["review_candidate"],
                    "target_notional": [25_000.0],
                }
            ),
            account_profile=personal_trader_profile(account_equity=1_000_000),
            session_id="paper_001",
            execution_log_path=tmp_path / "execution.jsonl",
            broker_report_path=tmp_path / "broker.json",
            quote_snapshot={
                "quote": {
                    "symbol": "HK.00001",
                    "lot_size": 500,
                    "last_price": 65.05,
                }
            },
            allow_lot_round_up=True,
        )
    )

    assert plan["ready_for_paper"] is True
    assert plan["planned_total_notional"] == 32525.0
    assert plan["lot_sizing"]["rounded_order_count"] == 1
    assert plan["orders"][0]["requested_notional"] == 25_000.0
    assert plan["orders"][0]["target_notional"] == 32525.0
    assert plan["orders"][0]["sizing_method"] == "lot_round_up"
    assert plan["orders"][0]["failed_reasons"] == [
        "target_notional_rounded_up_to_one_lot"
    ]


def test_build_paper_session_plan_blocks_round_up_above_single_name_limit(tmp_path):
    plan = build_paper_session_plan(
        PaperSessionPlanInput(
            portfolio=pl.DataFrame(
                {
                    "factor_name": ["factor_a"],
                    "direction_hint": ["as_is_candidate"],
                    "candidate_status": ["review_candidate"],
                    "target_notional": [25_000.0],
                }
            ),
            account_profile=personal_trader_profile(account_equity=300_000),
            session_id="paper_001",
            execution_log_path=tmp_path / "execution.jsonl",
            broker_report_path=tmp_path / "broker.json",
            quote_snapshot={
                "quote": {
                    "symbol": "HK.00001",
                    "lot_size": 500,
                    "last_price": 65.05,
                }
            },
            allow_lot_round_up=True,
        )
    )

    assert plan["ready_for_paper"] is False
    assert plan["orders"][0]["status"] == "blocked_below_one_lot"
    assert plan["lot_sizing"]["suggested_actions"] == [
        "choose_lower_lot_notional_symbol_or_raise_single_name_limit"
    ]
