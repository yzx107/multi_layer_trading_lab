from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import polars as pl
import typer

from multi_layer_trading_lab.adapters.external_repos import (
    ExternalRepoConfig,
    check_external_repos,
    default_external_repo_config,
)
from multi_layer_trading_lab.adapters.futu.client import (
    FutuMode,
    FutuOpenDConfig,
    check_opend_readiness,
)
from multi_layer_trading_lab.adapters.futu.reports import (
    extract_futu_order_report_rows_from_ticket_responses,
    extract_futu_order_report_rows_from_web_log,
    futu_order_reports_to_execution_reports,
    load_futu_order_report_rows,
    write_futu_order_report_rows,
)
from multi_layer_trading_lab.adapters.ifind.client import IFindClient
from multi_layer_trading_lab.adapters.ifind.status import (
    build_ifind_connection_plan,
    ifind_lake_counts,
)
from multi_layer_trading_lab.adapters.ifind.token import (
    inspect_ifind_tokens,
    refresh_ifind_access_token_status,
)
from multi_layer_trading_lab.adapters.l2_loader.loader import L2Loader
from multi_layer_trading_lab.adapters.l2_loader.profile import (
    build_order_add_coverage,
    discover_l2_zip_paths,
    list_l2_zip_members,
    load_l2_mapping,
    normalize_order_add_zip_batch,
    normalize_order_add_zip_member,
    profile_l2_file,
    profile_l2_file_from_frame,
    read_l2_sample,
    read_l2_zip_member_sample,
    write_l2_mapping_template,
)
from multi_layer_trading_lab.adapters.readiness import (
    check_ifind_readiness,
    check_tushare_readiness,
)
from multi_layer_trading_lab.adapters.source_status import build_source_adapter_status
from multi_layer_trading_lab.adapters.tushare.client import TushareClient
from multi_layer_trading_lab.backtest.order_add import (
    backtest_order_add_candidates,
    sweep_order_add_thresholds,
)
from multi_layer_trading_lab.contracts import IFIND_EVENTS_CONTRACT, validate_dataset
from multi_layer_trading_lab.execution.dry_run_evidence import (
    build_dry_run_execution_evidence,
)
from multi_layer_trading_lab.execution.opend_tickets import (
    export_opend_paper_tickets as write_opend_paper_tickets,
)
from multi_layer_trading_lab.execution.opend_tickets import (
    fetch_opend_account_status as read_opend_account_status,
)
from multi_layer_trading_lab.execution.opend_tickets import (
    fetch_opend_quote_snapshot as read_opend_quote_snapshot,
)
from multi_layer_trading_lab.execution.opend_tickets import (
    fetch_opend_runtime_status as read_opend_runtime_status,
)
from multi_layer_trading_lab.execution.opend_tickets import (
    submit_opend_paper_tickets as post_opend_paper_tickets,
)
from multi_layer_trading_lab.execution.paper_audit import (
    PaperAuditInput,
    run_paper_promotion_audit,
)
from multi_layer_trading_lab.execution.paper_blocker_report import write_paper_blocker_report
from multi_layer_trading_lab.execution.paper_evidence import (
    PaperEvidenceInput,
    build_paper_evidence,
)
from multi_layer_trading_lab.execution.paper_evidence_bundle import (
    combine_paper_evidence_files,
)
from multi_layer_trading_lab.execution.paper_execution_log import (
    build_paper_execution_log_from_futu_report,
)
from multi_layer_trading_lab.execution.paper_plan import (
    PaperSessionPlanInput,
    build_paper_session_plan,
)
from multi_layer_trading_lab.execution.paper_progress import (
    write_paper_progress,
    write_paper_session_calendar,
)
from multi_layer_trading_lab.execution.paper_simulate_status import (
    write_paper_simulate_status,
)
from multi_layer_trading_lab.execution.profitability_evidence import (
    ProfitabilityEvidenceInput,
    build_mark_prices_from_opend_quote_snapshot,
    build_profitability_evidence,
)
from multi_layer_trading_lab.execution.reconciliation import (
    load_execution_log,
    reconcile_execution_reports,
)
from multi_layer_trading_lab.execution.session_ledger import write_paper_session_ledger
from multi_layer_trading_lab.features.daily.basic import build_daily_features
from multi_layer_trading_lab.features.l2.basic import build_l2_bucket_features
from multi_layer_trading_lab.features.l2.hshare_verified import (
    build_hshare_verified_order_features,
)
from multi_layer_trading_lab.features.l2.order_add import build_order_add_bucket_features
from multi_layer_trading_lab.pipelines.demo_pipeline import run_data_pipeline, run_demo_stack
from multi_layer_trading_lab.pipelines.research_pipeline import run_research_workflow
from multi_layer_trading_lab.reports.objective_audit import (
    ObjectiveAuditInput,
    build_objective_audit,
    write_objective_audit_report,
)
from multi_layer_trading_lab.reports.ops import OpsDailyReportInput, render_ops_daily_report
from multi_layer_trading_lab.reports.readiness import (
    GoLiveReadinessInput,
    build_go_live_readiness_manifest,
)
from multi_layer_trading_lab.research.audit import (
    ResearchAuditInput,
    evaluate_order_add_research_gate,
    run_research_promotion_audit,
)
from multi_layer_trading_lab.research.external_portfolio import (
    audit_external_portfolio_cost_capacity,
    build_external_factor_portfolio,
    evaluate_external_factor_portfolio,
)
from multi_layer_trading_lab.research.external_signals import build_external_research_signal_events
from multi_layer_trading_lab.research.factor_factory import load_factor_factory_summary
from multi_layer_trading_lab.research.hshare_verified import evaluate_hshare_verified_summary
from multi_layer_trading_lab.research.input_manifest import (
    ResearchInputManifestConfig,
    build_research_input_manifest,
)
from multi_layer_trading_lab.research.lookahead import audit_factor_factory_lookahead_lineage
from multi_layer_trading_lab.risk.profile import personal_trader_profile
from multi_layer_trading_lab.settings import settings
from multi_layer_trading_lab.signals.order_add import build_order_add_signal_candidates
from multi_layer_trading_lab.storage.freshness import (
    build_freshness_status,
    inspect_parquet_dataset,
)
from multi_layer_trading_lab.storage.parquet_store import ParquetStore

app = typer.Typer(help="Multi-layer trading lab CLI")


@app.command()
def init_master(data_root: str = "data") -> None:
    outputs = run_data_pipeline(Path(data_root))
    typer.echo(f"security_master rows={outputs['security_master'].height}")


@app.command()
def fetch_history(data_root: str = "data") -> None:
    outputs = run_data_pipeline(Path(data_root))
    typer.echo(f"daily_bars rows={outputs['daily_bars'].height}")


@app.command()
def fetch_tushare_to_lake(
    lake_root: str = "data/lake",
    symbols: str = "00700.HK,AAPL.US,600519.SH",
    start: str = "2026-03-10",
    end: str = "2026-04-08",
    calendar_exchange: str = "SSE",
    minute_trade_date: str = "2026-04-01",
    token: str | None = None,
    allow_stub: bool = False,
    use_real: bool = False,
) -> None:
    resolved_token = settings.tushare_token if token is None else token
    readiness = check_tushare_readiness(resolved_token)
    if not readiness.ready and not allow_stub:
        typer.echo("status=blocked")
        typer.echo(f"failed_reasons={','.join(readiness.failed_reasons)}")
        return
    if readiness.ready and not allow_stub and not use_real:
        typer.echo("status=blocked")
        typer.echo("failed_reasons=real_tushare_adapter_requires_use_real")
        return

    client = TushareClient(token=resolved_token, use_real=use_real)
    store = ParquetStore(Path(lake_root))
    requested_symbols = [symbol.strip() for symbol in symbols.split(",") if symbol.strip()]
    start_date = datetime.fromisoformat(start).date()
    end_date = datetime.fromisoformat(end).date()
    minute_date = datetime.fromisoformat(minute_trade_date).date()

    if use_real:
        unsupported = [
            symbol for symbol in requested_symbols if symbol.split(".")[-1] not in {"SH", "SZ"}
        ]
        requested_symbols = [
            symbol for symbol in requested_symbols if symbol.split(".")[-1] in {"SH", "SZ"}
        ]
        if unsupported:
            typer.echo(f"skipped_symbols={','.join(unsupported)}")
        if not requested_symbols:
            typer.echo("status=blocked")
            typer.echo("failed_reasons=no_supported_tushare_cn_symbols")
            return

    markets = sorted(
        {
            symbol.split(".")[-1].replace("SH", "CN").replace("SZ", "CN")
            for symbol in requested_symbols
        }
    )
    try:
        security_master = pl.concat(
            [client.fetch_security_master(market) for market in markets],
            how="diagonal_relaxed",
        )
        daily_bars = pl.concat(
            [client.fetch_daily_bars(symbol, start_date, end_date) for symbol in requested_symbols],
            how="diagonal_relaxed",
        )
        daily_features = build_daily_features(daily_bars)
        trade_calendar = client.fetch_trade_calendar(
            start_date,
            end_date,
            exchange=calendar_exchange,
        )
        minute_bars = pl.concat(
            [client.fetch_minute_bars(symbol, minute_date) for symbol in requested_symbols],
            how="diagonal_relaxed",
        )
    except Exception as exc:
        typer.echo("status=blocked")
        typer.echo(f"failed_reasons=tushare_fetch_failed:{type(exc).__name__}")
        typer.echo(f"error={exc}")
        return

    store.write("security_master", security_master)
    store.write("daily_bars", daily_bars)
    store.write("daily_features", daily_features)
    store.write("trade_calendar", trade_calendar)
    store.write("minute_bars", minute_bars)
    typer.echo("status=real_adapter" if use_real else "status=stub_adapter")
    typer.echo(f"security_master_rows={security_master.height}")
    typer.echo(f"daily_bars_rows={daily_bars.height}")
    typer.echo(f"daily_features_rows={daily_features.height}")
    typer.echo(f"trade_calendar_rows={trade_calendar.height}")
    typer.echo(f"minute_bars_rows={minute_bars.height}")


@app.command()
def ifind_token_status(
    access_token: str | None = None,
    refresh_token: str | None = None,
) -> None:
    resolved_access_token = settings.ifind_access_token if access_token is None else access_token
    resolved_refresh_token = (
        settings.ifind_refresh_token if refresh_token is None else refresh_token
    )
    status = inspect_ifind_tokens(
        access_token=resolved_access_token,
        refresh_token=resolved_refresh_token,
    )
    typer.echo(f"ifind_access_token_present={str(status.has_access_token).lower()}")
    typer.echo(f"ifind_refresh_token_present={str(status.has_refresh_token).lower()}")
    if status.refresh_token_expires_at is not None:
        typer.echo(
            "ifind_refresh_token_expires_at="
            f"{status.refresh_token_expires_at.isoformat()}"
        )
    if status.refresh_token_valid_now is not None:
        typer.echo(
            "ifind_refresh_token_valid_now="
            f"{str(status.refresh_token_valid_now).lower()}"
        )
    if status.parse_error:
        typer.echo(f"ifind_refresh_token_parse_error={status.parse_error}")


@app.command()
def ifind_connection_plan(
    lake_root: str = "data/lake",
    output_path: str = "data/logs/ifind_connection_plan.json",
    access_token: str | None = None,
    refresh_token: str | None = None,
    events_endpoint: str | None = None,
) -> None:
    resolved_access_token = settings.ifind_access_token if access_token is None else access_token
    resolved_refresh_token = (
        settings.ifind_refresh_token if refresh_token is None else refresh_token
    )
    resolved_endpoint = (
        settings.ifind_events_endpoint if events_endpoint is None else events_endpoint
    )
    plan = build_ifind_connection_plan(
        lake_root=Path(lake_root),
        access_token=resolved_access_token,
        refresh_token=resolved_refresh_token,
        events_endpoint=resolved_endpoint,
    )
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(plan.to_dict(), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    typer.echo(f"ready_for_real_http_fetch={str(plan.ready_for_real_http_fetch).lower()}")
    typer.echo(f"ready_for_real_file_mode={str(plan.ready_for_real_file_mode).lower()}")
    typer.echo(f"ifind_connection_plan={output}")
    if plan.failed_reasons:
        typer.echo(f"failed_reasons={','.join(plan.failed_reasons)}")


@app.command()
def ifind_refresh_access_token_smoke(
    refresh_token: str | None = None,
    output_path: str = "data/logs/ifind_access_token_refresh_status.json",
    url: str = "https://quantapi.51ifind.com/api/v1/get_access_token",
    timeout_seconds: float = 15.0,
) -> None:
    resolved_refresh_token = (
        settings.ifind_refresh_token if refresh_token is None else refresh_token
    )
    status = refresh_ifind_access_token_status(
        refresh_token=resolved_refresh_token,
        url=url,
        timeout_seconds=timeout_seconds,
    )
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(status.to_dict(), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    typer.echo(f"ifind_access_token_refresh_ok={str(status.ok).lower()}")
    typer.echo(f"ifind_access_token_received={str(status.access_token_received).lower()}")
    typer.echo(f"ifind_access_token_refresh_status={output}")
    if status.failed_reason:
        typer.echo(f"failed_reason={status.failed_reason}")


@app.command()
def fetch_ifind_to_lake(
    lake_root: str = "data/lake",
    symbols: str = "00700.HK,AAPL.US",
    start: str = "2026-04-01",
    end: str = "2026-04-08",
    username: str | None = None,
    password: str | None = None,
    access_token: str | None = None,
    refresh_token: str | None = None,
    events_endpoint: str | None = None,
    validation_report_path: str = "data/logs/ifind_events_validation.json",
    allow_stub: bool = False,
    use_real: bool = False,
) -> None:
    resolved_username = settings.ifind_username if username is None else username
    resolved_password = settings.ifind_password if password is None else password
    use_env_tokens = username is None and password is None
    resolved_access_token = (
        settings.ifind_access_token if access_token is None and use_env_tokens else access_token
    )
    resolved_refresh_token = (
        settings.ifind_refresh_token if refresh_token is None and use_env_tokens else refresh_token
    )
    readiness = check_ifind_readiness(
        resolved_username,
        resolved_password,
        resolved_access_token,
        resolved_refresh_token,
    )
    if not readiness.ready and not allow_stub:
        typer.echo("status=blocked")
        typer.echo(f"failed_reasons={','.join(readiness.failed_reasons)}")
        return
    if readiness.ready and not allow_stub and not use_real:
        typer.echo("status=blocked")
        typer.echo("failed_reasons=real_ifind_adapter_requires_use_real")
        return
    resolved_endpoint = (
        settings.ifind_events_endpoint if events_endpoint is None else events_endpoint
    )
    requested_symbols = [symbol.strip() for symbol in symbols.split(",") if symbol.strip()]
    try:
        events = IFindClient(
            username=resolved_username,
            password=resolved_password,
            access_token=resolved_access_token,
            refresh_token=resolved_refresh_token,
            use_real=use_real,
            events_endpoint=resolved_endpoint,
        ).fetch_events(
            symbols=requested_symbols,
            start=datetime.fromisoformat(start).date(),
            end=datetime.fromisoformat(end).date(),
        )
    except Exception as exc:
        typer.echo("status=blocked")
        typer.echo(f"failed_reasons=ifind_fetch_failed:{type(exc).__name__}")
        typer.echo(f"error={exc}")
        return
    validation = validate_dataset(events, IFIND_EVENTS_CONTRACT)
    validation_payload = {
        "valid": validation.passed,
        "rows": events.height,
        "source_run_id": (
            events["source_run_id"][0]
            if events.height > 0 and "source_run_id" in events.columns
            else None
        ),
        "missing_columns": validation.missing_columns,
        "type_mismatches": validation.type_mismatches,
        "duplicate_key_rows": validation.duplicate_key_rows,
        "failed_reasons": [
            reason
            for reason, present in [
                ("missing_columns", bool(validation.missing_columns)),
                ("type_mismatches", bool(validation.type_mismatches)),
                ("duplicate_key_rows", bool(validation.duplicate_key_rows)),
            ]
            if present
        ],
    }
    validation_output = Path(validation_report_path)
    validation_output.parent.mkdir(parents=True, exist_ok=True)
    validation_output.write_text(
        json.dumps(validation_payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    if not validation.passed:
        typer.echo("status=blocked")
        typer.echo(f"validation_report={validation_output}")
        typer.echo(f"failed_reasons={','.join(validation_payload['failed_reasons'])}")
        return
    output = ParquetStore(Path(lake_root)).write("ifind_events", events)
    typer.echo("status=real_adapter" if use_real else "status=stub_adapter")
    typer.echo(f"ifind_events_rows={events.height}")
    typer.echo(f"validation_report={validation_output}")
    typer.echo(f"output={output}")


@app.command()
def import_ifind_events_file(
    file_path: str,
    lake_root: str = "data/lake",
    dataset: str = "ifind_events",
    source_run_id: str | None = None,
    overwrite: bool = False,
) -> None:
    try:
        events = IFindClient(use_real=True).load_events_file(
            Path(file_path),
            source_run_id=source_run_id,
        )
    except Exception as exc:
        typer.echo("status=blocked")
        typer.echo(f"failed_reasons=ifind_file_import_failed:{type(exc).__name__}")
        typer.echo(f"error={exc}")
        return
    validation = validate_dataset(events, IFIND_EVENTS_CONTRACT)
    if not validation.passed:
        typer.echo("status=blocked")
        failed_reasons: list[str] = []
        if validation.missing_columns:
            failed_reasons.append("missing_columns")
            typer.echo(f"missing_columns={','.join(validation.missing_columns)}")
        if validation.type_mismatches:
            failed_reasons.append("type_mismatches")
            typer.echo(f"type_mismatches={','.join(validation.type_mismatches)}")
        if validation.duplicate_key_rows:
            failed_reasons.append("duplicate_key_rows")
            typer.echo(f"duplicate_key_rows={validation.duplicate_key_rows}")
        typer.echo(f"failed_reasons={','.join(failed_reasons)}")
        return
    output = Path(lake_root) / dataset / "part-000.parquet"
    if output.exists() and not overwrite:
        typer.echo("status=blocked")
        typer.echo("failed_reasons=ifind_events_dataset_exists")
        typer.echo(f"existing_output={output}")
        return
    output = ParquetStore(Path(lake_root)).write(dataset, events)
    typer.echo("status=real_file_adapter")
    typer.echo(f"ifind_events_rows={events.height}")
    typer.echo(f"dataset={dataset}")
    typer.echo(f"output={output}")


@app.command()
def validate_ifind_events_file(
    file_path: str,
    source_run_id: str | None = None,
    output_path: str | None = None,
) -> None:
    output = Path(output_path) if output_path else None
    try:
        events = IFindClient(use_real=True).load_events_file(
            Path(file_path),
            source_run_id=source_run_id,
        )
    except Exception as exc:
        payload = {
            "valid": False,
            "file_path": file_path,
            "rows": 0,
            "failed_reasons": [f"ifind_file_validate_failed:{type(exc).__name__}"],
            "error": str(exc),
        }
        if output is not None:
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text(
                json.dumps(payload, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
        typer.echo("status=blocked")
        typer.echo(f"failed_reasons=ifind_file_validate_failed:{type(exc).__name__}")
        typer.echo(f"error={exc}")
        return
    result = validate_dataset(events, IFIND_EVENTS_CONTRACT)
    failed_reasons: list[str] = []
    if result.missing_columns:
        failed_reasons.append("missing_columns")
    if result.type_mismatches:
        failed_reasons.append("type_mismatches")
    if result.duplicate_key_rows:
        failed_reasons.append("duplicate_key_rows")
    payload = {
        "valid": result.passed,
        "file_path": file_path,
        "rows": events.height,
        "source_run_id": source_run_id,
        "missing_columns": result.missing_columns,
        "type_mismatches": result.type_mismatches,
        "duplicate_key_rows": result.duplicate_key_rows,
        "failed_reasons": failed_reasons,
    }
    if output is not None:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        typer.echo(f"validation_report={output}")
    typer.echo(f"ifind_events_file_valid={str(result.passed).lower()}")
    typer.echo(f"ifind_events_rows={events.height}")
    if result.missing_columns:
        typer.echo(f"missing_columns={','.join(result.missing_columns)}")
    if result.type_mismatches:
        typer.echo(f"type_mismatches={','.join(result.type_mismatches)}")
    if result.duplicate_key_rows:
        typer.echo(f"duplicate_key_rows={result.duplicate_key_rows}")


@app.command()
def write_ifind_events_template(
    output_path: str = "data/templates/ifind_events_template.csv",
) -> None:
    output = IFindClient.write_events_template(Path(output_path))
    typer.echo(f"ifind_events_template={output}")


@app.command()
def import_l2(data_root: str = "data") -> None:
    outputs = run_data_pipeline(Path(data_root))
    typer.echo(f"intraday_l2_features rows={outputs['intraday_l2_features'].height}")


@app.command()
def profile_l2_sample(path: str, limit: int = 1000, mapping_output: str | None = None) -> None:
    profile = profile_l2_file(Path(path), limit=limit)
    typer.echo(profile.as_text())
    if mapping_output:
        output = write_l2_mapping_template(profile, Path(mapping_output))
        typer.echo(f"mapping_template={output}")


@app.command()
def profile_l2_zip(
    path: str,
    category: str = "OrderAdd",
    member: str | None = None,
    limit: int = 1000,
    max_members: int = 5,
) -> None:
    members = list_l2_zip_members(Path(path), category=category)
    typer.echo(f"zip_path={path}")
    typer.echo(f"category={category}")
    typer.echo(f"member_count={len(members)}")
    for item in members[:max_members]:
        typer.echo(f"member={item}")
    if not members:
        return

    selected_member = member or members[0]
    sample = read_l2_zip_member_sample(Path(path), selected_member, limit=limit)
    typer.echo(f"sample_member={selected_member}")
    typer.echo(f"sample_rows={sample.height}")
    typer.echo(f"sample_columns={','.join(sample.columns)}")
    suggested = profile_l2_file_from_frame(Path(f"{path}:{selected_member}"), sample)
    for target, source in sorted(suggested.suggested_mapping.items()):
        typer.echo(f"map.{target}={source}")
    if suggested.missing_targets:
        typer.echo(f"missing_targets={','.join(suggested.missing_targets)}")


@app.command()
def import_l2_sample(
    path: str,
    mapping_path: str,
    lake_root: str = "data/lake",
    dataset: str = "raw_l2_ticks",
    limit: int = 1_000_000,
) -> None:
    raw = read_l2_sample(Path(path), limit=limit)
    mapping = load_l2_mapping(Path(mapping_path))
    normalized = L2Loader(Path(path).parent).normalize_raw_frame(raw, mapping=mapping)
    output = ParquetStore(Path(lake_root)).write(dataset, normalized)
    typer.echo(f"raw_rows={raw.height}")
    typer.echo(f"normalized_rows={normalized.height}")
    typer.echo(f"dataset={dataset}")
    typer.echo(f"output={output}")


@app.command()
def import_l2_zip_order_add(
    path: str,
    member: str | None = None,
    lake_root: str = "data/lake",
    dataset: str = "raw_l2_order_add",
    limit: int = 100_000,
) -> None:
    members = list_l2_zip_members(Path(path), category="OrderAdd")
    if not members:
        typer.echo("status=no_order_add_members")
        return
    selected_member = member or members[0]
    normalized = normalize_order_add_zip_member(Path(path), selected_member, limit=limit)
    output = ParquetStore(Path(lake_root)).write(dataset, normalized)
    typer.echo(f"zip_path={path}")
    typer.echo(f"member={selected_member}")
    typer.echo(f"rows={normalized.height}")
    typer.echo(f"dataset={dataset}")
    typer.echo(f"output={output}")


@app.command()
def import_l2_zip_order_add_batch(
    zip_paths: str,
    symbols: str = "00001.HK",
    lake_root: str = "data/lake",
    dataset: str = "raw_l2_order_add",
    limit_per_member: int = 100_000,
) -> None:
    requested_paths = [Path(item.strip()) for item in zip_paths.split(",") if item.strip()]
    requested_symbols = [item.strip() for item in symbols.split(",") if item.strip()]
    normalized = normalize_order_add_zip_batch(
        requested_paths,
        requested_symbols,
        limit_per_member=limit_per_member,
    )
    output = ParquetStore(Path(lake_root)).write(dataset, normalized)
    typer.echo(f"zip_count={len(requested_paths)}")
    typer.echo(f"symbol_count={len(requested_symbols)}")
    typer.echo(f"rows={normalized.height}")
    typer.echo(f"dataset={dataset}")
    typer.echo(f"output={output}")


@app.command()
def discover_l2_zips(
    raw_root: str = "/Volumes/Data/港股Tick数据",
    start: str = "2025-01-01",
    end: str = "2025-01-31",
) -> None:
    start_date = datetime.fromisoformat(start).date()
    end_date = datetime.fromisoformat(end).date()
    paths = discover_l2_zip_paths(Path(raw_root), start_date, end_date)
    typer.echo(f"zip_count={len(paths)}")
    for path in paths:
        typer.echo(str(path))


@app.command()
def check_l2_order_add_coverage(
    raw_root: str = "/Volumes/Data/港股Tick数据",
    start: str = "2025-01-01",
    end: str = "2025-01-31",
    symbols: str = "00001.HK",
    output_path: str | None = None,
) -> None:
    paths = discover_l2_zip_paths(
        Path(raw_root),
        datetime.fromisoformat(start).date(),
        datetime.fromisoformat(end).date(),
    )
    requested_symbols = [item.strip() for item in symbols.split(",") if item.strip()]
    coverage = build_order_add_coverage(list(paths), requested_symbols)
    available = int(coverage.filter(pl.col("available")).height) if not coverage.is_empty() else 0
    missing = coverage.height - available
    typer.echo(f"zip_count={len(paths)}")
    typer.echo(f"symbol_count={len(requested_symbols)}")
    typer.echo(f"coverage_rows={coverage.height}")
    typer.echo(f"available_rows={available}")
    typer.echo(f"missing_rows={missing}")
    if output_path:
        output = Path(output_path)
        output.parent.mkdir(parents=True, exist_ok=True)
        coverage.write_csv(output)
        typer.echo(f"coverage_output={output}")


@app.command()
def import_l2_zip_order_add_range(
    raw_root: str = "/Volumes/Data/港股Tick数据",
    start: str = "2025-01-01",
    end: str = "2025-01-31",
    symbols: str = "00001.HK",
    lake_root: str = "data/lake",
    dataset: str = "raw_l2_order_add",
    limit_per_member: int = 100_000,
) -> None:
    paths = discover_l2_zip_paths(
        Path(raw_root),
        datetime.fromisoformat(start).date(),
        datetime.fromisoformat(end).date(),
    )
    requested_symbols = [item.strip() for item in symbols.split(",") if item.strip()]
    normalized = normalize_order_add_zip_batch(
        list(paths),
        requested_symbols,
        limit_per_member=limit_per_member,
    )
    output = ParquetStore(Path(lake_root)).write(dataset, normalized)
    typer.echo(f"zip_count={len(paths)}")
    typer.echo(f"symbol_count={len(requested_symbols)}")
    typer.echo(f"rows={normalized.height}")
    typer.echo(f"dataset={dataset}")
    typer.echo(f"output={output}")


@app.command()
def build_l2_features_from_lake(
    lake_root: str = "data/lake",
    raw_dataset: str = "raw_l2_ticks",
    output_dataset: str = "intraday_l2_features",
    bucket: str = "1m",
) -> None:
    store = ParquetStore(Path(lake_root))
    raw = store.read(raw_dataset)
    if raw.is_empty():
        typer.echo("raw_rows=0")
        typer.echo("features_rows=0")
        typer.echo("status=no_raw_l2_ticks")
        return

    aggregated = L2Loader(Path(lake_root)).aggregate(raw, bucket=bucket)
    features = build_l2_bucket_features(aggregated, bucket_size=bucket)
    output = store.write(output_dataset, features)
    typer.echo(f"raw_rows={raw.height}")
    typer.echo(f"aggregated_rows={aggregated.height}")
    typer.echo(f"features_rows={features.height}")
    typer.echo(f"output={output}")


@app.command()
def build_l2_order_add_features_from_lake(
    lake_root: str = "data/lake",
    raw_dataset: str = "raw_l2_order_add",
    output_dataset: str = "l2_order_add_features",
    bucket: str = "1m",
    large_order_volume: int = 10_000,
) -> None:
    store = ParquetStore(Path(lake_root))
    raw = store.read(raw_dataset)
    if raw.is_empty():
        typer.echo("raw_rows=0")
        typer.echo("features_rows=0")
        typer.echo("status=no_raw_l2_order_add")
        return

    features = build_order_add_bucket_features(
        raw,
        bucket_size=bucket,
        large_order_volume=large_order_volume,
    )
    output = store.write(output_dataset, features)
    typer.echo(f"raw_rows={raw.height}")
    typer.echo(f"features_rows={features.height}")
    typer.echo(f"dataset={output_dataset}")
    typer.echo(f"output={output}")


@app.command()
def build_hshare_verified_l2_features(
    verified_root: str = "/Volumes/Data/港股Tick数据/verified",
    dates: str = "2026-04-01",
    symbols: str = "00001.HK",
    lake_root: str = "data/lake",
    output_dataset: str = "intraday_l2_features",
    bucket: str = "1m",
    max_rows: int = 250_000,
) -> None:
    requested_dates = [item.strip() for item in dates.split(",") if item.strip()]
    requested_tickers = [_normalize_hk_ticker(item) for item in symbols.split(",") if item.strip()]
    frames: list[pl.DataFrame] = []
    missing_dates: list[str] = []
    for date_text in requested_dates:
        trade_date = datetime.fromisoformat(date_text).date()
        date_dir = (
            Path(verified_root)
            / "verified_orders"
            / f"year={trade_date.year}"
            / f"date={trade_date.isoformat()}"
        )
        files = sorted(date_dir.glob("*.parquet"))
        if not files:
            missing_dates.append(date_text)
            continue
        scan = (
            pl.scan_parquet([str(path) for path in files])
            .filter(pl.col("instrument_key").is_in(requested_tickers))
            .select(["date", "instrument_key", "SendTime", "Price", "Volume"])
        )
        if max_rows > 0:
            scan = scan.limit(max_rows)
        frames.append(scan.collect())

    orders = pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame()
    if orders.is_empty():
        typer.echo(f"requested_dates={len(requested_dates)}")
        typer.echo(f"requested_symbols={len(requested_tickers)}")
        typer.echo("orders_rows=0")
        typer.echo("features_rows=0")
        typer.echo("status=no_hshare_verified_orders")
        if missing_dates:
            typer.echo(f"missing_dates={','.join(missing_dates)}")
        return

    source_run_id = f"hshare-verified-orders-{datetime.now(UTC):%Y%m%dT%H%M%S}"
    features = build_hshare_verified_order_features(
        orders,
        bucket_size=bucket,
        source_run_id=source_run_id,
    )
    output = ParquetStore(Path(lake_root)).write(output_dataset, features)
    typer.echo(f"requested_dates={len(requested_dates)}")
    typer.echo(f"requested_symbols={len(requested_tickers)}")
    typer.echo(f"orders_rows={orders.height}")
    typer.echo(f"features_rows={features.height}")
    typer.echo(f"dataset={output_dataset}")
    typer.echo(f"source_run_id={source_run_id}")
    typer.echo(f"output={output}")
    if missing_dates:
        typer.echo(f"missing_dates={','.join(missing_dates)}")


def _normalize_hk_ticker(symbol: str) -> str:
    value = symbol.strip().upper()
    if value.startswith("HK."):
        value = value[3:]
    if value.endswith(".HK"):
        value = value[:-3]
    return value.zfill(5) if value.isdigit() else value


@app.command()
def build_order_add_signals_from_lake(
    lake_root: str = "data/lake",
    feature_dataset: str = "l2_order_add_features",
    output_dataset: str = "order_add_signal_candidates",
    min_order_add_volume: int = 20_000,
    min_large_order_ratio: float = 0.25,
) -> None:
    store = ParquetStore(Path(lake_root))
    features = store.read(feature_dataset)
    if features.is_empty():
        typer.echo("feature_rows=0")
        typer.echo("candidate_rows=0")
        typer.echo("status=no_l2_order_add_features")
        return

    candidates = build_order_add_signal_candidates(
        features,
        min_order_add_volume=min_order_add_volume,
        min_large_order_ratio=min_large_order_ratio,
    )
    output = store.write(output_dataset, candidates)
    typer.echo(f"feature_rows={features.height}")
    typer.echo(f"candidate_rows={candidates.height}")
    typer.echo(f"dataset={output_dataset}")
    typer.echo(f"output={output}")


@app.command()
def backtest_order_add_signals_from_lake(
    lake_root: str = "data/lake",
    candidate_dataset: str = "order_add_signal_candidates",
    feature_dataset: str = "l2_order_add_features",
    trades_dataset: str = "order_add_backtest_trades",
    summary_dataset: str = "order_add_backtest_summary",
    horizon_buckets: int = 1,
    cost_bps: float = 15.0,
) -> None:
    store = ParquetStore(Path(lake_root))
    candidates = store.read(candidate_dataset)
    features = store.read(feature_dataset)
    result = backtest_order_add_candidates(
        candidates,
        features,
        horizon_buckets=horizon_buckets,
        cost_bps=cost_bps,
    )
    trades_output = store.write(trades_dataset, result.trades)
    summary_output = store.write(summary_dataset, result.summary)
    summary = result.summary.row(0, named=True)
    typer.echo(f"candidate_rows={candidates.height}")
    typer.echo(f"trade_count={summary['trade_count']}")
    typer.echo(f"win_rate={summary['win_rate']:.6f}")
    typer.echo(f"avg_net_ret={summary['avg_net_ret']:.8f}")
    typer.echo(f"total_net_ret={summary['total_net_ret']:.8f}")
    typer.echo(f"trades_output={trades_output}")
    typer.echo(f"summary_output={summary_output}")


@app.command()
def sweep_order_add_thresholds_from_lake(
    lake_root: str = "data/lake",
    feature_dataset: str = "l2_order_add_features",
    output_dataset: str = "order_add_threshold_sweep",
    volume_thresholds: str = "20000,50000,100000,200000",
    large_order_ratio_thresholds: str = "0.03,0.05,0.10,0.25",
    horizon_buckets: int = 1,
    cost_bps: float = 15.0,
    planned_notional: float | None = None,
) -> None:
    store = ParquetStore(Path(lake_root))
    features = store.read(feature_dataset)
    volumes = [int(value.strip()) for value in volume_thresholds.split(",") if value.strip()]
    ratios = [
        float(value.strip()) for value in large_order_ratio_thresholds.split(",") if value.strip()
    ]
    sweep = sweep_order_add_thresholds(
        features,
        volume_thresholds=volumes,
        large_order_ratio_thresholds=ratios,
        horizon_buckets=horizon_buckets,
        cost_bps=cost_bps,
        planned_notional=planned_notional,
    )
    output = store.write(output_dataset, sweep)
    best = sweep.row(0, named=True) if not sweep.is_empty() else None
    typer.echo(f"feature_rows={features.height}")
    typer.echo(f"sweep_rows={sweep.height}")
    if best:
        typer.echo(f"best_volume={best['min_order_add_volume']}")
        typer.echo(f"best_large_order_ratio={best['min_large_order_ratio']:.6f}")
        typer.echo(f"best_trade_count={best['trade_count']}")
        typer.echo(f"best_avg_net_ret={best['avg_net_ret']:.8f}")
        typer.echo(f"best_total_net_ret={best['total_net_ret']:.8f}")
    typer.echo(f"output={output}")


@app.command()
def generate_features(data_root: str = "data") -> None:
    outputs = run_data_pipeline(Path(data_root))
    typer.echo(
        f"daily_features rows={outputs['daily_features'].height}, "
        f"intraday_l2_features rows={outputs['intraday_l2_features'].height}"
    )


@app.command()
def demo_backtest(data_root: str = "data", mode: str = "dry_run") -> None:
    outputs = run_demo_stack(Path(data_root), execution_mode=mode)
    result = outputs["backtest_result"]
    metrics = result.metrics
    typer.echo(
        "backtest "
        f"fills={metrics.fills} rejected={metrics.rejected} "
        f"pnl={metrics.total_pnl:.2f} turnover={metrics.turnover:.2f} "
        f"max_dd={metrics.max_drawdown:.2f}"
    )


@app.command()
def dry_run_signals(data_root: str = "data") -> None:
    outputs = run_demo_stack(Path(data_root), execution_mode="dry_run")
    typer.echo(
        f"signals={len(outputs['signal_event_objects'])} "
        f"log={outputs['execution_log_path']}"
    )


@app.command()
def research_demo(data_root: str = "data") -> None:
    outputs = run_research_workflow(Path(data_root))
    typer.echo(
        "research "
        f"daily={outputs['daily_features'].height} "
        f"l2={outputs['intraday_l2_features'].height} "
        f"signals={outputs['signal_events'].height} "
        f"report={outputs['research_report_path']}"
    )


@app.command()
def validate_research(data_root: str = "data") -> None:
    outputs = run_research_workflow(Path(data_root))
    summary = outputs["validation_summary"]
    passed = int(summary.filter(pl.col("passed")).height)
    total = summary.height
    typer.echo(f"validation passed={passed}/{total}")


@app.command()
def risk_precheck(account_equity: float = 1_000_000.0) -> None:
    profile = personal_trader_profile(account_equity=account_equity)
    typer.echo(f"account_equity={profile.account_equity:.2f}")
    typer.echo(f"max_single_name_notional={profile.max_single_name_notional:.2f}")
    typer.echo(f"max_strategy_notional={profile.max_strategy_notional:.2f}")
    typer.echo(f"max_daily_drawdown={profile.max_daily_drawdown:.2f}")
    typer.echo(f"max_gross_notional={profile.max_gross_notional:.2f}")
    typer.echo(f"max_open_slippage_bps={profile.max_open_slippage_bps:.2f}")
    typer.echo(f"default_kelly_scale={profile.default_kelly_scale:.3f}")


@app.command()
def opend_precheck(
    host: str = "127.0.0.1",
    port: int = 11111,
    env: str = "SIMULATE",
    mode: FutuMode = FutuMode.DRY_RUN,
    unlock_password_set: bool = False,
    manual_live_enable: bool = False,
) -> None:
    readiness = check_opend_readiness(
        FutuOpenDConfig(
            host=host,
            port=port,
            env=env,
            mode=mode,
            unlock_password_set=unlock_password_set,
            manual_live_enable=manual_live_enable,
        )
    )
    typer.echo(f"opend_ready={str(readiness.ready).lower()}")
    if readiness.failed_reasons:
        typer.echo(f"failed_reasons={','.join(readiness.failed_reasons)}")


@app.command()
def reconcile_futu_report(
    execution_log_path: str,
    futu_report_path: str,
    price_tolerance: float = 0.01,
) -> None:
    local_records = load_execution_log(Path(execution_log_path))
    broker_reports = futu_order_reports_to_execution_reports(
        load_futu_order_report_rows(Path(futu_report_path))
    )
    result = reconcile_execution_reports(
        local_records,
        broker_reports,
        price_tolerance=price_tolerance,
    )
    typer.echo(f"reconciliation_clean={str(result.clean).lower()}")
    typer.echo(f"matched_orders={result.matched_orders}")
    typer.echo(f"break_count={len(result.breaks)}")
    for item in result.breaks:
        typer.echo(
            f"break order_id={item.order_id} reason={item.reason} "
            f"local={item.local_value} broker={item.broker_value}"
        )


@app.command()
def extract_futu_web_report(
    web_log_path: str,
    output_path: str = "data/logs/futu_order_report.json",
) -> None:
    rows = extract_futu_order_report_rows_from_web_log(Path(web_log_path))
    output = write_futu_order_report_rows(Path(output_path), rows)
    typer.echo(f"report_rows={len(rows)}")
    typer.echo(f"broker_report_path={output}")


@app.command()
def extract_futu_ticket_response_report(
    response_path: str = "data/logs/opend_paper_ticket_responses.jsonl",
    output_path: str = "data/logs/futu_order_report.from_responses.json",
) -> None:
    if not Path(response_path).exists():
        typer.echo("status=blocked")
        typer.echo("failed_reasons=missing_opend_ticket_response_path")
        return
    rows = extract_futu_order_report_rows_from_ticket_responses(Path(response_path))
    output = write_futu_order_report_rows(Path(output_path), rows)
    typer.echo("status=ready")
    typer.echo(f"report_rows={len(rows)}")
    typer.echo(f"broker_report_path={output}")


@app.command()
def build_paper_execution_log(
    ticket_path: str = "data/logs/opend_paper_tickets.jsonl",
    broker_report_path: str = "data/logs/futu_order_report.json",
    execution_log_path: str = "data/logs/execution_log.jsonl",
) -> None:
    result = build_paper_execution_log_from_futu_report(
        Path(ticket_path),
        Path(broker_report_path),
        execution_log_path=Path(execution_log_path),
    )
    if result.failed_reasons:
        typer.echo("status=blocked")
        typer.echo(f"failed_reasons={','.join(result.failed_reasons)}")
        return
    typer.echo("status=ready")
    typer.echo(f"execution_log_rows={result.rows}")
    typer.echo(f"execution_log_path={result.execution_log_path}")


@app.command()
def build_paper_session_evidence_bundle(
    ticket_path: str = "data/logs/opend_paper_tickets.jsonl",
    response_path: str | None = "data/logs/opend_paper_ticket_responses.jsonl",
    broker_report_path: str = "data/logs/futu_order_report.json",
    execution_log_path: str = "data/logs/execution_log.jsonl",
    profitability_evidence_path: str = "data/logs/profitability_evidence.json",
    paper_sessions: int = 0,
    mark_prices_path: str | None = None,
    max_allowed_slippage: float = 0.05,
    max_allowed_drawdown: float = 10_000.0,
    manual_live_enable: bool = False,
    price_tolerance: float = 0.01,
) -> None:
    broker_path = Path(broker_report_path)
    if response_path:
        if not Path(response_path).exists():
            typer.echo("status=blocked")
            typer.echo("failed_reasons=missing_opend_ticket_response_path")
            return
        rows = extract_futu_order_report_rows_from_ticket_responses(Path(response_path))
        broker_path = write_futu_order_report_rows(broker_path, rows)
        typer.echo(f"broker_report_rows={len(rows)}")
        typer.echo(f"broker_report_path={broker_path}")

    execution_result = build_paper_execution_log_from_futu_report(
        Path(ticket_path),
        broker_path,
        execution_log_path=Path(execution_log_path),
    )
    if execution_result.failed_reasons:
        typer.echo("status=blocked")
        typer.echo(f"failed_reasons={','.join(execution_result.failed_reasons)}")
        return

    paper_result = build_paper_evidence(
        PaperEvidenceInput(
            execution_log_path=Path(execution_log_path),
            broker_report_path=broker_path,
            paper_sessions=paper_sessions,
            max_allowed_slippage=max_allowed_slippage,
            manual_live_enable=manual_live_enable,
            price_tolerance=price_tolerance,
        )
    )
    profitability = build_profitability_evidence(
        ProfitabilityEvidenceInput(
            execution_log_path=Path(execution_log_path),
            broker_report_path=broker_path,
            output_path=Path(profitability_evidence_path),
            paper_sessions=paper_sessions,
            mark_prices_path=Path(mark_prices_path) if mark_prices_path else None,
            max_allowed_drawdown=max_allowed_drawdown,
            price_tolerance=price_tolerance,
        )
    )

    typer.echo("status=ready")
    typer.echo(f"execution_log_rows={execution_result.rows}")
    typer.echo(f"paper_evidence_ready={str(paper_result.ready).lower()}")
    if paper_result.audit is not None:
        typer.echo(f"paper_to_live_approved={str(paper_result.audit.decision.approved).lower()}")
    typer.echo(f"profitability_evidence_ready={str(profitability['ready']).lower()}")
    typer.echo(f"net_pnl={float(profitability['net_pnl']):.2f}")
    failed = list(paper_result.failed_reasons)
    if paper_result.audit is not None:
        failed.extend(paper_result.audit.decision.failed_reasons)
    failed.extend(profitability["failed_reasons"])
    if failed:
        typer.echo(f"failed_reasons={','.join(str(reason) for reason in failed)}")


@app.command()
def paper_audit(
    execution_log_path: str,
    futu_report_path: str,
    paper_sessions: int = 0,
    max_allowed_slippage: float = 0.05,
    manual_live_enable: bool = False,
    price_tolerance: float = 0.01,
) -> None:
    local_records = load_execution_log(Path(execution_log_path))
    broker_reports = futu_order_reports_to_execution_reports(
        load_futu_order_report_rows(Path(futu_report_path))
    )
    reconciliation = reconcile_execution_reports(
        local_records,
        broker_reports,
        price_tolerance=price_tolerance,
    )
    audit = run_paper_promotion_audit(
        PaperAuditInput(
            paper_sessions=paper_sessions,
            local_records=local_records,
            reconciliation=reconciliation,
            max_allowed_slippage=max_allowed_slippage,
            manual_live_enable=manual_live_enable,
        )
    )
    typer.echo(f"paper_to_live_approved={str(audit.decision.approved).lower()}")
    typer.echo(f"paper_sessions={audit.evidence.paper_sessions}")
    typer.echo(f"order_reject_rate={audit.evidence.order_reject_rate:.6f}")
    typer.echo(f"reconciliation_clean={str(audit.evidence.reconciliation_clean).lower()}")
    typer.echo(
        f"slippage_within_assumption="
        f"{str(audit.evidence.slippage_within_assumption).lower()}"
    )
    if audit.decision.failed_reasons:
        typer.echo(f"failed_reasons={','.join(audit.decision.failed_reasons)}")


@app.command()
def paper_evidence(
    execution_log_path: str = "data/logs/execution_log.jsonl",
    broker_report_path: str = "data/logs/futu_order_report.json",
    paper_sessions: int = 0,
    max_allowed_slippage: float = 0.05,
    manual_live_enable: bool = False,
    price_tolerance: float = 0.01,
) -> None:
    result = build_paper_evidence(
        PaperEvidenceInput(
            execution_log_path=Path(execution_log_path),
            broker_report_path=Path(broker_report_path),
            paper_sessions=paper_sessions,
            max_allowed_slippage=max_allowed_slippage,
            manual_live_enable=manual_live_enable,
            price_tolerance=price_tolerance,
        )
    )
    typer.echo(f"paper_evidence_ready={str(result.ready).lower()}")
    typer.echo(f"execution_log_rows={result.execution_log_rows}")
    typer.echo(f"broker_report_rows={result.broker_report_rows}")
    if result.audit is not None:
        typer.echo(f"paper_to_live_approved={str(result.audit.decision.approved).lower()}")
        typer.echo(f"paper_sessions={result.audit.evidence.paper_sessions}")
        typer.echo(f"order_reject_rate={result.audit.evidence.order_reject_rate:.6f}")
        typer.echo(f"reconciliation_clean={str(result.audit.evidence.reconciliation_clean).lower()}")
    failed = list(result.failed_reasons)
    if result.audit is not None:
        failed.extend(result.audit.decision.failed_reasons)
    if failed:
        typer.echo(f"failed_reasons={','.join(failed)}")


@app.command()
def profitability_evidence(
    execution_log_path: str = "data/logs/execution_log.jsonl",
    broker_report_path: str = "data/logs/futu_order_report.json",
    output_path: str = "data/logs/profitability_evidence.json",
    paper_sessions: int = 0,
    mark_prices_path: str | None = None,
    max_allowed_drawdown: float = 10_000.0,
    price_tolerance: float = 0.01,
) -> None:
    evidence = build_profitability_evidence(
        ProfitabilityEvidenceInput(
            execution_log_path=Path(execution_log_path),
            broker_report_path=Path(broker_report_path),
            output_path=Path(output_path),
            paper_sessions=paper_sessions,
            mark_prices_path=Path(mark_prices_path) if mark_prices_path else None,
            max_allowed_drawdown=max_allowed_drawdown,
            price_tolerance=price_tolerance,
        )
    )
    typer.echo(f"profitability_evidence_ready={str(evidence['ready']).lower()}")
    typer.echo(f"net_pnl={float(evidence['net_pnl']):.2f}")
    typer.echo(f"max_drawdown={float(evidence['max_drawdown']):.2f}")
    typer.echo(f"reconciled={str(evidence['reconciled']).lower()}")
    typer.echo(f"profitability_evidence={output_path}")
    if evidence["failed_reasons"]:
        typer.echo(f"failed_reasons={','.join(evidence['failed_reasons'])}")


@app.command()
def paper_session_ledger(
    execution_log_path: str = "data/logs/execution_log.jsonl",
    broker_report_path: str = "data/logs/futu_order_report.json",
    output_path: str = "data/logs/paper_session_ledger.json",
) -> None:
    ledger = write_paper_session_ledger(
        execution_log_path=Path(execution_log_path),
        broker_report_path=Path(broker_report_path),
        output_path=Path(output_path),
    )
    typer.echo(
        "ready_for_profitability_evidence="
        f"{str(ledger.ready_for_profitability_evidence).lower()}"
    )
    typer.echo(f"inferred_session_count={ledger.inferred_session_count}")
    typer.echo(f"execution_log_rows={ledger.execution_log_rows}")
    typer.echo(f"broker_report_rows={ledger.broker_report_rows}")
    typer.echo(f"dry_run_rows={ledger.dry_run_rows}")
    typer.echo(f"paper_session_ledger={output_path}")
    if ledger.failed_reasons:
        typer.echo(f"failed_reasons={','.join(ledger.failed_reasons)}")


@app.command()
def paper_progress(
    execution_log_path: str = "data/logs/execution_log.paper_combined.jsonl",
    broker_report_path: str = "data/logs/futu_order_report.paper_combined.json",
    profitability_evidence_path: str | None = (
        "data/logs/profitability_evidence.paper_combined.json"
    ),
    output_path: str = "data/logs/paper_progress.json",
    target_sessions: int = 20,
) -> None:
    progress = write_paper_progress(
        execution_log_path=Path(execution_log_path),
        broker_report_path=Path(broker_report_path),
        profitability_evidence_path=Path(profitability_evidence_path)
        if profitability_evidence_path
        else None,
        output_path=Path(output_path),
        target_sessions=target_sessions,
    )
    typer.echo(f"ready_for_live_review={str(progress.ready_for_live_review).lower()}")
    typer.echo(f"inferred_session_count={progress.inferred_session_count}")
    typer.echo(f"sessions_remaining={progress.sessions_remaining}")
    typer.echo(f"dry_run_rows={progress.dry_run_rows}")
    if progress.net_pnl is not None:
        typer.echo(f"net_pnl={progress.net_pnl:.2f}")
    if progress.max_drawdown is not None:
        typer.echo(f"max_drawdown={progress.max_drawdown:.2f}")
    if progress.cash_drawdown is not None:
        typer.echo(f"cash_drawdown={progress.cash_drawdown:.2f}")
    if progress.reconciled is not None:
        typer.echo(f"reconciled={str(progress.reconciled).lower()}")
    typer.echo(f"paper_progress={output_path}")
    if progress.failed_reasons:
        typer.echo(f"failed_reasons={','.join(progress.failed_reasons)}")


@app.command()
def paper_session_calendar(
    execution_log_path: str = "data/logs/execution_log.paper_combined.jsonl",
    broker_report_path: str = "data/logs/futu_order_report.paper_combined.json",
    output_path: str = "data/logs/paper_session_calendar.json",
    as_of_date: str | None = None,
    target_sessions: int = 20,
    require_collect_today: bool = False,
) -> None:
    calendar = write_paper_session_calendar(
        execution_log_path=Path(execution_log_path),
        broker_report_path=Path(broker_report_path),
        output_path=Path(output_path),
        as_of_date=as_of_date,
        target_sessions=target_sessions,
    )
    typer.echo(f"next_required_action={calendar.next_required_action}")
    typer.echo(f"as_of_date={calendar.as_of_date}")
    typer.echo(f"has_session_today={str(calendar.has_session_today).lower()}")
    typer.echo(f"inferred_session_count={calendar.inferred_session_count}")
    typer.echo(f"sessions_remaining={calendar.sessions_remaining}")
    if calendar.last_session_date:
        typer.echo(f"last_session_date={calendar.last_session_date}")
    typer.echo(f"paper_session_calendar={output_path}")
    if calendar.failed_reasons:
        typer.echo(f"failed_reasons={','.join(calendar.failed_reasons)}")
    if (
        require_collect_today
        and calendar.next_required_action != "collect_today_paper_session"
    ):
        typer.echo("status=blocked")
        typer.echo(
            "failed_reason=paper_session_calendar_not_collect_today:"
            f"{calendar.next_required_action}"
        )
        raise typer.Exit(1)


@app.command()
def combine_paper_evidence(
    execution_log_paths: str,
    broker_report_paths: str,
    output_execution_log_path: str = "data/logs/execution_log.paper_combined.jsonl",
    output_broker_report_path: str = "data/logs/futu_order_report.paper_combined.json",
) -> None:
    result = combine_paper_evidence_files(
        execution_log_paths=_split_paths(execution_log_paths),
        broker_report_paths=_split_paths(broker_report_paths),
        output_execution_log_path=Path(output_execution_log_path),
        output_broker_report_path=Path(output_broker_report_path),
    )
    if result.failed_reasons:
        typer.echo("status=blocked")
        typer.echo(f"execution_log_rows={result.execution_log_rows}")
        typer.echo(f"broker_report_rows={result.broker_report_rows}")
        typer.echo(f"failed_reasons={','.join(result.failed_reasons)}")
        return
    typer.echo("status=ready")
    typer.echo(f"execution_log_rows={result.execution_log_rows}")
    typer.echo(f"broker_report_rows={result.broker_report_rows}")
    typer.echo(f"execution_log_path={result.execution_log_path}")
    typer.echo(f"broker_report_path={result.broker_report_path}")


@app.command()
def paper_simulate_status(
    response_path: str = "data/logs/opend_paper_ticket_responses.jsonl",
    output_path: str = "data/logs/paper_simulate_status.json",
) -> None:
    status = write_paper_simulate_status(
        response_path=Path(response_path),
        output_path=Path(output_path),
    )
    typer.echo(
        "ready_for_session_collection="
        f"{str(status.ready_for_session_collection).lower()}"
    )
    typer.echo(f"response_rows={status.response_rows}")
    typer.echo(f"paper_rows={status.paper_rows}")
    typer.echo(f"dry_run_rows={status.dry_run_rows}")
    typer.echo(f"submitted_rows={status.submitted_rows}")
    typer.echo(f"order_report_rows={status.order_report_rows}")
    typer.echo(f"paper_simulate_status={output_path}")
    if status.failed_reasons:
        typer.echo(f"failed_reasons={','.join(status.failed_reasons)}")


@app.command()
def paper_blocker_report(
    output_path: str = "data/logs/paper_blocker_report.json",
    opend_runtime_status_path: str | None = "data/logs/opend_runtime_status.json",
    paper_simulate_status_path: str | None = "data/logs/paper_simulate_status.json",
    paper_session_calendar_path: str | None = "data/logs/paper_session_calendar.json",
    paper_progress_path: str | None = "data/logs/paper_progress.json",
) -> None:
    report = write_paper_blocker_report(
        output_path=Path(output_path),
        runtime_status_path=Path(opend_runtime_status_path)
        if opend_runtime_status_path
        else None,
        paper_simulate_status_path=Path(paper_simulate_status_path)
        if paper_simulate_status_path
        else None,
        paper_calendar_path=Path(paper_session_calendar_path)
        if paper_session_calendar_path
        else None,
        paper_progress_path=Path(paper_progress_path) if paper_progress_path else None,
    )
    typer.echo(f"ready_for_next_session={str(report.ready_for_next_session).lower()}")
    if report.ready_for_live_review is not None:
        typer.echo(f"ready_for_live_review={str(report.ready_for_live_review).lower()}")
    if report.next_required_action:
        typer.echo(f"next_required_action={report.next_required_action}")
    if report.sessions_remaining is not None:
        typer.echo(f"sessions_remaining={report.sessions_remaining}")
    typer.echo(f"paper_blocker_report={output_path}")
    if report.failed_reasons:
        typer.echo(f"failed_reasons={','.join(report.failed_reasons)}")
    if report.next_session_failed_reasons:
        typer.echo(
            f"next_session_failed_reasons={','.join(report.next_session_failed_reasons)}"
        )


def _split_paths(value: str) -> tuple[Path, ...]:
    return tuple(Path(item.strip()) for item in value.split(",") if item.strip())


@app.command()
def build_mark_prices_from_opend_quote(
    quote_snapshot_path: str = "data/logs/opend_quote_snapshot.json",
    output_path: str = "data/logs/mark_prices.json",
) -> None:
    try:
        marks = build_mark_prices_from_opend_quote_snapshot(
            Path(quote_snapshot_path),
            Path(output_path),
        )
    except Exception as exc:
        typer.echo("status=blocked")
        typer.echo(f"failed_reasons=mark_prices_from_opend_quote_failed:{type(exc).__name__}")
        typer.echo(f"error={exc}")
        return
    typer.echo("status=ready")
    typer.echo(f"mark_price_count={len(marks)}")
    typer.echo(f"mark_prices_path={output_path}")


@app.command()
def data_source_precheck(
    tushare_token: str | None = None,
    ifind_username: str | None = None,
    ifind_password: str | None = None,
    ifind_access_token: str | None = None,
    ifind_refresh_token: str | None = None,
) -> None:
    resolved_tushare_token = settings.tushare_token if tushare_token is None else tushare_token
    resolved_ifind_username = settings.ifind_username if ifind_username is None else ifind_username
    resolved_ifind_password = settings.ifind_password if ifind_password is None else ifind_password
    use_env_ifind_tokens = ifind_username is None and ifind_password is None
    resolved_ifind_access_token = (
        settings.ifind_access_token
        if ifind_access_token is None and use_env_ifind_tokens
        else ifind_access_token
    )
    resolved_ifind_refresh_token = (
        settings.ifind_refresh_token
        if ifind_refresh_token is None and use_env_ifind_tokens
        else ifind_refresh_token
    )
    checks = [
        check_tushare_readiness(resolved_tushare_token),
        check_ifind_readiness(
            resolved_ifind_username,
            resolved_ifind_password,
            resolved_ifind_access_token,
            resolved_ifind_refresh_token,
        ),
    ]
    for check in checks:
        typer.echo(f"{check.source}_ready={str(check.ready).lower()}")
        if check.failed_reasons:
            typer.echo(f"{check.source}_failed_reasons={','.join(check.failed_reasons)}")


@app.command()
def data_adapter_status(
    lake_root: str = "data/lake",
    tushare_token: str | None = None,
    ifind_username: str | None = None,
    ifind_password: str | None = None,
    ifind_access_token: str | None = None,
    ifind_refresh_token: str | None = None,
    ifind_adapter_status: str = "auto",
) -> None:
    resolved_tushare_token = settings.tushare_token if tushare_token is None else tushare_token
    resolved_ifind_username = settings.ifind_username if ifind_username is None else ifind_username
    resolved_ifind_password = settings.ifind_password if ifind_password is None else ifind_password
    use_env_ifind_tokens = ifind_username is None and ifind_password is None
    resolved_ifind_access_token = (
        settings.ifind_access_token
        if ifind_access_token is None and use_env_ifind_tokens
        else ifind_access_token
    )
    resolved_ifind_refresh_token = (
        settings.ifind_refresh_token
        if ifind_refresh_token is None and use_env_ifind_tokens
        else ifind_refresh_token
    )
    resolved_ifind_adapter_status = (
        "real_adapter"
        if ifind_adapter_status == "auto"
        and (
            settings.ifind_events_endpoint
            or (resolved_ifind_access_token and resolved_ifind_refresh_token)
        )
        else "stub_adapter"
        if ifind_adapter_status == "auto"
        else ifind_adapter_status
    )
    if (
        resolved_ifind_adapter_status in {"real_adapter", "real_file_adapter"}
        and not _lake_has_real_ifind_events(Path(lake_root))
    ):
        resolved_ifind_adapter_status = f"{resolved_ifind_adapter_status}_missing_lake_data"
    statuses = [
        build_source_adapter_status(
            check_tushare_readiness(resolved_tushare_token),
            adapter_status="real_adapter",
        ),
        build_source_adapter_status(
            check_ifind_readiness(
                resolved_ifind_username,
                resolved_ifind_password,
                resolved_ifind_access_token,
                resolved_ifind_refresh_token,
            ),
            adapter_status=resolved_ifind_adapter_status,
        ),
    ]
    for status in statuses:
        typer.echo(f"{status.source}_credential_ready={str(status.credential_ready).lower()}")
        typer.echo(f"{status.source}_adapter_status={status.adapter_status}")
        typer.echo(f"{status.source}_live_data_ready={str(status.live_data_ready).lower()}")
        if status.failed_reasons:
            typer.echo(f"{status.source}_failed_reasons={','.join(status.failed_reasons)}")


@app.command()
def ifind_ingestion_status(
    lake_root: str = "data/lake",
    output_path: str = "data/logs/ifind_ingestion_status.json",
    ifind_access_token: str | None = None,
    ifind_refresh_token: str | None = None,
    events_endpoint: str | None = None,
) -> None:
    resolved_access_token = (
        settings.ifind_access_token if ifind_access_token is None else ifind_access_token
    )
    resolved_refresh_token = (
        settings.ifind_refresh_token if ifind_refresh_token is None else ifind_refresh_token
    )
    resolved_endpoint = (
        settings.ifind_events_endpoint if events_endpoint is None else events_endpoint
    )
    token_status = inspect_ifind_tokens(
        access_token=resolved_access_token,
        refresh_token=resolved_refresh_token,
    )
    lake_counts = ifind_lake_counts(Path(lake_root))
    real_rows = lake_counts["ifind_real_rows"] + lake_counts["ifind_real_file_rows"]
    failed_reasons: list[str] = []
    if not token_status.token_pair_present:
        failed_reasons.append("missing_ifind_token_pair")
    if token_status.refresh_token_valid_now is False:
        failed_reasons.append("expired_ifind_refresh_token")
    if real_rows <= 0:
        failed_reasons.append("missing_real_ifind_lake_rows")
    payload = {
        "ready": not failed_reasons,
        "lake_root": lake_root,
        "events_endpoint_configured": bool(resolved_endpoint),
        "access_token_present": token_status.has_access_token,
        "refresh_token_present": token_status.has_refresh_token,
        "refresh_token_expires_at": (
            token_status.refresh_token_expires_at.isoformat()
            if token_status.refresh_token_expires_at
            else None
        ),
        "refresh_token_valid_now": token_status.refresh_token_valid_now,
        "refresh_token_parse_error": token_status.parse_error,
        **lake_counts,
        "failed_reasons": failed_reasons,
        "next_actions": [
            "run fetch-ifind-to-lake --use-real for official report_query smoke",
            "or export iFind events from terminal and run validate/import-ifind-events-file",
        ]
        if failed_reasons
        else [],
    }
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    typer.echo(f"ifind_ingestion_ready={str(payload['ready']).lower()}")
    typer.echo(f"ifind_total_rows={lake_counts['ifind_total_rows']}")
    typer.echo(f"ifind_real_rows={lake_counts['ifind_real_rows']}")
    typer.echo(f"ifind_real_file_rows={lake_counts['ifind_real_file_rows']}")
    typer.echo(f"ifind_stub_rows={lake_counts['ifind_stub_rows']}")
    typer.echo(f"ifind_ingestion_status={output}")
    if failed_reasons:
        typer.echo(f"failed_reasons={','.join(failed_reasons)}")


@app.command()
def external_repo_precheck(
    hshare_lab_root: str | None = None,
    factor_factory_root: str | None = None,
    hshare_data_root: str | None = None,
    tushare_repo_root: str | None = None,
    opend_execution_root: str | None = None,
) -> None:
    defaults = default_external_repo_config()
    result = check_external_repos(
        ExternalRepoConfig(
            hshare_lab_root=Path(hshare_lab_root) if hshare_lab_root else defaults.hshare_lab_root,
            factor_factory_root=Path(factor_factory_root)
            if factor_factory_root
            else defaults.factor_factory_root,
            hshare_data_root=(
                Path(hshare_data_root) if hshare_data_root else defaults.hshare_data_root
            ),
            tushare_repo_root=(
                Path(tushare_repo_root) if tushare_repo_root else defaults.tushare_repo_root
            ),
            opend_execution_root=(
                Path(opend_execution_root)
                if opend_execution_root
                else defaults.opend_execution_root
            ),
        )
    )
    typer.echo(f"external_repos_ready={str(result.ready).lower()}")
    for check in (
        result.hshare_lab,
        result.factor_factory,
        result.tushare_repo,
        result.opend_execution,
    ):
        typer.echo(f"{check.name}_ready={str(check.ready).lower()}")
        typer.echo(f"{check.name}_root={check.root}")
        if check.missing_paths:
            typer.echo(
                f"{check.name}_missing_paths="
                f"{','.join(str(path) for path in check.missing_paths)}"
            )
    for name, path in result.reusable_inputs.items():
        typer.echo(f"reuse.{name}={path}")


@app.command()
def research_input_manifest(
    output_path: str = "data/logs/research_input_manifest.json",
    hshare_lab_root: str | None = None,
    factor_factory_root: str | None = None,
    hshare_data_root: str | None = None,
    tushare_repo_root: str | None = None,
    opend_execution_root: str | None = None,
    max_sample_paths: int = 8,
) -> None:
    defaults = default_external_repo_config()
    manifest = build_research_input_manifest(
        ResearchInputManifestConfig(
            external_repos=ExternalRepoConfig(
                hshare_lab_root=Path(hshare_lab_root)
                if hshare_lab_root
                else defaults.hshare_lab_root,
                factor_factory_root=Path(factor_factory_root)
                if factor_factory_root
                else defaults.factor_factory_root,
                hshare_data_root=(
                    Path(hshare_data_root) if hshare_data_root else defaults.hshare_data_root
                ),
                tushare_repo_root=(
                    Path(tushare_repo_root) if tushare_repo_root else defaults.tushare_repo_root
                ),
                opend_execution_root=(
                    Path(opend_execution_root)
                    if opend_execution_root
                    else defaults.opend_execution_root
                ),
            ),
            max_sample_paths=max_sample_paths,
        )
    )
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    typer.echo(f"research_inputs_ready={str(manifest['ready']).lower()}")
    typer.echo(f"research_input_manifest={output}")


@app.command()
def factor_factory_summary(
    registry_root: str | None = None,
    lake_root: str = "data/lake",
    candidates_dataset: str = "factor_factory_candidates",
    families_dataset: str = "factor_factory_families",
) -> None:
    defaults = default_external_repo_config()
    root = Path(registry_root) if registry_root else defaults.factor_factory_root / "registry"
    summary = load_factor_factory_summary(root)
    store = ParquetStore(Path(lake_root))
    candidates_output = store.write(candidates_dataset, summary.candidates)
    families_output = store.write(families_dataset, summary.family_summary)
    passed = (
        int((summary.candidates["decision"] == "pass").sum())
        if "decision" in summary.candidates.columns
        else 0
    )
    typer.echo(f"registry_root={root}")
    typer.echo(f"candidate_rows={summary.candidates.height}")
    typer.echo(f"gate_b_passed={passed}")
    typer.echo(f"family_rows={summary.family_summary.height}")
    typer.echo(f"candidates_output={candidates_output}")
    typer.echo(f"families_output={families_output}")


@app.command()
def external_factor_portfolio(
    lake_root: str = "data/lake",
    candidates_dataset: str = "factor_factory_candidates",
    output_dataset: str = "external_factor_portfolio",
    account_equity: float = 1_000_000.0,
    min_abs_rank_ic: float = 0.05,
    min_nmi: float = 0.01,
    max_family_weight: float = 0.50,
) -> None:
    store = ParquetStore(Path(lake_root))
    candidates = store.read(candidates_dataset)
    portfolio = build_external_factor_portfolio(
        candidates,
        account_profile=personal_trader_profile(account_equity=account_equity),
        min_abs_rank_ic=min_abs_rank_ic,
        min_nmi=min_nmi,
        max_family_weight=max_family_weight,
    )
    output = store.write(output_dataset, portfolio)
    reviewable = portfolio.filter(pl.col("candidate_status") == "review_candidate")
    typer.echo(f"candidate_rows={candidates.height}")
    typer.echo(f"review_candidate_rows={reviewable.height}")
    typer.echo(f"target_notional={reviewable['target_notional'].sum():.2f}")
    typer.echo(f"output={output}")


@app.command()
def external_portfolio_audit(
    lake_root: str = "data/lake",
    portfolio_dataset: str = "external_factor_portfolio",
    account_equity: float = 1_000_000.0,
    min_review_candidates: int = 1,
    min_target_notional: float = 1.0,
    assumed_total_cost_bps: float = 35.0,
    max_total_cost_bps: float = 35.0,
) -> None:
    portfolio = ParquetStore(Path(lake_root)).read(portfolio_dataset)
    profile = personal_trader_profile(account_equity=account_equity)
    evidence = evaluate_external_factor_portfolio(
        portfolio,
        account_profile=profile,
        min_review_candidates=min_review_candidates,
        min_target_notional=min_target_notional,
    )
    cost_audit, capacity_audit = audit_external_portfolio_cost_capacity(
        portfolio,
        account_profile=profile,
        assumed_total_cost_bps=assumed_total_cost_bps,
        max_total_cost_bps=max_total_cost_bps,
    )
    typer.echo(f"external_portfolio_approved={str(evidence.approved).lower()}")
    typer.echo(f"candidate_count={evidence.candidate_count}")
    typer.echo(f"review_candidate_count={evidence.review_candidate_count}")
    typer.echo(f"target_notional={evidence.target_notional:.2f}")
    typer.echo(f"max_single_candidate_notional={evidence.max_single_candidate_notional:.2f}")
    typer.echo(f"cost_audit_passed={str(cost_audit.passed).lower()}")
    typer.echo(f"capacity_audit_passed={str(capacity_audit.passed).lower()}")
    if evidence.failed_reasons:
        typer.echo(f"failed_reasons={','.join(evidence.failed_reasons)}")
    if cost_audit.failed_reasons:
        typer.echo(f"cost_failed_reasons={','.join(cost_audit.failed_reasons)}")
    if capacity_audit.failed_reasons:
        typer.echo(f"capacity_failed_reasons={','.join(capacity_audit.failed_reasons)}")


@app.command()
def external_lookahead_audit(
    lake_root: str = "data/lake",
    portfolio_dataset: str = "external_factor_portfolio",
    registry_root: str | None = None,
) -> None:
    defaults = default_external_repo_config()
    resolved_registry = (
        Path(registry_root) if registry_root else defaults.factor_factory_root / "registry"
    )
    portfolio = ParquetStore(Path(lake_root)).read(portfolio_dataset)
    evidence = audit_factor_factory_lookahead_lineage(portfolio, registry_root=resolved_registry)
    typer.echo(f"lookahead_audit_passed={str(evidence.passed).lower()}")
    typer.echo(f"reviewed_factor_count={evidence.reviewed_factor_count}")
    typer.echo(f"matched_pre_eval_count={evidence.matched_pre_eval_count}")
    if evidence.failed_reasons:
        typer.echo(f"failed_reasons={','.join(evidence.failed_reasons)}")


@app.command()
def paper_session_plan(
    output_path: str = "data/logs/paper_session_plan.json",
    lake_root: str = "data/lake",
    portfolio_dataset: str = "external_factor_portfolio",
    account_equity: float = 1_000_000.0,
    session_id: str = "paper_next",
    execution_log_path: str = "data/logs/execution_log.jsonl",
    broker_report_path: str = "data/logs/futu_order_report.json",
    opend_env: str = "SIMULATE",
    opend_mode: str = "paper",
    quote_snapshot_path: str | None = None,
    allow_lot_round_up: bool = False,
) -> None:
    portfolio = ParquetStore(Path(lake_root)).read(portfolio_dataset)
    quote_snapshot = (
        json.loads(Path(quote_snapshot_path).read_text(encoding="utf-8"))
        if quote_snapshot_path
        else None
    )
    plan = build_paper_session_plan(
        PaperSessionPlanInput(
            portfolio=portfolio,
            account_profile=personal_trader_profile(account_equity=account_equity),
            session_id=session_id,
            execution_log_path=Path(execution_log_path),
            broker_report_path=Path(broker_report_path),
            opend_env=opend_env,
            opend_mode=opend_mode,
            quote_snapshot=quote_snapshot,
            allow_lot_round_up=allow_lot_round_up,
        )
    )
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(plan, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    typer.echo(f"ready_for_paper={str(plan['ready_for_paper']).lower()}")
    typer.echo(f"planned_order_count={plan['planned_order_count']}")
    typer.echo(f"planned_total_notional={float(plan['planned_total_notional']):.2f}")
    typer.echo(f"rounded_order_count={plan['lot_sizing']['rounded_order_count']}")
    typer.echo(f"paper_session_plan={output}")
    if plan["failed_reasons"]:
        typer.echo(f"failed_reasons={','.join(plan['failed_reasons'])}")


@app.command()
def export_opend_paper_tickets(
    plan_path: str = "data/logs/paper_session_plan.json",
    output_path: str = "data/logs/opend_paper_tickets.jsonl",
    symbol: str | None = None,
    reference_price: float | None = None,
    lot_size: int | None = None,
    order_type: str = "NORMAL",
    quote_snapshot_path: str | None = None,
) -> None:
    plan = json.loads(Path(plan_path).read_text(encoding="utf-8"))
    quote_snapshot = (
        json.loads(Path(quote_snapshot_path).read_text(encoding="utf-8"))
        if quote_snapshot_path
        else None
    )
    result = write_opend_paper_tickets(
        plan,
        Path(output_path),
        symbol=symbol,
        reference_price=reference_price,
        lot_size=lot_size,
        order_type=order_type,
        quote_snapshot=quote_snapshot,
    )
    if result.failed_reasons:
        typer.echo("status=blocked")
        typer.echo(f"failed_reasons={','.join(result.failed_reasons)}")
        return
    typer.echo("status=ready_for_opend_dry_run")
    typer.echo(f"ticket_count={result.ticket_count}")
    typer.echo(f"opend_ticket_path={result.output_path}")


@app.command()
def submit_opend_paper_tickets(
    ticket_path: str = "data/logs/opend_paper_tickets.jsonl",
    output_path: str = "data/logs/opend_paper_ticket_responses.jsonl",
    base_url: str = "http://127.0.0.1:8766",
    submit_paper_simulate: bool = False,
    opend_runtime_status_path: str | None = None,
    allow_resubmit: bool = False,
    allow_failed_resubmit: bool = False,
    timeout_seconds: float = 8.0,
    max_attempts: int = 3,
    retry_delay_seconds: float = 0.5,
) -> None:
    result = post_opend_paper_tickets(
        Path(ticket_path),
        Path(output_path),
        base_url=base_url,
        submit_paper_simulate=submit_paper_simulate,
        opend_runtime_status_path=Path(opend_runtime_status_path)
        if opend_runtime_status_path
        else None,
        allow_resubmit=allow_resubmit,
        allow_failed_resubmit=allow_failed_resubmit,
        timeout_seconds=timeout_seconds,
        max_attempts=max_attempts,
        retry_delay_seconds=retry_delay_seconds,
    )
    if result.failed_reasons:
        typer.echo("status=blocked")
        typer.echo(f"submitted_count={result.submitted_count}")
        typer.echo(f"failed_reasons={','.join(result.failed_reasons)}")
        return
    typer.echo("status=ready")
    typer.echo(f"submitted_count={result.submitted_count}")
    typer.echo(f"response_count={result.response_count}")
    typer.echo(f"opend_response_path={result.output_path}")


@app.command()
def fetch_opend_account_status(
    output_path: str = "data/logs/opend_account_status.json",
    base_url: str = "http://127.0.0.1:8766",
    timeout_seconds: float = 8.0,
    require_paper_simulate_ready: bool = False,
) -> None:
    try:
        status = read_opend_account_status(
            base_url=base_url,
            timeout_seconds=timeout_seconds,
        )
    except Exception as exc:
        typer.echo("status=blocked")
        typer.echo(f"failed_reasons=opend_account_status_failed:{type(exc).__name__}")
        typer.echo(f"error={exc}")
        return
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(status, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    typer.echo(f"ready_for_paper_simulate={str(status['ready_for_paper_simulate']).lower()}")
    typer.echo(f"simulate_account_count={status['simulate_account_count']}")
    typer.echo(f"hk_stock_simulate_account_count={status['hk_stock_simulate_account_count']}")
    typer.echo(f"opend_account_status={output}")
    if status["failed_reasons"]:
        typer.echo(f"failed_reasons={','.join(status['failed_reasons'])}")
    if require_paper_simulate_ready and not status["ready_for_paper_simulate"]:
        typer.echo("status=blocked")
        typer.echo("failed_reason=opend_account_not_ready_for_paper_simulate")
        raise typer.Exit(1)


@app.command()
def fetch_opend_runtime_status(
    output_path: str = "data/logs/opend_runtime_status.json",
    base_url: str = "http://127.0.0.1:8766",
    timeout_seconds: float = 8.0,
    require_order_submission_ready: bool = False,
) -> None:
    try:
        status = read_opend_runtime_status(
            base_url=base_url,
            timeout_seconds=timeout_seconds,
        )
    except Exception as exc:
        typer.echo("status=blocked")
        typer.echo(f"failed_reasons=opend_runtime_status_failed:{type(exc).__name__}")
        typer.echo(f"error={exc}")
        return
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(status, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    typer.echo(
        "ready_for_order_submission="
        f"{str(status['ready_for_order_submission']).lower()}"
    )
    typer.echo(f"kill_switch={str(status.get('kill_switch') is True).lower()}")
    typer.echo(f"opend_runtime_status={output}")
    if status["failed_reasons"]:
        typer.echo(f"failed_reasons={','.join(status['failed_reasons'])}")
    if require_order_submission_ready and not status["ready_for_order_submission"]:
        typer.echo("status=blocked")
        typer.echo("failed_reason=opend_runtime_not_ready_for_order_submission")
        raise typer.Exit(1)


@app.command()
def fetch_opend_quote_snapshot(
    symbol: str,
    output_path: str = "data/logs/opend_quote_snapshot.json",
    base_url: str = "http://127.0.0.1:8766",
    timeout_seconds: float = 8.0,
) -> None:
    try:
        snapshot = read_opend_quote_snapshot(
            base_url=base_url,
            symbol=symbol,
            timeout_seconds=timeout_seconds,
        )
    except Exception as exc:
        typer.echo("status=blocked")
        typer.echo(f"failed_reasons=opend_quote_snapshot_failed:{type(exc).__name__}")
        typer.echo(f"error={exc}")
        return
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(snapshot, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    quote = snapshot.get("quote", {}) if isinstance(snapshot.get("quote"), dict) else {}
    typer.echo("status=ready")
    typer.echo(f"symbol={quote.get('symbol')}")
    typer.echo(f"lot_size={quote.get('lot_size')}")
    typer.echo(f"quote_snapshot_path={output}")


@app.command()
def build_opend_dry_run_evidence(
    ticket_path: str = "data/logs/opend_paper_tickets.jsonl",
    execution_log_path: str = "data/logs/execution_log.dry_run.jsonl",
    broker_report_path: str = "data/logs/futu_order_report.dry_run.json",
) -> None:
    result = build_dry_run_execution_evidence(
        Path(ticket_path),
        execution_log_path=Path(execution_log_path),
        broker_report_path=Path(broker_report_path),
    )
    if result.failed_reasons:
        typer.echo("status=blocked")
        typer.echo(f"failed_reasons={','.join(result.failed_reasons)}")
        return
    typer.echo("status=ready")
    typer.echo(f"order_count={result.order_count}")
    typer.echo(f"execution_log_path={result.execution_log_path}")
    typer.echo(f"broker_report_path={result.broker_report_path}")


@app.command()
def hshare_verified_audit(
    summary_path: str = "/Volumes/Data/港股Tick数据/verified/manifests/year=2025/summary.json",
    min_selected_dates: int = 20,
    allow_partial: bool = False,
) -> None:
    evidence = evaluate_hshare_verified_summary(
        Path(summary_path),
        min_selected_dates=min_selected_dates,
        allow_partial=allow_partial,
    )
    typer.echo(f"hshare_verified_ready={str(evidence.ready).lower()}")
    typer.echo(f"year={evidence.year}")
    typer.echo(f"selected_date_count={evidence.selected_date_count}")
    typer.echo(f"completed_count={evidence.completed_count}")
    typer.echo(f"failed_count={evidence.failed_count}")
    typer.echo(f"orders_rows={evidence.orders_rows}")
    typer.echo(f"trades_rows={evidence.trades_rows}")
    typer.echo(f"is_partial={str(evidence.is_partial).lower()}")
    if evidence.failed_reasons:
        typer.echo(f"failed_reasons={','.join(evidence.failed_reasons)}")


@app.command()
def build_external_research_signals(
    lake_root: str = "data/lake",
    portfolio_dataset: str = "external_factor_portfolio",
    output_dataset: str = "external_research_signal_events",
    hshare_verified_summary_path: str = (
        "/Volumes/Data/港股Tick数据/verified/manifests/year=2025/summary.json"
    ),
    max_dates: int | None = None,
) -> None:
    store = ParquetStore(Path(lake_root))
    portfolio = store.read(portfolio_dataset)
    signals = build_external_research_signal_events(
        portfolio,
        hshare_summary_path=Path(hshare_verified_summary_path),
        max_dates=max_dates,
    )
    output = store.write(output_dataset, signals)
    distinct_dates = (
        signals.select(pl.col("trade_date")).unique().height if not signals.is_empty() else 0
    )
    typer.echo(f"portfolio_rows={portfolio.height}")
    typer.echo(f"signal_rows={signals.height}")
    typer.echo(f"distinct_trade_dates={distinct_dates}")
    typer.echo(f"output={output}")


@app.command()
def research_audit(
    lake_root: str = "data/lake",
    signal_dataset: str = "signal_events",
    no_lookahead_audit_passed: bool = False,
    cost_model_applied: bool = False,
    capacity_check_passed: bool = False,
) -> None:
    signal_events = ParquetStore(Path(lake_root)).read(signal_dataset)
    result = run_research_promotion_audit(
        ResearchAuditInput(
            signal_events=signal_events,
            no_lookahead_audit_passed=no_lookahead_audit_passed,
            cost_model_applied=cost_model_applied,
            capacity_check_passed=capacity_check_passed,
        )
    )
    typer.echo(f"research_to_paper_approved={str(result.decision.approved).lower()}")
    typer.echo(f"trade_count={result.evidence.trade_count}")
    typer.echo(f"distinct_trade_dates={result.evidence.distinct_trade_dates}")
    if result.decision.failed_reasons:
        typer.echo(f"failed_reasons={','.join(result.decision.failed_reasons)}")


@app.command()
def order_add_research_gate(
    lake_root: str = "data/lake",
    threshold_sweep_dataset: str = "order_add_threshold_sweep",
    account_equity: float = 1_000_000.0,
    min_trade_count: int = 30,
    min_avg_net_ret: float = 0.0,
    min_total_net_ret: float = 0.0,
) -> None:
    threshold_sweep = ParquetStore(Path(lake_root)).read(threshold_sweep_dataset)
    result = evaluate_order_add_research_gate(
        threshold_sweep,
        min_trade_count=min_trade_count,
        min_avg_net_ret=min_avg_net_ret,
        min_total_net_ret=min_total_net_ret,
        account_profile=personal_trader_profile(account_equity=account_equity),
    )
    typer.echo(f"order_add_research_approved={str(result.approved).lower()}")
    if result.best_params:
        typer.echo(f"best_volume={result.best_params['min_order_add_volume']}")
        typer.echo(
            f"best_large_order_ratio={result.best_params['min_large_order_ratio']:.6f}"
        )
        typer.echo(f"best_trade_count={result.best_params['trade_count']}")
        typer.echo(f"best_avg_net_ret={result.best_params['avg_net_ret']:.8f}")
        typer.echo(f"best_total_net_ret={result.best_params['total_net_ret']:.8f}")
        typer.echo(
            "estimated_single_trade_notional="
            f"{result.best_params['estimated_single_trade_notional']:.2f}"
        )
        typer.echo(
            "estimated_strategy_notional="
            f"{result.best_params['estimated_strategy_notional']:.2f}"
        )
    if result.failed_reasons:
        typer.echo(f"failed_reasons={','.join(result.failed_reasons)}")


@app.command()
def ops_report(
    output_path: str = "data/logs/ops_daily_report.md",
    lake_root: str = "data/lake",
    account_equity: float = 1_000_000.0,
    opend_env: str = "SIMULATE",
    opend_mode: FutuMode = FutuMode.DRY_RUN,
    max_age_hours: int = 24,
    signal_dataset: str = "signal_events",
    include_research_audit: bool = False,
    no_lookahead_audit_passed: bool = False,
    cost_model_applied: bool = False,
    capacity_check_passed: bool = False,
    use_external_portfolio_audits: bool = False,
    use_external_lookahead_audit: bool = False,
    external_portfolio_dataset: str = "external_factor_portfolio",
    execution_log_path: str | None = None,
    futu_report_path: str | None = None,
    paper_sessions: int = 0,
    max_allowed_slippage: float = 0.05,
    manual_live_enable: bool = False,
) -> None:
    profile = personal_trader_profile(account_equity=account_equity)
    readiness = check_opend_readiness(
        FutuOpenDConfig(env=opend_env, mode=opend_mode)
    )
    research_to_paper = None
    if include_research_audit:
        cost_audit = None
        capacity_audit = None
        resolved_no_lookahead = no_lookahead_audit_passed
        if use_external_portfolio_audits:
            cost_audit, capacity_audit = audit_external_portfolio_cost_capacity(
                ParquetStore(Path(lake_root)).read(external_portfolio_dataset),
                account_profile=profile,
            )
        if use_external_lookahead_audit:
            resolved_no_lookahead = audit_factor_factory_lookahead_lineage(
                ParquetStore(Path(lake_root)).read(external_portfolio_dataset),
                registry_root=default_external_repo_config().factor_factory_root / "registry",
            ).passed
        research_to_paper = run_research_promotion_audit(
            ResearchAuditInput(
                signal_events=ParquetStore(Path(lake_root)).read(signal_dataset),
                no_lookahead_audit_passed=resolved_no_lookahead,
                cost_model_applied=cost_model_applied,
                capacity_check_passed=capacity_check_passed,
                cost_audit=cost_audit,
                capacity_audit=capacity_audit,
            )
        ).decision

    paper_to_live = None
    if execution_log_path and futu_report_path:
        local_records = load_execution_log(Path(execution_log_path))
        broker_reports = futu_order_reports_to_execution_reports(
            load_futu_order_report_rows(Path(futu_report_path))
        )
        reconciliation = reconcile_execution_reports(local_records, broker_reports)
        paper_to_live = run_paper_promotion_audit(
            PaperAuditInput(
                paper_sessions=paper_sessions,
                local_records=local_records,
                reconciliation=reconciliation,
                max_allowed_slippage=max_allowed_slippage,
                manual_live_enable=manual_live_enable,
            )
        ).decision

    path = render_ops_daily_report(
        Path(output_path),
        OpsDailyReportInput(
            report_date=datetime.now(UTC).date(),
            account_profile=profile,
            opend_readiness=readiness,
            research_to_paper=research_to_paper,
            paper_to_live=paper_to_live,
            data_source_readiness=(
                check_tushare_readiness(settings.tushare_token),
                check_ifind_readiness(
                    settings.ifind_username,
                    settings.ifind_password,
                    settings.ifind_access_token,
                    settings.ifind_refresh_token,
                ),
            ),
            data_freshness=build_freshness_status(
                Path(lake_root),
                ["security_master", "daily_features", "intraday_l2_features", "ifind_events"],
                max_age=timedelta(hours=max_age_hours),
            ),
        ),
    )
    typer.echo(f"ops_report={path}")


@app.command()
def go_live_readiness(
    output_path: str = "data/logs/go_live_readiness.json",
    lake_root: str = "data/lake",
    account_equity: float = 1_000_000.0,
    tushare_token: str | None = None,
    ifind_username: str | None = None,
    ifind_password: str | None = None,
    ifind_access_token: str | None = None,
    ifind_refresh_token: str | None = None,
    tushare_adapter_status: str = "real_adapter",
    ifind_adapter_status: str = "auto",
    opend_env: str = "SIMULATE",
    opend_mode: FutuMode = FutuMode.DRY_RUN,
    unlock_password_set: bool = False,
    manual_live_enable: bool = False,
    max_age_hours: int = 24,
    hshare_verified_summary_path: str = (
        "/Volumes/Data/港股Tick数据/verified/manifests/year=2025/summary.json"
    ),
    min_hshare_verified_dates: int = 20,
    allow_partial_hshare_verified: bool = False,
    external_portfolio_dataset: str = "external_factor_portfolio",
    use_external_portfolio_audits: bool = False,
    use_external_lookahead_audit: bool = False,
    signal_dataset: str = "signal_events",
    no_lookahead_audit_passed: bool = False,
    cost_model_applied: bool = False,
    capacity_check_passed: bool = False,
    execution_log_path: str | None = None,
    futu_report_path: str | None = None,
    paper_sessions: int = 0,
    max_allowed_slippage: float = 0.05,
) -> None:
    profile = personal_trader_profile(account_equity=account_equity)
    lake = Path(lake_root)
    store = ParquetStore(lake)
    data_sources = (
        check_tushare_readiness(settings.tushare_token if tushare_token is None else tushare_token),
        check_ifind_readiness(
            settings.ifind_username if ifind_username is None else ifind_username,
            settings.ifind_password if ifind_password is None else ifind_password,
            settings.ifind_access_token if ifind_access_token is None else ifind_access_token,
            settings.ifind_refresh_token if ifind_refresh_token is None else ifind_refresh_token,
        ),
    )
    resolved_ifind_adapter_status = (
        "real_adapter"
        if ifind_adapter_status == "auto"
        and (
            settings.ifind_events_endpoint
            or (
                (settings.ifind_access_token if ifind_access_token is None else ifind_access_token)
                and (
                    settings.ifind_refresh_token
                    if ifind_refresh_token is None
                    else ifind_refresh_token
                )
            )
        )
        else "stub_adapter"
        if ifind_adapter_status == "auto"
        else ifind_adapter_status
    )
    if (
        resolved_ifind_adapter_status in {"real_adapter", "real_file_adapter"}
        and not _lake_has_real_ifind_events(lake)
    ):
        resolved_ifind_adapter_status = f"{resolved_ifind_adapter_status}_missing_lake_data"
    source_adapters = (
        build_source_adapter_status(data_sources[0], adapter_status=tushare_adapter_status),
        build_source_adapter_status(data_sources[1], adapter_status=resolved_ifind_adapter_status),
    )
    freshness = tuple(
        inspect_parquet_dataset(
            lake,
            dataset,
            max_age=timedelta(hours=max_age_hours),
        )
        for dataset in [
            "security_master",
            "daily_features",
            "intraday_l2_features",
            "ifind_events",
        ]
    )
    cost_audit = None
    capacity_audit = None
    if use_external_portfolio_audits:
        cost_audit, capacity_audit = audit_external_portfolio_cost_capacity(
            store.read(external_portfolio_dataset),
            account_profile=profile,
        )
    resolved_no_lookahead = no_lookahead_audit_passed
    if use_external_lookahead_audit:
        resolved_no_lookahead = audit_factor_factory_lookahead_lineage(
            store.read(external_portfolio_dataset),
            registry_root=default_external_repo_config().factor_factory_root / "registry",
        ).passed
    research_audit_result = run_research_promotion_audit(
        ResearchAuditInput(
            signal_events=store.read(signal_dataset),
            no_lookahead_audit_passed=resolved_no_lookahead,
            cost_model_applied=cost_model_applied,
            capacity_check_passed=capacity_check_passed,
            cost_audit=cost_audit,
            capacity_audit=capacity_audit,
        )
    )
    hshare_verified_evidence = evaluate_hshare_verified_summary(
        Path(hshare_verified_summary_path),
        min_selected_dates=min_hshare_verified_dates,
        allow_partial=allow_partial_hshare_verified,
    )
    external_portfolio_evidence = evaluate_external_factor_portfolio(
        store.read(external_portfolio_dataset),
        account_profile=profile,
    )
    opend_readiness = check_opend_readiness(
        FutuOpenDConfig(
            env=opend_env,
            mode=opend_mode,
            unlock_password_set=unlock_password_set,
            manual_live_enable=manual_live_enable,
        )
    )

    paper_audit_result = None
    if execution_log_path and futu_report_path:
        local_records = load_execution_log(Path(execution_log_path))
        broker_reports = futu_order_reports_to_execution_reports(
            load_futu_order_report_rows(Path(futu_report_path))
        )
        reconciliation = reconcile_execution_reports(local_records, broker_reports)
        paper_audit_result = run_paper_promotion_audit(
            PaperAuditInput(
                paper_sessions=paper_sessions,
                local_records=local_records,
                reconciliation=reconciliation,
                max_allowed_slippage=max_allowed_slippage,
                manual_live_enable=manual_live_enable,
            )
        )

    manifest = build_go_live_readiness_manifest(
        GoLiveReadinessInput(
            account_profile=profile,
            data_sources=data_sources,
            data_freshness=freshness,
            source_adapters=source_adapters,
            opend_readiness=opend_readiness,
            research_evidence=research_audit_result.evidence,
            research_decision=research_audit_result.decision,
            hshare_verified_evidence=hshare_verified_evidence,
            external_portfolio_evidence=external_portfolio_evidence,
            paper_evidence=paper_audit_result.evidence if paper_audit_result else None,
            paper_decision=paper_audit_result.decision if paper_audit_result else None,
        )
    )
    output = Path(output_path)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    typer.echo(f"go_live_approved={str(manifest['go_live_approved']).lower()}")
    typer.echo(f"readiness_manifest={output}")


@app.command()
def objective_audit(
    readiness_manifest_path: str = "data/logs/go_live_readiness.json",
    output_path: str = "data/logs/objective_audit.json",
    research_input_manifest_path: str | None = None,
    profitability_evidence_path: str | None = None,
    ifind_validation_report_path: str | None = "data/logs/ifind_events_validation.json",
    ifind_ingestion_status_path: str | None = "data/logs/ifind_ingestion_status.json",
    opend_runtime_status_path: str | None = "data/logs/opend_runtime_status.json",
    opend_account_status_path: str | None = "data/logs/opend_account_status.json",
    opend_quote_snapshot_path: str | None = "data/logs/opend_quote_snapshot.json",
    opend_ticket_response_path: str | None = "data/logs/opend_paper_ticket_responses.jsonl",
    paper_simulate_status_path: str | None = None,
    execution_log_path: str | None = None,
    broker_report_path: str | None = None,
    paper_blocker_report_path: str | None = None,
    paper_progress_path: str | None = None,
) -> None:
    audit = build_objective_audit(
        ObjectiveAuditInput(
            readiness_manifest_path=Path(readiness_manifest_path),
            output_path=Path(output_path),
            research_input_manifest_path=Path(research_input_manifest_path)
            if research_input_manifest_path
            else None,
            profitability_evidence_path=Path(profitability_evidence_path)
            if profitability_evidence_path
            else None,
            ifind_validation_report_path=Path(ifind_validation_report_path)
            if ifind_validation_report_path
            else None,
            ifind_ingestion_status_path=Path(ifind_ingestion_status_path)
            if ifind_ingestion_status_path
            else None,
            opend_runtime_status_path=Path(opend_runtime_status_path)
            if opend_runtime_status_path
            else None,
            opend_account_status_path=Path(opend_account_status_path)
            if opend_account_status_path
            else None,
            opend_quote_snapshot_path=Path(opend_quote_snapshot_path)
            if opend_quote_snapshot_path
            else None,
            opend_ticket_response_path=Path(opend_ticket_response_path)
            if opend_ticket_response_path
            else None,
            paper_simulate_status_path=Path(paper_simulate_status_path)
            if paper_simulate_status_path
            else None,
            execution_log_path=Path(execution_log_path) if execution_log_path else None,
            broker_report_path=Path(broker_report_path) if broker_report_path else None,
            paper_blocker_report_path=Path(paper_blocker_report_path)
            if paper_blocker_report_path
            else None,
            paper_progress_path=Path(paper_progress_path) if paper_progress_path else None,
        )
    )
    typer.echo(f"objective_achieved={str(audit['objective_achieved']).lower()}")
    typer.echo(f"objective_audit={output_path}")
    if audit["blocked_requirements"]:
        typer.echo(f"blocked_requirements={','.join(audit['blocked_requirements'])}")


@app.command()
def objective_audit_report(
    audit_path: str = "data/logs/objective_audit.json",
    output_path: str = "data/logs/objective_audit.md",
) -> None:
    output = write_objective_audit_report(Path(audit_path), Path(output_path))
    typer.echo(f"objective_audit_report={output}")


def _lake_has_real_ifind_events(lake_root: Path) -> bool:
    try:
        events = ParquetStore(lake_root).read("ifind_events")
    except Exception:
        return False
    if events.is_empty() or "data_source" not in events.columns:
        return False
    return (
        events.filter(pl.col("data_source").is_in(["ifind_real", "ifind_real_file"])).height > 0
    )


def _ifind_lake_counts(lake_root: Path) -> dict[str, int]:
    return ifind_lake_counts(lake_root)


if __name__ == "__main__":
    app()
