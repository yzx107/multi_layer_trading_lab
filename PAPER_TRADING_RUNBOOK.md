# Paper Trading Runbook

本 runbook 定义从 research pass 到 paper execution evidence 的最小闭环。它不会自动提交真实订单，也不会把 paper 结果伪造成 live 证据。

## 1. 刷新每日证据链

```bash
.venv/bin/python scripts/run_daily_ops.py \
  --lake-root data/lake \
  --report-path data/logs/ops_daily_report.md \
  --readiness-path data/logs/go_live_readiness.json
```

默认会刷新：

- Hshare / 因子工厂输入清单
- 因子工厂 Gate B 候选摘要
- 外部组合预算
- Hshare verified 覆盖生成的 research audit signals
- paper session plan
- ops report
- go-live readiness manifest

默认不会读取不存在的 broker report，也不会尝试解锁 live。

## 2. 检查 paper session plan

```bash
.venv/bin/python -m multi_layer_trading_lab.cli paper-session-plan \
  --output-path data/logs/paper_session_plan.json \
  --lake-root data/lake \
  --account-equity 1000000 \
  --opend-mode paper \
  --opend-env SIMULATE \
  --quote-snapshot-path data/logs/opend_quote_snapshot.json
```

输出文件：

```text
data/logs/paper_session_plan.json
```

关键字段：

- `ready_for_paper`: 是否允许进入 paper execution 准备
- `planned_order_count`: 候选 paper 订单数
- `planned_total_notional`: paper 组合目标名义金额
- `lot_sizing`: OpenD quote 推导的 lot 约束、低于一手订单数和建议动作
- `orders[].status`: 必须是 `planned_not_submitted` 才能导出 OpenD ticket；低于一手会标记为 `blocked_below_one_lot`
- `orders[].requested_notional`: 研究侧原始目标名义金额
- `orders[].sizing_method`: `requested_notional` 或显式打开后的一手放大 `lot_round_up`
- `orders[].min_lot_notional`: 根据 OpenD quote 推导的一手最低名义金额
- `paper_evidence_paths.execution_log_path`: 本地执行日志路径
- `paper_evidence_paths.broker_report_path`: Futu/OpenD 回报文件路径

默认不会把低于一手的订单自动放大；这类订单会 fail closed，避免把结构验证误当成可下单计划。人工确认后，如果一手金额仍在单票和策略预算内，可以显式打开一手放大：

```bash
.venv/bin/python -m multi_layer_trading_lab.cli paper-session-plan \
  --output-path data/logs/paper_session_plan.lot_round_up.json \
  --lake-root data/lake \
  --account-equity 1000000 \
  --opend-mode paper \
  --opend-env SIMULATE \
  --quote-snapshot-path data/logs/opend_quote_snapshot.json \
  --allow-lot-round-up
```

## 3. 导出 OpenD dry-run tickets

`paper-session-plan` 只生成研究侧计划，不会直接变成券商订单。进入 OpenD 前必须显式提供标的、参考价格和 lot size，导出 dry-run ticket：

```bash
.venv/bin/python -m multi_layer_trading_lab.cli fetch-opend-quote-snapshot 00001.HK \
  --base-url http://127.0.0.1:8766 \
  --output-path data/logs/opend_quote_snapshot.json
.venv/bin/python -m multi_layer_trading_lab.cli export-opend-paper-tickets \
  --plan-path data/logs/paper_session_plan.json \
  --output-path data/logs/opend_paper_tickets.jsonl \
  --quote-snapshot-path data/logs/opend_quote_snapshot.json
```

导出的 `web_normal_order_payload` 对齐 `/Users/yxin/AI_Workstation/futu-opend-execution` 的 Web normal order payload，并强制 `dry_run=true`、`real=false`、`submit_real=false`。

`quote_snapshot` 应来自 OpenD `/api/quote` 或 `FutuNormalTradeClient.read_quote`，至少包含 `symbol`、`lot_size` 和一个正价格字段。没有 quote snapshot 时也可手动传 `--symbol`、`--reference-price`、`--lot-size`，但这只适合结构验证。

如果缺少 `symbol` / `reference_price` / `lot_size`，或者目标金额低于一手，命令会 fail closed。真实 paper session 应使用 OpenD 读取的实时 quote 和正确 lot size，而不是手填示例价格。

确认 ticket 后，可以把它逐条提交到 `/Users/yxin/AI_Workstation/futu-opend-execution` Web API 的 `/api/normal/order`。这个命令只允许 `dry_run=true`、`real=false` 的 ticket：

```bash
.venv/bin/python -m multi_layer_trading_lab.cli submit-opend-paper-tickets \
  --ticket-path data/logs/opend_paper_tickets.jsonl \
  --output-path data/logs/opend_paper_ticket_responses.jsonl \
  --base-url http://127.0.0.1:8766
```

如果 OpenD Web UI 未启动、ticket 试图提交真实订单、或 HTTP 调用失败，命令会 fail closed，并把失败 response 写入 JSONL 方便审计。清理 kill-switch 或修好 OpenD 后，只重试这些失败 response 时使用 `--allow-failed-resubmit`；不要用它重提已经 `submitted=true` 的 ticket，成功提交过的 ticket 仍由默认幂等保护拦截。

通过 `scripts/run_daily_ops.py --submit-opend-paper-simulate-tickets` 执行时，runtime / account / calendar precheck 阻断会跳过后续提交，但仍继续生成 blocker report、ops report、go-live readiness 和 objective audit。脚本最终返回非零退出码，方便 launchd 或手工执行识别当天没有收集到 paper session。

如果已经生成了 OpenD response JSONL，可以先抽成 Futu broker report 形状：

```bash
.venv/bin/python -m multi_layer_trading_lab.cli extract-futu-ticket-response-report \
  --response-path data/logs/opend_paper_ticket_responses.jsonl \
  --output-path data/logs/futu_order_report.from_responses.json
```

dry-run response 会保留 `dry_run=true`，只能用于接口演练；`paper-evidence` 会继续拒绝它作为真实 paper 证据。

## 4. 演练 dry-run evidence 形状

在真实 paper session 前，可以先把 dry-run tickets 转成 execution log 和模拟 broker report，用来验证 reconciliation / audit 文件接口：

```bash
.venv/bin/python -m multi_layer_trading_lab.cli build-opend-dry-run-evidence \
  --ticket-path data/logs/opend_paper_tickets.jsonl \
  --execution-log-path data/logs/execution_log.dry_run.jsonl \
  --broker-report-path data/logs/futu_order_report.dry_run.json
.venv/bin/python -m multi_layer_trading_lab.cli paper-evidence \
  --execution-log-path data/logs/execution_log.dry_run.jsonl \
  --broker-report-path data/logs/futu_order_report.dry_run.json \
  --paper-sessions 1
```

这一步只验证证据文件形状和对账流程。输出里保留 `dry_run=true`，`paper-evidence` 会标记 `dry_run_execution_log_not_real_paper` / `dry_run_broker_report_not_real_paper`，不能替代真实 OpenD paper session，也不能让 `paper_to_live` 通过。

## 5. 接入真实 paper 执行证据

完成 OpenD paper session 后，必须提供两个真实文件：

```text
data/logs/execution_log.jsonl
data/logs/futu_order_report.json
```

如果订单是通过 `/Users/yxin/AI_Workstation/futu-opend-execution` Web UI 执行，可以从 Web JSONL 日志抽取 broker report：

```bash
.venv/bin/python -m multi_layer_trading_lab.cli extract-futu-web-report \
  /Users/yxin/AI_Workstation/futu-opend-execution/logs/web_ui.jsonl \
  --output-path data/logs/futu_order_report.json
.venv/bin/python -m multi_layer_trading_lab.cli build-paper-execution-log \
  --ticket-path data/logs/opend_paper_tickets.jsonl \
  --broker-report-path data/logs/futu_order_report.json \
  --execution-log-path data/logs/execution_log.jsonl
```

OpenD ticket 的 `remark` 会写入当前项目生成的 `ticket_id`，抽取器会把 broker report 的 `local_order_id` 设为该 `remark`，用于和本地 execution log 对账。

如果已经有 ticket、OpenD response 或 broker report，可以用 bundle 命令串起后续证据：

```bash
.venv/bin/python -m multi_layer_trading_lab.cli build-paper-session-evidence-bundle \
  --ticket-path data/logs/opend_paper_tickets.jsonl \
  --response-path data/logs/opend_paper_ticket_responses.jsonl \
  --broker-report-path data/logs/futu_order_report.json \
  --execution-log-path data/logs/execution_log.jsonl \
  --profitability-evidence-path data/logs/profitability_evidence.json \
  --paper-sessions 20
```

如果已经有真实 broker report 而不是 response JSONL，可以传空 `--response-path ""`，直接从 broker report 生成 execution / paper / profitability evidence。

如果 execution log 里还有未平仓持仓，可以从 OpenD quote snapshot 生成 mark price JSON 后再跑 bundle：

```bash
.venv/bin/python -m multi_layer_trading_lab.cli build-mark-prices-from-opend-quote \
  --quote-snapshot-path data/logs/opend_quote_snapshot.json \
  --output-path data/logs/mark_prices.json
```

如果 `paper-blocker-report` 显示 OpenD kill switch 被打开，先生成操作员交接工件：

```bash
.venv/bin/python -m multi_layer_trading_lab.cli paper-operator-handoff \
  --paper-blocker-report-path data/logs/paper_blocker_report.json \
  --output-path data/logs/paper_operator_handoff.json
```

该命令只读 blocker report，不会清除 kill switch，也不会提交 paper/live 订单。输出里的
`order_submission_allowed=false` 和 `remediation_automation_allowed=false` 是硬约束；解除
kill switch 只能由操作员在明确授权后于自动化之外完成。

校验证据：

```bash
.venv/bin/python -m multi_layer_trading_lab.cli paper-evidence \
  --execution-log-path data/logs/execution_log.jsonl \
  --broker-report-path data/logs/futu_order_report.json \
  --paper-sessions 20
```

只有满足以下条件，paper evidence 才能支持后续 live gate：

- execution log 存在且非空
- Futu/OpenD broker report 存在且非空
- 本地订单和券商回报可对账
- 拒单率低于 gate 阈值
- 滑点不超过设定阈值
- paper session 数达到阈值
- live 前显式设置 `--manual-live-enable`

## 6. 带 paper 证据运行 daily ops

```bash
.venv/bin/python scripts/run_daily_ops.py \
  --lake-root data/lake \
  --report-path data/logs/ops_daily_report.md \
  --readiness-path data/logs/go_live_readiness.json \
  --ticket-path data/logs/opend_paper_tickets.jsonl \
  --response-path data/logs/opend_paper_ticket_responses.jsonl \
  --execution-log-path data/logs/execution_log.jsonl \
  --broker-report-path data/logs/futu_order_report.json \
  --opend-quote-snapshot-path data/logs/opend_quote_snapshot.json \
  --paper-operator-handoff-path data/logs/paper_operator_handoff.json \
  --market-holiday-dates YYYY-MM-DD,YYYY-MM-DD \
  --build-mark-prices-from-opend-quote \
  --mark-prices-path data/logs/mark_prices.json \
  --paper-sessions 20
```

`--market-holiday-dates` 用于显式传入港股非交易日，避免假期误触发 paper session 收集；`--build-mark-prices-from-opend-quote` 会在 evidence bundle 前把 OpenD quote snapshot 转成未平仓持仓估值所需的 mark price JSON，减少手工拼接。

即使 paper evidence 通过，live 仍需要显式增加：

```bash
--manual-live-enable
```

这是人工确认闸门，不应由定时任务默认开启。
