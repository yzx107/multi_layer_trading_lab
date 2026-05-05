# multi_layer_trading_lab

`multi_layer_trading_lab` 是一个面向本地研究机的港股 / 美股多层研究与执行框架骨架，服务于中低频、事件驱动、港美联动以及港股开盘到日内的微观结构研究与执行优化。

当前版本已经包含两条可运行链路：

1. `demo execution loop`
2. `research deepening workflow`

并补充了一条面向个人交易者真实约束的产品路线：

3. `personal trading operating blueprint`

其中 research 这条链是本轮重点，覆盖：

- contract validation
- feature registry
- dataset quality report
- symbol normalization
- horizon labeling / event outcomes
- Bayesian posterior attachment
- batch lead-lag / transfer entropy ranking
- HK `OrderAdd` event microstructure feature set
- `OrderAdd` pressure signal candidates
- markdown research summary report

## 与现有工作区的分工

这个 repo 不是重新造一个港股 L2 数据底座，也不是替代已有因子工厂。工作区内已有上游能力应直接复用：

- `/Users/yxin/AI_Workstation/Hshare_Lab_v2`: 港股 L2 raw / stage / DQA / verified layer 的事实源
- `/Users/yxin/AI_Workstation/hk_factor_autoresearch`: 港股因子 contract、research card、固定 harness、gate 与 registry
- `/Users/yxin/AI_Workstation/Ashare_Lab`: Tushare 连接与 smoke script 参考
- `/Users/yxin/AI_Workstation/futu-opend-execution`: Futu/OpenD execution safety model 与 paper/dry-run 执行参考

本项目的定位是下游组合层：消费 Hshare 的 verified 数据和因子工厂的晋级产物，在这里做 Bayesian / TE / Kelly 研究编排、百万级个人账户风控、OpenD dry-run / paper / live 审计和每日 ops readiness。

详见 [EXTERNAL_REUSE_MAP.md](/Users/yxin/AI_Workstation/Bayes_TE_Kelly Trading/EXTERNAL_REUSE_MAP.md)。

第一版的基础闭环仍然保留：

1. 用 `Tushare` 拉取 `security_master` 与日频 / 分钟级研究数据
2. 从本地港股 `L2 tick` 文件加载并聚合微观结构特征
3. 将数据统一落到 `Parquet + DuckDB`
4. 生成 `daily_features` 与 `intraday_l2_features`
5. 基于简单规则、Bayesian setup 或 lead-lag 候选关系生成 `signal_events`
6. 进入最小事件驱动 backtest
7. 通过 `dry-run / paper` broker adapter 输出拟下单与 `execution_log`

## 系统定位

这个 repo 不是交易所直连 `HFT` 系统，也不试图伪装为超低延迟撮合基础设施。它适合：

- 港股中低频与事件驱动
- 港美联动研究
- 港股开盘到日内的微观结构 alpha 研究
- 持有周期以 `intraday` 到 `1-5D` 为主
- 百万级个人账户在 `Mac mini + MacBook Air` 环境下做本地研究、paper execution 与审慎小规模 live 验证

它暂时不适合：

- 交易所直连低延迟 `HFT`
- 亚毫秒级 order book replay
- 复杂多账户 OMS/EMS
- 生产级高可用容灾与风控编排

## 数据源分工

### Tushare

用于研究底座与广覆盖面板数据：

- `A-share / HK / US` 基础证券信息
- 日线、分钟线、资金流、板块、事件
- 日频与分钟频横截面特征

### 港股 L2 Tick 历史数据

用于港股微观结构研究：

- order book / tick trade / opening auction 相关行为
- bid-ask imbalance、trade imbalance、order cancellation proxy
- 开盘前后 `1s / 5s / 30s / 1m` 聚合特征

### Futu OpenD / OpenAPI

用于港股实时行情与执行：

- 行情订阅
- order book / tick 订阅
- paper / dry-run / live adapter 抽象

### IBKR

用于美股与备份执行通道：

- 美股常规下单
- 另一条 paper / live 执行通道

## 当前 v1 支持与边界

### v1 已支持的设计目标

- 单 repo、清晰分层
- 本地优先的 `Parquet + DuckDB` 研究存储
- contract-first 的逻辑表定义
- adapter / feature / model / execution 分离
- 研究与执行解耦
- dry-run / paper / live 三态接口规划
- feature lineage、signal log、execution log 的观测设计
- 研究侧 `contract validation + quality report + feature registry`
- `symbol normalization + horizon labeling + event outcome extraction`
- `Bayesian posterior` 和 `lead-lag` 批量扫描
- `research_summary.md` 报告产出

### v1 预计只做最小实现的部分

- `transfer entropy` 先提供基础实现或 placeholder
- `Futu / IBKR` 先落接口与 dry-run demo，不强依赖真实凭证
- 风控先覆盖关键约束，不追求完整 OMS/EMS

## 目录说明

核心结构按功能与可扩展性拆分：

- `configs/`: 环境、存储、策略、broker、风控样例配置
- `data_contracts/`: 核心逻辑表 schema 与 lineage 文档
- `src/`: adapters、storage、features、models、signals、backtest、execution、risk
- `scripts/`: CLI 和运维脚本入口
- `tests/`: contract、feature、model、backtest、adapter smoke tests

## 从研究走到执行

建议流程如下：

1. 初始化环境变量与本地目录
2. 拉取 `security_master`、日线、分钟线样本
3. 导入一个交易日的港股 `L2` 样本并聚合
4. 生成 `daily_features`、`intraday_l2_features`
5. 运行一个 baseline signal pipeline
6. 进入 backtest 验证收益、换手、回撤、hit ratio
7. 使用统一 `BrokerAdapter` 切到 `dry-run` 或 `paper`
8. 将拟订单、成交、风控快照写入执行表

如果只做 research、暂不碰交易模块，现在也可以单独跑：

1. 生成 contract-aligned `security_master / daily_features / intraday_l2_features`
2. 做 `quality + validation`
3. 生成 `feature_registry`
4. 跑 `horizon labels / Bayesian posterior / lead-lag`
5. 输出 `research_summary.md`

## 配置与凭证

所有凭证都从环境变量读取，禁止硬编码：

- `TUSHARE_TOKEN`
- `FUTU_HOST`, `FUTU_PORT`
- `IBKR_HOST`, `IBKR_PORT`, `IBKR_CLIENT_ID`

参考 [.env.example](/Users/yxin/AI_Workstation/Bayes_TE_Kelly Trading/.env.example) 与 `configs/` 下的样例文件。

## 技术栈

- Python `3.11+`
- `pyproject.toml`
- `uv` 优先，兼容 `pip`
- `polars`, `duckdb`, `pyarrow`
- `typer`
- `pydantic-settings`
- `pytest`
- `structlog`
- `ruff`, `black`, `pre-commit`

## 开发原则

- 先立 `contract`，再写 storage / feature / backtest / execution
- 不过度工程化，但保留清晰扩展边界
- 不把 tick 原始流直接当最终信号输入，必须经聚合特征层
- `lead-lag / transfer entropy` 只用于发现候选信息流，不是因果证明器
- `Bayesian update` 用于动态更新 setup 胜率
- `Kelly` 只作为仓位上限与风险预算，不是机械下单器

## 配套文档

- [ARCHITECTURE.md](/Users/yxin/AI_Workstation/Bayes_TE_Kelly Trading/ARCHITECTURE.md)
- [ROADMAP.md](/Users/yxin/AI_Workstation/Bayes_TE_Kelly Trading/ROADMAP.md)
- [TASKS.md](/Users/yxin/AI_Workstation/Bayes_TE_Kelly Trading/TASKS.md)
- [CHANGELOG.md](/Users/yxin/AI_Workstation/Bayes_TE_Kelly Trading/CHANGELOG.md)
- [data_contracts/README.md](/Users/yxin/AI_Workstation/Bayes_TE_Kelly Trading/data_contracts/README.md)
- [PERSONAL_TRADING_BLUEPRINT.md](/Users/yxin/AI_Workstation/Bayes_TE_Kelly Trading/PERSONAL_TRADING_BLUEPRINT.md)

## 快速运行

安装依赖：

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -e '.[dev]'
```

运行 research demo：

```bash
.venv/bin/python -m multi_layer_trading_lab.cli research-demo
.venv/bin/python -m multi_layer_trading_lab.cli validate-research
```

拉取 Tushare 数据到 lake：

```bash
.venv/bin/python -m multi_layer_trading_lab.cli fetch-tushare-to-lake --lake-root data/lake
```

默认不会因为存在 token 就自动联网；真实 Tushare Pro 路径需要显式打开：

```bash
.venv/bin/python -m multi_layer_trading_lab.cli fetch-tushare-to-lake \
  --lake-root data/lake \
  --symbols 600519.SH,000001.SZ \
  --use-real
```

当前真实 Tushare 路径覆盖 A 股 `stock_basic`、`daily` 和 `trade_cal`，用于 A 股参考数据与日频研究输入；港股 L2 事实源仍复用 Hshare Lab。

拉取 iFind 事件数据到 lake：

```bash
.venv/bin/python -m multi_layer_trading_lab.cli fetch-ifind-to-lake --lake-root data/lake
```

真实 iFind HTTP 事件路径可以直接走官方 `report_query` 公告接口；如有自建事件服务，也可以显式配置 endpoint：

```bash
.venv/bin/python -m multi_layer_trading_lab.cli fetch-ifind-to-lake \
  --lake-root data/lake \
  --symbols 00700.HK \
  --use-real
```

当前 iFind 真实路径默认调用同花顺官方 `https://quantapi.51ifind.com/api/v1/report_query`，把公告查询结果标准化为 `ifind_events`；如果配置 `IFIND_EVENTS_ENDPOINT`，则改用自定义 HTTP 事件服务。无论哪种 HTTP 路径，都需要跑真实 smoke 并让 lake 出现 `ifind_real` 行后才能进入 go-live 证据链。

如果暂时没有可调用的 iFind HTTP endpoint，可以先从 iFind 终端导出事件 CSV/JSON，再导入 lake；这会标记为 `ifind_real_file`，用于本地研究和 go-live 审计中的真实文件 adapter 路径：

```bash
.venv/bin/python -m multi_layer_trading_lab.cli write-ifind-events-template \
  --output-path data/templates/ifind_events_template.csv
.venv/bin/python -m multi_layer_trading_lab.cli validate-ifind-events-file \
  /path/to/ifind_events.csv \
  --source-run-id manual-ifind-export-20260503 \
  --output-path data/logs/ifind_events_validation.json
.venv/bin/python -m multi_layer_trading_lab.cli import-ifind-events-file \
  /path/to/ifind_events.csv \
  --lake-root data/lake \
  --source-run-id manual-ifind-export-20260503
.venv/bin/python -m multi_layer_trading_lab.cli data-adapter-status \
  --lake-root data/lake \
  --ifind-adapter-status real_file_adapter
```

`import-ifind-events-file` 默认不会覆盖已存在的 `ifind_events` dataset；确认要替换时才显式加 `--overwrite`。

查看个人账户风控预检：

```bash
.venv/bin/python -m multi_layer_trading_lab.cli risk-precheck --account-equity 1000000
```

检查当前项目是否能复用 Hshare Lab 和因子工厂：

```bash
.venv/bin/python -m multi_layer_trading_lab.cli external-repo-precheck
.venv/bin/python -m multi_layer_trading_lab.cli research-input-manifest \
  --output-path data/logs/research_input_manifest.json
.venv/bin/python -m multi_layer_trading_lab.cli factor-factory-summary --lake-root data/lake
```

查看 Futu OpenD 执行前检查：

```bash
.venv/bin/python -m multi_layer_trading_lab.cli opend-precheck --mode paper --env SIMULATE
.venv/bin/python -m multi_layer_trading_lab.cli reconcile-futu-report data/logs/execution_log.jsonl /path/to/futu_order_report.json
.venv/bin/python -m multi_layer_trading_lab.cli paper-audit data/logs/execution_log.jsonl /path/to/futu_order_report.json --paper-sessions 20
.venv/bin/python -m multi_layer_trading_lab.cli paper-evidence \
  --execution-log-path data/logs/execution_log.jsonl \
  --broker-report-path data/logs/futu_order_report.json \
  --paper-sessions 20
.venv/bin/python -m multi_layer_trading_lab.cli fetch-opend-quote-snapshot 00001.HK \
  --base-url http://127.0.0.1:8766 \
  --output-path data/logs/opend_quote_snapshot.json
.venv/bin/python -m multi_layer_trading_lab.cli export-opend-paper-tickets \
  --plan-path data/logs/paper_session_plan.json \
  --output-path data/logs/opend_paper_tickets.jsonl \
  --quote-snapshot-path data/logs/opend_quote_snapshot.json
.venv/bin/python -m multi_layer_trading_lab.cli submit-opend-paper-tickets \
  --ticket-path data/logs/opend_paper_tickets.jsonl \
  --output-path data/logs/opend_paper_ticket_responses.jsonl \
  --base-url http://127.0.0.1:8766 \
  --submit-paper-simulate
.venv/bin/python -m multi_layer_trading_lab.cli extract-futu-ticket-response-report \
  --response-path data/logs/opend_paper_ticket_responses.jsonl \
  --output-path data/logs/futu_order_report.from_responses.json
.venv/bin/python -m multi_layer_trading_lab.cli paper-simulate-status \
  --response-path data/logs/opend_paper_ticket_responses.jsonl \
  --output-path data/logs/paper_simulate_status.json
.venv/bin/python -m multi_layer_trading_lab.cli build-paper-session-evidence-bundle \
  --ticket-path data/logs/opend_paper_tickets.jsonl \
  --response-path data/logs/opend_paper_ticket_responses.jsonl \
  --broker-report-path data/logs/futu_order_report.json \
  --execution-log-path data/logs/execution_log.jsonl \
  --profitability-evidence-path data/logs/profitability_evidence.json \
  --paper-sessions 20
.venv/bin/python -m multi_layer_trading_lab.cli build-opend-dry-run-evidence \
  --ticket-path data/logs/opend_paper_tickets.jsonl \
  --execution-log-path data/logs/execution_log.dry_run.jsonl \
  --broker-report-path data/logs/futu_order_report.dry_run.json
.venv/bin/python -m multi_layer_trading_lab.cli extract-futu-web-report \
  /Users/yxin/AI_Workstation/futu-opend-execution/logs/web_ui.jsonl \
  --output-path data/logs/futu_order_report.json
.venv/bin/python -m multi_layer_trading_lab.cli build-paper-execution-log \
  --ticket-path data/logs/opend_paper_tickets.jsonl \
  --broker-report-path data/logs/futu_order_report.json \
  --execution-log-path data/logs/execution_log.jsonl
```

`paper-session-plan` 默认会阻断低于一手的订单，并在 `lot_sizing` 里写入原因和建议动作。人工确认一手金额仍在单票/策略预算内后，可用 `--allow-lot-round-up` 生成显式一手放大的 paper plan，再导出 OpenD dry-run ticket。

查看真实数据源凭证检查：

```bash
.venv/bin/python -m multi_layer_trading_lab.cli ifind-token-status
.venv/bin/python -m multi_layer_trading_lab.cli ifind-refresh-access-token-smoke \
  --output-path data/logs/ifind_access_token_refresh_status.json
.venv/bin/python -m multi_layer_trading_lab.cli ifind-ingestion-status \
  --lake-root data/lake \
  --output-path data/logs/ifind_ingestion_status.json
.venv/bin/python -m multi_layer_trading_lab.cli ifind-connection-plan \
  --lake-root data/lake \
  --output-path data/logs/ifind_connection_plan.json
.venv/bin/python -m multi_layer_trading_lab.cli data-source-precheck
.venv/bin/python -m multi_layer_trading_lab.cli data-adapter-status
```

`ifind-token-status` 只输出 access/refresh token 是否存在和 refresh token 过期时间，不打印 token 原文。`ifind-refresh-access-token-smoke` 调用同花顺官方 `get_access_token` HTTP 接口，只记录是否换到 access token 和过期时间，不把 token 写入报告。`ifind-ingestion-status` 会把官方 report_query / 自定义 endpoint 配置状态、lake 中 `ifind_real` / `ifind_real_file` / `ifind_stub` 行数和下一步动作写成 JSON。`ifind-connection-plan` 进一步生成不含密钥的 smoke 准备报告，区分“可以跑 HTTP 真实拉取”和“已经有真实文件行”。`data-source-precheck` 只说明凭证是否存在；`data-adapter-status` 额外说明当前是否已经是真实 adapter。当前 Tushare 已有真实 adapter 路径，iFind 在跑通官方 HTTP 拉取或导入真实终端导出文件前仍不会解锁 go-live。

生成本地运行日报：

```bash
.venv/bin/python -m multi_layer_trading_lab.cli ops-report --output-path data/logs/ops_daily_report.md
```

生成机器可读的上线总闸门 manifest：

```bash
.venv/bin/python -m multi_layer_trading_lab.cli go-live-readiness \
  --output-path data/logs/go_live_readiness.json \
  --lake-root data/lake \
  --opend-mode paper \
  --opend-env SIMULATE
```

该命令会汇总账户风险预算、Tushare/iFind 凭证、核心 lake 数据新鲜度、research->paper、paper->live 与 Futu OpenD 状态。默认缺少真实证据时输出 `go_live_approved=false`。

从真实 paper/live execution log 与 Futu broker report 生成盈利证据：

```bash
.venv/bin/python -m multi_layer_trading_lab.cli profitability-evidence \
  --execution-log-path data/logs/execution_log.jsonl \
  --broker-report-path data/logs/futu_order_report.json \
  --output-path data/logs/profitability_evidence.json \
  --paper-sessions 20
```

在正式生成盈利证据前，先用真实日志和 broker report 推导 paper session 数，避免只靠手填 `--paper-sessions`：

```bash
.venv/bin/python -m multi_layer_trading_lab.cli paper-session-ledger \
  --execution-log-path data/logs/execution_log.jsonl \
  --broker-report-path data/logs/futu_order_report.json \
  --output-path data/logs/paper_session_ledger.json
```

该命令会拒绝 dry-run 行，并要求从日志日期推导出至少 20 个 session，作为进入 `profitability-evidence` 前的证据预检。

如果还有未平仓持仓，需要额外提供 mark price JSON：

```bash
.venv/bin/python -m multi_layer_trading_lab.cli build-mark-prices-from-opend-quote \
  --quote-snapshot-path data/logs/opend_quote_snapshot.json \
  --output-path data/logs/mark_prices.json
.venv/bin/python -m multi_layer_trading_lab.cli profitability-evidence \
  --execution-log-path data/logs/execution_log.jsonl \
  --broker-report-path data/logs/futu_order_report.json \
  --output-path data/logs/profitability_evidence.json \
  --paper-sessions 20 \
  --mark-prices-path data/logs/mark_prices.json
```

把“百万级个人账户、港股 L2/Tushare/iFind/OpenD、可赚钱”这个总目标映射到机器可读完成度审计：

```bash
.venv/bin/python -m multi_layer_trading_lab.cli objective-audit \
  --readiness-manifest-path data/logs/go_live_readiness.json \
  --output-path data/logs/objective_audit.json \
  --profitability-evidence-path data/logs/profitability_evidence.json \
  --ifind-validation-report-path data/logs/ifind_events_validation.json \
  --ifind-ingestion-status-path data/logs/ifind_ingestion_status.json \
  --opend-quote-snapshot-path data/logs/opend_quote_snapshot.json \
  --opend-ticket-response-path data/logs/opend_paper_ticket_responses.jsonl
```

这个命令不会用测试绿灯代替真实交易证据；缺少真实 OpenD quote/response、真实 paper/live 正收益、broker 对账、真实 adapter 或上线门禁时会保持 `objective_achieved=false`。

Mac mini 日常运行入口：

```bash
.venv/bin/python scripts/run_daily_ops.py \
  --lake-root data/lake \
  --report-path data/logs/ops_daily_report.md \
  --readiness-path data/logs/go_live_readiness.json
```

日常运行会在 readiness 后自动生成：

- `data/logs/objective_audit.json`: 机器可读目标完成审计
- `data/logs/objective_audit.md`: 可读版目标缺口报告
- `data/logs/ifind_ingestion_status.json`: iFind endpoint/token/lake 真实行数诊断
- `data/logs/ifind_connection_plan.json`: 不含密钥的 iFind HTTP/file 模式下一步计划

daily ops 的 `paper-session-plan` 会默认消费 `data/logs/opend_quote_snapshot.json`。如果 quote snapshot 存在，会按真实 `lot_size` 和参考价格执行一手约束；低于一手的计划会阻断并提示人工确认是否放大到一手。

人工确认一手金额仍在单票/策略预算内后，daily ops 可以显式打开：

```bash
.venv/bin/python scripts/run_daily_ops.py \
  --lake-root data/lake \
  --allow-lot-round-up \
  --export-opend-ticket-path data/logs/opend_paper_tickets.daily_lot_round_up.jsonl
```

`--export-opend-ticket-path` 只导出 `dry_run=true` / `real=false` / `submit_real=false` 的 OpenD ticket，供提交前人工审阅；提交仍需要单独运行 `submit-opend-paper-tickets`。默认提交仍是 Web UI dry-run；只有显式追加 `--submit-paper-simulate` 时才会向 `/api/normal/order` 发送 `paper=true`，走 `/Users/yxin/AI_Workstation/futu-opend-execution` 的 Futu `TrdEnv.SIMULATE` 路径。该路径不是 live 交易，live 仍受 real 开关、确认短语和解锁门禁保护。

确认 `futu-opend-execution` Web UI 已在本机运行后，也可以显式追加 dry-run 提交：

```bash
.venv/bin/python scripts/run_daily_ops.py \
  --lake-root data/lake \
  --allow-lot-round-up \
  --export-opend-ticket-path data/logs/opend_paper_tickets.daily_lot_round_up.jsonl \
  --opend-ticket-response-path data/logs/opend_paper_ticket_responses.daily_lot_round_up.jsonl \
  --submit-opend-dry-run-tickets \
  --submit-opend-max-attempts 5 \
  --submit-opend-retry-delay-seconds 0.5
```

该开关只调用 `submit-opend-paper-tickets` 的 dry-run 路径，响应中应保持 `submitted=false`。提交命令默认会重试 3 次；如果 Web UI 刚启动，可以用 `--submit-opend-max-attempts` 和 `--submit-opend-retry-delay-seconds` 调整。

确认 paper SIMULATE 环境可用、且 ticket 已人工审阅后，可以显式提交到 Futu 模拟交易环境：

```bash
.venv/bin/python scripts/run_daily_ops.py \
  --lake-root data/lake \
  --allow-lot-round-up \
  --export-opend-ticket-path data/logs/opend_paper_tickets.daily_lot_round_up.jsonl \
  --opend-ticket-response-path data/logs/opend_paper_ticket_responses.paper_simulate.jsonl \
  --submit-opend-paper-simulate-tickets \
  --submit-opend-max-attempts 5 \
  --submit-opend-retry-delay-seconds 0.5
```

提交后先跑 `paper-simulate-status`。只有 `paper_rows > 0`、`dry_run_rows = 0`、`submitted_rows > 0` 且 `ready_for_session_collection=true`，这些响应才可以进入 20-session paper 证据收集；历史 dry-run response 不能用来冒充 paper SIMULATE。

如果要让 daily ops 在同一轮里完成“导出 ticket -> 提交 SIMULATE -> 验真 -> 生成当日 broker report / execution log / profitability evidence”，显式提供当日输出路径：

```bash
.venv/bin/python scripts/run_daily_ops.py \
  --lake-root data/lake \
  --allow-lot-round-up \
  --opend-account-status-path data/logs/opend_account_status.paper_simulate_YYYYMMDD.json \
  --export-opend-ticket-path data/logs/opend_paper_tickets.paper_simulate_YYYYMMDD.jsonl \
  --opend-ticket-response-path data/logs/opend_paper_ticket_responses.paper_simulate_YYYYMMDD.jsonl \
  --paper-simulate-status-path data/logs/paper_simulate_status.paper_simulate_YYYYMMDD.json \
  --submit-opend-paper-simulate-tickets \
  --require-paper-session-calendar-collect \
  --execution-log-path data/logs/execution_log.paper_simulate_YYYYMMDD.jsonl \
  --broker-report-path data/logs/futu_order_report.paper_simulate_YYYYMMDD.json \
  --profitability-evidence-path data/logs/profitability_evidence.paper_simulate_YYYYMMDD.json \
  --paper-session-calendar-path data/logs/paper_session_calendar.paper_simulate_YYYYMMDD.json \
  --paper-progress-path data/logs/paper_progress.paper_simulate_YYYYMMDD.json \
  --paper-sessions 1 \
  --build-mark-prices-from-opend-quote \
  --mark-prices-path data/logs/mark_prices.paper_simulate_YYYYMMDD.json
```

这条路径仍不会触碰 live：ticket 保持 `real=false` / `submit_real=false`，提交时只加 `paper=true`。它适合生成单日 paper 证据；晋级仍必须用合并后的 20-session evidence。
启用 `--submit-opend-paper-simulate-tickets` 时，daily ops 会先调用 `fetch-opend-account-status` 检查 `/api/accounts`；如果没有 HK 股票模拟账户，会在提交前阻断。

`submit-opend-paper-tickets` 默认带幂等保护：如果 output response JSONL 已经存在同一个 `ticket_id`，会在本地返回 `ticket_already_submitted:<ticket_id>`，不会再次调用 OpenD。只有确知要重提同一张 ticket 时才显式加 `--allow-resubmit`。daily ops 的 paper SIMULATE 路径默认只允许重试已经失败的 response（`--allow-failed-resubmit`），例如 kill-switch 拒单后清理 kill-switch 再跑；已经有成功提交 response 的 ticket 仍会被阻断。

每天收盘后，把当天非 dry-run 的 paper execution log 和 Futu broker report 累积到合并证据文件，再跑 session ledger / profitability gate：

```bash
.venv/bin/python -m multi_layer_trading_lab.cli combine-paper-evidence \
  data/logs/execution_log.paper_simulate_YYYYMMDD.jsonl,data/logs/execution_log.paper_simulate_YYYYMMDD_NEXT.jsonl \
  data/logs/futu_order_report.paper_simulate_YYYYMMDD.json,data/logs/futu_order_report.paper_simulate_YYYYMMDD_NEXT.json \
  --output-execution-log-path data/logs/execution_log.paper_combined.jsonl \
  --output-broker-report-path data/logs/futu_order_report.paper_combined.json
.venv/bin/python -m multi_layer_trading_lab.cli paper-session-ledger \
  --execution-log-path data/logs/execution_log.paper_combined.jsonl \
  --broker-report-path data/logs/futu_order_report.paper_combined.json \
  --output-path data/logs/paper_session_ledger.paper_combined.json
.venv/bin/python -m multi_layer_trading_lab.cli paper-session-calendar \
  --execution-log-path data/logs/execution_log.paper_combined.jsonl \
  --broker-report-path data/logs/futu_order_report.paper_combined.json \
  --output-path data/logs/paper_session_calendar.paper_combined.json
.venv/bin/python -m multi_layer_trading_lab.cli profitability-evidence \
  --execution-log-path data/logs/execution_log.paper_combined.jsonl \
  --broker-report-path data/logs/futu_order_report.paper_combined.json \
  --output-path data/logs/profitability_evidence.paper_combined.json \
  --paper-sessions 20 \
  --mark-prices-path data/logs/mark_prices.json
.venv/bin/python -m multi_layer_trading_lab.cli paper-progress \
  --execution-log-path data/logs/execution_log.paper_combined.jsonl \
  --broker-report-path data/logs/futu_order_report.paper_combined.json \
  --profitability-evidence-path data/logs/profitability_evidence.paper_combined.json \
  --output-path data/logs/paper_progress.paper_combined.json
```

`combine-paper-evidence` 会拒绝 dry-run 行、空文件和重复 order id；只有合并后 `paper-session-ledger` 推导出至少 20 个真实 session，`profitability-evidence` 才可能进入正收益对账判断。
`paper-session-calendar` 会根据合并日志判断今天是否已有 broker-backed session，并输出 `collect_today_paper_session`、`wait_next_trade_date` 或 `target_complete`，避免同一天重复累计。
`paper-progress` 是每日收口视图，会直接输出还差多少 session、当前 net PnL、最大回撤、是否 broker reconciled，以及是否可以进入 live review。

带真实 iFind 终端导出文件运行时，daily ops 会先预检并写出验收报告，再导入 lake，随后 readiness/objective audit 会消费 `ifind_events`：

```bash
.venv/bin/python scripts/run_daily_ops.py \
  --lake-root data/lake \
  --ifind-events-file-path /path/to/ifind_events.csv \
  --ifind-validation-report-path data/logs/ifind_events_validation.json \
  --ifind-source-run-id manual-ifind-export-20260503
```

带真实 paper 执行证据运行：

```bash
.venv/bin/python scripts/run_daily_ops.py \
  --lake-root data/lake \
  --report-path data/logs/ops_daily_report.md \
  --readiness-path data/logs/go_live_readiness.json \
  --execution-log-path data/logs/execution_log.jsonl \
  --broker-report-path data/logs/futu_order_report.json \
  --paper-sessions 20
```

如果同时提供 OpenD ticket、response 和 mark price，daily ops 会自动串联生成 execution log、paper evidence 和 profitability evidence，再进入目标审计。也可以打开 `--build-mark-prices-from-opend-quote`，直接从 OpenD quote snapshot 生成 `mark_prices.json`：

```bash
.venv/bin/python scripts/run_daily_ops.py \
  --lake-root data/lake \
  --ticket-path data/logs/opend_paper_tickets.jsonl \
  --response-path data/logs/opend_paper_ticket_responses.jsonl \
  --broker-report-path data/logs/futu_order_report.json \
  --execution-log-path data/logs/execution_log.jsonl \
  --profitability-evidence-path data/logs/profitability_evidence.json \
  --opend-quote-snapshot-path data/logs/opend_quote_snapshot.json \
  --build-mark-prices-from-opend-quote \
  --mark-prices-path data/logs/mark_prices.json \
  --paper-sessions 20
```

paper 执行流程见 [PAPER_TRADING_RUNBOOK.md](/Users/yxin/AI_Workstation/Bayes_TE_Kelly Trading/PAPER_TRADING_RUNBOOK.md)。

生成 macOS `launchd` 定时任务配置：

```bash
.venv/bin/python scripts/render_launchd_plist.py \
  --output-path configs/launchd/com.yxin.mttl.daily-ops.plist \
  --hour 18 \
  --minute 30
```

生成后可先检查 plist，再按需复制到 `~/Library/LaunchAgents/` 并用 `launchctl` 加载。默认只生成配置文件，不直接安装到系统目录。
生成的 launchd 配置只跑 daily ops 状态、readiness、objective audit 和 paper progress；默认不包含 `--submit-opend-paper-simulate-tickets` 或 `--submit-opend-dry-run-tickets`，不会自动提交 OpenD 订单。

检查 research 是否允许进入 paper：

```bash
.venv/bin/python -m multi_layer_trading_lab.cli research-audit --lake-root data/lake
```

校准真实港股 L2 样本字段：

```bash
.venv/bin/python -m multi_layer_trading_lab.cli profile-l2-sample /path/to/l2_sample.parquet
.venv/bin/python -m multi_layer_trading_lab.cli profile-l2-sample /path/to/l2_sample.parquet --mapping-output configs/l2_mapping.json
.venv/bin/python -m multi_layer_trading_lab.cli import-l2-sample /path/to/l2_sample.parquet configs/l2_mapping.json --lake-root data/lake
.venv/bin/python -m multi_layer_trading_lab.cli build-l2-features-from-lake --lake-root data/lake
```

查看并导入真实港股 Tick zip 中的 `OrderAdd` 事件：

```bash
.venv/bin/python -m multi_layer_trading_lab.cli profile-l2-zip /Volumes/Data/港股Tick数据/2025/20250123.zip --category OrderAdd
.venv/bin/python -m multi_layer_trading_lab.cli import-l2-zip-order-add /Volumes/Data/港股Tick数据/2025/20250123.zip \
  --member 20250123/OrderAdd/00001.csv \
  --lake-root data/lake
.venv/bin/python -m multi_layer_trading_lab.cli build-l2-order-add-features-from-lake --lake-root data/lake
.venv/bin/python -m multi_layer_trading_lab.cli build-order-add-signals-from-lake --lake-root data/lake
.venv/bin/python -m multi_layer_trading_lab.cli backtest-order-add-signals-from-lake --lake-root data/lake
.venv/bin/python -m multi_layer_trading_lab.cli sweep-order-add-thresholds-from-lake --lake-root data/lake --planned-notional 8000
.venv/bin/python -m multi_layer_trading_lab.cli order-add-research-gate --lake-root data/lake --account-equity 1000000
```

`OrderAdd` 是订单簿事件，不是一档 bid/ask 快照；当前会先标准化写入 `raw_l2_order_add`，再聚合为 `l2_order_add_features`，用于新增委托强度、量、价格层级与大单比例研究。
`sweep-order-add-thresholds-from-lake` 用 `planned-notional` 表示实际计划下单名义金额；`order-add-research-gate` 会同时检查最佳阈值组合的交易数、成本后收益和个人账户容量约束。

多日批量导入可以用逗号分隔 zip 路径：

```bash
.venv/bin/python -m multi_layer_trading_lab.cli import-l2-zip-order-add-batch \
  /Volumes/Data/港股Tick数据/2025/20250123.zip,/Volumes/Data/港股Tick数据/2025/20250124.zip \
  --symbols 00001.HK \
  --lake-root data/lake
```

也可以按日期区间自动发现 zip：

```bash
.venv/bin/python -m multi_layer_trading_lab.cli discover-l2-zips \
  --raw-root /Volumes/Data/港股Tick数据 \
  --start 2025-01-20 \
  --end 2025-01-24
.venv/bin/python -m multi_layer_trading_lab.cli check-l2-order-add-coverage \
  --raw-root /Volumes/Data/港股Tick数据 \
  --start 2025-01-20 \
  --end 2025-01-24 \
  --symbols 00001.HK,00005.HK,00700.HK \
  --output-path data/logs/order_add_coverage.csv
.venv/bin/python -m multi_layer_trading_lab.cli import-l2-zip-order-add-range \
  --raw-root /Volumes/Data/港股Tick数据 \
  --start 2025-01-20 \
  --end 2025-01-24 \
  --symbols 00001.HK \
  --lake-root data/lake
```

或直接运行脚本：

```bash
.venv/bin/python scripts/run_research_demo.py
```

运行测试：

```bash
.venv/bin/python -m pytest
```
