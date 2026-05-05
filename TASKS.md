# Tasks

## 当前阶段

阶段目标：在基础闭环之上，把非交易模块推进到一个更像样的 research 版本，优先完成 `contract > quality > features > labels > research workflow`。

## In Progress

- [x] 初始化顶层文档骨架
- [x] 定义核心 data contracts
- [x] 提供环境变量模板与配置样例
- [x] 建立 `pyproject.toml` 与开发工具链
- [x] 落地 storage 层目录约定与 DuckDB helper
- [x] 实现 `Tushare adapter` 最小可用接口
- [x] 实现 `L2 loader` 与时间桶聚合
- [x] 实现 daily / intraday / l2 feature factories
- [x] 实现 Bayesian / Kelly / lead-lag 基础模块
- [x] 实现最小事件驱动 backtest
- [x] 实现 dry-run broker adapter
- [x] 补齐 CLI 与 demo pipeline
- [x] 补齐 contract / feature / backtest tests
- [x] 为核心 research 表补充代码级 contract validator
- [x] 增加 `feature_registry` 与 dataset quality report
- [x] 增加 `symbol normalization`、`horizon labeling`、`event outcomes`
- [x] 增加非交易版 `research workflow` 与 markdown report
- [x] 增加个人交易者版系统蓝图、风险画像配置与代码级风险参数转换
- [x] 增加 research -> paper -> live 的 promotion gate verifier
- [x] 增加基于 `PersonalAccountProfile` 的 CLI 风控预检输出
- [x] 增加 `iFind` staging adapter 与事件/基本面 enrichment contract
- [x] 增加港股 L2 vendor schema mapping 与标准化层
- [x] 增加 Futu OpenD paper/live readiness checker 与 CLI 预检
- [x] 增加本地运行日报，汇总风控预算、OpenD 状态、晋级门槛与数据新鲜度占位
- [x] 把日报的数据新鲜度接到真实 Parquet lake 检查
- [x] 增加 Tushare / iFind 凭证 readiness checker 与 CLI 预检
- [x] 增加 research promotion audit，把样本数、交易日期、泄漏/成本/容量审计汇总成 paper 晋级证据
- [x] 增加 research-audit CLI，从 lake 的 `signal_events` 生成 paper 晋级判断
- [x] 增加交易成本与个人账户容量审计模块，支持 paper 晋级前的成本/容量硬门槛
- [x] 增加真实港股 L2 样本 profiling CLI，用于推断 vendor 字段到 `L2ColumnMapping`
- [x] 增加 L2 mapping JSON 模板导出/加载，支持真实样本校准后复用
- [x] 增加 import-l2-sample CLI，用 mapping JSON 标准化真实 L2 样本并写入 Parquet lake
- [x] 增加 build-l2-features-from-lake CLI，把 `raw_l2_ticks` 聚合成 `intraday_l2_features`
- [x] 增加真实港股 Tick zip 的 `OrderAdd` profiling 与导入路径，标准化写入 `raw_l2_order_add`
- [x] 增加 `raw_l2_order_add` 到 `l2_order_add_features` 的订单事件微观结构特征构建
- [x] 把 `l2_order_add_features` 接入 research workflow、feature registry 与 lead-lag 候选排序
- [x] 增加 `order_add_pressure` 候选信号筛选，并合并进标准 `signal_events`
- [x] 增加真实多日 `OrderAdd` 批量导入 CLI 与 lake 侧候选信号生成 CLI
- [x] 增加 `order_add_signal_candidates` 快速回测 CLI，输出成本后收益摘要
- [x] 增加 `OrderAdd` 阈值扫描 CLI，用成本后收益排序并淘汰负收益参数组合
- [x] 增加 `OrderAdd` research gate，把最佳 sweep 组合的交易数和成本后收益接入 paper 阻断逻辑
- [x] 把个人账户容量约束接入 `OrderAdd` research gate，阻断超过百万级账户限额的参数组合
- [x] 将 `OrderAdd` 信号强度阈值与实际计划下单名义金额拆分，支持容量合规 sizing 评估
- [x] 增加按日期区间自动发现 Tick zip 并导入 `OrderAdd` 的 CLI，支持扩大真实样本窗口
- [x] 增加 `OrderAdd` 多日多标的 coverage 检查 CLI，导入前识别缺失 member
- [x] 增加 fetch-tushare-to-lake CLI，默认要求 token，显式 `--allow-stub` 才允许 demo 数据落盘
- [x] 增加 fetch-ifind-to-lake CLI，默认要求账号密码，显式 `--allow-stub` 才允许 demo 事件数据落盘
- [x] 将 iFind HTTP endpoint fetch 标记为真实 adapter 路径，补齐 endpoint 成功/失败 smoke 测试与 fail-closed 行为
- [x] 增加 execution reconciliation 模块，对比本地 execution log 与 broker 回报
- [x] 增加 Futu/OpenD 回报转换器，把 broker 回报标准化为 `BrokerExecutionReport`
- [x] 增加 reconcile-futu-report CLI，读取本地 execution log 与 Futu 回报文件并输出对账 breaks
- [x] 增加 paper promotion audit，把 paper sessions、拒单率、对账和滑点汇总成 live 晋级证据
- [x] 增加 paper-audit CLI，基于 execution log 与 Futu 回报输出是否允许进入 live
- [x] 增加 go-live-readiness CLI，生成机器可读上线总闸门 JSON manifest
- [x] 增加 Mac mini 日常运行脚本，串联本地日报与上线总闸门 manifest
- [x] 增加 macOS launchd plist 生成器，支持把日常运行脚本纳入本机定时任务
- [x] 增加 Hshare Lab v2 / hk_factor_autoresearch 外部复用边界、配置样例、precheck CLI 和 research input manifest，避免重复造 L2 清洗与因子工厂
- [x] 增加因子工厂 Gate B / family registry 只读汇总，落到当前 lake 作为组合研究候选输入
- [x] 增加外部组合 research signals、Hshare verified 覆盖审计、lookahead lineage 审计，让 research_to_paper 基于真实外部证据链通过
- [x] 增加 paper session plan、paper evidence 校验器和 daily ops paper 证据参数面，live gate 默认保持人工阻断
- [x] 增加 lot-aware paper session plan，基于 OpenD quote snapshot 提前阻断目标金额低于一手的 paper order
- [x] 增加 OpenD paper dry-run ticket 导出器，把 paper plan 转成 `futu-opend-execution` 可审阅 payload，并对缺标的/价格/lot size fail closed
- [x] 增加 OpenD quote snapshot 抓取 CLI，对接 `futu-opend-execution` `/api/quote` 并校验 symbol / lot_size / price
- [x] 增加 OpenD dry-run evidence builder，把 ticket JSONL 转成 execution log 和模拟 broker report，用于演练 paper audit 文件接口
- [x] 增加 OpenD paper ticket submitter，逐条 POST 到 `futu-opend-execution` `/api/normal/order`，只允许 dry-run ticket 并记录响应 JSONL
- [x] 增加 OpenD ticket response -> Futu broker report 抽取器，保留 dry-run 标记防止误入真实 paper evidence
- [x] 增加 Futu/OpenD Web JSONL broker report 抽取器，用 `remark`/ticket_id 回收到 paper 对账链
- [x] 增加真实 Futu broker report + ticket 生成本地 execution log 的 builder，拒绝 dry-run broker report
- [x] 增加 objective-audit CLI，把总目标映射到数据源、OpenD、百万级风控、paper/live 与正收益证据的完成度清单
- [x] 收紧 objective-audit：OpenD 不再只看配置 readiness，必须提供真实 quote snapshot 和 ticket response 运行证据
- [x] 扩展 objective-audit 输出 success criteria 和 prompt-to-artifact checklist，防止用测试绿灯替代真实目标完成审计
- [x] 增加 objective-audit-report CLI，把机器审计 JSON 渲染为每日可读 Markdown 缺口报告
- [x] 将 objective-audit 和 objective-audit-report 接入 Mac mini daily ops 链路
- [x] 增加 profitability-evidence CLI，从真实 execution log / Futu broker report 生成可对账 PnL 证据，并阻断 dry-run 或缺 mark price 的未平仓持仓
- [x] 增加 OpenD quote snapshot -> mark price JSON 命令，减少未平仓盈利证据手工拼接
- [x] 增加 paper-session evidence bundle CLI，串联 response/broker report、execution log、paper evidence 和 profitability evidence
- [x] 将 paper-session evidence bundle 接入 daily ops，提供真实 ticket/broker/mark 后自动生成盈利证据
- [x] 将 OpenD quote -> mark price 接入 daily ops，打开开关后在 bundle 前自动生成 `mark_prices.json`
- [x] 增加 iFind 事件导出 CSV 模板生成命令，降低真实 `ifind_real_file` 导入门槛
- [x] 增加 iFind 事件文件预检命令，导入 lake 前先跑 contract 校验
- [x] 扩展 iFind 事件文件预检输出 JSON 报告，保留真实导入前验收证据
- [x] 收紧 iFind 文件导入命令，contract 校验失败或主键重复时拒绝写入 lake
- [x] 收紧 iFind 文件导入覆盖行为，默认拒绝覆盖已有 dataset，需显式 `--overwrite`
- [x] 将 iFind 文件预检和导入接入 daily ops，提供导出文件后自动写验收报告并入湖
- [x] 将 iFind 文件验收报告接入 objective-audit 和 completion checklist
- [x] 收紧 objective-audit：传入 iFind 验收报告路径但文件缺失时显式标记 `missing_ifind_validation_report`
- [x] 收紧 `data-adapter-status`，iFind real/file adapter 必须在 lake 中有真实数据行才算 live
- [x] 扩展 Tushare 真实适配器到 A 股 `trade_cal`，fetch 层写出 `trade_calendar` reference dataset

## Next Up

- [ ] 用真实 OpenD paper session 生成 execution log 与 Futu broker report，跑满 paper_to_live 所需 session 证据
- [x] 支持 OpenD quote snapshot 输入，用 `symbol` / `lot_size` / price 自动生成 paper ticket 的合法下单数量
- [ ] 启动运行中的 `futu-opend-execution` Web UI / OpenD，跑真实 quote snapshot 和 dry-run paper order
- [x] 把真实 `Tushare` token 接入 fetch 层，覆盖 A 股 `stock_basic` / `daily` / `trade_cal`
- [x] 增加 iFind 可插拔真实事件 adapter，支持 provider/HTTP endpoint 标准化到 `ifind_events`
- [ ] 扩展 Tushare 真实适配器到 HK-US reference / minute panel
- [ ] 确认 iFind 终端/HTTP endpoint 并跑真实 iFind smoke
- [ ] 生成可对账的真实 paper/live 盈利证据 JSON，要求正 `net_pnl`、回撤未破限、broker reconciliation 通过
- [ ] 用更多真实交易日调参并做成本/容量审计，淘汰成本后为负的候选
- [ ] 把日报的数据新鲜度扩展到 DuckDB view 与 source freshness 字段
- [ ] 做更稳健的 `transfer entropy` 估计与显著性检验
- [ ] 增加 `event taxonomy` 与 hold horizon registry
- [ ] 做研究侧 `universe / calendar / session` 管理
- [ ] 增加更丰富的港美联动特征
- [ ] 把 quality report 接成更细的 freshness / null / drift 监控

## 文档与 Contract TODO

- [x] 说明 repo 定位、支持频率与非目标
- [x] 说明数据源分工
- [x] 定义 `security_master`
- [x] 定义 `daily_features`
- [x] 定义 `intraday_l2_features`
- [x] 定义 `execution_log`
- [x] 补充 `signal_events / orders / fills / positions / risk_snapshots / feature_registry`
- [x] 为代码实现补充字段级校验器
- [x] 将 contract schema 与测试 fixture 对齐

## 建议开发顺序

1. 真实数据接入：`Tushare / L2 schema`
2. research universe / calendar / session 管理
3. 研究指标深化：`lead-lag / TE / Bayesian setup library`
4. 事件驱动样本管理与标签体系
5. 观察层增强：quality / lineage / freshness
6. 最后再回到 paper / live execution 硬化

## 协作备注

- 文档与 contract 优先保持稳定，不轻易破坏字段语义
- 若代码实现与 contract 冲突，优先修改实现；若确需调整 contract，请同步更新 `CHANGELOG.md`
- 涉及券商或数据源凭证的部分，一律从环境变量读取
