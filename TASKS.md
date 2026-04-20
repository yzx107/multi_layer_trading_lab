# Tasks

## 当前阶段

阶段目标：把 `contract > storage > features > backtest > execution` 这条链条的基础立住，并形成最小可运行闭环。

## In Progress

- [x] 初始化顶层文档骨架
- [x] 定义核心 data contracts
- [x] 提供环境变量模板与配置样例
- [ ] 建立 `pyproject.toml` 与开发工具链
- [ ] 落地 storage 层目录约定与 DuckDB helper
- [ ] 实现 `Tushare adapter` 最小可用接口
- [ ] 实现 `L2 loader` 与时间桶聚合
- [ ] 实现 daily / intraday / l2 feature factories
- [ ] 实现 Bayesian / Kelly / lead-lag 基础模块
- [ ] 实现最小事件驱动 backtest
- [ ] 实现 dry-run broker adapter
- [ ] 补齐 CLI 与 demo pipeline
- [ ] 补齐 contract / feature / backtest tests

## 文档与 Contract TODO

- [x] 说明 repo 定位、支持频率与非目标
- [x] 说明数据源分工
- [x] 定义 `security_master`
- [x] 定义 `daily_features`
- [x] 定义 `intraday_l2_features`
- [x] 定义 `execution_log`
- [x] 补充 `signal_events / orders / fills / positions / risk_snapshots / feature_registry`
- [ ] 为代码实现补充字段级校验器
- [ ] 将 contract schema 与测试 fixture 对齐

## 建议开发顺序

1. 完成 `pyproject.toml`、lint、test、目录初始化
2. 按 `data_contracts/` 实现 schema model 与 IO helper
3. 打通 `Tushare -> Parquet`
4. 打通 `L2 file -> normalized ticks -> aggregated buckets`
5. 生成 feature 表
6. 生成 signal 与 backtest report
7. 接入 `dry-run` execution

## 协作备注

- 文档与 contract 优先保持稳定，不轻易破坏字段语义
- 若代码实现与 contract 冲突，优先修改实现；若确需调整 contract，请同步更新 `CHANGELOG.md`
- 涉及券商或数据源凭证的部分，一律从环境变量读取
