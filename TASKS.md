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

## Next Up

- [ ] 把真实 `Tushare` token 接入 fetch 层
- [ ] 对接真实港股 `L2` 文件 schema，而不是 demo sample
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
