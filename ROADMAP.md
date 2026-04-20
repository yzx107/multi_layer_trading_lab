# Roadmap

## v0.1 Foundation

目标：建立可运行、可扩展、不过度工程化的研究与执行基础。

范围：

- repo 初始化与质量工具
- 核心 data contracts
- `Tushare` 与 `L2 loader` 最小接入
- `daily_features` 与 `intraday_l2_features`
- baseline signal pipeline
- 最小事件驱动 backtest
- `Futu / IBKR` dry-run execution 抽象

完成标准：

- 能走通 demo 闭环
- 合同表定义清晰
- 文档可指导后续 worker 实现代码

## v0.2 Research Deepening

目标：增强研究质量与样本管理。

计划项：

- 更完整的港股 / 美股 / A 股 symbol normalization
- `feature_registry` 与数据质量检查
- lead-lag 扫描批处理
- `transfer entropy` 替换为更稳健实现
- Bayesian setup library 与 rolling posterior
- 事件标签与 hold horizon 体系

## v0.3 Execution Hardening

目标：让 dry-run / paper 更贴近真实流程。

计划项：

- `Futu` 行情订阅与 paper order 完整链路
- `IBKR` adapter 补充 order state mapping
- 风控规则执行顺序与拒单原因标准化
- execution reconciliation
- 开盘滑点监控与交易失真保护

## v0.4 Strategy Expansion

目标：支持更多可复用策略模板。

计划项：

- 港股事件驱动模板
- 港美联动模板
- 开盘微观结构模板
- Bayesian regime switcher
- strategy config registry

## v0.5 Ops & Observability

目标：增强长期运行可维护性。

计划项：

- 更完善的 execution dashboard 数据源
- lineage 审计与 feature freshness 监控
- cron / launchd 运行样例
- 数据缺口报警与 broker heartbeat

## 非目标

以下内容不在近期 roadmap 的核心范围内：

- 交易所 co-location
- ultra-low-latency C++ matching stack
- 多区域云原生大集群
- 用单一模型替代完整研究流程
