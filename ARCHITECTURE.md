# Architecture

## 设计目标

本项目采用单 repo、多层解耦架构，优先保证：

- `contract-first`
- 研究层与执行层解耦
- 本地优先，适合 `Mac mini / MacBook`
- 能从研究平滑走到 `dry-run / paper / live`
- 第一版保留最小闭环，不为了“平台感”过度抽象

## 分层概览

### 1. Ingestion Layer

负责从外部数据源读取原始数据并标准化。

- `TushareAdapter`
- `L2Loader`
- `FutuMarketDataAdapter`
- `IBKRMarketDataAdapter` 占位

输出：

- 原始或轻度标准化的 `Parquet`
- 带 schema 与数据源标签的 staging dataset

### 2. Storage Layer

负责本地持久化、分区布局、DuckDB 查询入口与轻量 catalog。

- 存储介质：`Parquet`
- 查询层：`DuckDB`
- 分区建议：`dataset / market / trade_date`

输出：

- 标准化逻辑表
- 特征表
- 日志表

### 3. Feature Layer

负责将日线、分钟线、L2 聚合数据转成可复用特征。

- `daily/`
- `intraday/`
- `l2/`

原则：

- 不把 raw tick 直接喂给信号层
- feature 产物需要带 `feature_set_version`
- 每次产出记录 lineage

### 4. Model & Signal Layer

研究框架由三类模块构成：

- `lead_lag / transfer_entropy`: 候选领先关系扫描
- `bayesian_updater`: setup 胜率、命中率与 regime 概率更新
- `kelly_risk_budget`: 风险预算上限

信号层负责把研究输出转成标准 `signal_events`。

### 5. Backtest Layer

最小事件驱动回测流程：

1. 读取 `signal_events`
2. 交给 `OrderManager`
3. 应用简单成本模型
4. 生成 `orders / fills / positions`
5. 写回 `execution_log`
6. 汇总 `PnL / turnover / drawdown / hit ratio`

### 6. Execution Layer

统一抽象为：

- `BrokerAdapter`
- `MarketDataAdapter`
- `OrderManager`
- `RiskManager`

执行模式：

- `dry-run`: 仅打印与落日志
- `paper`: 调用券商模拟环境
- `live`: 留作真实执行

### 7. Risk & Observability Layer

风控：

- 单票最大仓位
- 单策略风险预算
- 当日最大回撤熔断
- 信号过期保护
- 行情异常禁开仓
- 开盘滑点阈值停单

观测：

- `signal_events`
- `execution_log`
- `orders / fills / positions`
- `risk_snapshots`
- `feature_registry`

## 逻辑数据流

```text
Tushare / L2 files / Futu / IBKR
        ↓
   adapters / loaders
        ↓
  parquet staging datasets
        ↓
 standardized logical tables
        ↓
     feature factories
        ↓
  daily_features / intraday_l2_features
        ↓
    models + signal rules
        ↓
      signal_events
        ↓
  backtest or broker adapters
        ↓
orders / fills / execution_log / positions
```

## 关键边界

### 研究与执行边界

研究层只输出标准化 `signal_events`，不直接依赖某个 broker SDK 的订单对象。

执行层只消费：

- `signal_events`
- account / mode / broker config
- 风控规则

这保证后续可以：

- 研究继续使用历史数据与本地回测
- 执行侧自由切换 `Futu` 与 `IBKR`

### Tick 与 Feature 边界

raw `L2 tick` 只用于：

- 重建聚合桶
- 生成微观结构摘要特征

信号模型默认消费 `intraday_l2_features`，而不是直接消费原始 tick。

### Kelly 定位

`Kelly` 只服务于：

- 仓位上限
- strategy-level 风险预算

默认建议输出：

- `quarter Kelly`
- `eighth Kelly`

不得将其实现为“只要有信号就按满凯利直接下单”。

## 存储约定

建议目录：

```text
data/
  raw/
  staging/
  curated/
  features/
  logs/
duckdb/
  research.duckdb
```

分区建议：

- 高频或准高频表：`dataset / market / trade_date`
- 日频表：`dataset / market / as_of_date`
- 执行表：`broker / account / trade_date`

时间戳约定：

- 市场事件时间统一使用 `event_ts`
- ingestion 时间使用 `ingested_at`
- 研究表默认保留原时区字段或用 `exchange_tz`
- 跨市场联动计算建议显式记录 `source_tz` 与 `normalized_ts_utc`

## 配置系统

`configs/` 下保留环境无关的样例配置：

- `base.yaml`
- `storage.yaml`
- `brokers.yaml`
- `risk.yaml`
- `research.yaml`
- `contracts.yaml`

敏感值只来自环境变量，不进入 git。

## 初版取舍

第一版明确不做：

- 实盘级分布式撮合路由
- 复杂企业级权限系统
- 流式状态服务
- 多机一致性编排

第一版优先做：

- 可运行最小闭环
- 明确 schema 与 lineage
- 可扩展但轻量的 adapter / execution interface
