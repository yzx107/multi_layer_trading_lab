# Data Contracts

本目录定义项目的核心逻辑表与字段语义，目标是让：

- adapter 有统一落地口径
- feature 计算有稳定输入
- backtest / execution 有统一输出
- 后续测试可以围绕 contract 建立

## 设计原则

- 先定义逻辑表，再决定物理存储细节
- 逻辑主键必须清晰
- 每张表都明确时间字段口径
- 保留 lineage 与 source 标记
- 对于尚未完全确定的字段，优先增加 `*_raw` 或占位字段，而不是隐式改语义

## 时间约定

- `event_ts`: 市场事件发生时间
- `bar_start_ts` / `bar_end_ts`: 聚合桶边界
- `as_of_date`: 日频或截面特征对应交易日
- `ingested_at`: 数据写入本地存储时间
- `created_at` / `updated_at`: 系统内部记录创建更新时间

## 主表

- [security_master.md](security_master.md)
- [daily_features.md](daily_features.md)
- [intraday_l2_features.md](intraday_l2_features.md)
- [signal_events.md](signal_events.md)
- [execution_log.md](execution_log.md)
- [orders.md](orders.md)
- [fills.md](fills.md)
- [positions.md](positions.md)
- [risk_snapshots.md](risk_snapshots.md)
- [feature_registry.md](feature_registry.md)
