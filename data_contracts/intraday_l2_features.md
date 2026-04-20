# intraday_l2_features

## Purpose

存储由港股 `L2 tick` 聚合得到的微观结构特征，作为分钟内与开盘信号的标准输入。

## Grain

一行代表一个证券在某个时间桶上的一个 `feature_set_version` 微观结构特征快照。

## Primary Key

- `security_id`
- `bar_start_ts`
- `bucket_size`
- `feature_set_version`

## Partition Recommendation

- `market`
- `trade_date`
- `bucket_size`

## Timestamp Convention

- `trade_date`: 交易日
- `bar_start_ts`, `bar_end_ts`: 桶边界，建议保留交易所本地时区
- `event_ts_min`, `event_ts_max`: 该桶内原始事件时间范围

## Fields

| Field | Type | Required | Description |
|---|---|---:|---|
| `security_id` | `string` | Y | 统一证券标识 |
| `market` | `string` | Y | 首版主要为 `HK` |
| `trade_date` | `date` | Y | 交易日 |
| `bucket_size` | `string` | Y | `1s` / `5s` / `30s` / `1m` |
| `bar_start_ts` | `timestamp` | Y | 时间桶起点 |
| `bar_end_ts` | `timestamp` | Y | 时间桶终点 |
| `mid_price_open` | `float64` | N | 桶起始 mid |
| `mid_price_close` | `float64` | N | 桶结束 mid |
| `mid_ret` | `float64` | N | mid price 收益 |
| `spread_bps_avg` | `float64` | N | 平均价差，bps |
| `bid_ask_imbalance` | `float64` | N | 买卖盘失衡 |
| `trade_imbalance` | `float64` | N | 主动成交方向失衡 |
| `cancel_rate_proxy` | `float64` | N | 撤单率 proxy |
| `depth_slope_bid` | `float64` | N | bid side depth slope |
| `depth_slope_ask` | `float64` | N | ask side depth slope |
| `depth_total_top5` | `float64` | N | 前五档深度摘要 |
| `preopen_stat_flag` | `bool` | N | 是否属于开盘前统计窗 |
| `event_count` | `int64` | N | 桶内事件数 |
| `trade_value` | `float64` | N | 成交额 |
| `feature_set_version` | `string` | Y | 如 `l2_v1` |
| `data_source` | `string` | Y | 如 `hk_l2_local` |
| `source_dataset` | `string` | Y | 上游数据集 |
| `source_run_id` | `string` | N | 上游任务 id |
| `event_ts_min` | `timestamp` | N | 桶内最早事件时间 |
| `event_ts_max` | `timestamp` | N | 桶内最晚事件时间 |
| `computed_at` | `timestamp` | Y | 特征生成时间 |
| `ingested_at` | `timestamp` | N | 原始文件落地时间 |

## Source

本地港股 `L2 tick` 历史文件，经 loader 标准化与聚合得到。

## Lineage

- 上游：raw order book / trade tick files
- 下游：opening alpha、microstructure signal、execution quality diagnostics

## Notes

- v1 允许 `cancel_rate_proxy` 采用近似算法，前提是字段语义清楚
- 若原始数据字段有限，可先退化为 top-of-book 与 trade imbalance 特征
