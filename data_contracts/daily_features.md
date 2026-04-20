# daily_features

## Purpose

存储日频研究特征，主要来自 `Tushare` 日线与辅助资金流、事件映射。

## Grain

一行代表一个证券在一个 `as_of_date` 的一个 `feature_set_version` 下的日频特征快照。

## Primary Key

- `security_id`
- `as_of_date`
- `feature_set_version`

## Partition Recommendation

- `market`
- `as_of_date`

## Timestamp Convention

- `as_of_date`: 特征归属交易日
- `computed_at`: 特征计算完成时间
- `ingested_at`: 原始数据落地时间

## Fields

| Field | Type | Required | Description |
|---|---|---:|---|
| `security_id` | `string` | Y | 统一证券标识 |
| `market` | `string` | Y | `HK` / `US` / `CN` |
| `as_of_date` | `date` | Y | 特征对应交易日 |
| `close` | `float64` | N | 收盘价 |
| `adj_close` | `float64` | N | 复权收盘价 |
| `ret_1d` | `float64` | N | 1 日收益 |
| `ret_5d` | `float64` | N | 5 日收益 |
| `ret_20d` | `float64` | N | 20 日收益 |
| `realized_vol_5d` | `float64` | N | 5 日历史波动率 |
| `realized_vol_20d` | `float64` | N | 20 日历史波动率 |
| `turnover_rate` | `float64` | N | 换手率 |
| `volume_ratio_5d` | `float64` | N | 量比或相对成交量 |
| `gap_from_prev_close` | `float64` | N | 当日相对前收 gap |
| `us_overnight_lead_proxy` | `float64` | N | 美股隔夜映射占位特征 |
| `southbound_flow_proxy` | `float64` | N | 南向资金占位特征 |
| `northbound_flow_proxy` | `float64` | N | 北向资金占位特征 |
| `event_tag` | `string` | N | 事件标签，如业绩、配售、回购 |
| `feature_set_version` | `string` | Y | 如 `daily_v1` |
| `data_source` | `string` | Y | 主要为 `tushare` |
| `source_dataset` | `string` | Y | 上游数据集 |
| `source_run_id` | `string` | N | 上游任务 id |
| `computed_at` | `timestamp` | Y | 特征生成时间 |
| `ingested_at` | `timestamp` | N | 原始数据写入时间 |

## Source

- `Tushare daily bars`
- `Tushare minute / flow / event` 的日频映射

## Lineage

- 上游：`security_master`, daily/minute raw datasets
- 下游：cross-sectional screens, event models, Bayesian setup statistics

## Notes

- 资金流与港美联动特征在 v1 可以先提供占位字段
- 若特征计算依赖多个上游表，建议在实现层额外记录 `lineage_json`
