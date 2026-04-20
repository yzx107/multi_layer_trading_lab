# security_master

## Purpose

统一维护证券基础信息，作为跨 `Tushare / Futu / IBKR` 的 symbol normalization 基础表。

## Grain

一行代表某个 `security_id` 在某个生效区间内的静态元数据快照。

## Primary Key

建议逻辑主键：

- `security_id`
- `effective_from`

若首版不做 SCD，可先用：

- `security_id`

## Partition Recommendation

- `market`
- `active_flag`

## Timestamp Convention

- `listed_date`, `delisted_date`: 交易所日期口径
- `effective_from`, `effective_to`: metadata 生效区间
- `ingested_at`: 本地落地时间

## Fields

| Field | Type | Required | Description |
|---|---|---:|---|
| `security_id` | `string` | Y | 内部统一标识，建议如 `HK.00700`、`US.AAPL` |
| `ticker` | `string` | Y | 市场内 ticker |
| `market` | `string` | Y | `HK` / `US` / `CN` |
| `exchange` | `string` | Y | 如 `SEHK`, `NASDAQ`, `NYSE`, `SSE` |
| `asset_type` | `string` | Y | `equity`, `etf`, `adr`, `index`, `warrant` |
| `currency` | `string` | Y | 交易币种 |
| `lot_size` | `int64` | N | 交易手数，港股尤为重要 |
| `name` | `string` | N | 证券简称 |
| `sector` | `string` | N | 行业或板块 |
| `country` | `string` | N | 主要上市国家或地区 |
| `primary_listing_flag` | `bool` | N | 是否 primary listing |
| `southbound_eligible_flag` | `bool` | N | 是否可纳入 southbound |
| `northbound_proxy_flag` | `bool` | N | 预留字段，用于跨境映射 |
| `listed_date` | `date` | N | 上市日期 |
| `delisted_date` | `date` | N | 退市日期 |
| `active_flag` | `bool` | Y | 当前是否有效 |
| `effective_from` | `timestamp` | N | 元数据生效起点 |
| `effective_to` | `timestamp` | N | 元数据生效终点 |
| `data_source` | `string` | Y | 如 `tushare`, `manual_override` |
| `source_symbol` | `string` | N | 源系统代码 |
| `source_dataset` | `string` | Y | 上游数据集名 |
| `source_run_id` | `string` | N | 上游抓取任务 id |
| `ingested_at` | `timestamp` | Y | 本地写入时间 |

## Source

主要来自 `Tushare`，必要时允许手工 override。

## Lineage

- 上游：`Tushare security basics`
- 下游：`daily_features`, `intraday_l2_features`, `signal_events`, execution universe

## Notes

- `security_id` 必须作为跨 adapter 的统一键
- 港股 code 与美股 code 的 normalization 规则建议单独模块实现
