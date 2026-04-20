# feature_registry

## Purpose

登记 feature 集版本、字段说明、依赖与产出频率，保证 feature lineage 可追踪。

## Grain

一行代表一个 feature 集定义或版本。

## Primary Key

- `feature_set_version`

## Fields

| Field | Type | Required | Description |
|---|---|---:|---|
| `feature_set_version` | `string` | Y | 如 `daily_v1`, `l2_v1` |
| `feature_domain` | `string` | Y | `daily`, `intraday`, `l2` |
| `owner` | `string` | N | 负责人或模块名 |
| `description` | `string` | Y | 特征集说明 |
| `feature_columns` | `string` | Y | 列名列表，可为 JSON 字符串 |
| `source_tables` | `string` | Y | 依赖表列表 |
| `refresh_frequency` | `string` | N | `daily`, `eod`, `intraday_batch` |
| `quality_status` | `string` | N | `draft`, `validated`, `deprecated` |
| `created_at` | `timestamp` | Y | 创建时间 |
| `updated_at` | `timestamp` | Y | 更新时间 |

## Lineage

- 上游：研发登记
- 下游：feature pipelines, tests, monitoring
