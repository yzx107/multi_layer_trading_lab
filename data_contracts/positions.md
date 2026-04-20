# positions

## Purpose

记录账户或策略维度的持仓快照。

## Grain

一行代表某账户 / 策略 / 证券在某个快照时点的持仓。

## Primary Key

- `account_id`
- `strategy_id`
- `security_id`
- `snapshot_ts`

## Fields

| Field | Type | Required | Description |
|---|---|---:|---|
| `account_id` | `string` | Y | 账户标识 |
| `strategy_id` | `string` | Y | 策略标识 |
| `security_id` | `string` | Y | 统一证券标识 |
| `snapshot_ts` | `timestamp` | Y | 快照时间 |
| `trade_date` | `date` | Y | 交易日 |
| `position_qty` | `float64` | Y | 持仓数量 |
| `avg_cost` | `float64` | N | 持仓成本 |
| `market_price` | `float64` | N | 估值价格 |
| `market_value` | `float64` | N | 市值 |
| `unrealized_pnl` | `float64` | N | 浮盈亏 |
| `realized_pnl_day` | `float64` | N | 当日已实现盈亏 |
| `gross_exposure` | `float64` | N | 毛敞口 |
| `net_exposure` | `float64` | N | 净敞口 |
| `updated_at` | `timestamp` | Y | 更新时间 |

## Lineage

- 上游：`fills`, market data snapshots
- 下游：risk checks, pnl reporting
