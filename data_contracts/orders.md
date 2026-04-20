# orders

## Purpose

记录标准化订单对象，用于 backtest 与 live execution 共用的订单语义层。

## Grain

一行代表一张订单。

## Primary Key

- `order_id`

## Fields

| Field | Type | Required | Description |
|---|---|---:|---|
| `order_id` | `string` | Y | 内部订单 id |
| `signal_id` | `string` | N | 来源信号 |
| `strategy_id` | `string` | Y | 策略标识 |
| `broker` | `string` | Y | broker 标识 |
| `account_id` | `string` | N | 账户 id |
| `security_id` | `string` | Y | 统一证券标识 |
| `trade_date` | `date` | Y | 交易日 |
| `created_ts` | `timestamp` | Y | 订单创建时间 |
| `submitted_ts` | `timestamp` | N | 订单提交时间 |
| `side` | `string` | Y | 买卖方向 |
| `order_type` | `string` | Y | `market`, `limit`, `stop`, `auction` |
| `time_in_force` | `string` | N | `day`, `ioc`, `gtc` |
| `qty` | `float64` | Y | 下单数量 |
| `limit_price` | `float64` | N | 限价 |
| `status` | `string` | Y | 内部订单状态 |
| `broker_order_id` | `string` | N | 券商订单 id |
| `mode` | `string` | Y | `dry_run`, `paper`, `live` |
| `reason_code` | `string` | N | 下单原因 |
| `updated_at` | `timestamp` | Y | 最后更新时间 |

## Lineage

- 上游：`signal_events`
- 下游：`fills`, `positions`, `execution_log`
