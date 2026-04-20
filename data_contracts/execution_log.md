# execution_log

## Purpose

统一记录从信号到订单、到成交、到状态变更的执行事件流，用于审计、回放与监控。

## Grain

一行代表一条 execution-side event。

## Primary Key

- `execution_event_id`

## Partition Recommendation

- `broker`
- `account_id`
- `trade_date`

## Timestamp Convention

- `event_ts`: 执行事件时间
- `broker_ts`: broker 返回时间
- `created_at`: 本地落日志时间

## Fields

| Field | Type | Required | Description |
|---|---|---:|---|
| `execution_event_id` | `string` | Y | 执行事件唯一 id |
| `trade_date` | `date` | Y | 交易日 |
| `event_ts` | `timestamp` | Y | 本地执行事件时间 |
| `broker_ts` | `timestamp` | N | broker 回执时间 |
| `mode` | `string` | Y | `dry_run`, `paper`, `live` |
| `broker` | `string` | Y | `futu`, `ibkr`, `simulated` |
| `account_id` | `string` | N | 账户标识 |
| `strategy_id` | `string` | N | 策略标识 |
| `signal_id` | `string` | N | 对应信号 id |
| `order_id` | `string` | N | 内部订单 id |
| `broker_order_id` | `string` | N | 券商订单 id |
| `fill_id` | `string` | N | 成交 id |
| `security_id` | `string` | N | 统一证券标识 |
| `event_type` | `string` | Y | `signal_received`, `risk_rejected`, `order_submitted`, `order_ack`, `fill`, `cancelled`, `expired` |
| `side` | `string` | N | 买卖方向 |
| `qty` | `float64` | N | 数量 |
| `price` | `float64` | N | 订单或成交价格 |
| `notional` | `float64` | N | 名义金额 |
| `slippage_bps` | `float64` | N | 估算滑点 |
| `fee_amount` | `float64` | N | 手续费 |
| `status` | `string` | N | 执行状态 |
| `reject_reason` | `string` | N | 风控或 broker 拒单原因 |
| `message` | `string` | N | 扩展说明 |
| `source_run_id` | `string` | N | 执行批次 id |
| `created_at` | `timestamp` | Y | 本地日志写入时间 |

## Source

由 backtest engine 或 broker adapter 写入。

## Lineage

- 上游：`signal_events`, risk decisions, broker callbacks
- 下游：execution analytics, audit trail, post-trade review

## Notes

- `execution_log` 是事实事件流，不应被当作当前订单状态表
- 当前订单最新状态应从 `orders` 聚合得到
