# fills

## Purpose

记录订单成交明细。

## Grain

一行代表一笔成交。

## Primary Key

- `fill_id`

## Fields

| Field | Type | Required | Description |
|---|---|---:|---|
| `fill_id` | `string` | Y | 成交 id |
| `order_id` | `string` | Y | 对应订单 |
| `broker_order_id` | `string` | N | 券商订单 id |
| `security_id` | `string` | Y | 统一证券标识 |
| `trade_date` | `date` | Y | 交易日 |
| `fill_ts` | `timestamp` | Y | 成交时间 |
| `side` | `string` | Y | 买卖方向 |
| `fill_qty` | `float64` | Y | 成交数量 |
| `fill_price` | `float64` | Y | 成交价格 |
| `fill_notional` | `float64` | N | 成交金额 |
| `fee_amount` | `float64` | N | 成交费用 |
| `slippage_bps` | `float64` | N | 相对参考价滑点 |
| `liquidity_flag` | `string` | N | 主动 / 被动占位 |
| `created_at` | `timestamp` | Y | 入库时间 |

## Lineage

- 上游：`orders`, broker callbacks, simulated matching
- 下游：`positions`, execution analytics
