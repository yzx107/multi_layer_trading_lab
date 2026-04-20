# risk_snapshots

## Purpose

记录策略、账户或交易会话的风险状态，用于风控审计与熔断决策。

## Grain

一行代表某个风险对象在某时刻的风险快照。

## Primary Key

- `risk_snapshot_id`

## Fields

| Field | Type | Required | Description |
|---|---|---:|---|
| `risk_snapshot_id` | `string` | Y | 快照 id |
| `snapshot_ts` | `timestamp` | Y | 快照时间 |
| `trade_date` | `date` | Y | 交易日 |
| `scope_type` | `string` | Y | `account`, `strategy`, `symbol` |
| `scope_id` | `string` | Y | 账户、策略或证券标识 |
| `gross_exposure` | `float64` | N | 毛敞口 |
| `net_exposure` | `float64` | N | 净敞口 |
| `risk_budget_used` | `float64` | N | 已使用风险预算 |
| `max_drawdown_day` | `float64` | N | 当日回撤 |
| `signal_age_seconds` | `float64` | N | 当前信号时延 |
| `quote_staleness_seconds` | `float64` | N | 行情陈旧度 |
| `open_slippage_bps` | `float64` | N | 开盘滑点 |
| `trading_halted_flag` | `bool` | N | 是否停止开仓 |
| `halt_reason` | `string` | N | 熔断或停单原因 |
| `created_at` | `timestamp` | Y | 写入时间 |

## Lineage

- 上游：positions, execution events, market data health
- 下游：`execution_log`, risk dashboard
