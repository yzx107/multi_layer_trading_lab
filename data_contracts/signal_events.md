# signal_events

## Purpose

标准化研究信号输出，作为 backtest 与 execution 的统一输入。

## Grain

一行代表一个策略在某时刻对某证券产生的一次可执行信号。

## Primary Key

- `signal_id`

并建议唯一约束：

- `strategy_id`
- `security_id`
- `event_ts`
- `signal_type`

## Partition Recommendation

- `strategy_id`
- `trade_date`

## Fields

| Field | Type | Required | Description |
|---|---|---:|---|
| `signal_id` | `string` | Y | 信号唯一 id |
| `strategy_id` | `string` | Y | 策略标识 |
| `setup_id` | `string` | N | setup 或子策略标识 |
| `security_id` | `string` | Y | 统一证券标识 |
| `market` | `string` | Y | 市场 |
| `trade_date` | `date` | Y | 交易日 |
| `event_ts` | `timestamp` | Y | 信号生成时间 |
| `signal_type` | `string` | Y | `entry`, `exit`, `reduce`, `reverse` |
| `side` | `string` | Y | `buy`, `sell`, `short_cover`, `sell_short` |
| `horizon_tag` | `string` | N | 如 `open_30m`, `1d`, `3d` |
| `strength` | `float64` | N | 原始信号强度 |
| `posterior_win_prob` | `float64` | N | Bayesian posterior mean |
| `expected_payoff_ratio` | `float64` | N | 预期盈亏比 |
| `target_notional` | `float64` | N | 目标名义仓位 |
| `kelly_fraction_used` | `float64` | N | 实际使用的 Kelly fraction |
| `expiry_ts` | `timestamp` | N | 信号失效时间 |
| `quote_ts` | `timestamp` | N | 生成信号时参考行情时间 |
| `reason_code` | `string` | N | 规则命中原因 |
| `feature_set_version` | `string` | N | 关联 feature 集版本 |
| `model_version` | `string` | N | 关联 model 版本 |
| `data_source` | `string` | Y | 上游数据源摘要 |
| `source_run_id` | `string` | N | 生成任务 id |
| `created_at` | `timestamp` | Y | 信号入库时间 |

## Source

由 feature layer 与 model layer 联合生成。

## Lineage

- 上游：`daily_features`, `intraday_l2_features`
- 下游：`orders`, `execution_log`, backtest engine

## Notes

- `signal_events` 不应直接绑定某券商 order object
- 对于不可执行的研究打分，不应写入此表，应保留在研究结果表
