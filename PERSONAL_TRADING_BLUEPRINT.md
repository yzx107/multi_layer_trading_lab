# Personal Trading Blueprint

## Objective

Build `multi_layer_trading_lab` into a professional, local-first trading research and execution system for a personal trader with:

- HK L2 historical data
- Tushare data
- iFind data
- Futu OpenD market data and execution
- Mac mini as the main research/runtime machine
- MacBook Air as review, monitoring, and emergency-control machine
- Account equity around the low millions

The project should be engineered to maximize the chance of finding durable edges, but it must not assume profit before evidence. The operating rule is:

```text
research evidence -> paper execution evidence -> small live capital -> scaled live capital
```

## Institutional Method, Personal Scale

The professional pattern is not "one model predicts the market." It is a production research loop:

- clean data contracts
- reproducible datasets
- feature lineage
- point-in-time joins
- leakage checks
- transaction-cost and capacity modeling
- walk-forward evaluation
- paper/live execution reconciliation
- risk budgets that survive bad days

For this account size and hardware, the right target is not high-frequency trading. The right target is a small number of liquid, observable, capacity-aware strategies:

- HK opening microstructure from L2 imbalance and auction behavior
- HK intraday continuation/reversal after open
- HK/US lead-lag candidates using ADR, sector, index, and large-cap relationships
- event-driven setups from Tushare/iFind event and fundamentals data
- Bayesian setup scoring to decide when a researched pattern deserves capital

## Data Source Roles

### HK L2

Primary use:

- opening auction and first 5/15/30 minute behavior
- book imbalance, trade imbalance, spread, depth slope, and abnormal turnover
- execution slippage and liquidity filters

Rule:

- raw ticks are never consumed directly by strategy code
- L2 must first become versioned `intraday_l2_features`

### Tushare

Primary use:

- market calendars
- security master
- daily and minute panels
- cross-market context
- corporate actions and basic factors where available

Rule:

- all Tushare-derived datasets need refresh time, source tag, and point-in-time assumptions

### iFind

Primary use:

- event, news, fundamentals, and cross-check datasets
- validation against Tushare where fields overlap
- event taxonomy enrichment

Rule:

- iFind is an enrichment layer, not a hidden dependency for the core demo path

### Futu OpenD

Primary use:

- realtime quotes
- order book sampling
- paper/live order routing
- broker-side state reconciliation

Rule:

- `live` requires manual enablement, clean paper logs, and active risk guardrails

## Capital And Risk Defaults

For a `1,000,000` account-equity baseline:

- max single-name notional: `8%`, or `80,000`
- max strategy notional: `20%`, or `200,000`
- max gross exposure: `100%`, or `1,000,000`
- max daily drawdown halt: `1%`, or `10,000`
- default Kelly scale: `1/8 Kelly`
- opening slippage halt: `35 bps`

These defaults are encoded in:

- `configs/personal_trading.yaml`
- `multi_layer_trading_lab.risk.profile.PersonalAccountProfile`

## Promotion Gates

### Research To Paper

A strategy can move to paper only after:

- at least `80` historical trades
- at least `20` distinct trade dates
- no-lookahead audit passes
- cost model is applied
- liquidity and capacity checks pass
- drawdown and hit-rate behavior are understood by regime

### Paper To Small Live

A strategy can move to small live only after:

- at least `20` paper sessions
- order reject rate below `2%`
- broker reconciliation is clean
- realized paper slippage is inside the research assumption
- live enablement is manual and explicit

### Small Live To Scaled Live

Scaling requires:

- stable live fills
- controlled slippage
- no unexplained PnL breaks
- no risk-limit overrides
- research and live distributions remain comparable

## Mac Mini / MacBook Operating Model

Mac mini:

- nightly ingestion
- Parquet/DuckDB research store
- feature generation
- batch backtests
- Futu OpenD runtime when actively trading
- logs and reports

MacBook Air:

- review reports
- monitor paper/live session state
- emergency manual stop
- lightweight analysis only

This avoids building cloud orchestration before the research loop proves value.

## Next Engineering Milestones

1. Implement real data adapters for Tushare, HK L2 schema, and iFind staging.
2. Add market calendar, session, and universe management.
3. Add leakage, cost, and capacity checks as hard research gates.
4. Add strategy templates for HK open microstructure and HK/US lead-lag.
5. Harden Futu OpenD paper execution and reconciliation.
6. Add daily reports that show data freshness, candidate signals, risk budget, and execution quality.

## Non-Negotiable Boundaries

- No hardcoded credentials.
- No direct raw-tick strategy consumption.
- No live trading from unvalidated research output.
- No full Kelly sizing.
- No scaling a strategy that only works before costs.
- No treating transfer entropy or lead-lag ranking as causality proof.
