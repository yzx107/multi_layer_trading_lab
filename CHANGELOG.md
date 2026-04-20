# Changelog

## 2026-04-20

### Added

- 初始化顶层文档：
  - `README.md`
  - `ARCHITECTURE.md`
  - `ROADMAP.md`
  - `TASKS.md`
  - `CHANGELOG.md`
- 新增环境变量模板 `.env.example`
- 新增 `configs/` 配置样例：
  - `base.yaml`
  - `storage.yaml`
  - `brokers.yaml`
  - `risk.yaml`
  - `research.yaml`
  - `contracts.yaml`
- 新增 `data_contracts/` 文档化 schema：
  - `security_master`
  - `daily_features`
  - `intraday_l2_features`
  - `execution_log`
  - `signal_events`
  - `orders`
  - `fills`
  - `positions`
  - `risk_snapshots`
  - `feature_registry`

### Notes

- 当前 contract 设计以 `Parquet + DuckDB` 为中心，优先支持本地研究与 demo execution。
- `Futu / IBKR / Tushare` 凭证与连接参数只通过环境变量注入。
- `transfer entropy`、实时 execution 与 broker 细节仍需后续代码实现补全。
