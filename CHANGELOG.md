# Changelog

## 2026-04-23

### Added

- 新增代码级 contract 验证模块：
  - `src/multi_layer_trading_lab/contracts/`
- 新增 storage 侧 research helper：
  - `storage/quality.py`
  - `storage/registry.py`
- 新增 research labeling：
  - `labels/normalization.py`
  - `labels/horizon.py`
- 新增更完整的 modeling helper：
  - `attach_setup_posteriors`
  - `summarize_setup_posteriors`
  - `estimate_transfer_entropy`
  - `batch_scan_lead_lag`
- 新增非交易 research workflow：
  - `pipelines/research_pipeline.py`
  - `reports/summary.py`
  - `scripts/run_research_demo.py`
- 新增 research / contract / quality / lead-lag / labels tests

### Changed

- 将 `Tushare adapter`、daily features、intraday features、L2 features 向 contract 字段靠拢，增加 `security_id`、`market`、`feature_set_version`、`source metadata`
- research CLI 新增：
  - `research-demo`
  - `validate-research`
- `research_summary.md` 现在会输出：
  - contract validation
  - dataset quality
  - Bayesian posterior summary
  - lead-lag candidate ranking

### Notes

- 这轮重点是把 repo 从“能跑 demo”推进到“像样的 research 版本”
- 按用户要求，仍然没有去扩展真实交易模块
- `transfer entropy` 目前仍是 ranking aid，不是 causal proof

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
