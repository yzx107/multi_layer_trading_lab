# multi_layer_trading_lab

`multi_layer_trading_lab` 是一个面向本地研究机的港股 / 美股多层研究与执行框架骨架，服务于中低频、事件驱动、港美联动以及港股开盘到日内的微观结构研究与执行优化。

当前版本已经包含两条可运行链路：

1. `demo execution loop`
2. `research deepening workflow`

其中 research 这条链是本轮重点，覆盖：

- contract validation
- feature registry
- dataset quality report
- symbol normalization
- horizon labeling / event outcomes
- Bayesian posterior attachment
- batch lead-lag / transfer entropy ranking
- markdown research summary report

第一版的基础闭环仍然保留：

1. 用 `Tushare` 拉取 `security_master` 与日频 / 分钟级研究数据
2. 从本地港股 `L2 tick` 文件加载并聚合微观结构特征
3. 将数据统一落到 `Parquet + DuckDB`
4. 生成 `daily_features` 与 `intraday_l2_features`
5. 基于简单规则、Bayesian setup 或 lead-lag 候选关系生成 `signal_events`
6. 进入最小事件驱动 backtest
7. 通过 `dry-run / paper` broker adapter 输出拟下单与 `execution_log`

## 系统定位

这个 repo 不是交易所直连 `HFT` 系统，也不试图伪装为超低延迟撮合基础设施。它适合：

- 港股中低频与事件驱动
- 港美联动研究
- 港股开盘到日内的微观结构 alpha 研究
- 持有周期以 `intraday` 到 `1-5D` 为主

它暂时不适合：

- 交易所直连低延迟 `HFT`
- 亚毫秒级 order book replay
- 复杂多账户 OMS/EMS
- 生产级高可用容灾与风控编排

## 数据源分工

### Tushare

用于研究底座与广覆盖面板数据：

- `A-share / HK / US` 基础证券信息
- 日线、分钟线、资金流、板块、事件
- 日频与分钟频横截面特征

### 港股 L2 Tick 历史数据

用于港股微观结构研究：

- order book / tick trade / opening auction 相关行为
- bid-ask imbalance、trade imbalance、order cancellation proxy
- 开盘前后 `1s / 5s / 30s / 1m` 聚合特征

### Futu OpenD / OpenAPI

用于港股实时行情与执行：

- 行情订阅
- order book / tick 订阅
- paper / dry-run / live adapter 抽象

### IBKR

用于美股与备份执行通道：

- 美股常规下单
- 另一条 paper / live 执行通道

## 当前 v1 支持与边界

### v1 已支持的设计目标

- 单 repo、清晰分层
- 本地优先的 `Parquet + DuckDB` 研究存储
- contract-first 的逻辑表定义
- adapter / feature / model / execution 分离
- 研究与执行解耦
- dry-run / paper / live 三态接口规划
- feature lineage、signal log、execution log 的观测设计
- 研究侧 `contract validation + quality report + feature registry`
- `symbol normalization + horizon labeling + event outcome extraction`
- `Bayesian posterior` 和 `lead-lag` 批量扫描
- `research_summary.md` 报告产出

### v1 预计只做最小实现的部分

- `transfer entropy` 先提供基础实现或 placeholder
- `Futu / IBKR` 先落接口与 dry-run demo，不强依赖真实凭证
- 风控先覆盖关键约束，不追求完整 OMS/EMS

## 目录说明

核心结构按功能与可扩展性拆分：

- `configs/`: 环境、存储、策略、broker、风控样例配置
- `data_contracts/`: 核心逻辑表 schema 与 lineage 文档
- `src/`: adapters、storage、features、models、signals、backtest、execution、risk
- `scripts/`: CLI 和运维脚本入口
- `tests/`: contract、feature、model、backtest、adapter smoke tests

## 从研究走到执行

建议流程如下：

1. 初始化环境变量与本地目录
2. 拉取 `security_master`、日线、分钟线样本
3. 导入一个交易日的港股 `L2` 样本并聚合
4. 生成 `daily_features`、`intraday_l2_features`
5. 运行一个 baseline signal pipeline
6. 进入 backtest 验证收益、换手、回撤、hit ratio
7. 使用统一 `BrokerAdapter` 切到 `dry-run` 或 `paper`
8. 将拟订单、成交、风控快照写入执行表

如果只做 research、暂不碰交易模块，现在也可以单独跑：

1. 生成 contract-aligned `security_master / daily_features / intraday_l2_features`
2. 做 `quality + validation`
3. 生成 `feature_registry`
4. 跑 `horizon labels / Bayesian posterior / lead-lag`
5. 输出 `research_summary.md`

## 配置与凭证

所有凭证都从环境变量读取，禁止硬编码：

- `TUSHARE_TOKEN`
- `FUTU_HOST`, `FUTU_PORT`
- `IBKR_HOST`, `IBKR_PORT`, `IBKR_CLIENT_ID`

参考 [.env.example](/Users/yxin/AI_Workstation/Bayes_TE_Kelly Trading/.env.example) 与 `configs/` 下的样例文件。

## 技术栈

- Python `3.11+`
- `pyproject.toml`
- `uv` 优先，兼容 `pip`
- `polars`, `duckdb`, `pyarrow`
- `typer`
- `pydantic-settings`
- `pytest`
- `structlog`
- `ruff`, `black`, `pre-commit`

## 开发原则

- 先立 `contract`，再写 storage / feature / backtest / execution
- 不过度工程化，但保留清晰扩展边界
- 不把 tick 原始流直接当最终信号输入，必须经聚合特征层
- `lead-lag / transfer entropy` 只用于发现候选信息流，不是因果证明器
- `Bayesian update` 用于动态更新 setup 胜率
- `Kelly` 只作为仓位上限与风险预算，不是机械下单器

## 配套文档

- [ARCHITECTURE.md](/Users/yxin/AI_Workstation/Bayes_TE_Kelly Trading/ARCHITECTURE.md)
- [ROADMAP.md](/Users/yxin/AI_Workstation/Bayes_TE_Kelly Trading/ROADMAP.md)
- [TASKS.md](/Users/yxin/AI_Workstation/Bayes_TE_Kelly Trading/TASKS.md)
- [CHANGELOG.md](/Users/yxin/AI_Workstation/Bayes_TE_Kelly Trading/CHANGELOG.md)
- [data_contracts/README.md](/Users/yxin/AI_Workstation/Bayes_TE_Kelly Trading/data_contracts/README.md)

## 快速运行

安装依赖：

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -e '.[dev]'
```

运行 research demo：

```bash
.venv/bin/python -m multi_layer_trading_lab.cli research-demo
.venv/bin/python -m multi_layer_trading_lab.cli validate-research
```

或直接运行脚本：

```bash
.venv/bin/python scripts/run_research_demo.py
```

运行测试：

```bash
.venv/bin/python -m pytest
```
