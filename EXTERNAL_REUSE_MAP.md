# External Reuse Map

本项目不重新实现已经存在的港股 L2 清洗、DQA、verified layer 和因子工厂。当前 repo 的职责是把这些上游产物接入到个人交易者可运行的研究、组合、风控与执行编排里。

## 上游职责

### Hshare Lab v2

- repo: `/Users/yxin/AI_Workstation/Hshare_Lab_v2`
- 数据根路径: `/Volumes/Data/港股Tick数据`
- 拥有层级:
  - raw: `/Volumes/Data/港股Tick数据/2025`, `/Volumes/Data/港股Tick数据/2026`
  - stage/candidate cleaned: `/Volumes/Data/港股Tick数据/candidate_cleaned`
  - DQA: `/Volumes/Data/港股Tick数据/dqa`
  - verified: `/Volumes/Data/港股Tick数据/verified`
- 本项目只读消费:
  - `candidate_cleaned` 用于结构探索、覆盖检查和临时 profile
  - `verified` 用于正式研究输入
- 本项目不做:
  - raw 改写
  - stage contract 重定义
  - 字段语义升级
  - full-year DQA

### hk_factor_autoresearch

- repo: `/Users/yxin/AI_Workstation/hk_factor_autoresearch`
- 拥有层级:
  - factor contract
  - research card
  - fixed harness
  - Gate A/B/C/D/E promotion policy
  - factor registry and experiment runs
- 本项目只读消费:
  - `registry/` 中的候选、家族、晋级记录
  - `runs/` 中的固定 harness 输出
  - 已通过 gate 的 factor output / summaries
- 本项目不做:
  - 重新写普通因子工厂
  - 绕过 harness 的临时因子实验
  - 用未晋级因子直接驱动交易

### Ashare_Lab / Tushare

- repo: `/Users/yxin/AI_Workstation/Ashare_Lab`
- 本项目只复用其 Tushare 连接与 smoke script 经验
- Tushare 凭证只允许从环境变量或本地 `.env` 读取，不写入代码

### futu-opend-execution / OpenD

- repo: `/Users/yxin/AI_Workstation/futu-opend-execution`
- OpenD app: `/Users/yxin/AI_Workstation/Futu_OpenD_9.6.5618_Mac`
- 本项目复用其 execution safety model、paper/dry-run 优先和日志/对账边界
- 本项目不默认下真实订单；live 必须由 paper evidence、reconciliation 和 `manual_live_enable` 解锁

## 当前 Repo 职责

`multi_layer_trading_lab` 应集中在以下层：

- 把 Hshare verified、Tushare、iFind、因子工厂输出统一成研究输入清单
- 在因子候选之上做 Bayesian posterior、lead-lag / transfer entropy 排序和组合层筛选
- 按百万级个人账户约束做 Kelly 上限、流动性、集中度和回撤门控
- 维护 dry-run / paper / live 的 OpenD 执行审计和 reconciler
- 输出每日 ops/readiness 报告，决定是否允许从研究推进到 paper，再从 paper 推进到 live

## 本地检查

```bash
.venv/bin/python -m multi_layer_trading_lab.cli external-repo-precheck
.venv/bin/python -m multi_layer_trading_lab.cli research-input-manifest \
  --output-path data/logs/research_input_manifest.json
.venv/bin/python -m multi_layer_trading_lab.cli factor-factory-summary --lake-root data/lake
```

这些命令只检查关键路径、契约入口和可复用产物清单，不会修改任何上游 repo。
`factor-factory-summary` 会把因子工厂的 `gate_b_log.tsv` 与 `factor_families.tsv` 合并成当前 repo lake 中的只读候选摘要，供后续 Bayesian / TE / Kelly 组合层消费。
