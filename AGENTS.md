# CNEquity AI 编码说明

## AI 项目说明

CNEquity 是自托管的中国市场研究数据湖。它将公开数据源采集到本地 Parquet 湖，保留
行级溯源和质量证据，并通过 Python、DuckDB、Polars、dashboard 与 MCP 提供稳定的只读
查询接口。目标是让研究脚本和 AI agent 使用同一份可复查的历史数据，而不是让模型临时
抓取不可复现的快照。

项目的核心生命周期是：`adapter -> 通过校验的 staging -> 受门禁保护的 compact ->
curated -> derived -> 质量证据`。其中，PIT 语义、历史 universe（含退市标的）、复权、
数据源身份和采集时间都是研究契约的一部分；发现覆盖、来源或数据质量不确定时，应显式
失败、告警或记录证据，不能用默认值或其他来源静默补齐。

CNEquity 是研究数据基础设施，不生成交易信号、不执行交易，也不把采集、重试或清理暴露
给查询、dashboard 或 MCP。AI agent 的职责是基于已验证的数据解释和分析；写入数据湖的
职责仅属于受编排和校验保护的采集路径。

本仓库是 fork 后的私有仓库：`fork_main` 仅跟踪原始仓库，`main` 承载本仓库的修改。
未经用户明确授权，不得自动修改 `fork_main`，包括对该分支执行 `pull`、`push`、提交、
rebase、merge、reset 或任何其他会改变其状态的 Git 操作。需要同步上游或回写原仓库时，
必须先取得用户的明确指令。

本仓库基于 `fork_main` 演进并包含私有自定义功能。同步、rebase 或处理冲突时，若私有功能
与原仓库功能冲突，或两者的实现方案存在实质分歧，必须先提交给人工确认和协调后再继续。
不得为了消除冲突而粗暴丢弃、覆盖或自动选择任一侧实现；应保留双方意图、说明冲突点，并
等待用户决定合并、改造、保留或放弃的方案。

## 部署说明

线上 Kubernetes 服务为 `quant` namespace 中的 `cnequity`，部署配置和发布脚本位于
`/home/codingdie/codes/home-cloud/cnequity/`。只有在用户明确要求部署、相关测试通过且业务代码
已经提交并推送后，才能在该目录先执行 `./build.sh` 构建并推送镜像，再执行 `./k8s/install.sh`
应用清单并等待 `cnequity` 就绪。

构建时必须显式确认 `CNEQUITY_REPO` 和 `CNEQUITY_BRANCH` 指向已推送的目标仓库与分支；不要让
部署脚本回退到其他仓库或分支。不得绕过 `home-cloud` 直接对线上 `cnequity` 执行临时 `kubectl`
更新，也不得用未提交或未推送的业务代码部署。若部署涉及配置变更，应先在 `home-cloud` 的受
版本控制清单中完成修改。

## 开始前先读

修改前按任务读取最小必要的权威文档：

- [架构总览](docs/architecture/overview.md)：数据流、包边界、湖分层和研究语义。
- [开发约定](docs/development/conventions.md)：包职责与代码放置规则。
- [测试说明](docs/development/testing.md)：marker、fixture 与测试命令。
- [新增数据集](docs/development/adding-dataset.md)：注册、schema、调度和文档要求。
- [发布治理](docs/development/release-governance.md)：数据契约与发布门禁。

## 仓库地图

| 路径 | 职责 |
| --- | --- |
| `src/cnequity/domain/` | 数据集 schema、主键、元数据和共享领域规则。 |
| `src/cnequity/adapters/` | 薄的数据源 I/O、分页和源侧解析。 |
| `src/cnequity/steps/` | 注册的采集 step、staging 写入与数据集编排。 |
| `src/cnequity/orchestrator/` | Wave DAG、manifest、重试和 worker pool。 |
| `src/cnequity/storage/` | Parquet 布局、原子写入、压实、revision 和水位。 |
| `src/cnequity/derive/` | 可重算的派生数据集。 |
| `src/cnequity/quality/` | 数据集审计、交叉校验、源 diff 和 findings。 |
| `src/cnequity/query/` | 只读 Python/DuckDB 查询契约。 |
| `src/cnequity/serve/` | 只读本地 dashboard。 |
| `proxy/` | 独立只读 HTTP proxy；不得导入 `cnequity` 或写入数据湖。 |
| `tests/` | 主包测试；单元测试默认离线。 |
| `proxy/tests/` | 独立 proxy 测试套件。 |
| `docs/adr/` | 持久的非平凡架构取舍。 |

## 不可破坏的数据规则

- 保持湖生命周期：adapter -> 通过校验的 staging -> 受门禁保护的 compact
  -> curated -> derived -> 质量证据。不得绕过 staging，也不得在部分或失败 batch
  后推进水位。
- 每个 frame 写入前都要校验。schema、主键、分区和溯源定义在
  `domain/schemas.py` 与 `domain/datasets.py`。
- curated 行的 `source`、`data_version`、`fetched_at` 必须真实。未知、空或失败的
  源响应不能被认证为已有数据。
- adapter 必须保持薄：传输、分页和源侧怪异逻辑放入 `adapters/`；调度、窗口、manifest
  和落湖逻辑放入 `steps/` 或 orchestrator。
- 对 PIT、历史覆盖、退市、ST/停牌、复权因子和源身份采取 fail-closed 策略。契约要求
  error、warning、retry 或明确溯源时，不得静默替换默认值或其他数据源。
- 修改 schema 类型、单位、主键、PIT 语义或历史含义即为契约变更。需要时补测试、数据集
  文档以及 schema version 或迁移决策。
- 遵守数据源政策、限流和请求边界。新增数据源时必须同步处理许可/来源政策和溯源。

## 按改动类型执行

### 数据集或数据源

1. 先定义或更新 schema 与 `DatasetSpec`。
2. 源侧解析放在 adapter，并归一化到 canonical schema。
3. 在对应的 `steps/` 分层注册 step，并从 `steps/__init__.py` import 新模块。
4. 为正常、畸形、空和部分响应补离线测试（按实际适用范围）。
5. 公共行为改变时更新 `docs/datasets/catalog.md`、`docs/datasets/sources.md` 与
   契约/发布说明。

### Query 或 Serve

- 保持只读；查询和 dashboard 路径不得修改数据湖，也不得隐藏历史覆盖缺口。
- 通过现有 reader/query 接口应用 PIT、universe、交易日历和复权规则，不要在调用方
  重新实现。

### Proxy

- `proxy/` 必须能在不安装主包的情况下独立安装和测试。
- 只读取已文档化的 Parquet 路径；不得读取主项目配置、manifest、SQLite 或元数据，也不得
  向数据湖写入缓存文件。
- 保持请求限制、文件预算、参数校验和 Bearer Token 要求；公开路由改变时更新
  `proxy/docs/API.md`。

### Frontend

- 源码在 `frontend/`；`src/cnequity/serve/static/` 下的 dashboard 产物需要提交。
- 修改 dashboard 后，在 `frontend/` 运行 `npm ci` 和 `npm run check`。不得引入 CDN
  依赖，dashboard 必须能离线运行。

## 验证

优先使用已有虚拟环境。默认测试排除标记为 `network` 的用例。

```bash
.venv/bin/ruff check .
.venv/bin/ruff format --check .
PYTHONPATH=src .venv/bin/python -m pytest tests/ -q
PYTHONPATH=proxy/src .venv/bin/python -m pytest proxy/tests -q
```

局部改动先跑最近的单元测试文件，再跑相关套件。联网测试必须显式使用 `-m network`，
不能替代确定性的离线覆盖。

## 安全工作方式

- 不得提交本地数据湖、运行时产物、凭据或用户配置；特别是
  `configs/cnequity.toml`、`data/` 和 `logs/`。
- 未获用户明确授权时，不得执行破坏性数据湖操作、数据源变更、发布、部署或远端历史改写。
- 保留无关的工作区改动；交付前运行 `git diff --check`，并将环境限制与代码失败分开报告。
- 优先做小而有测试保障的改动，避免大范围重构。跨层且长期有效的设计选择应写 ADR，而不是
  隐藏在实现细节中。
