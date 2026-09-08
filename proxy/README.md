# CNEquity Query Proxy

独立的只读 HTTP 查询服务。它不导入 `cnequity`，不读取原项目的 TOML、SQLite、
DuckDB 或元数据；与主项目唯一的接口是本地 Parquet 文件布局：

```
{CNEQUITY_PROXY_DATA_ROOT}/curated/daily_bars/trade_date=YYYY-MM-DD/*.parquet
{CNEQUITY_PROXY_DATA_ROOT}/curated/minute_bars/trade_date=YYYY-MM-DD/*.parquet
{CNEQUITY_PROXY_DATA_ROOT}/curated/minute_bars_5m/trade_date=YYYY-MM-DD/*.parquet
{CNEQUITY_PROXY_DATA_ROOT}/curated/trading_status/trade_date=YYYY-MM/*.parquet
{CNEQUITY_PROXY_DATA_ROOT}/curated/instruments/part-merged.parquet
{CNEQUITY_PROXY_DATA_ROOT}/curated/valuation_metrics/trade_date=YYYY-MM-DD/*.parquet
{CNEQUITY_PROXY_DATA_ROOT}/curated/industry_members/as_of_date=YYYY-MM-DD/*.parquet
{CNEQUITY_PROXY_DATA_ROOT}/curated/sector_members/as_of_date=YYYY-MM-DD/*.parquet
{CNEQUITY_PROXY_DATA_ROOT}/curated/index_constituents/as_of_date=YYYY-MM/*.parquet
{CNEQUITY_PROXY_DATA_ROOT}/curated/fund_flow/trade_date=YYYY-MM-DD/*.parquet
{CNEQUITY_PROXY_DATA_ROOT}/curated/analyst_consensus/forecast_date=YYYY-MM-DD/*.parquet
{CNEQUITY_PROXY_DATA_ROOT}/curated/hot_rank/trade_date=YYYY-MM/*.parquet
{CNEQUITY_PROXY_DATA_ROOT}/curated/sentiment_scores/trade_date=YYYY-MM/*.parquet
{CNEQUITY_PROXY_DATA_ROOT}/derived/adj_factors/trade_date=YYYY-MM-DD/*.parquet
```

当前提供证券基础信息、每日交易状态、单股轻量摘要、全市场原始日线/后复权因子/交易状态批量下载、
`1d`、`1m`、`5m`、`1w`、`1mo` K 线与复权因子查询。批量下载以流式 TAR 原样下发指定日期窗口
匹配的 `daily_bars`、`adj_factors` 或 `trading_status` Parquet 文件，不经 DuckDB 或 JSON；摘要聚合
基础信息、最新市场状态、行业/板块/指数归属及轻量信号。周/月线由本地日 K 聚合，`1m` 和 `5m`
读取各自独立的日内 Parquet 目录，交易状态读取独立的月分区目录；它不会抓取上游数据、不会写入
数据湖，也不会修改任何现有文件。

## 安装与启动

```bash
cd proxy
python -m pip install -e .
export CNEQUITY_PROXY_DATA_ROOT=/absolute/path/to/data/cnequity
export CNEQUITY_PROXY_API_KEY=replace-with-a-long-random-secret
cnequity-query-proxy
```

默认监听 `127.0.0.1:8790`。使用 `CNEQUITY_PROXY_HOST` 和
`CNEQUITY_PROXY_PORT` 覆盖。非回环地址启动时必须设置
`CNEQUITY_PROXY_API_KEY`；业务请求通过 `Authorization: Bearer <token>` 鉴权。

若由反向代理将外部路径前缀剥离后转发到 Proxy，设置与该前缀相同的
`CNEQUITY_PROXY_ROOT_PATH`。例如将 `https://cnequity.codingdie.com/query/*` 重写到服务根路径时，
设置 `CNEQUITY_PROXY_ROOT_PATH=/query`；这样 `/query/docs` 中的 OpenAPI 请求仍会指向
`/query/openapi.json`。

交互式接口文档位于 `http://127.0.0.1:8790/docs`，健康检查为 `/healthz`。
参数、复权公式、分页、错误码和请求示例以 [完整 API 文档](docs/API.md) 为准；新增或变更
接口时需要同步更新该文档，并由代理测试校验路由清单。

Python 调用方可安装独立的 [CNEquity Query SDK](../sdk/README.md)，通过类型化客户端查询全部
公开 HTTP 接口，而无需导入代理服务端或数据湖项目。

## 性能与运行边界

- 先按 `trade_date=` 分区目录选择日 K、日内 K、复权因子和交易状态文件；交易状态按月分区，证券主数据只打开固定 canonical 文件，避免全湖递归 glob。
- 单股摘要只打开十个白名单轻量数据集的最新分区；`market.latest_market` 最多返回一条最新可用日级行情快照，不读取分钟线、日 K 历史、财报或股东明细。每个模块保留各自的日期与溯源。
- SQL 只投影接口所需列，并将 symbol、日期、名称搜索和页大小过滤推给 DuckDB。
- 全市场日线、后复权因子和交易状态批量下载只枚举各自匹配的 Parquet 分区，并以 1 MiB 块流式
  读取原始文件；不会将全市场行或完整归档留在内存。它们不使用普通查询额度，也不施加日期、文件、
  原始字节或并发下载配额。交易状态按月分区，日期窗口会下载重叠月份的整月文件，研究端再按文件内
  `trade_date` 精确过滤。
- 日/周/月 JSON 查询默认单次窗口最多 3660 天；`1m` 默认最多 31 天、`5m` 默认最多 90 天，均可用
  环境变量调整。非批量查询单次最多打开 6000 个 Parquet 文件。
- 周/月线先逐日复权再聚合；日内线不在请求期内从另一个频率重采样，避免额外扫描和语义偏差。
- 进程内 LRU 缓存默认保存 60 秒，分区目录索引也以相同 TTL 刷新；这使日更后最多一分钟延迟，不会向数据湖写缓存文件。
- 默认最多 4 个未命中普通查询同时扫描；K 线、因子、证券、交易状态与单股摘要 JSON 接口共享这个
  额度，超过时立即返回 `429`，避免突发请求拖慢本地磁盘和采集任务。

可配置环境变量：

| 变量 | 默认值 | 作用 |
| --- | --- | --- |
| `CNEQUITY_PROXY_DATA_ROOT` | 必填 | 数据湖根目录 |
| `CNEQUITY_PROXY_API_KEY` | 空 | Bearer Token；对外绑定时必填 |
| `CNEQUITY_PROXY_HOST` | `127.0.0.1` | 监听地址 |
| `CNEQUITY_PROXY_PORT` | `8790` | 监听端口 |
| `CNEQUITY_PROXY_ROOT_PATH` | 空 | 反向代理剥离的外部路径前缀，例如 `/query` |
| `CNEQUITY_PROXY_DEFAULT_WINDOW_DAYS` | `365` | 未给日期时的默认窗口 |
| `CNEQUITY_PROXY_MAX_WINDOW_DAYS` | `3660` | 单次允许的最大日期跨度 |
| `CNEQUITY_PROXY_DEFAULT_MINUTE_WINDOW_DAYS` | `5` | `1m` 未给日期时的默认窗口 |
| `CNEQUITY_PROXY_MAX_MINUTE_WINDOW_DAYS` | `31` | `1m` 单次允许的最大日期跨度 |
| `CNEQUITY_PROXY_DEFAULT_5M_WINDOW_DAYS` | `20` | `5m` 未给日期时的默认窗口 |
| `CNEQUITY_PROXY_MAX_5M_WINDOW_DAYS` | `90` | `5m` 单次允许的最大日期跨度 |
| `CNEQUITY_PROXY_DEFAULT_LIMIT` | `1000` | 默认每页条数 |
| `CNEQUITY_PROXY_MAX_BARS` | `5000` | 单页最大条数 |
| `CNEQUITY_PROXY_MAX_FILES` | `6000` | 非批量查询单次最多打开的 Parquet 文件数 |
| `CNEQUITY_PROXY_CACHE_TTL_SECONDS` | `60` | 结果与分区索引的 TTL；`0` 禁用结果缓存 |
| `CNEQUITY_PROXY_CACHE_ENTRIES` | `512` | 每个查询服务的 LRU 最大条目数 |
| `CNEQUITY_PROXY_MAX_CONCURRENT_QUERIES` | `4` | 同时执行的缓存未命中查询数 |
| `CNEQUITY_PROXY_DUCKDB_THREADS` | `1` | 单条 DuckDB 查询可用线程数 |

## 测试

```bash
cd proxy
python -m pip install -e . --group dev
pytest
```
