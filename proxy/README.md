# CNEquity Query Proxy

独立的只读 HTTP 查询服务。它不导入 `cnequity`，不读取原项目的 TOML、SQLite、
DuckDB 或元数据；与主项目唯一的接口是本地 Parquet 文件布局：

```
{CNEQUITY_PROXY_DATA_ROOT}/curated/daily_bars/trade_date=YYYY-MM-DD/*.parquet
{CNEQUITY_PROXY_DATA_ROOT}/curated/minute_bars/trade_date=YYYY-MM-DD/*.parquet
{CNEQUITY_PROXY_DATA_ROOT}/curated/minute_bars_5m/trade_date=YYYY-MM-DD/*.parquet
{CNEQUITY_PROXY_DATA_ROOT}/curated/instruments/part-merged.parquet
{CNEQUITY_PROXY_DATA_ROOT}/derived/adj_factors/trade_date=YYYY-MM-DD/*.parquet
```

当前提供证券基础信息、`1d`、`1m`、`5m`、`1w`、`1mo` K 线与复权因子查询。周/月线由本地
日 K 聚合，`1m` 和 `5m` 读取各自独立的日内 Parquet 目录；它不会抓取上游数据、不会写入数据湖，
也不会修改任何现有文件。

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

交互式接口文档位于 `http://127.0.0.1:8790/docs`，健康检查为 `/healthz`。
参数、复权公式、分页、错误码和请求示例以 [完整 API 文档](docs/API.md) 为准；新增或变更
接口时需要同步更新该文档，并由代理测试校验路由清单。

## 性能与运行边界

- 先按 `trade_date=` 分区目录选择日 K、日内 K 和复权因子文件；证券主数据只打开固定 canonical 文件，避免全湖递归 glob。
- SQL 只投影接口所需列，并将 symbol、日期、名称搜索和页大小过滤推给 DuckDB。
- 日/周/月默认单次窗口最多 3660 天；`1m` 默认最多 31 天、`5m` 默认最多 90 天，均可用环境变量调整。单次最多打开 6000 个 Parquet 文件。
- 周/月线先逐日复权再聚合；日内线不在请求期内从另一个频率重采样，避免额外扫描和语义偏差。
- 进程内 LRU 缓存默认保存 15 秒，分区目录索引也以相同 TTL 刷新；这使日更后最多短暂延迟，不会向数据湖写缓存文件。
- 默认最多 4 个未命中查询同时扫描；超过时立即返回 `429`，避免突发请求拖慢本地磁盘和采集任务。

可配置环境变量：

| 变量 | 默认值 | 作用 |
| --- | --- | --- |
| `CNEQUITY_PROXY_DATA_ROOT` | 必填 | 数据湖根目录 |
| `CNEQUITY_PROXY_API_KEY` | 空 | Bearer Token；对外绑定时必填 |
| `CNEQUITY_PROXY_HOST` | `127.0.0.1` | 监听地址 |
| `CNEQUITY_PROXY_PORT` | `8790` | 监听端口 |
| `CNEQUITY_PROXY_DEFAULT_WINDOW_DAYS` | `365` | 未给日期时的默认窗口 |
| `CNEQUITY_PROXY_MAX_WINDOW_DAYS` | `3660` | 单次允许的最大日期跨度 |
| `CNEQUITY_PROXY_DEFAULT_MINUTE_WINDOW_DAYS` | `5` | `1m` 未给日期时的默认窗口 |
| `CNEQUITY_PROXY_MAX_MINUTE_WINDOW_DAYS` | `31` | `1m` 单次允许的最大日期跨度 |
| `CNEQUITY_PROXY_DEFAULT_5M_WINDOW_DAYS` | `20` | `5m` 未给日期时的默认窗口 |
| `CNEQUITY_PROXY_MAX_5M_WINDOW_DAYS` | `90` | `5m` 单次允许的最大日期跨度 |
| `CNEQUITY_PROXY_DEFAULT_LIMIT` | `1000` | 默认每页条数 |
| `CNEQUITY_PROXY_MAX_BARS` | `5000` | 单页最大条数 |
| `CNEQUITY_PROXY_MAX_FILES` | `6000` | 单次最多打开的 Parquet 文件数 |
| `CNEQUITY_PROXY_CACHE_TTL_SECONDS` | `15` | 结果与分区索引的 TTL；`0` 禁用结果缓存 |
| `CNEQUITY_PROXY_CACHE_ENTRIES` | `256` | LRU 最大条目数 |
| `CNEQUITY_PROXY_MAX_CONCURRENT_QUERIES` | `4` | 同时执行的缓存未命中查询数 |
| `CNEQUITY_PROXY_DUCKDB_THREADS` | `1` | 单条 DuckDB 查询可用线程数 |

## 测试

```bash
cd proxy
python -m pip install -e . --group dev
pytest
```
