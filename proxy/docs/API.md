# CNEquity Query Proxy API

版本：`v1`。这是一个独立、只读的 HTTP 服务，只读取本地 Parquet 数据湖中的下列
目录，不导入 `cnequity` 主包，也不读取其配置、元数据或数据库：

```text
{CNEQUITY_PROXY_DATA_ROOT}/curated/daily_bars/trade_date=YYYY-MM-DD/*.parquet
{CNEQUITY_PROXY_DATA_ROOT}/curated/minute_bars/trade_date=YYYY-MM-DD/*.parquet
{CNEQUITY_PROXY_DATA_ROOT}/curated/minute_bars_5m/trade_date=YYYY-MM-DD/*.parquet
{CNEQUITY_PROXY_DATA_ROOT}/curated/trading_calendar/trade_date=YYYY/*.parquet
{CNEQUITY_PROXY_DATA_ROOT}/curated/trading_status/trade_date=YYYY-MM/*.parquet
{CNEQUITY_PROXY_DATA_ROOT}/curated/dragon_tiger/trade_date=YYYY-MM/*.parquet
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

生产环境请通过 HTTPS 反向代理公开服务。默认监听 `127.0.0.1:8790`，交互式 OpenAPI
文档位于 `/docs`，机器可读规范位于 `/openapi.json`。

若反向代理以路径前缀公开服务，并在转发前剥离该前缀，运行 Proxy 时必须设置
`CNEQUITY_PROXY_ROOT_PATH`。当前部署使用
`https://cnequity.codingdie.com/query`，因此设置 `CNEQUITY_PROXY_ROOT_PATH=/query`；文档地址为
`https://cnequity.codingdie.com/query/docs`，机器可读规范为
`https://cnequity.codingdie.com/query/openapi.json`。下文路由清单中的路径是 Proxy 收到的内部路径，
对外访问时在其前加上 `/query`。

Python 调用方可使用独立的 [CNEquity Query SDK](../../sdk/README.md)，获取与本文相同的类型化
查询能力，而无需直接处理 JSON、分页游标或 HTTP 状态码。

## 通用约定

- 所有日期使用 ISO 8601 格式：`YYYY-MM-DD`。
- `symbol` 使用 6 位代码和交易所后缀：`600519.SH`、`000001.SZ`、`430047.BJ`。大小写不敏感，
  响应统一使用大写。
- K 线、复权因子与交易状态按时间正序返回；证券列表按 `symbol` 升序返回。`cursor` 是上一页的
  `next_cursor`；将它原样传回即可读取后续数据。
- `1d`、`1w`、`1mo` 与交易状态 JSON 查询省略 `start` 和 `end` 时，结束日为服务端当天，开始日为
  结束日前 `CNEQUITY_PROXY_DEFAULT_WINDOW_DAYS` 个自然日，默认 365 天；单次窗口受
  `CNEQUITY_PROXY_MAX_WINDOW_DAYS` 限制，默认 3660 天。全市场批量下载必须传入日期，且不受该
  窗口上限限制。
- `1m` 默认窗口为 `CNEQUITY_PROXY_DEFAULT_MINUTE_WINDOW_DAYS`（默认 5 天），最大为
  `CNEQUITY_PROXY_MAX_MINUTE_WINDOW_DAYS`（默认 31 天）。`5m` 分别使用
  `CNEQUITY_PROXY_DEFAULT_5M_WINDOW_DAYS`（默认 20 天）和
  `CNEQUITY_PROXY_MAX_5M_WINDOW_DAYS`（默认 90 天）。
- 单页 `limit` 默认 1000，最大 5000，实际值可由运行配置调整。
- 除全市场日线、复权因子、交易日历、交易状态与龙虎榜批量下载外，业务端点响应
  `application/json`。批量下载响应 `application/x-tar`，其中是原始 Parquet 文件。没有写入、更新或删除接口。

## 认证与缓存

服务配置 `CNEQUITY_PROXY_API_KEY` 后，除 `/healthz`、`/docs`、`/redoc`、`/openapi.json` 外，
所有接口均须携带：

```http
Authorization: Bearer <CNEQUITY_PROXY_API_KEY>
```

非回环地址启动时 API Key 必填。业务响应带有以下头：

| 响应头 | 含义 |
| --- | --- |
| `X-Cache: HIT` / `MISS` / `BYPASS` | 是否命中代理进程内 TTL LRU 缓存；批量下载恒为 `BYPASS` |
| `Cache-Control` | JSON 查询在缓存启用时为 `private, max-age=<TTL>`；批量下载与 TTL 为 0 的查询均为 `no-store` |

## 路由清单

下表是受自动化测试约束的路由清单。新增、删除或改名业务路由时必须同步更新此表和对应章节。

| 方法和路径 | 认证 | 说明 |
| --- | --- | --- |
| `GET /healthz` | 否 | 服务存活探针 |
| `GET /v1/instruments` | 是（配置 Key 时） | 查询证券基础信息与代码 |
| `GET /v1/trading-calendar/batch` | 是（配置 Key 时） | 流式下载原始交易日历 Parquet |
| `GET /v1/trading-status/batch` | 是（配置 Key 时） | 流式下载全市场原始交易状态 Parquet |
| `GET /v1/trading-status/{symbol}` | 是（配置 Key 时） | 查询证券每日交易状态与风险警示 |
| `GET /v1/stocks/{symbol}/summary` | 是（配置 Key 时） | 查询单只证券的轻量当前摘要 |
| `GET /v1/dragon-tiger/batch` | 是（配置 Key 时） | 流式下载全市场原始龙虎榜 Parquet |
| `GET /v1/valuation-metrics/batch` | 是（配置 Key 时） | 流式下载全市场原始估值指标 Parquet |
| `GET /v1/sector-members/batch` | 是（配置 Key 时） | 流式下载原始板块成分历史快照 Parquet |
| `GET /v1/kline/batch` | 是（配置 Key 时） | 流式下载全市场原始日线 Parquet |
| `GET /v1/kline/{symbol}` | 是（配置 Key 时） | 查询原始、前复权或后复权的日、日内、周、月 K |
| `GET /v1/adjustment-factors/batch` | 是（配置 Key 时） | 流式下载全市场原始后复权因子 Parquet |
| `GET /v1/adjustment-factors/{symbol}` | 是（配置 Key 时） | 查询前复权或后复权因子 |

## 健康检查

### `GET /healthz`

不检查数据湖可读性，也不要求认证。适用于负载均衡存活探针。

```json
{"status":"ok"}
```

## 证券基础信息

### `GET /v1/instruments`

查询本地证券主数据，适合在查询 K 线前发现或校验代码。默认保留已退市证券，避免因为默认
筛选引入幸存者偏差。当前读取固定的
`curated/instruments/part-merged.parquet`，不会递归扫描整个 `curated` 目录。

| 参数 | 位置 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- | --- |
| `symbol` | query | string | 否 | - | 精确匹配 6 位代码和交易所后缀，大小写不敏感 |
| `q` | query | string | 否 | - | 代码或证券名称的大小写不敏感包含搜索，长度 1 到 64 |
| `exchange` | query | `SH` / `SZ` / `BJ` | 否 | - | 交易所，大小写不敏感 |
| `asset_type` | query | string | 否 | - | 资产类型精确匹配，例如 `stock`、`etf`、`index`；大小写不敏感 |
| `as_of` | query | date | 否 | - | 仅返回当日已上市且尚未退市的记录；`list_date` 缺失时视为不限制上市日 |
| `cursor` | query | string | 否 | - | 上一页的 `next_cursor`，即一个 `symbol` |
| `limit` | query | integer | 否 | 1000 | 每页记录数，范围 1 到运行配置的上限 |

`as_of` 仅根据 `list_date` 与 `delist_date` 过滤，不读取 `trading_status`，因此不等同于「当日
可交易股票池」，也不会排除停牌或风险警示证券。筛选条件是：

```text
(list_date is null or list_date <= as_of)
and (delist_date is null or delist_date >= as_of)
```

按名称搜索：

```bash
curl -H "Authorization: Bearer $CNEQUITY_PROXY_API_KEY" \
  "https://proxy.example/v1/instruments?q=%E8%8C%85%E5%8F%B0&asset_type=stock"
```

查询指定日期在市的沪市股票：

```bash
curl -H "Authorization: Bearer $CNEQUITY_PROXY_API_KEY" \
  "https://proxy.example/v1/instruments?exchange=SH&asset_type=stock&as_of=2025-01-31"
```

成功响应示例：

```json
{
  "instruments": [
    {
      "symbol": "600519.SH",
      "name": "贵州茅台",
      "exchange": "SH",
      "asset_type": "stock",
      "list_date": "2001-08-27",
      "delist_date": null,
      "prev_symbol": null
    }
  ],
  "as_of": "2025-01-31",
  "next_cursor": null
}
```

## 交易日历

### `GET /v1/trading-calendar/batch`

一次下载给定日期窗口重叠年份的 `trading_calendar` 原始 Parquet 文件。该接口不使用 DuckDB，不转成
JSON，也不推断缺失交易日；服务只选择已有的
`curated/trading_calendar/trade_date=YYYY/*.parquet` 文件，并以流式 TAR 返回。

| 参数 | 位置 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- | --- |
| `start` | query | date | 是 | - | 闭区间开始日，用于选择重叠年分区 |
| `end` | query | date | 是 | - | 闭区间结束日，用于选择重叠年分区 |

`trading_calendar` 的物理分区是年，不是日。因此即使只请求一天，也会下载该日所在的**整年**原始
Parquet；客户端应在解压后按文件内的日级 `trade_date` 列做精确过滤。这保留了数据湖文件的原始字节，
避免服务端重编码。

```bash
curl -L -H "Authorization: Bearer $CNEQUITY_PROXY_API_KEY" \
  "https://proxy.example/v1/trading-calendar/batch?start=2025-01-01&end=2025-12-31" \
  --output cnequity-trading-calendar-2025.tar
```

## 交易状态

### `GET /v1/trading-status/batch`

一次下载给定日期窗口重叠月份的全市场 `trading_status` 原始 Parquet 文件。该接口不使用
DuckDB，不转成 JSON，不做代码筛选、字段投影或状态推断；服务只选择已有的
`curated/trading_status/trade_date=YYYY-MM/*.parquet` 文件，并以流式 TAR 返回。

| 参数 | 位置 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- | --- |
| `start` | query | date | 是 | - | 闭区间开始日，用于选择重叠月分区 |
| `end` | query | date | 是 | - | 闭区间结束日，用于选择重叠月分区 |

`trading_status` 的物理分区是月，不是日。因此例如请求一天也会下载该日所在的**整月**原始
Parquet；客户端应在解压后按文件内的日级 `trade_date` 列做精确过滤。这个边界是刻意保留原始
文件、避免服务端重编码的结果。

```bash
curl -L -H "Authorization: Bearer $CNEQUITY_PROXY_API_KEY" \
  "https://proxy.example/v1/trading-status/batch?start=2025-01-01&end=2025-12-31" \
  --output cnequity-trading-status-2025.tar
```

### `GET /v1/trading-status/{symbol}`

查询证券在本地数据湖中已有的每日交易状态。服务只读取与窗口重叠的
`curated/trading_status/trade_date=YYYY-MM/*.parquet` 月分区，不读取证券主数据或主项目的
任何运行时组件。

| 参数 | 位置 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- | --- |
| `symbol` | path | string | 是 | - | 股票或北交所代码，见通用约定 |
| `start` | query | date | 否 | 动态窗口 | 闭区间开始日 |
| `end` | query | date | 否 | 当天 | 闭区间结束日 |
| `cursor` | query | date | 否 | - | 上一页的 `next_cursor`，不得早于 `start` |
| `status` | query | `normal` / `suspended` / `delisted` | 否 | - | 精确匹配交易状态，大小写不敏感 |
| `is_trading` | query | boolean | 否 | - | 精确匹配数据湖记录的当日可交易标记 |
| `risk_warning` | query | boolean | 否 | - | 精确匹配 ST/*ST 风险警示标记；不会匹配 `null` |
| `limit` | query | integer | 否 | 1000 | 每页状态记录数，范围 1 到运行配置的上限 |

每条 `statuses` 记录包含：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `trade_date` | date | 状态所对应的交易日 |
| `is_trading` | boolean | 数据湖记录的该日是否可交易 |
| `status` | `normal` / `suspended` / `delisted` | 交易状态；风险警示不编码在这个字段中 |
| `risk_warning` | boolean / null | 是否带 ST/*ST 风险警示；`null` 表示数据湖没有该事实的证据 |

`status` 与 `risk_warning` 是正交事实。例如，`status=suspended` 且 `risk_warning=true` 表示风险
警示证券当日停牌。筛选 `risk_warning=false` 时只返回显式为 `false` 的记录，`null` 不会被当作
`false`。

没有状态记录不代表 `normal`，而是该日期没有可用的状态事实。调用方若需要构造完整证券池，
应先明确自己的缺失值处理规则，不能由本接口凭空补全为可交易。

查询风险警示停牌记录：

```bash
curl -H "Authorization: Bearer $CNEQUITY_PROXY_API_KEY" \
  "https://proxy.example/v1/trading-status/600519.SH?start=2025-01-01&end=2025-01-31&status=suspended&risk_warning=true"
```

成功响应示例：

```json
{
  "symbol": "600519.SH",
  "start": "2025-01-01",
  "end": "2025-01-31",
  "statuses": [
    {
      "trade_date": "2025-01-16",
      "is_trading": false,
      "status": "suspended",
      "risk_warning": true
    }
  ],
  "next_cursor": null
}
```

## 单股摘要

### `GET /v1/stocks/{symbol}/summary`

一次返回单只证券的轻量当前事实，适合股票详情页或研究 agent 的首屏上下文。服务首先校验
`curated/instruments/part-merged.parquet` 中存在该证券，再只打开每个白名单数据集的**最新分区**；
不会回扫旧分区寻找历史记录，也不会联网补数。

| 参数 | 位置 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- | --- |
| `symbol` | path | string | 是 | - | 股票或北交所代码，见通用约定 |

响应分为以下轻量模块：

| 模块 | 内容 |
| --- | --- |
| `instrument` | 证券主数据及其溯源 |
| `market` | 最新可用日级行情快照、交易状态、估值 |
| `classification` | 当前行业、东财板块、指数成分归属 |
| `signals` | 资金流、分析师一致预期、人气榜、情绪分数 |

每条已返回事实都带自己的日期字段、`source`、`data_version` 和 UTC `fetched_at`；模块间的日期
不保证相同。某数据集目录不存在或尚无 Parquet 文件时，对应值为 `null` 或空数组，且其名称会列入
`unavailable_datasets`。一个可用数据集内没有该证券的行只表示当前分区没有该事实，不会被当作零值
或正常状态。

`index_memberships[].weight` 在数据源未提供权重时为 `null`。当前入湖的成分源会以 `0` 作为内部
占位，因此接口特意不把它暴露为“零权重”。

`market.latest_market` 最多返回一条最新可用日级 OHLCV 行情快照；它不是 K 线序列，也不承诺
实时盘口语义。日/分钟 K 历史和复权结果应使用 `/v1/kline/{symbol}`。

摘要刻意不包含日/分钟 K 历史、复权因子历史、财报长表、股东明细、公告新闻事件流，以及
`industry_index`、`sector_bars`。这些数据具有较大的历史体量或不能安全地按现有编码直接关联，
应使用专门的明细接口。

请求示例：

```bash
curl -H "Authorization: Bearer $CNEQUITY_PROXY_API_KEY" \
  "https://proxy.example/v1/stocks/600519.SH/summary"
```

成功响应示例：

```json
{
  "symbol": "600519.SH",
  "instrument": {
    "symbol": "600519.SH",
    "name": "贵州茅台",
    "exchange": "SH",
    "asset_type": "stock",
    "list_date": "2001-08-27",
    "delist_date": null,
    "prev_symbol": null,
    "provenance": {
      "source": "eastmoney",
      "data_version": "v1",
      "fetched_at": "2026-09-07T09:10:00Z"
    }
  },
  "market": {
    "latest_market": {
      "trade_date": "2026-09-04",
      "open": 1420.0,
      "high": 1442.0,
      "low": 1417.0,
      "close": 1436.0,
      "volume": 2134567,
      "amount": 3065000000.0,
      "provenance": {
        "source": "tdx_protocol",
        "data_version": "v2",
        "fetched_at": "2026-09-04T07:10:00Z"
      }
    },
    "trading_status": null,
    "valuation": null
  },
  "classification": {
    "industries": [],
    "sectors": [],
    "index_memberships": [
      {
        "index_symbol": "000300.SH",
        "as_of_date": "2026-09-07",
        "weight": null,
        "provenance": {
          "source": "eastmoney",
          "data_version": "v1",
          "fetched_at": "2026-09-07T09:10:00Z"
        }
      }
    ]
  },
  "signals": {
    "fund_flow": null,
    "analyst_consensus": null,
    "hot_rank": null,
    "sentiments": []
  },
  "unavailable_datasets": []
}
```

## 全市场批量 Parquet 下载

批量接口面向本地研究湖同步，而非行级 JSON 查询：服务只按分区目录选择白名单数据集中的原始
Parquet 文件，并以流式 TAR 返回。它们不使用 DuckDB，不做代码筛选、复权、重采样或字段投影；
归档内保留从数据湖根目录开始的相对路径，因此可直接解压到另一个数据湖根目录。

成功响应的 `Content-Type` 为 `application/x-tar`，并带有：

| 响应头 | 含义 |
| --- | --- |
| `Content-Disposition` | 建议的 `.tar` 文件名 |
| `X-CNEQUITY-Data-Files` | 归档中原始 Parquet 文件数 |
| `X-CNEQUITY-Data-Bytes` | 原始 Parquet 字节总数，不含 TAR 容器开销 |

五个批量接口不进入结果缓存，也不占用普通查询的并发扫描额度。代理不对它们施加日期跨度、文件数、
原始字节数或并发下载额度；只校验 `start <= end`，并流式返回实际存在的白名单分区。

### `GET /v1/kline/batch`

一次下载给定日期窗口内的全市场 `daily_bars` 原始 Parquet 文件，适合数百个交易日的研究数据拉取。
服务只选择已有的 `curated/daily_bars/trade_date=.../*.parquet` 日分区；日线是未复权的原始价格。

| 参数 | 位置 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- | --- |
| `start` | query | date | 是 | - | 闭区间开始日 |
| `end` | query | date | 是 | - | 闭区间结束日 |

示例：

```bash
curl -L -H "Authorization: Bearer $CNEQUITY_PROXY_API_KEY" \
  "https://proxy.example/v1/kline/batch?start=2025-01-01&end=2025-12-31" \
  --output cnequity-daily-bars-2025.tar

tar -tf cnequity-daily-bars-2025.tar
tar -xf cnequity-daily-bars-2025.tar -C /research/lake
```

## 龙虎榜

### `GET /v1/dragon-tiger/batch`

一次下载给定日期窗口重叠月份的全市场 `dragon_tiger` 原始 Parquet 文件。该接口不使用 DuckDB，
不转成 JSON，不做代码筛选或字段投影；服务只选择已有的
`curated/dragon_tiger/trade_date=YYYY-MM/*.parquet` 文件，并以流式 TAR 返回。

| 参数 | 位置 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- | --- |
| `start` | query | date | 是 | - | 闭区间开始日，用于选择重叠月分区 |
| `end` | query | date | 是 | - | 闭区间结束日，用于选择重叠月分区 |

`dragon_tiger` 的物理分区是月，不是日。因此例如请求一天也会下载该日所在的**整月**原始
Parquet；客户端应在解压后按文件内的 `trade_date` 列做精确过滤。

```bash
curl -L -H "Authorization: Bearer $CNEQUITY_PROXY_API_KEY" \
  "https://proxy.example/v1/dragon-tiger/batch?start=2025-01-01&end=2025-12-31" \
  --output cnequity-dragon-tiger-2025.tar
```

## K 线

### `GET /v1/kline/{symbol}`

查询未复权或复权后的 `1d`、`1m`、`5m`、`1w`、`1mo` K 线。所有周期使用同一个入口；
复权规则和 `base_date` 契约完全一致。

| 参数 | 位置 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- | --- |
| `symbol` | path | string | 是 | - | 股票或北交所代码，见通用约定 |
| `start` | query | date | 否 | 动态窗口 | 闭区间开始日 |
| `end` | query | date | 否 | 当天 | 闭区间结束日 |
| `cursor` | query | string | 否 | - | 上一页的 `next_cursor`，不得早于 `start`；日/周/月线是日期，日内线是无时区时间戳 |
| `limit` | query | integer | 否 | 1000 | 每页 K 线数，范围 1 到运行配置的上限 |
| `interval` | query | `1d` / `1m` / `5m` / `1w` / `1mo` | 否 | `1d` | K 线周期，详见下文 |
| `adjustment` | query | `none` / `hfq` / `qfq` | 否 | `none` | 复权方式 |
| `adjust` | query | `none` / `hfq` / `qfq` | 否 | - | 已废弃的 `adjustment` 别名，不能与其冲突 |
| `base_date` | query | date | 条件必填 | - | `adjustment=qfq` 时必填；其他方式禁止传入 |
| `strict_adjustment` | query | boolean | 否 | `true` | 缺少交易日因子时是否拒绝响应 |

复权计算使用本地持久化的后复权因子 `F_hfq`：

```text
none:  P(t) = P_raw(t)
hfq:   P(t) = P_raw(t) * F_hfq(t)
qfq:   P(t) = P_raw(t) * F_hfq(t) / F_hfq(base_date)
```

`qfq` 的 `base_date` 可在查询窗口外。严格模式下，基准日或任一返回交易日缺少因子时响应
`409`；非严格模式将该行保留为原始价格，且 `adjustment_exact=false`。对于周/月线，任一所含
交易日缺因子都会令整个周期的 `adjustment_exact=false`。

### 周期与数据源

| `interval` | 数据源 | 返回时间字段 | 聚合规则 | 游标 |
| --- | --- | --- | --- | --- |
| `1d` | `curated/daily_bars` | `trade_date` | 不聚合 | `YYYY-MM-DD` |
| `1m` | `curated/minute_bars` | `trade_date`、`bar_time` | 不聚合 | 无时区 ISO 8601 时间戳 |
| `5m` | `curated/minute_bars_5m` | `trade_date`、`bar_time` | 不聚合 | 无时区 ISO 8601 时间戳 |
| `1w` | `curated/daily_bars` | `period_start`、`period_end`、`trade_date` | 周一至周日 | `trade_date` |
| `1mo` | `curated/daily_bars` | `period_start`、`period_end`、`trade_date` | 自然月 | `trade_date` |

`1m` 与 `5m` 的 `bar_time` 是无时区的 `Asia/Shanghai` 墙钟收盘时间。例如 `09:35:00` 的
5 分钟 bar 覆盖 09:30 至 09:35。日内数据集分别独立存储，服务不会为 `5m` 扫描 `1m` 目录或
在查询时由 1 分钟数据重采样。

`1w` 和 `1mo` 先对每一个日 K 应用 `hfq` 或 `qfq`，再聚合：`open` 为首个交易日的开盘价，
`high`/`low` 为全部已复权日 K 的最大/最小值，`close` 为最后交易日的收盘价，`volume` 与
`amount` 为求和。因此它们不是把聚合后的原始 OHLC 再乘一个单独因子。

周/月响应中的 `period_start` 和 `period_end` 是自然周或自然月的日历边界；`trade_date` 是该根
K 实际包含的最后交易日，也是分页游标。窗口恰好切在自然周期中间时，首尾 K 只使用落在请求
窗口内的日 K，仍保留完整的日历周期边界。周/月 `adjustment_factor` 是最后交易日实际使用的
因子，仅供追溯；OHLC 使用的是所含各交易日自己的因子。

原始日 K：

```bash
curl -H "Authorization: Bearer $CNEQUITY_PROXY_API_KEY" \
  "https://proxy.example/v1/kline/600519.SH?start=2025-01-01&end=2025-01-31"
```

指定基准日的前复权日 K：

```bash
curl -H "Authorization: Bearer $CNEQUITY_PROXY_API_KEY" \
  "https://proxy.example/v1/kline/600519.SH?adjustment=qfq&base_date=2025-01-31"
```

5 分钟前复权 K：

```bash
curl -H "Authorization: Bearer $CNEQUITY_PROXY_API_KEY" \
  "https://proxy.example/v1/kline/600519.SH?interval=5m&start=2025-01-05&end=2025-01-05&adjustment=qfq&base_date=2025-01-05"
```

后复权周线：

```bash
curl -H "Authorization: Bearer $CNEQUITY_PROXY_API_KEY" \
  "https://proxy.example/v1/kline/600519.SH?interval=1w&start=2025-01-01&end=2025-03-31&adjustment=hfq"
```

成功响应示例：

```json
{
  "symbol": "600519.SH",
  "interval": "1d",
  "adjustment": "qfq",
  "strict_adjustment": true,
  "start": "2025-01-01",
  "end": "2025-01-31",
  "candles": [
    {
      "trade_date": "2025-01-02",
      "open": 1410.5,
      "high": 1423.0,
      "low": 1408.2,
      "close": 1418.0,
      "volume": 1234567,
      "amount": 1750000000.0,
      "adjustment_factor": 0.98,
      "adjustment_exact": true
    }
  ],
  "base_date": "2025-01-31",
  "base_factor_date": "2025-01-31",
  "next_cursor": null
}
```

未复权时 `adjustment_factor` 与 `adjustment_exact` 均为 `null`。`next_cursor=null` 表示已到达
当前窗口末尾。日内 K 的 candle 额外包含 `bar_time`；周/月 K 的 candle 则使用如下结构：

```json
{
  "period_start": "2025-01-06",
  "period_end": "2025-01-12",
  "trade_date": "2025-01-10",
  "open": 1400.0,
  "high": 1430.0,
  "low": 1390.0,
  "close": 1425.0,
  "volume": 5200000,
  "amount": 7400000000.0,
  "adjustment_factor": 1.02,
  "adjustment_exact": true
}
```

## 复权因子

### `GET /v1/adjustment-factors/batch`

一次下载给定日期窗口内的全市场 `adj_factors` 原始 Parquet 文件。服务只选择已有的
`derived/adj_factors/trade_date=YYYY-MM-DD/*.parquet` 日分区；归档中的因子是湖内持久化的
后复权因子 `hfq`，不会在服务端按 `base_date` 计算前复权因子。

| 参数 | 位置 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- | --- |
| `start` | query | date | 是 | - | 闭区间开始日 |
| `end` | query | date | 是 | - | 闭区间结束日 |

研究端把该归档与同窗口的 `/v1/kline/batch` 日线按 `(symbol, trade_date)` 关联：后复权价格为
`P_raw(t) * F_hfq(t)`；以前复权基准日 `b` 为准时，使用 `P_raw(t) * F_hfq(t) / F_hfq(b)`。
这样基准日和复权结果属于研究输入，而不是服务端不可追溯的批量派生结果。

```bash
curl -L -H "Authorization: Bearer $CNEQUITY_PROXY_API_KEY" \
  "https://proxy.example/v1/adjustment-factors/batch?start=2025-01-01&end=2025-12-31" \
  --output cnequity-adjustment-factors-2025.tar
```

### `GET /v1/adjustment-factors/{symbol}`

查询可直接乘到原始 OHLC 的复权因子。`hfq` 返回数据湖中持久化的累计因子；`qfq` 在服务端
按调用方选择的基准日归一化。

| 参数 | 位置 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- | --- |
| `symbol` | path | string | 是 | - | 股票或北交所代码，见通用约定 |
| `start` | query | date | 否 | 动态窗口 | 闭区间开始日 |
| `end` | query | date | 否 | 当天 | 闭区间结束日 |
| `cursor` | query | date | 否 | - | 上一页的 `next_cursor`，不得早于 `start` |
| `limit` | query | integer | 否 | 1000 | 每页因子数，范围 1 到运行配置的上限 |
| `adjustment` | query | `hfq` / `qfq` | 否 | `hfq` | 返回的因子类型 |
| `adjust` | query | `hfq` / `qfq` | 否 | - | 已废弃的 `adjustment` 别名，不能与其冲突 |
| `base_date` | query | date | 条件必填 | - | `adjustment=qfq` 时必填；`hfq` 时禁止传入 |

```text
hfq factor: F_hfq(t)
qfq factor: F_hfq(t) / F_hfq(base_date)
```

`qfq` 基准日可在查询窗口外，但必须存在精确、正数且有限的本地后复权因子，否则返回 `409`。
该接口只返回已持久化的有效因子行，不对缺失交易日填充 `1.0`。

后复权因子：

```bash
curl -H "Authorization: Bearer $CNEQUITY_PROXY_API_KEY" \
  "https://proxy.example/v1/adjustment-factors/600519.SH?start=2025-01-01&end=2025-01-31"
```

以前一交易日为基准的前复权因子：

```bash
curl -H "Authorization: Bearer $CNEQUITY_PROXY_API_KEY" \
  "https://proxy.example/v1/adjustment-factors/600519.SH?adjustment=qfq&base_date=2025-01-31"
```

成功响应示例：

```json
{
  "symbol": "600519.SH",
  "adjustment": "qfq",
  "start": "2025-01-01",
  "end": "2025-01-31",
  "factors": [
    {"trade_date": "2025-01-02", "factor": 0.98},
    {"trade_date": "2025-01-03", "factor": 0.98}
  ],
  "base_date": "2025-01-31",
  "base_factor_date": "2025-01-31",
  "next_cursor": null
}
```

## 分页

当响应中的 `next_cursor` 非 `null` 时，传入该值请求下一页，并保持原有筛选条件不变。`1d`、
`1w`、`1mo` K、复权因子与交易状态接口的游标是日期；`1m`、`5m` K 的游标是 `bar_time`，即无时区
`Asia/Shanghai` ISO 8601 时间戳；证券接口的游标是 `symbol`。例如：

```text
GET /v1/kline/600519.SH?start=2025-01-01&end=2025-12-31&limit=500
-> next_cursor = 2025-06-30

GET /v1/kline/600519.SH?start=2025-01-01&end=2025-12-31&limit=500&cursor=2025-06-30

GET /v1/kline/600519.SH?interval=5m&start=2025-01-05&end=2025-01-05&limit=500
-> next_cursor = 2025-01-05T10:15:00

GET /v1/kline/600519.SH?interval=5m&start=2025-01-05&end=2025-01-05&limit=500&cursor=2025-01-05T10:15:00

GET /v1/trading-status/600519.SH?start=2025-01-01&end=2025-12-31&status=suspended&limit=500
-> next_cursor = 2025-06-30

GET /v1/trading-status/600519.SH?start=2025-01-01&end=2025-12-31&status=suspended&limit=500&cursor=2025-06-30
```

## 错误响应

错误统一为：

```json
{"detail":"可读的错误说明"}
```

| 状态码 | 场景 |
| --- | --- |
| `401` | API Key 已配置但未携带或携带了错误的 Bearer Token |
| `404` | 单股摘要请求的证券不在证券主数据中，或任一批量接口的窗口没有可下载的 Parquet 文件 |
| `409` | 严格 K 线复权缺因子，或前复权因子的基准日因子不存在 |
| `413` | 非批量查询需打开的 Parquet 文件数超过 `CNEQUITY_PROXY_MAX_FILES` |
| `422` | 代码、日期、窗口、游标、页大小、证券筛选或复权参数不合法 |
| `429` | 未命中缓存的普通查询扫描数超过 `CNEQUITY_PROXY_MAX_CONCURRENT_QUERIES`；含 `Retry-After: 1` |
| `503` | 所需本地数据湖目录或证券主文件不存在、文件正在切换或当前不可读 |

## 数据与性能边界

- 日 K、周/月聚合、日内 K、复权因子与交易状态均先按 `trade_date=` 目录剪裁文件；交易日历按年分区，交易状态与龙虎榜按月分区，其他时序数据按日分区。证券主数据只打开固定 canonical 文件，均不递归扫描整个数据湖。
- 单股摘要只读取列出的十个轻量数据集的最新分区并共用一次磁盘查询额度；不会读取 `minute_bars`、`minute_bars_5m` 或任何历史长表分区。
- 全市场日线、后复权因子、交易日历、交易状态与龙虎榜批量下载直接逐块读取原始 Parquet，不做
  DuckDB 扫描，也不在内存中聚合全市场行。五者不使用普通查询额度，也没有代理层的日期、文件、字节或
  并发下载配额。
- `1m` 与 `5m` 分别使用独立的短窗口上限和数据目录；周/月聚合复用日 K 的窗口及文件预算，并在 DuckDB 内以单次扫描完成逐日复权和聚合。
- SQL 只读取接口需要的列，且使用参数化查询。
- 全部 K 线、复权因子、交易状态、证券主数据与单股摘要共享未命中缓存的并发扫描额度，避免并发请求拖慢采集任务。
- 查询结果和分区索引均使用进程内 TTL 缓存；代理绝不向数据湖写入缓存或其他文件。

## 接口变更维护

新增、删除或修改对外接口时，必须在同一变更中：

1. 更新本文件的路由清单、参数表、错误行为和至少一个请求/响应示例。
2. 更新对应的 API 测试。
3. 运行代理测试。`test_api_documentation.py` 会校验路由清单、接口章节及全部 path/query
   参数名与实际业务路由完全一致；未同步文档将导致测试失败。


## 估值指标历史批量下载

### `GET /v1/valuation-metrics/batch`

下载已有 `curated/valuation_metrics/trade_date=YYYY-MM-DD/*.parquet` 原始文件，
适合批量获取全市场流通市值、总市值及估值历史。日期按 `trade_date` 分区选择，包含起止日；
查某个最新交易日时，`start` 与 `end` 传同一天，不会自动寻找或替换为其他日期。

| 参数 | 位置 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- | --- |
| `start` | query | date | 是 | - | YYYY-MM-DD，闭区间开始日 |
| `end` | query | date | 是 | - | YYYY-MM-DD，闭区间结束日 |

响应沿用日线批量接口：`application/x-tar`，归档路径相对数据湖根目录，文件名为
`cnequity-valuation-metrics-{start}-{end}.tar`。保留原始 Parquet 字节及全部现有字段，
包括 `float_mv`、`total_mv`（单位：元）、`pe_ttm`、`pb`、`ps_ttm`、`source`、
`data_version`、`trade_date` 和 `fetched_at`；不投影、不重编码、不补值，缺失市值仍为空。
已有跨日采集记录的日期与采集时间原样返回，本接口不修正两者，也不触发采集或写湖。
归档仅代表已有数据，不保证窗口内全市场证券或字段齐全。

鉴权、错误处理及文件预算与现有批量接口一致：配置 API Key 时要求 Bearer Token；
参数缺失、日期格式/日期值无效或 start 晚于 end 返回 422；窗口无文件返回 404；
数据集目录不可用或准备文件失败返回 503。批量下载不使用普通查询的 `max_files`、
窗口天数及并发额度；保留受控文件路径检查和流式读取。
响应含 `X-CNEQUITY-Data-Files`、`X-CNEQUITY-Data-Bytes`、
`Cache-Control: no-store` 和 `X-Cache: BYPASS`。

```http
GET /v1/valuation-metrics/batch?start=2026-09-07&end=2026-09-07
Authorization: Bearer <API_KEY>
```

```bash
curl -L -H "Authorization: Bearer $CNEQUITY_PROXY_API_KEY" \
  "https://proxy.example/v1/valuation-metrics/batch?start=2026-09-07&end=2026-09-07" \
  --output cnequity-valuation-metrics-2026-09-07.tar
```

## 板块成分历史批量下载

### `GET /v1/sector-members/batch`

只读取 `curated/sector_members/as_of_date=YYYY-MM-DD/*.parquet`，沿用日线批量接口的
流式 TAR 格式、鉴权和响应头。归档保留相对数据湖根目录的路径、原始文件字节、全部字段
（包括 `source`、`data_version`、`fetched_at`），不筛选行或重编码。

| 参数 | 位置 | 类型 | 必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- | --- |
| `start` | query | date | 是 | - | YYYY-MM-DD，闭区间开始日 |
| `end` | query | date | 是 | - | YYYY-MM-DD，闭区间结束日 |
| `include_previous_snapshot` | query | boolean | 否 | false | 额外包含严格早于 start 的最近一个日快照的全部文件 |

“完整快照”指经过采集校验和 compact 门禁发布到 curated 的整个日分区；接口不读取
manifest 或质量元数据，不重新认证源侧覆盖，也不按板块拼接不同日期的成员。
只选择已有日分区，忽略无日期文件、月/年分区和没有 Parquet 文件的目录。

开启 `include_previous_snapshot` 后，即使窗口内没有快照，只要存在此前快照仍可返回 TAR。
若不存在此前快照，则仅返回窗口内文件；调用方应检查归档中的 `as_of_date`，不能假定
窗口第一天已有成员覆盖。窗口内文件和请求的此前快照都不存在时返回 404，数据集目录
不存在时返回 503，缺少或无效日期、start 晚于 end、无效布尔参数返回 422。

本接口不触发采集、不补齐缺失记录、不用未来快照代替历史快照。与日线批量下载一致，
不受普通查询窗口、文件数和并发额度限制；响应包含 `X-CNEQUITY-Data-Files`、
`X-CNEQUITY-Data-Bytes`、`Cache-Control: no-store` 和 `X-Cache: BYPASS`。

```bash
curl -L -H "Authorization: Bearer $CNEQUITY_PROXY_API_KEY" \
  "https://proxy.example/v1/sector-members/batch?start=2025-01-01&end=2025-12-31&include_previous_snapshot=true" \
  --output cnequity-sector-members-2025.tar
```
