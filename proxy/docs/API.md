# CNEquity Query Proxy API

版本：`v1`。这是一个独立、只读的 HTTP/JSON 服务，只读取本地 Parquet 数据湖中的下列
目录，不导入 `cnequity` 主包，也不读取其配置、元数据或数据库：

```text
{CNEQUITY_PROXY_DATA_ROOT}/curated/daily_bars/trade_date=YYYY-MM-DD/*.parquet
{CNEQUITY_PROXY_DATA_ROOT}/curated/minute_bars/trade_date=YYYY-MM-DD/*.parquet
{CNEQUITY_PROXY_DATA_ROOT}/curated/minute_bars_5m/trade_date=YYYY-MM-DD/*.parquet
{CNEQUITY_PROXY_DATA_ROOT}/curated/instruments/part-merged.parquet
{CNEQUITY_PROXY_DATA_ROOT}/derived/adj_factors/trade_date=YYYY-MM-DD/*.parquet
```

生产环境请通过 HTTPS 反向代理公开服务。默认监听 `127.0.0.1:8790`，交互式 OpenAPI
文档位于 `/docs`，机器可读规范位于 `/openapi.json`。

## 通用约定

- 所有日期使用 ISO 8601 格式：`YYYY-MM-DD`。
- `symbol` 使用 6 位代码和交易所后缀：`600519.SH`、`000001.SZ`、`430047.BJ`。大小写不敏感，
  响应统一使用大写。
- K 线与复权因子按时间正序返回；证券列表按 `symbol` 升序返回。`cursor` 是上一页的
  `next_cursor`；将它原样传回即可读取后续数据。
- `1d`、`1w`、`1mo` 省略 `start` 和 `end` 时，结束日为服务端当天，开始日为结束日前
  `CNEQUITY_PROXY_DEFAULT_WINDOW_DAYS` 个自然日，默认 365 天；单次窗口受
  `CNEQUITY_PROXY_MAX_WINDOW_DAYS` 限制，默认 3660 天。
- `1m` 默认窗口为 `CNEQUITY_PROXY_DEFAULT_MINUTE_WINDOW_DAYS`（默认 5 天），最大为
  `CNEQUITY_PROXY_MAX_MINUTE_WINDOW_DAYS`（默认 31 天）。`5m` 分别使用
  `CNEQUITY_PROXY_DEFAULT_5M_WINDOW_DAYS`（默认 20 天）和
  `CNEQUITY_PROXY_MAX_5M_WINDOW_DAYS`（默认 90 天）。
- 单页 `limit` 默认 1000，最大 5000，实际值可由运行配置调整。
- 所有业务端点响应 `application/json`。没有写入、更新或删除接口。

## 认证与缓存

服务配置 `CNEQUITY_PROXY_API_KEY` 后，除 `/healthz`、`/docs`、`/redoc`、`/openapi.json` 外，
所有接口均须携带：

```http
Authorization: Bearer <CNEQUITY_PROXY_API_KEY>
```

非回环地址启动时 API Key 必填。业务响应带有以下头：

| 响应头 | 含义 |
| --- | --- |
| `X-Cache: HIT` / `MISS` | 是否命中代理进程内 TTL LRU 缓存 |
| `Cache-Control` | 缓存启用时为 `private, max-age=<TTL>`；TTL 为 0 时为 `no-store` |

## 路由清单

下表是受自动化测试约束的路由清单。新增、删除或改名业务路由时必须同步更新此表和对应章节。

| 方法和路径 | 认证 | 说明 |
| --- | --- | --- |
| `GET /healthz` | 否 | 服务存活探针 |
| `GET /v1/instruments` | 是（配置 Key 时） | 查询证券基础信息与代码 |
| `GET /v1/kline/{symbol}` | 是（配置 Key 时） | 查询原始、前复权或后复权的日、日内、周、月 K |
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
`1w`、`1mo` K 与因子接口的游标是日期；`1m`、`5m` K 的游标是 `bar_time`，即无时区
`Asia/Shanghai` ISO 8601 时间戳；证券接口的游标是 `symbol`。例如：

```text
GET /v1/kline/600519.SH?start=2025-01-01&end=2025-12-31&limit=500
-> next_cursor = 2025-06-30

GET /v1/kline/600519.SH?start=2025-01-01&end=2025-12-31&limit=500&cursor=2025-06-30

GET /v1/kline/600519.SH?interval=5m&start=2025-01-05&end=2025-01-05&limit=500
-> next_cursor = 2025-01-05T10:15:00

GET /v1/kline/600519.SH?interval=5m&start=2025-01-05&end=2025-01-05&limit=500&cursor=2025-01-05T10:15:00
```

## 错误响应

错误统一为：

```json
{"detail":"可读的错误说明"}
```

| 状态码 | 场景 |
| --- | --- |
| `401` | API Key 已配置但未携带或携带了错误的 Bearer Token |
| `409` | 严格 K 线复权缺因子，或前复权因子的基准日因子不存在 |
| `413` | 本次查询需打开的 Parquet 文件数超过 `CNEQUITY_PROXY_MAX_FILES` |
| `422` | 代码、日期、窗口、游标、页大小、证券筛选或复权参数不合法 |
| `429` | 未命中缓存的扫描数超过 `CNEQUITY_PROXY_MAX_CONCURRENT_QUERIES`；含 `Retry-After: 1` |
| `503` | 所需本地数据湖目录或证券主文件不存在、文件正在切换或当前不可读 |

## 数据与性能边界

- 日 K、周/月聚合、日内 K 与复权因子均先按 `trade_date=` 目录剪裁文件；证券主数据只打开固定 canonical 文件，均不递归扫描整个数据湖。
- `1m` 与 `5m` 分别使用独立的短窗口上限和数据目录；周/月聚合复用日 K 的窗口及文件预算，并在 DuckDB 内以单次扫描完成逐日复权和聚合。
- SQL 只读取接口需要的列，且使用参数化查询。
- 全部 K 线、复权因子与证券主数据共享未命中缓存的并发扫描额度，避免并发请求拖慢采集任务。
- 查询结果和分区索引均使用进程内 TTL 缓存；代理绝不向数据湖写入缓存或其他文件。

## 接口变更维护

新增、删除或修改对外接口时，必须在同一变更中：

1. 更新本文件的路由清单、参数表、错误行为和至少一个请求/响应示例。
2. 更新对应的 API 测试。
3. 运行代理测试。`test_api_documentation.py` 会校验路由清单、接口章节及全部 path/query
   参数名与实际业务路由完全一致；未同步文档将导致测试失败。
