# CNEquity Query SDK

`cnequity-query-sdk` 是 [CNEquity Query Proxy](../proxy/README.md) 的独立、同步 Python
客户端。它只通过 HTTP 调用 Proxy v1，不导入数据湖、FastAPI 服务端或主 `cnequity` 包。

## 安装

仓库内本地安装：

```bash
python -m pip install /path/to/cnequity/sdk
```

运行时唯一依赖为 `httpx`。Python 版本要求为 3.10 或以上。

## 快速开始

```python
from datetime import date

from cnequity_query_sdk import QueryClient

with QueryClient("https://proxy.example", api_key="your-token") as client:
    summary = client.get_stock_summary("600519.SH")
    print(summary.instrument.name)
    print(summary.market.latest_market.close)

    for candle in client.iter_kline(
        "600519.SH",
        start=date(2026, 1, 1),
        end=date(2026, 1, 31),
        adjustment="qfq",
        base_date=date(2026, 1, 31),
    ):
        print(candle.trade_date, candle.close)

    archive = client.download_market_daily_bars(
        "daily-bars-2026.tar",
        start=date(2026, 1, 1),
        end=date(2026, 6, 30),
    )
    factors = client.download_market_adjustment_factors(
        "adjustment-factors-2026.tar",
        start=date(2026, 1, 1),
        end=date(2026, 6, 30),
    )
    statuses = client.download_market_trading_status(
        "trading-status-2026.tar",
        start=date(2026, 1, 1),
        end=date(2026, 6, 30),
    )
    print(archive.path, archive.data_file_count)
```

`summary.market.latest_market` 是最新可用的一条日级 OHLCV 行情快照，不是 K 线序列或实时
盘口。日、日内、周和月 K 线应使用 `get_kline()` 或 `iter_kline()`。

## 查询方法

| 方法 | 对应路由 | 返回类型 |
| --- | --- | --- |
| `health()` | `GET /healthz` | `Health` |
| `list_instruments()` / `iter_instruments()` | `GET /v1/instruments` | `InstrumentPage` / `Instrument` |
| `get_trading_status()` / `iter_trading_status()` | `GET /v1/trading-status/{symbol}` | `TradingStatusPage` / `TradingStatus` |
| `get_stock_summary()` | `GET /v1/stocks/{symbol}/summary` | `StockSummary` |
| `download_market_daily_bars()` | `GET /v1/kline/batch` | `MarketDailyBarsDownload` |
| `download_market_adjustment_factors()` | `GET /v1/adjustment-factors/batch` | `AdjustmentFactorsDownload` |
| `download_market_trading_status()` | `GET /v1/trading-status/batch` | `TradingStatusDownload` |
| `get_kline()` / `iter_kline()` | `GET /v1/kline/{symbol}` | `KlinePage` / `DailyCandle`、`IntradayCandle` 或 `PeriodCandle` |
| `get_adjustment_factors()` / `iter_adjustment_factors()` | `GET /v1/adjustment-factors/{symbol}` | `AdjustmentFactorPage` / `AdjustmentFactor` |

所有分页迭代器会原样保留筛选条件和复权基准日。若服务端返回未推进的游标，SDK 会抛出
`ResponseDecodeError`，避免无穷循环。

三个 `download_market_*()` 方法都使用一个 HTTP 请求，将全市场原始 Parquet 文件流式写入 TAR。
目标文件已存在时默认抛出 `FileExistsError`；下载失败时会删除 SDK 自己创建的临时文件，不会留下
看似完整的归档。默认不施加单次下载超时；需要限制时传入 `timeout=`。

日线归档是未复权价格，复权因子归档是湖内持久化的后复权因子；客户端可按研究所选基准日计算前复权。
交易状态按月物理分区，日期窗口会下载重叠月份的完整原始文件，解压后应按文件内的 `trade_date` 列
做精确日期过滤。三个批量接口不设代理层的日期、文件数、原始字节或并发下载额度。

## 错误处理

网络、超时和 TLS 错误抛出 `TransportError`。服务端 HTTP 错误都继承 `APIError`，并按状态码
映射为 `AuthenticationError`、`NotFoundError`、`ConflictError`、`RequestTooLargeError`、
`RequestValidationError`、`RateLimitError` 或 `ServiceUnavailableError`。对于 `429`，可从
`RateLimitError.retry_after` 读取服务端的 `Retry-After` 秒数。

成功响应不符合公开 JSON 契约时抛出 `ResponseDecodeError`，不会把日期、时间或数值悄悄保留为
未校验的字典或字符串。
