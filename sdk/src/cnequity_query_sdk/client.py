"""CNEquity Query Proxy 的同步 Python 客户端。"""

from __future__ import annotations

import os
from collections.abc import Iterator, Mapping
from datetime import date, datetime
from pathlib import Path
from types import TracebackType
from typing import Any, TypeAlias, TypeVar
from urllib.parse import quote
from uuid import uuid4

import httpx

from cnequity_query_sdk.errors import (
    APIError,
    AuthenticationError,
    ConflictError,
    NotFoundError,
    RateLimitError,
    RequestTooLargeError,
    RequestValidationError,
    ResponseDecodeError,
    ServiceUnavailableError,
    TransportError,
)
from cnequity_query_sdk.models import (
    Adjustment,
    AdjustmentFactor,
    AdjustmentFactorPage,
    AdjustmentFactorsDownload,
    Candle,
    DragonTigerDownload,
    FactorAdjustment,
    Health,
    Instrument,
    InstrumentPage,
    KlineInterval,
    KlinePage,
    MarketDailyBarsDownload,
    ParquetArchiveDownload,
    StockSummary,
    TradingCalendarDownload,
    TradingState,
    TradingStatus,
    TradingStatusDownload,
    TradingStatusPage,
)

KlineCursor: TypeAlias = str | date | datetime
QueryParameter: TypeAlias = str | int | float | bool | date | datetime
_Download = TypeVar("_Download", bound=ParquetArchiveDownload)

_ERROR_TYPES: dict[int, type[APIError]] = {
    401: AuthenticationError,
    404: NotFoundError,
    409: ConflictError,
    413: RequestTooLargeError,
    422: RequestValidationError,
    429: RateLimitError,
    503: ServiceUnavailableError,
}


def _serialize_parameter(value: QueryParameter) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _query_parameters(**values: QueryParameter | None) -> dict[str, str]:
    return {
        name: _serialize_parameter(value) for name, value in values.items() if value is not None
    }


def _validate_limit(limit: int | None) -> None:
    if limit is not None and (isinstance(limit, bool) or limit < 1):
        raise ValueError("limit 必须是正整数")


def _validate_date_range(start: date, end: date) -> None:
    if (
        not isinstance(start, date)
        or isinstance(start, datetime)
        or not isinstance(end, date)
        or isinstance(end, datetime)
    ):
        raise ValueError("start 和 end 必须是 date")
    if start > end:
        raise ValueError("start 不能晚于 end")


def _validate_chunk_size(chunk_size: int) -> None:
    if isinstance(chunk_size, bool) or chunk_size < 1:
        raise ValueError("chunk_size 必须是正整数")


def _symbol_path(symbol: str) -> str:
    if not isinstance(symbol, str) or not symbol:
        raise ValueError("symbol 必须是非空字符串")
    return quote(symbol.upper(), safe=".")


class QueryClient:
    """访问 CNEquity Query Proxy v1 的同步、类型化客户端。

    Parameters
    ----------
    base_url:
        代理根地址，例如 ``https://proxy.example``。可包含反向代理路径前缀。
    api_key:
        可选的 Bearer Token。未设置时仍可访问未启用鉴权的代理和 ``health``。
    timeout:
        单次 HTTP 请求的超时配置，默认 10 秒。
    transport:
        可选的 httpx transport，主要用于测试或受控网络环境。
    """

    def __init__(
        self,
        base_url: str,
        *,
        api_key: str | None = None,
        timeout: float | httpx.Timeout | None = 10.0,
        transport: httpx.BaseTransport | None = None,
        headers: Mapping[str, str] | None = None,
    ):
        normalized_base_url = base_url.strip().rstrip("/")
        if not normalized_base_url:
            raise ValueError("base_url 不能为空")

        request_headers = {
            "Accept": "application/json",
            "User-Agent": "cnequity-query-sdk/0.1.0",
        }
        if headers is not None:
            request_headers.update(headers)
        if api_key:
            request_headers["Authorization"] = f"Bearer {api_key}"

        self._client = httpx.Client(
            base_url=f"{normalized_base_url}/",
            headers=request_headers,
            timeout=timeout,
            transport=transport,
        )

    def __enter__(self) -> QueryClient:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self.close()

    def close(self) -> None:
        """关闭底层 HTTP 连接池。"""
        self._client.close()

    def health(self) -> Health:
        """读取无需鉴权的服务存活状态。"""
        return Health.from_payload(self._get("healthz"))

    def list_instruments(
        self,
        *,
        symbol: str | None = None,
        query: str | None = None,
        exchange: str | None = None,
        asset_type: str | None = None,
        as_of: date | None = None,
        cursor: str | None = None,
        limit: int | None = None,
    ) -> InstrumentPage:
        """查询一页证券主数据。"""
        _validate_limit(limit)
        return InstrumentPage.from_payload(
            self._get(
                "v1/instruments",
                _query_parameters(
                    symbol=symbol.upper() if symbol is not None else None,
                    q=query,
                    exchange=exchange.upper() if exchange is not None else None,
                    asset_type=asset_type.lower() if asset_type is not None else None,
                    as_of=as_of,
                    cursor=cursor.upper() if cursor is not None else None,
                    limit=limit,
                ),
            )
        )

    def iter_instruments(
        self,
        *,
        symbol: str | None = None,
        query: str | None = None,
        exchange: str | None = None,
        asset_type: str | None = None,
        as_of: date | None = None,
        cursor: str | None = None,
        limit: int | None = None,
    ) -> Iterator[Instrument]:
        """按游标遍历匹配的全部证券主数据。"""
        current_cursor = cursor
        while True:
            page = self.list_instruments(
                symbol=symbol,
                query=query,
                exchange=exchange,
                asset_type=asset_type,
                as_of=as_of,
                cursor=current_cursor,
                limit=limit,
            )
            yield from page.instruments
            if page.next_cursor is None:
                return
            self._ensure_progress(current_cursor, page.next_cursor, "证券")
            current_cursor = page.next_cursor

    def get_trading_status(
        self,
        symbol: str,
        *,
        start: date | None = None,
        end: date | None = None,
        cursor: date | None = None,
        status: TradingState | None = None,
        is_trading: bool | None = None,
        risk_warning: bool | None = None,
        limit: int | None = None,
    ) -> TradingStatusPage:
        """查询一页证券交易状态。"""
        _validate_limit(limit)
        return TradingStatusPage.from_payload(
            self._get(
                f"v1/trading-status/{_symbol_path(symbol)}",
                _query_parameters(
                    start=start,
                    end=end,
                    cursor=cursor,
                    status=status,
                    is_trading=is_trading,
                    risk_warning=risk_warning,
                    limit=limit,
                ),
            )
        )

    def iter_trading_status(
        self,
        symbol: str,
        *,
        start: date | None = None,
        end: date | None = None,
        cursor: date | None = None,
        status: TradingState | None = None,
        is_trading: bool | None = None,
        risk_warning: bool | None = None,
        limit: int | None = None,
    ) -> Iterator[TradingStatus]:
        """按游标遍历证券交易状态。"""
        current_cursor = cursor
        while True:
            page = self.get_trading_status(
                symbol,
                start=start,
                end=end,
                cursor=current_cursor,
                status=status,
                is_trading=is_trading,
                risk_warning=risk_warning,
                limit=limit,
            )
            yield from page.statuses
            if page.next_cursor is None:
                return
            self._ensure_progress(current_cursor, page.next_cursor, "交易状态")
            current_cursor = page.next_cursor

    def get_stock_summary(self, symbol: str) -> StockSummary:
        """查询一只证券的轻量当前摘要。"""
        return StockSummary.from_payload(self._get(f"v1/stocks/{_symbol_path(symbol)}/summary"))

    def download_market_daily_bars(
        self,
        destination: str | Path,
        *,
        start: date,
        end: date,
        overwrite: bool = False,
        chunk_size: int = 1024 * 1024,
        timeout: float | httpx.Timeout | None = None,
    ) -> MarketDailyBarsDownload:
        """将未复权全市场日线 Parquet 流式下载为 TAR，不把归档留在内存。"""
        return self._download_parquet_archive(
            destination,
            path="v1/kline/batch",
            start=start,
            end=end,
            overwrite=overwrite,
            chunk_size=chunk_size,
            timeout=timeout,
            resource_name="全市场日线",
            result_type=MarketDailyBarsDownload,
        )

    def download_market_adjustment_factors(
        self,
        destination: str | Path,
        *,
        start: date,
        end: date,
        overwrite: bool = False,
        chunk_size: int = 1024 * 1024,
        timeout: float | httpx.Timeout | None = None,
    ) -> AdjustmentFactorsDownload:
        """将全市场原始后复权因子 Parquet 流式下载为 TAR。"""
        return self._download_parquet_archive(
            destination,
            path="v1/adjustment-factors/batch",
            start=start,
            end=end,
            overwrite=overwrite,
            chunk_size=chunk_size,
            timeout=timeout,
            resource_name="全市场复权因子",
            result_type=AdjustmentFactorsDownload,
        )

    def download_market_trading_calendar(
        self,
        destination: str | Path,
        *,
        start: date,
        end: date,
        overwrite: bool = False,
        chunk_size: int = 1024 * 1024,
        timeout: float | httpx.Timeout | None = None,
    ) -> TradingCalendarDownload:
        """将原始交易日历 Parquet 流式下载为 TAR。"""
        return self._download_parquet_archive(
            destination,
            path="v1/trading-calendar/batch",
            start=start,
            end=end,
            overwrite=overwrite,
            chunk_size=chunk_size,
            timeout=timeout,
            resource_name="交易日历",
            result_type=TradingCalendarDownload,
        )

    def download_market_trading_status(
        self,
        destination: str | Path,
        *,
        start: date,
        end: date,
        overwrite: bool = False,
        chunk_size: int = 1024 * 1024,
        timeout: float | httpx.Timeout | None = None,
    ) -> TradingStatusDownload:
        """将全市场原始交易状态 Parquet 流式下载为 TAR。"""
        return self._download_parquet_archive(
            destination,
            path="v1/trading-status/batch",
            start=start,
            end=end,
            overwrite=overwrite,
            chunk_size=chunk_size,
            timeout=timeout,
            resource_name="全市场交易状态",
            result_type=TradingStatusDownload,
        )

    def download_market_dragon_tiger(
        self,
        destination: str | Path,
        *,
        start: date,
        end: date,
        overwrite: bool = False,
        chunk_size: int = 1024 * 1024,
        timeout: float | httpx.Timeout | None = None,
    ) -> DragonTigerDownload:
        """将全市场原始龙虎榜 Parquet 流式下载为 TAR。"""
        return self._download_parquet_archive(
            destination,
            path="v1/dragon-tiger/batch",
            start=start,
            end=end,
            overwrite=overwrite,
            chunk_size=chunk_size,
            timeout=timeout,
            resource_name="全市场龙虎榜",
            result_type=DragonTigerDownload,
        )

    def _download_parquet_archive(
        self,
        destination: str | Path,
        *,
        path: str,
        start: date,
        end: date,
        overwrite: bool,
        chunk_size: int,
        timeout: float | httpx.Timeout | None,
        resource_name: str,
        result_type: type[_Download],
    ) -> _Download:
        _validate_date_range(start, end)
        _validate_chunk_size(chunk_size)
        target = Path(destination).expanduser()
        if not target.name:
            raise ValueError("destination 必须是文件路径")
        if not target.parent.is_dir():
            raise FileNotFoundError(f"下载目录不存在: {target.parent}")
        if target.exists() and not overwrite:
            raise FileExistsError(f"下载目标已存在: {target}")

        temporary = target.with_name(f".{target.name}.{uuid4().hex}.part")
        bytes_written = 0
        try:
            try:
                with self._client.stream(
                    "GET",
                    path,
                    params=_query_parameters(start=start, end=end),
                    headers={"Accept": "application/x-tar"},
                    timeout=timeout,
                ) as response:
                    if not response.is_success:
                        response.read()
                        raise self._api_error(response)
                    content_type = response.headers.get("content-type", "").split(";", 1)[0].lower()
                    if content_type != "application/x-tar":
                        raise ResponseDecodeError(f"{resource_name}批量接口返回了非 TAR 成功响应")
                    data_file_count = self._required_response_header_int(
                        response,
                        "X-CNEQUITY-Data-Files",
                    )
                    data_bytes = self._required_response_header_int(
                        response,
                        "X-CNEQUITY-Data-Bytes",
                    )
                    with temporary.open("xb") as output:
                        for chunk in response.iter_bytes(chunk_size=chunk_size):
                            output.write(chunk)
                            bytes_written += len(chunk)
            except httpx.RequestError as exc:
                raise TransportError(f"下载{resource_name}失败: {exc}") from exc

            os.replace(temporary, target)
        finally:
            temporary.unlink(missing_ok=True)

        return result_type(
            path=target,
            bytes_written=bytes_written,
            data_file_count=data_file_count,
            data_bytes=data_bytes,
        )

    def get_kline(
        self,
        symbol: str,
        *,
        start: date | None = None,
        end: date | None = None,
        cursor: KlineCursor | None = None,
        interval: KlineInterval = "1d",
        adjustment: Adjustment = "none",
        base_date: date | None = None,
        strict_adjustment: bool = True,
        limit: int | None = None,
    ) -> KlinePage:
        """查询一页日、日内、周或月 K 线。"""
        _validate_limit(limit)
        return KlinePage.from_payload(
            self._get(
                f"v1/kline/{_symbol_path(symbol)}",
                _query_parameters(
                    start=start,
                    end=end,
                    cursor=cursor,
                    interval=interval,
                    adjustment=adjustment,
                    base_date=base_date,
                    strict_adjustment=strict_adjustment,
                    limit=limit,
                ),
            )
        )

    def iter_kline(
        self,
        symbol: str,
        *,
        start: date | None = None,
        end: date | None = None,
        cursor: KlineCursor | None = None,
        interval: KlineInterval = "1d",
        adjustment: Adjustment = "none",
        base_date: date | None = None,
        strict_adjustment: bool = True,
        limit: int | None = None,
    ) -> Iterator[Candle]:
        """按游标遍历指定窗口内的 K 线。"""
        current_cursor = cursor
        while True:
            page = self.get_kline(
                symbol,
                start=start,
                end=end,
                cursor=current_cursor,
                interval=interval,
                adjustment=adjustment,
                base_date=base_date,
                strict_adjustment=strict_adjustment,
                limit=limit,
            )
            yield from page.candles
            if page.next_cursor is None:
                return
            self._ensure_progress(current_cursor, page.next_cursor, "K 线")
            current_cursor = page.next_cursor

    def get_adjustment_factors(
        self,
        symbol: str,
        *,
        start: date | None = None,
        end: date | None = None,
        cursor: date | None = None,
        adjustment: FactorAdjustment = "hfq",
        base_date: date | None = None,
        limit: int | None = None,
    ) -> AdjustmentFactorPage:
        """查询一页后复权或前复权因子。"""
        _validate_limit(limit)
        return AdjustmentFactorPage.from_payload(
            self._get(
                f"v1/adjustment-factors/{_symbol_path(symbol)}",
                _query_parameters(
                    start=start,
                    end=end,
                    cursor=cursor,
                    adjustment=adjustment,
                    base_date=base_date,
                    limit=limit,
                ),
            )
        )

    def iter_adjustment_factors(
        self,
        symbol: str,
        *,
        start: date | None = None,
        end: date | None = None,
        cursor: date | None = None,
        adjustment: FactorAdjustment = "hfq",
        base_date: date | None = None,
        limit: int | None = None,
    ) -> Iterator[AdjustmentFactor]:
        """按游标遍历复权因子。"""
        current_cursor = cursor
        while True:
            page = self.get_adjustment_factors(
                symbol,
                start=start,
                end=end,
                cursor=current_cursor,
                adjustment=adjustment,
                base_date=base_date,
                limit=limit,
            )
            yield from page.factors
            if page.next_cursor is None:
                return
            self._ensure_progress(current_cursor, page.next_cursor, "复权因子")
            current_cursor = page.next_cursor

    def _get(self, path: str, parameters: Mapping[str, str] | None = None) -> Mapping[str, Any]:
        try:
            response = self._client.get(path, params=parameters)
        except httpx.RequestError as exc:
            raise TransportError(f"请求代理失败: {exc}") from exc
        if not response.is_success:
            raise self._api_error(response)
        try:
            payload = response.json()
        except ValueError as exc:
            raise ResponseDecodeError("代理返回了非 JSON 成功响应") from exc
        if not isinstance(payload, Mapping):
            raise ResponseDecodeError("代理成功响应必须是 JSON 对象")
        return payload

    @staticmethod
    def _api_error(response: httpx.Response) -> APIError:
        detail = f"HTTP {response.status_code}"
        try:
            payload = response.json()
        except ValueError:
            payload = None
        if isinstance(payload, Mapping) and isinstance(payload.get("detail"), str):
            detail = payload["detail"]
        elif response.text:
            detail = response.text

        retry_after: float | None = None
        if response.status_code == 429:
            raw_retry_after = response.headers.get("Retry-After")
            if raw_retry_after is not None:
                try:
                    retry_after = float(raw_retry_after)
                except ValueError:
                    pass
        error_type = _ERROR_TYPES.get(response.status_code, APIError)
        return error_type(response.status_code, detail, retry_after=retry_after)

    @staticmethod
    def _required_response_header_int(response: httpx.Response, name: str) -> int:
        raw_value = response.headers.get(name)
        if raw_value is None:
            raise ResponseDecodeError(f"批量 Parquet 响应缺少 {name} 头")
        try:
            value = int(raw_value)
        except ValueError as exc:
            raise ResponseDecodeError(f"批量 Parquet 响应的 {name} 头必须是非负整数") from exc
        if value < 0:
            raise ResponseDecodeError(f"批量 Parquet 响应的 {name} 头必须是非负整数")
        return value

    @staticmethod
    def _ensure_progress(
        current_cursor: KlineCursor | None,
        next_cursor: KlineCursor,
        resource: str,
    ) -> None:
        if current_cursor == next_cursor:
            raise ResponseDecodeError(f"{resource}分页游标没有推进，已停止以避免无限循环")


__all__ = ["QueryClient"]
