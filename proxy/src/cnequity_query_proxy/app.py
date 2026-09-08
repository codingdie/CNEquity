"""K 线 HTTP/JSON API。"""

from __future__ import annotations

import hmac
import re
import threading
from dataclasses import asdict
from datetime import date, datetime, timedelta
from typing import Annotated, Literal, cast

from fastapi import FastAPI, HTTPException, Query, Request, Response
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field

from cnequity_query_proxy.factors import AdjustmentFactorService, FactorAdjustment
from cnequity_query_proxy.instruments import InstrumentService
from cnequity_query_proxy.kline import (
    Adjustment,
    AdjustmentCoverageError,
    KlineService,
    LakeUnavailable,
    QueryBusy,
    QueryFailed,
    QueryTooWide,
)
from cnequity_query_proxy.market_daily_bars import (
    MarketDailyBarsService,
    NoBatchParquetFiles,
    ParquetBatchService,
)
from cnequity_query_proxy.minute_kline import MinuteKlineService
from cnequity_query_proxy.period_kline import PeriodKlineService
from cnequity_query_proxy.settings import ProxySettings
from cnequity_query_proxy.summary import InstrumentNotFound, StockSummaryService
from cnequity_query_proxy.trading_status import TradingState, TradingStatusService

_SYMBOL = re.compile(r"^[0-9A-Z]{6}\.(?:SH|SZ|BJ)$")
_ASSET_TYPE = re.compile(r"^[a-z][a-z0-9_]{0,31}$")
_TRADING_STATES = frozenset({"normal", "suspended", "delisted"})
_PUBLIC_PATHS = frozenset({"/healthz", "/docs", "/docs/oauth2-redirect", "/openapi.json", "/redoc"})


class CandleResponse(BaseModel):
    """一根日 K；选择复权时 OHLC 已被调整。"""

    trade_date: date
    open: float
    high: float
    low: float
    close: float
    volume: int | None
    amount: float | None
    adjustment_factor: float | None = Field(
        default=None,
        description="hfq/qfq 实际使用的因子；未复权时为 null。",
    )
    adjustment_exact: bool | None = Field(
        default=None,
        description="复权因子是否精确覆盖该交易日；未复权时为 null。",
    )


class KlineResponse(BaseModel):
    """按交易日正序返回的一页日 K。"""

    symbol: str
    interval: Literal["1d"] = "1d"
    adjustment: Adjustment = "none"
    strict_adjustment: bool
    start: date
    end: date
    candles: list[CandleResponse]
    base_date: date | None = Field(
        default=None,
        description="qfq 显式指定的复权基准日；hfq/none 时为 null。",
    )
    base_factor_date: date | None = Field(
        default=None,
        description="实际命中的 qfq 后复权因子日期；应与 base_date 相同。",
    )
    next_cursor: date | None = Field(
        default=None,
        description="下一页传回 cursor；null 表示当前窗口已读完。",
    )


class IntradayCandleResponse(BaseModel):
    """一根日内 K；bar_time 是 Asia/Shanghai 的无时区收盘时间。"""

    trade_date: date
    bar_time: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int | None
    amount: float | None
    adjustment_factor: float | None = Field(
        default=None,
        description="hfq/qfq 实际使用的因子；未复权时为 null。",
    )
    adjustment_exact: bool | None = Field(
        default=None,
        description="复权因子是否精确覆盖该交易日；未复权时为 null。",
    )


class IntradayKlineResponse(BaseModel):
    """按 bar_time 正序返回的一页 1 分钟或 5 分钟 K。"""

    symbol: str
    interval: Literal["1m", "5m"]
    adjustment: Adjustment = "none"
    strict_adjustment: bool
    start: date
    end: date
    candles: list[IntradayCandleResponse]
    base_date: date | None = Field(
        default=None,
        description="qfq 显式指定的复权基准日；hfq/none 时为 null。",
    )
    base_factor_date: date | None = Field(
        default=None,
        description="实际命中的 qfq 后复权因子日期；应与 base_date 相同。",
    )
    next_cursor: datetime | None = Field(
        default=None,
        description="下一页传回 cursor；null 表示当前窗口已读完。",
    )


class PeriodCandleResponse(BaseModel):
    """一根周线或月线；OHLC 在日线复权后聚合。"""

    period_start: date = Field(description="周/月所在自然周期的开始日。")
    period_end: date = Field(description="周/月所在自然周期的结束日。")
    trade_date: date = Field(description="本次聚合实际包含的最后一个交易日。")
    open: float
    high: float
    low: float
    close: float
    volume: int | None
    amount: float | None
    adjustment_factor: float | None = Field(
        default=None,
        description="该周期最后交易日实际使用的因子；未复权时为 null。",
    )
    adjustment_exact: bool | None = Field(
        default=None,
        description="聚合所含全部交易日的复权因子是否精确；未复权时为 null。",
    )


class PeriodKlineResponse(BaseModel):
    """按最后交易日正序返回的一页周线或月线。"""

    symbol: str
    interval: Literal["1w", "1mo"]
    adjustment: Adjustment = "none"
    strict_adjustment: bool
    start: date
    end: date
    candles: list[PeriodCandleResponse]
    base_date: date | None = Field(
        default=None,
        description="qfq 显式指定的复权基准日；hfq/none 时为 null。",
    )
    base_factor_date: date | None = Field(
        default=None,
        description="实际命中的 qfq 后复权因子日期；应与 base_date 相同。",
    )
    next_cursor: date | None = Field(
        default=None,
        description="下一页传回 cursor；其值是上一页最后一根 K 的 trade_date。",
    )


class AdjustmentFactorResponse(BaseModel):
    """一个交易日的复权因子。"""

    trade_date: date
    factor: float = Field(
        description="可直接乘到原始 OHLC 上的因子；qfq 已按 base_date 归一化。",
    )


class AdjustmentFactorPageResponse(BaseModel):
    """按交易日正序返回的一页复权因子。"""

    symbol: str
    adjustment: FactorAdjustment
    start: date
    end: date
    factors: list[AdjustmentFactorResponse]
    base_date: date | None = Field(
        default=None,
        description="qfq 显式指定的复权基准日；hfq 时为 null。",
    )
    base_factor_date: date | None = Field(
        default=None,
        description="实际命中的 qfq 后复权因子日期；应与 base_date 相同。",
    )
    next_cursor: date | None = Field(
        default=None,
        description="下一页传回 cursor；null 表示当前窗口已读完。",
    )


class InstrumentResponse(BaseModel):
    """一个可用于发现代码的证券主数据条目。"""

    symbol: str
    name: str
    exchange: str
    asset_type: str
    list_date: date | None
    delist_date: date | None
    prev_symbol: str | None


class InstrumentPageResponse(BaseModel):
    """按 symbol 正序返回的一页证券主数据。"""

    instruments: list[InstrumentResponse]
    as_of: date | None = Field(
        default=None,
        description="按上市/退市日期筛选的日期；null 表示不做在市过滤。",
    )
    next_cursor: str | None = Field(
        default=None,
        description="下一页传回 cursor；null 表示当前筛选结果已读完。",
    )


class TradingStatusRecordResponse(BaseModel):
    """一只证券在一个交易日的状态。"""

    trade_date: date
    is_trading: bool = Field(description="数据湖记录的当日是否可交易。")
    status: str = Field(description="交易状态，当前契约为 normal、suspended 或 delisted。")
    risk_warning: bool | None = Field(
        default=None,
        description="是否有 ST/*ST 风险警示；null 表示数据湖没有该事实的证据。",
    )


class TradingStatusPageResponse(BaseModel):
    """按交易日正序返回的一页交易状态。"""

    symbol: str
    start: date
    end: date
    statuses: list[TradingStatusRecordResponse]
    next_cursor: date | None = Field(
        default=None,
        description="下一页传回 cursor；null 表示当前窗口已读完。",
    )


class ProvenanceResponse(BaseModel):
    """摘要内一条落盘事实的来源和采集时间。"""

    source: str
    data_version: str
    fetched_at: datetime | None


class StockSummaryInstrumentResponse(BaseModel):
    symbol: str
    name: str
    exchange: str
    asset_type: str
    list_date: date | None
    delist_date: date | None
    prev_symbol: str | None
    provenance: ProvenanceResponse


class StockSummaryLatestMarketResponse(BaseModel):
    """最新可用日级行情快照，不是 K 线序列或实时盘口。"""

    trade_date: date
    open: float
    high: float
    low: float
    close: float
    volume: int | None
    amount: float | None
    provenance: ProvenanceResponse


class StockSummaryTradingStatusResponse(BaseModel):
    trade_date: date
    is_trading: bool
    status: str
    risk_warning: bool | None
    provenance: ProvenanceResponse


class StockSummaryValuationResponse(BaseModel):
    trade_date: date
    pe_ttm: float | None
    pb: float | None
    ps_ttm: float | None
    total_mv: float | None
    float_mv: float | None
    provenance: ProvenanceResponse


class StockSummaryMarketResponse(BaseModel):
    latest_market: StockSummaryLatestMarketResponse | None
    trading_status: StockSummaryTradingStatusResponse | None
    valuation: StockSummaryValuationResponse | None


class StockSummaryIndustryResponse(BaseModel):
    classification_system: str
    industry_code: str
    industry_name: str
    as_of_date: date
    provenance: ProvenanceResponse


class StockSummarySectorResponse(BaseModel):
    sector_code: str
    sector_name: str
    as_of_date: date
    provenance: ProvenanceResponse


class StockSummaryIndexMembershipResponse(BaseModel):
    index_symbol: str
    as_of_date: date
    weight: float | None = Field(
        default=None,
        description="数据源未提供成分权重时为 null；不能将其理解为零权重。",
    )
    provenance: ProvenanceResponse


class StockSummaryClassificationResponse(BaseModel):
    industries: list[StockSummaryIndustryResponse]
    sectors: list[StockSummarySectorResponse]
    index_memberships: list[StockSummaryIndexMembershipResponse]


class StockSummaryFundFlowResponse(BaseModel):
    trade_date: date
    main_net_inflow: float | None
    super_large_net_inflow: float | None
    large_net_inflow: float | None
    medium_net_inflow: float | None
    small_net_inflow: float | None
    provenance: ProvenanceResponse


class StockSummaryAnalystConsensusResponse(BaseModel):
    forecast_date: date
    forecast_year: int | None
    eps_forecast: float | None
    pe_forecast: float | None
    target_price: float | None
    rating: str | None
    analyst_count: int | None
    provenance: ProvenanceResponse


class StockSummaryHotRankResponse(BaseModel):
    trade_date: date
    rank: int | None
    rank_change: int | None
    hist_rank: int | None
    provenance: ProvenanceResponse


class StockSummarySentimentResponse(BaseModel):
    trade_date: date
    score_channel: str
    sentiment_score: float | None
    headline_count: int | None
    provenance: ProvenanceResponse


class StockSummarySignalsResponse(BaseModel):
    fund_flow: StockSummaryFundFlowResponse | None
    analyst_consensus: StockSummaryAnalystConsensusResponse | None
    hot_rank: StockSummaryHotRankResponse | None
    sentiments: list[StockSummarySentimentResponse]


class StockSummaryResponse(BaseModel):
    """单只证券的轻量当前摘要，不包含历史时序或长表明细。"""

    symbol: str
    instrument: StockSummaryInstrumentResponse
    market: StockSummaryMarketResponse
    classification: StockSummaryClassificationResponse
    signals: StockSummarySignalsResponse
    unavailable_datasets: list[str] = Field(
        description="本地目录不存在或尚无 Parquet 文件的可选摘要数据集。"
    )


def _normalize_symbol(value: str, *, parameter: str = "symbol") -> str:
    symbol = value.upper()
    if not _SYMBOL.fullmatch(symbol):
        raise HTTPException(
            422,
            f"{parameter} 必须是 6 位代码加 .SH、.SZ 或 .BJ，例如 600519.SH",
        )
    return symbol


def _normalize_exchange(value: str | None) -> str | None:
    if value is None:
        return None
    exchange = value.strip().upper()
    if exchange not in {"SH", "SZ", "BJ"}:
        raise HTTPException(422, "exchange 必须是 SH、SZ 或 BJ")
    return exchange


def _normalize_asset_type(value: str | None) -> str | None:
    if value is None:
        return None
    asset_type = value.strip().lower()
    if not _ASSET_TYPE.fullmatch(asset_type):
        raise HTTPException(422, "asset_type 只能包含小写字母、数字和下划线")
    return asset_type


def _normalize_trading_state(value: str | None) -> TradingState | None:
    if value is None:
        return None
    state = value.strip().lower()
    if state not in _TRADING_STATES:
        raise HTTPException(422, "status 必须是 normal、suspended 或 delisted")
    return cast(TradingState, state)


def _normalize_query(value: str | None) -> str | None:
    if value is None:
        return None
    query = value.strip()
    if not query:
        raise HTTPException(422, "q 不能为空")
    return query


def _parse_date_cursor(value: str | None, *, interval: str) -> date | None:
    if value is None:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise HTTPException(422, f"interval={interval} 的 cursor 必须是 YYYY-MM-DD 日期") from exc


def _parse_intraday_cursor(value: str | None, *, interval: str) -> datetime | None:
    if value is None:
        return None
    if "T" not in value and " " not in value:
        raise HTTPException(
            422,
            f"interval={interval} 的 cursor 必须是无时区 ISO 8601 时间戳，例如 2026-01-02T09:31:00",
        )
    try:
        cursor = datetime.fromisoformat(value)
    except ValueError as exc:
        raise HTTPException(
            422,
            f"interval={interval} 的 cursor 必须是无时区 ISO 8601 时间戳，例如 2026-01-02T09:31:00",
        ) from exc
    if cursor.tzinfo is not None or cursor.utcoffset() is not None:
        raise HTTPException(
            422,
            f"interval={interval} 的 cursor 必须是不带时区的 Asia/Shanghai 墙钟时间",
        )
    if cursor.second != 0 or cursor.microsecond != 0:
        raise HTTPException(422, f"interval={interval} 的 cursor 必须精确到分钟")
    return cursor


def _resolve_window(
    settings: ProxySettings,
    *,
    start: date | None,
    end: date | None,
    default_window_days: int | None = None,
    max_window_days: int | None = None,
) -> tuple[date, date]:
    resolved_end = end or date.today()
    effective_default = default_window_days or settings.default_window_days
    effective_maximum = max_window_days or settings.max_window_days
    resolved_start = start or resolved_end - timedelta(days=effective_default)
    if resolved_start > resolved_end:
        raise HTTPException(422, "start 不能晚于 end")
    if (resolved_end - resolved_start).days > effective_maximum:
        raise HTTPException(
            422,
            f"日期窗口最多 {effective_maximum} 天；请拆分请求或提高代理配置上限",
        )
    return resolved_start, resolved_end


def create_app(settings: ProxySettings) -> FastAPI:
    """创建独立代理应用，不读取或初始化主项目的任何对象。"""
    app = FastAPI(
        title="CNEquity Query Proxy",
        version="0.1.0",
        description="Standalone, read-only HTTP API over local Parquet files.",
    )
    query_slots = threading.BoundedSemaphore(settings.max_concurrent_queries)
    service = KlineService(settings, slots=query_slots)
    factor_service = AdjustmentFactorService(settings, slots=query_slots)
    instrument_service = InstrumentService(settings, slots=query_slots)
    trading_status_service = TradingStatusService(settings, slots=query_slots)
    minute_kline_service = MinuteKlineService(settings, interval="1m", slots=query_slots)
    five_minute_kline_service = MinuteKlineService(settings, interval="5m", slots=query_slots)
    weekly_kline_service = PeriodKlineService(settings, interval="1w", slots=query_slots)
    monthly_kline_service = PeriodKlineService(settings, interval="1mo", slots=query_slots)
    stock_summary_service = StockSummaryService(settings, slots=query_slots)
    market_daily_bars_service = MarketDailyBarsService(settings)
    adjustment_factors_batch_service = ParquetBatchService(
        settings,
        root=settings.adj_factors_root,
        dataset_name="adj_factors",
        data_label="复权因子",
    )
    trading_status_batch_service = ParquetBatchService(
        settings,
        root=settings.trading_status_root,
        dataset_name="trading_status",
        data_label="交易状态",
    )
    app.state.settings = settings
    app.state.kline_service = service
    app.state.adjustment_factor_service = factor_service
    app.state.instrument_service = instrument_service
    app.state.trading_status_service = trading_status_service
    app.state.minute_kline_service = minute_kline_service
    app.state.five_minute_kline_service = five_minute_kline_service
    app.state.weekly_kline_service = weekly_kline_service
    app.state.monthly_kline_service = monthly_kline_service
    app.state.stock_summary_service = stock_summary_service
    app.state.market_daily_bars_service = market_daily_bars_service
    app.state.adjustment_factors_batch_service = adjustment_factors_batch_service
    app.state.trading_status_batch_service = trading_status_batch_service

    def batch_archive_response(
        service: ParquetBatchService,
        *,
        start: date,
        end: date,
        filename_prefix: str,
        unavailable_detail: str,
    ) -> StreamingResponse:
        if start > end:
            raise HTTPException(422, "start 不能晚于 end")
        try:
            archive = service.open_archive(start=start, end=end)
        except NoBatchParquetFiles as exc:
            raise HTTPException(404, str(exc)) from exc
        except LakeUnavailable as exc:
            raise HTTPException(503, unavailable_detail) from exc
        except QueryFailed as exc:
            raise HTTPException(503, str(exc)) from exc

        filename = f"{filename_prefix}-{start.isoformat()}-{end.isoformat()}.tar"
        return StreamingResponse(
            archive.iter_bytes(),
            media_type="application/x-tar",
            headers={
                "Content-Disposition": f'attachment; filename="{filename}"',
                "Cache-Control": "no-store",
                "X-Cache": "BYPASS",
                "X-CNEQUITY-Data-Files": str(archive.file_count),
                "X-CNEQUITY-Data-Bytes": str(archive.data_bytes),
            },
        )

    @app.middleware("http")
    async def authenticate(request: Request, call_next):
        expected = request.app.state.settings.api_key
        if expected and request.url.path not in _PUBLIC_PATHS:
            scheme, _, supplied = request.headers.get("authorization", "").partition(" ")
            if scheme.lower() != "bearer" or not hmac.compare_digest(supplied, expected):
                return JSONResponse(
                    {"detail": "unauthorized"},
                    status_code=401,
                    headers={"WWW-Authenticate": "Bearer"},
                )
        return await call_next(request)

    @app.get("/healthz", include_in_schema=False)
    def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/v1/instruments", response_model=InstrumentPageResponse)
    def instruments(
        response: Response,
        symbol: str | None = None,
        q: Annotated[str | None, Query(min_length=1, max_length=64)] = None,
        exchange: str | None = None,
        asset_type: str | None = None,
        as_of: date | None = None,
        cursor: str | None = None,
        limit: Annotated[int | None, Query(ge=1)] = None,
    ) -> InstrumentPageResponse:
        normalized_symbol = _normalize_symbol(symbol) if symbol is not None else None
        normalized_cursor = (
            _normalize_symbol(cursor, parameter="cursor") if cursor is not None else None
        )
        normalized_query = _normalize_query(q)
        normalized_exchange = _normalize_exchange(exchange)
        normalized_asset_type = _normalize_asset_type(asset_type)
        resolved_limit = limit if limit is not None else settings.default_limit
        if resolved_limit > settings.max_bars:
            raise HTTPException(422, f"limit 不能超过 {settings.max_bars}")

        try:
            page, cache_hit = instrument_service.query(
                symbol=normalized_symbol,
                query=normalized_query,
                exchange=normalized_exchange,
                asset_type=normalized_asset_type,
                as_of=as_of,
                cursor=normalized_cursor,
                limit=resolved_limit,
            )
        except QueryBusy as exc:
            raise HTTPException(429, str(exc), headers={"Retry-After": "1"}) from exc
        except LakeUnavailable as exc:
            raise HTTPException(503, "证券主数据暂不可用") from exc
        except QueryFailed as exc:
            raise HTTPException(503, str(exc)) from exc

        response.headers["X-Cache"] = "HIT" if cache_hit else "MISS"
        response.headers["Cache-Control"] = (
            "no-store"
            if settings.cache_ttl_seconds == 0
            else f"private, max-age={int(settings.cache_ttl_seconds)}"
        )
        return InstrumentPageResponse(
            instruments=[
                InstrumentResponse(**instrument.__dict__) for instrument in page.instruments
            ],
            as_of=as_of,
            next_cursor=page.next_cursor,
        )

    @app.get(
        "/v1/trading-status/batch",
        response_class=StreamingResponse,
        responses={
            200: {
                "content": {"application/x-tar": {}},
                "description": "原始全市场交易状态 Parquet 的流式 TAR 归档。",
            }
        },
    )
    def market_trading_status_batch(start: date, end: date) -> StreamingResponse:
        """流式下载所选窗口重叠月份的原始全市场交易状态 Parquet。"""
        return batch_archive_response(
            trading_status_batch_service,
            start=start,
            end=end,
            filename_prefix="cnequity-trading-status",
            unavailable_detail="交易状态数据湖暂不可用",
        )

    @app.get(
        "/v1/trading-status/{symbol}",
        response_model=TradingStatusPageResponse,
    )
    def trading_status(
        symbol: str,
        response: Response,
        start: date | None = None,
        end: date | None = None,
        cursor: date | None = None,
        status: str | None = None,
        is_trading: bool | None = None,
        risk_warning: bool | None = None,
        limit: Annotated[int | None, Query(ge=1)] = None,
    ) -> TradingStatusPageResponse:
        normalized_symbol = _normalize_symbol(symbol)
        normalized_status = _normalize_trading_state(status)
        resolved_start, resolved_end = _resolve_window(settings, start=start, end=end)
        resolved_limit = limit if limit is not None else settings.default_limit
        if resolved_limit > settings.max_bars:
            raise HTTPException(422, f"limit 不能超过 {settings.max_bars}")
        if cursor is not None and cursor < resolved_start:
            raise HTTPException(422, "cursor 不能早于 start")

        try:
            page, cache_hit = trading_status_service.query(
                symbol=normalized_symbol,
                start=resolved_start,
                end=resolved_end,
                limit=resolved_limit,
                cursor=cursor,
                status=normalized_status,
                is_trading=is_trading,
                risk_warning=risk_warning,
            )
        except QueryBusy as exc:
            raise HTTPException(429, str(exc), headers={"Retry-After": "1"}) from exc
        except QueryTooWide as exc:
            raise HTTPException(413, str(exc)) from exc
        except LakeUnavailable as exc:
            raise HTTPException(503, "交易状态数据湖暂不可用") from exc
        except QueryFailed as exc:
            raise HTTPException(503, str(exc)) from exc

        response.headers["X-Cache"] = "HIT" if cache_hit else "MISS"
        response.headers["Cache-Control"] = (
            "no-store"
            if settings.cache_ttl_seconds == 0
            else f"private, max-age={int(settings.cache_ttl_seconds)}"
        )
        return TradingStatusPageResponse(
            symbol=normalized_symbol,
            start=resolved_start,
            end=resolved_end,
            statuses=[TradingStatusRecordResponse(**item.__dict__) for item in page.statuses],
            next_cursor=page.next_cursor,
        )

    @app.get(
        "/v1/stocks/{symbol}/summary",
        response_model=StockSummaryResponse,
    )
    def stock_summary(symbol: str, response: Response) -> StockSummaryResponse:
        normalized_symbol = _normalize_symbol(symbol)
        try:
            summary, cache_hit = stock_summary_service.query(symbol=normalized_symbol)
        except InstrumentNotFound as exc:
            raise HTTPException(404, "证券主数据中不存在该代码") from exc
        except QueryBusy as exc:
            raise HTTPException(429, str(exc), headers={"Retry-After": "1"}) from exc
        except QueryTooWide as exc:
            raise HTTPException(413, str(exc)) from exc
        except LakeUnavailable as exc:
            raise HTTPException(503, "股票摘要所需证券主数据暂不可用") from exc
        except QueryFailed as exc:
            raise HTTPException(503, str(exc)) from exc

        response.headers["X-Cache"] = "HIT" if cache_hit else "MISS"
        response.headers["Cache-Control"] = (
            "no-store"
            if settings.cache_ttl_seconds == 0
            else f"private, max-age={int(settings.cache_ttl_seconds)}"
        )
        return StockSummaryResponse.model_validate(
            {
                "symbol": summary.symbol,
                "instrument": asdict(summary.instrument),
                "market": {
                    "latest_market": asdict(summary.daily_bar) if summary.daily_bar else None,
                    "trading_status": asdict(summary.trading_status)
                    if summary.trading_status
                    else None,
                    "valuation": asdict(summary.valuation) if summary.valuation else None,
                },
                "classification": {
                    "industries": [asdict(item) for item in summary.industries],
                    "sectors": [asdict(item) for item in summary.sectors],
                    "index_memberships": [asdict(item) for item in summary.index_memberships],
                },
                "signals": {
                    "fund_flow": asdict(summary.fund_flow) if summary.fund_flow else None,
                    "analyst_consensus": asdict(summary.analyst_consensus)
                    if summary.analyst_consensus
                    else None,
                    "hot_rank": asdict(summary.hot_rank) if summary.hot_rank else None,
                    "sentiments": [asdict(item) for item in summary.sentiments],
                },
                "unavailable_datasets": list(summary.unavailable_datasets),
            }
        )

    @app.get(
        "/v1/kline/batch",
        response_class=StreamingResponse,
        responses={
            200: {
                "content": {"application/x-tar": {}},
                "description": "原始全市场日线 Parquet 的流式 TAR 归档。",
            }
        },
    )
    def market_daily_bars_batch(start: date, end: date) -> StreamingResponse:
        """流式下载所选日期窗口内原始、未复权的全市场日线 Parquet。"""
        return batch_archive_response(
            market_daily_bars_service,
            start=start,
            end=end,
            filename_prefix="cnequity-daily-bars",
            unavailable_detail="日 K 数据湖暂不可用",
        )

    @app.get(
        "/v1/kline/{symbol}",
        response_model=KlineResponse | IntradayKlineResponse | PeriodKlineResponse,
    )
    def kline(
        symbol: str,
        response: Response,
        start: date | None = None,
        end: date | None = None,
        cursor: str | None = None,
        interval: Literal["1d", "1m", "5m", "1w", "1mo"] = "1d",
        adjustment: Adjustment = "none",
        adjust: Annotated[Adjustment | None, Query(deprecated=True)] = None,
        base_date: date | None = None,
        strict_adjustment: bool = True,
        limit: Annotated[int | None, Query(ge=1)] = None,
    ) -> KlineResponse | IntradayKlineResponse | PeriodKlineResponse:
        normalized_symbol = _normalize_symbol(symbol)
        intraday_window = (
            settings.intraday_window_days(interval) if interval in {"1m", "5m"} else None
        )
        resolved_start, resolved_end = _resolve_window(
            settings,
            start=start,
            end=end,
            default_window_days=intraday_window[0] if intraday_window is not None else None,
            max_window_days=intraday_window[1] if intraday_window is not None else None,
        )
        resolved_limit = limit if limit is not None else settings.default_limit
        if resolved_limit > settings.max_bars:
            raise HTTPException(422, f"limit 不能超过 {settings.max_bars}")
        if adjust is not None and adjustment != "none" and adjust != adjustment:
            raise HTTPException(422, "adjust 与 adjustment 不能给出不同值")
        effective_adjustment = adjust or adjustment
        if effective_adjustment == "qfq" and base_date is None:
            raise HTTPException(422, "adjustment=qfq 时必须传 base_date")
        if effective_adjustment != "qfq" and base_date is not None:
            raise HTTPException(422, "base_date 仅适用于 adjustment=qfq")

        if interval in {"1m", "5m"}:
            intraday_cursor = _parse_intraday_cursor(cursor, interval=interval)
            if intraday_cursor is not None and intraday_cursor.date() < resolved_start:
                raise HTTPException(422, "cursor 不能早于 start")
            intraday_service = (
                minute_kline_service if interval == "1m" else five_minute_kline_service
            )
            try:
                page, cache_hit = intraday_service.query(
                    symbol=normalized_symbol,
                    start=resolved_start,
                    end=resolved_end,
                    limit=resolved_limit,
                    cursor=intraday_cursor,
                    adjustment=effective_adjustment,
                    strict_adjustment=strict_adjustment,
                    base_date=base_date,
                )
            except QueryBusy as exc:
                raise HTTPException(429, str(exc), headers={"Retry-After": "1"}) from exc
            except QueryTooWide as exc:
                raise HTTPException(413, str(exc)) from exc
            except AdjustmentCoverageError as exc:
                raise HTTPException(409, str(exc)) from exc
            except LakeUnavailable as exc:
                raise HTTPException(503, f"{interval} K 数据湖暂不可用") from exc
            except QueryFailed as exc:
                raise HTTPException(503, str(exc)) from exc

            response.headers["X-Cache"] = "HIT" if cache_hit else "MISS"
            response.headers["Cache-Control"] = (
                "no-store"
                if settings.cache_ttl_seconds == 0
                else f"private, max-age={int(settings.cache_ttl_seconds)}"
            )
            return IntradayKlineResponse(
                symbol=normalized_symbol,
                interval=interval,
                adjustment=effective_adjustment,
                strict_adjustment=strict_adjustment,
                start=resolved_start,
                end=resolved_end,
                candles=[IntradayCandleResponse(**candle.__dict__) for candle in page.candles],
                base_date=page.base_date,
                base_factor_date=page.base_factor_date,
                next_cursor=page.next_cursor,
            )

        date_cursor = _parse_date_cursor(cursor, interval=interval)
        if date_cursor is not None and date_cursor < resolved_start:
            raise HTTPException(422, "cursor 不能早于 start")
        if interval in {"1w", "1mo"}:
            period_service = weekly_kline_service if interval == "1w" else monthly_kline_service
            try:
                page, cache_hit = period_service.query(
                    symbol=normalized_symbol,
                    start=resolved_start,
                    end=resolved_end,
                    limit=resolved_limit,
                    cursor=date_cursor,
                    adjustment=effective_adjustment,
                    strict_adjustment=strict_adjustment,
                    base_date=base_date,
                )
            except QueryBusy as exc:
                raise HTTPException(429, str(exc), headers={"Retry-After": "1"}) from exc
            except QueryTooWide as exc:
                raise HTTPException(413, str(exc)) from exc
            except AdjustmentCoverageError as exc:
                raise HTTPException(409, str(exc)) from exc
            except LakeUnavailable as exc:
                raise HTTPException(503, f"{interval} K 数据湖暂不可用") from exc
            except QueryFailed as exc:
                raise HTTPException(503, str(exc)) from exc

            response.headers["X-Cache"] = "HIT" if cache_hit else "MISS"
            response.headers["Cache-Control"] = (
                "no-store"
                if settings.cache_ttl_seconds == 0
                else f"private, max-age={int(settings.cache_ttl_seconds)}"
            )
            return PeriodKlineResponse(
                symbol=normalized_symbol,
                interval=interval,
                adjustment=effective_adjustment,
                strict_adjustment=strict_adjustment,
                start=resolved_start,
                end=resolved_end,
                candles=[PeriodCandleResponse(**candle.__dict__) for candle in page.candles],
                base_date=page.base_date,
                base_factor_date=page.base_factor_date,
                next_cursor=page.next_cursor,
            )

        try:
            page, cache_hit = service.query(
                symbol=normalized_symbol,
                start=resolved_start,
                end=resolved_end,
                limit=resolved_limit,
                cursor=date_cursor,
                adjustment=effective_adjustment,
                strict_adjustment=strict_adjustment,
                base_date=base_date,
            )
        except QueryBusy as exc:
            raise HTTPException(429, str(exc), headers={"Retry-After": "1"}) from exc
        except QueryTooWide as exc:
            raise HTTPException(413, str(exc)) from exc
        except AdjustmentCoverageError as exc:
            raise HTTPException(409, str(exc)) from exc
        except LakeUnavailable as exc:
            raise HTTPException(503, "日 K 数据湖暂不可用") from exc
        except QueryFailed as exc:
            raise HTTPException(503, str(exc)) from exc

        response.headers["X-Cache"] = "HIT" if cache_hit else "MISS"
        response.headers["Cache-Control"] = (
            "no-store"
            if settings.cache_ttl_seconds == 0
            else f"private, max-age={int(settings.cache_ttl_seconds)}"
        )
        return KlineResponse(
            symbol=normalized_symbol,
            interval="1d",
            adjustment=effective_adjustment,
            strict_adjustment=strict_adjustment,
            start=resolved_start,
            end=resolved_end,
            candles=[CandleResponse(**candle.__dict__) for candle in page.candles],
            base_date=page.base_date,
            base_factor_date=page.base_factor_date,
            next_cursor=page.next_cursor,
        )

    @app.get(
        "/v1/adjustment-factors/batch",
        response_class=StreamingResponse,
        responses={
            200: {
                "content": {"application/x-tar": {}},
                "description": "原始全市场后复权因子 Parquet 的流式 TAR 归档。",
            }
        },
    )
    def market_adjustment_factors_batch(start: date, end: date) -> StreamingResponse:
        """流式下载所选窗口内原始全市场后复权因子 Parquet。"""
        return batch_archive_response(
            adjustment_factors_batch_service,
            start=start,
            end=end,
            filename_prefix="cnequity-adjustment-factors",
            unavailable_detail="复权因子数据湖暂不可用",
        )

    @app.get(
        "/v1/adjustment-factors/{symbol}",
        response_model=AdjustmentFactorPageResponse,
    )
    def adjustment_factors(
        symbol: str,
        response: Response,
        start: date | None = None,
        end: date | None = None,
        cursor: date | None = None,
        adjustment: FactorAdjustment | None = None,
        adjust: Annotated[FactorAdjustment | None, Query(deprecated=True)] = None,
        base_date: date | None = None,
        limit: Annotated[int | None, Query(ge=1)] = None,
    ) -> AdjustmentFactorPageResponse:
        normalized_symbol = _normalize_symbol(symbol)
        resolved_start, resolved_end = _resolve_window(settings, start=start, end=end)
        resolved_limit = limit if limit is not None else settings.default_limit
        if resolved_limit > settings.max_bars:
            raise HTTPException(422, f"limit 不能超过 {settings.max_bars}")
        if cursor is not None and cursor < resolved_start:
            raise HTTPException(422, "cursor 不能早于 start")
        if adjust is not None and adjustment is not None and adjust != adjustment:
            raise HTTPException(422, "adjust 与 adjustment 不能给出不同值")
        effective_adjustment = adjust or adjustment or "hfq"
        if effective_adjustment == "qfq" and base_date is None:
            raise HTTPException(422, "adjustment=qfq 时必须传 base_date")
        if effective_adjustment == "hfq" and base_date is not None:
            raise HTTPException(422, "base_date 仅适用于 adjustment=qfq")

        try:
            page, cache_hit = factor_service.query(
                symbol=normalized_symbol,
                start=resolved_start,
                end=resolved_end,
                limit=resolved_limit,
                cursor=cursor,
                adjustment=effective_adjustment,
                base_date=base_date,
            )
        except QueryBusy as exc:
            raise HTTPException(429, str(exc), headers={"Retry-After": "1"}) from exc
        except QueryTooWide as exc:
            raise HTTPException(413, str(exc)) from exc
        except AdjustmentCoverageError as exc:
            raise HTTPException(409, str(exc)) from exc
        except LakeUnavailable as exc:
            raise HTTPException(503, "复权因子数据湖暂不可用") from exc
        except QueryFailed as exc:
            raise HTTPException(503, str(exc)) from exc

        response.headers["X-Cache"] = "HIT" if cache_hit else "MISS"
        response.headers["Cache-Control"] = (
            "no-store"
            if settings.cache_ttl_seconds == 0
            else f"private, max-age={int(settings.cache_ttl_seconds)}"
        )
        return AdjustmentFactorPageResponse(
            symbol=normalized_symbol,
            adjustment=effective_adjustment,
            start=resolved_start,
            end=resolved_end,
            factors=[AdjustmentFactorResponse(**factor.__dict__) for factor in page.factors],
            base_date=page.base_date,
            base_factor_date=page.base_factor_date,
            next_cursor=page.next_cursor,
        )

    return app
