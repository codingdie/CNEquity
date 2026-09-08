"""CNEquity Query Proxy 公开接口契约对应的类型化模型。"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Literal, TypeAlias, TypeVar, cast

from cnequity_query_sdk.errors import ResponseDecodeError

Payload: TypeAlias = Mapping[str, Any]
Adjustment: TypeAlias = Literal["none", "hfq", "qfq"]
FactorAdjustment: TypeAlias = Literal["hfq", "qfq"]
KlineInterval: TypeAlias = Literal["1d", "1m", "5m", "1w", "1mo"]
TradingState: TypeAlias = Literal["normal", "suspended", "delisted"]

_T = TypeVar("_T")
_ADJUSTMENTS = frozenset({"none", "hfq", "qfq"})
_FACTOR_ADJUSTMENTS = frozenset({"hfq", "qfq"})
_KLINE_INTERVALS = frozenset({"1d", "1m", "5m", "1w", "1mo"})
_TRADING_STATES = frozenset({"normal", "suspended", "delisted"})


def _required(payload: Payload, field: str) -> Any:
    try:
        return payload[field]
    except KeyError as exc:
        raise ResponseDecodeError(f"响应缺少字段 {field!r}") from exc


def _object(value: Any, field: str) -> Payload:
    if not isinstance(value, Mapping):
        raise ResponseDecodeError(f"响应字段 {field!r} 必须是对象")
    return cast(Payload, value)


def _array(value: Any, field: str) -> list[Any]:
    if not isinstance(value, list):
        raise ResponseDecodeError(f"响应字段 {field!r} 必须是数组")
    return value


def _string(value: Any, field: str) -> str:
    if not isinstance(value, str):
        raise ResponseDecodeError(f"响应字段 {field!r} 必须是字符串")
    return value


def _integer(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise ResponseDecodeError(f"响应字段 {field!r} 必须是整数")
    return value


def _number(value: Any, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ResponseDecodeError(f"响应字段 {field!r} 必须是数字")
    return float(value)


def _boolean(value: Any, field: str) -> bool:
    if not isinstance(value, bool):
        raise ResponseDecodeError(f"响应字段 {field!r} 必须是布尔值")
    return value


def _date(value: Any, field: str) -> date:
    text = _string(value, field)
    try:
        return date.fromisoformat(text)
    except ValueError as exc:
        raise ResponseDecodeError(f"响应字段 {field!r} 不是 ISO 日期") from exc


def _datetime(value: Any, field: str, *, require_timezone: bool = False) -> datetime:
    text = _string(value, field)
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ResponseDecodeError(f"响应字段 {field!r} 不是 ISO 日期时间") from exc
    if require_timezone and parsed.tzinfo is None:
        raise ResponseDecodeError(f"响应字段 {field!r} 必须带时区")
    return parsed


def _optional(payload: Payload, field: str, parser: Callable[[Any, str], _T]) -> _T | None:
    value = _required(payload, field)
    return None if value is None else parser(value, field)


def _literal(payload: Payload, field: str, allowed: frozenset[str]) -> str:
    value = _string(_required(payload, field), field)
    if value not in allowed:
        options = ", ".join(sorted(allowed))
        raise ResponseDecodeError(f"响应字段 {field!r} 必须是 {options} 之一")
    return value


def _object_tuple(payload: Payload, field: str, parser: Callable[[Payload], _T]) -> tuple[_T, ...]:
    values = _array(_required(payload, field), field)
    return tuple(parser(_object(value, f"{field}[{index}]")) for index, value in enumerate(values))


@dataclass(frozen=True, slots=True)
class Health:
    status: str

    @classmethod
    def from_payload(cls, payload: Payload) -> Health:
        return cls(status=_string(_required(payload, "status"), "status"))


@dataclass(frozen=True, slots=True)
class ParquetArchiveDownload:
    """一次原始 Parquet TAR 下载的落盘结果。"""

    path: Path
    bytes_written: int
    data_file_count: int
    data_bytes: int


@dataclass(frozen=True, slots=True)
class MarketDailyBarsDownload(ParquetArchiveDownload):
    """一次全市场日线 TAR 下载的落盘结果。"""


@dataclass(frozen=True, slots=True)
class AdjustmentFactorsDownload(ParquetArchiveDownload):
    """一次全市场后复权因子 TAR 下载的落盘结果。"""


@dataclass(frozen=True, slots=True)
class TradingStatusDownload(ParquetArchiveDownload):
    """一次全市场交易状态 TAR 下载的落盘结果。"""


@dataclass(frozen=True, slots=True)
class Instrument:
    symbol: str
    name: str
    exchange: str
    asset_type: str
    list_date: date | None
    delist_date: date | None
    prev_symbol: str | None

    @classmethod
    def from_payload(cls, payload: Payload) -> Instrument:
        return cls(
            symbol=_string(_required(payload, "symbol"), "symbol"),
            name=_string(_required(payload, "name"), "name"),
            exchange=_string(_required(payload, "exchange"), "exchange"),
            asset_type=_string(_required(payload, "asset_type"), "asset_type"),
            list_date=_optional(payload, "list_date", _date),
            delist_date=_optional(payload, "delist_date", _date),
            prev_symbol=_optional(payload, "prev_symbol", _string),
        )


@dataclass(frozen=True, slots=True)
class InstrumentPage:
    instruments: tuple[Instrument, ...]
    as_of: date | None
    next_cursor: str | None

    @classmethod
    def from_payload(cls, payload: Payload) -> InstrumentPage:
        return cls(
            instruments=_object_tuple(payload, "instruments", Instrument.from_payload),
            as_of=_optional(payload, "as_of", _date),
            next_cursor=_optional(payload, "next_cursor", _string),
        )


@dataclass(frozen=True, slots=True)
class TradingStatus:
    trade_date: date
    is_trading: bool
    status: TradingState
    risk_warning: bool | None

    @classmethod
    def from_payload(cls, payload: Payload) -> TradingStatus:
        return cls(
            trade_date=_date(_required(payload, "trade_date"), "trade_date"),
            is_trading=_boolean(_required(payload, "is_trading"), "is_trading"),
            status=cast(TradingState, _literal(payload, "status", _TRADING_STATES)),
            risk_warning=_optional(payload, "risk_warning", _boolean),
        )


@dataclass(frozen=True, slots=True)
class TradingStatusPage:
    symbol: str
    start: date
    end: date
    statuses: tuple[TradingStatus, ...]
    next_cursor: date | None

    @classmethod
    def from_payload(cls, payload: Payload) -> TradingStatusPage:
        return cls(
            symbol=_string(_required(payload, "symbol"), "symbol"),
            start=_date(_required(payload, "start"), "start"),
            end=_date(_required(payload, "end"), "end"),
            statuses=_object_tuple(payload, "statuses", TradingStatus.from_payload),
            next_cursor=_optional(payload, "next_cursor", _date),
        )


@dataclass(frozen=True, slots=True)
class Provenance:
    source: str
    data_version: str
    fetched_at: datetime | None

    @classmethod
    def from_payload(cls, payload: Payload) -> Provenance:
        return cls(
            source=_string(_required(payload, "source"), "source"),
            data_version=_string(_required(payload, "data_version"), "data_version"),
            fetched_at=_optional(
                payload,
                "fetched_at",
                lambda value, field: _datetime(value, field, require_timezone=True),
            ),
        )


@dataclass(frozen=True, slots=True)
class StockSummaryInstrument:
    symbol: str
    name: str
    exchange: str
    asset_type: str
    list_date: date | None
    delist_date: date | None
    prev_symbol: str | None
    provenance: Provenance

    @classmethod
    def from_payload(cls, payload: Payload) -> StockSummaryInstrument:
        return cls(
            symbol=_string(_required(payload, "symbol"), "symbol"),
            name=_string(_required(payload, "name"), "name"),
            exchange=_string(_required(payload, "exchange"), "exchange"),
            asset_type=_string(_required(payload, "asset_type"), "asset_type"),
            list_date=_optional(payload, "list_date", _date),
            delist_date=_optional(payload, "delist_date", _date),
            prev_symbol=_optional(payload, "prev_symbol", _string),
            provenance=Provenance.from_payload(
                _object(_required(payload, "provenance"), "provenance")
            ),
        )


@dataclass(frozen=True, slots=True)
class LatestMarket:
    trade_date: date
    open: float
    high: float
    low: float
    close: float
    volume: int | None
    amount: float | None
    provenance: Provenance

    @classmethod
    def from_payload(cls, payload: Payload) -> LatestMarket:
        return cls(
            trade_date=_date(_required(payload, "trade_date"), "trade_date"),
            open=_number(_required(payload, "open"), "open"),
            high=_number(_required(payload, "high"), "high"),
            low=_number(_required(payload, "low"), "low"),
            close=_number(_required(payload, "close"), "close"),
            volume=_optional(payload, "volume", _integer),
            amount=_optional(payload, "amount", _number),
            provenance=Provenance.from_payload(
                _object(_required(payload, "provenance"), "provenance")
            ),
        )


@dataclass(frozen=True, slots=True)
class SummaryTradingStatus:
    trade_date: date
    is_trading: bool
    status: TradingState
    risk_warning: bool | None
    provenance: Provenance

    @classmethod
    def from_payload(cls, payload: Payload) -> SummaryTradingStatus:
        return cls(
            trade_date=_date(_required(payload, "trade_date"), "trade_date"),
            is_trading=_boolean(_required(payload, "is_trading"), "is_trading"),
            status=cast(TradingState, _literal(payload, "status", _TRADING_STATES)),
            risk_warning=_optional(payload, "risk_warning", _boolean),
            provenance=Provenance.from_payload(
                _object(_required(payload, "provenance"), "provenance")
            ),
        )


@dataclass(frozen=True, slots=True)
class Valuation:
    trade_date: date
    pe_ttm: float | None
    pb: float | None
    ps_ttm: float | None
    total_mv: float | None
    float_mv: float | None
    provenance: Provenance

    @classmethod
    def from_payload(cls, payload: Payload) -> Valuation:
        return cls(
            trade_date=_date(_required(payload, "trade_date"), "trade_date"),
            pe_ttm=_optional(payload, "pe_ttm", _number),
            pb=_optional(payload, "pb", _number),
            ps_ttm=_optional(payload, "ps_ttm", _number),
            total_mv=_optional(payload, "total_mv", _number),
            float_mv=_optional(payload, "float_mv", _number),
            provenance=Provenance.from_payload(
                _object(_required(payload, "provenance"), "provenance")
            ),
        )


@dataclass(frozen=True, slots=True)
class StockSummaryMarket:
    latest_market: LatestMarket | None
    trading_status: SummaryTradingStatus | None
    valuation: Valuation | None

    @classmethod
    def from_payload(cls, payload: Payload) -> StockSummaryMarket:
        return cls(
            latest_market=_optional(payload, "latest_market", _parse_latest_market),
            trading_status=_optional(payload, "trading_status", _parse_summary_trading_status),
            valuation=_optional(payload, "valuation", _parse_valuation),
        )


@dataclass(frozen=True, slots=True)
class IndustryMembership:
    classification_system: str
    industry_code: str
    industry_name: str
    as_of_date: date
    provenance: Provenance

    @classmethod
    def from_payload(cls, payload: Payload) -> IndustryMembership:
        return cls(
            classification_system=_string(
                _required(payload, "classification_system"), "classification_system"
            ),
            industry_code=_string(_required(payload, "industry_code"), "industry_code"),
            industry_name=_string(_required(payload, "industry_name"), "industry_name"),
            as_of_date=_date(_required(payload, "as_of_date"), "as_of_date"),
            provenance=Provenance.from_payload(
                _object(_required(payload, "provenance"), "provenance")
            ),
        )


@dataclass(frozen=True, slots=True)
class SectorMembership:
    sector_code: str
    sector_name: str
    as_of_date: date
    provenance: Provenance

    @classmethod
    def from_payload(cls, payload: Payload) -> SectorMembership:
        return cls(
            sector_code=_string(_required(payload, "sector_code"), "sector_code"),
            sector_name=_string(_required(payload, "sector_name"), "sector_name"),
            as_of_date=_date(_required(payload, "as_of_date"), "as_of_date"),
            provenance=Provenance.from_payload(
                _object(_required(payload, "provenance"), "provenance")
            ),
        )


@dataclass(frozen=True, slots=True)
class IndexMembership:
    index_symbol: str
    as_of_date: date
    weight: float | None
    provenance: Provenance

    @classmethod
    def from_payload(cls, payload: Payload) -> IndexMembership:
        return cls(
            index_symbol=_string(_required(payload, "index_symbol"), "index_symbol"),
            as_of_date=_date(_required(payload, "as_of_date"), "as_of_date"),
            weight=_optional(payload, "weight", _number),
            provenance=Provenance.from_payload(
                _object(_required(payload, "provenance"), "provenance")
            ),
        )


@dataclass(frozen=True, slots=True)
class StockSummaryClassification:
    industries: tuple[IndustryMembership, ...]
    sectors: tuple[SectorMembership, ...]
    index_memberships: tuple[IndexMembership, ...]

    @classmethod
    def from_payload(cls, payload: Payload) -> StockSummaryClassification:
        return cls(
            industries=_object_tuple(payload, "industries", IndustryMembership.from_payload),
            sectors=_object_tuple(payload, "sectors", SectorMembership.from_payload),
            index_memberships=_object_tuple(
                payload, "index_memberships", IndexMembership.from_payload
            ),
        )


@dataclass(frozen=True, slots=True)
class FundFlow:
    trade_date: date
    main_net_inflow: float | None
    super_large_net_inflow: float | None
    large_net_inflow: float | None
    medium_net_inflow: float | None
    small_net_inflow: float | None
    provenance: Provenance

    @classmethod
    def from_payload(cls, payload: Payload) -> FundFlow:
        return cls(
            trade_date=_date(_required(payload, "trade_date"), "trade_date"),
            main_net_inflow=_optional(payload, "main_net_inflow", _number),
            super_large_net_inflow=_optional(payload, "super_large_net_inflow", _number),
            large_net_inflow=_optional(payload, "large_net_inflow", _number),
            medium_net_inflow=_optional(payload, "medium_net_inflow", _number),
            small_net_inflow=_optional(payload, "small_net_inflow", _number),
            provenance=Provenance.from_payload(
                _object(_required(payload, "provenance"), "provenance")
            ),
        )


@dataclass(frozen=True, slots=True)
class AnalystConsensus:
    forecast_date: date
    forecast_year: int | None
    eps_forecast: float | None
    pe_forecast: float | None
    target_price: float | None
    rating: str | None
    analyst_count: int | None
    provenance: Provenance

    @classmethod
    def from_payload(cls, payload: Payload) -> AnalystConsensus:
        return cls(
            forecast_date=_date(_required(payload, "forecast_date"), "forecast_date"),
            forecast_year=_optional(payload, "forecast_year", _integer),
            eps_forecast=_optional(payload, "eps_forecast", _number),
            pe_forecast=_optional(payload, "pe_forecast", _number),
            target_price=_optional(payload, "target_price", _number),
            rating=_optional(payload, "rating", _string),
            analyst_count=_optional(payload, "analyst_count", _integer),
            provenance=Provenance.from_payload(
                _object(_required(payload, "provenance"), "provenance")
            ),
        )


@dataclass(frozen=True, slots=True)
class HotRank:
    trade_date: date
    rank: int | None
    rank_change: int | None
    hist_rank: int | None
    provenance: Provenance

    @classmethod
    def from_payload(cls, payload: Payload) -> HotRank:
        return cls(
            trade_date=_date(_required(payload, "trade_date"), "trade_date"),
            rank=_optional(payload, "rank", _integer),
            rank_change=_optional(payload, "rank_change", _integer),
            hist_rank=_optional(payload, "hist_rank", _integer),
            provenance=Provenance.from_payload(
                _object(_required(payload, "provenance"), "provenance")
            ),
        )


@dataclass(frozen=True, slots=True)
class Sentiment:
    trade_date: date
    score_channel: str
    sentiment_score: float | None
    headline_count: int | None
    provenance: Provenance

    @classmethod
    def from_payload(cls, payload: Payload) -> Sentiment:
        return cls(
            trade_date=_date(_required(payload, "trade_date"), "trade_date"),
            score_channel=_string(_required(payload, "score_channel"), "score_channel"),
            sentiment_score=_optional(payload, "sentiment_score", _number),
            headline_count=_optional(payload, "headline_count", _integer),
            provenance=Provenance.from_payload(
                _object(_required(payload, "provenance"), "provenance")
            ),
        )


@dataclass(frozen=True, slots=True)
class StockSummarySignals:
    fund_flow: FundFlow | None
    analyst_consensus: AnalystConsensus | None
    hot_rank: HotRank | None
    sentiments: tuple[Sentiment, ...]

    @classmethod
    def from_payload(cls, payload: Payload) -> StockSummarySignals:
        return cls(
            fund_flow=_optional(payload, "fund_flow", _parse_fund_flow),
            analyst_consensus=_optional(payload, "analyst_consensus", _parse_analyst_consensus),
            hot_rank=_optional(payload, "hot_rank", _parse_hot_rank),
            sentiments=_object_tuple(payload, "sentiments", Sentiment.from_payload),
        )


@dataclass(frozen=True, slots=True)
class StockSummary:
    symbol: str
    instrument: StockSummaryInstrument
    market: StockSummaryMarket
    classification: StockSummaryClassification
    signals: StockSummarySignals
    unavailable_datasets: tuple[str, ...]

    @classmethod
    def from_payload(cls, payload: Payload) -> StockSummary:
        unavailable = _array(_required(payload, "unavailable_datasets"), "unavailable_datasets")
        return cls(
            symbol=_string(_required(payload, "symbol"), "symbol"),
            instrument=StockSummaryInstrument.from_payload(
                _object(_required(payload, "instrument"), "instrument")
            ),
            market=StockSummaryMarket.from_payload(_object(_required(payload, "market"), "market")),
            classification=StockSummaryClassification.from_payload(
                _object(_required(payload, "classification"), "classification")
            ),
            signals=StockSummarySignals.from_payload(
                _object(_required(payload, "signals"), "signals")
            ),
            unavailable_datasets=tuple(
                _string(value, f"unavailable_datasets[{index}]")
                for index, value in enumerate(unavailable)
            ),
        )


@dataclass(frozen=True, slots=True)
class DailyCandle:
    trade_date: date
    open: float
    high: float
    low: float
    close: float
    volume: int | None
    amount: float | None
    adjustment_factor: float | None
    adjustment_exact: bool | None

    @classmethod
    def from_payload(cls, payload: Payload) -> DailyCandle:
        return cls(
            trade_date=_date(_required(payload, "trade_date"), "trade_date"),
            open=_number(_required(payload, "open"), "open"),
            high=_number(_required(payload, "high"), "high"),
            low=_number(_required(payload, "low"), "low"),
            close=_number(_required(payload, "close"), "close"),
            volume=_optional(payload, "volume", _integer),
            amount=_optional(payload, "amount", _number),
            adjustment_factor=_optional(payload, "adjustment_factor", _number),
            adjustment_exact=_optional(payload, "adjustment_exact", _boolean),
        )


@dataclass(frozen=True, slots=True)
class IntradayCandle:
    trade_date: date
    bar_time: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int | None
    amount: float | None
    adjustment_factor: float | None
    adjustment_exact: bool | None

    @classmethod
    def from_payload(cls, payload: Payload) -> IntradayCandle:
        return cls(
            trade_date=_date(_required(payload, "trade_date"), "trade_date"),
            bar_time=_datetime(_required(payload, "bar_time"), "bar_time"),
            open=_number(_required(payload, "open"), "open"),
            high=_number(_required(payload, "high"), "high"),
            low=_number(_required(payload, "low"), "low"),
            close=_number(_required(payload, "close"), "close"),
            volume=_optional(payload, "volume", _integer),
            amount=_optional(payload, "amount", _number),
            adjustment_factor=_optional(payload, "adjustment_factor", _number),
            adjustment_exact=_optional(payload, "adjustment_exact", _boolean),
        )


@dataclass(frozen=True, slots=True)
class PeriodCandle:
    period_start: date
    period_end: date
    trade_date: date
    open: float
    high: float
    low: float
    close: float
    volume: int | None
    amount: float | None
    adjustment_factor: float | None
    adjustment_exact: bool | None

    @classmethod
    def from_payload(cls, payload: Payload) -> PeriodCandle:
        return cls(
            period_start=_date(_required(payload, "period_start"), "period_start"),
            period_end=_date(_required(payload, "period_end"), "period_end"),
            trade_date=_date(_required(payload, "trade_date"), "trade_date"),
            open=_number(_required(payload, "open"), "open"),
            high=_number(_required(payload, "high"), "high"),
            low=_number(_required(payload, "low"), "low"),
            close=_number(_required(payload, "close"), "close"),
            volume=_optional(payload, "volume", _integer),
            amount=_optional(payload, "amount", _number),
            adjustment_factor=_optional(payload, "adjustment_factor", _number),
            adjustment_exact=_optional(payload, "adjustment_exact", _boolean),
        )


Candle: TypeAlias = DailyCandle | IntradayCandle | PeriodCandle


@dataclass(frozen=True, slots=True)
class KlinePage:
    symbol: str
    interval: KlineInterval
    adjustment: Adjustment
    strict_adjustment: bool
    start: date
    end: date
    candles: tuple[Candle, ...]
    base_date: date | None
    base_factor_date: date | None
    next_cursor: date | datetime | None

    @classmethod
    def from_payload(cls, payload: Payload) -> KlinePage:
        interval = cast(KlineInterval, _literal(payload, "interval", _KLINE_INTERVALS))
        candle_parser: Callable[[Payload], Candle]
        cursor_parser: Callable[[Any, str], date | datetime]
        if interval == "1d":
            candle_parser = DailyCandle.from_payload
            cursor_parser = _date
        elif interval in {"1m", "5m"}:
            candle_parser = IntradayCandle.from_payload
            cursor_parser = _datetime
        else:
            candle_parser = PeriodCandle.from_payload
            cursor_parser = _date
        return cls(
            symbol=_string(_required(payload, "symbol"), "symbol"),
            interval=interval,
            adjustment=cast(Adjustment, _literal(payload, "adjustment", _ADJUSTMENTS)),
            strict_adjustment=_boolean(
                _required(payload, "strict_adjustment"), "strict_adjustment"
            ),
            start=_date(_required(payload, "start"), "start"),
            end=_date(_required(payload, "end"), "end"),
            candles=_object_tuple(payload, "candles", candle_parser),
            base_date=_optional(payload, "base_date", _date),
            base_factor_date=_optional(payload, "base_factor_date", _date),
            next_cursor=_optional(payload, "next_cursor", cursor_parser),
        )


@dataclass(frozen=True, slots=True)
class AdjustmentFactor:
    trade_date: date
    factor: float

    @classmethod
    def from_payload(cls, payload: Payload) -> AdjustmentFactor:
        return cls(
            trade_date=_date(_required(payload, "trade_date"), "trade_date"),
            factor=_number(_required(payload, "factor"), "factor"),
        )


@dataclass(frozen=True, slots=True)
class AdjustmentFactorPage:
    symbol: str
    adjustment: FactorAdjustment
    start: date
    end: date
    factors: tuple[AdjustmentFactor, ...]
    base_date: date | None
    base_factor_date: date | None
    next_cursor: date | None

    @classmethod
    def from_payload(cls, payload: Payload) -> AdjustmentFactorPage:
        return cls(
            symbol=_string(_required(payload, "symbol"), "symbol"),
            adjustment=cast(FactorAdjustment, _literal(payload, "adjustment", _FACTOR_ADJUSTMENTS)),
            start=_date(_required(payload, "start"), "start"),
            end=_date(_required(payload, "end"), "end"),
            factors=_object_tuple(payload, "factors", AdjustmentFactor.from_payload),
            base_date=_optional(payload, "base_date", _date),
            base_factor_date=_optional(payload, "base_factor_date", _date),
            next_cursor=_optional(payload, "next_cursor", _date),
        )


def _parse_latest_market(value: Any, field: str) -> LatestMarket:
    return LatestMarket.from_payload(_object(value, field))


def _parse_summary_trading_status(value: Any, field: str) -> SummaryTradingStatus:
    return SummaryTradingStatus.from_payload(_object(value, field))


def _parse_valuation(value: Any, field: str) -> Valuation:
    return Valuation.from_payload(_object(value, field))


def _parse_fund_flow(value: Any, field: str) -> FundFlow:
    return FundFlow.from_payload(_object(value, field))


def _parse_analyst_consensus(value: Any, field: str) -> AnalystConsensus:
    return AnalystConsensus.from_payload(_object(value, field))


def _parse_hot_rank(value: Any, field: str) -> HotRank:
    return HotRank.from_payload(_object(value, field))


__all__ = [
    "Adjustment",
    "AdjustmentFactor",
    "AdjustmentFactorPage",
    "AnalystConsensus",
    "Candle",
    "DailyCandle",
    "FactorAdjustment",
    "FundFlow",
    "Health",
    "HotRank",
    "IndexMembership",
    "IndustryMembership",
    "Instrument",
    "InstrumentPage",
    "IntradayCandle",
    "KlineInterval",
    "KlinePage",
    "LatestMarket",
    "PeriodCandle",
    "Provenance",
    "SectorMembership",
    "Sentiment",
    "StockSummary",
    "StockSummaryClassification",
    "StockSummaryInstrument",
    "StockSummaryMarket",
    "StockSummarySignals",
    "SummaryTradingStatus",
    "TradingState",
    "TradingStatus",
    "TradingStatusPage",
    "Valuation",
]
