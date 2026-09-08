"""QueryClient 的 HTTP 契约、分页和类型化解析。"""

from __future__ import annotations

import io
import tarfile
from collections.abc import Callable
from datetime import date, datetime, timezone
from typing import Any

import httpx
import pytest

from cnequity_query_sdk import (
    AdjustmentFactor,
    AdjustmentFactorsDownload,
    APIError,
    AuthenticationError,
    ConflictError,
    DailyCandle,
    DragonTigerDownload,
    IntradayCandle,
    NotFoundError,
    PeriodCandle,
    QueryClient,
    RateLimitError,
    RequestTooLargeError,
    RequestValidationError,
    ResponseDecodeError,
    ServiceUnavailableError,
    TradingCalendarDownload,
    TradingStatus,
    TradingStatusDownload,
    TransportError,
)


def _response(
    request: httpx.Request,
    payload: dict[str, Any],
    *,
    status_code: int = 200,
    headers: dict[str, str] | None = None,
) -> httpx.Response:
    return httpx.Response(status_code, json=payload, headers=headers, request=request)


def _client(handler: Callable[[httpx.Request], httpx.Response]) -> QueryClient:
    return QueryClient(
        "https://proxy.example/prefix",
        api_key="secret",
        transport=httpx.MockTransport(handler),
    )


def _parquet_archive(member_name: str) -> bytes:
    output = io.BytesIO()
    with tarfile.open(fileobj=output, mode="w:") as archive:
        content = b"parquet-bytes"
        member = tarfile.TarInfo(member_name)
        member.size = len(content)
        archive.addfile(member, io.BytesIO(content))
    return output.getvalue()


def _daily_bars_archive() -> bytes:
    return _parquet_archive("curated/daily_bars/trade_date=2026-01-02/part-merged.parquet")


def _instrument(symbol: str = "600519.SH") -> dict[str, Any]:
    return {
        "symbol": symbol,
        "name": "贵州茅台",
        "exchange": "SH",
        "asset_type": "stock",
        "list_date": "2001-08-27",
        "delist_date": None,
        "prev_symbol": None,
    }


def _provenance() -> dict[str, Any]:
    return {
        "source": "eastmoney",
        "data_version": "v1",
        "fetched_at": "2026-01-06T09:00:00Z",
    }


def _summary_payload() -> dict[str, Any]:
    return {
        "symbol": "600519.SH",
        "instrument": {**_instrument(), "provenance": _provenance()},
        "market": {
            "latest_market": {
                "trade_date": "2026-01-06",
                "open": 12.0,
                "high": 14.0,
                "low": 11.0,
                "close": 13.0,
                "volume": 120,
                "amount": 1560.0,
                "provenance": _provenance(),
            },
            "trading_status": {
                "trade_date": "2026-01-06",
                "is_trading": True,
                "status": "normal",
                "risk_warning": False,
                "provenance": _provenance(),
            },
            "valuation": {
                "trade_date": "2026-01-06",
                "pe_ttm": 18.4,
                "pb": 6.5,
                "ps_ttm": None,
                "total_mv": 1645000000000.0,
                "float_mv": 1645000000000.0,
                "provenance": _provenance(),
            },
        },
        "classification": {
            "industries": [
                {
                    "classification_system": "eastmoney",
                    "industry_code": "1277",
                    "industry_name": "白酒Ⅱ",
                    "as_of_date": "2026-01-06",
                    "provenance": _provenance(),
                }
            ],
            "sectors": [
                {
                    "sector_code": "1277",
                    "sector_name": "白酒Ⅱ",
                    "as_of_date": "2026-01-06",
                    "provenance": _provenance(),
                }
            ],
            "index_memberships": [
                {
                    "index_symbol": "000300.SH",
                    "as_of_date": "2026-01-06",
                    "weight": None,
                    "provenance": _provenance(),
                }
            ],
        },
        "signals": {
            "fund_flow": None,
            "analyst_consensus": None,
            "hot_rank": None,
            "sentiments": [],
        },
        "unavailable_datasets": [],
    }


def _daily_page(*, next_cursor: str | None = None) -> dict[str, Any]:
    return {
        "symbol": "600519.SH",
        "interval": "1d",
        "adjustment": "none",
        "strict_adjustment": True,
        "start": "2026-01-01",
        "end": "2026-01-06",
        "candles": [
            {
                "trade_date": "2026-01-02",
                "open": 10.0,
                "high": 12.0,
                "low": 9.0,
                "close": 11.0,
                "volume": 100,
                "amount": 1100.0,
                "adjustment_factor": None,
                "adjustment_exact": None,
            }
        ],
        "base_date": None,
        "base_factor_date": None,
        "next_cursor": next_cursor,
    }


def _status_page(*, next_cursor: str | None = None) -> dict[str, Any]:
    return {
        "symbol": "600519.SH",
        "start": "2026-01-01",
        "end": "2026-01-06",
        "statuses": [
            {
                "trade_date": "2026-01-02",
                "is_trading": True,
                "status": "normal",
                "risk_warning": False,
            }
        ],
        "next_cursor": next_cursor,
    }


def _factor_page(*, next_cursor: str | None = None) -> dict[str, Any]:
    return {
        "symbol": "600519.SH",
        "adjustment": "hfq",
        "start": "2026-01-01",
        "end": "2026-01-06",
        "factors": [{"trade_date": "2026-01-02", "factor": 2.0}],
        "base_date": None,
        "base_factor_date": None,
        "next_cursor": next_cursor,
    }


def test_list_instruments_uses_the_proxy_prefix_auth_and_typed_dates():
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        assert request.url.path == "/prefix/v1/instruments"
        assert request.headers["authorization"] == "Bearer secret"
        assert dict(request.url.params) == {
            "symbol": "600519.SH",
            "q": "茅台",
            "exchange": "SH",
            "asset_type": "stock",
            "as_of": "2026-01-06",
            "limit": "20",
        }
        return _response(
            request,
            {"instruments": [_instrument()], "as_of": "2026-01-06", "next_cursor": None},
        )

    with _client(handler) as client:
        page = client.list_instruments(
            symbol="600519.sh",
            query="茅台",
            exchange="sh",
            asset_type="STOCK",
            as_of=date(2026, 1, 6),
            limit=20,
        )

    assert page.instruments[0].name == "贵州茅台"
    assert page.as_of == date(2026, 1, 6)


def test_stock_summary_parses_latest_market_and_provenance():
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/prefix/v1/stocks/600519.SH/summary"
        return _response(request, _summary_payload())

    with _client(handler) as client:
        summary = client.get_stock_summary("600519.sh")

    assert summary.market.latest_market is not None
    assert summary.market.latest_market.close == 13.0
    assert summary.market.latest_market.trade_date == date(2026, 1, 6)
    assert summary.instrument.provenance.fetched_at == datetime(2026, 1, 6, 9, tzinfo=timezone.utc)
    assert summary.classification.index_memberships[0].weight is None


def test_download_market_daily_bars_streams_to_a_file_without_json(tmp_path):
    archive = _daily_bars_archive()

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        assert request.url.path == "/prefix/v1/kline/batch"
        assert dict(request.url.params) == {"start": "2026-01-01", "end": "2026-01-06"}
        assert request.headers["accept"] == "application/x-tar"
        return httpx.Response(
            200,
            content=archive,
            headers={
                "Content-Type": "application/x-tar",
                "X-CNEQUITY-Data-Files": "3",
                "X-CNEQUITY-Data-Bytes": "12345",
            },
            request=request,
        )

    target = tmp_path / "daily-bars.tar"
    with _client(handler) as client:
        result = client.download_market_daily_bars(
            target,
            start=date(2026, 1, 1),
            end=date(2026, 1, 6),
            chunk_size=3,
        )

    assert result.path == target
    assert result.bytes_written == len(archive)
    assert result.data_file_count == 3
    assert result.data_bytes == 12345
    assert target.read_bytes() == archive
    assert not list(tmp_path.glob("*.part"))


def test_download_market_daily_bars_fails_loudly_and_leaves_no_partial_file(tmp_path):
    target = tmp_path / "daily-bars.tar"

    def rejected(request: httpx.Request) -> httpx.Response:
        return _response(request, {"detail": "too large"}, status_code=413)

    with _client(rejected) as client:
        with pytest.raises(RequestTooLargeError, match="too large"):
            client.download_market_daily_bars(
                target,
                start=date(2026, 1, 1),
                end=date(2026, 1, 6),
            )

    assert not target.exists()
    assert not list(tmp_path.glob("*.part"))

    target.write_bytes(b"existing")
    with _client(rejected) as client:
        with pytest.raises(FileExistsError):
            client.download_market_daily_bars(
                target,
                start=date(2026, 1, 1),
                end=date(2026, 1, 6),
            )
        with pytest.raises(ValueError, match="start"):
            client.download_market_daily_bars(
                tmp_path / "other.tar",
                start=date(2026, 1, 6),
                end=date(2026, 1, 1),
            )


@pytest.mark.parametrize(
    ("method_name", "endpoint", "member_name", "result_type"),
    [
        (
            "download_market_adjustment_factors",
            "/prefix/v1/adjustment-factors/batch",
            "derived/adj_factors/trade_date=2026-01-02/part-merged.parquet",
            AdjustmentFactorsDownload,
        ),
        (
            "download_market_trading_status",
            "/prefix/v1/trading-status/batch",
            "curated/trading_status/trade_date=2026-01/part-merged.parquet",
            TradingStatusDownload,
        ),
        (
            "download_market_trading_calendar",
            "/prefix/v1/trading-calendar/batch",
            "curated/trading_calendar/trade_date=2026/part-merged.parquet",
            TradingCalendarDownload,
        ),
        (
            "download_market_dragon_tiger",
            "/prefix/v1/dragon-tiger/batch",
            "curated/dragon_tiger/trade_date=2026-01/part-merged.parquet",
            DragonTigerDownload,
        ),
    ],
)
def test_download_market_batch_data_streams_parquet_archives(
    tmp_path,
    method_name: str,
    endpoint: str,
    member_name: str,
    result_type: type[AdjustmentFactorsDownload]
    | type[TradingCalendarDownload]
    | type[TradingStatusDownload]
    | type[DragonTigerDownload],
):
    archive = _parquet_archive(member_name)

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        assert request.url.path == endpoint
        assert dict(request.url.params) == {"start": "2026-01-01", "end": "2026-01-06"}
        assert request.headers["accept"] == "application/x-tar"
        return httpx.Response(
            200,
            content=archive,
            headers={
                "Content-Type": "application/x-tar",
                "X-CNEQUITY-Data-Files": "1",
                "X-CNEQUITY-Data-Bytes": "13",
            },
            request=request,
        )

    target = tmp_path / f"{method_name}.tar"
    with _client(handler) as client:
        result = getattr(client, method_name)(
            target,
            start=date(2026, 1, 1),
            end=date(2026, 1, 6),
            chunk_size=3,
        )

    assert isinstance(result, result_type)
    assert result.path == target
    assert result.bytes_written == len(archive)
    assert result.data_file_count == 1
    assert result.data_bytes == 13
    assert target.read_bytes() == archive


def test_trading_status_and_factor_parameters_are_forwarded():
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/trading-status/600519.SH"):
            assert dict(request.url.params) == {
                "start": "2026-01-01",
                "end": "2026-01-06",
                "status": "suspended",
                "is_trading": "false",
                "risk_warning": "true",
                "limit": "2",
            }
            return _response(request, _status_page())
        assert request.url.path.endswith("/adjustment-factors/600519.SH")
        assert dict(request.url.params) == {
            "start": "2026-01-01",
            "end": "2026-01-06",
            "adjustment": "qfq",
            "base_date": "2026-01-06",
            "limit": "2",
        }
        return _response(
            request,
            {
                **_factor_page(),
                "adjustment": "qfq",
                "base_date": "2026-01-06",
                "base_factor_date": "2026-01-06",
            },
        )

    with _client(handler) as client:
        statuses = client.get_trading_status(
            "600519.SH",
            start=date(2026, 1, 1),
            end=date(2026, 1, 6),
            status="suspended",
            is_trading=False,
            risk_warning=True,
            limit=2,
        )
        factors = client.get_adjustment_factors(
            "600519.SH",
            start=date(2026, 1, 1),
            end=date(2026, 1, 6),
            adjustment="qfq",
            base_date=date(2026, 1, 6),
            limit=2,
        )

    assert statuses.statuses[0].status == "normal"
    assert factors.adjustment == "qfq"
    assert factors.base_date == date(2026, 1, 6)


def test_get_kline_parses_intraday_and_period_candles():
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.params["interval"] == "5m":
            assert request.url.path == "/prefix/v1/kline/600519.SH"
            assert request.url.params["adjustment"] == "qfq"
            assert request.url.params["strict_adjustment"] == "false"
            return _response(
                request,
                {
                    "symbol": "600519.SH",
                    "interval": "5m",
                    "adjustment": "qfq",
                    "strict_adjustment": False,
                    "start": "2026-01-06",
                    "end": "2026-01-06",
                    "candles": [
                        {
                            "trade_date": "2026-01-06",
                            "bar_time": "2026-01-06T09:35:00",
                            "open": 12.0,
                            "high": 13.0,
                            "low": 11.0,
                            "close": 12.5,
                            "volume": 100,
                            "amount": 1250.0,
                            "adjustment_factor": 1.0,
                            "adjustment_exact": True,
                        }
                    ],
                    "base_date": "2026-01-06",
                    "base_factor_date": "2026-01-06",
                    "next_cursor": "2026-01-06T09:35:00",
                },
            )
        return _response(
            request,
            {
                "symbol": "600519.SH",
                "interval": "1w",
                "adjustment": "hfq",
                "strict_adjustment": True,
                "start": "2026-01-01",
                "end": "2026-01-31",
                "candles": [
                    {
                        "period_start": "2026-01-05",
                        "period_end": "2026-01-11",
                        "trade_date": "2026-01-09",
                        "open": 12.0,
                        "high": 14.0,
                        "low": 11.0,
                        "close": 13.0,
                        "volume": 120,
                        "amount": 1560.0,
                        "adjustment_factor": 2.0,
                        "adjustment_exact": True,
                    }
                ],
                "base_date": None,
                "base_factor_date": None,
                "next_cursor": None,
            },
        )

    with _client(handler) as client:
        intraday = client.get_kline(
            "600519.SH",
            start=date(2026, 1, 6),
            end=date(2026, 1, 6),
            interval="5m",
            adjustment="qfq",
            base_date=date(2026, 1, 6),
            strict_adjustment=False,
        )
        period = client.get_kline("600519.SH", interval="1w", adjustment="hfq")

    assert isinstance(intraday.candles[0], IntradayCandle)
    assert intraday.next_cursor == datetime(2026, 1, 6, 9, 35)
    assert isinstance(period.candles[0], PeriodCandle)
    assert period.candles[0].period_start == date(2026, 1, 5)


def test_iterators_follow_all_supported_cursor_types():
    requests: list[tuple[str, str | None]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        cursor = request.url.params.get("cursor")
        requests.append((request.url.path, cursor))
        if request.url.path.endswith("/instruments"):
            if cursor is None:
                return _response(
                    request,
                    {
                        "instruments": [_instrument("000001.SZ")],
                        "as_of": None,
                        "next_cursor": "000001.SZ",
                    },
                )
            return _response(
                request, {"instruments": [_instrument()], "as_of": None, "next_cursor": None}
            )
        if request.url.path.endswith("/trading-status/600519.SH"):
            return _response(request, _status_page(next_cursor=None))
        if request.url.path.endswith("/kline/600519.SH"):
            return _response(request, _daily_page(next_cursor=None))
        if request.url.path.endswith("/adjustment-factors/600519.SH"):
            return _response(request, _factor_page(next_cursor=None))
        raise AssertionError(f"unexpected path: {request.url.path}")

    with _client(handler) as client:
        instruments = list(client.iter_instruments(limit=1))
        statuses = list(client.iter_trading_status("600519.SH", limit=1))
        candles = list(client.iter_kline("600519.SH", limit=1))
        factors = list(client.iter_adjustment_factors("600519.SH", limit=1))

    assert [instrument.symbol for instrument in instruments] == ["000001.SZ", "600519.SH"]
    assert isinstance(statuses[0], TradingStatus)
    assert isinstance(candles[0], DailyCandle)
    assert isinstance(factors[0], AdjustmentFactor)
    assert requests[1] == ("/prefix/v1/instruments", "000001.SZ")


@pytest.mark.parametrize(
    ("status_code", "error_type"),
    [
        (401, AuthenticationError),
        (404, NotFoundError),
        (409, ConflictError),
        (413, RequestTooLargeError),
        (422, RequestValidationError),
        (429, RateLimitError),
        (503, ServiceUnavailableError),
    ],
)
def test_http_errors_are_mapped_to_sdk_exceptions(status_code: int, error_type: type[APIError]):
    def handler(request: httpx.Request) -> httpx.Response:
        return _response(
            request,
            {"detail": "contract failure"},
            status_code=status_code,
            headers={"Retry-After": "1"} if status_code == 429 else None,
        )

    with _client(handler) as client:
        with pytest.raises(error_type) as caught:
            client.list_instruments()

    assert caught.value.detail == "contract failure"
    assert caught.value.retry_after == (1.0 if status_code == 429 else None)


def test_invalid_responses_transport_failures_and_invalid_limits_fail_loudly():
    def invalid_response(request: httpx.Request) -> httpx.Response:
        return _response(request, {"status": 1})

    with _client(invalid_response) as client:
        with pytest.raises(ResponseDecodeError):
            client.health()

    def transport_failure(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("offline", request=request)

    with _client(transport_failure) as client:
        with pytest.raises(TransportError):
            client.health()

    with _client(invalid_response) as client:
        with pytest.raises(ValueError, match="limit"):
            client.list_instruments(limit=0)


def test_iterators_reject_a_non_advancing_cursor():
    def handler(request: httpx.Request) -> httpx.Response:
        return _response(
            request,
            {"instruments": [], "as_of": None, "next_cursor": "600519.SH"},
        )

    with _client(handler) as client:
        with pytest.raises(ResponseDecodeError, match="没有推进"):
            list(client.iter_instruments(cursor="600519.SH"))
