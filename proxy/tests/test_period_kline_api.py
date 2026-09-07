"""周线、月线聚合 HTTP API。"""

from __future__ import annotations

from dataclasses import replace
from datetime import date
from pathlib import Path

from fastapi.testclient import TestClient

from cnequity_query_proxy.app import create_app
from cnequity_query_proxy.settings import ProxySettings
from conftest import write_bars, write_factor


def _window(**extra: object) -> dict[str, object]:
    return {"start": "2026-01-01", "end": "2026-01-06", **extra}


def test_weekly_kline_aggregates_daily_ohlcv_in_calendar_weeks(client: TestClient):
    response = client.get("/v1/kline/600519.SH", params=_window(interval="1w"))

    assert response.status_code == 200
    body = response.json()
    assert body["interval"] == "1w"
    assert body["candles"] == [
        {
            "period_start": "2025-12-29",
            "period_end": "2026-01-04",
            "trade_date": "2026-01-02",
            "open": 10.0,
            "high": 12.0,
            "low": 9.0,
            "close": 11.0,
            "volume": 100,
            "amount": 1100.0,
            "adjustment_factor": None,
            "adjustment_exact": None,
        },
        {
            "period_start": "2026-01-05",
            "period_end": "2026-01-11",
            "trade_date": "2026-01-06",
            "open": 11.0,
            "high": 14.0,
            "low": 10.0,
            "close": 13.0,
            "volume": 230,
            "amount": 2880.0,
            "adjustment_factor": None,
            "adjustment_exact": None,
        },
    ]


def test_monthly_kline_uses_calendar_month_bounds(client: TestClient):
    response = client.get("/v1/kline/600519.SH", params=_window(interval="1mo"))

    assert response.status_code == 200
    assert response.json()["candles"] == [
        {
            "period_start": "2026-01-01",
            "period_end": "2026-01-31",
            "trade_date": "2026-01-06",
            "open": 10.0,
            "high": 14.0,
            "low": 9.0,
            "close": 13.0,
            "volume": 330,
            "amount": 3980.0,
            "adjustment_factor": None,
            "adjustment_exact": None,
        }
    ]


def test_period_kline_applies_daily_adjustment_before_aggregation(
    lake_root: Path,
    client: TestClient,
):
    write_bars(
        lake_root,
        date(2026, 2, 2),
        [("600519.SH", date(2026, 2, 2), 10, 30, 5, 20, 10, 200, "tdx_protocol")],
    )
    write_bars(
        lake_root,
        date(2026, 2, 3),
        [("600519.SH", date(2026, 2, 3), 20, 100, 10, 50, 20, 1000, "tdx_protocol")],
    )
    write_factor(lake_root, date(2026, 2, 2), [("600519.SH", date(2026, 2, 2), "hfq", 4)])
    write_factor(lake_root, date(2026, 2, 3), [("600519.SH", date(2026, 2, 3), "hfq", 1)])

    response = client.get(
        "/v1/kline/600519.SH",
        params={"start": "2026-02-02", "end": "2026-02-03", "interval": "1w", "adjustment": "hfq"},
    )

    assert response.status_code == 200
    candle = response.json()["candles"][0]
    assert candle["open"] == 40.0
    assert candle["high"] == 120.0
    assert candle["low"] == 10.0
    assert candle["close"] == 50.0
    assert candle["adjustment_factor"] == 1.0
    assert candle["adjustment_exact"] is True


def test_period_kline_supports_qfq_and_keeps_the_explicit_base_date(client: TestClient):
    first = client.get(
        "/v1/kline/600519.SH",
        params=_window(interval="1w", adjustment="qfq", base_date="2026-01-06", limit=1),
    )

    assert first.status_code == 200
    assert first.json()["candles"][0]["close"] == 5.5
    assert first.json()["candles"][0]["adjustment_factor"] == 0.5
    assert first.json()["next_cursor"] == "2026-01-02"

    second = client.get(
        "/v1/kline/600519.SH",
        params=_window(
            interval="1w",
            adjustment="qfq",
            base_date="2026-01-06",
            limit=1,
            cursor=first.json()["next_cursor"],
        ),
    )

    assert second.status_code == 200
    assert second.json()["candles"][0]["close"] == 13.0
    assert second.json()["base_date"] == second.json()["base_factor_date"] == "2026-01-06"


def test_period_kline_marks_a_period_inexact_when_one_daily_factor_is_missing(
    lake_root: Path,
    client: TestClient,
):
    write_bars(
        lake_root,
        date(2026, 1, 7),
        [("600519.SH", date(2026, 1, 7), 13, 15, 12, 14, 130, 1820, "tdx_protocol")],
    )
    params = {"start": "2026-01-05", "end": "2026-01-07", "interval": "1w", "adjustment": "hfq"}

    strict = client.get("/v1/kline/600519.SH", params=params)
    permissive = client.get(
        "/v1/kline/600519.SH",
        params={**params, "strict_adjustment": "false"},
    )

    assert strict.status_code == 409
    assert permissive.status_code == 200
    assert permissive.json()["candles"][0]["adjustment_exact"] is False


def test_period_kline_obeys_the_daily_file_budget(settings: ProxySettings):
    client = TestClient(create_app(replace(settings, max_files=2)))

    response = client.get("/v1/kline/600519.SH", params=_window(interval="1mo"))

    assert response.status_code == 413
