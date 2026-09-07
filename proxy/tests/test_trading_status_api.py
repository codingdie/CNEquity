"""交易状态 HTTP API、数据契约与性能边界。"""

from __future__ import annotations

from dataclasses import replace
from datetime import date
from pathlib import Path

from fastapi.testclient import TestClient

from cnequity_query_proxy.app import create_app
from cnequity_query_proxy.settings import ProxySettings
from conftest import write_trading_status


def _window(**extra: object) -> dict[str, object]:
    return {"start": "2026-01-01", "end": "2026-01-06", **extra}


def test_trading_status_returns_one_symbol_in_trade_date_order(client: TestClient):
    response = client.get("/v1/trading-status/600519.sh", params=_window())

    assert response.status_code == 200
    body = response.json()
    assert body == {
        "symbol": "600519.SH",
        "start": "2026-01-01",
        "end": "2026-01-06",
        "statuses": [
            {
                "trade_date": "2026-01-02",
                "is_trading": True,
                "status": "normal",
                "risk_warning": False,
            },
            {
                "trade_date": "2026-01-05",
                "is_trading": False,
                "status": "suspended",
                "risk_warning": True,
            },
            {
                "trade_date": "2026-01-06",
                "is_trading": False,
                "status": "delisted",
                "risk_warning": None,
            },
        ],
        "next_cursor": None,
    }
    assert response.headers["x-cache"] == "MISS"

    missing = client.get("/v1/trading-status/600000.SH", params=_window())
    assert missing.status_code == 200
    assert missing.json()["statuses"] == []


def test_trading_status_supports_state_and_boolean_filters(client: TestClient):
    suspended = client.get(
        "/v1/trading-status/600519.SH",
        params=_window(status="SUSPENDED"),
    )
    non_trading = client.get(
        "/v1/trading-status/600519.SH",
        params=_window(is_trading="false"),
    )
    warning = client.get(
        "/v1/trading-status/600519.SH",
        params=_window(risk_warning="true"),
    )
    other_symbol = client.get("/v1/trading-status/000001.SZ", params=_window())

    assert suspended.status_code == non_trading.status_code == warning.status_code == 200
    assert [item["trade_date"] for item in suspended.json()["statuses"]] == ["2026-01-05"]
    assert [item["trade_date"] for item in non_trading.json()["statuses"]] == [
        "2026-01-05",
        "2026-01-06",
    ]
    assert [item["trade_date"] for item in warning.json()["statuses"]] == ["2026-01-05"]
    assert other_symbol.status_code == 200
    assert other_symbol.json()["statuses"] == [
        {
            "trade_date": "2026-01-02",
            "is_trading": True,
            "status": "normal",
            "risk_warning": True,
        }
    ]


def test_trading_status_paginates_by_date_and_uses_the_response_cache(client: TestClient):
    path = "/v1/trading-status/600519.SH"
    first = client.get(path, params=_window(limit=2))

    assert first.status_code == 200
    assert [item["trade_date"] for item in first.json()["statuses"]] == [
        "2026-01-02",
        "2026-01-05",
    ]
    assert first.json()["next_cursor"] == "2026-01-05"
    assert first.headers["x-cache"] == "MISS"

    cached = client.get(path, params=_window(limit=2))
    assert cached.headers["x-cache"] == "HIT"

    second = client.get(
        path,
        params=_window(limit=2, cursor=first.json()["next_cursor"]),
    )
    assert second.status_code == 200
    assert [item["trade_date"] for item in second.json()["statuses"]] == ["2026-01-06"]
    assert second.json()["next_cursor"] is None


def test_trading_status_prunes_unrelated_month_partitions_before_duckdb_scans(
    lake_root: Path,
    client: TestClient,
):
    broken = lake_root / "curated" / "trading_status" / "trade_date=2025-12"
    broken.mkdir()
    (broken / "part-merged.parquet").write_bytes(b"not a parquet file")

    response = client.get("/v1/trading-status/600519.SH", params=_window())

    assert response.status_code == 200


def test_trading_status_rejects_a_query_that_exceeds_the_file_budget(
    lake_root: Path,
    settings: ProxySettings,
):
    write_trading_status(
        lake_root,
        date(2026, 2, 1),
        [("600519.SH", date(2026, 2, 2), True, "normal", False)],
    )
    client = TestClient(create_app(replace(settings, max_files=1)))

    response = client.get(
        "/v1/trading-status/600519.SH",
        params={"start": "2026-01-01", "end": "2026-02-02"},
    )

    assert response.status_code == 413


def test_trading_status_rejects_invalid_or_expensive_requests(client: TestClient):
    assert client.get("/v1/trading-status/not-a-symbol", params=_window()).status_code == 422
    assert (
        client.get("/v1/trading-status/600519.SH", params=_window(status="st")).status_code == 422
    )
    assert (
        client.get(
            "/v1/trading-status/600519.SH",
            params=_window(start="2026-01-06", end="2026-01-01"),
        ).status_code
        == 422
    )
    assert (
        client.get(
            "/v1/trading-status/600519.SH",
            params={"start": "2025-01-01", "end": "2026-01-06"},
        ).status_code
        == 422
    )
    assert client.get("/v1/trading-status/600519.SH", params=_window(limit=101)).status_code == 422
    assert (
        client.get(
            "/v1/trading-status/600519.SH",
            params=_window(cursor="not-a-date"),
        ).status_code
        == 422
    )
    assert (
        client.get(
            "/v1/trading-status/600519.SH",
            params=_window(cursor="2025-12-31"),
        ).status_code
        == 422
    )
    assert (
        client.get(
            "/v1/trading-status/600519.SH",
            params=_window(is_trading="not-a-boolean"),
        ).status_code
        == 422
    )
    assert (
        client.get(
            "/v1/trading-status/600519.SH",
            params=_window(risk_warning="not-a-boolean"),
        ).status_code
        == 422
    )


def test_trading_status_reports_an_unavailable_local_directory(lake_root: Path, client: TestClient):
    status_root = lake_root / "curated" / "trading_status"
    status_root.rename(lake_root / "curated" / "trading_status-unavailable")

    assert client.get("/v1/trading-status/600519.SH", params=_window()).status_code == 503


def test_trading_status_shares_the_disk_query_budget_with_all_other_services(
    settings: ProxySettings,
):
    app = create_app(replace(settings, max_concurrent_queries=1))

    slots = app.state.kline_service._slots
    assert app.state.adjustment_factor_service._slots is slots
    assert app.state.instrument_service._slots is slots
    assert app.state.trading_status_service._slots is slots
    assert app.state.minute_kline_service._slots is slots
    assert app.state.five_minute_kline_service._slots is slots
    assert app.state.weekly_kline_service._slots is slots
    assert app.state.monthly_kline_service._slots is slots


def test_trading_status_never_writes_to_the_data_lake(lake_root: Path, client: TestClient):
    before = sorted(path.relative_to(lake_root) for path in lake_root.rglob("*"))
    response = client.get("/v1/trading-status/600519.SH", params=_window())
    after = sorted(path.relative_to(lake_root) for path in lake_root.rglob("*"))

    assert response.status_code == 200
    assert after == before
