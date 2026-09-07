"""1 分钟与 5 分钟 K 线 HTTP API。"""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

from fastapi.testclient import TestClient

from cnequity_query_proxy.app import create_app
from cnequity_query_proxy.settings import ProxySettings


def _window(**extra: object) -> dict[str, object]:
    return {"start": "2026-01-01", "end": "2026-01-06", **extra}


def test_one_minute_kline_returns_naive_close_timestamps(client: TestClient):
    response = client.get("/v1/kline/600519.sh", params=_window(interval="1m"))

    assert response.status_code == 200
    body = response.json()
    assert body["symbol"] == "600519.SH"
    assert body["interval"] == "1m"
    assert [candle["bar_time"] for candle in body["candles"]] == [
        "2026-01-02T09:31:00",
        "2026-01-02T09:32:00",
        "2026-01-05T09:31:00",
        "2026-01-05T09:32:00",
        "2026-01-06T09:31:00",
    ]
    assert body["candles"][0]["adjustment_factor"] is None
    assert body["candles"][0]["adjustment_exact"] is None


def test_five_minute_kline_uses_its_own_dataset_and_local_adjustment_factors(
    client: TestClient,
):
    hfq = client.get("/v1/kline/600519.SH", params=_window(interval="5m", adjustment="hfq"))
    qfq = client.get(
        "/v1/kline/600519.SH",
        params=_window(interval="5m", adjustment="qfq", base_date="2026-01-06"),
    )

    assert hfq.status_code == qfq.status_code == 200
    assert [row["bar_time"] for row in qfq.json()["candles"]] == [
        "2026-01-02T09:35:00",
        "2026-01-02T09:40:00",
        "2026-01-05T09:35:00",
        "2026-01-06T09:35:00",
    ]
    assert [row["close"] for row in hfq.json()["candles"]] == [21.0, 22.0, 48.0, 52.0]
    assert [row["close"] for row in qfq.json()["candles"]] == [5.25, 5.5, 12.0, 13.0]
    assert qfq.json()["base_date"] == qfq.json()["base_factor_date"] == "2026-01-06"
    assert all(row["adjustment_exact"] for row in qfq.json()["candles"])


def test_five_minute_kline_paginates_by_timestamp(client: TestClient):
    first = client.get("/v1/kline/600519.SH", params=_window(interval="5m", limit=2))

    assert first.status_code == 200
    assert first.json()["next_cursor"] == "2026-01-02T09:40:00"

    second = client.get(
        "/v1/kline/600519.SH",
        params=_window(interval="5m", limit=2, cursor=first.json()["next_cursor"]),
    )

    assert second.status_code == 200
    assert [row["bar_time"] for row in second.json()["candles"]] == [
        "2026-01-05T09:35:00",
        "2026-01-06T09:35:00",
    ]
    assert second.json()["next_cursor"] is None


def test_intraday_kline_rejects_a_date_only_cursor(client: TestClient):
    response = client.get(
        "/v1/kline/600519.SH",
        params=_window(interval="5m", cursor="2026-01-02"),
    )

    assert response.status_code == 422


def test_five_minute_kline_prunes_unrelated_partitions_before_duckdb_scans(
    lake_root: Path,
    client: TestClient,
):
    broken = lake_root / "curated" / "minute_bars_5m" / "trade_date=2025-01-02"
    broken.mkdir()
    (broken / "part-merged.parquet").write_bytes(b"not a parquet file")

    response = client.get("/v1/kline/600519.SH", params=_window(interval="5m"))

    assert response.status_code == 200


def test_five_minute_kline_enforces_its_own_short_window_limit(settings: ProxySettings):
    client = TestClient(
        create_app(
            replace(
                settings,
                default_five_minute_window_days=2,
                max_five_minute_window_days=2,
            )
        )
    )

    response = client.get("/v1/kline/600519.SH", params=_window(interval="5m"))

    assert response.status_code == 422


def test_all_kline_frequencies_share_the_disk_query_budget(settings: ProxySettings):
    app = create_app(replace(settings, max_concurrent_queries=1))

    slots = app.state.kline_service._slots
    assert app.state.minute_kline_service._slots is slots
    assert app.state.five_minute_kline_service._slots is slots
    assert app.state.weekly_kline_service._slots is slots
    assert app.state.monthly_kline_service._slots is slots
