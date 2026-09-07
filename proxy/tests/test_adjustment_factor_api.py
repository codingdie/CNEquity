"""复权因子 HTTP API、基准日与性能边界。"""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

from fastapi.testclient import TestClient

from cnequity_query_proxy.app import create_app
from cnequity_query_proxy.settings import ProxySettings


def _window(**extra: object) -> dict[str, object]:
    return {"start": "2026-01-01", "end": "2026-01-06", **extra}


def test_hfq_factors_return_the_local_cumulative_series(client: TestClient):
    response = client.get("/v1/adjustment-factors/600519.sh", params=_window())

    assert response.status_code == 200
    body = response.json()
    assert body["symbol"] == "600519.SH"
    assert body["adjustment"] == "hfq"
    assert body["base_date"] is None
    assert [item["trade_date"] for item in body["factors"]] == [
        "2026-01-02",
        "2026-01-05",
        "2026-01-06",
    ]
    assert [item["factor"] for item in body["factors"]] == [2.0, 4.0, 4.0]
    assert response.headers["x-cache"] == "MISS"


def test_qfq_factors_are_normalized_to_the_explicit_base_date(client: TestClient):
    response = client.get(
        "/v1/adjustment-factors/600519.SH",
        params=_window(adjustment="qfq", base_date="2026-01-06"),
    )

    assert response.status_code == 200
    body = response.json()
    assert body["adjustment"] == "qfq"
    assert [item["factor"] for item in body["factors"]] == [0.5, 1.0, 1.0]
    assert body["base_date"] == body["base_factor_date"] == "2026-01-06"


def test_qfq_factor_keeps_the_base_date_across_pages(client: TestClient):
    first = client.get(
        "/v1/adjustment-factors/600519.SH",
        params=_window(adjustment="qfq", base_date="2026-01-06", limit=1),
    )
    assert first.status_code == 200
    assert first.json()["factors"][0]["factor"] == 0.5
    assert first.json()["next_cursor"] == "2026-01-02"

    second = client.get(
        "/v1/adjustment-factors/600519.SH",
        params=_window(
            adjustment="qfq",
            base_date="2026-01-06",
            limit=1,
            cursor=first.json()["next_cursor"],
        ),
    )
    assert second.status_code == 200
    assert second.json()["factors"][0]["factor"] == 1.0
    assert second.json()["base_date"] == "2026-01-06"


def test_qfq_factor_accepts_a_base_date_outside_the_query_window(client: TestClient):
    response = client.get(
        "/v1/adjustment-factors/600519.SH",
        params={
            "start": "2026-01-02",
            "end": "2026-01-05",
            "adjustment": "qfq",
            "base_date": "2026-01-06",
        },
    )

    assert response.status_code == 200
    assert [item["factor"] for item in response.json()["factors"]] == [0.5, 1.0]
    assert response.json()["base_date"] == "2026-01-06"


def test_qfq_factor_rejects_a_missing_base_factor(client: TestClient):
    response = client.get(
        "/v1/adjustment-factors/600519.SH",
        params=_window(adjustment="qfq", base_date="2026-01-04"),
    )

    assert response.status_code == 409


def test_adjustment_factor_rejects_invalid_adjustment_requests(client: TestClient):
    qfq_without_base = client.get(
        "/v1/adjustment-factors/600519.SH",
        params=_window(adjustment="qfq"),
    )
    hfq_with_base = client.get(
        "/v1/adjustment-factors/600519.SH",
        params=_window(adjustment="hfq", base_date="2026-01-06"),
    )
    conflicting_aliases = client.get(
        "/v1/adjustment-factors/600519.SH",
        params=_window(adjustment="hfq", adjust="qfq", base_date="2026-01-06"),
    )
    unsupported_adjustment = client.get(
        "/v1/adjustment-factors/600519.SH",
        params=_window(adjustment="none"),
    )

    assert qfq_without_base.status_code == 422
    assert hfq_with_base.status_code == 422
    assert conflicting_aliases.status_code == 422
    assert unsupported_adjustment.status_code == 422


def test_adjustment_factor_uses_the_response_cache(client: TestClient):
    path = "/v1/adjustment-factors/600519.SH"
    assert client.get(path, params=_window()).headers["x-cache"] == "MISS"
    assert client.get(path, params=_window()).headers["x-cache"] == "HIT"


def test_adjustment_factor_does_not_scan_daily_bar_partitions(lake_root: Path, client: TestClient):
    broken = lake_root / "curated" / "daily_bars" / "trade_date=2025-01-02"
    broken.mkdir()
    (broken / "part-merged.parquet").write_bytes(b"not a parquet file")

    response = client.get("/v1/adjustment-factors/600519.SH", params=_window())

    assert response.status_code == 200


def test_adjustment_factor_rejects_a_query_that_exceeds_the_file_budget(
    settings: ProxySettings,
):
    client = TestClient(create_app(replace(settings, max_files=2)))

    response = client.get("/v1/adjustment-factors/600519.SH", params=_window())

    assert response.status_code == 413


def test_adjustment_factor_and_kline_share_the_disk_query_budget(settings: ProxySettings):
    app = create_app(replace(settings, max_concurrent_queries=1))

    assert app.state.kline_service._slots is app.state.adjustment_factor_service._slots


def test_adjustment_factor_never_writes_to_the_data_lake(lake_root: Path, client: TestClient):
    before = sorted(path.relative_to(lake_root) for path in lake_root.rglob("*"))
    response = client.get("/v1/adjustment-factors/600519.SH", params=_window())
    after = sorted(path.relative_to(lake_root) for path in lake_root.rglob("*"))

    assert response.status_code == 200
    assert after == before
