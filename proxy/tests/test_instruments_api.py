"""证券基础信息 HTTP API 与独立读取边界。"""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path

from fastapi.testclient import TestClient

from cnequity_query_proxy.app import create_app
from cnequity_query_proxy.settings import ProxySettings


def test_instruments_returns_business_fields_and_supports_filters(client: TestClient):
    response = client.get(
        "/v1/instruments",
        params={"exchange": "sh", "asset_type": "stock", "as_of": "2026-01-01"},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["as_of"] == "2026-01-01"
    assert body["next_cursor"] is None
    assert body["instruments"] == [
        {
            "symbol": "600519.SH",
            "name": "贵州茅台",
            "exchange": "SH",
            "asset_type": "stock",
            "list_date": "2001-08-27",
            "delist_date": None,
            "prev_symbol": None,
        }
    ]
    assert response.headers["x-cache"] == "MISS"

    search = client.get("/v1/instruments", params={"q": "茅台"})
    assert search.status_code == 200
    assert [item["symbol"] for item in search.json()["instruments"]] == ["600519.SH"]


def test_instruments_can_lookup_an_exact_symbol_and_preserve_delisted_records(client: TestClient):
    response = client.get("/v1/instruments", params={"symbol": "600001.sh"})

    assert response.status_code == 200
    assert response.json()["instruments"][0]["prev_symbol"] == "600001.OLD"
    assert response.json()["instruments"][0]["delist_date"] == "2020-01-01"


def test_instruments_paginates_by_symbol_and_uses_the_response_cache(client: TestClient):
    first = client.get("/v1/instruments", params={"limit": 2})

    assert first.status_code == 200
    assert [item["symbol"] for item in first.json()["instruments"]] == [
        "000001.SZ",
        "159915.SZ",
    ]
    assert first.json()["next_cursor"] == "159915.SZ"
    assert first.headers["x-cache"] == "MISS"

    cached = client.get("/v1/instruments", params={"limit": 2})
    assert cached.headers["x-cache"] == "HIT"

    second = client.get(
        "/v1/instruments",
        params={"limit": 2, "cursor": first.json()["next_cursor"]},
    )
    assert second.status_code == 200
    assert [item["symbol"] for item in second.json()["instruments"]] == [
        "430047.BJ",
        "600001.SH",
    ]
    assert second.json()["next_cursor"] == "600001.SH"


def test_instruments_rejects_invalid_query_parameters(client: TestClient):
    assert client.get("/v1/instruments", params={"symbol": "not-a-symbol"}).status_code == 422
    assert client.get("/v1/instruments", params={"cursor": "not-a-symbol"}).status_code == 422
    assert client.get("/v1/instruments", params={"exchange": "US"}).status_code == 422
    assert client.get("/v1/instruments", params={"asset_type": "stock;drop"}).status_code == 422
    assert client.get("/v1/instruments", params={"q": ""}).status_code == 422
    assert client.get("/v1/instruments", params={"limit": 101}).status_code == 422


def test_instruments_reads_only_the_canonical_local_file(lake_root: Path, client: TestClient):
    broken = lake_root / "curated" / "instruments" / "old" / "part-merged.parquet"
    broken.parent.mkdir()
    broken.write_bytes(b"not a parquet file")

    response = client.get("/v1/instruments")

    assert response.status_code == 200


def test_instruments_reports_an_unavailable_local_file(lake_root: Path, client: TestClient):
    (lake_root / "curated" / "instruments" / "part-merged.parquet").unlink()

    assert client.get("/v1/instruments").status_code == 503


def test_instruments_share_the_disk_query_budget(settings: ProxySettings):
    app = create_app(replace(settings, max_concurrent_queries=1))

    assert app.state.kline_service._slots is app.state.instrument_service._slots
    assert app.state.adjustment_factor_service._slots is app.state.instrument_service._slots


def test_instruments_never_write_to_the_data_lake(lake_root: Path, client: TestClient):
    before = sorted(path.relative_to(lake_root) for path in lake_root.rglob("*"))
    response = client.get("/v1/instruments")
    after = sorted(path.relative_to(lake_root) for path in lake_root.rglob("*"))

    assert response.status_code == 200
    assert after == before
