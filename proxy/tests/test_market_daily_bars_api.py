"""全市场日线批量下载的 HTTP 契约。"""

from __future__ import annotations

import io
import tarfile
from dataclasses import replace
from pathlib import Path

from fastapi.testclient import TestClient

from cnequity_query_proxy.app import create_app
from cnequity_query_proxy.settings import ProxySettings


def _window(**extra: object) -> dict[str, object]:
    return {"start": "2026-01-01", "end": "2026-01-06", **extra}


def test_market_daily_bars_batch_streams_original_parquet_files(
    lake_root: Path, client: TestClient
):
    response = client.get("/v1/kline/batch", params=_window())

    assert response.status_code == 200
    assert response.headers["content-type"] == "application/x-tar"
    assert response.headers["content-disposition"] == (
        'attachment; filename="cnequity-daily-bars-2026-01-01-2026-01-06.tar"'
    )
    assert response.headers["cache-control"] == "no-store"
    assert response.headers["x-cache"] == "BYPASS"
    assert response.headers["x-cnequity-data-files"] == "3"

    with tarfile.open(fileobj=io.BytesIO(response.content), mode="r:") as archive:
        names = archive.getnames()
        assert names == [
            "curated/daily_bars/trade_date=2026-01-02/part-merged.parquet",
            "curated/daily_bars/trade_date=2026-01-05/part-merged.parquet",
            "curated/daily_bars/trade_date=2026-01-06/part-merged.parquet",
        ]
        for name in names:
            source = lake_root / name
            member = archive.extractfile(name)
            assert member is not None
            assert member.read() == source.read_bytes()

    expected_data_bytes = sum(
        path.stat().st_size
        for path in (lake_root / "curated" / "daily_bars").glob("trade_date=*/*.parquet")
    )
    assert response.headers["x-cnequity-data-bytes"] == str(expected_data_bytes)


def test_market_daily_bars_batch_rejects_invalid_windows_but_has_no_batch_quotas(
    settings: ProxySettings,
):
    client = TestClient(create_app(settings))

    assert client.get("/v1/kline/batch").status_code == 422
    assert (
        client.get(
            "/v1/kline/batch", params=_window(start="2026-01-06", end="2026-01-01")
        ).status_code
        == 422
    )
    response = TestClient(
        create_app(replace(settings, default_window_days=2, max_window_days=2, max_files=1))
    ).get("/v1/kline/batch", params=_window())

    assert response.status_code == 200
    assert response.headers["x-cnequity-data-files"] == "3"


def test_market_daily_bars_batch_handles_empty_lake_and_authentication(
    settings: ProxySettings, tmp_path: Path
):
    empty_client = TestClient(create_app(replace(settings, data_root=tmp_path / "missing")))
    assert empty_client.get("/v1/kline/batch", params=_window()).status_code == 503

    authenticated_client = TestClient(create_app(replace(settings, api_key="secret")))
    assert authenticated_client.get("/v1/kline/batch", params=_window()).status_code == 401
    assert (
        authenticated_client.get(
            "/v1/kline/batch",
            params=_window(),
            headers={"Authorization": "Bearer secret"},
        ).status_code
        == 200
    )


def test_market_daily_bars_batch_returns_404_when_window_has_no_partitions(client: TestClient):
    response = client.get(
        "/v1/kline/batch",
        params={"start": "2026-02-01", "end": "2026-02-02"},
    )
    assert response.status_code == 404


def test_market_daily_bars_batch_bypasses_regular_query_quota(settings: ProxySettings):
    app = create_app(replace(settings, max_concurrent_queries=1))
    query_slots = app.state.kline_service._slots
    assert query_slots.acquire(blocking=False)
    try:
        response = TestClient(app).get("/v1/kline/batch", params=_window())
        assert response.status_code == 200
    finally:
        query_slots.release()


def test_market_daily_bars_batch_never_writes_to_the_data_lake(lake_root: Path, client: TestClient):
    before = sorted(path.relative_to(lake_root) for path in lake_root.rglob("*"))
    response = client.get("/v1/kline/batch", params=_window())
    after = sorted(path.relative_to(lake_root) for path in lake_root.rglob("*"))

    assert response.status_code == 200
    assert after == before
