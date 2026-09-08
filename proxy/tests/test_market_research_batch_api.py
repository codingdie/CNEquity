"""全市场批量下载的 HTTP 契约。"""

from __future__ import annotations

import io
import tarfile
from dataclasses import replace
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from cnequity_query_proxy.app import create_app
from cnequity_query_proxy.settings import ProxySettings


def _window(**extra: object) -> dict[str, object]:
    return {"start": "2026-01-01", "end": "2026-01-06", **extra}


@pytest.mark.parametrize(
    ("path", "archive_name", "partition_glob", "filename"),
    [
        (
            "/v1/adjustment-factors/batch",
            "derived/adj_factors",
            "trade_date=2026-01-*",
            "cnequity-adjustment-factors-2026-01-01-2026-01-06.tar",
        ),
        (
            "/v1/trading-status/batch",
            "curated/trading_status",
            "trade_date=2026-01",
            "cnequity-trading-status-2026-01-01-2026-01-06.tar",
        ),
        (
            "/v1/trading-calendar/batch",
            "curated/trading_calendar",
            "trade_date=2026",
            "cnequity-trading-calendar-2026-01-01-2026-01-06.tar",
        ),
        (
            "/v1/dragon-tiger/batch",
            "curated/dragon_tiger",
            "trade_date=2026-01",
            "cnequity-dragon-tiger-2026-01-01-2026-01-06.tar",
        ),
    ],
)
def test_market_batches_stream_original_parquet_files(
    lake_root: Path,
    client: TestClient,
    path: str,
    archive_name: str,
    partition_glob: str,
    filename: str,
):
    response = client.get(path, params=_window())

    assert response.status_code == 200
    assert response.headers["content-type"] == "application/x-tar"
    assert response.headers["content-disposition"] == f'attachment; filename="{filename}"'
    assert response.headers["cache-control"] == "no-store"
    assert response.headers["x-cache"] == "BYPASS"

    source_paths = sorted((lake_root / archive_name).glob(f"{partition_glob}/*.parquet"))
    with tarfile.open(fileobj=io.BytesIO(response.content), mode="r:") as archive:
        assert archive.getnames() == [str(item.relative_to(lake_root)) for item in source_paths]
        for source in source_paths:
            member = archive.extractfile(str(source.relative_to(lake_root)))
            assert member is not None
            assert member.read() == source.read_bytes()

    assert response.headers["x-cnequity-data-files"] == str(len(source_paths))
    assert response.headers["x-cnequity-data-bytes"] == str(
        sum(item.stat().st_size for item in source_paths)
    )


@pytest.mark.parametrize(
    ("path", "archive_name"),
    [
        ("/v1/trading-status/batch", "curated/trading_status"),
        ("/v1/dragon-tiger/batch", "curated/dragon_tiger"),
    ],
)
def test_monthly_batches_select_overlapping_month_files_without_reencoding(
    lake_root: Path,
    client: TestClient,
    path: str,
    archive_name: str,
):
    response = client.get(
        path,
        params={"start": "2026-01-02", "end": "2026-01-02"},
    )

    assert response.status_code == 200
    name = f"{archive_name}/trade_date=2026-01/part-merged.parquet"
    with tarfile.open(fileobj=io.BytesIO(response.content), mode="r:") as archive:
        member = archive.extractfile(name)
        assert member is not None
        assert member.read() == (lake_root / name).read_bytes()


def test_trading_calendar_batch_selects_overlapping_year_files_without_reencoding(
    lake_root: Path,
    client: TestClient,
):
    response = client.get(
        "/v1/trading-calendar/batch",
        params={"start": "2026-01-02", "end": "2026-01-02"},
    )

    assert response.status_code == 200
    name = "curated/trading_calendar/trade_date=2026/part-merged.parquet"
    with tarfile.open(fileobj=io.BytesIO(response.content), mode="r:") as archive:
        assert archive.getnames() == [name]
        member = archive.extractfile(name)
        assert member is not None
        assert member.read() == (lake_root / name).read_bytes()


@pytest.mark.parametrize(
    "path",
    [
        "/v1/adjustment-factors/batch",
        "/v1/trading-calendar/batch",
        "/v1/trading-status/batch",
        "/v1/dragon-tiger/batch",
    ],
)
def test_market_batches_validate_windows_but_have_no_batch_quotas(
    settings: ProxySettings,
    client: TestClient,
    path: str,
):
    assert client.get(path).status_code == 422
    assert client.get(path, params=_window(start="2026-01-06", end="2026-01-01")).status_code == 422
    response = TestClient(
        create_app(replace(settings, default_window_days=2, max_window_days=2, max_files=1))
    ).get(path, params=_window())
    assert response.status_code == 200
    assert client.get(path, params={"start": "2028-02-01", "end": "2028-02-02"}).status_code == 404


@pytest.mark.parametrize(
    "path",
    [
        "/v1/adjustment-factors/batch",
        "/v1/trading-calendar/batch",
        "/v1/trading-status/batch",
        "/v1/dragon-tiger/batch",
    ],
)
def test_market_batches_bypass_regular_query_quota(settings: ProxySettings, path: str):
    app = create_app(replace(settings, max_concurrent_queries=1, max_files=1))
    query_slots = app.state.kline_service._slots
    assert query_slots.acquire(blocking=False)
    try:
        response = TestClient(app).get(path, params=_window())
        assert response.status_code == 200
    finally:
        query_slots.release()


@pytest.mark.parametrize(
    "path",
    [
        "/v1/adjustment-factors/batch",
        "/v1/trading-calendar/batch",
        "/v1/trading-status/batch",
        "/v1/dragon-tiger/batch",
    ],
)
def test_market_batches_require_authentication_and_never_write_to_the_lake(
    lake_root: Path,
    settings: ProxySettings,
    path: str,
):
    authenticated_client = TestClient(create_app(replace(settings, api_key="secret")))
    assert authenticated_client.get(path, params=_window()).status_code == 401
    assert (
        authenticated_client.get(
            path,
            params=_window(),
            headers={"Authorization": "Bearer secret"},
        ).status_code
        == 200
    )

    before = sorted(item.relative_to(lake_root) for item in lake_root.rglob("*"))
    response = TestClient(create_app(settings)).get(path, params=_window())
    after = sorted(item.relative_to(lake_root) for item in lake_root.rglob("*"))

    assert response.status_code == 200
    assert after == before
