"""板块历史快照批量下载契约。"""

import io
import tarfile
from pathlib import Path

import duckdb
import pytest
from fastapi.testclient import TestClient

from cnequity_query_proxy.app import create_app
from cnequity_query_proxy.settings import ProxySettings

PATH = "/v1/sector-members/batch"


@pytest.fixture
def snapshot_lake(tmp_path: Path) -> Path:
    with duckdb.connect() as connection:
        for day in ("2025-12-01", "2025-12-31", "2026-01-02", "2026-01-06", "2026-02-01"):
            partition = tmp_path / "curated/sector_members" / f"as_of_date={day}"
            partition.mkdir(parents=True)
            for part in (1, 2):
                connection.execute(
                    "COPY (SELECT $day::DATE AS as_of_date, $sector AS sector_code, "
                    "'600519.SH' AS symbol, 'eastmoney' AS source, 'v1' AS data_version, "
                    "TIMESTAMP '2026-01-01 12:00:00' AS fetched_at, 42 AS extra_field) "
                    "TO $path (FORMAT PARQUET)",
                    {
                        "day": day,
                        "sector": f"BK{part}",
                        "path": str(partition / f"part-{part}.parquet"),
                    },
                )
    return tmp_path


@pytest.mark.parametrize(
    ("start", "end", "previous", "days"),
    [
        ("2026-01-02", "2026-01-06", None, ["2026-01-02", "2026-01-06"]),
        ("2026-01-02", "2026-01-06", "false", ["2026-01-02", "2026-01-06"]),
        ("2026-01-02", "2026-01-06", "true", ["2025-12-31", "2026-01-02", "2026-01-06"]),
        ("2026-01-03", "2026-01-05", "true", ["2026-01-02"]),
        ("2025-12-01", "2025-12-01", "true", ["2025-12-01"]),
        ("2025-11-01", "2025-11-30", "true", []),
        ("2026-01-03", "2026-01-05", "false", []),
    ],
)
def test_snapshot_selection_and_original_bytes(snapshot_lake, start, end, previous, days):
    settings = ProxySettings(
        data_root=snapshot_lake, max_files=1, default_window_days=1, max_window_days=1
    )
    app = create_app(settings)
    params = {"start": start, "end": end}
    if previous is not None:
        params["include_previous_snapshot"] = previous
    before = {
        str(p.relative_to(snapshot_lake)): p.read_bytes() for p in snapshot_lake.rglob("*.parquet")
    }
    slots = app.state.kline_service._slots
    for _ in range(settings.max_concurrent_queries):
        assert slots.acquire(blocking=False)
    try:
        response = TestClient(app).get(PATH, params=params)
    finally:
        for _ in range(settings.max_concurrent_queries):
            slots.release()
    assert response.status_code == (200 if days else 404)
    if days:
        expected = sorted(
            name for name in before if any(f"as_of_date={day}/" in name for day in days)
        )
        assert response.headers["content-type"] == "application/x-tar"
        assert response.headers["content-disposition"] == (
            f'attachment; filename="cnequity-sector-members-{start}-{end}.tar"'
        )
        assert response.headers["x-cache"] == "BYPASS"
        assert response.headers["cache-control"] == "no-store"
        assert response.headers["x-cnequity-data-files"] == str(len(expected))
        assert response.headers["x-cnequity-data-bytes"] == str(
            sum(len(before[n]) for n in expected)
        )
        with tarfile.open(fileobj=io.BytesIO(response.content)) as archive:
            assert archive.getnames() == expected
            for name in expected:
                assert archive.extractfile(name).read() == before[name]
    assert {
        str(p.relative_to(snapshot_lake)): p.read_bytes() for p in snapshot_lake.rglob("*.parquet")
    } == before


@pytest.mark.parametrize(
    "params",
    [
        {},
        {"start": "2026-01-01"},
        {"end": "2026-01-02"},
        {"start": "2026-01-03", "end": "2026-01-02"},
        *[
            {"start": value, "end": "2026-03-01"}
            for value in ("2026-02-30", "20260101", "1767225600", "2026-01-01T00:00:00", "2026-1-1")
        ],
        {"start": "2026-01-01", "end": "2026-01-02", "include_previous_snapshot": "invalid"},
    ],
)
def test_invalid_parameters(tmp_path, params):
    client = TestClient(create_app(ProxySettings(data_root=tmp_path)))
    assert client.get(PATH, params=params).status_code == 422


def test_authentication_and_missing_lake(tmp_path):
    client = TestClient(create_app(ProxySettings(data_root=tmp_path, api_key="secret")))
    params = {"start": "2026-01-01", "end": "2026-01-02"}
    assert client.get(PATH, params=params).status_code == 401
    assert (
        client.get(PATH, params=params, headers={"Authorization": "Bearer secret"}).status_code
        == 503
    )


def test_undated_and_coarse_partitions_are_not_snapshots(tmp_path):
    root = tmp_path / "curated/sector_members"
    for directory in (root, root / "as_of_date=2025", root / "as_of_date=2026-01"):
        directory.mkdir(parents=True, exist_ok=True)
        (directory / "part.parquet").write_bytes(b"must not be returned")
    (root / "as_of_date=2025-12-31").mkdir()
    client = TestClient(create_app(ProxySettings(data_root=tmp_path)))
    assert (
        client.get(
            PATH,
            params={
                "start": "2026-01-01",
                "end": "2026-01-02",
                "include_previous_snapshot": "true",
            },
        ).status_code
        == 404
    )
