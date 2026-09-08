"""估值批量接口必须原样保留日期、空市值及所有溯源字段。"""

import hashlib
import io
import tarfile
from dataclasses import replace
from pathlib import Path

import duckdb
import pytest
from fastapi.testclient import TestClient

from cnequity_query_proxy.app import create_app
from cnequity_query_proxy.settings import ProxySettings

PATH = "/v1/valuation-metrics/batch"
WINDOW = {"start": "2026-09-07", "end": "2026-09-08"}


@pytest.fixture
def valuation_settings(tmp_path: Path) -> ProxySettings:
    with duckdb.connect() as connection:
        for day in ("2026-09-04", "2026-09-07", "2026-09-08", "2026-09-09"):
            target = tmp_path / "curated/valuation_metrics" / f"trade_date={day}"
            target.mkdir(parents=True)
            for part in (1, 2):
                connection.execute(
                    "COPY (SELECT '600519.SH' AS symbol, $day::DATE AS trade_date, "
                    "12.5 AS pe_ttm, 1.2 AS pb, 3.4 AS ps_ttm, "
                    "NULL::DOUBLE AS float_mv, $mv::DOUBLE AS total_mv, "
                    "'eastmoney' AS source, 'v1' AS data_version, "
                    "TIMESTAMP '2026-09-10 01:00:00' AS fetched_at, 42 AS extra_field) "
                    "TO $path (FORMAT PARQUET)",
                    {
                        "day": day,
                        "mv": None if part == 1 else 123456789,
                        "path": str(target / f"part-{part}.parquet"),
                    },
                )
    return ProxySettings(data_root=tmp_path)


def _lake_hashes(root: Path) -> dict[str, str]:
    return {
        str(p.relative_to(root)): hashlib.sha256(p.read_bytes()).hexdigest()
        for p in root.rglob("*")
        if p.is_file()
    }


@pytest.mark.parametrize("end", ["2026-09-07", "2026-09-08"])
def test_original_archive_and_inclusive_dates(valuation_settings, end):
    root = valuation_settings.data_root
    before = _lake_hashes(root)
    response = TestClient(create_app(valuation_settings)).get(PATH, params={**WINDOW, "end": end})
    assert response.status_code == 200
    expected = sorted(
        str(p.relative_to(root))
        for p in root.rglob("*.parquet")
        if "trade_date=2026-09-07" in str(p)
        or (end == "2026-09-08" and "trade_date=2026-09-08" in str(p))
    )
    assert response.headers["content-type"] == "application/x-tar"
    assert response.headers["content-disposition"] == (
        f'attachment; filename="cnequity-valuation-metrics-2026-09-07-{end}.tar"'
    )
    assert response.headers["x-cnequity-data-files"] == str(len(expected))
    assert response.headers["x-cnequity-data-bytes"] == str(
        sum((root / p).stat().st_size for p in expected)
    )
    assert response.headers["cache-control"] == "no-store"
    assert response.headers["x-cache"] == "BYPASS"
    with tarfile.open(fileobj=io.BytesIO(response.content)) as archive:
        assert archive.getnames() == expected
        for name in expected:
            # 字节相等也保证空值、跨日 fetched_at、trade_date 及额外字段未经改写。
            assert archive.extractfile(name).read() == (root / name).read_bytes()
    assert _lake_hashes(root) == before


@pytest.mark.parametrize(
    "params",
    [
        {},
        {"start": "2026-09-07"},
        {"end": "2026-09-07"},
        {"start": "2026-09-08", "end": "2026-09-07"},
        *[
            {**WINDOW, key: value}
            for key in ("start", "end")
            for value in ("2026-02-30", "20260907", "2026-9-7", "2026-09-07T00:00:00", "1788739200")
        ],
    ],
)
def test_invalid_dates(tmp_path, params):
    assert (
        TestClient(create_app(ProxySettings(data_root=tmp_path)))
        .get(PATH, params=params)
        .status_code
        == 422
    )


def test_auth_errors_and_query_budget(valuation_settings, tmp_path):
    settings = replace(
        valuation_settings,
        api_key="secret",
        max_files=1,
        default_window_days=1,
        max_window_days=1,
        max_concurrent_queries=1,
    )
    app = create_app(settings)
    client = TestClient(app)
    headers = {"Authorization": "Bearer secret"}
    assert client.get(PATH, params=WINDOW).status_code == 401
    assert (
        client.get(PATH, params=WINDOW, headers={"Authorization": "Bearer wrong"}).status_code
        == 401
    )
    slots = app.state.kline_service._slots
    assert slots.acquire(blocking=False)
    try:
        assert client.get(PATH, params=WINDOW, headers=headers).status_code == 200
    finally:
        slots.release()
    assert (
        client.get(
            PATH, params={"start": "2026-09-05", "end": "2026-09-06"}, headers=headers
        ).status_code
        == 404
    )
    assert (
        TestClient(create_app(replace(settings, data_root=tmp_path / "missing")))
        .get(PATH, params=WINDOW, headers=headers)
        .status_code
        == 503
    )


def test_outside_dataset_file_fails_closed(valuation_settings, tmp_path):
    outside = tmp_path / "outside.parquet"
    outside.write_bytes(b"not in valuation dataset")
    partition = valuation_settings.valuation_metrics_root / "trade_date=2026-09-07"
    (partition / "escape.parquet").symlink_to(outside)
    response = TestClient(create_app(valuation_settings)).get(PATH, params=WINDOW)
    assert response.status_code == 503
