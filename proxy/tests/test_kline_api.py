"""独立 K 线代理的 API、复权与性能边界。"""

from __future__ import annotations

import re
from dataclasses import replace
from datetime import date
from pathlib import Path

from fastapi.testclient import TestClient

from cnequity_query_proxy.app import create_app
from cnequity_query_proxy.settings import ProxySettings
from conftest import write_bars


def _window(**extra: object) -> dict[str, object]:
    return {"start": "2026-01-01", "end": "2026-01-06", **extra}


def test_kline_returns_one_symbol_in_trade_date_order(client: TestClient):
    response = client.get("/v1/kline/600519.sh", params=_window())

    assert response.status_code == 200
    body = response.json()
    assert body["symbol"] == "600519.SH"
    assert body["interval"] == "1d"
    assert body["adjustment"] == "none"
    assert [candle["trade_date"] for candle in body["candles"]] == [
        "2026-01-02",
        "2026-01-05",
        "2026-01-06",
    ]
    assert body["candles"][0] == {
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
    assert "source" not in body["candles"][0]
    assert response.headers["x-cache"] == "MISS"


def test_hfq_and_qfq_use_only_local_hfq_factor_files(client: TestClient):
    hfq = client.get("/v1/kline/600519.SH", params=_window(adjustment="hfq"))
    qfq = client.get(
        "/v1/kline/600519.SH",
        params=_window(adjustment="qfq", base_date="2026-01-06"),
    )

    assert hfq.status_code == qfq.status_code == 200
    assert [row["close"] for row in hfq.json()["candles"]] == [22.0, 48.0, 52.0]
    # 前复权锚定调用方指定的基准日，故该日收盘价等于原始值 13。
    assert [row["close"] for row in qfq.json()["candles"]] == [5.5, 12.0, 13.0]
    assert all(row["adjustment_exact"] for row in qfq.json()["candles"])
    assert qfq.json()["base_date"] == qfq.json()["base_factor_date"] == "2026-01-06"


def test_qfq_keeps_the_explicit_base_date_across_pages(client: TestClient):
    first = client.get(
        "/v1/kline/600519.SH",
        params=_window(adjustment="qfq", base_date="2026-01-06", limit=1),
    )
    assert first.status_code == 200
    assert first.json()["candles"][0]["close"] == 5.5
    assert first.json()["next_cursor"] == "2026-01-02"

    second = client.get(
        "/v1/kline/600519.SH",
        params=_window(
            adjustment="qfq",
            base_date="2026-01-06",
            limit=1,
            cursor=first.json()["next_cursor"],
        ),
    )
    assert second.status_code == 200
    assert second.json()["candles"][0]["close"] == 12.0


def test_qfq_base_date_can_be_outside_the_query_window(client: TestClient):
    response = client.get(
        "/v1/kline/600519.SH",
        params={
            "start": "2026-01-02",
            "end": "2026-01-05",
            "adjustment": "qfq",
            "base_date": "2026-01-06",
        },
    )

    assert response.status_code == 200
    assert [row["close"] for row in response.json()["candles"]] == [5.5, 12.0]
    assert response.json()["base_date"] == "2026-01-06"


def test_qfq_requires_an_exact_factor_for_the_explicit_base_date(client: TestClient):
    params = _window(adjustment="qfq", base_date="2026-01-04")

    strict = client.get("/v1/kline/600519.SH", params=params)
    permissive = client.get(
        "/v1/kline/600519.SH",
        params={**params, "strict_adjustment": "false"},
    )

    assert strict.status_code == 409
    assert permissive.status_code == 200
    body = permissive.json()
    assert [row["close"] for row in body["candles"]] == [11.0, 12.0, 13.0]
    assert body["base_date"] == "2026-01-04"
    assert body["base_factor_date"] is None
    assert all(row["adjustment_exact"] is False for row in body["candles"])


def test_missing_factor_fails_closed_by_default_and_is_marked_when_allowed(
    lake_root: Path, client: TestClient
):
    write_bars(
        lake_root,
        date(2026, 1, 7),
        [("600519.SH", date(2026, 1, 7), 13, 15, 12, 14, 130, 1820, "tdx_protocol")],
    )
    params = {"start": "2026-01-07", "end": "2026-01-07", "adjustment": "hfq"}
    strict = client.get("/v1/kline/600519.SH", params=params)
    permissive = client.get(
        "/v1/kline/600519.SH",
        params={**params, "strict_adjustment": "false"},
    )

    assert strict.status_code == 409
    assert permissive.status_code == 200
    assert permissive.json()["candles"][0]["close"] == 14.0
    assert permissive.json()["candles"][0]["adjustment_exact"] is False


def test_kline_paginates_with_a_date_cursor(client: TestClient):
    first = client.get("/v1/kline/600519.SH", params=_window(limit=2))
    assert first.status_code == 200
    assert first.json()["next_cursor"] == "2026-01-05"

    second = client.get(
        "/v1/kline/600519.SH",
        params=_window(limit=2, cursor=first.json()["next_cursor"]),
    )
    assert second.status_code == 200
    assert [row["trade_date"] for row in second.json()["candles"]] == ["2026-01-06"]
    assert second.json()["next_cursor"] is None


def test_kline_uses_the_response_cache(client: TestClient):
    assert client.get("/v1/kline/600519.SH", params=_window()).headers["x-cache"] == "MISS"
    assert client.get("/v1/kline/600519.SH", params=_window()).headers["x-cache"] == "HIT"


def test_kline_prunes_unrelated_partitions_before_duckdb_scans(lake_root: Path, client: TestClient):
    broken = lake_root / "curated" / "daily_bars" / "trade_date=2025-01-02"
    broken.mkdir()
    (broken / "part-merged.parquet").write_bytes(b"not a parquet file")

    response = client.get("/v1/kline/600519.SH", params=_window())
    assert response.status_code == 200


def test_kline_rejects_invalid_or_expensive_requests(client: TestClient):
    bad_symbol = client.get("/v1/kline/not-a-symbol", params={"start": "2026-01-01"})
    assert bad_symbol.status_code == 422

    inverted_dates = client.get(
        "/v1/kline/600519.SH", params=_window(start="2026-01-06", end="2026-01-01")
    )
    assert inverted_dates.status_code == 422

    wide_window = client.get(
        "/v1/kline/600519.SH",
        params={"start": "2025-01-01", "end": "2026-01-06"},
    )
    assert wide_window.status_code == 422

    large_page = client.get("/v1/kline/600519.SH", params=_window(limit=101))
    assert large_page.status_code == 422

    qfq_without_base = client.get("/v1/kline/600519.SH", params=_window(adjustment="qfq"))
    assert qfq_without_base.status_code == 422


def test_kline_rejects_a_window_that_exceeds_the_file_budget(settings: ProxySettings):
    client = TestClient(create_app(replace(settings, max_files=2)))
    response = client.get("/v1/kline/600519.SH", params=_window())
    assert response.status_code == 413


def test_kline_requires_bearer_token_when_configured(settings: ProxySettings):
    client = TestClient(create_app(replace(settings, api_key="secret")))

    assert client.get("/v1/kline/600519.SH").status_code == 401
    assert client.get("/healthz").status_code == 200
    assert (
        client.get("/v1/kline/600519.SH", headers={"Authorization": "Bearer secret"}).status_code
        == 200
    )


def test_proxy_never_writes_to_the_data_lake(lake_root: Path, client: TestClient):
    before = sorted(path.relative_to(lake_root) for path in lake_root.rglob("*"))
    response = client.get("/v1/kline/600519.SH", params=_window())
    after = sorted(path.relative_to(lake_root) for path in lake_root.rglob("*"))

    assert response.status_code == 200
    assert after == before


def test_proxy_never_imports_the_original_project_package():
    source_root = Path(__file__).parents[1] / "src" / "cnequity_query_proxy"
    import_pattern = re.compile(r"^\s*(?:from|import)\s+cnequity(?:\.|\s|$)", re.MULTILINE)
    assert not any(import_pattern.search(path.read_text()) for path in source_root.glob("*.py"))
