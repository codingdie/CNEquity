"""单股轻量摘要 API 的聚合边界。"""

from __future__ import annotations

from dataclasses import replace
from datetime import date, datetime, timezone
from pathlib import Path

import duckdb
from fastapi.testclient import TestClient

from cnequity_query_proxy.app import create_app
from cnequity_query_proxy.settings import ProxySettings
from conftest import write_instruments


def _write_dataset(
    root: Path,
    dataset: str,
    partition: str,
    columns: str,
    rows: list[tuple],
) -> None:
    target = root / "curated" / dataset / partition
    target.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(":memory:")
    try:
        connection.execute(f"CREATE TABLE records ({columns})")
        placeholders = ", ".join("?" for _ in rows[0])
        connection.executemany(f"INSERT INTO records VALUES ({placeholders})", rows)
        escaped_path = (target / "part-merged.parquet").as_posix().replace("'", "''")
        connection.execute(f"COPY records TO '{escaped_path}' (FORMAT PARQUET)")
    finally:
        connection.close()


def _timestamp() -> datetime:
    return datetime(2026, 2, 3, 9, 5, tzinfo=timezone.utc)


def _populate_summary_datasets(root: Path) -> None:
    provenance = "source VARCHAR, data_version VARCHAR, fetched_at TIMESTAMPTZ"
    timestamp = _timestamp()
    _write_dataset(
        root,
        "daily_bars",
        "trade_date=2026-02-03",
        "symbol VARCHAR, trade_date DATE, open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, "
        f"volume BIGINT, amount DOUBLE, {provenance}",
        [
            (
                "600519.SH",
                date(2026, 2, 3),
                13.0,
                15.0,
                12.0,
                14.0,
                130,
                1820.0,
                "tdx_protocol",
                "v2",
                timestamp,
            )
        ],
    )
    _write_dataset(
        root,
        "trading_status",
        "trade_date=2026-02",
        "symbol VARCHAR, trade_date DATE, is_trading BOOLEAN, status VARCHAR, risk_warning BOOLEAN, "
        f"{provenance}",
        [
            (
                "600519.SH",
                date(2026, 2, 3),
                True,
                "normal",
                False,
                "eastmoney",
                "v1",
                timestamp,
            )
        ],
    )
    _write_dataset(
        root,
        "valuation_metrics",
        "trade_date=2026-02-03",
        "symbol VARCHAR, trade_date DATE, pe_ttm DOUBLE, pb DOUBLE, ps_ttm DOUBLE, total_mv DOUBLE, "
        f"float_mv DOUBLE, {provenance}",
        [
            (
                "600519.SH",
                date(2026, 2, 3),
                22.5,
                8.1,
                9.7,
                1_800_000_000_000.0,
                1_400_000_000_000.0,
                "eastmoney",
                "v1",
                timestamp,
            )
        ],
    )
    _write_dataset(
        root,
        "industry_members",
        "as_of_date=2026-02-03",
        "symbol VARCHAR, classification_system VARCHAR, industry_code VARCHAR, industry_name VARCHAR, "
        f"as_of_date DATE, {provenance}",
        [
            (
                "600519.SH",
                "eastmoney",
                "BK0477",
                "酿酒行业旧快照",
                date(2026, 2, 2),
                "eastmoney",
                "v1",
                timestamp,
            ),
            (
                "600519.SH",
                "eastmoney",
                "BK0477",
                "酿酒行业",
                date(2026, 2, 3),
                "eastmoney",
                "v1",
                timestamp,
            ),
            (
                "600519.SH",
                "sw",
                "801124",
                "白酒 II",
                date(2026, 2, 3),
                "sw",
                "v1",
                timestamp,
            ),
        ],
    )
    _write_dataset(
        root,
        "sector_members",
        "as_of_date=2026-02-03",
        f"symbol VARCHAR, sector_code VARCHAR, sector_name VARCHAR, as_of_date DATE, {provenance}",
        [
            (
                "600519.SH",
                "BK0815",
                "白酒",
                date(2026, 2, 3),
                "eastmoney",
                "v1",
                timestamp,
            ),
            (
                "600519.SH",
                "BK0800",
                "大消费",
                date(2026, 2, 3),
                "eastmoney",
                "v1",
                timestamp,
            ),
        ],
    )
    _write_dataset(
        root,
        "index_constituents",
        "as_of_date=2026-02",
        f"index_symbol VARCHAR, symbol VARCHAR, as_of_date DATE, weight DOUBLE, {provenance}",
        [
            (
                "000300.SH",
                "600519.SH",
                date(2026, 2, 2),
                0.0,
                "eastmoney",
                "v1",
                timestamp,
            ),
            (
                "000300.SH",
                "600519.SH",
                date(2026, 2, 3),
                0.0,
                "eastmoney",
                "v1",
                timestamp,
            ),
            (
                "000001.SH",
                "600519.SH",
                date(2026, 2, 3),
                0.0,
                "eastmoney",
                "v1",
                timestamp,
            ),
        ],
    )
    _write_dataset(
        root,
        "fund_flow",
        "trade_date=2026-02-03",
        "symbol VARCHAR, trade_date DATE, main_net_inflow DOUBLE, super_large_net_inflow DOUBLE, "
        "large_net_inflow DOUBLE, medium_net_inflow DOUBLE, small_net_inflow DOUBLE, "
        f"{provenance}",
        [
            (
                "600519.SH",
                date(2026, 2, 3),
                1_000_000.0,
                2_000_000.0,
                3_000_000.0,
                -500_000.0,
                -3_500_000.0,
                "eastmoney",
                "v1",
                timestamp,
            )
        ],
    )
    _write_dataset(
        root,
        "analyst_consensus",
        "forecast_date=2026-02-03",
        "symbol VARCHAR, forecast_date DATE, forecast_year BIGINT, eps_forecast DOUBLE, "
        f"pe_forecast DOUBLE, target_price DOUBLE, rating VARCHAR, analyst_count BIGINT, {provenance}",
        [
            (
                "600519.SH",
                date(2026, 2, 3),
                2026,
                75.5,
                None,
                1800.0,
                "buy",
                32,
                "eastmoney",
                "v1",
                timestamp,
            )
        ],
    )
    _write_dataset(
        root,
        "hot_rank",
        "trade_date=2026-02",
        "symbol VARCHAR, trade_date DATE, rank BIGINT, rank_change BIGINT, hist_rank BIGINT, "
        f"{provenance}",
        [
            (
                "600519.SH",
                date(2026, 2, 2),
                13,
                1,
                20,
                "eastmoney",
                "v1",
                timestamp,
            ),
            (
                "600519.SH",
                date(2026, 2, 3),
                9,
                4,
                20,
                "eastmoney",
                "v1",
                timestamp,
            ),
        ],
    )
    _write_dataset(
        root,
        "sentiment_scores",
        "trade_date=2026-02",
        "symbol VARCHAR, trade_date DATE, score_channel VARCHAR, sentiment_score DOUBLE, "
        f"headline_count BIGINT, {provenance}",
        [
            (
                "600519.SH",
                date(2026, 2, 2),
                "news",
                0.1,
                3,
                "derived",
                "v1",
                timestamp,
            ),
            (
                "600519.SH",
                date(2026, 2, 3),
                "news",
                0.4,
                5,
                "derived",
                "v1",
                timestamp,
            ),
            (
                "600519.SH",
                date(2026, 2, 3),
                "announcement",
                -0.2,
                2,
                "derived",
                "v1",
                timestamp,
            ),
        ],
    )


def _summary_settings(root: Path) -> ProxySettings:
    return ProxySettings(
        data_root=root,
        default_limit=100,
        max_bars=100,
        max_files=100,
        cache_ttl_seconds=30,
        cache_entries=8,
        max_concurrent_queries=2,
        duckdb_threads=1,
    )


def test_stock_summary_aggregates_lightweight_current_facts(lake_root: Path, client: TestClient):
    _populate_summary_datasets(lake_root)

    response = client.get("/v1/stocks/600519.sh/summary")

    assert response.status_code == 200
    body = response.json()
    assert body["symbol"] == "600519.SH"
    assert body["instrument"]["name"] == "贵州茅台"
    assert body["instrument"]["provenance"]["source"] == "tdx_protocol"
    assert body["market"]["latest_market"] == {
        "trade_date": "2026-02-03",
        "open": 13.0,
        "high": 15.0,
        "low": 12.0,
        "close": 14.0,
        "volume": 130,
        "amount": 1820.0,
        "provenance": {
            "source": "tdx_protocol",
            "data_version": "v2",
            "fetched_at": "2026-02-03T09:05:00Z",
        },
    }
    assert "daily_bar" not in body["market"]
    assert body["market"]["trading_status"]["status"] == "normal"
    assert body["market"]["valuation"]["total_mv"] == 1_800_000_000_000.0
    assert body["classification"]["industries"] == [
        {
            "classification_system": "eastmoney",
            "industry_code": "BK0477",
            "industry_name": "酿酒行业",
            "as_of_date": "2026-02-03",
            "provenance": {
                "source": "eastmoney",
                "data_version": "v1",
                "fetched_at": "2026-02-03T09:05:00Z",
            },
        },
        {
            "classification_system": "sw",
            "industry_code": "801124",
            "industry_name": "白酒 II",
            "as_of_date": "2026-02-03",
            "provenance": {
                "source": "sw",
                "data_version": "v1",
                "fetched_at": "2026-02-03T09:05:00Z",
            },
        },
    ]
    assert [item["sector_code"] for item in body["classification"]["sectors"]] == [
        "BK0800",
        "BK0815",
    ]
    assert body["classification"]["index_memberships"] == [
        {
            "index_symbol": "000001.SH",
            "as_of_date": "2026-02-03",
            "weight": None,
            "provenance": {
                "source": "eastmoney",
                "data_version": "v1",
                "fetched_at": "2026-02-03T09:05:00Z",
            },
        },
        {
            "index_symbol": "000300.SH",
            "as_of_date": "2026-02-03",
            "weight": None,
            "provenance": {
                "source": "eastmoney",
                "data_version": "v1",
                "fetched_at": "2026-02-03T09:05:00Z",
            },
        },
    ]
    assert body["signals"]["fund_flow"]["main_net_inflow"] == 1_000_000.0
    assert body["signals"]["analyst_consensus"]["rating"] == "buy"
    assert body["signals"]["hot_rank"]["rank"] == 9
    assert [item["score_channel"] for item in body["signals"]["sentiments"]] == [
        "announcement",
        "news",
    ]
    assert body["unavailable_datasets"] == []
    assert response.headers["x-cache"] == "MISS"
    assert client.get("/v1/stocks/600519.SH/summary").headers["x-cache"] == "HIT"


def test_stock_summary_marks_missing_optional_datasets_without_inventing_facts(tmp_path: Path):
    write_instruments(
        tmp_path,
        [
            (
                "600519.SH",
                "贵州茅台",
                "SH",
                "stock",
                date(2001, 8, 27),
                None,
                None,
                "eastmoney",
                "v1",
                _timestamp(),
            )
        ],
    )
    client = TestClient(create_app(_summary_settings(tmp_path)))

    response = client.get("/v1/stocks/600519.SH/summary")

    assert response.status_code == 200
    body = response.json()
    assert body["market"] == {
        "latest_market": None,
        "trading_status": None,
        "valuation": None,
    }
    assert body["classification"] == {
        "industries": [],
        "sectors": [],
        "index_memberships": [],
    }
    assert body["signals"] == {
        "fund_flow": None,
        "analyst_consensus": None,
        "hot_rank": None,
        "sentiments": [],
    }
    assert body["unavailable_datasets"] == [
        "daily_bars",
        "trading_status",
        "valuation_metrics",
        "industry_members",
        "sector_members",
        "index_constituents",
        "fund_flow",
        "analyst_consensus",
        "hot_rank",
        "sentiment_scores",
    ]


def test_stock_summary_validates_symbols_and_returns_404_for_unknown_instrument(client: TestClient):
    assert client.get("/v1/stocks/not-a-symbol/summary").status_code == 422
    assert client.get("/v1/stocks/600000.SH/summary").status_code == 404


def test_stock_summary_prunes_unrelated_old_partitions(lake_root: Path, client: TestClient):
    _populate_summary_datasets(lake_root)
    broken = lake_root / "curated" / "valuation_metrics" / "trade_date=2026-01-01"
    broken.mkdir(parents=True)
    (broken / "part-merged.parquet").write_bytes(b"not a parquet file")

    response = client.get("/v1/stocks/600519.SH/summary")

    assert response.status_code == 200


def test_stock_summary_respects_shared_file_budget(lake_root: Path, settings: ProxySettings):
    _populate_summary_datasets(lake_root)
    client = TestClient(create_app(replace(settings, max_files=1)))

    response = client.get("/v1/stocks/600519.SH/summary")

    assert response.status_code == 413


def test_stock_summary_shares_the_disk_query_budget(settings: ProxySettings):
    app = create_app(replace(settings, max_concurrent_queries=1))

    assert app.state.stock_summary_service._slots is app.state.kline_service._slots


def test_stock_summary_requires_bearer_token_when_configured(
    lake_root: Path, settings: ProxySettings
):
    _populate_summary_datasets(lake_root)
    client = TestClient(create_app(replace(settings, api_key="secret")))

    assert client.get("/v1/stocks/600519.SH/summary").status_code == 401
    assert (
        client.get(
            "/v1/stocks/600519.SH/summary",
            headers={"Authorization": "Bearer secret"},
        ).status_code
        == 200
    )


def test_stock_summary_never_writes_to_the_data_lake(lake_root: Path, client: TestClient):
    _populate_summary_datasets(lake_root)
    before = sorted(path.relative_to(lake_root) for path in lake_root.rglob("*"))

    response = client.get("/v1/stocks/600519.SH/summary")

    after = sorted(path.relative_to(lake_root) for path in lake_root.rglob("*"))
    assert response.status_code == 200
    assert after == before
