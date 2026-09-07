"""独立查询代理的共享测试数据湖。"""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import duckdb
import pytest
from fastapi.testclient import TestClient

from cnequity_query_proxy.app import create_app
from cnequity_query_proxy.settings import ProxySettings


def _copy_table(connection: duckdb.DuckDBPyConnection, table: str, path: Path) -> None:
    escaped_path = path.as_posix().replace("'", "''")
    connection.execute(f"COPY {table} TO '{escaped_path}' (FORMAT PARQUET)")


def write_bars(root: Path, day: date, rows: list[tuple]) -> None:
    target = root / "curated" / "daily_bars" / f"trade_date={day.isoformat()}"
    target.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(":memory:")
    try:
        connection.execute(
            """
            CREATE TABLE bars (
                symbol VARCHAR,
                trade_date DATE,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume BIGINT,
                amount DOUBLE,
                source VARCHAR
            )
            """
        )
        connection.executemany("INSERT INTO bars VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", rows)
        _copy_table(connection, "bars", target / "part-merged.parquet")
    finally:
        connection.close()


def write_factor(root: Path, day: date, rows: list[tuple]) -> None:
    target = root / "derived" / "adj_factors" / f"trade_date={day.isoformat()}"
    target.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(":memory:")
    try:
        connection.execute(
            """
            CREATE TABLE factors (
                symbol VARCHAR,
                trade_date DATE,
                adjust_type VARCHAR,
                factor DOUBLE
            )
            """
        )
        connection.executemany("INSERT INTO factors VALUES (?, ?, ?, ?)", rows)
        _copy_table(connection, "factors", target / "part-merged.parquet")
    finally:
        connection.close()


def write_minute_bars(
    root: Path,
    day: date,
    rows: list[tuple],
    *,
    dataset: str = "minute_bars",
) -> None:
    target = root / "curated" / dataset / f"trade_date={day.isoformat()}"
    target.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(":memory:")
    try:
        connection.execute(
            """
            CREATE TABLE minute_bars (
                symbol VARCHAR,
                trade_date DATE,
                bar_time TIMESTAMP,
                frequency VARCHAR,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume BIGINT,
                amount DOUBLE,
                source VARCHAR
            )
            """
        )
        connection.executemany(
            "INSERT INTO minute_bars VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows
        )
        _copy_table(connection, "minute_bars", target / "part-merged.parquet")
    finally:
        connection.close()


def write_instruments(root: Path, rows: list[tuple]) -> None:
    target = root / "curated" / "instruments"
    target.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(":memory:")
    try:
        connection.execute(
            """
            CREATE TABLE instruments (
                symbol VARCHAR,
                name VARCHAR,
                exchange VARCHAR,
                asset_type VARCHAR,
                list_date DATE,
                delist_date DATE,
                prev_symbol VARCHAR,
                source VARCHAR,
                data_version VARCHAR,
                fetched_at TIMESTAMP
            )
            """
        )
        connection.executemany(
            "INSERT INTO instruments VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", rows
        )
        _copy_table(connection, "instruments", target / "part-merged.parquet")
    finally:
        connection.close()


@pytest.fixture
def lake_root(tmp_path: Path) -> Path:
    write_bars(
        tmp_path,
        date(2026, 1, 2),
        [
            ("600519.SH", date(2026, 1, 2), 10, 12, 9, 11, 100, 1100, "tdx_protocol"),
            ("000001.SZ", date(2026, 1, 2), 20, 21, 19, 20, 200, 4000, "tdx_protocol"),
        ],
    )
    write_bars(
        tmp_path,
        date(2026, 1, 5),
        [("600519.SH", date(2026, 1, 5), 11, 13, 10, 12, 110, 1320, "tdx_protocol")],
    )
    write_bars(
        tmp_path,
        date(2026, 1, 6),
        [("600519.SH", date(2026, 1, 6), 12, 14, 11, 13, 120, 1560, "tdx_protocol")],
    )
    write_factor(tmp_path, date(2026, 1, 2), [("600519.SH", date(2026, 1, 2), "hfq", 2)])
    write_factor(tmp_path, date(2026, 1, 5), [("600519.SH", date(2026, 1, 5), "hfq", 4)])
    write_factor(tmp_path, date(2026, 1, 6), [("600519.SH", date(2026, 1, 6), "hfq", 4)])
    write_minute_bars(
        tmp_path,
        date(2026, 1, 2),
        [
            (
                "600519.SH",
                date(2026, 1, 2),
                datetime(2026, 1, 2, 9, 31),
                "1m",
                10,
                11,
                9,
                10.5,
                100,
                1050,
                "tdx_protocol",
            ),
            (
                "600519.SH",
                date(2026, 1, 2),
                datetime(2026, 1, 2, 9, 32),
                "1m",
                10.5,
                12,
                10,
                11,
                120,
                1320,
                "tdx_protocol",
            ),
        ],
    )
    write_minute_bars(
        tmp_path,
        date(2026, 1, 5),
        [
            (
                "600519.SH",
                date(2026, 1, 5),
                datetime(2026, 1, 5, 9, 31),
                "1m",
                11,
                13,
                10.5,
                12,
                150,
                1800,
                "tdx_protocol",
            ),
            (
                "600519.SH",
                date(2026, 1, 5),
                datetime(2026, 1, 5, 9, 32),
                "1m",
                12,
                13,
                11.5,
                12.5,
                160,
                2000,
                "tdx_protocol",
            ),
        ],
    )
    write_minute_bars(
        tmp_path,
        date(2026, 1, 6),
        [
            (
                "600519.SH",
                date(2026, 1, 6),
                datetime(2026, 1, 6, 9, 31),
                "1m",
                12,
                14,
                11.5,
                13,
                170,
                2210,
                "tdx_protocol",
            )
        ],
    )
    write_minute_bars(
        tmp_path,
        date(2026, 1, 2),
        [
            (
                "600519.SH",
                date(2026, 1, 2),
                datetime(2026, 1, 2, 9, 35),
                "5m",
                10,
                11,
                9,
                10.5,
                100,
                1050,
                "tdx_protocol",
            ),
            (
                "600519.SH",
                date(2026, 1, 2),
                datetime(2026, 1, 2, 9, 40),
                "5m",
                10.5,
                12,
                10,
                11,
                120,
                1320,
                "tdx_protocol",
            ),
        ],
        dataset="minute_bars_5m",
    )
    write_minute_bars(
        tmp_path,
        date(2026, 1, 5),
        [
            (
                "600519.SH",
                date(2026, 1, 5),
                datetime(2026, 1, 5, 9, 35),
                "5m",
                11,
                13,
                10.5,
                12,
                150,
                1800,
                "tdx_protocol",
            )
        ],
        dataset="minute_bars_5m",
    )
    write_minute_bars(
        tmp_path,
        date(2026, 1, 6),
        [
            (
                "600519.SH",
                date(2026, 1, 6),
                datetime(2026, 1, 6, 9, 35),
                "5m",
                12,
                14,
                11.5,
                13,
                170,
                2210,
                "tdx_protocol",
            )
        ],
        dataset="minute_bars_5m",
    )
    write_instruments(
        tmp_path,
        [
            (
                "000001.SZ",
                "平安银行",
                "SZ",
                "stock",
                date(1991, 4, 3),
                None,
                None,
                "tdx_protocol",
                "test",
                None,
            ),
            (
                "159915.SZ",
                "创业板 ETF",
                "SZ",
                "etf",
                date(2011, 12, 9),
                None,
                None,
                "tdx_protocol",
                "test",
                None,
            ),
            (
                "430047.BJ",
                "诺思兰德",
                "BJ",
                "stock",
                date(2014, 8, 18),
                None,
                None,
                "tdx_protocol",
                "test",
                None,
            ),
            (
                "600001.SH",
                "退市示例",
                "SH",
                "stock",
                date(1990, 12, 19),
                date(2020, 1, 1),
                "600001.OLD",
                "baostock",
                "test",
                None,
            ),
            (
                "600519.SH",
                "贵州茅台",
                "SH",
                "stock",
                date(2001, 8, 27),
                None,
                None,
                "tdx_protocol",
                "test",
                None,
            ),
        ],
    )
    return tmp_path


@pytest.fixture
def settings(lake_root: Path) -> ProxySettings:
    return ProxySettings(
        data_root=lake_root,
        default_window_days=30,
        max_window_days=60,
        default_minute_window_days=3,
        max_minute_window_days=10,
        default_five_minute_window_days=4,
        max_five_minute_window_days=12,
        default_limit=100,
        max_bars=100,
        max_files=100,
        cache_ttl_seconds=30,
        cache_entries=8,
        max_concurrent_queries=2,
        duckdb_threads=1,
    )


@pytest.fixture
def client(settings: ProxySettings) -> TestClient:
    return TestClient(create_app(settings))
