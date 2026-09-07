"""日内 K 线查询、前后复权、缓存与并发保护。"""

from __future__ import annotations

import logging
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Literal

import duckdb

from cnequity_query_proxy.kline import (
    Adjustment,
    AdjustmentCoverageError,
    LakeUnavailable,
    QueryBusy,
    QueryFailed,
    QueryTooWide,
)
from cnequity_query_proxy.partitions import PartitionIndex
from cnequity_query_proxy.settings import ProxySettings

logger = logging.getLogger(__name__)

IntradayInterval = Literal["1m", "5m"]


@dataclass(frozen=True)
class MinuteCandle:
    """一根日内 K；bar_time 是 Asia/Shanghai 的无时区收盘时间。"""

    trade_date: date
    bar_time: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int | None
    amount: float | None
    adjustment_factor: float | None
    adjustment_exact: bool | None


@dataclass(frozen=True)
class MinuteKlinePage:
    """带时间戳游标的日内 K 线页。"""

    candles: tuple[MinuteCandle, ...]
    next_cursor: datetime | None
    base_date: date | None
    base_factor_date: date | None


@dataclass(frozen=True)
class _CacheEntry:
    value: MinuteKlinePage
    expires_at: float


class MinuteKlineCache:
    """有界 TTL LRU，缓存不可变的日内 K 线查询结果。"""

    def __init__(self, *, ttl_seconds: float, max_entries: int):
        self._ttl_seconds = ttl_seconds
        self._max_entries = max_entries
        self._entries: OrderedDict[tuple[object, ...], _CacheEntry] = OrderedDict()
        self._lock = threading.Lock()

    def get(self, key: tuple[object, ...]) -> MinuteKlinePage | None:
        if self._ttl_seconds == 0:
            return None
        now = time.monotonic()
        with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                return None
            if entry.expires_at <= now:
                del self._entries[key]
                return None
            self._entries.move_to_end(key)
            return entry.value

    def put(self, key: tuple[object, ...], value: MinuteKlinePage) -> None:
        if self._ttl_seconds == 0:
            return
        with self._lock:
            self._entries[key] = _CacheEntry(value, time.monotonic() + self._ttl_seconds)
            self._entries.move_to_end(key)
            while len(self._entries) > self._max_entries:
                self._entries.popitem(last=False)


class MinuteKlineRepository:
    """只依赖日内 K 与 adj_factors Parquet 布局的 DuckDB 查询器。"""

    def __init__(self, settings: ProxySettings, *, interval: IntradayInterval = "1m"):
        self._settings = settings
        self._interval = interval
        self._bars_partitions = PartitionIndex(
            settings.intraday_bars_root(interval),
            ttl_seconds=settings.cache_ttl_seconds,
            dataset_name=f"minute_bars_{interval}",
        )
        self._factors_partitions = PartitionIndex(
            settings.adj_factors_root,
            ttl_seconds=settings.cache_ttl_seconds,
            dataset_name="adj_factors",
        )

    def fetch(
        self,
        *,
        symbol: str,
        start: date,
        end: date,
        limit: int,
        cursor: datetime | None,
        adjustment: Adjustment,
        strict_adjustment: bool,
        base_date: date | None,
    ) -> MinuteKlinePage:
        effective_start = max(start, cursor.date()) if cursor is not None else start
        if effective_start > end:
            return MinuteKlinePage((), None, base_date, None)

        for attempt in range(2):
            try:
                bar_files = self._bars_partitions.files_for(effective_start, end)
            except FileNotFoundError as exc:
                raise LakeUnavailable(str(exc)) from exc
            if not bar_files:
                return MinuteKlinePage((), None, base_date, None)

            factor_files: list[Path] = []
            if adjustment != "none":
                try:
                    factor_files = self._factors_partitions.files_for(effective_start, end)
                    if adjustment == "qfq":
                        assert base_date is not None  # 路由层已验证这个公开契约。
                        if not effective_start <= base_date <= end:
                            factor_files.extend(
                                self._factors_partitions.files_for(base_date, base_date)
                            )
                        factor_files = sorted(set(factor_files))
                except FileNotFoundError:
                    # 因子尚未生成时保留原价，并在每根 K 上显式标记为不精确。
                    factor_files = []
            file_count = len(bar_files) + len(factor_files)
            if file_count > self._settings.max_files:
                raise QueryTooWide(
                    f"查询需要打开 {file_count} 个 Parquet 文件，超过上限 "
                    f"{self._settings.max_files}；请缩短日期范围"
                )

            try:
                return self._query(
                    bar_files=bar_files,
                    factor_files=factor_files,
                    symbol=symbol,
                    effective_start=effective_start,
                    end=end,
                    limit=limit,
                    cursor=cursor,
                    adjustment=adjustment,
                    strict_adjustment=strict_adjustment,
                    base_date=base_date,
                )
            except duckdb.Error as exc:
                # compact 可能在目录索引和实际打开文件之间替换某个 part；刷新一次即可。
                if attempt == 0:
                    logger.info(
                        "%s K 文件在读取中变化，刷新分区索引后重试: %s", self._interval, exc
                    )
                    self._bars_partitions.refresh()
                    self._factors_partitions.refresh()
                    continue
                logger.warning("%s K 查询失败: %s", self._interval, exc)
                raise QueryFailed(f"{self._interval} K 数据暂时不可读，请稍后重试") from exc

        raise QueryFailed(f"{self._interval} K 数据暂时不可读，请稍后重试")  # pragma: no cover

    def _query(
        self,
        *,
        bar_files: list[Path],
        factor_files: list[Path],
        symbol: str,
        effective_start: date,
        end: date,
        limit: int,
        cursor: datetime | None,
        adjustment: Adjustment,
        strict_adjustment: bool,
        base_date: date | None,
    ) -> MinuteKlinePage:
        connection = duckdb.connect(
            database=":memory:",
            config={"threads": str(self._settings.duckdb_threads)},
        )
        try:
            if adjustment == "none":
                rows = self._raw_rows(
                    connection, bar_files, symbol, effective_start, end, limit, cursor
                )
            elif not factor_files:
                rows = self._inexact_rows(
                    connection, bar_files, symbol, effective_start, end, limit, cursor
                )
            elif adjustment == "hfq":
                rows = self._hfq_rows(
                    connection,
                    bar_files,
                    factor_files,
                    symbol,
                    effective_start,
                    end,
                    limit,
                    cursor,
                )
            else:
                rows = self._qfq_rows(
                    connection,
                    bar_files,
                    factor_files,
                    symbol,
                    effective_start,
                    end,
                    limit,
                    cursor,
                    base_date,
                )
        finally:
            connection.close()

        has_more = len(rows) > limit
        page_rows = rows[:limit]
        candles = tuple(
            MinuteCandle(
                trade_date=row[0],
                bar_time=row[1],
                open=float(row[2]),
                high=float(row[3]),
                low=float(row[4]),
                close=float(row[5]),
                volume=int(row[6]) if row[6] is not None else None,
                amount=float(row[7]) if row[7] is not None else None,
                adjustment_factor=float(row[8]) if row[8] is not None else None,
                adjustment_exact=bool(row[9]) if row[9] is not None else None,
            )
            for row in page_rows
        )
        base_factor_date = page_rows[0][10] if adjustment == "qfq" and page_rows else None
        if strict_adjustment and adjustment != "none":
            if adjustment == "qfq" and candles and base_factor_date != base_date:
                raise AdjustmentCoverageError(
                    f"qfq 基准日 {base_date} 缺少后复权因子；"
                    "可传 strict_adjustment=false 并检查 adjustment_exact"
                )
            inexact_dates = sorted(
                {candle.trade_date.isoformat() for candle in candles if not candle.adjustment_exact}
            )
            if inexact_dates:
                raise AdjustmentCoverageError(
                    f"{adjustment} 缺少复权因子覆盖: {', '.join(inexact_dates[:3])}；"
                    "可传 strict_adjustment=false 并检查 adjustment_exact"
                )
        return MinuteKlinePage(
            candles,
            candles[-1].bar_time if has_more else None,
            base_date if adjustment == "qfq" else None,
            base_factor_date,
        )

    @staticmethod
    def _paths(files: list[Path]) -> list[str]:
        return [str(path) for path in files]

    def _raw_rows(
        self,
        connection: duckdb.DuckDBPyConnection,
        files: list[Path],
        symbol: str,
        start: date,
        end: date,
        limit: int,
        cursor: datetime | None,
    ) -> list[tuple]:
        cursor_filter = "AND bar_time > ?" if cursor is not None else ""
        parameters: list[object] = [self._paths(files), symbol, self._interval, start, end]
        if cursor is not None:
            parameters.append(cursor)
        parameters.append(limit + 1)
        return connection.execute(
            f"""
            SELECT trade_date, bar_time, open, high, low, close, volume, amount,
                   NULL::DOUBLE, NULL::BOOLEAN, NULL::DATE
            FROM read_parquet(?, union_by_name = true)
            WHERE symbol = ? AND frequency = ? AND trade_date >= ? AND trade_date <= ?
            {cursor_filter}
            ORDER BY bar_time ASC
            LIMIT ?
            """,
            parameters,
        ).fetchall()

    def _inexact_rows(
        self,
        connection: duckdb.DuckDBPyConnection,
        files: list[Path],
        symbol: str,
        start: date,
        end: date,
        limit: int,
        cursor: datetime | None,
    ) -> list[tuple]:
        cursor_filter = "AND bar_time > ?" if cursor is not None else ""
        parameters: list[object] = [self._paths(files), symbol, self._interval, start, end]
        if cursor is not None:
            parameters.append(cursor)
        parameters.append(limit + 1)
        return connection.execute(
            f"""
            SELECT trade_date, bar_time, open, high, low, close, volume, amount,
                   1.0::DOUBLE, false, NULL::DATE
            FROM read_parquet(?, union_by_name = true)
            WHERE symbol = ? AND frequency = ? AND trade_date >= ? AND trade_date <= ?
            {cursor_filter}
            ORDER BY bar_time ASC
            LIMIT ?
            """,
            parameters,
        ).fetchall()

    def _hfq_rows(
        self,
        connection: duckdb.DuckDBPyConnection,
        bar_files: list[Path],
        factor_files: list[Path],
        symbol: str,
        start: date,
        end: date,
        limit: int,
        cursor: datetime | None,
    ) -> list[tuple]:
        cursor_filter = "AND b.bar_time > ?" if cursor is not None else ""
        parameters: list[object] = [
            self._paths(factor_files),
            symbol,
            start,
            end,
            self._paths(bar_files),
            symbol,
            self._interval,
            start,
            end,
        ]
        if cursor is not None:
            parameters.append(cursor)
        parameters.append(limit + 1)
        return connection.execute(
            f"""
            WITH factors AS (
                SELECT trade_date, factor
                FROM read_parquet(?, union_by_name = true)
                WHERE symbol = ? AND adjust_type = 'hfq'
                  AND trade_date >= ? AND trade_date <= ?
            )
            SELECT b.trade_date, b.bar_time,
                   b.open * COALESCE(f.factor, 1.0),
                   b.high * COALESCE(f.factor, 1.0),
                   b.low * COALESCE(f.factor, 1.0),
                   b.close * COALESCE(f.factor, 1.0),
                   b.volume, b.amount, COALESCE(f.factor, 1.0),
                   f.factor IS NOT NULL, NULL::DATE
            FROM read_parquet(?, union_by_name = true) b
            LEFT JOIN factors f ON b.trade_date = f.trade_date
            WHERE b.symbol = ? AND b.frequency = ?
              AND b.trade_date >= ? AND b.trade_date <= ?
              {cursor_filter}
            ORDER BY b.bar_time ASC
            LIMIT ?
            """,
            parameters,
        ).fetchall()

    def _qfq_rows(
        self,
        connection: duckdb.DuckDBPyConnection,
        bar_files: list[Path],
        factor_files: list[Path],
        symbol: str,
        effective_start: date,
        end: date,
        limit: int,
        cursor: datetime | None,
        base_date: date | None,
    ) -> list[tuple]:
        assert base_date is not None
        cursor_filter = "AND bar_time > ?" if cursor is not None else ""
        parameters: list[object] = [
            self._paths(bar_files),
            symbol,
            self._interval,
            effective_start,
            end,
        ]
        if cursor is not None:
            parameters.append(cursor)
        parameters.extend(
            [
                self._paths(factor_files),
                symbol,
                effective_start,
                end,
                base_date,
                base_date,
                limit + 1,
            ]
        )
        return connection.execute(
            f"""
            WITH bars AS MATERIALIZED (
                SELECT trade_date, bar_time, open, high, low, close, volume, amount
                FROM read_parquet(?, union_by_name = true)
                WHERE symbol = ? AND frequency = ?
                  AND trade_date >= ? AND trade_date <= ?
                  {cursor_filter}
            ),
            factors AS (
                SELECT trade_date, factor
                FROM read_parquet(?, union_by_name = true)
                WHERE symbol = ? AND adjust_type = 'hfq'
                  AND ((trade_date >= ? AND trade_date <= ?) OR trade_date = ?)
            ),
            base_factor AS (
                SELECT trade_date, factor
                FROM factors
                WHERE trade_date = ?
                LIMIT 1
            )
            SELECT b.trade_date, b.bar_time,
                   b.open * COALESCE(f.factor / a.factor, 1.0),
                   b.high * COALESCE(f.factor / a.factor, 1.0),
                   b.low * COALESCE(f.factor / a.factor, 1.0),
                   b.close * COALESCE(f.factor / a.factor, 1.0),
                   b.volume, b.amount, COALESCE(f.factor / a.factor, 1.0),
                   f.factor IS NOT NULL AND a.factor IS NOT NULL,
                   a.trade_date
            FROM bars b
            LEFT JOIN factors f ON b.trade_date = f.trade_date
            LEFT JOIN base_factor a ON true
            ORDER BY b.bar_time ASC
            LIMIT ?
            """,
            parameters,
        ).fetchall()


class MinuteKlineService:
    """日内 K 请求缓存与磁盘扫描并发上限。"""

    def __init__(
        self,
        settings: ProxySettings,
        *,
        interval: IntradayInterval = "1m",
        slots: threading.BoundedSemaphore | None = None,
    ):
        self._interval = interval
        self._repository = MinuteKlineRepository(settings, interval=interval)
        self._cache = MinuteKlineCache(
            ttl_seconds=settings.cache_ttl_seconds,
            max_entries=settings.cache_entries,
        )
        self._slots = slots or threading.BoundedSemaphore(settings.max_concurrent_queries)

    def query(
        self,
        *,
        symbol: str,
        start: date,
        end: date,
        limit: int,
        cursor: datetime | None,
        adjustment: Adjustment,
        strict_adjustment: bool,
        base_date: date | None,
    ) -> tuple[MinuteKlinePage, bool]:
        key = (
            self._interval,
            symbol,
            start,
            end,
            limit,
            cursor,
            adjustment,
            strict_adjustment,
            base_date,
        )
        cached = self._cache.get(key)
        if cached is not None:
            return cached, True
        if not self._slots.acquire(blocking=False):
            raise QueryBusy("查询繁忙，请稍后重试")
        try:
            cached = self._cache.get(key)
            if cached is not None:
                return cached, True
            page = self._repository.fetch(
                symbol=symbol,
                start=start,
                end=end,
                limit=limit,
                cursor=cursor,
                adjustment=adjustment,
                strict_adjustment=strict_adjustment,
                base_date=base_date,
            )
            self._cache.put(key, page)
            return page, False
        finally:
            self._slots.release()
