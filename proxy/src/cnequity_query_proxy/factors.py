"""复权因子查询、缓存与并发保护。"""

from __future__ import annotations

import logging
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Literal

import duckdb

from cnequity_query_proxy.kline import (
    AdjustmentCoverageError,
    LakeUnavailable,
    QueryBusy,
    QueryFailed,
    QueryTooWide,
)
from cnequity_query_proxy.partitions import PartitionIndex
from cnequity_query_proxy.settings import ProxySettings

logger = logging.getLogger(__name__)

FactorAdjustment = Literal["hfq", "qfq"]


@dataclass(frozen=True)
class AdjustmentFactor:
    """一个交易日可直接乘到原始价格上的复权因子。"""

    trade_date: date
    factor: float


@dataclass(frozen=True)
class AdjustmentFactorPage:
    """带游标与前复权基准信息的因子页。"""

    factors: tuple[AdjustmentFactor, ...]
    next_cursor: date | None
    base_date: date | None
    base_factor_date: date | None


@dataclass(frozen=True)
class _CacheEntry:
    value: AdjustmentFactorPage
    expires_at: float


class FactorCache:
    """有界 TTL LRU，缓存不可变的复权因子查询结果。"""

    def __init__(self, *, ttl_seconds: float, max_entries: int):
        self._ttl_seconds = ttl_seconds
        self._max_entries = max_entries
        self._entries: OrderedDict[tuple[object, ...], _CacheEntry] = OrderedDict()
        self._lock = threading.Lock()

    def get(self, key: tuple[object, ...]) -> AdjustmentFactorPage | None:
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

    def put(self, key: tuple[object, ...], value: AdjustmentFactorPage) -> None:
        if self._ttl_seconds == 0:
            return
        with self._lock:
            self._entries[key] = _CacheEntry(value, time.monotonic() + self._ttl_seconds)
            self._entries.move_to_end(key)
            while len(self._entries) > self._max_entries:
                self._entries.popitem(last=False)


class AdjustmentFactorRepository:
    """仅读取 ``adj_factors`` Parquet 布局的因子查询器。"""

    def __init__(self, settings: ProxySettings):
        self._settings = settings
        self._partitions = PartitionIndex(
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
        cursor: date | None,
        adjustment: FactorAdjustment,
        base_date: date | None,
    ) -> AdjustmentFactorPage:
        effective_start = max(start, cursor + timedelta(days=1)) if cursor else start
        if effective_start > end:
            return AdjustmentFactorPage((), None, base_date if adjustment == "qfq" else None, None)

        for attempt in range(2):
            try:
                page_files = self._partitions.files_for(effective_start, end)
                base_files: list[Path] = []
                if adjustment == "qfq":
                    assert base_date is not None  # 路由层已验证这个公开契约。
                    base_files = self._partitions.files_for(base_date, base_date)
            except FileNotFoundError as exc:
                raise LakeUnavailable(str(exc)) from exc

            file_count = len(set(page_files).union(base_files))
            if file_count > self._settings.max_files:
                raise QueryTooWide(
                    f"查询需要打开 {file_count} 个 Parquet 文件，超过上限 "
                    f"{self._settings.max_files}；请缩短日期范围"
                )
            if adjustment == "hfq" and not page_files:
                return AdjustmentFactorPage((), None, None, None)

            try:
                return self._query(
                    page_files=page_files,
                    base_files=base_files,
                    symbol=symbol,
                    start=effective_start,
                    end=end,
                    limit=limit,
                    adjustment=adjustment,
                    base_date=base_date,
                )
            except duckdb.Error as exc:
                # compact 可能在目录索引和实际打开文件之间替换某个 part；刷新一次即可。
                if attempt == 0:
                    logger.info("复权因子文件在读取中变化，刷新分区索引后重试: %s", exc)
                    self._partitions.refresh()
                    continue
                logger.warning("复权因子查询失败: %s", exc)
                raise QueryFailed("复权因子暂时不可读，请稍后重试") from exc

        raise QueryFailed("复权因子暂时不可读，请稍后重试")  # pragma: no cover

    def _query(
        self,
        *,
        page_files: list[Path],
        base_files: list[Path],
        symbol: str,
        start: date,
        end: date,
        limit: int,
        adjustment: FactorAdjustment,
        base_date: date | None,
    ) -> AdjustmentFactorPage:
        connection = duckdb.connect(
            database=":memory:",
            config={"threads": str(self._settings.duckdb_threads)},
        )
        try:
            if adjustment == "hfq":
                rows = self._hfq_rows(connection, page_files, symbol, start, end, limit)
                base_factor_date = None
            else:
                assert base_date is not None
                base_factor = self._base_factor(connection, base_files, symbol, base_date)
                if base_factor is None:
                    raise AdjustmentCoverageError(
                        f"qfq 基准日 {base_date} 缺少后复权因子，无法计算前复权因子"
                    )
                rows = self._qfq_rows(
                    connection,
                    page_files,
                    symbol,
                    start,
                    end,
                    limit,
                    base_factor,
                )
                base_factor_date = base_date
        finally:
            connection.close()

        has_more = len(rows) > limit
        factors = tuple(AdjustmentFactor(row[0], float(row[1])) for row in rows[:limit])
        return AdjustmentFactorPage(
            factors,
            factors[-1].trade_date if has_more else None,
            base_date if adjustment == "qfq" else None,
            base_factor_date,
        )

    @staticmethod
    def _paths(files: list[Path]) -> list[str]:
        return [str(path) for path in files]

    def _base_factor(
        self,
        connection: duckdb.DuckDBPyConnection,
        files: list[Path],
        symbol: str,
        base_date: date,
    ) -> float | None:
        if not files:
            return None
        row = connection.execute(
            """
            SELECT factor
            FROM read_parquet(?, union_by_name = true)
            WHERE symbol = ? AND adjust_type = 'hfq' AND trade_date = ?
              AND factor IS NOT NULL AND factor > 0 AND isfinite(factor)
            LIMIT 1
            """,
            [self._paths(files), symbol, base_date],
        ).fetchone()
        return float(row[0]) if row is not None else None

    def _hfq_rows(
        self,
        connection: duckdb.DuckDBPyConnection,
        files: list[Path],
        symbol: str,
        start: date,
        end: date,
        limit: int,
    ) -> list[tuple]:
        return connection.execute(
            """
            SELECT trade_date, factor
            FROM read_parquet(?, union_by_name = true)
            WHERE symbol = ? AND adjust_type = 'hfq'
              AND trade_date >= ? AND trade_date <= ?
              AND factor IS NOT NULL AND factor > 0 AND isfinite(factor)
            ORDER BY trade_date ASC
            LIMIT ?
            """,
            [self._paths(files), symbol, start, end, limit + 1],
        ).fetchall()

    def _qfq_rows(
        self,
        connection: duckdb.DuckDBPyConnection,
        files: list[Path],
        symbol: str,
        start: date,
        end: date,
        limit: int,
        base_factor: float,
    ) -> list[tuple]:
        if not files:
            return []
        return connection.execute(
            """
            SELECT trade_date, factor / ?
            FROM read_parquet(?, union_by_name = true)
            WHERE symbol = ? AND adjust_type = 'hfq'
              AND trade_date >= ? AND trade_date <= ?
              AND factor IS NOT NULL AND factor > 0 AND isfinite(factor)
            ORDER BY trade_date ASC
            LIMIT ?
            """,
            [base_factor, self._paths(files), symbol, start, end, limit + 1],
        ).fetchall()


class AdjustmentFactorService:
    """复权因子请求缓存与磁盘扫描并发上限。"""

    def __init__(
        self,
        settings: ProxySettings,
        *,
        slots: threading.BoundedSemaphore | None = None,
    ):
        self._repository = AdjustmentFactorRepository(settings)
        self._cache = FactorCache(
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
        cursor: date | None,
        adjustment: FactorAdjustment,
        base_date: date | None,
    ) -> tuple[AdjustmentFactorPage, bool]:
        key = (symbol, start, end, limit, cursor, adjustment, base_date)
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
                base_date=base_date,
            )
            self._cache.put(key, page)
            return page, False
        finally:
            self._slots.release()
