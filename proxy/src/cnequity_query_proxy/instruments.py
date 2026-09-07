"""证券基础信息查询、缓存与并发保护。"""

from __future__ import annotations

import logging
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import duckdb

from cnequity_query_proxy.kline import LakeUnavailable, QueryBusy, QueryFailed
from cnequity_query_proxy.settings import ProxySettings

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Instrument:
    """一个证券主数据条目，不包含采集侧溯源字段。"""

    symbol: str
    name: str
    exchange: str
    asset_type: str
    list_date: date | None
    delist_date: date | None
    prev_symbol: str | None


@dataclass(frozen=True)
class InstrumentPage:
    """按 symbol 正序返回的证券主数据页。"""

    instruments: tuple[Instrument, ...]
    next_cursor: str | None


@dataclass(frozen=True)
class _CacheEntry:
    value: InstrumentPage
    expires_at: float


class InstrumentCache:
    """有界 TTL LRU，缓存不可变的证券主数据查询结果。"""

    def __init__(self, *, ttl_seconds: float, max_entries: int):
        self._ttl_seconds = ttl_seconds
        self._max_entries = max_entries
        self._entries: OrderedDict[tuple[object, ...], _CacheEntry] = OrderedDict()
        self._lock = threading.Lock()

    def get(self, key: tuple[object, ...]) -> InstrumentPage | None:
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

    def put(self, key: tuple[object, ...], value: InstrumentPage) -> None:
        if self._ttl_seconds == 0:
            return
        with self._lock:
            self._entries[key] = _CacheEntry(value, time.monotonic() + self._ttl_seconds)
            self._entries.move_to_end(key)
            while len(self._entries) > self._max_entries:
                self._entries.popitem(last=False)


class InstrumentRepository:
    """仅读取单个 instruments canonical Parquet 文件的 DuckDB 查询器。"""

    def __init__(self, settings: ProxySettings):
        self._settings = settings

    def fetch(
        self,
        *,
        symbol: str | None,
        query: str | None,
        exchange: str | None,
        asset_type: str | None,
        as_of: date | None,
        cursor: str | None,
        limit: int,
    ) -> InstrumentPage:
        path = self._settings.instruments_file
        if not path.is_file():
            raise LakeUnavailable(f"instruments 主文件不存在: {path}")

        for attempt in range(2):
            try:
                return self._query(
                    path=path,
                    duckdb_threads=self._settings.duckdb_threads,
                    symbol=symbol,
                    query=query,
                    exchange=exchange,
                    asset_type=asset_type,
                    as_of=as_of,
                    cursor=cursor,
                    limit=limit,
                )
            except duckdb.Error as exc:
                # compact 可能在文件检查和打开之间原子替换主文件，重试一次即可。
                if attempt == 0:
                    logger.info("证券主文件在读取中变化，重试: %s", exc)
                    continue
                logger.warning("证券主数据查询失败: %s", exc)
                raise QueryFailed("证券主数据暂时不可读，请稍后重试") from exc

        raise QueryFailed("证券主数据暂时不可读，请稍后重试")  # pragma: no cover

    @staticmethod
    def _query(
        *,
        path: Path,
        duckdb_threads: int,
        symbol: str | None,
        query: str | None,
        exchange: str | None,
        asset_type: str | None,
        as_of: date | None,
        cursor: str | None,
        limit: int,
    ) -> InstrumentPage:
        conditions: list[str] = []
        parameters: list[object] = [str(path)]
        if symbol is not None:
            conditions.append("symbol = ?")
            parameters.append(symbol)
        if exchange is not None:
            conditions.append("exchange = ?")
            parameters.append(exchange)
        if asset_type is not None:
            conditions.append("asset_type = ?")
            parameters.append(asset_type)
        if as_of is not None:
            conditions.append(
                "(list_date IS NULL OR list_date <= ?) "
                "AND (delist_date IS NULL OR delist_date >= ?)"
            )
            parameters.extend((as_of, as_of))
        if cursor is not None:
            conditions.append("symbol > ?")
            parameters.append(cursor)
        if query is not None:
            conditions.append(
                "(contains(lower(symbol), lower(?)) OR contains(lower(name), lower(?)))"
            )
            parameters.extend((query, query))

        where = " AND ".join(conditions) if conditions else "TRUE"
        connection = duckdb.connect(
            database=":memory:",
            config={"threads": str(duckdb_threads)},
        )
        try:
            rows = connection.execute(
                f"""
                SELECT symbol, name, exchange, asset_type, list_date, delist_date, prev_symbol
                FROM read_parquet(?)
                WHERE {where}
                ORDER BY symbol ASC
                LIMIT ?
                """,
                [*parameters, limit + 1],
            ).fetchall()
        finally:
            connection.close()

        has_more = len(rows) > limit
        instruments = tuple(
            Instrument(
                symbol=row[0],
                name=row[1],
                exchange=row[2],
                asset_type=row[3],
                list_date=row[4],
                delist_date=row[5],
                prev_symbol=row[6],
            )
            for row in rows[:limit]
        )
        return InstrumentPage(
            instruments=instruments,
            next_cursor=instruments[-1].symbol if has_more else None,
        )


class InstrumentService:
    """证券主数据请求缓存与磁盘扫描并发上限。"""

    def __init__(
        self,
        settings: ProxySettings,
        *,
        slots: threading.BoundedSemaphore | None = None,
    ):
        self._repository = InstrumentRepository(settings)
        self._cache = InstrumentCache(
            ttl_seconds=settings.cache_ttl_seconds,
            max_entries=settings.cache_entries,
        )
        self._slots = slots or threading.BoundedSemaphore(settings.max_concurrent_queries)

    def query(
        self,
        *,
        symbol: str | None,
        query: str | None,
        exchange: str | None,
        asset_type: str | None,
        as_of: date | None,
        cursor: str | None,
        limit: int,
    ) -> tuple[InstrumentPage, bool]:
        key = (symbol, query, exchange, asset_type, as_of, cursor, limit)
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
                query=query,
                exchange=exchange,
                asset_type=asset_type,
                as_of=as_of,
                cursor=cursor,
                limit=limit,
            )
            self._cache.put(key, page)
            return page, False
        finally:
            self._slots.release()
