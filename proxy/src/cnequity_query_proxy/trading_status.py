"""交易状态查询、缓存、分区裁剪与并发保护。"""

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
    LakeUnavailable,
    QueryBusy,
    QueryFailed,
    QueryTooWide,
)
from cnequity_query_proxy.partitions import PartitionIndex
from cnequity_query_proxy.settings import ProxySettings

logger = logging.getLogger(__name__)

TradingState = Literal["normal", "suspended", "delisted"]


@dataclass(frozen=True)
class TradingStatus:
    """一只证券在一个交易日的本地状态记录。"""

    trade_date: date
    is_trading: bool
    status: str
    risk_warning: bool | None


@dataclass(frozen=True)
class TradingStatusPage:
    """带交易日游标的交易状态页。"""

    statuses: tuple[TradingStatus, ...]
    next_cursor: date | None


@dataclass(frozen=True)
class _CacheEntry:
    value: TradingStatusPage
    expires_at: float


class TradingStatusCache:
    """有界 TTL LRU，缓存不可变的交易状态查询结果。"""

    def __init__(self, *, ttl_seconds: float, max_entries: int):
        self._ttl_seconds = ttl_seconds
        self._max_entries = max_entries
        self._entries: OrderedDict[tuple[object, ...], _CacheEntry] = OrderedDict()
        self._lock = threading.Lock()

    def get(self, key: tuple[object, ...]) -> TradingStatusPage | None:
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

    def put(self, key: tuple[object, ...], value: TradingStatusPage) -> None:
        if self._ttl_seconds == 0:
            return
        with self._lock:
            self._entries[key] = _CacheEntry(value, time.monotonic() + self._ttl_seconds)
            self._entries.move_to_end(key)
            while len(self._entries) > self._max_entries:
                self._entries.popitem(last=False)


class TradingStatusRepository:
    """仅读取 trading_status Parquet 布局的 DuckDB 查询器。"""

    def __init__(self, settings: ProxySettings):
        self._settings = settings
        self._partitions = PartitionIndex(
            settings.trading_status_root,
            ttl_seconds=settings.cache_ttl_seconds,
            dataset_name="trading_status",
        )

    def fetch(
        self,
        *,
        symbol: str,
        start: date,
        end: date,
        limit: int,
        cursor: date | None,
        status: TradingState | None,
        is_trading: bool | None,
        risk_warning: bool | None,
    ) -> TradingStatusPage:
        effective_start = max(start, cursor + timedelta(days=1)) if cursor else start
        if effective_start > end:
            return TradingStatusPage((), None)

        for attempt in range(2):
            try:
                files = self._partitions.files_for(effective_start, end)
            except FileNotFoundError as exc:
                raise LakeUnavailable(str(exc)) from exc
            if not files:
                return TradingStatusPage((), None)
            if len(files) > self._settings.max_files:
                raise QueryTooWide(
                    f"查询需要打开 {len(files)} 个 Parquet 文件，超过上限 "
                    f"{self._settings.max_files}；请缩短日期范围"
                )

            try:
                return self._query(
                    files=files,
                    symbol=symbol,
                    start=effective_start,
                    end=end,
                    limit=limit,
                    status=status,
                    is_trading=is_trading,
                    risk_warning=risk_warning,
                )
            except duckdb.Error as exc:
                # compact 可能在目录索引和实际打开文件之间替换某个 part；刷新一次即可。
                if attempt == 0:
                    logger.info("交易状态文件在读取中变化，刷新分区索引后重试: %s", exc)
                    self._partitions.refresh()
                    continue
                logger.warning("交易状态查询失败: %s", exc)
                raise QueryFailed("交易状态暂时不可读，请稍后重试") from exc

        raise QueryFailed("交易状态暂时不可读，请稍后重试")  # pragma: no cover

    @staticmethod
    def _paths(files: list[Path]) -> list[str]:
        return [str(path) for path in files]

    def _query(
        self,
        *,
        files: list[Path],
        symbol: str,
        start: date,
        end: date,
        limit: int,
        status: TradingState | None,
        is_trading: bool | None,
        risk_warning: bool | None,
    ) -> TradingStatusPage:
        conditions = ["symbol = ?", "trade_date >= ?", "trade_date <= ?"]
        parameters: list[object] = [self._paths(files), symbol, start, end]
        if status is not None:
            conditions.append("status = ?")
            parameters.append(status)
        if is_trading is not None:
            conditions.append("is_trading = ?")
            parameters.append(is_trading)
        if risk_warning is not None:
            conditions.append("risk_warning = ?")
            parameters.append(risk_warning)

        connection = duckdb.connect(
            database=":memory:",
            config={"threads": str(self._settings.duckdb_threads)},
        )
        try:
            rows = connection.execute(
                f"""
                SELECT trade_date, is_trading, status, risk_warning
                -- 月分区名同样叫 trade_date，不能让 Hive 推断覆盖文件内的日级列。
                FROM read_parquet(?, union_by_name = true, hive_partitioning = false)
                WHERE {" AND ".join(conditions)}
                ORDER BY trade_date ASC
                LIMIT ?
                """,
                [*parameters, limit + 1],
            ).fetchall()
        finally:
            connection.close()

        has_more = len(rows) > limit
        statuses = tuple(
            TradingStatus(
                trade_date=row[0],
                is_trading=bool(row[1]),
                status=str(row[2]),
                risk_warning=bool(row[3]) if row[3] is not None else None,
            )
            for row in rows[:limit]
        )
        return TradingStatusPage(
            statuses=statuses,
            next_cursor=statuses[-1].trade_date if has_more else None,
        )


class TradingStatusService:
    """交易状态请求缓存与磁盘扫描并发上限。"""

    def __init__(
        self,
        settings: ProxySettings,
        *,
        slots: threading.BoundedSemaphore | None = None,
    ):
        self._repository = TradingStatusRepository(settings)
        self._cache = TradingStatusCache(
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
        status: TradingState | None,
        is_trading: bool | None,
        risk_warning: bool | None,
    ) -> tuple[TradingStatusPage, bool]:
        key = (
            symbol,
            start,
            end,
            limit,
            cursor,
            status,
            is_trading,
            risk_warning,
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
                status=status,
                is_trading=is_trading,
                risk_warning=risk_warning,
            )
            self._cache.put(key, page)
            return page, False
        finally:
            self._slots.release()
