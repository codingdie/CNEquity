"""日 K 查询、前后复权、缓存与并发保护。"""

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

from cnequity_query_proxy.partitions import PartitionIndex
from cnequity_query_proxy.settings import ProxySettings

logger = logging.getLogger(__name__)

Adjustment = Literal["none", "hfq", "qfq"]


class LakeUnavailable(RuntimeError):
    """日 K 文件目录暂时不可用。"""


class QueryTooWide(ValueError):
    """请求会打开过多文件。"""


class QueryBusy(RuntimeError):
    """并发扫描已到上限。"""


class QueryFailed(RuntimeError):
    """Parquet 文件在读取中变化或不符合最小日 K 契约。"""


class AdjustmentCoverageError(RuntimeError):
    """请求的复权类型缺少必要因子。"""


@dataclass(frozen=True)
class Candle:
    """一根日 K；有复权时 OHLC 已按 adjustment_factor 计算。"""

    trade_date: date
    open: float
    high: float
    low: float
    close: float
    volume: int | None
    amount: float | None
    adjustment_factor: float | None
    adjustment_exact: bool | None


@dataclass(frozen=True)
class KlinePage:
    """带游标的 K 线页。"""

    candles: tuple[Candle, ...]
    next_cursor: date | None
    base_date: date | None
    base_factor_date: date | None


@dataclass(frozen=True)
class _CacheEntry:
    value: KlinePage
    expires_at: float


class KlineCache:
    """有界 TTL LRU，缓存已经序列化前的不可变查询结果。"""

    def __init__(self, *, ttl_seconds: float, max_entries: int):
        self._ttl_seconds = ttl_seconds
        self._max_entries = max_entries
        self._entries: OrderedDict[tuple[object, ...], _CacheEntry] = OrderedDict()
        self._lock = threading.Lock()

    def get(self, key: tuple[object, ...]) -> KlinePage | None:
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

    def put(self, key: tuple[object, ...], value: KlinePage) -> None:
        if self._ttl_seconds == 0:
            return
        with self._lock:
            self._entries[key] = _CacheEntry(value, time.monotonic() + self._ttl_seconds)
            self._entries.move_to_end(key)
            while len(self._entries) > self._max_entries:
                self._entries.popitem(last=False)


class KlineRepository:
    """只依赖日 K 与复权因子 Parquet 布局的 DuckDB 查询器。"""

    def __init__(self, settings: ProxySettings):
        self._settings = settings
        self._bars_partitions = PartitionIndex(
            settings.daily_bars_root,
            ttl_seconds=settings.cache_ttl_seconds,
            dataset_name="daily_bars",
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
        cursor: date | None,
        adjustment: Adjustment,
        strict_adjustment: bool,
        base_date: date | None,
    ) -> KlinePage:
        effective_start = max(start, cursor + timedelta(days=1)) if cursor else start
        if effective_start > end:
            return KlinePage((), None, base_date, None)

        for attempt in range(2):
            try:
                bar_files = self._bars_partitions.files_for(effective_start, end)
            except FileNotFoundError as exc:
                raise LakeUnavailable(str(exc)) from exc
            if not bar_files:
                return KlinePage((), None, base_date, None)

            factor_files: list[Path] = []
            if adjustment != "none":
                try:
                    if adjustment == "qfq":
                        assert base_date is not None  # 路由层已验证这个公开契约。
                        # 翻页只需要当前页的日因子和显式基准日因子；不要因首个
                        # 页面范围而在每一页重复打开已消费的历史分区。
                        factor_files = self._factors_partitions.files_for(effective_start, end)
                        if not effective_start <= base_date <= end:
                            factor_files.extend(
                                self._factors_partitions.files_for(base_date, base_date)
                            )
                        factor_files = sorted(set(factor_files))
                    else:
                        factor_files = self._factors_partitions.files_for(effective_start, end)
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
                    adjustment=adjustment,
                    strict_adjustment=strict_adjustment,
                    base_date=base_date,
                )
            except duckdb.Error as exc:
                # compact 可能在目录索引和实际打开文件之间替换某个 part；刷新一次即可。
                if attempt == 0:
                    logger.info("日 K 文件在读取中变化，刷新分区索引后重试: %s", exc)
                    self._bars_partitions.refresh()
                    self._factors_partitions.refresh()
                    continue
                logger.warning("日 K 查询失败: %s", exc)
                raise QueryFailed("日 K 数据暂时不可读，请稍后重试") from exc

        raise QueryFailed("日 K 数据暂时不可读，请稍后重试")  # pragma: no cover

    def _query(
        self,
        *,
        bar_files: list[Path],
        factor_files: list[Path],
        symbol: str,
        effective_start: date,
        end: date,
        limit: int,
        adjustment: Adjustment,
        strict_adjustment: bool,
        base_date: date | None,
    ) -> KlinePage:
        connection = duckdb.connect(
            database=":memory:",
            config={"threads": str(self._settings.duckdb_threads)},
        )
        try:
            if adjustment == "none":
                rows = self._raw_rows(connection, bar_files, symbol, effective_start, end, limit)
            elif not factor_files:
                rows = self._inexact_rows(
                    connection, bar_files, symbol, effective_start, end, limit
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
                    base_date,
                )
        finally:
            connection.close()

        has_more = len(rows) > limit
        page_rows = rows[:limit]
        candles = tuple(
            Candle(
                trade_date=row[0],
                open=float(row[1]),
                high=float(row[2]),
                low=float(row[3]),
                close=float(row[4]),
                volume=int(row[5]) if row[5] is not None else None,
                amount=float(row[6]) if row[6] is not None else None,
                adjustment_factor=float(row[7]) if row[7] is not None else None,
                adjustment_exact=bool(row[8]) if row[8] is not None else None,
            )
            for row in page_rows
        )
        base_factor_date = page_rows[0][9] if adjustment == "qfq" and page_rows else None
        if strict_adjustment and adjustment != "none":
            if adjustment == "qfq" and candles and base_factor_date != base_date:
                raise AdjustmentCoverageError(
                    f"qfq 基准日 {base_date} 缺少后复权因子；"
                    "可传 strict_adjustment=false 并检查 adjustment_exact"
                )
            inexact = [
                candle.trade_date.isoformat() for candle in candles if not candle.adjustment_exact
            ]
            if inexact:
                raise AdjustmentCoverageError(
                    f"{adjustment} 缺少复权因子覆盖: {', '.join(inexact[:3])}；"
                    "可传 strict_adjustment=false 并检查 adjustment_exact"
                )
        return KlinePage(
            candles,
            candles[-1].trade_date if has_more else None,
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
    ) -> list[tuple]:
        return connection.execute(
            """
            SELECT trade_date, open, high, low, close, volume, amount,
                   NULL::DOUBLE, NULL::BOOLEAN, NULL::DATE
            FROM read_parquet(?, union_by_name = true)
            WHERE symbol = ? AND trade_date >= ? AND trade_date <= ?
            ORDER BY trade_date ASC
            LIMIT ?
            """,
            [self._paths(files), symbol, start, end, limit + 1],
        ).fetchall()

    def _inexact_rows(
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
            SELECT trade_date, open, high, low, close, volume, amount,
                   1.0::DOUBLE, false, NULL::DATE
            FROM read_parquet(?, union_by_name = true)
            WHERE symbol = ? AND trade_date >= ? AND trade_date <= ?
            ORDER BY trade_date ASC
            LIMIT ?
            """,
            [self._paths(files), symbol, start, end, limit + 1],
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
    ) -> list[tuple]:
        return connection.execute(
            """
            WITH factors AS (
                SELECT trade_date, factor
                FROM read_parquet(?, union_by_name = true)
                WHERE symbol = ? AND adjust_type = 'hfq'
                  AND trade_date >= ? AND trade_date <= ?
            )
            SELECT b.trade_date,
                   b.open * COALESCE(f.factor, 1.0),
                   b.high * COALESCE(f.factor, 1.0),
                   b.low * COALESCE(f.factor, 1.0),
                   b.close * COALESCE(f.factor, 1.0),
                   b.volume, b.amount, COALESCE(f.factor, 1.0),
                   f.factor IS NOT NULL, NULL::DATE
            FROM read_parquet(?, union_by_name = true) b
            LEFT JOIN factors f ON b.trade_date = f.trade_date
            WHERE b.symbol = ? AND b.trade_date >= ? AND b.trade_date <= ?
            ORDER BY b.trade_date ASC
            LIMIT ?
            """,
            [
                self._paths(factor_files),
                symbol,
                start,
                end,
                self._paths(bar_files),
                symbol,
                start,
                end,
                limit + 1,
            ],
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
        base_date: date | None,
    ) -> list[tuple]:
        assert base_date is not None
        return connection.execute(
            """
            WITH bars AS MATERIALIZED (
                SELECT trade_date, open, high, low, close, volume, amount
                FROM read_parquet(?, union_by_name = true)
                WHERE symbol = ? AND trade_date >= ? AND trade_date <= ?
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
            SELECT b.trade_date,
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
            ORDER BY b.trade_date ASC
            LIMIT ?
            """,
            [
                self._paths(bar_files),
                symbol,
                effective_start,
                end,
                self._paths(factor_files),
                symbol,
                effective_start,
                end,
                base_date,
                base_date,
                limit + 1,
            ],
        ).fetchall()


class KlineService:
    """请求缓存与磁盘扫描并发上限。"""

    def __init__(
        self,
        settings: ProxySettings,
        *,
        slots: threading.BoundedSemaphore | None = None,
    ):
        self._repository = KlineRepository(settings)
        self._cache = KlineCache(
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
        adjustment: Adjustment,
        strict_adjustment: bool,
        base_date: date | None,
    ) -> tuple[KlinePage, bool]:
        key = (symbol, start, end, limit, cursor, adjustment, strict_adjustment, base_date)
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
