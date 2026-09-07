"""周线、月线聚合查询、复权、缓存与并发保护。"""

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

PeriodInterval = Literal["1w", "1mo"]


@dataclass(frozen=True)
class PeriodCandle:
    """一根按日 K 聚合的周线或月线。"""

    period_start: date
    period_end: date
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
class PeriodKlinePage:
    """带交易日游标的周线或月线页。"""

    candles: tuple[PeriodCandle, ...]
    next_cursor: date | None
    base_date: date | None
    base_factor_date: date | None


@dataclass(frozen=True)
class _CacheEntry:
    value: PeriodKlinePage
    expires_at: float


class PeriodKlineCache:
    """有界 TTL LRU，缓存不可变的周期 K 线查询结果。"""

    def __init__(self, *, ttl_seconds: float, max_entries: int):
        self._ttl_seconds = ttl_seconds
        self._max_entries = max_entries
        self._entries: OrderedDict[tuple[object, ...], _CacheEntry] = OrderedDict()
        self._lock = threading.Lock()

    def get(self, key: tuple[object, ...]) -> PeriodKlinePage | None:
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

    def put(self, key: tuple[object, ...], value: PeriodKlinePage) -> None:
        if self._ttl_seconds == 0:
            return
        with self._lock:
            self._entries[key] = _CacheEntry(value, time.monotonic() + self._ttl_seconds)
            self._entries.move_to_end(key)
            while len(self._entries) > self._max_entries:
                self._entries.popitem(last=False)


class PeriodKlineRepository:
    """只依赖 daily_bars 与 adj_factors 的周线、月线 DuckDB 查询器。"""

    def __init__(self, settings: ProxySettings, *, interval: PeriodInterval):
        self._settings = settings
        self._interval = interval
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
    ) -> PeriodKlinePage:
        effective_start = max(start, cursor + timedelta(days=1)) if cursor else start
        if effective_start > end:
            return PeriodKlinePage((), None, base_date if adjustment == "qfq" else None, None)

        for attempt in range(2):
            try:
                bar_files = self._bars_partitions.files_for(effective_start, end)
            except FileNotFoundError as exc:
                raise LakeUnavailable(str(exc)) from exc
            if not bar_files:
                return PeriodKlinePage((), None, base_date if adjustment == "qfq" else None, None)

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
                    # 因子尚未生成时保留原价，并在每根周期 K 上明确标记为不精确。
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
        adjustment: Adjustment,
        strict_adjustment: bool,
        base_date: date | None,
    ) -> PeriodKlinePage:
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
            PeriodCandle(
                period_start=row[0],
                period_end=row[1],
                trade_date=row[2],
                open=float(row[3]),
                high=float(row[4]),
                low=float(row[5]),
                close=float(row[6]),
                volume=int(row[7]) if row[7] is not None else None,
                amount=float(row[8]) if row[8] is not None else None,
                adjustment_factor=float(row[9]) if row[9] is not None else None,
                adjustment_exact=bool(row[10]) if row[10] is not None else None,
            )
            for row in page_rows
        )
        base_factor_date = page_rows[0][11] if adjustment == "qfq" and page_rows else None
        if strict_adjustment and adjustment != "none":
            if adjustment == "qfq" and candles and base_factor_date != base_date:
                raise AdjustmentCoverageError(
                    f"qfq 基准日 {base_date} 缺少后复权因子；"
                    "可传 strict_adjustment=false 并检查 adjustment_exact"
                )
            inexact_dates = [
                candle.trade_date.isoformat()
                for candle in candles
                if candle.adjustment_exact is not True
            ]
            if inexact_dates:
                raise AdjustmentCoverageError(
                    f"{adjustment} 缺少复权因子覆盖: {', '.join(inexact_dates[:3])}；"
                    "可传 strict_adjustment=false 并检查 adjustment_exact"
                )
        return PeriodKlinePage(
            candles,
            candles[-1].trade_date if has_more else None,
            base_date if adjustment == "qfq" else None,
            base_factor_date,
        )

    @staticmethod
    def _paths(files: list[Path]) -> list[str]:
        return [str(path) for path in files]

    def _period_expressions(self) -> tuple[str, str]:
        if self._interval == "1w":
            return (
                "date_trunc('week', trade_date)::DATE",
                "(date_trunc('week', trade_date)::DATE + 6)",
            )
        return (
            "date_trunc('month', trade_date)::DATE",
            "(date_trunc('month', trade_date) + INTERVAL '1 month' - INTERVAL '1 day')::DATE",
        )

    def _aggregate_rows(
        self,
        connection: duckdb.DuckDBPyConnection,
        *,
        supporting_ctes: str | None,
        adjusted_rows: str,
        parameters: list[object],
        limit: int,
    ) -> list[tuple]:
        period_start, period_end = self._period_expressions()
        cte_prefix = f"{supporting_ctes}," if supporting_ctes else ""
        return connection.execute(
            f"""
            WITH {cte_prefix}
            adjusted_bars AS MATERIALIZED (
                {adjusted_rows}
            ),
            periods AS (
                SELECT {period_start} AS period_start,
                       {period_end} AS period_end,
                       max(trade_date) AS trade_date,
                       arg_min(open, trade_date) AS open,
                       max(high) AS high,
                       min(low) AS low,
                       arg_max(close, trade_date) AS close,
                       sum(volume) AS volume,
                       sum(amount) AS amount,
                       arg_max(adjustment_factor, trade_date) AS adjustment_factor,
                       bool_and(adjustment_exact) AS adjustment_exact,
                       max(base_factor_date) AS base_factor_date
                FROM adjusted_bars
                GROUP BY 1, 2
            )
            SELECT period_start, period_end, trade_date, open, high, low, close, volume, amount,
                   adjustment_factor, adjustment_exact, base_factor_date
            FROM periods
            ORDER BY trade_date ASC
            LIMIT ?
            """,
            [*parameters, limit + 1],
        ).fetchall()

    def _raw_rows(
        self,
        connection: duckdb.DuckDBPyConnection,
        files: list[Path],
        symbol: str,
        start: date,
        end: date,
        limit: int,
    ) -> list[tuple]:
        return self._aggregate_rows(
            connection,
            supporting_ctes=None,
            adjusted_rows="""
                SELECT trade_date, open, high, low, close, volume, amount,
                       NULL::DOUBLE AS adjustment_factor,
                       NULL::BOOLEAN AS adjustment_exact,
                       NULL::DATE AS base_factor_date
                FROM read_parquet(?, union_by_name = true)
                WHERE symbol = ? AND trade_date >= ? AND trade_date <= ?
            """,
            parameters=[self._paths(files), symbol, start, end],
            limit=limit,
        )

    def _inexact_rows(
        self,
        connection: duckdb.DuckDBPyConnection,
        files: list[Path],
        symbol: str,
        start: date,
        end: date,
        limit: int,
    ) -> list[tuple]:
        return self._aggregate_rows(
            connection,
            supporting_ctes=None,
            adjusted_rows="""
                SELECT trade_date, open, high, low, close, volume, amount,
                       1.0::DOUBLE AS adjustment_factor,
                       false AS adjustment_exact,
                       NULL::DATE AS base_factor_date
                FROM read_parquet(?, union_by_name = true)
                WHERE symbol = ? AND trade_date >= ? AND trade_date <= ?
            """,
            parameters=[self._paths(files), symbol, start, end],
            limit=limit,
        )

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
        return self._aggregate_rows(
            connection,
            supporting_ctes="""
                factors AS MATERIALIZED (
                    SELECT trade_date, factor
                    FROM read_parquet(?, union_by_name = true)
                    WHERE symbol = ? AND adjust_type = 'hfq'
                      AND trade_date >= ? AND trade_date <= ?
                      AND factor IS NOT NULL AND factor > 0 AND isfinite(factor)
                )
            """,
            adjusted_rows="""
                SELECT b.trade_date,
                       b.open * COALESCE(f.factor, 1.0) AS open,
                       b.high * COALESCE(f.factor, 1.0) AS high,
                       b.low * COALESCE(f.factor, 1.0) AS low,
                       b.close * COALESCE(f.factor, 1.0) AS close,
                       b.volume, b.amount, COALESCE(f.factor, 1.0) AS adjustment_factor,
                       f.factor IS NOT NULL AS adjustment_exact,
                       NULL::DATE AS base_factor_date
                FROM read_parquet(?, union_by_name = true) b
                LEFT JOIN factors f ON b.trade_date = f.trade_date
                WHERE b.symbol = ? AND b.trade_date >= ? AND b.trade_date <= ?
            """,
            parameters=[
                self._paths(factor_files),
                symbol,
                start,
                end,
                self._paths(bar_files),
                symbol,
                start,
                end,
            ],
            limit=limit,
        )

    def _qfq_rows(
        self,
        connection: duckdb.DuckDBPyConnection,
        bar_files: list[Path],
        factor_files: list[Path],
        symbol: str,
        start: date,
        end: date,
        limit: int,
        base_date: date | None,
    ) -> list[tuple]:
        assert base_date is not None
        return self._aggregate_rows(
            connection,
            supporting_ctes="""
                factors AS MATERIALIZED (
                    SELECT trade_date, factor
                    FROM read_parquet(?, union_by_name = true)
                    WHERE symbol = ? AND adjust_type = 'hfq'
                      AND ((trade_date >= ? AND trade_date <= ?) OR trade_date = ?)
                      AND factor IS NOT NULL AND factor > 0 AND isfinite(factor)
                ),
                base_factor AS (
                    SELECT trade_date, factor
                    FROM factors
                    WHERE trade_date = ?
                    LIMIT 1
                )
            """,
            adjusted_rows="""
                SELECT b.trade_date,
                       b.open * COALESCE(f.factor / a.factor, 1.0) AS open,
                       b.high * COALESCE(f.factor / a.factor, 1.0) AS high,
                       b.low * COALESCE(f.factor / a.factor, 1.0) AS low,
                       b.close * COALESCE(f.factor / a.factor, 1.0) AS close,
                       b.volume, b.amount,
                       COALESCE(f.factor / a.factor, 1.0) AS adjustment_factor,
                       f.factor IS NOT NULL AND a.factor IS NOT NULL AS adjustment_exact,
                       a.trade_date AS base_factor_date
                FROM read_parquet(?, union_by_name = true) b
                LEFT JOIN factors f ON b.trade_date = f.trade_date
                LEFT JOIN base_factor a ON true
                WHERE b.symbol = ? AND b.trade_date >= ? AND b.trade_date <= ?
            """,
            parameters=[
                self._paths(factor_files),
                symbol,
                start,
                end,
                base_date,
                base_date,
                self._paths(bar_files),
                symbol,
                start,
                end,
            ],
            limit=limit,
        )


class PeriodKlineService:
    """周期 K 请求缓存与磁盘扫描并发上限。"""

    def __init__(
        self,
        settings: ProxySettings,
        *,
        interval: PeriodInterval,
        slots: threading.BoundedSemaphore | None = None,
    ):
        self._interval = interval
        self._repository = PeriodKlineRepository(settings, interval=interval)
        self._cache = PeriodKlineCache(
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
    ) -> tuple[PeriodKlinePage, bool]:
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
