"""单只证券的轻量当前摘要查询。"""

from __future__ import annotations

import logging
import threading
import time
from collections import OrderedDict
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path

import duckdb

from cnequity_query_proxy.kline import LakeUnavailable, QueryBusy, QueryFailed, QueryTooWide
from cnequity_query_proxy.partitions import PartitionIndex
from cnequity_query_proxy.settings import ProxySettings

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Provenance:
    """一行已落盘事实的来源与采集时间。"""

    source: str
    data_version: str
    fetched_at: datetime | None


@dataclass(frozen=True)
class SummaryInstrument:
    symbol: str
    name: str
    exchange: str
    asset_type: str
    list_date: date | None
    delist_date: date | None
    prev_symbol: str | None
    provenance: Provenance


@dataclass(frozen=True)
class SummaryDailyBar:
    trade_date: date
    open: float
    high: float
    low: float
    close: float
    volume: int | None
    amount: float | None
    provenance: Provenance


@dataclass(frozen=True)
class SummaryTradingStatus:
    trade_date: date
    is_trading: bool
    status: str
    risk_warning: bool | None
    provenance: Provenance


@dataclass(frozen=True)
class SummaryValuation:
    trade_date: date
    pe_ttm: float | None
    pb: float | None
    ps_ttm: float | None
    total_mv: float | None
    float_mv: float | None
    provenance: Provenance


@dataclass(frozen=True)
class SummaryIndustry:
    classification_system: str
    industry_code: str
    industry_name: str
    as_of_date: date
    provenance: Provenance


@dataclass(frozen=True)
class SummarySector:
    sector_code: str
    sector_name: str
    as_of_date: date
    provenance: Provenance


@dataclass(frozen=True)
class SummaryIndexMembership:
    index_symbol: str
    as_of_date: date
    weight: float | None
    provenance: Provenance


@dataclass(frozen=True)
class SummaryFundFlow:
    trade_date: date
    main_net_inflow: float | None
    super_large_net_inflow: float | None
    large_net_inflow: float | None
    medium_net_inflow: float | None
    small_net_inflow: float | None
    provenance: Provenance


@dataclass(frozen=True)
class SummaryAnalystConsensus:
    forecast_date: date
    forecast_year: int | None
    eps_forecast: float | None
    pe_forecast: float | None
    target_price: float | None
    rating: str | None
    analyst_count: int | None
    provenance: Provenance


@dataclass(frozen=True)
class SummaryHotRank:
    trade_date: date
    rank: int | None
    rank_change: int | None
    hist_rank: int | None
    provenance: Provenance


@dataclass(frozen=True)
class SummarySentiment:
    trade_date: date
    score_channel: str
    sentiment_score: float | None
    headline_count: int | None
    provenance: Provenance


@dataclass(frozen=True)
class StockSummary:
    """一只证券在各轻量数据集中的最新可用事实。"""

    symbol: str
    instrument: SummaryInstrument
    daily_bar: SummaryDailyBar | None
    trading_status: SummaryTradingStatus | None
    valuation: SummaryValuation | None
    industries: tuple[SummaryIndustry, ...]
    sectors: tuple[SummarySector, ...]
    index_memberships: tuple[SummaryIndexMembership, ...]
    fund_flow: SummaryFundFlow | None
    analyst_consensus: SummaryAnalystConsensus | None
    hot_rank: SummaryHotRank | None
    sentiments: tuple[SummarySentiment, ...]
    unavailable_datasets: tuple[str, ...]


class InstrumentNotFound(LookupError):
    """证券主数据不存在该代码。"""


@dataclass(frozen=True)
class _CacheEntry:
    value: StockSummary
    expires_at: float


class SummaryCache:
    """有界 TTL LRU，缓存不可变的单股摘要。"""

    def __init__(self, *, ttl_seconds: float, max_entries: int):
        self._ttl_seconds = ttl_seconds
        self._max_entries = max_entries
        self._entries: OrderedDict[str, _CacheEntry] = OrderedDict()
        self._lock = threading.Lock()

    def get(self, symbol: str) -> StockSummary | None:
        if self._ttl_seconds == 0:
            return None
        now = time.monotonic()
        with self._lock:
            entry = self._entries.get(symbol)
            if entry is None:
                return None
            if entry.expires_at <= now:
                del self._entries[symbol]
                return None
            self._entries.move_to_end(symbol)
            return entry.value

    def put(self, symbol: str, value: StockSummary) -> None:
        if self._ttl_seconds == 0:
            return
        with self._lock:
            self._entries[symbol] = _CacheEntry(value, time.monotonic() + self._ttl_seconds)
            self._entries.move_to_end(symbol)
            while len(self._entries) > self._max_entries:
                self._entries.popitem(last=False)


class StockSummaryRepository:
    """仅读取摘要白名单中的最新 Parquet 分区。"""

    _DATASET_ORDER = (
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
    )

    def __init__(self, settings: ProxySettings):
        self._settings = settings
        ttl = settings.cache_ttl_seconds
        self._partitions = {
            "daily_bars": PartitionIndex(
                settings.daily_bars_root, ttl_seconds=ttl, dataset_name="daily_bars"
            ),
            "trading_status": PartitionIndex(
                settings.trading_status_root, ttl_seconds=ttl, dataset_name="trading_status"
            ),
            "valuation_metrics": PartitionIndex(
                settings.valuation_metrics_root, ttl_seconds=ttl, dataset_name="valuation_metrics"
            ),
            "industry_members": PartitionIndex(
                settings.industry_members_root,
                ttl_seconds=ttl,
                dataset_name="industry_members",
                partition_key="as_of_date",
            ),
            "sector_members": PartitionIndex(
                settings.sector_members_root,
                ttl_seconds=ttl,
                dataset_name="sector_members",
                partition_key="as_of_date",
            ),
            "index_constituents": PartitionIndex(
                settings.index_constituents_root,
                ttl_seconds=ttl,
                dataset_name="index_constituents",
                partition_key="as_of_date",
            ),
            "fund_flow": PartitionIndex(
                settings.fund_flow_root, ttl_seconds=ttl, dataset_name="fund_flow"
            ),
            "analyst_consensus": PartitionIndex(
                settings.analyst_consensus_root,
                ttl_seconds=ttl,
                dataset_name="analyst_consensus",
                partition_key="forecast_date",
            ),
            "hot_rank": PartitionIndex(
                settings.hot_rank_root, ttl_seconds=ttl, dataset_name="hot_rank"
            ),
            "sentiment_scores": PartitionIndex(
                settings.sentiment_scores_root, ttl_seconds=ttl, dataset_name="sentiment_scores"
            ),
        }

    def fetch(self, *, symbol: str) -> StockSummary:
        for attempt in range(2):
            try:
                return self._fetch_once(symbol=symbol)
            except duckdb.Error as exc:
                if attempt == 0:
                    logger.info("股票摘要文件在读取中变化，刷新分区索引后重试: %s", exc)
                    for index in self._partitions.values():
                        index.refresh()
                    continue
                logger.warning("股票摘要查询失败: %s", exc)
                raise QueryFailed("股票摘要暂时不可读，请稍后重试") from exc

        raise QueryFailed("股票摘要暂时不可读，请稍后重试")  # pragma: no cover

    def _fetch_once(self, *, symbol: str) -> StockSummary:
        instrument_path = self._settings.instruments_file
        if not instrument_path.is_file():
            raise LakeUnavailable(f"instruments 主文件不存在: {instrument_path}")

        files_by_dataset: dict[str, list[Path]] = {}
        unavailable: list[str] = []
        for dataset in self._DATASET_ORDER:
            try:
                files = self._partitions[dataset].latest_files()
            except FileNotFoundError:
                unavailable.append(dataset)
                continue
            if not files:
                unavailable.append(dataset)
                continue
            files_by_dataset[dataset] = files

        file_count = len({path for files in files_by_dataset.values() for path in files})
        if file_count > self._settings.max_files:
            raise QueryTooWide(
                f"股票摘要需要打开 {file_count} 个 Parquet 文件，超过上限 "
                f"{self._settings.max_files}；请提高代理配置上限"
            )

        connection = duckdb.connect(
            database=":memory:",
            config={"threads": str(self._settings.duckdb_threads)},
        )
        try:
            instrument = self._instrument(connection, instrument_path, symbol)
            if instrument is None:
                raise InstrumentNotFound(symbol)

            return StockSummary(
                symbol=symbol,
                instrument=instrument,
                daily_bar=self._daily_bar(connection, files_by_dataset.get("daily_bars"), symbol),
                trading_status=self._trading_status(
                    connection, files_by_dataset.get("trading_status"), symbol
                ),
                valuation=self._valuation(
                    connection, files_by_dataset.get("valuation_metrics"), symbol
                ),
                industries=self._industries(
                    connection, files_by_dataset.get("industry_members"), symbol
                ),
                sectors=self._sectors(connection, files_by_dataset.get("sector_members"), symbol),
                index_memberships=self._index_memberships(
                    connection, files_by_dataset.get("index_constituents"), symbol
                ),
                fund_flow=self._fund_flow(connection, files_by_dataset.get("fund_flow"), symbol),
                analyst_consensus=self._analyst_consensus(
                    connection, files_by_dataset.get("analyst_consensus"), symbol
                ),
                hot_rank=self._hot_rank(connection, files_by_dataset.get("hot_rank"), symbol),
                sentiments=self._sentiments(
                    connection, files_by_dataset.get("sentiment_scores"), symbol
                ),
                unavailable_datasets=tuple(unavailable),
            )
        finally:
            connection.close()

    @staticmethod
    def _paths(files: list[Path] | None) -> list[str] | None:
        return [str(path) for path in files] if files else None

    @staticmethod
    def _provenance(row: tuple, *, offset: int) -> Provenance:
        raw_fetched_at = row[offset + 2]
        if isinstance(raw_fetched_at, datetime) or raw_fetched_at is None:
            fetched_at = raw_fetched_at
        else:
            text = str(raw_fetched_at)
            if text.endswith("+00"):
                text = f"{text}:00"
            fetched_at = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return Provenance(
            source=str(row[offset]),
            data_version=str(row[offset + 1]),
            fetched_at=fetched_at,
        )

    def _instrument(
        self,
        connection: duckdb.DuckDBPyConnection,
        path: Path,
        symbol: str,
    ) -> SummaryInstrument | None:
        row = connection.execute(
            """
            SELECT symbol, name, exchange, asset_type, list_date, delist_date, prev_symbol,
                   source, data_version,
                   strftime(fetched_at AT TIME ZONE 'UTC', '%Y-%m-%dT%H:%M:%SZ') AS fetched_at
            FROM read_parquet(?)
            WHERE symbol = ?
            LIMIT 1
            """,
            [str(path), symbol],
        ).fetchone()
        if row is None:
            return None
        return SummaryInstrument(
            symbol=str(row[0]),
            name=str(row[1]),
            exchange=str(row[2]),
            asset_type=str(row[3]),
            list_date=row[4],
            delist_date=row[5],
            prev_symbol=row[6],
            provenance=self._provenance(row, offset=7),
        )

    def _daily_bar(
        self,
        connection: duckdb.DuckDBPyConnection,
        files: list[Path] | None,
        symbol: str,
    ) -> SummaryDailyBar | None:
        paths = self._paths(files)
        if paths is None:
            return None
        row = connection.execute(
            """
            SELECT trade_date, open, high, low, close, volume, amount,
                   source, data_version,
                   strftime(fetched_at AT TIME ZONE 'UTC', '%Y-%m-%dT%H:%M:%SZ') AS fetched_at
            FROM read_parquet(?, union_by_name = true, hive_partitioning = false)
            WHERE symbol = ?
            ORDER BY trade_date DESC, fetched_at DESC NULLS LAST
            LIMIT 1
            """,
            [paths, symbol],
        ).fetchone()
        if row is None:
            return None
        return SummaryDailyBar(
            trade_date=row[0],
            open=float(row[1]),
            high=float(row[2]),
            low=float(row[3]),
            close=float(row[4]),
            volume=int(row[5]) if row[5] is not None else None,
            amount=float(row[6]) if row[6] is not None else None,
            provenance=self._provenance(row, offset=7),
        )

    def _trading_status(
        self,
        connection: duckdb.DuckDBPyConnection,
        files: list[Path] | None,
        symbol: str,
    ) -> SummaryTradingStatus | None:
        paths = self._paths(files)
        if paths is None:
            return None
        row = connection.execute(
            """
            SELECT trade_date, is_trading, status, risk_warning, source, data_version,
                   strftime(fetched_at AT TIME ZONE 'UTC', '%Y-%m-%dT%H:%M:%SZ') AS fetched_at
            FROM read_parquet(?, union_by_name = true, hive_partitioning = false)
            WHERE symbol = ?
            ORDER BY trade_date DESC, fetched_at DESC NULLS LAST
            LIMIT 1
            """,
            [paths, symbol],
        ).fetchone()
        if row is None:
            return None
        return SummaryTradingStatus(
            trade_date=row[0],
            is_trading=bool(row[1]),
            status=str(row[2]),
            risk_warning=bool(row[3]) if row[3] is not None else None,
            provenance=self._provenance(row, offset=4),
        )

    def _valuation(
        self,
        connection: duckdb.DuckDBPyConnection,
        files: list[Path] | None,
        symbol: str,
    ) -> SummaryValuation | None:
        paths = self._paths(files)
        if paths is None:
            return None
        row = connection.execute(
            """
            SELECT trade_date, pe_ttm, pb, ps_ttm, total_mv, float_mv,
                   source, data_version,
                   strftime(fetched_at AT TIME ZONE 'UTC', '%Y-%m-%dT%H:%M:%SZ') AS fetched_at
            FROM read_parquet(?, union_by_name = true, hive_partitioning = false)
            WHERE symbol = ?
            ORDER BY trade_date DESC, fetched_at DESC NULLS LAST
            LIMIT 1
            """,
            [paths, symbol],
        ).fetchone()
        if row is None:
            return None
        return SummaryValuation(
            trade_date=row[0],
            pe_ttm=float(row[1]) if row[1] is not None else None,
            pb=float(row[2]) if row[2] is not None else None,
            ps_ttm=float(row[3]) if row[3] is not None else None,
            total_mv=float(row[4]) if row[4] is not None else None,
            float_mv=float(row[5]) if row[5] is not None else None,
            provenance=self._provenance(row, offset=6),
        )

    def _industries(
        self,
        connection: duckdb.DuckDBPyConnection,
        files: list[Path] | None,
        symbol: str,
    ) -> tuple[SummaryIndustry, ...]:
        paths = self._paths(files)
        if paths is None:
            return ()
        rows = self._latest_snapshot_rows(
            connection,
            paths,
            "as_of_date, classification_system, industry_code, industry_name, "
            "source, data_version, "
            "strftime(fetched_at AT TIME ZONE 'UTC', '%Y-%m-%dT%H:%M:%SZ') AS fetched_at",
            "as_of_date",
            symbol,
            "classification_system ASC, industry_code ASC",
        )
        return tuple(
            SummaryIndustry(
                as_of_date=row[0],
                classification_system=str(row[1]),
                industry_code=str(row[2]),
                industry_name=str(row[3]),
                provenance=self._provenance(row, offset=4),
            )
            for row in rows
        )

    def _sectors(
        self,
        connection: duckdb.DuckDBPyConnection,
        files: list[Path] | None,
        symbol: str,
    ) -> tuple[SummarySector, ...]:
        paths = self._paths(files)
        if paths is None:
            return ()
        rows = self._latest_snapshot_rows(
            connection,
            paths,
            "as_of_date, sector_code, sector_name, source, data_version, "
            "strftime(fetched_at AT TIME ZONE 'UTC', '%Y-%m-%dT%H:%M:%SZ') AS fetched_at",
            "as_of_date",
            symbol,
            "sector_code ASC",
        )
        return tuple(
            SummarySector(
                as_of_date=row[0],
                sector_code=str(row[1]),
                sector_name=str(row[2]),
                provenance=self._provenance(row, offset=3),
            )
            for row in rows
        )

    def _index_memberships(
        self,
        connection: duckdb.DuckDBPyConnection,
        files: list[Path] | None,
        symbol: str,
    ) -> tuple[SummaryIndexMembership, ...]:
        paths = self._paths(files)
        if paths is None:
            return ()
        rows = self._latest_snapshot_rows(
            connection,
            paths,
            "as_of_date, index_symbol, weight, source, data_version, "
            "strftime(fetched_at AT TIME ZONE 'UTC', '%Y-%m-%dT%H:%M:%SZ') AS fetched_at",
            "as_of_date",
            symbol,
            "index_symbol ASC",
        )
        return tuple(
            SummaryIndexMembership(
                as_of_date=row[0],
                index_symbol=str(row[1]),
                # 现有两个入湖源都没有成分权重，0 是适配器占位，不能误传为零权重。
                weight=float(row[2]) if row[2] not in (None, 0.0) else None,
                provenance=self._provenance(row, offset=3),
            )
            for row in rows
        )

    def _fund_flow(
        self,
        connection: duckdb.DuckDBPyConnection,
        files: list[Path] | None,
        symbol: str,
    ) -> SummaryFundFlow | None:
        paths = self._paths(files)
        if paths is None:
            return None
        row = connection.execute(
            """
            SELECT trade_date, main_net_inflow, super_large_net_inflow, large_net_inflow,
                   medium_net_inflow, small_net_inflow, source, data_version,
                   strftime(fetched_at AT TIME ZONE 'UTC', '%Y-%m-%dT%H:%M:%SZ') AS fetched_at
            FROM read_parquet(?, union_by_name = true, hive_partitioning = false)
            WHERE symbol = ?
            ORDER BY trade_date DESC, fetched_at DESC NULLS LAST
            LIMIT 1
            """,
            [paths, symbol],
        ).fetchone()
        if row is None:
            return None
        return SummaryFundFlow(
            trade_date=row[0],
            main_net_inflow=float(row[1]) if row[1] is not None else None,
            super_large_net_inflow=float(row[2]) if row[2] is not None else None,
            large_net_inflow=float(row[3]) if row[3] is not None else None,
            medium_net_inflow=float(row[4]) if row[4] is not None else None,
            small_net_inflow=float(row[5]) if row[5] is not None else None,
            provenance=self._provenance(row, offset=6),
        )

    def _analyst_consensus(
        self,
        connection: duckdb.DuckDBPyConnection,
        files: list[Path] | None,
        symbol: str,
    ) -> SummaryAnalystConsensus | None:
        paths = self._paths(files)
        if paths is None:
            return None
        row = connection.execute(
            """
            SELECT forecast_date, forecast_year, eps_forecast, pe_forecast, target_price, rating,
                   analyst_count, source, data_version,
                   strftime(fetched_at AT TIME ZONE 'UTC', '%Y-%m-%dT%H:%M:%SZ') AS fetched_at
            FROM read_parquet(?, union_by_name = true, hive_partitioning = false)
            WHERE symbol = ?
            ORDER BY forecast_date DESC, fetched_at DESC NULLS LAST
            LIMIT 1
            """,
            [paths, symbol],
        ).fetchone()
        if row is None:
            return None
        return SummaryAnalystConsensus(
            forecast_date=row[0],
            forecast_year=int(row[1]) if row[1] is not None else None,
            eps_forecast=float(row[2]) if row[2] is not None else None,
            pe_forecast=float(row[3]) if row[3] is not None else None,
            target_price=float(row[4]) if row[4] is not None else None,
            rating=str(row[5]) if row[5] is not None else None,
            analyst_count=int(row[6]) if row[6] is not None else None,
            provenance=self._provenance(row, offset=7),
        )

    def _hot_rank(
        self,
        connection: duckdb.DuckDBPyConnection,
        files: list[Path] | None,
        symbol: str,
    ) -> SummaryHotRank | None:
        paths = self._paths(files)
        if paths is None:
            return None
        rows = self._latest_snapshot_rows(
            connection,
            paths,
            "trade_date, rank, rank_change, hist_rank, source, data_version, "
            "strftime(fetched_at AT TIME ZONE 'UTC', '%Y-%m-%dT%H:%M:%SZ') AS fetched_at",
            "trade_date",
            symbol,
            "rank ASC NULLS LAST",
        )
        if not rows:
            return None
        row = rows[0]
        return SummaryHotRank(
            trade_date=row[0],
            rank=int(row[1]) if row[1] is not None else None,
            rank_change=int(row[2]) if row[2] is not None else None,
            hist_rank=int(row[3]) if row[3] is not None else None,
            provenance=self._provenance(row, offset=4),
        )

    def _sentiments(
        self,
        connection: duckdb.DuckDBPyConnection,
        files: list[Path] | None,
        symbol: str,
    ) -> tuple[SummarySentiment, ...]:
        paths = self._paths(files)
        if paths is None:
            return ()
        rows = self._latest_snapshot_rows(
            connection,
            paths,
            "trade_date, score_channel, sentiment_score, headline_count, "
            "source, data_version, "
            "strftime(fetched_at AT TIME ZONE 'UTC', '%Y-%m-%dT%H:%M:%SZ') AS fetched_at",
            "trade_date",
            symbol,
            "score_channel ASC",
        )
        return tuple(
            SummarySentiment(
                trade_date=row[0],
                score_channel=str(row[1]),
                sentiment_score=float(row[2]) if row[2] is not None else None,
                headline_count=int(row[3]) if row[3] is not None else None,
                provenance=self._provenance(row, offset=4),
            )
            for row in rows
        )

    @staticmethod
    def _latest_snapshot_rows(
        connection: duckdb.DuckDBPyConnection,
        paths: list[str],
        columns: str,
        date_column: str,
        symbol: str,
        order_by: str,
    ) -> list[tuple]:
        return connection.execute(
            f"""
            WITH latest AS (
                SELECT max({date_column}) AS value
                FROM read_parquet(?, union_by_name = true, hive_partitioning = false)
            )
            SELECT {columns}
            FROM read_parquet(?, union_by_name = true, hive_partitioning = false)
            WHERE symbol = ?
              AND {date_column} = (SELECT value FROM latest)
            ORDER BY {order_by}
            """,
            [paths, paths, symbol],
        ).fetchall()


class StockSummaryService:
    """股票摘要的缓存与共享磁盘查询额度。"""

    def __init__(
        self,
        settings: ProxySettings,
        *,
        slots: threading.BoundedSemaphore | None = None,
    ):
        self._repository = StockSummaryRepository(settings)
        self._cache = SummaryCache(
            ttl_seconds=settings.cache_ttl_seconds,
            max_entries=settings.cache_entries,
        )
        self._slots = slots or threading.BoundedSemaphore(settings.max_concurrent_queries)

    def query(self, *, symbol: str) -> tuple[StockSummary, bool]:
        cached = self._cache.get(symbol)
        if cached is not None:
            return cached, True
        if not self._slots.acquire(blocking=False):
            raise QueryBusy("查询繁忙，请稍后重试")
        try:
            cached = self._cache.get(symbol)
            if cached is not None:
                return cached, True
            summary = self._repository.fetch(symbol=symbol)
            self._cache.put(symbol, summary)
            return summary, False
        finally:
            self._slots.release()
