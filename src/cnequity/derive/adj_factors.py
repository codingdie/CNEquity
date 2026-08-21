from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from pathlib import Path

import httpx
import polars as pl

from cnequity.adapters.sina.adj_factors import (
    SinaAdjFactorUnavailableError,
    fetch_adj_factor_series,
)
from cnequity.config import Config
from cnequity.domain.canonical import dedupe_lazy_by_primary_key
from cnequity.domain.rate_limit import source_request
from cnequity.domain.schemas import with_provenance
from cnequity.domain.symbols import is_cdr_symbol, parse_symbol
from cnequity.file_lock import lake_mutation_lock
from cnequity.storage.atomic import write_parquet_atomic
from cnequity.storage.parquet import CuratedWriter
from cnequity.storage.state import StateStore

logger = logging.getLogger(__name__)

# Only hfq is persisted; qfq is derived at query time (ADR-0004).
STORED_ADJUST_TYPE = "hfq"

# Derive step fails when uncached fetch failures exceed this share of symbol×type tasks.
FAIL_RATIO_THRESHOLD = 0.05

# A single trading day's hfq factor step beyond this ratio cannot come from any real
# corporate action — even the largest historical splits step the factor by ~10x. Such a
# jump signals a factor break (a dropped/misaligned event date or a corrupt source series),
# so we surface it as a fail-loud finding. This is a coarse tripwire for gross corruption;
# it deliberately does not try to catch ~2x breaks, which are indistinguishable from a real
# 10-for-10 bonus at the factor level alone and are guarded downstream via raw-vs-adjusted
# return divergence.
MAX_FACTOR_STEP_RATIO = 20.0

# Calendar days of already written factor partitions the cross-check reads back
# to give an append-only run a left edge for its first step. A month covers an
# ordinary suspension; a longer gap simply leaves that symbol's first new day
# unchecked until its next step.
CROSSCHECK_PRIOR_LOOKBACK_DAYS = 35

# Symbols whose missing history one incremental run will realign. A daily run
# normally finds none; this bounds the first run after a deep `cne backfill
# daily_bars`, which would otherwise realign the whole market in one go.
UNCOVERED_REFRESH_LIMIT = 500

# A daily-bars gap-fill can insert rows into a partition behind the derived
# watermark (for example a 2026-08-19 row landed after the 2026-08-20 derive).
# Reconcile a short recent window each run so those late rows do not stay at
# factor=1.0 with adj_is_exact=false indefinitely.
RECENT_FACTOR_RECONCILE_DAYS = 3
_EMPTY_BAR_DATES = pl.DataFrame(schema={"symbol": pl.Utf8, "trade_date": pl.Date})
_ADJ_PK = ["symbol", "trade_date", "adjust_type"]
_RETRY_STATE_FIELD = "retry_symbols"
_UNAVAILABLE_STATE_FIELD = "source_unavailable_symbols"


class AdjFactorsFetchError(RuntimeError):
    """Raised when adj factor fetch fails and no cache is available."""


class AdjFactorsSourceUnavailableError(AdjFactorsFetchError):
    """Raised when the configured source explicitly has no series for a symbol."""


class AdjFactorsDeriveError(RuntimeError):
    """Raised when too many symbols lack adj factors after derive."""

    def __init__(self, message: str, *, findings: list[dict]):
        super().__init__(message)
        self.findings = findings


class AdjFactorsResult:
    __slots__ = ("rows", "task_count", "failed", "findings")

    def __init__(
        self,
        rows: int,
        task_count: int,
        failed: list[str],
        findings: list[dict],
    ) -> None:
        self.rows = rows
        self.task_count = task_count
        self.failed = failed
        self.findings = findings

    @property
    def fail_ratio(self) -> float:
        if not self.task_count:
            return 0.0
        return len(self.failed) / self.task_count


def _is_cdr(symbol: str) -> bool:
    try:
        info = parse_symbol(symbol)
    except ValueError:
        return False
    return is_cdr_symbol(info.code, info.exchange)


def _adj_factors_watermark(config: Config) -> date | None:
    """Latest trade_date partition already present under derived/adj_factors."""
    # Lazy import: query.reader imports STORED_ADJUST_TYPE from this module.
    from cnequity.query.parquet_scan import list_hive_partition_dates

    dates = list_hive_partition_dates(config.derived_root / "adj_factors", "trade_date")
    return dates[-1] if dates else None


def _retry_symbols(config: Config) -> set[str]:
    """Symbols whose uncached fetch failed on a prior derive run."""
    return StateStore(config.meta_root).get_string_set("adj_factors", _RETRY_STATE_FIELD)


def _source_unavailable_symbols(config: Config) -> set[str]:
    """Symbols with an explicit, permanent source-unavailable classification."""
    return StateStore(config.meta_root).get_string_set("adj_factors", _UNAVAILABLE_STATE_FIELD)


def _update_retry_symbols(
    config: Config,
    *,
    previous: set[str],
    succeeded: set[str],
    failed: set[str],
) -> None:
    """Keep failed symbols retryable until a run writes aligned factor rows.

    The derived date watermark is global, while fetches run per symbol. A
    transient failure can therefore be hidden behind a newer partition unless
    the symbol-level retry intent is persisted separately.
    """
    remaining = (previous - succeeded) | failed
    StateStore(config.meta_root).set_string_set(
        "adj_factors",
        _RETRY_STATE_FIELD,
        remaining,
    )


def _update_source_unavailable_symbols(
    config: Config,
    *,
    previous: set[str],
    succeeded: set[str],
    newly_unavailable: set[str],
) -> None:
    """Persist explicit source gaps without turning them into retry storms."""
    remaining = (previous - succeeded) | newly_unavailable
    StateStore(config.meta_root).set_string_set(
        "adj_factors",
        _UNAVAILABLE_STATE_FIELD,
        remaining,
    )


def _load_daily_bar_dates(
    config: Config,
    *,
    start: date | None = None,
    symbols: list[str] | None = None,
) -> pl.DataFrame:
    """Load symbol×trade_date pairs from daily_bars (lazy hive scan when possible)."""
    # Lazy import: query.reader imports STORED_ADJUST_TYPE from this module.
    from cnequity.query.parquet_scan import collect_parquet_root

    bars_path = config.curated_root / "daily_bars"
    try:
        bars = collect_parquet_root(
            bars_path,
            partition_col="trade_date",
            start=start,
            symbols=symbols,
        )
    except FileNotFoundError:
        return _EMPTY_BAR_DATES.clone()
    if bars.is_empty() or not {"symbol", "trade_date"}.issubset(bars.columns):
        return _EMPTY_BAR_DATES.clone()
    if "volume" in bars.columns:
        # Keep all dates for a symbol that has traded at least once: suspended
        # rows still need the carried-forward factor for adjusted queries. But
        # a symbol represented only by zero-volume placeholders is not a real
        # factor task and must not trigger a Sina request.
        traded = collect_parquet_root(
            bars_path,
            partition_col="trade_date",
            start=start,
            symbols=symbols,
            traded_only=True,
        )
        traded_symbols = set(traded.get_column("symbol").unique().to_list())
        bars = bars.filter(pl.col("symbol").is_in(traded_symbols))
        if bars.is_empty():
            return _EMPTY_BAR_DATES.clone()
    return bars.select(["symbol", "trade_date"]).unique().sort(["symbol", "trade_date"])


def _bars_for_derive(
    config: Config,
    *,
    watermark: date | None,
    refresh_set: set[str],
    full: bool,
) -> pl.DataFrame:
    """Select bar dates needed for this derive run (append-only when possible)."""
    if full or watermark is None:
        return _load_daily_bar_dates(config)

    frames: list[pl.DataFrame] = []
    # New trading days since the last derived partition.
    incremental = _load_daily_bar_dates(config, start=watermark).filter(
        pl.col("trade_date") > watermark
    )
    if not incremental.is_empty():
        frames.append(incremental)
    # An existing watermark partition can be incomplete: daily_bars may have
    # landed more rows after an earlier derive finished (retry, gap-fill, manual
    # compact). The watermark is a date, not a coverage receipt, so compare the
    # latest factor partition against the bars it should cover instead of
    # treating it as complete. Also look back over recent sessions: a late
    # gap-fill row can land in an older partition after the watermark already
    # advanced past it.
    if watermark is not None:
        missing = _bars_missing_factor_partitions(
            config,
            watermark,
            lookback_days=RECENT_FACTOR_RECONCILE_DAYS,
        )
        if not missing.is_empty():
            frames.append(missing)
    # Ex-date / new-listing / explicit rebackfill: realign full history for those symbols.
    if refresh_set:
        refreshed = _load_daily_bar_dates(config, symbols=sorted(refresh_set))
        if not refreshed.is_empty():
            frames.append(refreshed)
    if not frames:
        return _EMPTY_BAR_DATES.clone()
    return (
        pl.concat(frames, how="diagonal_relaxed")
        .unique(subset=["symbol", "trade_date"], keep="last")
        .sort(["symbol", "trade_date"])
    )


def _bars_missing_factor_partitions(
    config: Config,
    watermark: date,
    *,
    lookback_days: int,
) -> pl.DataFrame:
    """Bars in a recent window that the factor partitions have no row for."""
    # Lazy import: query.reader imports STORED_ADJUST_TYPE from this module.
    from cnequity.query.parquet_scan import dataset_has_parquet

    start = watermark - timedelta(days=max(0, lookback_days - 1))
    bars = _load_daily_bar_dates(config, start=start).filter(
        pl.col("trade_date") <= watermark
    )
    if bars.is_empty():
        return bars

    cdr_symbols = [symbol for symbol in bars["symbol"].unique().to_list() if _is_cdr(symbol)]
    if cdr_symbols:
        bars = bars.filter(~pl.col("symbol").is_in(cdr_symbols))
        if bars.is_empty():
            return bars

    factor_frames: list[pl.DataFrame] = []
    for trade_date in bars["trade_date"].unique().sort().to_list():
        part = (
            config.derived_root
            / "adj_factors"
            / f"trade_date={trade_date.isoformat()}"
        )
        if not dataset_has_parquet(part):
            continue
        factors = _read_parquet_files(sorted(part.rglob("*.parquet")))
        if factors.is_empty() or not {"symbol", "trade_date"}.issubset(factors.columns):
            continue
        factor_frames.append(factors.select(["symbol", "trade_date"]).unique())

    if not factor_frames:
        return bars
    factors = pl.concat(factor_frames, how="diagonal_relaxed")
    return bars.join(factors, on=["symbol", "trade_date"], how="anti")


def _bars_missing_factor_partition(config: Config, watermark: date) -> pl.DataFrame:
    """Backward-compatible single-partition wrapper for direct callers/tests."""
    return _bars_missing_factor_partitions(config, watermark, lookback_days=0)


def _align_factors_to_bars(
    sym_bars: pl.DataFrame,
    symbol: str,
    factors: pl.DataFrame,
    adjust_type: str,
) -> pl.DataFrame:
    sym_dates = sym_bars.select("trade_date").sort("trade_date")
    if sym_dates.is_empty():
        return pl.DataFrame()

    # Sina emits a sparse step function: one row per corporate-action date, with the
    # factor level that applies from that date forward. The factor on any trading day is
    # therefore the most recent event on or before it — an as-of (backward) join, not an
    # exact-date join. An exact join drops every event date that isn't itself a bar date
    # (e.g. all pre-history events when bars start long after IPO), leaving leading bars to
    # default to 1.0 and turning the first in-window event into a spurious >1000x jump.
    factors_sorted = factors.select(["trade_date", "factor"]).sort("trade_date")
    aligned = sym_dates.join_asof(factors_sorted, on="trade_date", strategy="backward")
    aligned = aligned.with_columns(pl.col("factor").fill_null(1.0))
    return aligned.with_columns(
        pl.lit(symbol).alias("symbol"),
        pl.lit(adjust_type).alias("adjust_type"),
    )


def _cache_path(config: Config, symbol: str, adjust_type: str) -> Path:
    cache_dir = config.meta_root / "adj_factors_cache"
    cache_dir.mkdir(parents=True, exist_ok=True)
    safe = symbol.replace(".", "_")
    return cache_dir / f"{safe}_{adjust_type}.parquet"


def _load_cache(config: Config, symbol: str, adjust_type: str) -> pl.DataFrame | None:
    path = _cache_path(config, symbol, adjust_type)
    if not path.exists():
        return None
    return pl.read_parquet(path).select(["trade_date", "factor"])


def _save_cache(config: Config, symbol: str, adjust_type: str, factors: pl.DataFrame) -> None:
    if factors.is_empty():
        return
    path = _cache_path(config, symbol, adjust_type)
    write_parquet_atomic(path, factors, compression="zstd")


def _read_parquet_files(files: list[Path]) -> pl.DataFrame:
    if not files:
        return pl.DataFrame()
    return pl.concat([pl.read_parquet(f) for f in files], how="diagonal_relaxed")


def _corporate_action_symbols_on(config: Config, trade_date: date) -> set[str]:
    root = config.curated_root / "corporate_actions"
    if not root.exists():
        return set()

    part_files = list((root / f"ex_date={trade_date.isoformat()}").glob("**/*.parquet"))
    files = part_files or list(root.glob("**/*.parquet"))
    df = _read_parquet_files(files)
    if df.is_empty() or not {"symbol", "ex_date"}.issubset(df.columns):
        return set()
    today = df.filter(pl.col("ex_date") == trade_date)
    return set(today["symbol"].unique().to_list())


def _new_listing_symbols_on(config: Config, trade_date: date) -> set[str]:
    root = config.curated_root / "instruments"
    if not root.exists():
        return set()

    df = _read_parquet_files(list(root.glob("**/*.parquet")))
    if df.is_empty() or not {"symbol", "list_date"}.issubset(df.columns):
        return set()
    listed = df.filter(pl.col("list_date") == trade_date)
    return set(listed["symbol"].unique().to_list())


def _delisted_symbols(config: Config) -> set[str]:
    """Return symbols with a formal delist date in the canonical catalog."""
    from cnequity.query.parquet_scan import dataset_has_parquet, scan_parquet_root

    root = config.curated_root / "instruments"
    if not dataset_has_parquet(root):
        return set()
    instruments = dedupe_lazy_by_primary_key(scan_parquet_root(root), "instruments").collect()
    if "symbol" not in instruments.columns or "delist_date" not in instruments.columns:
        return set()
    return set(
        instruments.filter(pl.col("delist_date").is_not_null())
        .get_column("symbol")
        .unique()
        .to_list()
    )


def _uncovered_symbols(config: Config) -> set[str]:
    """Symbols with bars the factor table does not reach.

    The derive is append-only from its watermark, which handles new sessions and
    misses everything else. `cne backfill daily_bars` adds *old* dates, and old
    dates are behind the watermark by definition, so the history it lands never
    gets a factor — silently, because `load(adjust=…)` fills factor=1.0 and only
    marks `adj_is_exact`. An append-only derive can leave historical BJ
    bars temporarily unadjusted until a targeted re-derive visits their full
    history; the self-heal below schedules those symbols for refresh.

    Compared per symbol rather than per row: a (symbol, trade_date) anti-join
    against a 338M-row daily_bars on every run would cost more than the derive.
    Min/max per symbol catches endpoint gaps; when the factor span reaches the
    latest bar, distinct-day counts also catch a deleted middle partition.
    A refreshed symbol realigns its whole history anyway.
    """
    from cnequity.query.parquet_scan import dataset_has_parquet, scan_parquet_root

    bars_root = config.curated_root / "daily_bars"
    if not dataset_has_parquet(bars_root):
        return set()
    bars = scan_parquet_root(bars_root, traded_only=True)
    bar_span = (
        bars.group_by("symbol")
        .agg(
            pl.col("trade_date").min().alias("bar_first"),
            pl.col("trade_date").max().alias("bar_last"),
            pl.col("trade_date").n_unique().alias("bar_days"),
        )
        .collect()
    )
    if bar_span.is_empty():
        return set()

    fac_root = config.derived_root / "adj_factors"
    if not dataset_has_parquet(fac_root):
        return set(bar_span["symbol"].to_list())
    factor_rows = scan_parquet_root(fac_root)
    factor_schema = set(factor_rows.collect_schema().names())
    if "adjust_type" in factor_schema:
        factor_rows = factor_rows.filter(pl.col("adjust_type") == STORED_ADJUST_TYPE)
    if "factor" in factor_schema:
        factor_rows = factor_rows.filter(
            pl.col("factor").is_not_null() & pl.col("factor").is_finite() & (pl.col("factor") > 0)
        )
    fac_span = (
        factor_rows.select("symbol", "trade_date")
        .group_by("symbol")
        .agg(
            pl.col("trade_date").min().alias("fac_first"),
            pl.col("trade_date").max().alias("fac_last"),
            pl.col("trade_date").n_unique().alias("fac_days"),
        )
        .collect()
    )
    # Only the *backward* direction. `fac_last < bar_last` is true on every
    # ordinary run — today's bar lands before its factor is derived — so
    # including it would force a full-history realign of the entire market
    # daily. New sessions are precisely what the incremental path is for.
    joined = bar_span.join(fac_span, on="symbol", how="left")
    # A missing middle factor is another uncovered history, but do not count
    # the expected terminal lag: today's bar can be written before Sina's
    # factor is available. Only enforce exact day counts when the factor span
    # already reaches the latest bar; endpoint gaps remain covered by the first
    # condition above.
    uncovered = joined.filter(
        pl.col("fac_first").is_null()
        | (pl.col("fac_first") > pl.col("bar_first"))
        | ((pl.col("fac_last") >= pl.col("bar_last")) & (pl.col("fac_days") < pl.col("bar_days")))
    )
    # Stocks and ETFs/LOFs. Sina serves fund factors in its ``s`` field (the
    # adapter converts them), so ETF hfq series are real and must be self-healed
    # like stocks. CDRs go for the same reason as before: the task loop already
    # drops them, so they can never be covered.
    candidates = {s for s in uncovered["symbol"].to_list() if not _is_cdr(s)}
    inst_root = config.curated_root / "instruments"
    if dataset_has_parquet(inst_root):
        instruments = dedupe_lazy_by_primary_key(
            scan_parquet_root(inst_root), "instruments"
        ).collect()
        if "asset_type" in instruments.columns:
            priced = set(
                instruments.filter(pl.col("asset_type").is_in(["stock", "etf"]))["symbol"].to_list()
            )
            # An explicit asset_type column is authoritative even when the
            # current catalog contains no priced assets (for example an ETF-only
            # fixture or a fully filtered research scope). Falling back to
            # every uncovered symbol would schedule non-priced factor fetches.
            candidates &= priced
    return candidates


def _event_refresh_symbols(config: Config, trade_date: date) -> set[str]:
    """Symbols whose factor cache should be refreshed for this trading date."""
    return _corporate_action_symbols_on(config, trade_date) | _new_listing_symbols_on(
        config, trade_date
    )


def _needs_refresh(
    cached: pl.DataFrame | None,
    force: bool,
) -> bool:
    if force:
        return True
    if cached is None or cached.is_empty():
        return True
    return False


def _resolve_factors(
    config: Config,
    symbol: str,
    adjust_type: str,
    sym_bars: pl.DataFrame,
    *,
    force: bool,
    client: httpx.Client,
) -> pl.DataFrame | None:
    cached = _load_cache(config, symbol, adjust_type)
    if not _needs_refresh(cached, force):
        return cached

    source = config.adj_factors_source
    try:
        if source != "sina":
            logger.warning("Unknown adj_factors source %s; skipping %s", source, symbol)
            return cached
        # Keep the source lease across the actual HTTP call.  The derive pool
        # may run beside DAG waves and must share the same cap as every other
        # Sina request; source_request also preserves the cross-process QPS
        # reservation configured for this source.
        with source_request(config, source):
            factors = fetch_adj_factor_series(symbol, adjust_type, client=client)
        _save_cache(config, symbol, adjust_type, factors)
        return factors
    except SinaAdjFactorUnavailableError as exc:
        if cached is None or cached.is_empty():
            raise AdjFactorsSourceUnavailableError(
                f"No cached adj factors for {symbol} ({adjust_type}): {exc}"
            ) from exc
        logger.warning("External adj factors failed for %s (%s): %s", symbol, adjust_type, exc)
        return cached
    except Exception as exc:
        if cached is None or cached.is_empty():
            raise AdjFactorsFetchError(
                f"No cached adj factors for {symbol} ({adjust_type}): {exc}"
            ) from exc
        logger.warning("External adj factors failed for %s (%s): %s", symbol, adjust_type, exc)
        return cached


def _factor_continuity_findings(out: pl.DataFrame) -> list[dict]:
    """Flag symbols whose stored factor jumps beyond any plausible corporate action.

    hfq factors are a step function that moves only on ex-dates by a bounded ratio; a
    day-over-day jump above MAX_FACTOR_STEP_RATIO (or a symmetric collapse) means the
    aligned series is broken. Emits one finding per offending symbol at its worst step.
    """
    if out.is_empty() or out.height < 2:
        return []
    ratios = (
        out.sort(["symbol", "adjust_type", "trade_date"])
        .with_columns(
            (pl.col("factor") / pl.col("factor").shift(1))
            .over(["symbol", "adjust_type"])
            .alias("_step_ratio")
        )
        .filter(pl.col("_step_ratio").is_not_null() & (pl.col("factor") > 0))
        .filter(
            (pl.col("_step_ratio") > MAX_FACTOR_STEP_RATIO)
            | (pl.col("_step_ratio") < 1.0 / MAX_FACTOR_STEP_RATIO)
        )
    )
    if ratios.is_empty():
        return []

    ratios = ratios.with_columns(
        pl.max_horizontal(pl.col("_step_ratio"), (1.0 / pl.col("_step_ratio"))).alias("_severity")
    )
    worst = (
        ratios.sort("_severity", descending=True)
        .group_by(["symbol", "adjust_type"], maintain_order=True)
        .first()
    )
    findings: list[dict] = []
    for row in worst.iter_rows(named=True):
        td = row["trade_date"]
        td_str = td.isoformat() if isinstance(td, date) else str(td)
        findings.append(
            {
                "dataset": "adj_factors",
                "severity": "error",
                "check": "adj_factor_continuity",
                "message": (
                    f"{row['symbol']} ({row['adjust_type']}) factor jumps "
                    f"{row['_step_ratio']:.1f}x on {td_str} to {row['factor']:.4g} "
                    f"(>{MAX_FACTOR_STEP_RATIO:.0f}x); likely a factor break, not a "
                    f"corporate action"
                ),
                "symbol": row["symbol"],
                "adjust_type": row["adjust_type"],
                "trade_date": td_str,
            }
        )
    return findings


# --- Recomputed-factor cross-check -----------------------------------------
#
# The stored series comes from one vendor. `corporate_actions` is a *second,
# independent* derivation of the same quantity, from a different vendor
# (EastMoney primary / TDX backup), so recomputing the factor step from it
# turns a single-source table into a two-source one without adding a source.
#
# An hfq factor is a step function that moves only on ex-dates, and the size of
# the step is fully determined by the action and the prior close:
#
#     ex-rights reference price
#         P_ref = (P_prev - D + A*PA) / (1 + B + T + A)
#     continuity of the adjusted series across the ex-date
#         f_ex / f_prev = P_prev / P_ref
#                       = (1 + B + T + A) * P_prev / (P_prev - D + A*PA)
#
# with D the pretax cash dividend per share, B the bonus (送股) ratio, T the
# transfer (转股) ratio, A the allotment (配股) ratio and PA the allotment
# price. On a day with no action every term vanishes and the expected ratio is
# exactly 1.0 — which is what catches the opposite failure, a factor that steps
# on a day the action table knows nothing about.
#
# The check runs for qfq as well as hfq: qfq is hfq divided by a per-symbol
# constant, and a constant divisor leaves consecutive-day ratios unchanged.
_ACTION_FIELDS = (
    "cash_dividend",
    "bonus_ratio",
    "transfer_ratio",
    "allotment_ratio",
    "allotment_price",
)
_ACTION_TERMS = ("_dividend", "_bonus", "_transfer", "_allotment", "_allot_cash")
_EMPTY_ACTION_TERMS = pl.DataFrame(
    schema={
        "symbol": pl.Utf8,
        "ex_date": pl.Date,
        **{term: pl.Float64 for term in _ACTION_TERMS},
    }
)


def _prior_factor_rows(config: Config, symbols: list[str], before: date) -> pl.DataFrame:
    """Last stored factor row per symbol strictly before ``before``.

    An append-only derive produces one new date per symbol, which has no
    predecessor inside its own output and therefore no measurable step — the
    exact run on which a missed ex-date would show up. Reading the already
    written partitions back supplies the missing left edge. Only a bounded
    lookback is read: the previous *bar* date is one partition back for a
    symbol that trades, and the window still covers a month of suspension
    without scanning the whole history.
    """
    from cnequity.query.parquet_scan import dataset_has_parquet, scan_parquet_root

    root = config.derived_root / "adj_factors"
    if not dataset_has_parquet(root):
        return pl.DataFrame()
    lookback_start = before - timedelta(days=CROSSCHECK_PRIOR_LOOKBACK_DAYS)
    prior = (
        scan_parquet_root(
            root,
            partition_col="trade_date",
            start=lookback_start,
            symbols=symbols,
        )
        .filter(pl.col("trade_date") < before)
        .select("symbol", "trade_date", "adjust_type", "factor")
        .collect()
    )
    if prior.is_empty():
        return prior
    return prior.sort("trade_date").group_by(["symbol", "adjust_type"], maintain_order=True).last()


def _closes_for(config: Config, symbols: list[str], start: date, end: date) -> pl.DataFrame:
    """Raw (unadjusted) closes over the compared window."""
    from cnequity.query.parquet_scan import collect_parquet_root

    try:
        bars = collect_parquet_root(
            config.curated_root / "daily_bars",
            partition_col="trade_date",
            start=start,
            end=end,
            symbols=symbols,
        )
    except FileNotFoundError:
        return pl.DataFrame()
    if bars.is_empty() or not {"symbol", "trade_date", "close"}.issubset(bars.columns):
        return pl.DataFrame()
    return bars.select("symbol", "trade_date", "close").unique(
        subset=["symbol", "trade_date"], keep="last"
    )


def _action_terms(config: Config, symbols: list[str], start: date, end: date) -> pl.DataFrame:
    """Per (symbol, ex_date) corporate-action terms over the compared window.

    ``action_type`` is part of the primary key, so one ex-date can carry a
    dividend row and an allotment row. The ratios add; the allotment *price* is
    not additive, so the cash each row returns to the holder is formed as
    ``ratio * price`` before aggregation.
    """
    from cnequity.query.parquet_scan import dataset_has_parquet, scan_parquet_root

    root = config.curated_root / "corporate_actions"
    if not dataset_has_parquet(root):
        return _EMPTY_ACTION_TERMS.clone()
    actions = dedupe_lazy_by_primary_key(
        scan_parquet_root(root, partition_col="ex_date", start=start, end=end, symbols=symbols),
        "corporate_actions",
    ).collect()
    if actions.is_empty() or not {"symbol", "ex_date"}.issubset(actions.columns):
        return _EMPTY_ACTION_TERMS.clone()
    for column in _ACTION_FIELDS:
        if column not in actions.columns:
            actions = actions.with_columns(pl.lit(None, dtype=pl.Float64).alias(column))
    terms = actions.select(
        "symbol",
        "ex_date",
        *[pl.col(c).cast(pl.Float64).fill_null(0.0).alias(c) for c in _ACTION_FIELDS],
    ).with_columns((pl.col("allotment_ratio") * pl.col("allotment_price")).alias("_allot_cash"))
    return terms.group_by(["symbol", "ex_date"]).agg(
        pl.col("cash_dividend").sum().alias("_dividend"),
        pl.col("bonus_ratio").sum().alias("_bonus"),
        pl.col("transfer_ratio").sum().alias("_transfer"),
        pl.col("allotment_ratio").sum().alias("_allotment"),
        pl.col("_allot_cash").sum().alias("_allot_cash"),
    )


def _corporate_action_crosscheck_findings(config: Config, out: pl.DataFrame) -> list[dict]:
    """Compare every stored factor step against the step the actions imply.

    Advisory by construction: it never fails the derive. The two vendors round
    differently and disagree on the odd definitional edge (a dividend paid in a
    second currency, a restated ratio), so the tolerance is a materiality
    threshold rather than an equality test. What it does catch is the class of
    break the continuity tripwire is blind to — a step of the wrong *size*, or
    a step on the wrong *day* — both of which sit far below 20x.
    """
    if out.is_empty() or not config.adj_factors_crosscheck_enabled:
        return []
    if not {"symbol", "trade_date", "adjust_type", "factor"}.issubset(out.columns):
        return []

    symbols = sorted(set(out.get_column("symbol").unique().to_list()))
    start = out.get_column("trade_date").min()
    end = out.get_column("trade_date").max()
    if not symbols or start is None or end is None:
        return []

    series = out.select("symbol", "trade_date", "adjust_type", "factor")
    prior = _prior_factor_rows(config, symbols, start)
    if not prior.is_empty():
        series = pl.concat([prior.select(series.columns), series], how="vertical_relaxed")
        start = min(start, prior.get_column("trade_date").min())

    closes = _closes_for(config, symbols, start, end)
    if closes.is_empty():
        return []

    steps = (
        series.unique(subset=["symbol", "trade_date", "adjust_type"], keep="last")
        .join(closes, on=["symbol", "trade_date"], how="left")
        .sort(["symbol", "adjust_type", "trade_date"])
        .with_columns(
            pl.col("factor").shift(1).over(["symbol", "adjust_type"]).alias("_prev_factor"),
            pl.col("close").shift(1).over(["symbol", "adjust_type"]).alias("_prev_close"),
        )
        .filter(
            pl.col("_prev_factor").is_not_null()
            & (pl.col("_prev_factor") > 0)
            & pl.col("factor").is_not_null()
            & (pl.col("factor") > 0)
            & pl.col("_prev_close").is_not_null()
            & (pl.col("_prev_close") > 0)
        )
    )
    if steps.is_empty():
        return []

    steps = (
        steps.join(
            _action_terms(config, symbols, start, end),
            left_on=["symbol", "trade_date"],
            right_on=["symbol", "ex_date"],
            how="left",
        )
        .with_columns([pl.col(term).fill_null(0.0) for term in _ACTION_TERMS])
        .with_columns(
            (pl.col("_prev_close") - pl.col("_dividend") + pl.col("_allot_cash")).alias(
                "_ref_price"
            ),
            (1.0 + pl.col("_bonus") + pl.col("_transfer") + pl.col("_allotment")).alias(
                "_share_mult"
            ),
            (pl.col("factor") / pl.col("_prev_factor")).alias("_actual"),
        )
    )

    findings = _degenerate_action_findings(steps.filter(pl.col("_ref_price") <= 0))

    diverged = (
        steps.filter(pl.col("_ref_price") > 0)
        .with_columns(
            (pl.col("_share_mult") * pl.col("_prev_close") / pl.col("_ref_price")).alias(
                "_expected"
            )
        )
        .filter(pl.col("_expected").is_finite() & (pl.col("_expected") > 0))
        .with_columns(
            (
                (pl.col("_actual") - pl.col("_expected")).abs() / pl.col("_expected") * 10_000.0
            ).alias("_bps")
        )
        .filter(pl.col("_bps") > config.adj_factors_crosscheck_tolerance_bps)
    )
    if diverged.is_empty():
        return findings

    # One finding per symbol at its worst day, matching the continuity check: a
    # full re-derive over twenty years would otherwise bury the audit. The day
    # count rides along so a single bad ex-date reads differently from a series
    # that has been wrong ever since.
    counts = diverged.group_by(["symbol", "adjust_type"]).agg(pl.len().alias("_divergent_days"))
    worst = (
        diverged.sort("_bps", descending=True)
        .group_by(["symbol", "adjust_type"], maintain_order=True)
        .first()
        .join(counts, on=["symbol", "adjust_type"], how="left")
        .sort("_bps", descending=True)
    )
    findings.extend(_crosscheck_finding(config, row) for row in worst.iter_rows(named=True))
    return findings


def _degenerate_action_findings(degenerate: pl.DataFrame) -> list[dict]:
    """Action rows whose terms imply a non-positive ex-rights reference price.

    Not a factor problem: a dividend larger than the entire prior close (or a
    negative allotment price) can only be a corrupt action row, and it would
    otherwise drop out of the comparison without a trace.
    """
    if degenerate.is_empty():
        return []
    worst = (
        degenerate.sort("_ref_price")
        .group_by(["symbol", "adjust_type"], maintain_order=True)
        .first()
    )
    findings: list[dict] = []
    for row in worst.iter_rows(named=True):
        td = row["trade_date"]
        td_str = td.isoformat() if isinstance(td, date) else str(td)
        findings.append(
            {
                "dataset": "adj_factors",
                "severity": "error",
                "check": "adj_factor_action_implies_nonpositive_price",
                "message": (
                    f"{row['symbol']} corporate action on {td_str} implies an ex-rights "
                    f"reference price of {row['_ref_price']:.4g} from a prior close of "
                    f"{row['_prev_close']:.4g} (dividend {row['_dividend']:.4g}, allotment "
                    f"cash {row['_allot_cash']:.4g}); the action row cannot be right and the "
                    "factor step cannot be checked against it"
                ),
                "symbol": row["symbol"],
                "adjust_type": row["adjust_type"],
                "trade_date": td_str,
                "backup_source": "corporate_actions",
            }
        )
    return findings


def _crosscheck_finding(config: Config, row: dict) -> dict:
    td = row["trade_date"]
    td_str = td.isoformat() if isinstance(td, date) else str(td)
    has_action = any(
        abs(row[term]) > 0 for term in ("_dividend", "_bonus", "_transfer", "_allotment")
    )
    if has_action:
        cause = (
            f"corporate_actions has dividend={row['_dividend']:.4g} "
            f"bonus={row['_bonus']:.4g} transfer={row['_transfer']:.4g} "
            f"allotment={row['_allotment']:.4g} on a prior close of {row['_prev_close']:.4g}"
        )
    else:
        cause = "corporate_actions has no ex-date that day, so the factor should not have moved"
    return {
        "dataset": "adj_factors",
        "severity": (
            "error" if row["_bps"] >= config.adj_factors_crosscheck_error_bps else "warning"
        ),
        "check": "adj_factor_corporate_action_divergence",
        "message": (
            f"{row['symbol']} ({row['adjust_type']}) factor steps {row['_actual']:.6g}x on "
            f"{td_str} but corporate_actions implies {row['_expected']:.6g}x "
            f"({row['_bps']:.0f} bps apart, {row['_divergent_days']} divergent day(s) this "
            f"run); {cause}"
        ),
        "symbol": row["symbol"],
        "adjust_type": row["adjust_type"],
        "trade_date": td_str,
        "primary_source": config.adj_factors_source,
        "backup_source": "corporate_actions",
        "actual_ratio": float(row["_actual"]),
        "expected_ratio": float(row["_expected"]),
        "divergence_bps": float(row["_bps"]),
        "divergent_days": int(row["_divergent_days"]),
    }


def _fetch_failure_finding(symbol: str, adjust_type: str, exc: Exception) -> dict:
    return {
        "dataset": "adj_factors",
        "severity": "error",
        "check": "adj_factor_fetch_failed",
        "message": f"No cached adj factors for {symbol} ({adjust_type}): {exc}",
        "symbol": symbol,
        "adjust_type": adjust_type,
    }


def _source_unavailable_finding(symbol: str, adjust_type: str, exc: Exception) -> dict:
    return {
        "dataset": "adj_factors",
        "severity": "warning",
        "check": "adj_factor_source_unavailable",
        "message": (
            f"{symbol} ({adjust_type}) is formally delisted and Sina returned an empty "
            "factor series; no exact adjusted history can be reconstructed from the "
            f"configured source ({exc}). Raw bars remain available; adjusted loads "
            "must expose adj_is_exact=False unless strict_adj=True"
        ),
        "symbol": symbol,
        "adjust_type": adjust_type,
        "source": "sina",
        "permanent": True,
    }


def _process_symbol_adj(
    config: Config,
    sym: str,
    adj: str,
    sym_bars: pl.DataFrame,
    *,
    force: bool,
    formally_delisted: bool = False,
    client: httpx.Client | None = None,
) -> tuple[pl.DataFrame | None, str | None, dict | None]:
    own_client = client is None
    if own_client:
        client = httpx.Client(timeout=20.0)
    try:
        try:
            factors = _resolve_factors(config, sym, adj, sym_bars, force=force, client=client)
        except AdjFactorsSourceUnavailableError as exc:
            if formally_delisted:
                return None, None, _source_unavailable_finding(sym, adj, exc)
            return None, f"{sym}:{adj}", _fetch_failure_finding(sym, adj, exc)
        except AdjFactorsFetchError as exc:
            return None, f"{sym}:{adj}", _fetch_failure_finding(sym, adj, exc)
        if factors is None or factors.is_empty():
            return None, None, None
        aligned = _align_factors_to_bars(sym_bars, sym, factors, adj)
        if aligned.is_empty():
            return None, None, None
        return aligned, None, None
    finally:
        if own_client:
            client.close()


def _write_adj_partitions(
    config: Config,
    out: pl.DataFrame,
    *,
    replace: bool,
) -> int:
    """Persist aligned factors. Append-only merges into existing partitions unless *replace*."""
    # query.__init__ imports this module for STORED_ADJUST_TYPE, so keep this
    # helper import lazy to avoid a derive↔query import cycle at module load.
    from cnequity.query.canonical import dedupe_by_primary_key

    writer = CuratedWriter(config.derived_root)
    total = 0
    for key, group in out.partition_by("trade_date", as_dict=True).items():
        td = key[0] if isinstance(key, tuple) else key
        td_str = td.isoformat() if isinstance(td, date) else str(td)
        out_dir = config.derived_root / "adj_factors" / f"trade_date={td_str}"
        out_dir.mkdir(parents=True, exist_ok=True)
        if replace:
            merged = group
        else:
            existing_files = sorted(out_dir.rglob("*.parquet"))
            if existing_files:
                existing = pl.concat(
                    [pl.read_parquet(path) for path in existing_files],
                    how="diagonal_relaxed",
                )
                merged = pl.concat([existing, group], how="diagonal_relaxed")
            else:
                merged = group
        merged = dedupe_by_primary_key(merged, "adj_factors")
        writer.write_partition("adj_factors", "trade_date", td_str, merged, "part-0.parquet")
        total += group.height
    return total


def compute_adj_factors(
    config: Config,
    adjust_type: str | None = None,
    *,
    refresh_symbols: list[str] | None = None,
    full: bool = False,
) -> AdjFactorsResult:
    """Derive hfq adj_factors (ADR-0004).

    Default path is append-only: only new trade_date partitions since the derived
    watermark are written, plus full-history merge for ex-date / new-listing /
    explicit refresh symbols. Pass ``full=True`` to rewrite every partition.
    """
    # The derive reads existing partitions and then merges/replaces them.  It
    # must not overlap compact, repartition, or another derive invocation.
    with lake_mutation_lock(config.meta_root, blocking=True):
        return _compute_adj_factors_locked(
            config,
            adjust_type,
            refresh_symbols=refresh_symbols,
            full=full,
        )


def _compute_adj_factors_locked(
    config: Config,
    adjust_type: str | None = None,
    *,
    refresh_symbols: list[str] | None = None,
    full: bool = False,
) -> AdjFactorsResult:
    """Implementation of :func:`compute_adj_factors` under the mutation lock."""
    adjust_types = [adjust_type] if adjust_type else list(config.adj_factors_types)
    skipped = [t for t in adjust_types if t != STORED_ADJUST_TYPE]
    if skipped:
        logger.warning(
            "adj_factors: ignoring non-persisted adjust_types %s (only %s is stored; "
            "derive qfq via load(..., adjust='qfq') — ADR-0004)",
            skipped,
            STORED_ADJUST_TYPE,
        )
    adjust_types = [STORED_ADJUST_TYPE]

    watermark = None if full else _adj_factors_watermark(config)
    # Event refresh uses the latest traded bar date in the lake (not just this
    # run's slice). A terminal zero-volume placeholder partition is not a
    # market session and must not drive corporate-action/new-listing refresh.
    from cnequity.query.universe import coverage_end_date

    latest_bar_date = coverage_end_date(config, "daily_bars")

    retry_symbols = _retry_symbols(config)
    source_unavailable_symbols = _source_unavailable_symbols(config)
    explicit_refresh = set(refresh_symbols or [])
    refresh_set = explicit_refresh | retry_symbols
    # An explicit empty response for a formally delisted symbol is a stable
    # source limitation, not a transient fetch failure. Re-probe only when the
    # caller explicitly asks for that symbol or requests a full derive.
    if not full:
        refresh_set -= source_unavailable_symbols - explicit_refresh
    if retry_symbols:
        logger.info(
            "adj_factors: retrying %d symbol(s) with a prior uncached fetch failure",
            len(retry_symbols),
        )
    if isinstance(latest_bar_date, date):
        refresh_set |= _event_refresh_symbols(config, latest_bar_date)
    if not full and watermark is not None:
        # Self-heal history the append-only path cannot see — see
        # `_uncovered_symbols`. Only meaningful when that path is in play:
        # `full`, and a lake with no watermark at all, already load every date,
        # and forcing a refresh there would only bypass a valid factor cache.
        uncovered = sorted(_uncovered_symbols(config) - refresh_set - source_unavailable_symbols)
        if uncovered:
            # Capped so a lake that has never derived does not turn one daily run
            # into a full-market sweep; the remainder is picked up next run, and
            # a name Sina genuinely cannot serve lands in `failed` rather than
            # being retried without limit.
            batch = uncovered[:UNCOVERED_REFRESH_LIMIT]
            logger.info(
                "adj_factors: %d symbol(s) have bars the factor table does not reach; "
                "realigning %d this run (e.g. %s)",
                len(uncovered),
                len(batch),
                batch[:5],
            )
            refresh_set |= set(batch)

    bars = _bars_for_derive(config, watermark=watermark, refresh_set=refresh_set, full=full)
    if bars.is_empty():
        logger.info(
            "adj_factors: nothing to derive (watermark=%s, refresh=%d, full=%s)",
            watermark,
            len(refresh_set),
            full,
        )
        return AdjFactorsResult(0, 0, [], [])

    replace = full or watermark is None
    mode = "full-replace" if replace else "append-only"
    logger.info(
        "adj_factors: %s watermark=%s symbols=%d dates=%d refresh=%d",
        mode,
        watermark,
        bars["symbol"].n_unique(),
        bars["trade_date"].n_unique(),
        len(refresh_set),
    )

    # Adjustment factors are HTTP-bound and use one client per task.  They
    # must not inherit the TDX/process-pool budget: on macOS the latter is
    # intentionally one process, while Sina can safely serve several paced
    # HTTP requests.  The source cap is a second guard for a parallel DAG or
    # a deliberately large derive pool; the cross-process rate limiter still
    # owns the QPS contract.
    configured_workers = config.adj_factor_worker_count()
    source_workers = config.source_concurrency_for(
        config.adj_factors_source,
        configured_workers,
    )
    workers = max(1, min(configured_workers, source_workers, 16))

    tasks: list[tuple[str, str, pl.DataFrame, bool]] = []
    skipped_cdr: list[str] = []
    for group in bars.partition_by("symbol"):
        sym = group["symbol"][0]
        if _is_cdr(sym):
            skipped_cdr.append(sym)
            continue
        sym_bars = group.select("trade_date").sort("trade_date")
        force = sym in refresh_set
        for adj in adjust_types:
            tasks.append((sym, adj, sym_bars, force))
    if skipped_cdr:
        logger.info(
            "adj_factors: skipping %d CDR symbol(s) %s — sina has no CDR factor "
            "coverage and all_a excludes CDRs; loads report adj_is_exact=False",
            len(skipped_cdr),
            skipped_cdr,
        )

    frames: list[pl.DataFrame] = []
    failed: list[str] = []
    findings: list[dict] = []
    succeeded: set[str] = set()
    newly_unavailable: set[str] = set()
    formally_delisted = _delisted_symbols(config)
    skipped_source_unavailable = (
        source_unavailable_symbols - explicit_refresh if not full else set()
    )

    def _consume(
        sym: str,
        aligned: pl.DataFrame | None,
        fail_key: str | None,
        finding: dict | None,
    ) -> None:
        """Merge one symbol result into the derive outcome state."""
        if fail_key:
            failed.append(fail_key)
        if finding:
            findings.append(finding)
            if finding.get("permanent"):
                newly_unavailable.add(sym)
                succeeded.add(sym)
        if aligned is not None:
            frames.append(aligned)
            succeeded.add(sym)

    if workers <= 1 or len(tasks) == 1:
        with httpx.Client(timeout=20.0) as client:
            for sym, adj, sym_bars, force in tasks:
                if sym in skipped_source_unavailable:
                    continue
                try:
                    aligned, fail_key, finding = _process_symbol_adj(
                        config,
                        sym,
                        adj,
                        sym_bars,
                        force=force,
                        formally_delisted=sym in formally_delisted,
                        client=client,
                    )
                    _consume(sym, aligned, fail_key, finding)
                except Exception as exc:  # noqa: BLE001 — keep symbol retryable
                    logger.warning("adj_factors failed for %s (%s): %s", sym, adj, exc)
                    _consume(sym, None, f"{sym}:{adj}", _fetch_failure_finding(sym, adj, exc))
    else:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {
                pool.submit(
                    _process_symbol_adj,
                    config,
                    sym,
                    adj,
                    sym_bars,
                    force=force,
                    formally_delisted=sym in formally_delisted,
                ): sym
                for sym, adj, sym_bars, force in tasks
                if sym not in skipped_source_unavailable
            }
            for fut in as_completed(futures):
                sym = futures[fut]
                adj = STORED_ADJUST_TYPE
                try:
                    aligned, fail_key, finding = fut.result()
                    _consume(sym, aligned, fail_key, finding)
                except Exception as exc:  # noqa: BLE001 — keep symbol retryable
                    logger.warning("adj_factors failed for %s (%s): %s", sym, adj, exc)
                    _consume(sym, None, f"{sym}:{adj}", _fetch_failure_finding(sym, adj, exc))

    if not frames:
        _update_retry_symbols(
            config,
            previous=retry_symbols,
            succeeded=succeeded,
            failed={item.rsplit(":", 1)[0] for item in failed},
        )
        _update_source_unavailable_symbols(
            config,
            previous=source_unavailable_symbols,
            succeeded=succeeded,
            newly_unavailable=newly_unavailable,
        )
        return AdjFactorsResult(0, len(tasks), failed, findings)

    out = pl.concat(frames, how="diagonal_relaxed").unique(subset=_ADJ_PK, keep="last")
    findings.extend(_factor_continuity_findings(out))
    findings.extend(_corporate_action_crosscheck_findings(config, out))
    out = with_provenance(out, source=config.adj_factors_source, data_version="v1")

    total = _write_adj_partitions(config, out, replace=replace)
    _update_retry_symbols(
        config,
        previous=retry_symbols,
        succeeded=succeeded,
        failed={item.rsplit(":", 1)[0] for item in failed},
    )
    _update_source_unavailable_symbols(
        config,
        previous=source_unavailable_symbols,
        succeeded=succeeded,
        newly_unavailable=newly_unavailable,
    )
    return AdjFactorsResult(total, len(tasks), failed, findings)
