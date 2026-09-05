"""Merge-style compact for instruments (preserve delisted symbols)."""

from __future__ import annotations

import hashlib
import json
import os
from datetime import date
from pathlib import Path

import polars as pl

from cnequity.domain.canonical import dedupe_by_primary_key
from cnequity.domain.schemas import INSTRUMENTS_SCHEMA, validate_dataframe
from cnequity.domain.symbols import is_subscription_placeholder
from cnequity.storage.atomic import write_parquet_atomic
from cnequity.storage.parquet import StagingWriter

def _absence_state_path(curated_root: Path) -> Path:
    return curated_root.parent / "meta" / "instruments_absence_streak.json"


def _save_absence_state(path: Path, state: dict[str, dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_text(json.dumps(state, sort_keys=True, indent=2), encoding="utf-8")
    os.replace(tmp, path)


def _formal_delist_dates(curated_root: Path, as_of: date) -> dict[str, date] | None:
    """Read the complete security-master delisting identity observation.

    A TDX/EastMoney instruments snapshot only answers which symbols it happened
    to return.  Its absence is not a delisting event, regardless of how many
    consecutive snapshots omit the symbol.  The Baostock full security-master
    evidence is deliberately separate from instruments so compact can verify
    the sticky ``delist_date`` instead of treating an old inference as fact.
    """
    path = (
        curated_root.parent
        / "meta"
        / "quality"
        / "evidence"
        / "delisted_security_identity"
        / "baostock-v1.json"
    )
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if (
            payload.get("claim") != "delisted_security_identity"
            or payload.get("evidence_version") != 1
            or payload.get("status") != "complete"
            or payload.get("source") != "baostock.query_stock_basic"
        ):
            return None
        dates = {
            str(symbol): date.fromisoformat(value)
            for symbol, value in payload.get("delisted_symbols", {}).items()
        }
    except (OSError, ValueError, TypeError, json.JSONDecodeError):
        return None
    return {symbol: value for symbol, value in dates.items() if value <= as_of}


def _apply_formal_delist_dates(
    df: pl.DataFrame, formal_dates: dict[str, date]
) -> pl.DataFrame:
    """Keep dates only when independently certified by the security master."""
    if df.is_empty() or "delist_date" not in df.columns:
        return df
    dates = pl.DataFrame(
        {
            "symbol": list(formal_dates),
            "_formal_delist_date": list(formal_dates.values()),
        },
        schema={"symbol": pl.Utf8, "_formal_delist_date": pl.Date},
    )
    return (
        df.drop("delist_date")
        .join(dates, on="symbol", how="left")
        .rename({"_formal_delist_date": "delist_date"})
    )


def _strip_subscription_placeholders(df: pl.DataFrame) -> pl.DataFrame:
    if df.is_empty() or "name" not in df.columns:
        return df
    keep = [not is_subscription_placeholder(name) for name in df["name"].to_list()]
    return df.filter(pl.Series(keep))


def _business_digest(df: pl.DataFrame) -> str:
    """Hash instrument business content while ignoring fetch provenance churn."""
    volatile = {"source", "data_version", "fetched_at", "run_id", "capture_id"}
    columns = [column for column in df.columns if column not in volatile]
    if not columns:
        return hashlib.sha256(b"[]").hexdigest()
    canonical = df.select(columns).sort(columns)
    encoded = json.dumps(
        canonical.to_dicts(), sort_keys=True, separators=(",", ":"), default=str
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def compact_instruments(
    staging_root: Path,
    curated_root: Path,
    run_id: str,
    trade_date: date,
    changed_files: list[Path] | None = None,
    base_root: Path | None = None,
    delist_identity_changed: set[str] | None = None,
) -> tuple[int, list[dict]]:
    """Merge staging instruments into curated, retaining symbols missing from TDX."""
    staging = StagingWriter(staging_root)
    files = staging.list_run_files("instruments", run_id)
    if not files:
        return 0, []

    incoming = pl.concat(
        [validate_dataframe(pl.read_parquet(f), "instruments") for f in files],
        how="diagonal_relaxed",
    )
    incoming = _strip_subscription_placeholders(incoming)
    incoming = dedupe_by_primary_key(incoming, "instruments")

    out_path = curated_root / "instruments" / "part-merged.parquet"
    read_dir = Path(base_root) if base_root is not None else out_path.parent
    curated_files = sorted(read_dir.rglob("*.parquet")) if read_dir.exists() else []
    # ``base_root`` is an immutable revision generation used as the merge
    # input.  Its canonical parquet path is necessarily different from the
    # mutable output path, but that does not make the generation a stale
    # fragment.  Treat only files discovered directly in the mutable curated
    # directory as fragments; otherwise a provenance-only refresh rewrites the
    # mutable parquet on every compact.
    had_fragments = base_root is None and any(path != out_path for path in curated_files)
    had_duplicate_rows = False
    had_removed_rows = False
    if curated_files:
        existing = pl.concat(
            [validate_dataframe(pl.read_parquet(path), "instruments") for path in curated_files],
            how="diagonal_relaxed",
        )
        raw_existing_height = existing.height
        existing = _strip_subscription_placeholders(existing)
        had_removed_rows = existing.height != raw_existing_height
        had_duplicate_rows = existing.height != existing.select("symbol").n_unique()
        existing = dedupe_by_primary_key(existing, "instruments")
    else:
        existing = pl.DataFrame(schema=INSTRUMENTS_SCHEMA)
    prior_delist_dates = {
        row["symbol"]: row["delist_date"]
        for row in existing.select("symbol", "delist_date").iter_rows(named=True)
    }
    # Preserve the raw generation's digest.  Identity reconciliation below can
    # intentionally change an existing row even when the incoming snapshot did
    # not, and that repair must still be written.
    before_business_digest = _business_digest(existing) if curated_files else None

    # ``delist_date`` is an identity claim, not a conclusion from the daily
    # live snapshot.  Earlier compact versions inferred it after two missing
    # snapshots, which mislabeled newly listed symbols when a TDX page was
    # incomplete.  Reconcile every candidate against the independent, complete
    # Baostock security-master evidence and thereby repair those old claims.
    # A missing or malformed evidence record is deliberately not equivalent to
    # a complete observation with zero delistings: retain the old dates until a
    # successful security-master refresh can decide them.
    formal_dates = _formal_delist_dates(curated_root, trade_date)
    if formal_dates is not None:
        incoming = _apply_formal_delist_dates(incoming, formal_dates)
    incoming_symbols = incoming["symbol"].to_list()
    findings: list[dict] = []
    absence_path = _absence_state_path(curated_root)
    if not existing.is_empty():
        if formal_dates is not None:
            existing = _apply_formal_delist_dates(existing, formal_dates)
        preserved = existing.filter(~pl.col("symbol").is_in(incoming_symbols))
        prior_dates = existing.select(
            [
                "symbol",
                pl.col("list_date").alias("_prior_list_date"),
                pl.col("delist_date").alias("_prior_delist_date"),
            ]
        )
        incoming = incoming.join(prior_dates, on="symbol", how="left")
        # List dates are snapshot metadata; formal delist dates were applied
        # above from the independent identity evidence and remain sticky.
        incoming = incoming.with_columns(
            pl.coalesce(pl.col("list_date"), pl.col("_prior_list_date")).alias("list_date"),
            pl.coalesce(pl.col("delist_date"), pl.col("_prior_delist_date")).alias("delist_date"),
        ).drop("_prior_list_date", "_prior_delist_date")
    else:
        preserved = pl.DataFrame(schema=INSTRUMENTS_SCHEMA)

    merged = pl.concat([incoming, preserved], how="diagonal_relaxed")
    merged = dedupe_by_primary_key(merged, "instruments")
    if delist_identity_changed is not None:
        current_delist_dates = {
            row["symbol"]: row["delist_date"]
            for row in merged.select("symbol", "delist_date").iter_rows(named=True)
        }
        delist_identity_changed.update(
            symbol
            for symbol in prior_delist_dates | current_delist_dates
            if prior_delist_dates.get(symbol) != current_delist_dates.get(symbol)
        )

    after_business_digest = _business_digest(merged)
    business_changed = before_business_digest != after_business_digest
    # A same-business-content refresh (for example only a new fetched_at or
    # source label) is a true no-op.  Avoid rewriting the canonical file so
    # downstream revision logic and file mtimes remain quiet.  Fragments are
    # still consolidated when present, but that cleanup alone is not a new
    # business revision.
    if (
        business_changed
        or had_fragments
        or had_duplicate_rows
        or had_removed_rows
        or not out_path.is_file()
    ):
        write_parquet_atomic(out_path, merged, compression="zstd")
        # Instruments is merge-style, so there is no partition writer to clean
        # up stale fragments. Keep one canonical file; readers and whole-lake
        # audits must not see an old ``part-*.parquet`` beside it.
        for stale in out_path.parent.rglob("*.parquet"):
            if stale != out_path:
                stale.unlink()
    if changed_files is not None and business_changed:
        changed_files.append(out_path)
    # Discard stale state written by the old absence-inference algorithm.  It
    # has no authority to influence future security-master identity.
    _save_absence_state(absence_path, {})
    return merged.height, findings
