"""Beijing exchange has no TDX route — bars must come from the fallback vendor."""

import json
from datetime import date, timedelta

import httpx
import polars as pl
import pytest

from cnequity.config import Config
from cnequity.domain.schemas import DAILY_BARS_SCHEMA
from cnequity.domain.symbols import is_tdx_servable, split_by_quote_source
from cnequity.steps.bars import (
    _resolve_daily_bar_scope,
    fetch_bars_via_sina,
    repair_bse_tip_amounts_from_curated,
)
from cnequity.steps.delisted import catalog_path
from cnequity.steps.reference import _merge_untdxable_instruments
from cnequity.storage.parquet import StagingWriter

_BAR_COLS = [c for c in DAILY_BARS_SCHEMA if c not in ("source", "data_version", "fetched_at")]


def _bars(symbol: str, days: list[date]) -> pl.DataFrame:
    n = len(days)
    return pl.DataFrame(
        {
            "symbol": [symbol] * n,
            "trade_date": days,
            "open": [1.0] * n,
            "high": [1.0] * n,
            "low": [1.0] * n,
            "close": [1.0] * n,
            "volume": [10] * n,
            "amount": [None] * n,
        },
        schema={c: DAILY_BARS_SCHEMA[c] for c in _BAR_COLS},
    )


def _staged(cfg, run_id) -> pl.DataFrame:
    files = StagingWriter(cfg.staging_root).list_run_files("daily_bars", run_id)
    if not files:
        return pl.DataFrame()
    return pl.concat([pl.read_parquet(f) for f in files], how="diagonal_relaxed")


# --- routing ----------------------------------------------------------------


def test_only_sh_and_sz_are_tdx_servable():
    assert is_tdx_servable("600519.SH") and is_tdx_servable("000001.SZ")
    assert not is_tdx_servable("920000.BJ"), "TDX has no Beijing market id"
    assert not is_tdx_servable("garbage")


def test_split_preserves_order_within_each_side():
    tdx, fallback = split_by_quote_source(["600519.SH", "920001.BJ", "000001.SZ", "920000.BJ"])

    assert tdx == ["600519.SH", "000001.SZ"]
    assert fallback == ["920001.BJ", "920000.BJ"]


# --- fallback fetch ---------------------------------------------------------


def test_fallback_bars_are_staged_with_their_own_provenance(tmp_path):
    cfg = Config(data_root=tmp_path / "data", sources={"sina": True})

    result = fetch_bars_via_sina(
        cfg,
        ["920000.BJ", "920001.BJ"],
        date(2026, 7, 20),
        date(2026, 7, 21),
        "run-1",
        fetch=lambda s, c: _bars(s, [date(2026, 7, 20), date(2026, 7, 21)]),
    )

    staged = _staged(cfg, "run-1")
    assert result["rows_written"] == 4
    assert set(staged["symbol"]) == {"920000.BJ", "920001.BJ"}
    assert staged["source"].unique().to_list() == ["sina"]


def test_bse_is_primary_for_a_current_single_session(tmp_path, monkeypatch):
    cfg = Config(
        data_root=tmp_path / "data",
        sources={"sina": True, "bse": True},
        source_intervals={"bse": 0.0, "sina_bars": 0.0},
    )
    day = date(2026, 8, 21)
    bse = _bars("920000.BJ", [day]).with_columns(pl.lit(1234.5).alias("amount"))
    monkeypatch.setattr("cnequity.steps.bars.list_trading_dates", lambda *args: [day])
    monkeypatch.setattr(
        "cnequity.adapters.bse.daily_quotes.fetch_daily_quotes", lambda *args, **kwargs: bse
    )

    def no_sina(*args, **kwargs):
        raise AssertionError("Sina should not be called when BSE covers the BJ tip")

    monkeypatch.setattr("cnequity.adapters.sina.bars.fetch_daily_bars_sina", no_sina)
    result = fetch_bars_via_sina(
        cfg,
        ["920000.BJ"],
        day - timedelta(days=1),
        day,
        "run-bse",
    )

    staged = _staged(cfg, "run-bse")
    assert result["rows_written"] == 1
    assert staged["amount"].item() == 1234.5
    assert staged["source"].item() == "bse"
    assert result["context_updates"]["audit_findings"][0]["check"] == "daily_bars_bse_tip"


def test_bse_rows_are_counted_with_sina_residuals(tmp_path, monkeypatch):
    cfg = Config(
        data_root=tmp_path / "data",
        sources={"sina": True, "bse": True},
        source_intervals={"bse": 0.0, "sina_bars": 0.0},
    )
    day = date(2026, 8, 21)
    bse = _bars("920000.BJ", [day]).with_columns(pl.lit(1234.5).alias("amount"))
    monkeypatch.setattr("cnequity.steps.bars.list_trading_dates", lambda *args: [day])
    monkeypatch.setattr(
        "cnequity.adapters.bse.daily_quotes.fetch_daily_quotes", lambda *args, **kwargs: bse
    )
    monkeypatch.setattr(
        "cnequity.adapters.sina.bars.fetch_daily_bars_sina",
        lambda symbol, **kwargs: _bars(symbol, [day]),
    )

    result = fetch_bars_via_sina(
        cfg,
        ["920000.BJ", "600519.SH"],
        day - timedelta(days=1),
        day,
        "run-mixed",
    )

    staged = _staged(cfg, "run-mixed")
    assert result["rows_written"] == 2
    assert set(staged["source"].unique().to_list()) == {"bse", "sina"}


def test_bse_tip_amount_requires_exact_sina_ohlcv(tmp_path, monkeypatch):
    cfg = Config(
        data_root=tmp_path / "data",
        sources={"sina": True, "bse": True},
        source_intervals={"bse": 0.0},
    )
    day = date(2026, 8, 21)
    bse = _bars("920000.BJ", [day]).with_columns(pl.lit(1234.5).alias("amount"))
    monkeypatch.setattr(
        "cnequity.adapters.bse.daily_quotes.fetch_daily_quotes", lambda *a, **k: bse
    )

    result = fetch_bars_via_sina(
        cfg,
        ["920000.BJ"],
        day,
        day,
        "run-1",
        fetch=lambda s, c: _bars(s, [day]),
    )

    staged = _staged(cfg, "run-1")
    assert staged["amount"].item() == 1234.5
    assert staged["source"].item() == "bse"
    assert result["context_updates"]["audit_findings"][0]["check"] == (
        "daily_bars_bse_amount_supplement"
    )


def test_bse_tip_mismatch_keeps_sina_amount_null(tmp_path, monkeypatch):
    cfg = Config(
        data_root=tmp_path / "data",
        sources={"sina": True, "bse": True},
        source_intervals={"bse": 0.0},
    )
    day = date(2026, 8, 21)
    bse = _bars("920000.BJ", [day]).with_columns(
        pl.lit(2.0).alias("close"),
        pl.lit(1234.5).alias("amount"),
    )
    monkeypatch.setattr(
        "cnequity.adapters.bse.daily_quotes.fetch_daily_quotes", lambda *a, **k: bse
    )

    result = fetch_bars_via_sina(
        cfg,
        ["920000.BJ"],
        day,
        day,
        "run-1",
        fetch=lambda s, c: _bars(s, [day]),
    )

    staged = _staged(cfg, "run-1")
    assert staged["amount"].item() is None
    assert staged["source"].item() == "sina"
    assert result["context_updates"]["audit_findings"][0]["check"] == (
        "daily_bars_bse_quote_mismatch"
    )


def test_scoped_daily_backfill_rejects_unknown_instrument(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "data")
    monkeypatch.setattr("cnequity.steps.bars.load_symbols", lambda config: ["920000.BJ"])

    with pytest.raises(RuntimeError, match="not present in instruments"):
        _resolve_daily_bar_scope(cfg, ["920000.BJ", "999999.BJ"])


def test_bse_curated_repair_does_not_call_sina(tmp_path, monkeypatch):
    cfg = Config(
        data_root=tmp_path / "data",
        sources={"bse": True},
        source_intervals={"bse": 0.0},
    )
    day = date(2026, 8, 21)
    part = cfg.curated_root / "daily_bars" / f"trade_date={day.isoformat()}"
    part.mkdir(parents=True)
    _bars("920000.BJ", [day]).write_parquet(part / "part-merged.parquet")
    bse = _bars("920000.BJ", [day]).with_columns(pl.lit(1234.5).alias("amount"))
    monkeypatch.setattr("cnequity.steps.bars.load_symbols", lambda config: ["920000.BJ"])
    monkeypatch.setattr(
        "cnequity.adapters.bse.daily_quotes.fetch_daily_quotes", lambda *a, **k: bse
    )

    result = repair_bse_tip_amounts_from_curated(cfg, day, "run-repair", ["920000.BJ"])

    staged = _staged(cfg, "run-repair")
    assert result["rows_written"] == 1
    assert staged["amount"].item() == 1234.5
    assert staged["source"].item() == "bse"


def test_bse_curated_repair_does_not_claim_success_when_bse_is_unavailable(tmp_path, monkeypatch):
    cfg = Config(
        data_root=tmp_path / "data",
        sources={"bse": True},
        source_intervals={"bse": 0.0},
    )
    day = date(2026, 8, 21)
    part = cfg.curated_root / "daily_bars" / f"trade_date={day.isoformat()}"
    part.mkdir(parents=True)
    _bars("920000.BJ", [day]).write_parquet(part / "part-merged.parquet")
    monkeypatch.setattr("cnequity.steps.bars.load_symbols", lambda config: ["920000.BJ"])
    monkeypatch.setattr(
        "cnequity.adapters.bse.daily_quotes.fetch_daily_quotes",
        lambda *a, **k: (_ for _ in ()).throw(RuntimeError("BSE down")),
    )

    result = repair_bse_tip_amounts_from_curated(cfg, day, "run-repair", ["920000.BJ"])

    assert result["status"] == "warning"
    assert result["rows_written"] == 0
    assert result["context_updates"]["audit_findings"][0]["check"] == (
        "daily_bars_bse_amount_unavailable"
    )


def test_one_dead_symbol_does_not_cost_the_whole_board(tmp_path):
    cfg = Config(data_root=tmp_path / "data", sources={"sina": True})

    def flaky(symbol, client):
        if symbol == "920000.BJ":
            raise ConnectionError("reset")
        return _bars(symbol, [date(2026, 7, 21)])

    result = fetch_bars_via_sina(
        cfg, ["920000.BJ", "920001.BJ"], date(2026, 7, 21), date(2026, 7, 21), "run-1", fetch=flaky
    )

    assert result["rows_written"] == 1
    assert result["failed_symbols"] == 1
    assert result["failed_symbol_names"] == ["920000.BJ"]
    finding = result["context_updates"]["audit_findings"][0]
    assert finding["check"] == "fallback_source_incomplete"
    assert "920000.BJ" in finding["message"]


def test_sina_empty_response_is_not_a_fetch_failure(tmp_path):
    cfg = Config(data_root=tmp_path / "data", sources={"sina": True})
    symbol = "920000.BJ"

    result = fetch_bars_via_sina(
        cfg,
        [symbol],
        date(2026, 7, 21),
        date(2026, 7, 21),
        "run-empty",
        fetch=lambda _symbol, _client: pl.DataFrame(
            schema={c: DAILY_BARS_SCHEMA[c] for c in _BAR_COLS}
        ),
    )

    assert "failed_symbols" not in result
    assert result["empty_symbol_names"] == [symbol]
    finding = result["context_updates"]["audit_findings"][0]
    assert finding["check"] == "fallback_source_empty"
    assert finding["severity"] == "info"


def test_no_fallback_symbols_is_a_cheap_noop(tmp_path):
    cfg = Config(data_root=tmp_path / "data", sources={"sina": True})

    def must_not_be_called(symbol, client):
        raise AssertionError("fetched nothing-to-fetch")

    result = fetch_bars_via_sina(
        cfg, [], date(2026, 7, 21), date(2026, 7, 21), "run-1", fetch=must_not_be_called
    )
    assert result["rows_written"] == 0


def test_sina_bars_retries_transient_rate_limit(tmp_path, monkeypatch):
    cfg = Config(
        data_root=tmp_path / "data",
        sources={"sina": True, "sina_bars": True},
        source_intervals={"sina_bars": 0.0},
        retry_backoff_seconds=5,
    )
    attempts = 0
    sleeps: list[float] = []
    request = httpx.Request("GET", "https://example.test/sina")

    def flaky(symbol, client):
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            response = httpx.Response(456, request=request)
            raise httpx.HTTPStatusError("rate limited", request=request, response=response)
        return _bars(symbol, [date(2026, 7, 21)])

    monkeypatch.setattr("cnequity.steps.bars.time.sleep", sleeps.append)
    result = fetch_bars_via_sina(
        cfg,
        ["920000.BJ"],
        date(2026, 7, 21),
        date(2026, 7, 21),
        "run-retry",
        fetch=flaky,
    )

    assert attempts == 3
    assert sleeps == [5.0, 10.0]
    assert result["rows_written"] == 1
    assert "failed_symbols" not in result


# --- instruments ------------------------------------------------------------


def _live_instruments(symbols):
    return pl.DataFrame(
        {
            "symbol": list(symbols),
            "name": ["x"] * len(symbols),
            "exchange": [s.split(".")[1] for s in symbols],
            "asset_type": ["stock"] * len(symbols),
            "list_date": pl.Series([None] * len(symbols), dtype=pl.Date),
            "delist_date": pl.Series([None] * len(symbols), dtype=pl.Date),
            "prev_symbol": [None] * len(symbols),
            "source": ["tdx_protocol"] * len(symbols),
            "data_version": ["v1"] * len(symbols),
        }
    )


def _cfg_with_catalog(tmp_path, entries: dict[str, str], bars_through: date):
    cfg = Config(data_root=tmp_path / "data", sources={"sina": True})
    path = catalog_path(cfg)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"delisted": entries, "never_issued": []}))
    part = cfg.curated_root / "daily_bars" / f"trade_date={bars_through.isoformat()}"
    part.mkdir(parents=True, exist_ok=True)
    pl.DataFrame({"symbol": ["600519.SH"], "trade_date": [bars_through]}).write_parquet(
        part / "part-merged.parquet"
    )
    return cfg


def test_beijing_symbols_are_added_to_the_instrument_list(tmp_path):
    """Without this the daily step never sees them and BJ stays empty forever."""
    cfg = _cfg_with_catalog(
        tmp_path, {"920000.BJ": "2026-07-21", "600001.SH": "2009-12-15"}, date(2026, 7, 21)
    )

    out = _merge_untdxable_instruments(cfg, _live_instruments(["600519.SH"]))

    assert set(out["symbol"]) == {"600519.SH", "920000.BJ"}
    bj = out.filter(pl.col("symbol") == "920000.BJ")
    assert bj["delist_date"].item() is None, "a trading stock must not carry a delist_date"
    assert bj["source"].item() == "sina"


def test_delisted_names_are_not_added_by_this_path(tmp_path):
    """Historical delistings belong to the backfill, not the live instrument list."""
    cfg = _cfg_with_catalog(tmp_path, {"600001.SH": "2009-12-15"}, date(2026, 7, 21))

    out = _merge_untdxable_instruments(cfg, _live_instruments(["600519.SH"]))

    assert set(out["symbol"]) == {"600519.SH"}


def test_a_missing_catalogue_is_not_fatal(tmp_path):
    cfg = Config(data_root=tmp_path / "data", sources={"sina": True})
    live = _live_instruments(["600519.SH"])

    assert _merge_untdxable_instruments(cfg, live).equals(live)
