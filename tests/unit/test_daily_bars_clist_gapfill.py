"""TDX tip gaps use clist first, then bounded kline recovery (ADR-0005)."""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from cnequity.adapters.eastmoney.bars import fetch_daily_bars_clist
from cnequity.config import Config, FailoverDatasetSpec
from cnequity.domain.schemas import with_provenance
from cnequity.orchestrator.manifest import Manifest
from cnequity.steps.bars import (
    _finish_daily_bars,
    _gapfill_tip_via_clist,
    _reject_preopen_placeholder,
    _resolve_recovered_daily_batches,
    _staged_daily_bar_partial_symbols,
    _staged_daily_bar_symbols,
)
from cnequity.storage import StagingWriter
from cnequity.storage.layout import init_data_layout


def _cfg(tmp_path) -> Config:
    cfg = Config(
        data_root=tmp_path / "data",
        workers=1,
        batch_size=10,
        tdx_allow_mock=True,
        failover_enabled=True,
        failover_datasets=[
            FailoverDatasetSpec(
                name="daily_bars",
                primary="tdx_protocol",
                backup="eastmoney",
            )
        ],
        sources={"eastmoney": True, "tdx_protocol": True, "sina": True},
    )
    init_data_layout(cfg)
    return cfg


def _bar_frame(symbols: list[str], d: date, *, volume: int = 100) -> pl.DataFrame:
    n = len(symbols)
    return with_provenance(
        pl.DataFrame(
            {
                "symbol": symbols,
                "trade_date": [d] * n,
                "open": [10.0] * n,
                "high": [11.0] * n,
                "low": [9.0] * n,
                "close": [10.5] * n,
                "volume": [volume] * n,
                "amount": [1000.0] * n,
            }
        ),
        source="tdx_protocol",
        data_version="v1",
    )


def test_fetch_daily_bars_clist_stamps_trade_date(monkeypatch):
    raw = [
        {
            "f12": "600519",
            "f13": 1,
            "f17": 100.0,
            "f15": 102.0,
            "f16": 99.0,
            "f2": 101.0,
            "f5": 1000,
            "f6": 1e6,
        },
        {
            "f12": "600519",
            "f13": 1,
            "f17": 100.0,
            "f15": 102.0,
            "f16": 99.0,
            "f2": 101.0,
            "f5": 1000,
            "f6": 1e6,
        },
        {
            "f12": "000001",
            "f13": 0,
            "f17": 10.0,
            "f15": 11.0,
            "f16": 9.0,
            "f2": 10.5,
            "f5": 2000,
            "f6": 2e6,
        },
    ]

    class _Client:
        def close(self):
            pass

    monkeypatch.setattr(
        "cnequity.adapters.eastmoney.bars.fetch_clist_pages",
        lambda client, fields: raw,
    )
    monkeypatch.setattr(
        "cnequity.adapters.eastmoney.bars.EastMoneyClient",
        lambda **kwargs: _Client(),
    )
    tip = date(2026, 7, 24)
    df = fetch_daily_bars_clist(tip, symbols={"600519.SH"})
    assert df.height == 1
    assert df["symbol"].to_list() == ["600519.SH"]
    assert df["trade_date"].to_list() == [tip]
    assert df["open"].to_list() == [100.0]
    assert df["high"].to_list() == [102.0]
    assert df["low"].to_list() == [99.0]
    assert df["close"].to_list() == [101.0]


def test_fetch_daily_bars_clist_drops_invalid_ohlcv_instead_of_zero(monkeypatch):
    monkeypatch.setattr(
        "cnequity.adapters.eastmoney.bars.fetch_clist_pages",
        lambda client, fields: [{}],
    )
    monkeypatch.setattr(
        "cnequity.adapters.eastmoney.bars.clist_rows_to_symbols",
        lambda rows: [
            (
                "600519.SH",
                {"f17": "bad", "f15": 102.0, "f16": 99.0, "f2": 101.0, "f5": 1000},
            )
        ],
    )

    class _Client:
        def close(self):
            pass

    df = fetch_daily_bars_clist(date(2026, 7, 24), client=_Client())
    assert df.is_empty()


def test_fetch_daily_bars_clist_drops_zero_price_placeholder(monkeypatch):
    monkeypatch.setattr(
        "cnequity.adapters.eastmoney.bars.fetch_clist_pages",
        lambda client, fields: [{}],
    )
    monkeypatch.setattr(
        "cnequity.adapters.eastmoney.bars.clist_rows_to_symbols",
        lambda rows: [
            (
                "600519.SH",
                {"f17": 0.0, "f15": 0.0, "f16": 0.0, "f2": 0.0, "f5": 1000},
            )
        ],
    )

    class _Client:
        def close(self):
            pass

    assert fetch_daily_bars_clist(date(2026, 7, 24), client=_Client()).is_empty()


def test_fetch_daily_bars_clist_drops_invalid_volume(monkeypatch):
    monkeypatch.setattr(
        "cnequity.adapters.eastmoney.bars.fetch_clist_pages",
        lambda client, fields: [{}],
    )
    monkeypatch.setattr(
        "cnequity.adapters.eastmoney.bars.clist_rows_to_symbols",
        lambda rows: [
            (
                "600519.SH",
                {
                    "f17": 100.0,
                    "f15": 102.0,
                    "f16": 99.0,
                    "f2": 101.0,
                    "f5": 1e300,
                },
            )
        ],
    )

    class _Client:
        def close(self):
            pass

    assert fetch_daily_bars_clist(date(2026, 7, 24), client=_Client()).is_empty()


def test_fetch_daily_bars_clist_closes_owned_client_on_failure(monkeypatch):
    from cnequity.adapters.eastmoney import bars as em_bars

    created = []

    class _OwnedClient:
        closed = False

        def close(self):
            self.closed = True

    def _factory(**kwargs):
        client = _OwnedClient()
        created.append(client)
        return client

    monkeypatch.setattr(em_bars, "EastMoneyClient", _factory)
    monkeypatch.setattr(
        em_bars,
        "fetch_clist_pages",
        lambda client, fields: (_ for _ in ()).throw(RuntimeError("clist down")),
    )
    with pytest.raises(RuntimeError, match="clist down"):
        fetch_daily_bars_clist(date(2026, 7, 24))
    assert created[0].closed is True


def test_tip_gapfill_writes_only_missing_keys(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    manifest = Manifest(cfg.manifest_path)
    run_id = manifest.start_run("daily:core")
    tip = date(2026, 7, 24)
    # TDX already staged one symbol.
    StagingWriter(cfg.staging_root).write_batch(
        "daily_bars", run_id, "tdx-batch-0", _bar_frame(["600519.SH"], tip)
    )

    clist = pl.DataFrame(
        {
            "symbol": ["600519.SH", "000001.SZ", "600000.SH"],
            "trade_date": [tip, tip, tip],
            "open": [1.0, 2.0, 3.0],
            "high": [1.0, 2.0, 3.0],
            "low": [1.0, 2.0, 3.0],
            "close": [1.0, 2.0, 3.0],
            "volume": [10, 20, 30],
            "amount": [100.0, 200.0, 300.0],
        }
    )
    monkeypatch.setattr(
        "cnequity.adapters.eastmoney.bars.fetch_daily_bars_clist",
        lambda trade_date, symbols=None, client=None, config=None: clist,
    )

    out = _gapfill_tip_via_clist(
        cfg,
        tip,
        run_id,
        expected_symbols=["600519.SH", "000001.SZ", "600000.SH"],
    )
    assert out["filled"] is True
    assert out["rows_written"] == 2
    staged = _staged_daily_bar_symbols(cfg, run_id, tip)
    assert staged == {"600519.SH", "000001.SZ", "600000.SH"}
    # Gap-fill batch must not re-stage the TDX key.
    gap_files = list((cfg.staging_root / "daily_bars" / f"run_id={run_id}").rglob("*.parquet"))
    gap_only = [f for f in gap_files if "em-clist-gapfill" in str(f)]
    assert gap_only
    gap_df = pl.read_parquet(gap_only[0])
    assert set(gap_df["symbol"].to_list()) == {"000001.SZ", "600000.SH"}
    assert gap_df["source"].unique().to_list() == ["eastmoney"]


def test_tip_tdx_fail_clist_recovers_step(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    manifest = Manifest(cfg.manifest_path)
    run_id = manifest.start_run("daily:core")
    tip = date(2026, 7, 24)
    manifest.start_batch(
        run_id,
        "tdx-batch-0",
        task_id="daily_bars",
        dataset="daily_bars",
        symbols=["600519.SH"],
        window_start=tip.isoformat(),
        window_end=tip.isoformat(),
    )
    manifest.finish_batch(run_id, "tdx-batch-0", "failed", error_message="TDX empty")
    monkeypatch.setattr(
        "cnequity.adapters.eastmoney.bars.fetch_daily_bars_clist",
        lambda trade_date, symbols=None, client=None, config=None: pl.DataFrame(
            {
                "symbol": ["600519.SH"],
                "trade_date": [tip],
                "open": [10.0],
                "high": [11.0],
                "low": [9.0],
                "close": [10.5],
                "volume": [100],
                "amount": [1000.0],
            }
        ),
    )
    result = _finish_daily_bars(
        cfg,
        tip,
        run_id,
        start=tip,
        end=tip,
        expected_tdx_symbols=["600519.SH"],
        tdx_result={
            "rows_read": 0,
            "rows_written": 0,
            "had_error": True,
            "failed_symbols": ["600519.SH"],
        },
        sina_result=None,
    )
    assert result["rows_written"] == 1
    assert any(
        f["check"] == "daily_bars_clist_gapfill"
        for f in result["context_updates"]["audit_findings"]
    )
    assert manifest.get_batch(run_id, "tdx-batch-0")["status"] == "success"


def test_clean_primary_tip_still_captures_daily_peer_snapshot(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    cfg.failover_datasets[0].snapshot_cadence = "daily"
    run_id = Manifest(cfg.manifest_path).start_run("daily:core")
    tip = date(2026, 7, 24)
    StagingWriter(cfg.staging_root).write_batch(
        "daily_bars", run_id, "tdx-batch-0", _bar_frame(["600519.SH"], tip)
    )
    calls = []
    monkeypatch.setattr(
        "cnequity.quality.failover.snapshot_daily_bars_clist",
        lambda *args, **kwargs: calls.append(kwargs) or pl.DataFrame(),
    )

    result = _finish_daily_bars(
        cfg,
        tip,
        run_id,
        start=tip,
        end=tip,
        expected_tdx_symbols=["600519.SH"],
        tdx_result={"rows_read": 1, "rows_written": 1},
        sina_result=None,
    )

    assert result["rows_written"] == 1
    assert calls and calls[0]["symbols"] == ["600519.SH"]
    finding = next(
        item
        for item in result["context_updates"]["audit_findings"]
        if item["check"] == "backup_snapshot_unavailable"
    )
    assert finding["peer_unavailable"] is True
    assert finding["retryable"] is True


def test_peer_snapshot_failure_is_observable_but_does_not_invalidate_primary(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    cfg.failover_datasets[0].snapshot_cadence = "daily"
    run_id = Manifest(cfg.manifest_path).start_run("daily:core")
    tip = date(2026, 7, 24)
    StagingWriter(cfg.staging_root).write_batch(
        "daily_bars", run_id, "tdx-batch-0", _bar_frame(["600519.SH"], tip)
    )
    monkeypatch.setattr(
        "cnequity.quality.failover.snapshot_daily_bars_clist",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("peer down")),
    )

    result = _finish_daily_bars(
        cfg,
        tip,
        run_id,
        start=tip,
        end=tip,
        expected_tdx_symbols=["600519.SH"],
        tdx_result={"rows_read": 1, "rows_written": 1},
        sina_result=None,
    )

    findings = result["context_updates"]["audit_findings"]
    finding = next(item for item in findings if item["check"] == "backup_snapshot_unavailable")
    assert finding["severity"] == "warning"
    assert finding["peer_unavailable"] is True
    assert finding["retryable"] is True


def test_historical_tip_backfill_validates_staged_end_not_job_as_of(tmp_path, monkeypatch):
    """A weekend/as-of date must not hide a successfully staged past session."""
    cfg = _cfg(tmp_path)
    manifest = Manifest(cfg.manifest_path)
    run_id = manifest.start_run("backfill")
    fetched = date(2026, 8, 21)
    StagingWriter(cfg.staging_root).write_batch(
        "daily_bars", run_id, "tdx-batch-0", _bar_frame(["600519.SH"], fetched)
    )
    monkeypatch.setattr(
        "cnequity.steps.bars._gapfill_tip_via_clist",
        lambda *args, **kwargs: {"rows_read": 0, "rows_written": 0, "audit_findings": []},
    )

    result = _finish_daily_bars(
        cfg,
        date(2026, 8, 23),
        run_id,
        start=fetched,
        end=fetched,
        expected_tdx_symbols=["600519.SH"],
        tdx_result={"rows_read": 1, "rows_written": 1},
        sina_result=None,
    )

    assert result["rows_written"] == 1


def test_tip_clist_leftover_uses_kline(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    run_id = Manifest(cfg.manifest_path).start_run("daily:core")
    tip = date(2026, 8, 18)
    expected = ["600519.SH", "161728.SZ"]

    monkeypatch.setattr(
        "cnequity.adapters.eastmoney.bars.fetch_daily_bars_clist",
        lambda *args, **kwargs: _bar_frame([expected[0]], tip),
    )
    kline_calls: list[list[str]] = []

    def kline(symbols, start, end, **kwargs):
        kline_calls.append(list(symbols))
        return _bar_frame(list(symbols), tip)

    monkeypatch.setattr("cnequity.adapters.eastmoney.bars.fetch_daily_bars", kline)

    result = _finish_daily_bars(
        cfg,
        tip,
        run_id,
        start=tip,
        end=tip,
        expected_tdx_symbols=expected,
        tdx_result={
            "rows_read": 0,
            "rows_written": 0,
            "had_error": True,
            "failed_symbols": expected,
        },
        sina_result=None,
    )

    assert kline_calls == [[expected[1]]]
    assert result["rows_written"] == 2
    assert _staged_daily_bar_symbols(cfg, run_id, tip) == set(expected)


def test_historical_tip_retry_uses_kline_not_live_clist(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    manifest = Manifest(cfg.manifest_path)
    run_id = manifest.start_run("daily:core")
    historical = date(2026, 8, 18)
    current = date(2026, 8, 19)
    batch_id = "2026-08-18_2026-08-18-batch-0"
    manifest.start_batch(
        run_id,
        batch_id,
        task_id="daily_bars",
        dataset="daily_bars",
        symbols=["161728.SZ"],
        window_start=historical.isoformat(),
        window_end=historical.isoformat(),
    )
    manifest.finish_batch(run_id, batch_id, "failed", error_message="TDX empty")

    calls: list[tuple] = []

    def no_clist(*args, **kwargs):
        calls.append(("clist", args, kwargs))
        return pl.DataFrame()

    def kline(symbols, start, end, **kwargs):
        calls.append(("kline", list(symbols), start, end))
        return _bar_frame(list(symbols), historical)

    monkeypatch.setattr("cnequity.adapters.eastmoney.bars.fetch_daily_bars_clist", no_clist)
    monkeypatch.setattr("cnequity.adapters.eastmoney.bars.fetch_daily_bars", kline)

    result = _finish_daily_bars(
        cfg,
        current,
        run_id,
        start=historical,
        end=historical,
        expected_tdx_symbols=["161728.SZ"],
        tdx_result={
            "rows_read": 0,
            "rows_written": 0,
            "had_error": True,
            "failed_symbols": ["161728.SZ"],
        },
        sina_result=None,
    )

    assert "clist" not in [call[0] for call in calls]
    assert ("kline", ["161728.SZ"], historical, historical) in calls
    assert result["rows_written"] == 1
    assert _staged_daily_bar_symbols(cfg, run_id, historical) == {"161728.SZ"}
    assert manifest.get_batch(run_id, batch_id)["status"] == "success"


def test_retry_routes_non_tdx_symbols_to_fallback(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    run_id = Manifest(cfg.manifest_path).start_run("daily:core")
    calls: list[tuple[list[str], date, date, str]] = []
    captured: dict = {}

    def fake_fallback(config, symbols, start, end, run_id, *, batch_prefix):
        calls.append((symbols, start, end, batch_prefix))
        return {"rows_read": 0, "rows_written": 0, "failed_symbols": 0}

    def fake_finish(*args, **kwargs):
        captured.update(kwargs)
        return {"rows_read": 0, "rows_written": 0}

    monkeypatch.setattr("cnequity.steps.bars.fetch_bars_via_sina", fake_fallback)
    monkeypatch.setattr("cnequity.steps.bars.fetch_daily_bars_parallel", pytest.fail)
    monkeypatch.setattr("cnequity.steps.bars._finish_daily_bars", fake_finish)
    monkeypatch.setattr("cnequity.steps.bars._merge_ownership_result", lambda out, *args: out)

    from cnequity.steps.bars import step_daily_bars

    step_daily_bars(
        cfg,
        date(2024, 6, 28),
        run_id,
        {"_retry_batch_specs": [("retry-0", ["920001.BJ"], date(2024, 6, 27), date(2024, 6, 28))]},
    )

    assert calls == [(["920001.BJ"], date(2024, 6, 27), date(2024, 6, 28), "retry-0-sina")]
    assert captured["expected_tdx_symbols"] == []
    assert captured["expected_fallback_symbols"] == ["920001.BJ"]


def test_tip_total_loss_still_raises(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    run_id = Manifest(cfg.manifest_path).start_run("daily:core")
    tip = date(2026, 7, 24)
    monkeypatch.setattr(
        "cnequity.adapters.eastmoney.bars.fetch_daily_bars_clist",
        lambda trade_date, symbols=None, client=None, config=None: pl.DataFrame(),
    )
    monkeypatch.setattr(
        "cnequity.adapters.eastmoney.bars.fetch_daily_bars",
        lambda *args, **kwargs: pl.DataFrame(),
    )
    monkeypatch.setattr(
        "cnequity.steps.bars._gapfill_tip_via_sina",
        lambda *args, **kwargs: {"rows_read": 0, "rows_written": 0, "complete": False},
    )
    with pytest.raises(RuntimeError, match="produced no staged tip rows"):
        _finish_daily_bars(
            cfg,
            tip,
            run_id,
            start=tip,
            end=tip,
            expected_tdx_symbols=["600519.SH", "000001.SZ"],
            tdx_result={
                "rows_read": 0,
                "rows_written": 0,
                "had_error": True,
                "failed_symbols": ["600519.SH", "000001.SZ"],
            },
            sina_result=None,
        )


def test_tip_partial_miss_after_gapfill_stays_strict_for_unknown_symbol(tmp_path, monkeypatch):
    # A market-sized response cannot prove that one remaining symbol had no
    # data.  Without listing/status/source-empty evidence the unknown key must
    # keep the checkpoint blocked; there is no market-level 5% allowance.
    cfg = _cfg(tmp_path)
    manifest = Manifest(cfg.manifest_path)
    run_id = manifest.start_run("daily:core")
    tip = date(2026, 7, 24)
    manifest.start_batch(
        run_id,
        "tdx-partial",
        task_id="daily_bars",
        dataset="daily_bars",
        symbols=["600519.SH", "000001.SZ"],
    )
    manifest.finish_batch(run_id, "tdx-partial", "failed", error_message="TDX partial")
    monkeypatch.setattr(
        "cnequity.adapters.eastmoney.bars.fetch_daily_bars_clist",
        lambda trade_date, symbols=None, client=None, config=None: pl.DataFrame(
            {
                "symbol": ["600519.SH"],
                "trade_date": [tip],
                "open": [10.0],
                "high": [11.0],
                "low": [9.0],
                "close": [10.5],
                "volume": [100],
                "amount": [1000.0],
            }
        ),
    )
    monkeypatch.setattr(
        "cnequity.adapters.eastmoney.bars.fetch_daily_bars",
        lambda *args, **kwargs: pl.DataFrame(),
    )
    monkeypatch.setattr(
        "cnequity.steps.bars._gapfill_tip_via_sina",
        lambda *args, **kwargs: {"rows_read": 0, "rows_written": 0, "complete": False},
    )
    with pytest.raises(RuntimeError, match="refusing to checkpoint") as exc_info:
        _finish_daily_bars(
            cfg,
            tip,
            run_id,
            start=tip,
            end=tip,
            expected_tdx_symbols=["600519.SH", "000001.SZ"],
            tdx_result={
                "rows_read": 0,
                "rows_written": 0,
                "had_error": True,
                "failed_symbols": ["600519.SH", "000001.SZ"],
            },
            sina_result=None,
        )

    assert f"000001.SZ@{tip.isoformat()}" in str(exc_info.value)
    assert manifest.get_batch(run_id, "tdx-partial")["status"] == "failed"


def test_tip_large_partial_miss_blocks_checkpoint(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    run_id = Manifest(cfg.manifest_path).start_run("daily:core")
    tip = date(2026, 7, 24)
    expected = [f"600{i:03d}.SH" for i in range(10)]
    monkeypatch.setattr(
        "cnequity.adapters.eastmoney.bars.fetch_daily_bars_clist",
        lambda trade_date, symbols=None, client=None, config=None: pl.DataFrame(
            {
                "symbol": [expected[0]],
                "trade_date": [tip],
                "open": [10.0],
                "high": [11.0],
                "low": [9.0],
                "close": [10.5],
                "volume": [100],
                "amount": [1000.0],
            }
        ),
    )
    monkeypatch.setattr(
        "cnequity.adapters.eastmoney.bars.fetch_daily_bars",
        lambda *args, **kwargs: pl.DataFrame(),
    )
    monkeypatch.setattr(
        "cnequity.steps.bars._gapfill_tip_via_sina",
        lambda *args, **kwargs: {"rows_read": 0, "rows_written": 0, "complete": False},
    )
    with pytest.raises(RuntimeError, match="refusing to checkpoint") as exc_info:
        _finish_daily_bars(
            cfg,
            tip,
            run_id,
            start=tip,
            end=tip,
            expected_tdx_symbols=expected,
            tdx_result={
                "rows_read": 0,
                "rows_written": 0,
                "had_error": True,
                "failed_symbols": expected,
            },
            sina_result=None,
        )

    assert f"{expected[1]}@{tip.isoformat()}" in str(exc_info.value)


def test_multiday_uses_kline_not_clist(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    run_id = Manifest(cfg.manifest_path).start_run("backfill")
    start, end = date(2024, 6, 20), date(2024, 6, 28)
    clist_calls: list = []
    kline_calls: list = []

    def _clist(*a, **k):
        clist_calls.append(1)
        return pl.DataFrame()

    def _kline(symbols, s, e, **k):
        kline_calls.append(list(symbols))
        days = [
            date(2024, 6, 20),
            date(2024, 6, 21),
            date(2024, 6, 24),
            date(2024, 6, 25),
            date(2024, 6, 26),
            date(2024, 6, 27),
            date(2024, 6, 28),
        ]
        rows = [
            {
                "symbol": symbol,
                "trade_date": day,
                "open": 1.0,
                "high": 1.0,
                "low": 1.0,
                "close": 1.0,
                "volume": 1,
                "amount": 1.0,
            }
            for symbol in symbols
            for day in days
        ]
        return pl.DataFrame(rows)

    monkeypatch.setattr("cnequity.adapters.eastmoney.bars.fetch_daily_bars_clist", _clist)
    monkeypatch.setattr("cnequity.adapters.eastmoney.bars.fetch_daily_bars", _kline)
    monkeypatch.setattr(
        "cnequity.steps.bars.fetch_bars_via_sina",
        lambda *args, **kwargs: {
            "rows_read": 0,
            "rows_written": 0,
            "failed_symbols": 1,
            "failed_symbol_names": ["600519.SH"],
            "empty_symbol_names": [],
        },
    )

    result = _finish_daily_bars(
        cfg,
        end,
        run_id,
        start=start,
        end=end,
        expected_tdx_symbols=["600519.SH"],
        tdx_result={
            "rows_read": 0,
            "rows_written": 0,
            "had_error": True,
            "failed_symbols": ["600519.SH"],
        },
        sina_result=None,
    )
    assert clist_calls == []
    assert kline_calls == [["600519.SH"]]
    assert result["rows_written"] == 7


def test_multiday_partial_miss_after_gapfill_stays_strict_for_unknown_symbol(tmp_path, monkeypatch):
    # Even a large multi-day response cannot certify one unresolved symbol
    # without symbol-level metadata/status/empty evidence.
    cfg = _cfg(tmp_path)
    run_id = Manifest(cfg.manifest_path).start_run("daily:core")
    start, end = date(2024, 6, 20), date(2024, 6, 21)
    days = [date(2024, 6, 20), date(2024, 6, 21)]
    expected = [f"600{i:03d}.SH" for i in range(20)]
    missing = expected[-1]

    def _kline(symbols, s, e, **k):
        rows = [
            {
                "symbol": symbol,
                "trade_date": day,
                "open": 1.0,
                "high": 1.0,
                "low": 1.0,
                "close": 1.0,
                "volume": 1,
                "amount": 1.0,
            }
            for symbol in symbols
            if symbol != missing
            for day in days
        ]
        return pl.DataFrame(rows)

    monkeypatch.setattr("cnequity.adapters.eastmoney.bars.fetch_daily_bars", _kline)
    monkeypatch.setattr(
        "cnequity.steps.bars.fetch_bars_via_sina",
        lambda *args, **kwargs: {
            "rows_read": 0,
            "rows_written": 0,
            "failed_symbols": len(expected),
            "failed_symbol_names": expected,
            "empty_symbol_names": [],
        },
    )

    with pytest.raises(RuntimeError, match="refusing to checkpoint") as exc_info:
        _finish_daily_bars(
            cfg,
            end,
            run_id,
            start=start,
            end=end,
            expected_tdx_symbols=expected,
            tdx_result={
                "rows_read": 0,
                "rows_written": 0,
                "had_error": True,
                "failed_symbols": expected,
            },
            sina_result=None,
        )

    assert f"{missing}@{end.isoformat()}" in str(exc_info.value)


def test_multiday_szse_fund_no_trade_is_confirmed_per_missing_date(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    run_id = Manifest(cfg.manifest_path).start_run("backfill")
    start, end = date(2024, 6, 20), date(2024, 6, 21)
    symbol = "160212.SZ"
    StagingWriter(cfg.staging_root).write_batch(
        "daily_bars", run_id, "tdx-control", _bar_frame([symbol], start)
    )
    monkeypatch.setattr(
        "cnequity.adapters.eastmoney.bars.fetch_daily_bars",
        lambda *args, **kwargs: pl.DataFrame(),
    )
    monkeypatch.setattr(
        "cnequity.steps.bars.fetch_bars_via_sina",
        lambda *args, **kwargs: {
            "rows_read": 0,
            "rows_written": 0,
            "failed_symbols": 1,
            "failed_symbol_names": [symbol],
            "empty_symbol_names": [],
        },
    )
    monkeypatch.setattr(
        "cnequity.adapters.exchange.daily_quotes.fetch_szse_fund_history",
        lambda *args, **kwargs: _bar_frame([symbol], start).drop(
            ["source", "data_version", "fetched_at"]
        ),
    )

    result = _finish_daily_bars(
        cfg,
        end,
        run_id,
        start=start,
        end=end,
        expected_tdx_symbols=[symbol],
        tdx_result={"rows_read": 1, "rows_written": 1, "failed_symbols": []},
        sina_result=None,
    )

    assert result["rows_written"] == 1
    findings = result["context_updates"]["audit_findings"]
    assert any(f["check"] == "daily_bars_szse_confirmed_no_trade" for f in findings)


def test_multiday_szse_fund_history_stages_a_missing_official_bar(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    run_id = Manifest(cfg.manifest_path).start_run("backfill")
    start, end = date(2024, 6, 20), date(2024, 6, 21)
    symbol = "160212.SZ"
    StagingWriter(cfg.staging_root).write_batch(
        "daily_bars", run_id, "tdx-control", _bar_frame([symbol], start)
    )
    monkeypatch.setattr(
        "cnequity.adapters.eastmoney.bars.fetch_daily_bars",
        lambda *args, **kwargs: pl.DataFrame(),
    )
    monkeypatch.setattr(
        "cnequity.steps.bars.fetch_bars_via_sina",
        lambda *args, **kwargs: {
            "rows_read": 0,
            "rows_written": 0,
            "failed_symbols": 1,
            "failed_symbol_names": [symbol],
            "empty_symbol_names": [],
        },
    )
    history = pl.concat([_bar_frame([symbol], start), _bar_frame([symbol], end)]).drop(
        ["source", "data_version", "fetched_at"]
    )
    monkeypatch.setattr(
        "cnequity.adapters.exchange.daily_quotes.fetch_szse_fund_history",
        lambda *args, **kwargs: history,
    )

    result = _finish_daily_bars(
        cfg,
        end,
        run_id,
        start=start,
        end=end,
        expected_tdx_symbols=[symbol],
        tdx_result={"rows_read": 1, "rows_written": 1, "failed_symbols": []},
        sina_result=None,
    )

    assert result["rows_written"] == 2
    staged = pl.concat(
        [
            pl.read_parquet(path)
            for path in StagingWriter(cfg.staging_root).list_run_files("daily_bars", run_id)
        ]
    )
    official = staged.filter((pl.col("symbol") == symbol) & (pl.col("trade_date") == end))
    assert official.select("source").item() == "szse"


def test_multiday_szse_history_without_control_stays_strict(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    run_id = Manifest(cfg.manifest_path).start_run("backfill")
    start, end = date(2024, 6, 20), date(2024, 6, 21)
    symbol = "160212.SZ"
    monkeypatch.setattr(
        "cnequity.adapters.eastmoney.bars.fetch_daily_bars",
        lambda *args, **kwargs: pl.DataFrame(),
    )
    monkeypatch.setattr(
        "cnequity.steps.bars.fetch_bars_via_sina",
        lambda *args, **kwargs: {
            "rows_read": 0,
            "rows_written": 0,
            "failed_symbols": 1,
            "failed_symbol_names": [symbol],
            "empty_symbol_names": [],
        },
    )
    monkeypatch.setattr(
        "cnequity.adapters.exchange.daily_quotes.fetch_szse_fund_history",
        lambda *args, **kwargs: pl.DataFrame(),
    )

    with pytest.raises(RuntimeError, match="refusing to checkpoint"):
        _finish_daily_bars(
            cfg,
            end,
            run_id,
            start=start,
            end=end,
            expected_tdx_symbols=[symbol],
            tdx_result={"rows_read": 0, "rows_written": 0, "failed_symbols": [symbol]},
            sina_result=None,
        )


def test_multiday_single_symbol_scope_still_raises(tmp_path, monkeypatch):
    # The tolerance above must not apply to a narrow explicit scope — a
    # scoped backfill or a `cne retry` batch of just one or two symbols,
    # where every symbol is the whole ask and "tolerate at least 1" would
    # make the run silently report success with nothing staged.
    cfg = _cfg(tmp_path)
    run_id = Manifest(cfg.manifest_path).start_run("daily:core")
    start, end = date(2024, 6, 20), date(2024, 6, 21)

    monkeypatch.setattr(
        "cnequity.adapters.eastmoney.bars.fetch_daily_bars",
        lambda *args, **kwargs: pl.DataFrame(),
    )
    monkeypatch.setattr(
        "cnequity.steps.bars.fetch_bars_via_sina",
        lambda *args, **kwargs: {
            "rows_read": 0,
            "rows_written": 0,
            "failed_symbols": 1,
            "failed_symbol_names": ["600519.SH"],
            "empty_symbol_names": [],
        },
    )

    with pytest.raises(RuntimeError, match="refusing to checkpoint"):
        _finish_daily_bars(
            cfg,
            end,
            run_id,
            start=start,
            end=end,
            expected_tdx_symbols=["600519.SH"],
            tdx_result={
                "rows_read": 0,
                "rows_written": 0,
                "had_error": True,
                "failed_symbols": ["600519.SH"],
            },
            sina_result=None,
        )


def test_multiday_large_partial_miss_blocks_checkpoint(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    run_id = Manifest(cfg.manifest_path).start_run("daily:core")
    start, end = date(2024, 6, 20), date(2024, 6, 21)
    days = [date(2024, 6, 20), date(2024, 6, 21)]
    expected = [f"600{i:03d}.SH" for i in range(10)]

    def _kline(symbols, s, e, **k):
        rows = [
            {
                "symbol": symbol,
                "trade_date": day,
                "open": 1.0,
                "high": 1.0,
                "low": 1.0,
                "close": 1.0,
                "volume": 1,
                "amount": 1.0,
            }
            for symbol in symbols
            if symbol == expected[0]
            for day in days
        ]
        return pl.DataFrame(rows)

    monkeypatch.setattr("cnequity.adapters.eastmoney.bars.fetch_daily_bars", _kline)
    monkeypatch.setattr(
        "cnequity.steps.bars.fetch_bars_via_sina",
        lambda *args, **kwargs: {
            "rows_read": 0,
            "rows_written": 0,
            "failed_symbols": len(expected),
            "failed_symbol_names": expected,
            "empty_symbol_names": [],
        },
    )

    with pytest.raises(RuntimeError, match="refusing to checkpoint"):
        _finish_daily_bars(
            cfg,
            end,
            run_id,
            start=start,
            end=end,
            expected_tdx_symbols=expected,
            tdx_result={
                "rows_read": 0,
                "rows_written": 0,
                "had_error": True,
                "failed_symbols": expected,
            },
            sina_result=None,
        )


def test_multiday_accepts_explicit_no_data_from_fallback(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    run_id = Manifest(cfg.manifest_path).start_run("backfill")
    start, end = date(2024, 6, 20), date(2024, 6, 28)

    monkeypatch.setattr(
        "cnequity.steps.bars.fetch_bars_via_sina",
        lambda *args, **kwargs: {
            "rows_read": 0,
            "rows_written": 0,
            "failed_symbols": 1,
            "failed_symbol_names": ["561833.SH"],
            "empty_symbol_names": ["561833.SH"],
        },
    )
    monkeypatch.setattr(
        "cnequity.adapters.eastmoney.bars.fetch_daily_bars",
        lambda *args, **kwargs: pl.DataFrame(),
    )

    result = _finish_daily_bars(
        cfg,
        end,
        run_id,
        start=start,
        end=end,
        expected_tdx_symbols=["561833.SH"],
        tdx_result={
            "rows_read": 0,
            "rows_written": 0,
            "had_error": True,
            "failed_symbols": ["561833.SH"],
        },
        sina_result=None,
    )

    assert result["rows_written"] == 0
    findings = result["context_updates"]["audit_findings"]
    assert any(f["check"] == "daily_bars_sina_expected_no_data" for f in findings)


def test_resolve_recovered_daily_batches_does_not_close_unrelated_failures(tmp_path):
    cfg = _cfg(tmp_path)
    manifest = Manifest(cfg.manifest_path)
    run_id = manifest.start_run("backfill")
    manifest.start_batch(
        run_id,
        "batch-a",
        "daily_bars",
        "daily_bars",
        symbols=["561833.SH"],
    )
    manifest.start_batch(
        run_id,
        "batch-b",
        "daily_bars",
        "daily_bars",
        symbols=["561834.SH"],
    )
    manifest.finish_batch(run_id, "batch-a", "failed", error_message="TDX empty")
    manifest.finish_batch(run_id, "batch-b", "failed", error_message="TDX empty")

    _resolve_recovered_daily_batches(cfg, run_id, resolved_symbols={"561833.SH"})

    batches = {row["batch_id"]: row for row in manifest.get_batches_for_run(run_id)}
    assert batches["batch-a"]["status"] == "success"
    assert batches["batch-b"]["status"] == "failed"


def test_multiday_partial_symbol_is_gapfilled_without_overwriting_primary_rows(
    tmp_path, monkeypatch
):
    cfg = _cfg(tmp_path)
    run_id = Manifest(cfg.manifest_path).start_run("backfill")
    start, end = date(2024, 6, 20), date(2024, 6, 24)
    symbol = "600519.SH"
    StagingWriter(cfg.staging_root).write_batch(
        "daily_bars", run_id, "tdx-start", _bar_frame([symbol], start)
    )
    StagingWriter(cfg.staging_root).write_batch(
        "daily_bars", run_id, "tdx-end", _bar_frame([symbol], end)
    )
    assert _staged_daily_bar_partial_symbols(cfg, run_id, [symbol], start, end) == {symbol}

    def _kline(symbols, s, e, **k):
        days = [date(2024, 6, 20), date(2024, 6, 21), date(2024, 6, 24)]
        return pl.concat([_bar_frame(symbols, day) for day in days])

    monkeypatch.setattr("cnequity.adapters.eastmoney.bars.fetch_daily_bars", _kline)
    monkeypatch.setattr(
        "cnequity.steps.bars.fetch_bars_via_sina",
        lambda *args, **kwargs: {
            "rows_read": 0,
            "rows_written": 0,
            "failed_symbols": 1,
            "failed_symbol_names": [symbol],
            "empty_symbol_names": [],
        },
    )
    result = _finish_daily_bars(
        cfg,
        end,
        run_id,
        start=start,
        end=end,
        expected_tdx_symbols=[symbol],
        tdx_result={
            "rows_read": 2,
            "rows_written": 2,
            "had_error": False,
            "failed_symbols": [],
        },
        sina_result=None,
    )
    assert result["rows_written"] == 3  # two primary rows + one recovered interior day
    assert _staged_daily_bar_symbols(cfg, run_id, None) == {symbol}


def test_multiday_partial_gap_uses_sina_after_eastmoney(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    run_id = Manifest(cfg.manifest_path).start_run("backfill")
    start, end = date(2024, 6, 20), date(2024, 6, 24)
    missing_day = date(2024, 6, 21)
    symbol = "600519.SH"
    StagingWriter(cfg.staging_root).write_batch(
        "daily_bars", run_id, "tdx-start", _bar_frame([symbol], start)
    )
    StagingWriter(cfg.staging_root).write_batch(
        "daily_bars", run_id, "tdx-end", _bar_frame([symbol], end)
    )
    assert _staged_daily_bar_symbols(cfg, run_id, end) == {symbol}
    calls: list[tuple[str, list[str]]] = []

    def _kline(symbols, s, e, **k):
        calls.append(("eastmoney", list(symbols)))
        return pl.DataFrame()

    def _sina(config, symbols, s, e, target_run_id, **kwargs):
        calls.append(("sina", list(symbols)))
        assert kwargs["only_missing_keys"] == {(symbol, missing_day)}
        StagingWriter(config.staging_root).write_batch(
            "daily_bars", target_run_id, "sina-final", _bar_frame([symbol], missing_day)
        )
        return {"rows_read": 1, "rows_written": 1}

    monkeypatch.setattr("cnequity.adapters.eastmoney.bars.fetch_daily_bars", _kline)
    monkeypatch.setattr("cnequity.steps.bars.fetch_bars_via_sina", _sina)

    result = _finish_daily_bars(
        cfg,
        end,
        run_id,
        start=start,
        end=end,
        expected_tdx_symbols=[symbol],
        tdx_result={"rows_read": 2, "rows_written": 2, "failed_symbols": []},
        sina_result=None,
    )

    assert calls == [("eastmoney", [symbol]), ("sina", [symbol])]
    assert result["rows_written"] == 3


def test_multiday_partial_symbol_detects_leading_session_gap(tmp_path):
    cfg = _cfg(tmp_path)
    run_id = Manifest(cfg.manifest_path).start_run("backfill")
    start, end = date(2024, 6, 20), date(2024, 6, 24)
    symbol = "600519.SH"
    StagingWriter(cfg.staging_root).write_batch(
        "daily_bars", run_id, "tdx-late-start", _bar_frame([symbol], date(2024, 6, 21))
    )
    StagingWriter(cfg.staging_root).write_batch(
        "daily_bars", run_id, "tdx-end", _bar_frame([symbol], end)
    )

    assert _staged_daily_bar_partial_symbols(cfg, run_id, [symbol], start, end) == {symbol}


def test_multiday_partial_symbol_respects_listing_and_delisting_edges(tmp_path):
    cfg = _cfg(tmp_path)
    run_id = Manifest(cfg.manifest_path).start_run("backfill")
    start, end = date(2024, 6, 20), date(2024, 6, 24)
    symbol = "600519.SH"
    instruments = cfg.curated_root / "instruments"
    instruments.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": [symbol],
            "list_date": [date(2024, 6, 21)],
            "delist_date": [date(2024, 6, 24)],
        }
    ).write_parquet(instruments / "part-merged.parquet")
    StagingWriter(cfg.staging_root).write_batch(
        "daily_bars", run_id, "tdx-listing", _bar_frame([symbol], date(2024, 6, 21))
    )
    StagingWriter(cfg.staging_root).write_batch(
        "daily_bars", run_id, "tdx-delisting", _bar_frame([symbol], end)
    )

    assert _staged_daily_bar_partial_symbols(cfg, run_id, [symbol], start, end) == set()


def test_multiday_fallback_failure_is_gapfilled_by_symbol(tmp_path, monkeypatch):
    cfg = _cfg(tmp_path)
    run_id = Manifest(cfg.manifest_path).start_run("backfill")
    start, end = date(2024, 6, 20), date(2024, 6, 24)
    symbol = "920001.BJ"
    calls: list[list[str]] = []

    def _kline(symbols, s, e, **k):
        calls.append(list(symbols))
        days = [date(2024, 6, 20), date(2024, 6, 21), date(2024, 6, 24)]
        return pl.concat([_bar_frame(symbols, day) for day in days])

    monkeypatch.setattr("cnequity.adapters.eastmoney.bars.fetch_daily_bars", _kline)
    monkeypatch.setattr(
        "cnequity.steps.bars.fetch_bars_via_sina",
        lambda *args, **kwargs: {
            "rows_read": 0,
            "rows_written": 0,
            "failed_symbols": 1,
            "failed_symbol_names": [symbol],
            "empty_symbol_names": [],
        },
    )
    result = _finish_daily_bars(
        cfg,
        end,
        run_id,
        start=start,
        end=end,
        expected_tdx_symbols=[],
        expected_fallback_symbols=[symbol],
        tdx_result={"rows_read": 0, "rows_written": 0, "had_error": False},
        sina_result={
            "rows_read": 0,
            "rows_written": 0,
            "failed_symbols": 1,
            "failed_symbol_names": [symbol],
        },
    )

    assert calls == [[symbol]]
    assert result["rows_written"] == 3


def test_preopen_placeholder_still_rejects_clist_flat_zeros(tmp_path):
    cfg = _cfg(tmp_path)
    run_id = Manifest(cfg.manifest_path).start_run("daily:core")
    tip = date(2026, 7, 24)
    flat = with_provenance(
        pl.DataFrame(
            {
                "symbol": ["600519.SH", "000001.SZ"],
                "trade_date": [tip, tip],
                "open": [10.0, 10.0],
                "high": [10.0, 10.0],
                "low": [10.0, 10.0],
                "close": [10.0, 10.0],
                "volume": [0, 0],
                "amount": [0.0, 0.0],
            }
        ),
        source="eastmoney",
        data_version="v1",
    )
    StagingWriter(cfg.staging_root).write_batch("daily_bars", run_id, "em-clist-gapfill", flat)
    with pytest.raises(RuntimeError, match="pre-open placeholders"):
        _reject_preopen_placeholder(cfg, run_id, tip)
