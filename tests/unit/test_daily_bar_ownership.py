from datetime import date, timedelta

import polars as pl

from cnequity.config import Config
from cnequity.orchestrator.manifest import Manifest
from cnequity.steps.bars import (
    _instrument_spans,
    _record_delegated_ownership_batch,
    _staged_daily_bar_missing_keys,
    _undated_etf_pre_trading_sessions,
    step_daily_bars,
)
from cnequity.steps.common import classify_daily_bar_ownership, load_curated_trading_status
from cnequity.storage.layout import init_data_layout


def test_daily_bar_ownership_is_explicit_for_every_symbol():
    symbols = ["600001.SH", "600002.SH", "600003.SH", "600004.SH"]
    spans = {
        "600001.SH": (date(2000, 1, 1), None),
        "600002.SH": (date(2000, 1, 1), date(2015, 12, 31)),
        "600003.SH": (date(2000, 1, 1), date(2020, 6, 1)),
        "600004.SH": (date(2025, 1, 1), None),
    }

    result = classify_daily_bar_ownership(
        symbols,
        spans,
        date(2016, 1, 1),
        date(2024, 12, 31),
    )

    assert result.generic == ["600001.SH"]
    assert result.delegated_delisted == ["600003.SH"]
    assert result.expected_no_data == ["600002.SH", "600004.SH"]
    assert set(result.generic + result.delegated_delisted + result.expected_no_data) == set(symbols)


def test_delisted_etf_is_not_sent_to_stock_recovery_gate():
    result = classify_daily_bar_ownership(
        ["517233.SH", "600003.SH"],
        {
            "517233.SH": (None, date(2026, 8, 18)),
            "600003.SH": (date(2000, 1, 1), date(2026, 8, 18)),
        },
        date(2026, 8, 15),
        date(2026, 8, 21),
    )

    assert result.generic == ["517233.SH"]
    assert result.delegated_delisted == ["600003.SH"]


def test_unlisted_etf_placeholder_is_not_claimed_as_verified_no_data():
    symbols = ["589430.SH", "588200.SH"]
    spans = {
        "589430.SH": (None, None, "etf"),
        "588200.SH": (date(2022, 10, 26), None, "etf"),
    }

    result = classify_daily_bar_ownership(
        symbols,
        spans,
        date(2026, 8, 18),
        date(2026, 8, 18),
        bar_universe={"588200.SH"},
    )

    assert result.placeholder == ["589430.SH"]
    assert result.expected_no_data == []
    assert result.generic == ["588200.SH"]


def test_traded_etf_without_list_date_stays_generic():
    result = classify_daily_bar_ownership(
        ["510300.SH"],
        {"510300.SH": (None, None, "etf")},
        date(2026, 8, 18),
        date(2026, 8, 18),
        bar_universe={"510300.SH"},
    )

    assert result.generic == ["510300.SH"]


def test_unlisted_etf_without_bar_universe_stays_generic():
    """Without a traded-bar universe the classifier stays conservative."""
    result = classify_daily_bar_ownership(
        ["589430.SH"],
        {"589430.SH": (None, None, "etf")},
        date(2026, 8, 18),
        date(2026, 8, 18),
    )

    assert result.generic == ["589430.SH"]


def test_undated_new_etf_excludes_only_leading_sessions_before_first_normal_status():
    sessions = [date(2026, 8, 28), date(2026, 8, 31), date(2026, 9, 1), date(2026, 9, 2)]
    status = {"589453.SH": {date(2026, 9, 2): True}}

    assert _undated_etf_pre_trading_sessions(
        "589453.SH",
        (None, None, "etf"),
        sessions,
        status,
        set(),
    ) == set(sessions[:3])


def test_undated_etf_does_not_hide_post_listing_or_established_etf_gaps():
    sessions = [date(2026, 9, 2), date(2026, 9, 3), date(2026, 9, 4)]
    status = {"589453.SH": {date(2026, 9, 2): True, date(2026, 9, 4): True}}

    assert (
        _undated_etf_pre_trading_sessions(
            "589453.SH",
            (None, None, "etf"),
            sessions,
            status,
            set(),
        )
        == set()
    )
    assert (
        _undated_etf_pre_trading_sessions(
            "589453.SH",
            (None, None, "etf"),
            sessions,
            status,
            {"589453.SH"},
        )
        == set()
    )


def test_missing_key_check_excludes_only_new_etf_pre_trading_prefix(tmp_path):
    cfg = Config(data_root=tmp_path / "data", workers=1)
    init_data_layout(cfg)
    symbol = "589453.SH"
    start = date(2026, 8, 28)
    end = date(2026, 9, 3)
    calendar_days = [start + timedelta(days=offset) for offset in range((end - start).days + 1)]

    instruments = cfg.curated_root / "instruments"
    instruments.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": [symbol],
            "name": ["测试ETF"],
            "asset_type": ["etf"],
            "list_date": [None],
            "delist_date": [None],
        }
    ).write_parquet(instruments / "part-merged.parquet")
    calendar = cfg.curated_root / "trading_calendar"
    calendar.mkdir(parents=True)
    pl.DataFrame(
        {
            "trade_date": calendar_days,
            "is_trading": [day.weekday() < 5 for day in calendar_days],
        }
    ).write_parquet(calendar / "part-merged.parquet")
    status = cfg.curated_root / "trading_status"
    status.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": [symbol],
            "trade_date": [date(2026, 9, 2)],
            "is_trading": [True],
        }
    ).write_parquet(status / "part-merged.parquet")
    staged = cfg.staging_root / "daily_bars" / "run_id=new-etf"
    staged.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": [symbol],
            "trade_date": [date(2026, 9, 2)],
        }
    ).write_parquet(staged / "part-0.parquet")
    bars = cfg.curated_root / "daily_bars" / "trade_date=2026-09-02"
    bars.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["600519.SH"],
            "trade_date": [date(2026, 9, 2)],
            "volume": [100],
        }
    ).write_parquet(bars / "part-merged.parquet")

    assert _instrument_spans(cfg)[symbol] == (None, None, "etf")
    assert load_curated_trading_status(cfg, start=start, end=end, symbols=[symbol]).select(
        "trade_date", "is_trading"
    ).to_dicts() == [{"trade_date": date(2026, 9, 2), "is_trading": True}]
    assert _staged_daily_bar_missing_keys(cfg, "new-etf", [symbol], start, end) == {
        (symbol, date(2026, 9, 3))
    }


def test_retry_only_etf_placeholder_is_audited_and_unblocks_original_batch(tmp_path):
    cfg = Config(data_root=tmp_path / "data", workers=1)
    init_data_layout(cfg)
    instruments = cfg.curated_root / "instruments"
    instruments.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["589430.SH", "600519.SH"],
            "name": ["某基金", "贵州茅台"],
            "asset_type": ["etf", "stock"],
            "list_date": [None, date(2001, 8, 27)],
            "delist_date": [None, None],
        }
    ).write_parquet(instruments / "part-merged.parquet")
    bars = cfg.curated_root / "daily_bars" / "trade_date=2024-06-27"
    bars.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": ["600519.SH"],
            "trade_date": [date(2024, 6, 27)],
            "volume": [100],
        }
    ).write_parquet(bars / "part-0.parquet")

    manifest = Manifest(cfg.manifest_path)
    run_id = manifest.start_run("daily:core")
    manifest.start_batch(
        run_id,
        "placeholder-retry",
        task_id="daily_bars",
        dataset="daily_bars",
        symbols=["589430.SH"],
        window_start="2024-06-28",
        window_end="2024-06-28",
    )
    manifest.finish_batch(
        run_id,
        "placeholder-retry",
        "failed",
        error_message="TDX returned no rows",
    )

    result = step_daily_bars(
        cfg,
        date(2024, 6, 28),
        run_id,
        {
            "_retry_batch_specs": [
                (
                    "placeholder-retry",
                    ["589430.SH"],
                    date(2024, 6, 28),
                    date(2024, 6, 28),
                )
            ]
        },
    )

    assert manifest.get_batch(run_id, "placeholder-retry")["status"] == "superseded"
    assert result["context_updates"]["daily_bars_ownership"]["placeholder"] == 1
    assert any(
        finding["check"] == "daily_bars_etf_placeholder_skipped"
        for finding in result["context_updates"]["audit_findings"]
    )


def test_incomplete_delisted_ownership_blocks_compaction_and_retries(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "data")
    run_id = "run-ownership"
    batch_id = "ownership-retry"
    monkeypatch.setattr("cnequity.steps.delisted.delisted_recovery_covers", lambda *args: False)

    assert (
        _record_delegated_ownership_batch(
            cfg,
            run_id,
            ["600003.SH"],
            date(2016, 1, 1),
            date(2024, 12, 31),
            batch_id=batch_id,
        )
        is False
    )
    manifest = Manifest(cfg.manifest_path)
    first = manifest.get_batch(run_id, batch_id)
    assert first["status"] == "warning"
    assert first["blocks_compaction"] == 1

    monkeypatch.setattr("cnequity.steps.delisted.delisted_recovery_covers", lambda *args: True)
    assert (
        _record_delegated_ownership_batch(
            cfg,
            run_id,
            ["600003.SH"],
            date(2016, 1, 1),
            date(2024, 12, 31),
            batch_id=batch_id,
        )
        is True
    )
    assert manifest.get_batch(run_id, batch_id)["status"] == "success"
