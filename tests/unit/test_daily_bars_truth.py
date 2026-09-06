from datetime import date, datetime, timedelta, timezone

import polars as pl

from cnequity.config import Config, load_config, validate_config
from cnequity.config.bootstrap import path_for_toml
from cnequity.steps.common import classify_daily_bar_ownership, incremental_trade_dates
from cnequity.storage.state import StateStore


def test_daily_bar_ownership_keeps_missing_catalog_or_partial_status_unknown():
    sessions = [date(2024, 6, 27), date(2024, 6, 28)]
    status = pl.DataFrame(
        {
            "symbol": ["600001.SH"],
            "trade_date": [sessions[0]],
            "is_trading": [False],
        }
    )

    result = classify_daily_bar_ownership(
        ["600001.SH", "600002.SH"],
        {"600001.SH": (date(2000, 1, 1), None, "stock")},
        sessions[0],
        sessions[-1],
        trading_status=status,
        trading_sessions=sessions,
    )

    # One status row does not prove a two-session absence, and no instrument
    # row means we cannot even establish the security's expected span.
    assert result.unknown == ["600001.SH", "600002.SH"]
    assert result.expected_no_data == []


def test_daily_bar_ownership_accepts_all_false_status_as_expected_no_data():
    sessions = [date(2024, 6, 27), date(2024, 6, 28)]
    status = pl.DataFrame(
        {
            "symbol": ["600001.SH", "600001.SH"],
            "trade_date": sessions,
            "is_trading": [False, False],
        }
    )

    result = classify_daily_bar_ownership(
        ["600001.SH"],
        {"600001.SH": (date(2000, 1, 1), None, "stock")},
        sessions[0],
        sessions[-1],
        trading_status=status,
        trading_sessions=sessions,
    )

    assert result.expected_no_data == ["600001.SH"]
    assert result.no_data_reasons == {"600001.SH": "trading_status_non_trading"}


def test_positive_status_wins_over_old_negative_evidence():
    session = date(2024, 6, 28)
    status = pl.DataFrame(
        {
            "symbol": ["600001.SH"],
            "trade_date": [session],
            "is_trading": [True],
        }
    )
    result = classify_daily_bar_ownership(
        ["600001.SH"],
        {"600001.SH": (date(2000, 1, 1), None, "stock")},
        session,
        session,
        trading_status=status,
        trading_sessions=[session],
        negative_evidence=[
            {
                "symbol": "600001.SH",
                "window_start": session.isoformat(),
                "window_end": session.isoformat(),
            }
        ],
    )

    assert result.generic == ["600001.SH"]
    assert result.expected_no_data == []


def test_daily_bar_ownership_rejects_source_empty_evidence():
    session = date(2024, 6, 28)

    result = classify_daily_bar_ownership(
        ["600001.SH"],
        {"600001.SH": (date(2000, 1, 1), None, "stock")},
        session,
        session,
        negative_evidence=[
            {
                "symbol": "600001.SH",
                "window_start": session.isoformat(),
                "window_end": session.isoformat(),
                "reason": "source_empty",
                "source": "sina",
            }
        ],
    )

    assert result.generic == ["600001.SH"]
    assert result.expected_no_data == []


def test_negative_evidence_is_ttl_bounded_and_catalog_revision_invalidates(tmp_path):
    store = StateStore(tmp_path / "meta")
    now = datetime(2024, 6, 28, tzinfo=timezone.utc)
    identity = {"instruments_revision": 3, "instruments_fingerprint": "catalog-a"}
    store.record_negative_evidence(
        "daily_bars",
        [
            {
                "symbol": "600001.SH",
                "window_start": "2024-06-28",
                "window_end": "2024-06-28",
                "reason": "source_empty",
                "source": "sina",
            }
        ],
        ttl_days=2,
        identity=identity,
        now=now,
    )

    assert (
        store.get_negative_evidence("daily_bars", identity=identity, now=now + timedelta(days=1))[
            0
        ]["symbol"]
        == "600001.SH"
    )
    # A catalog revision is an immediate invalidation, even before TTL.
    assert (
        store.get_negative_evidence(
            "daily_bars",
            identity={"instruments_revision": 4, "instruments_fingerprint": "catalog-b"},
            now=now + timedelta(days=1),
        )
        == []
    )
    assert (
        store.get_negative_evidence("daily_bars", identity=identity, now=now + timedelta(days=3))
        == []
    )


def test_daily_bars_incremental_window_reconciles_latest_trading_sessions(tmp_path):
    cfg = Config(data_root=tmp_path / "data")
    rows = []
    current = date(2024, 5, 20)
    end = date(2024, 6, 28)
    while current <= end:
        rows.append({"trade_date": current, "is_trading": current.weekday() < 5})
        current += timedelta(days=1)
    calendar = cfg.curated_root / "trading_calendar"
    calendar.mkdir(parents=True)
    pl.DataFrame(rows).write_parquet(calendar / "part-merged.parquet")
    StateStore(cfg.meta_root).set_date("daily_bars", date(2024, 6, 25))

    assert incremental_trade_dates(cfg, "daily_bars", end) == [
        date(2024, 6, 19),
        date(2024, 6, 20),
        date(2024, 6, 21),
        date(2024, 6, 24),
        date(2024, 6, 25),
        date(2024, 6, 26),
        date(2024, 6, 27),
        date(2024, 6, 28),
    ]


def test_incremental_negative_evidence_ttl_is_configurable(tmp_path):
    path = tmp_path / "config.toml"
    path.write_text(
        f'[data]\nroot = "{path_for_toml(tmp_path / "data")}"\n'
        "[orchestrator]\nworkers = 1\n"
        '[[job.daily.waves]]\nname = "w"\nsteps = ["instruments"]\n'
        "[incremental]\nnegative_evidence_ttl_days = 11\n",
        encoding="utf-8",
    )
    cfg = load_config(path)

    assert cfg.negative_evidence_ttl_days == 11
    assert validate_config(cfg) == []
