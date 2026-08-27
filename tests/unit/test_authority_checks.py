"""Publisher cross-checks (issue #10).

These are the only checks that can catch a vendor publishing on time, in the
right shape, with a wrong number — the shape of the `m2_yoy` defect in #3.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timezone

import polars as pl
import pytest

from cnequity.config import Config
from cnequity.quality import authority_checks as ac

TD = date(2026, 8, 1)
OBS = date(2026, 7, 31)


def _lake(tmp_path, *, pmi: float | None = None, status: dict[str, str] | None = None) -> Config:
    root = tmp_path / "data"
    if pmi is not None:
        part = root / "curated" / "macro_indicators" / f"obs_date={OBS.isoformat()}"
        part.mkdir(parents=True)
        pl.DataFrame(
            {
                "indicator_id": ["pmi_manufacturing"],
                "obs_date": [OBS],
                "value": [pmi],
                "frequency": ["monthly"],
                "source": ["eastmoney"],
                "data_version": ["v1"],
                "fetched_at": [datetime.now(timezone.utc)],
            }
        ).write_parquet(part / "p.parquet")
    if status is not None:
        part = root / "curated" / "trading_status" / f"trade_date={TD.isoformat()}"
        part.mkdir(parents=True)
        pl.DataFrame(
            {
                "symbol": list(status),
                "trade_date": [TD] * len(status),
                "is_trading": [True] * len(status),
                "status": list(status.values()),
                "source": ["eastmoney"] * len(status),
                "data_version": ["v1"] * len(status),
                "fetched_at": [datetime.now(timezone.utc)] * len(status),
            }
        ).write_parquet(part / "p.parquet")
    cfg = Config(data_root=root)
    cfg.sources = {"nbs": True, "exchange": True}
    return cfg


def _lake_with_instruments(
    tmp_path,
    *,
    status: dict[str, str],
    delisted: set[str] | None = None,
) -> Config:
    """Variant of ``_lake`` that also writes an instruments catalogue."""
    cfg = _lake(tmp_path, status=status)
    part = cfg.curated_root / "instruments"
    part.mkdir(parents=True, exist_ok=True)
    symbols = list(status)
    delisted = delisted or set()
    pl.DataFrame(
        {
            "symbol": symbols,
            "name": [f"公司{i}" for i in range(len(symbols))],
            "exchange": [s[-2:] for s in symbols],
            "asset_type": ["stock"] * len(symbols),
            "list_date": [date(2000, 1, 1)] * len(symbols),
            "delist_date": [date(2025, 1, 1) if symbol in delisted else None for symbol in symbols],
            "source": ["tdx_protocol"] * len(symbols),
            "data_version": ["v1"] * len(symbols),
            "fetched_at": [datetime.now(timezone.utc)] * len(symbols),
        }
    ).write_parquet(part / "p.parquet")
    return cfg


# --- PMI vs NBS --------------------------------------------------------------


def _publish(monkeypatch, value: float, obs: date = OBS):
    """Stand in for the NBS release; the check imports the adapter lazily."""
    import cnequity.adapters.nbs.pmi_release as nbs

    monkeypatch.setattr(
        nbs,
        "fetch_latest_pmi",
        lambda **_kw: {"obs_date": obs, "value": value, "url": "https://example/release"},
    )


def test_matching_pmi_is_silent(monkeypatch, tmp_path):
    _publish(monkeypatch, 49.2)
    assert ac.macro_pmi_vs_nbs(_lake(tmp_path, pmi=49.2), TD) == []


def test_drifted_pmi_is_an_error(monkeypatch, tmp_path):
    _publish(monkeypatch, 49.2)
    findings = ac.macro_pmi_vs_nbs(_lake(tmp_path, pmi=51.7), TD)
    assert len(findings) == 1
    f = findings[0]
    assert f["check"] == "macro_pmi_vs_nbs"
    assert f["severity"] == "error"
    assert f["curated_value"] == 51.7
    assert f["published_value"] == 49.2
    assert f["source_url"] == "https://example/release"


def test_float_noise_is_not_drift(monkeypatch, tmp_path):
    _publish(monkeypatch, 49.2)
    assert ac.macro_pmi_vs_nbs(_lake(tmp_path, pmi=49.2000001), TD) == []


def test_a_month_the_run_has_not_reached_is_not_compared(monkeypatch, tmp_path):
    _publish(monkeypatch, 49.2, obs=date(2026, 9, 30))
    assert ac.macro_pmi_vs_nbs(_lake(tmp_path, pmi=49.2), TD) == []


def test_missing_curated_month_is_left_to_the_staleness_check(monkeypatch, tmp_path):
    _publish(monkeypatch, 49.2)
    assert ac.macro_pmi_vs_nbs(_lake(tmp_path), TD) == []


def test_unreachable_publisher_is_silent(monkeypatch, tmp_path):
    import cnequity.adapters.nbs.pmi_release as nbs

    monkeypatch.setattr(nbs, "fetch_latest_pmi", lambda **_kw: None)
    assert ac.macro_pmi_vs_nbs(_lake(tmp_path, pmi=49.2), TD) == []


def test_pmi_check_is_off_without_the_source_flag(monkeypatch, tmp_path):
    def _boom(**_kw):
        raise AssertionError("must not reach the network when [sources.nbs] is absent")

    import cnequity.adapters.nbs.pmi_release as nbs

    monkeypatch.setattr(nbs, "fetch_latest_pmi", _boom)
    cfg = _lake(tmp_path, pmi=49.2)
    cfg.sources = {}
    assert ac.macro_pmi_vs_nbs(cfg, TD) == []


# --- ST vs the exchanges -----------------------------------------------------


def _exchange(monkeypatch, names: dict[str, str]):
    import cnequity.adapters.exchange.st_lists as ex

    monkeypatch.setattr(
        ex,
        "fetch_exchange_names_with_status",
        lambda **_kw: ex.ExchangeNamesResult(names=names, failures={}),
    )


def _universe(n: int, *, st_designated: int):
    syms = [f"{600000 + i:06d}.SH" for i in range(n)]
    names = {s: (f"ST公司{i}" if i < st_designated else f"公司{i}") for i, s in enumerate(syms)}
    return syms, names


def test_agreeing_labels_are_silent(monkeypatch, tmp_path):
    syms, names = _universe(50, st_designated=10)
    _exchange(monkeypatch, names)
    status = {s: ("st" if i < 10 else "normal") for i, s in enumerate(syms)}
    assert ac.st_labels_vs_exchange(_lake(tmp_path, status=status), TD) == []


def test_st_exchange_check_reads_only_the_audit_day(monkeypatch, tmp_path):
    syms, names = _universe(20, st_designated=2)
    status = {s: ("st" if i < 2 else "normal") for i, s in enumerate(syms)}
    cfg = _lake(tmp_path, status=status)
    _exchange(monkeypatch, names)

    requested: dict[str, object] = {}
    original = ac.scan_parquet_root

    def bounded_scan(*args, **kwargs):
        requested.update(kwargs)
        return original(*args, **kwargs)

    monkeypatch.setattr(ac, "scan_parquet_root", bounded_scan)

    assert ac.st_labels_vs_exchange(cfg, TD) == []
    assert requested["start"] == TD
    assert requested["end"] == TD


def test_star_st_statuses_are_counted_as_labels(monkeypatch, tmp_path):
    syms, names = _universe(50, st_designated=10)
    _exchange(monkeypatch, names)
    status = {s: ("*st" if i < 10 else "normal") for i, s in enumerate(syms)}
    assert ac.st_labels_vs_exchange(_lake(tmp_path, status=status), TD) == []


def test_missing_labels_are_an_error(monkeypatch, tmp_path):
    syms, names = _universe(50, st_designated=10)
    _exchange(monkeypatch, names)
    status = {s: "normal" for s in syms}
    findings = ac.st_labels_vs_exchange(_lake(tmp_path, status=status), TD)
    assert len(findings) == 1
    assert findings[0]["designated_not_labeled"] == 10
    assert findings[0]["labeled_not_designated"] == 0


def test_labels_the_exchange_does_not_designate_are_an_error(monkeypatch, tmp_path):
    syms, names = _universe(50, st_designated=0)
    _exchange(monkeypatch, names)
    status = {s: ("st" if i < 9 else "normal") for i, s in enumerate(syms)}
    findings = ac.st_labels_vs_exchange(_lake(tmp_path, status=status), TD)
    assert findings[0]["labeled_not_designated"] == 9


def test_names_the_lake_does_not_carry_are_not_a_shortfall(monkeypatch, tmp_path):
    """The exchanges list a company until formal delisting; feeds drop it sooner.

    Measured 2026-08-01: SSE designated 600355 and 603388 ST while neither
    EastMoney nor TDX still listed them. Counting those would burn the tolerance
    permanently, so both directions compare only over the shared universe.
    """
    syms, names = _universe(20, st_designated=0)
    # Ten ST names the exchange lists and the lake has never heard of.
    for i in range(10):
        names[f"{900000 + i:06d}.SH"] = f"*ST退市{i}"
    _exchange(monkeypatch, names)
    status = {s: "normal" for s in syms}
    assert ac.st_labels_vs_exchange(_lake(tmp_path, status=status), TD) == []


def test_delisted_st_names_are_not_a_shortfall(monkeypatch, tmp_path):
    """The exchange keeps a ST name until formal delisting; the lake records the end date.

    The shared-universe comparison must not count a symbol the lake already
    marks delisted, or the disagreement is permanent (2026-08-27: 600355.SH,
    603388.SH).
    """
    syms, names = _universe(20, st_designated=5)
    # Two of the ST names are already formally delisted in the catalogue.
    delisted = set(syms[:2])
    _exchange(monkeypatch, names)
    status = {s: "normal" for s in syms}
    cfg = _lake_with_instruments(tmp_path, status=status, delisted=delisted)
    assert ac.st_labels_vs_exchange(cfg, TD) == []


def test_small_disagreement_is_tolerated_as_naming_lag(monkeypatch, tmp_path):
    syms, names = _universe(50, st_designated=10)
    _exchange(monkeypatch, names)
    keep = 10 - ac.ST_MAX_DISAGREEMENT
    status = {s: ("st" if i < keep else "normal") for i, s in enumerate(syms)}
    assert ac.st_labels_vs_exchange(_lake(tmp_path, status=status), TD) == []


def test_unreachable_exchanges_are_silent(monkeypatch, tmp_path):
    import cnequity.adapters.exchange.st_lists as ex

    monkeypatch.setattr(
        ex,
        "fetch_exchange_names_with_status",
        lambda **_kw: ex.ExchangeNamesResult(names={}, failures={"sse": "down", "szse": "down"}),
    )
    syms, _ = _universe(20, st_designated=0)
    status = {s: "normal" for s in syms}
    assert ac.st_labels_vs_exchange(_lake(tmp_path, status=status), TD) == []


def test_partial_exchange_snapshot_is_not_reported_as_agreed(monkeypatch, tmp_path):
    syms, names = _universe(50, st_designated=10)
    import cnequity.adapters.exchange.st_lists as ex

    monkeypatch.setattr(
        ex,
        "fetch_exchange_names_with_status",
        lambda **_kw: ex.ExchangeNamesResult(names=names, failures={"szse": "down"}),
    )
    status = {s: "normal" for s in syms}
    assert ac.st_labels_vs_exchange(_lake(tmp_path, status=status), TD) == []


def test_st_check_is_off_without_the_source_flag(monkeypatch, tmp_path):
    import cnequity.adapters.exchange.st_lists as ex

    def _boom(**_kw):
        raise AssertionError("must not reach the network when [sources.exchange] is absent")

    monkeypatch.setattr(ex, "fetch_exchange_names", _boom)
    syms, _ = _universe(20, st_designated=0)
    cfg = _lake(tmp_path, status={s: "normal" for s in syms})
    cfg.sources = {}
    assert ac.st_labels_vs_exchange(cfg, TD) == []


# --- persistence -------------------------------------------------------------


def test_a_clean_run_still_leaves_evidence(monkeypatch, tmp_path):
    """A findings file cannot distinguish "checked, agreed" from "never checked"."""
    _publish(monkeypatch, 49.2)
    syms, names = _universe(20, st_designated=2)
    _exchange(monkeypatch, names)
    status = {s: ("st" if i < 2 else "normal") for i, s in enumerate(syms)}
    cfg = _lake(tmp_path, pmi=49.2, status=status)

    assert ac.run_authority_checks(cfg, TD) == []
    written = cfg.meta_root / "quality" / "source_diffs" / f"authority-{TD.isoformat()}.json"
    payload = json.loads(written.read_text(encoding="utf-8"))
    assert payload["kind"] == "authority_crosscheck"
    assert payload["checks"] == {
        "macro_pmi_vs_nbs": "agreed",
        "st_labels_vs_exchange": "agreed",
        # This lake carries no daily_bars, so the bar comparison records that it
        # had nothing to compare rather than claiming a clean result.
        "daily_bars_vs_exchange": "skipped_no_curated",
    }


def test_a_failing_publisher_does_not_break_the_run(monkeypatch, tmp_path):
    import cnequity.adapters.nbs.pmi_release as nbs

    def _boom(**_kw):
        raise RuntimeError("site down")

    monkeypatch.setattr(nbs, "fetch_latest_pmi", _boom)
    _exchange(monkeypatch, {})
    cfg = _lake(tmp_path, pmi=49.2, status={})
    assert ac.run_authority_checks(cfg, TD) == []
    written = cfg.meta_root / "quality" / "source_diffs" / f"authority-{TD.isoformat()}.json"
    assert "error" in json.loads(written.read_text(encoding="utf-8"))["checks"]["macro_pmi_vs_nbs"]


@pytest.mark.parametrize("flag", [{}, {"nbs": False, "exchange": False}])
def test_audit_stays_offline_when_the_sources_are_off(tmp_path, flag):
    cfg = _lake(tmp_path, pmi=49.2)
    cfg.sources = flag
    # No monkeypatching: a network call here would be a real request.
    assert ac.run_authority_checks(cfg, TD) == []
    payload = json.loads(
        (cfg.meta_root / "quality" / "source_diffs" / f"authority-{TD.isoformat()}.json").read_text(
            encoding="utf-8"
        )
    )
    assert payload["checks"] == {
        "macro_pmi_vs_nbs": "skipped_disabled",
        "st_labels_vs_exchange": "skipped_disabled",
        "daily_bars_vs_exchange": "skipped_disabled",
    }


def test_publisher_check_records_missing_curated_state(monkeypatch, tmp_path):
    _publish(monkeypatch, 49.2)
    cfg = _lake(tmp_path)

    assert ac.run_authority_checks(cfg, TD) == []
    payload = json.loads(
        (cfg.meta_root / "quality" / "source_diffs" / f"authority-{TD.isoformat()}.json").read_text(
            encoding="utf-8"
        )
    )
    assert payload["checks"] == {
        "macro_pmi_vs_nbs": "skipped_no_curated",
        "st_labels_vs_exchange": "skipped_no_curated",
        "daily_bars_vs_exchange": "skipped_no_curated",
    }


# --- daily_bars vs the exchanges ---------------------------------------------

BARS_DATE = date(2026, 8, 28)


def _bars_lake(tmp_path, rows: list[dict]) -> Config:
    root = tmp_path / "data"
    part = root / "curated" / "daily_bars" / f"trade_date={BARS_DATE.isoformat()}"
    part.mkdir(parents=True)
    pl.DataFrame(
        {
            "symbol": [r["symbol"] for r in rows],
            "trade_date": [BARS_DATE] * len(rows),
            "open": [r.get("open", r["close"]) for r in rows],
            "high": [r.get("high", r["close"]) for r in rows],
            "low": [r.get("low", r["close"]) for r in rows],
            "close": [r["close"] for r in rows],
            "volume": [float(r.get("volume", 1000.0)) for r in rows],
            "amount": [float(r.get("amount", 10000.0)) for r in rows],
            "source": ["tdx_protocol"] * len(rows),
            "data_version": ["v1"] * len(rows),
            "fetched_at": [datetime.now(timezone.utc)] * len(rows),
        }
    ).write_parquet(part / "p.parquet")
    cfg = Config(data_root=root)
    cfg.sources = {"exchange": True}
    return cfg


def _publish_quotes(monkeypatch, rows: list[dict], *, covered=("sse", "szse")):
    import cnequity.adapters.exchange.daily_quotes as dq

    quotes = dq._finish(
        [
            {
                "symbol": r["symbol"],
                "trade_date": BARS_DATE,
                "open": r.get("open", r["close"]),
                "high": r.get("high", r["close"]),
                "low": r.get("low", r["close"]),
                "close": r["close"],
                "volume": float(r.get("volume", 1000.0)),
                "amount": float(r.get("amount", 10000.0)),
            }
            for r in rows
        ]
    )
    monkeypatch.setattr(
        dq,
        "fetch_exchange_daily_quotes",
        lambda *a, **k: dq.ExchangeQuotesResult(
            quotes=quotes, covered=frozenset(covered), failures={}
        ),
    )


def test_bars_matching_the_exchange_are_silent(monkeypatch, tmp_path):
    rows = [{"symbol": "600000.SH", "close": 9.0}, {"symbol": "000001.SZ", "close": 11.65}]
    _publish_quotes(monkeypatch, rows)
    outcome = ac._daily_bars_vs_exchange_outcome(_bars_lake(tmp_path, rows), BARS_DATE)
    assert outcome.status == "agreed"
    assert outcome.findings == []


def test_a_szse_only_comparison_is_not_reported_as_full_coverage(monkeypatch, tmp_path):
    """SSE serves only the session it is publishing; SH may be uncomparable."""
    rows = [{"symbol": "000001.SZ", "close": 11.65}]
    _publish_quotes(monkeypatch, rows, covered=("szse",))
    outcome = ac._daily_bars_vs_exchange_outcome(_bars_lake(tmp_path, rows), BARS_DATE)
    assert outcome.status == "agreed_partial"


def test_a_close_that_disagrees_with_the_exchange_is_an_error(monkeypatch, tmp_path):
    curated = [{"symbol": "600000.SH", "close": 9.0}, {"symbol": "000001.SZ", "close": 11.65}]
    _publish_quotes(
        monkeypatch,
        [{"symbol": "600000.SH", "close": 9.0}, {"symbol": "000001.SZ", "close": 11.42}],
    )
    findings = ac.daily_bars_vs_exchange(_bars_lake(tmp_path, curated), BARS_DATE)
    assert [f["check"] for f in findings] == ["daily_bars_vs_exchange"]
    assert findings[0]["severity"] == "error"
    assert findings[0]["disagreeing_symbols"] == 1
    assert findings[0]["compared_symbols"] == 2
    assert findings[0]["symbols"] == ["000001.SZ"]


def test_a_traded_symbol_with_no_curated_bar_is_an_error(monkeypatch, tmp_path):
    curated = [{"symbol": "600000.SH", "close": 9.0}]
    _publish_quotes(
        monkeypatch,
        curated + [{"symbol": "000001.SZ", "close": 11.65, "volume": 8_385_190.0}],
    )
    findings = ac.daily_bars_vs_exchange(_bars_lake(tmp_path, curated), BARS_DATE)
    assert [f["check"] for f in findings] == ["daily_bars_missing_vs_exchange"]
    assert findings[0]["symbols"] == ["000001.SZ"]


def test_a_suspended_symbol_with_no_curated_bar_is_not_a_gap(monkeypatch, tmp_path):
    """SZSE lists a suspended security at zero volume; a quote feed emits no bar.

    Measured 2026-08-27 this was the whole of the difference, so counting it
    would report the convention as a defect every single day.
    """
    curated = [{"symbol": "600000.SH", "close": 9.0}]
    _publish_quotes(
        monkeypatch,
        curated + [{"symbol": "000016.SZ", "close": 2.33, "volume": 0.0, "amount": 0.0}],
    )
    assert ac.daily_bars_vs_exchange(_bars_lake(tmp_path, curated), BARS_DATE) == []


def test_turnover_below_the_divergent_share_stays_quiet(monkeypatch, tmp_path):
    # One of four names short on volume: 25% — under the 30% set here.
    curated = [{"symbol": f"60000{i}.SH", "close": 9.0, "volume": 1000.0} for i in range(4)]
    official = list(curated)
    official[0] = {**official[0], "volume": 2000.0, "amount": 20000.0}
    _publish_quotes(monkeypatch, official)
    cfg = _bars_lake(tmp_path, curated)
    cfg.exchange_audit_turnover_max_fraction = 0.30
    assert ac.daily_bars_vs_exchange(cfg, BARS_DATE) == []


def test_a_widening_turnover_gap_is_a_warning(monkeypatch, tmp_path):
    curated = [{"symbol": f"60000{i}.SH", "close": 9.0, "volume": 1000.0} for i in range(4)]
    official = [{**r, "volume": 2000.0, "amount": 20000.0} for r in curated]
    _publish_quotes(monkeypatch, official)
    findings = ac.daily_bars_vs_exchange(_bars_lake(tmp_path, curated), BARS_DATE)
    assert [f["check"] for f in findings] == ["daily_bars_turnover_vs_exchange"]
    assert findings[0]["severity"] == "warning"
    assert findings[0]["divergent_fraction"] == 1.0


def test_a_zero_on_either_side_is_not_relative_drift(monkeypatch, tmp_path):
    """A suspended day carries zeros; a ratio against zero is meaningless."""
    curated = [{"symbol": "600000.SH", "close": 0.0, "volume": 0.0, "amount": 0.0}]
    _publish_quotes(monkeypatch, [{"symbol": "600000.SH", "close": 9.0, "volume": 1000.0}])
    assert ac.daily_bars_vs_exchange(_bars_lake(tmp_path, curated), BARS_DATE) == []


def test_bars_check_is_off_without_the_source_flag(monkeypatch, tmp_path):
    rows = [{"symbol": "600000.SH", "close": 9.0}]
    _publish_quotes(monkeypatch, [{"symbol": "600000.SH", "close": 5.0}])
    cfg = _bars_lake(tmp_path, rows)
    cfg.sources = {}
    outcome = ac._daily_bars_vs_exchange_outcome(cfg, BARS_DATE)
    assert outcome.status == "skipped_disabled"
    assert outcome.findings == []


def test_an_unreachable_exchange_is_not_a_disagreement(monkeypatch, tmp_path):
    import cnequity.adapters.exchange.daily_quotes as dq

    rows = [{"symbol": "600000.SH", "close": 9.0}]
    monkeypatch.setattr(
        dq,
        "fetch_exchange_daily_quotes",
        lambda *a, **k: dq.ExchangeQuotesResult(
            quotes=dq._EMPTY_QUOTES.clone(), covered=frozenset(), failures={"sse": "timeout"}
        ),
    )
    outcome = ac._daily_bars_vs_exchange_outcome(_bars_lake(tmp_path, rows), BARS_DATE)
    assert outcome.status == "unavailable"
    assert outcome.findings == []
