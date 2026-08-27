"""Exchange-authority ST calibration for the daily trading_status step."""

from __future__ import annotations

from datetime import date, datetime, timezone

import polars as pl

from cnequity.config import Config
from cnequity.steps.reference import _active_all_a_symbols, _calibrate_st_with_exchange

TD = date(2026, 8, 27)


def _config(tmp_path, *, instruments: pl.DataFrame | None = None) -> Config:
    root = tmp_path / "data"
    if instruments is not None:
        part = root / "curated" / "instruments"
        part.mkdir(parents=True)
        instruments.write_parquet(part / "part-0.parquet")
    cfg = Config(data_root=root)
    cfg.sources = {"exchange": True}
    return cfg


def _status_frame(statuses: dict[str, str]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "symbol": list(statuses),
            "trade_date": [TD] * len(statuses),
            "is_trading": [s == "normal" for s in statuses.values()],
            "status": list(statuses.values()),
            "source": ["eastmoney"] * len(statuses),
            "data_version": ["v1"] * len(statuses),
            "fetched_at": [datetime.now(timezone.utc)] * len(statuses),
        }
    )


def _instruments(symbols: list[str], *, delisted: set[str] | None = None) -> pl.DataFrame:
    delisted = delisted or set()
    return pl.DataFrame(
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
    )


def _exchange(monkeypatch, names: dict[str, str], *, failures: dict[str, str] | None = None):
    import cnequity.adapters.exchange.st_lists as ex

    monkeypatch.setattr(
        ex,
        "fetch_exchange_names_with_status",
        lambda **_kw: ex.ExchangeNamesResult(names=names, failures=failures or {}),
    )


def test_calibration_adds_st_to_active_suspended_symbols(monkeypatch, tmp_path):
    """ST + 停牌并存：is_trading 保持 false，status 补成 st。"""
    syms = ["000016.SZ", "600491.SH", "000001.SZ"]
    _exchange(monkeypatch, {"000016.SZ": "*ST康佳A", "600491.SH": "ST龙元"})
    cfg = _config(tmp_path, instruments=_instruments(syms))
    frame = _status_frame(
        {"000016.SZ": "suspended", "600491.SH": "suspended", "000001.SZ": "normal"}
    )

    out, findings = _calibrate_st_with_exchange(frame, cfg, TD)

    assert findings == []
    rows = {r["symbol"]: r for r in out.iter_rows(named=True)}
    assert rows["000016.SZ"]["status"] == "st"
    assert rows["000016.SZ"]["is_trading"] is False
    assert rows["600491.SH"]["status"] == "st"
    assert rows["600491.SH"]["is_trading"] is False
    assert rows["000001.SZ"]["status"] == "normal"


def test_calibration_skips_delisted_st_names(monkeypatch, tmp_path):
    syms = ["600355.SH", "603388.SH", "000001.SZ"]
    _exchange(
        monkeypatch,
        {"600355.SH": "*ST精伦", "603388.SH": "*ST元成", "000001.SZ": "平安银行"},
    )
    cfg = _config(tmp_path, instruments=_instruments(syms, delisted={"600355.SH", "603388.SH"}))
    frame = _status_frame({"600355.SH": "normal", "603388.SH": "normal", "000001.SZ": "normal"})

    out, findings = _calibrate_st_with_exchange(frame, cfg, TD)

    assert findings == []
    rows = {r["symbol"]: r for r in out.iter_rows(named=True)}
    assert rows["600355.SH"]["status"] == "normal"
    assert rows["603388.SH"]["status"] == "normal"
    assert rows["000001.SZ"]["status"] == "normal"


def test_calibration_keeps_existing_st_and_star_st(monkeypatch, tmp_path):
    syms = ["000016.SZ", "600491.SH"]
    _exchange(monkeypatch, {"000016.SZ": "*ST康佳A", "600491.SH": "ST龙元"})
    cfg = _config(tmp_path, instruments=_instruments(syms))
    frame = _status_frame({"000016.SZ": "st", "600491.SH": "*st"})

    out, findings = _calibrate_st_with_exchange(frame, cfg, TD)

    assert findings == []
    rows = {r["symbol"]: r for r in out.iter_rows(named=True)}
    assert rows["000016.SZ"]["status"] == "st"
    assert rows["600491.SH"]["status"] == "*st"


def test_calibration_is_disabled_without_exchange_source(monkeypatch, tmp_path):
    def _boom(**_kw):
        raise AssertionError("must not reach the network when [sources.exchange] is absent")

    import cnequity.adapters.exchange.st_lists as ex

    monkeypatch.setattr(ex, "fetch_exchange_names_with_status", _boom)
    cfg = _config(tmp_path)
    cfg.sources = {}
    frame = _status_frame({"000016.SZ": "suspended"})

    out, findings = _calibrate_st_with_exchange(frame, cfg, TD)

    assert findings == []
    assert out.row(0, named=True)["status"] == "suspended"


def test_calibration_reports_unavailable_exchange(monkeypatch, tmp_path):
    _exchange(monkeypatch, {}, failures={"sse": "down", "szse": "down"})
    cfg = _config(tmp_path)
    cfg.sources = {"exchange": True}
    frame = _status_frame({"000016.SZ": "suspended"})

    out, findings = _calibrate_st_with_exchange(frame, cfg, TD)

    assert out.equals(frame)
    assert len(findings) == 1
    assert findings[0]["check"] == "trading_status_exchange_st_unavailable"


def test_active_symbols_uses_delist_date(tmp_path):
    cfg = _config(
        tmp_path, instruments=_instruments(["600355.SH", "600491.SH"], delisted={"600355.SH"})
    )
    assert _active_all_a_symbols(cfg, TD) == {"600491.SH"}


def test_active_symbols_none_without_instruments(tmp_path):
    assert _active_all_a_symbols(_config(tmp_path), TD) is None


def test_active_symbols_excludes_not_yet_listed(tmp_path):
    symbols = ["000016.SZ", "920001.BJ"]
    rows = {
        "symbol": symbols,
        "name": [f"公司{i}" for i in range(len(symbols))],
        "exchange": [s[-2:] for s in symbols],
        "asset_type": ["stock"] * len(symbols),
        "list_date": [date(2000, 1, 1), date(2026, 9, 1)],
        "delist_date": [None, None],
        "source": ["tdx_protocol"] * len(symbols),
        "data_version": ["v1"] * len(symbols),
        "fetched_at": [datetime.now(timezone.utc)] * len(symbols),
    }
    cfg = _config(tmp_path, instruments=pl.DataFrame(rows))
    assert _active_all_a_symbols(cfg, TD) == {"000016.SZ"}
