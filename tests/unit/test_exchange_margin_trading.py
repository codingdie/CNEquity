"""融资融券 read from the exchanges instead of a redistributor.

The values were verified against the curated EastMoney day for 2026-08-26 —
every compared field matched exactly over 3,522 shared symbols — so what these
cover is the parsing, the two publishers' differing horizons, and the one field
the SSE does not publish.
"""

from __future__ import annotations

import io
from datetime import date

import polars as pl
import pytest

from cnequity.adapters.exchange import margin_trading as em
from cnequity.config import Config
from cnequity.steps.capital import _fetch_margin_via_exchange, _margin_source

TD = date(2026, 8, 27)


class _Resp:
    def __init__(self, payload=None, content=b""):
        self._payload = payload
        self.content = content

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


def _serve(monkeypatch, resp):
    monkeypatch.setattr(
        em, "_client", lambda: type("C", (), {"get": staticmethod(lambda *a, **k: resp)})
    )


def _serve_pages(monkeypatch, responses):
    requested: list[str] = []
    pages = iter(responses)

    def get(url, *args, **kwargs):
        requested.append(url)
        return next(pages)

    monkeypatch.setattr(em, "_client", lambda: type("C", (), {"get": staticmethod(get)}))
    return requested


def _sse_payload(rows, *, total=None):
    return {"pageHelp": {"data": rows, "total": len(rows) if total is None else total}}


_SSE_ROW = {
    "stockCode": "600000",
    "rzye": 1452735156,
    "rzmre": 49295712,
    "rqyl": 38171440,
    "rqmcl": 2274700,
    "rqylje": None,
}


def test_sse_margin_parses_and_leaves_short_balance_null(monkeypatch):
    """SSE publishes 融券余量 but not 融券余额; the gap is carried, not filled."""
    _serve(monkeypatch, _Resp(_sse_payload([_SSE_ROW])))
    out = em.fetch_sse_margin_trading(TD)
    row = out.to_dicts()[0]
    assert row["symbol"] == "600000.SH"
    assert row["margin_balance"] == 1452735156
    assert row["margin_buy"] == 49295712
    assert row["short_sell_volume"] == 2274700
    assert row["short_balance"] is None


def test_sse_fills_short_balance_if_the_publisher_ever_does(monkeypatch):
    """Parsed from the real field rather than hardcoded to null."""
    _serve(monkeypatch, _Resp(_sse_payload([{**_SSE_ROW, "rqylje": 12345.0}])))
    assert em.fetch_sse_margin_trading(TD).to_dicts()[0]["short_balance"] == 12345.0


def test_a_truncated_sse_page_is_not_written_as_a_day(monkeypatch):
    """A short page would read downstream as securities leaving the list."""
    _serve(monkeypatch, _Resp(_sse_payload([_SSE_ROW], total=1999)))
    assert em.fetch_sse_margin_trading(TD).is_empty()


def test_sse_fetches_every_page_when_the_server_caps_the_response(monkeypatch):
    second_row = {**_SSE_ROW, "stockCode": "600001"}
    requested = _serve_pages(
        monkeypatch,
        [
            _Resp(_sse_payload([_SSE_ROW], total=2)),
            _Resp(_sse_payload([second_row], total=2)),
        ],
    )
    monkeypatch.setattr(em, "SSE_PAGE_SIZE", 1)

    out = em.fetch_sse_margin_trading(TD)

    assert out.get_column("symbol").to_list() == ["600000.SH", "600001.SH"]
    assert "pageHelp.pageNo=1" in requested[0]
    assert "pageHelp.pageNo=2" in requested[1]


def test_sse_drops_the_day_when_a_later_page_is_missing(monkeypatch):
    _serve_pages(
        monkeypatch,
        [
            _Resp(_sse_payload([_SSE_ROW], total=2)),
            _Resp(_sse_payload([], total=2)),
        ],
    )
    monkeypatch.setattr(em, "SSE_PAGE_SIZE", 1)

    assert em.fetch_sse_margin_trading(TD).is_empty()


def test_sse_skips_codes_outside_the_lake_universe(monkeypatch):
    _serve(monkeypatch, _Resp(_sse_payload([{**_SSE_ROW, "stockCode": "900902"}])))
    assert em.fetch_sse_margin_trading(TD).is_empty()


def _szse_workbook(rows):
    import pandas as pd

    buf = io.BytesIO()
    pd.DataFrame(rows).to_excel(buf, index=False)
    return buf.getvalue()


_SZSE_ROW = {
    "证券代码": "000001",
    "证券简称": "平安银行",
    "融资买入额(元)": "90,795,122",
    "融资余额(元)": "4,651,849,607",
    "融券卖出量(股/份)": "684,865",
    "融券余量(股/份)": "6,827,165",
    "融券余额(元)": "79,126,842",
    "融资融券余额(元)": "4,730,976,449",
}


def test_szse_margin_parses_raw_units_and_separators(monkeypatch):
    """The xlsx export states 元 and 股 directly — unlike its JSON twin."""
    _serve(monkeypatch, _Resp(content=_szse_workbook([_SZSE_ROW])))
    row = em.fetch_szse_margin_trading(TD).to_dicts()[0]
    assert row["symbol"] == "000001.SZ"
    assert row["margin_balance"] == pytest.approx(4_651_849_607.0)
    assert row["margin_buy"] == pytest.approx(90_795_122.0)
    assert row["short_balance"] == pytest.approx(79_126_842.0)
    assert row["short_sell_volume"] == pytest.approx(684_865.0)


def test_a_header_only_szse_export_means_not_published_yet(monkeypatch):
    """How SZSE represents a session it has not compiled — seen for 2026-08-28."""
    _serve(monkeypatch, _Resp(content=_szse_workbook([])))
    assert em.fetch_szse_margin_trading(TD).is_empty()


def test_a_renamed_szse_column_is_reported_not_guessed(monkeypatch):
    _serve(monkeypatch, _Resp(content=_szse_workbook([{"证券代码": "000001"}])))
    assert em.fetch_szse_margin_trading(TD).is_empty()


def test_combined_result_names_the_exchange_that_published(monkeypatch):
    monkeypatch.setattr(em, "fetch_sse_margin_trading", lambda *a, **k: em._EMPTY_MARGIN.clone())
    monkeypatch.setattr(
        em,
        "fetch_szse_margin_trading",
        lambda *a, **k: em._finish(
            [{"symbol": "000001.SZ", "trade_date": TD, "margin_balance": 1.0}]
        ),
    )
    result = em.fetch_exchange_margin_trading(TD)
    assert result.covered == frozenset({"szse"})
    assert result.failures == {"sse": "no usable rows"}


# --- step wiring -------------------------------------------------------------


def _cfg(tmp_path, **kw) -> Config:
    cfg = Config(data_root=tmp_path / "data")
    for key, value in kw.items():
        setattr(cfg, key, value)
    return cfg


def test_a_half_published_session_is_left_for_a_later_run(tmp_path, monkeypatch):
    """SZSE lands a business day after SSE; writing SH alone would strand SZ.

    An empty frame leaves the watermark where it is, so the next run fetches
    the same session complete rather than the dataset carrying half a market
    forever.
    """
    import cnequity.adapters.exchange.margin_trading as adapter

    monkeypatch.setattr(
        adapter,
        "fetch_exchange_margin_trading",
        lambda *a, **k: adapter.ExchangeMarginResult(
            rows=adapter._finish([{"symbol": "600000.SH", "trade_date": TD}]),
            covered=frozenset({"sse"}),
            failures={"szse": "no usable rows"},
        ),
    )
    assert _fetch_margin_via_exchange(TD, config=_cfg(tmp_path)).is_empty()


def test_a_fully_published_session_is_returned(tmp_path, monkeypatch):
    import cnequity.adapters.exchange.margin_trading as adapter

    rows = adapter._finish(
        [
            {"symbol": "600000.SH", "trade_date": TD, "margin_balance": 1.0},
            {"symbol": "000001.SZ", "trade_date": TD, "margin_balance": 2.0},
        ]
    )
    monkeypatch.setattr(
        adapter,
        "fetch_exchange_margin_trading",
        lambda *a, **k: adapter.ExchangeMarginResult(
            rows=rows, covered=frozenset({"sse", "szse"}), failures={}
        ),
    )
    assert _fetch_margin_via_exchange(TD, config=_cfg(tmp_path)).height == 2


def test_the_exchange_is_the_default_owner(tmp_path):
    assert _margin_source(_cfg(tmp_path)) == "exchange"


def test_a_disabled_source_is_refused_rather_than_swapped(tmp_path):
    """Choosing who owns these rows is an operator's call, not a fallback."""
    cfg = _cfg(tmp_path, sources={"exchange": False})
    with pytest.raises(RuntimeError, match="exchange source disabled"):
        _margin_source(cfg)


def test_an_unknown_source_is_refused(tmp_path):
    cfg = _cfg(tmp_path, margin_trading_source="tushare")
    with pytest.raises(RuntimeError, match="unknown source"):
        _margin_source(cfg)


def test_the_vendor_path_stays_selectable(tmp_path):
    assert _margin_source(_cfg(tmp_path, margin_trading_source="eastmoney")) == "eastmoney"


def test_the_backfill_stamps_the_selected_source(tmp_path, monkeypatch):
    """Provenance follows the fetcher; a lake must never mislabel who wrote it."""
    import cnequity.adapters.exchange.margin_trading as adapter
    from cnequity.steps.capital import _backfill_margin_trading

    cfg = _cfg(tmp_path)
    cfg._backfill_start = date(2026, 6, 1)
    cfg._backfill_end = date(2026, 6, 2)
    monkeypatch.setattr(cfg, "rate_limit", lambda source: None)
    monkeypatch.setattr(
        adapter,
        "fetch_exchange_margin_trading",
        lambda d, **k: adapter.ExchangeMarginResult(
            rows=adapter._finish(
                [
                    {
                        "symbol": f"{600000 + i:06d}.SH",
                        "trade_date": d,
                        "margin_balance": 1.0,
                        "margin_buy": 1.0,
                        "short_balance": None,
                        "short_sell_volume": 0.0,
                    }
                    for i in range(60)
                ]
            ),
            covered=frozenset({"sse", "szse"}),
            failures={},
        ),
    )
    out = _backfill_margin_trading(cfg, date(2026, 7, 1), "run-x")
    assert out["days_fetched"] == 2
    staged = list((cfg.staging_root / "margin_trading").glob("**/*.parquet"))
    assert set(pl.read_parquet(staged[0])["source"]) == {"exchange"}


def test_the_grace_window_follows_the_publisher(tmp_path, monkeypatch):
    """A day still inside the publisher's lag is pending; past it, an error.

    The vendor answers same-day, so two days is right there. The exchanges add
    a business day for SZSE, which a weekend stretches to three — a step that
    errored after two would fail every Monday for no reason.
    """
    from cnequity.steps import capital as cap
    from cnequity.storage.state import StateStore

    cfg = _cfg(tmp_path)
    cfg.staging_root.mkdir(parents=True, exist_ok=True)
    StateStore(cfg.meta_root).set_date("margin_trading", date(2026, 6, 25))
    monkeypatch.setattr(cap, "shanghai_now", lambda: __import__("datetime").datetime(2026, 6, 30))
    monkeypatch.setattr(cap, "_fetch_margin_via_exchange", lambda d, *, config: pl.DataFrame())

    # Friday 2026-06-26 is four days back from Tuesday: inside the exchange
    # grace window, so the day stays pending rather than failing the run.
    assert cap.step_margin_trading(cfg, date(2026, 6, 26), "run-a", {})["rows_written"] == 0

    # The vendor path has no such lag, so the same day is an error there.
    cfg.margin_trading_source = "eastmoney"
    monkeypatch.setattr(cap, "fetch_margin_trading", lambda d, **k: pl.DataFrame())
    with pytest.raises(RuntimeError, match="no rows returned"):
        cap.step_margin_trading(cfg, date(2026, 6, 26), "run-b", {})
