"""现行东财经济日程的响应、窗口和原始证据回归测试。"""

from __future__ import annotations

import json
from datetime import date

import httpx
import polars as pl
import pytest

from cnequity.adapters.eastmoney import economic_calendar as calendar
from cnequity.adapters.eastmoney.datacenter import EastMoneyDatacenterError
from cnequity.config import Config
from cnequity.domain.schemas import ECONOMIC_CALENDAR_SCHEMA
from cnequity.steps.newsboard import step_economic_calendar
from cnequity.storage.raw_archive import RawPayloadArchive
from cnequity.storage.state import StateStore


def _raw_item(**overrides) -> dict:
    # 2026-09-09 实查 RPT_CPH_FECALENDAR 的经济数据行。
    row = {
        "START_DATE": "2026-09-07 16:00:00",
        "FE_CODE": "152000008922731014",
        "FE_NAME": "中国:央行外汇储备(报告期:2026年08月)",
        "STD_TYPE_CODE": "2",
        "CITY": "中国",
    }
    return {**row, **overrides}


def _patch_rows(monkeypatch, rows):
    seen = {}

    def fetch(_client, report, **kwargs):
        seen.update(report=report, **kwargs)
        return rows

    monkeypatch.setattr(calendar, "fetch_datacenter", fetch)
    return seen


def _payload(rows, *, pages=1, count=None):
    return {
        "success": True,
        "message": "ok",
        "code": 0,
        "result": {"pages": pages, "count": len(rows) if count is None else count, "data": rows},
    }


def _mock_transport(monkeypatch, payloads):
    remaining = iter(payloads)
    requests = []

    def handle(request):
        requests.append(request)
        return httpx.Response(200, json=next(remaining))

    monkeypatch.setattr(
        calendar,
        "EastMoneyClient",
        lambda **kwargs: httpx.Client(transport=httpx.MockTransport(handle)),
    )
    return requests


def test_current_report_maps_events_and_bounds_the_source_request(monkeypatch):
    rows = [
        _raw_item(),
        _raw_item(),
        _raw_item(START_DATE="2026-09-08 23:59:00"),
        _raw_item(START_DATE="2026-09-06 23:59:00"),
        _raw_item(START_DATE="2026-09-09 00:00:00"),
    ]
    seen = _patch_rows(monkeypatch, rows)
    frame = calendar.fetch_economic_calendar_window(date(2026, 9, 7), date(2026, 9, 8))
    assert seen["report"] == "RPT_CPH_FECALENDAR"
    assert seen["filter_expr"] == (
        "(STD_TYPE_CODE=\"2\")(START_DATE>='2026-09-07')(START_DATE<'2026-09-09')"
    )
    assert seen["sort_columns"] == "START_DATE,FE_CODE"
    assert seen["sort_types"] == "1,1"
    assert frame.height == 2
    row = frame.sort("event_date").row(0, named=True)
    assert row["event_date"] == date(2026, 9, 7)
    assert row["event_time"] == "16:00"
    assert row["country"] == "中国"
    assert row["indicator"] == "中国:央行外汇储备(报告期:2026年08月)"
    assert row["event_id"] == "2026-09-07|16:00|中国|中国:央行外汇储备(报告期:2026年08月)"


def test_schedule_does_not_invent_numeric_values_or_midnight_time(monkeypatch):
    _patch_rows(
        monkeypatch,
        [_raw_item(START_DATE="2026-09-08 00:00:00", FE_NAME="中国:CPI:同比(%)", CITY=None)],
    )
    frame = calendar.fetch_economic_calendar(date(2026, 9, 8))
    row = frame.row(0, named=True)
    assert row["event_time"] == ""
    assert row["country"] == ""
    for field in ["importance", "forecast", "previous", "actual", "unit"]:
        assert row[field] is None
        assert frame.schema[field] == ECONOMIC_CALENDAR_SCHEMA[field]


@pytest.mark.parametrize(
    "bad_rows",
    [
        None,
        [None],
        [{}],
        [_raw_item(START_DATE="not-a-date")],
        [_raw_item(START_DATE="2026-09-07T16:00:00Z")],
        [_raw_item(FE_NAME=" ")],
        [_raw_item(STD_TYPE_CODE="1")],
    ],
)
def test_malformed_or_wrong_category_responses_fail_closed(monkeypatch, bad_rows):
    _patch_rows(monkeypatch, bad_rows)
    with pytest.raises(EastMoneyDatacenterError, match="RPT_CPH_FECALENDAR"):
        calendar.fetch_economic_calendar(date(2026, 9, 8))


def test_empty_response_keeps_the_canonical_column_types(monkeypatch):
    _patch_rows(monkeypatch, [])
    frame = calendar.fetch_economic_calendar(date(2026, 9, 8))
    assert frame.is_empty()
    assert frame.schema == {
        key: value
        for key, value in ECONOMIC_CALENDAR_SCHEMA.items()
        if key not in {"source", "data_version", "fetched_at"}
    }


def test_invalid_window_is_rejected_before_network(monkeypatch):
    seen = _patch_rows(monkeypatch, [])
    with pytest.raises(ValueError, match="start must not be after end"):
        calendar.fetch_economic_calendar_window(date(2026, 9, 9), date(2026, 9, 8))
    assert seen == {}


def test_rolling_window_passes_config_and_run_id(monkeypatch):
    seen = {}

    def fetch(start, end, **kwargs):
        seen.update(start=start, end=end, **kwargs)
        return "sentinel"

    monkeypatch.setattr(calendar, "fetch_economic_calendar_window", fetch)
    config = object()
    assert (
        calendar.fetch_economic_calendar(date(2026, 9, 8), config=config, run_id="r") == "sentinel"
    )
    assert seen == {
        "start": date(2026, 9, 6),
        "end": date(2026, 9, 22),
        "config": config,
        "run_id": "r",
    }


def test_step_archives_exact_wire_and_validates_staging_without_future_watermark(
    tmp_path, monkeypatch
):
    payload = _payload([_raw_item(START_DATE="2026-09-22 10:00:00")])
    requests = _mock_transport(monkeypatch, [payload])
    config = Config(data_root=tmp_path / "lake")
    result = step_economic_calendar(config, date(2026, 9, 8), "calendar-run", {})
    assert result["rows_written"] == 1
    assert requests[0].url.params["reportName"] == "RPT_CPH_FECALENDAR"
    frame = pl.read_parquet(list((config.staging_root / "economic_calendar").rglob("*.parquet")))
    assert frame.schema == ECONOMIC_CALENDAR_SCHEMA
    assert frame["source"].to_list() == ["eastmoney"]
    assert frame["event_date"].to_list() == [date(2026, 9, 22)]
    state = StateStore(config.meta_root)
    assert state.get_date("economic_calendar") is None
    assert state.get_date("economic_calendar", "last_snapshot_date") == date(2026, 9, 8)
    archive = RawPayloadArchive(config.meta_root)
    records = archive.records("economic_calendar")
    assert len(records) == 1
    assert records[0].request_scope == "rolling:2026-09-06:2026-09-22"
    assert json.loads(archive.read(records[0])) == payload


@pytest.mark.parametrize(
    "payload",
    [
        {"success": False, "message": "报表配置不存在,RPT_CPH_FECALENDAR", "code": 9501},
        _payload([]),
        _payload([_raw_item(), _raw_item(FE_NAME=None)]),
    ],
)
def test_source_failure_empty_or_malformed_batch_never_writes_staging(
    tmp_path, monkeypatch, payload
):
    _mock_transport(monkeypatch, [payload])
    config = Config(data_root=tmp_path / "lake")
    with pytest.raises(RuntimeError):
        step_economic_calendar(config, date(2026, 9, 8), "bad-calendar", {})
    assert not list(config.staging_root.rglob("*.parquet"))
    assert StateStore(config.meta_root).get_date("economic_calendar", "last_snapshot_date") is None


def test_partial_pagination_does_not_publish_a_partial_calendar(tmp_path, monkeypatch):
    rows = [_raw_item(FE_CODE=str(i), FE_NAME=f"指标{i}") for i in range(500)]
    empty_page = {"success": False, "message": "返回数据为空", "code": 9201}
    requests = _mock_transport(monkeypatch, [_payload(rows, pages=2, count=501), *[empty_page] * 3])
    monkeypatch.setattr("cnequity.adapters.eastmoney.datacenter.time.sleep", lambda _: None)
    config = Config(data_root=tmp_path / "lake")
    with pytest.raises(EastMoneyDatacenterError, match="truncated"):
        step_economic_calendar(config, date(2026, 9, 8), "partial-calendar", {})
    assert [r.url.params["pageNumber"] for r in requests] == ["1", "2", "2", "2"]
    assert not list(config.staging_root.rglob("*.parquet"))
    assert StateStore(config.meta_root).get_date("economic_calendar", "last_snapshot_date") is None
