"""东方财富经济数据发布日程的滚动快照，不提供预期值或公布值。"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta

import polars as pl

from cnequity.adapters.eastmoney.datacenter import EastMoneyDatacenterError, fetch_datacenter
from cnequity.adapters.eastmoney.em_auth import EastMoneyClient
from cnequity.adapters.eastmoney.raw import configured_archive
from cnequity.domain.schemas import ECONOMIC_CALENDAR_SCHEMA

# 来自 https://data.eastmoney.com/cjrl/default.html 的现行报表。
# STD_TYPE_CODE=2 仅含经济数据，不混入财经会议和其他事件。
_REPORT = "RPT_CPH_FECALENDAR"
_COLUMNS = "START_DATE,FE_CODE,FE_NAME,STD_TYPE_CODE,CITY"
_FRAME_SCHEMA = {
    name: dtype
    for name, dtype in ECONOMIC_CALENDAR_SCHEMA.items()
    if name not in {"source", "data_version", "fetched_at"}
}


def fetch_economic_calendar_window(
    start: date,
    end: date,
    *,
    config=None,
    run_id: str | None = None,
) -> pl.DataFrame:
    """获取 [start, end] 内的现行日程；源失败或畸形行不能认证为空成功。"""
    if start > end:
        raise ValueError("economic_calendar: start must not be after end")
    request_scope = f"rolling:{start.isoformat()}:{end.isoformat()}"
    end_exclusive = end + timedelta(days=1)
    client_kwargs = {"config": config} if config is not None else {}
    with EastMoneyClient(**client_kwargs) as client:
        archive = configured_archive(
            config,
            "economic_calendar",
            run_id=run_id,
            request_scope=request_scope,
        )
        data = fetch_datacenter(
            client,
            _REPORT,
            columns=_COLUMNS,
            filter_expr=(
                '(STD_TYPE_CODE="2")'
                f"(START_DATE>='{start.isoformat()}')"
                f"(START_DATE<'{end_exclusive.isoformat()}')"
            ),
            page_size=500,
            sort_columns="START_DATE,FE_CODE",
            sort_types="1,1",
            archive=archive,
            archive_dataset="economic_calendar",
            archive_run_id=run_id,
            archive_request_scope=request_scope,
        )

    if not isinstance(data, list):
        raise EastMoneyDatacenterError(f"{_REPORT}: expected an event list")
    rows: list[dict] = []
    for item in data:
        if (
            not isinstance(item, dict)
            or not {"START_DATE", "FE_NAME", "STD_TYPE_CODE"} <= item.keys()
        ):
            raise EastMoneyDatacenterError(f"{_REPORT}: missing event date/name/type columns")
        try:
            published = datetime.fromisoformat(str(item["START_DATE"]))
        except ValueError as exc:
            raise EastMoneyDatacenterError(f"{_REPORT}: invalid START_DATE") from exc
        if published.tzinfo is not None:
            raise EastMoneyDatacenterError(f"{_REPORT}: expected a local calendar timestamp")
        event_date = published.date()
        if event_date < start or event_date > end:
            continue
        if str(item["STD_TYPE_CODE"]) != "2":
            raise EastMoneyDatacenterError(f"{_REPORT}: response contains a non-economic event")
        indicator = item["FE_NAME"]
        if not isinstance(indicator, str) or not indicator.strip():
            raise EastMoneyDatacenterError(f"{_REPORT}: missing economic indicator name")
        indicator = indicator.strip()
        country = str(item.get("CITY") or "").strip()
        # 东财页面把 00:00 显示为仅日期，不能将它认证为明确的午夜发布时间。
        event_time = "" if published.time() == time.min else published.strftime("%H:%M")
        rows.append(
            {
                "event_id": f"{event_date.isoformat()}|{event_time}|{country}|{indicator}",
                "event_date": event_date,
                "event_time": event_time,
                "country": country,
                "indicator": indicator,
                # 日程表没有这些数值；保持已有可空列，不从名称或其他来源推算。
                "importance": None,
                "forecast": None,
                "previous": None,
                "actual": None,
                "unit": None,
            }
        )

    return pl.DataFrame(rows, schema=_FRAME_SCHEMA).unique(subset=["event_id"], keep="last")


def fetch_economic_calendar(
    trade_date: date,
    *,
    config=None,
    run_id: str | None = None,
) -> pl.DataFrame:
    """Rolling window [trade_date-2, trade_date+14] for snapshot daily runs."""
    start = trade_date - timedelta(days=2)
    end = trade_date + timedelta(days=14)
    return fetch_economic_calendar_window(start, end, config=config, run_id=run_id)
