"""跨窗口补齐只能复用成功、完整且保留来源的 staging 数据。"""

from datetime import date

import polars as pl
import pytest

from cnequity.domain.schemas import with_provenance
from cnequity.orchestrator.manifest import Manifest
from cnequity.steps import bars
from cnequity.storage import StagingWriter


@pytest.mark.parametrize(
    "status,window_start,window_end,days,expected",
    [
        ("success", "2026-09-18", "2026-09-30", [28, 29, 30], True),
        ("success", "2026-09-29", "2026-09-30", [29, 30], True),
        ("failed", "2026-09-18", "2026-09-30", [29, 30], False),
        ("running", "2026-09-18", "2026-09-30", [29, 30], False),
        ("success", "2026-09-18", "2026-09-30", [30], False),
        ("success", "2026-09-18", "2026-09-29", [28, 29], False),
    ],
)
def test_reuse_checks_batch_and_requested_sessions(
    config, monkeypatch, status, window_start, window_end, days, expected
):
    manifest = Manifest(config.manifest_path)
    old = manifest.start_run("backfill")
    new = manifest.start_run("backfill")
    manifest.start_batch(
        old,
        "bars",
        "daily_bars",
        "daily_bars",
        symbols=["600584.SH"],
        window_start=window_start,
        window_end=window_end,
    )
    if status != "running":
        manifest.finish_batch(old, "bars", status)
    frame = with_provenance(
        pl.DataFrame(
            {
                "symbol": ["600584.SH"] * len(days),
                "trade_date": [date(2026, 9, day) for day in days],
                "open": [10.0] * len(days),
                "high": [10.0] * len(days),
                "low": [10.0] * len(days),
                "close": [10.0] * len(days),
                "volume": [100] * len(days),
                "amount": [1000.0] * len(days),
            }
        ),
        source="tdx_protocol",
        data_version="v1",
    )
    writer = StagingWriter(config.staging_root)
    frame = pl.read_parquet(writer.write_batch("daily_bars", old, "bars", frame))
    monkeypatch.setattr(
        bars, "list_trading_dates", lambda *args: [date(2026, 9, 29), date(2026, 9, 30)]
    )
    reused = bars._reuse_successful_daily_bars(
        config, new, ["600584.SH"], date(2026, 9, 29), date(2026, 9, 30)
    )
    assert reused == ({"600584.SH"} if expected else set())
    files = writer.list_run_files("daily_bars", new)
    assert bool(files) is expected
    if expected:
        actual = pl.read_parquet(files[0]).sort("trade_date")
        assert actual.equals(
            frame.filter(pl.col("trade_date") >= date(2026, 9, 29)).sort("trade_date")
        )
    # 原默认调用仍只匹配完全相同窗口。
    exact = manifest.get_successful_batches("daily_bars", "2026-09-29", "2026-09-30")
    assert bool(exact) is (status == "success" and window_start == "2026-09-29")
