from datetime import date

import polars as pl

from cnequity.config import Config
from cnequity.domain.datasets import DATASETS
from cnequity.quality.verify import last_contiguous_dense_date
from cnequity.steps.finalize import step_derive_industry_index
from cnequity.storage.revisions import RevisionStore
from cnequity.storage.state import StateStore


def test_industry_index_empty_derive_is_retryable_warning(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "data")
    monkeypatch.setattr(
        "cnequity.derive.industry_index.derive_industry_index",
        lambda *_args, **_kwargs: {
            "rows": 0,
            "note": "no priced returns in [2026-08-07, 2026-08-07]",
        },
    )

    result = step_derive_industry_index(cfg, date(2026, 8, 7), "run-empty", {})

    assert result["status"] == "warning"
    finding = result["context_updates"]["audit_findings"][0]
    assert finding["check"] == "derived_empty"
    assert "no priced returns" in finding["message"]


def test_industry_index_without_membership_is_isolated_noop(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "data")
    monkeypatch.setattr(
        "cnequity.derive.industry_index.derive_industry_index",
        lambda *_args, **_kwargs: {"rows": 0, "note": "no 申万 membership rows"},
    )

    result = step_derive_industry_index(cfg, date(2026, 8, 7), "run-no-membership", {})

    assert result == {"rows_read": 0, "rows_written": 0}


def test_industry_index_already_current_remains_success_noop(tmp_path, monkeypatch):
    cfg = Config(data_root=tmp_path / "data")
    monkeypatch.setattr(
        "cnequity.derive.industry_index.derive_industry_index",
        lambda *_args, **_kwargs: {"rows": 0, "note": "industry_index already current"},
    )

    result = step_derive_industry_index(cfg, date(2026, 8, 7), "run-current", {})

    assert result == {"rows_read": 0, "rows_written": 0}


def test_industry_index_publishes_before_advancing_watermark(tmp_path, monkeypatch):
    """The COW reader must see the new generation before its watermark moves."""
    cfg = Config(data_root=tmp_path / "data")
    first_day = date(2026, 8, 3)
    second_day = date(2026, 8, 4)

    calendar_root = cfg.curated_root / "trading_calendar" / "trade_date=2026"
    calendar_root.mkdir(parents=True)
    pl.DataFrame(
        {
            "trade_date": [first_day, second_day],
            "is_trading": [True, True],
            "source": ["seed", "seed"],
            "data_version": ["v1", "v1"],
            "fetched_at": ["2026-08-04T00:00:00+00:00"] * 2,
        }
    ).write_parquet(calendar_root / "part-000.parquet")

    def industry_rows(day: date) -> pl.DataFrame:
        return pl.DataFrame(
            {
                "trade_date": [day, day],
                "industry_code": ["240301", "240301"],
                "level": ["L3", "L3"],
                "weighting": ["equal", "amount"],
                "ret": [0.01, 0.01],
                "n_members": [2, 2],
                "n_priced": [2, 2],
                "n_excluded": [0, 0],
                "amount": [1.0e8, 1.0e8],
            }
        )

    industry_root = cfg.derived_root / "industry_index" / "trade_date=2026"
    industry_root.mkdir(parents=True)
    industry_rows(first_day).write_parquet(industry_root / "part-000.parquet")
    revisions = RevisionStore(cfg.meta_root, cfg.curated_root, cfg.derived_root)
    revisions.ensure_current("industry_index")
    StateStore(cfg.meta_root).set_date("industry_index", first_day)

    monkeypatch.setattr(
        "cnequity.derive.industry_index.compute_industry_index",
        lambda *_args, **_kwargs: industry_rows(second_day),
    )

    result = step_derive_industry_index(
        cfg,
        second_day,
        "run-cow-watermark",
        {"derive_start": second_day, "derive_end": second_day},
    )

    assert result["dataset_revision"]["revision"] == 1
    assert last_contiguous_dense_date(cfg, DATASETS["industry_index"]) == second_day
    assert StateStore(cfg.meta_root).get_date("industry_index") == second_day


def test_small_adjustment_factor_failure_is_warning_and_retryable(tmp_path, monkeypatch):
    from cnequity.derive.adj_factors import AdjFactorsResult
    from cnequity.steps.finalize import step_derive_adj_factors

    cfg = Config(data_root=tmp_path / "data")
    monkeypatch.setattr(
        "cnequity.derive.adj_factors.compute_adj_factors",
        lambda *_args, **_kwargs: AdjFactorsResult(
            rows=99,
            task_count=100,
            failed=["600001.SH:hfq"],
            findings=[],
        ),
    )

    result = step_derive_adj_factors(cfg, date(2026, 8, 7), "run-adj-warning", {})

    assert result["status"] == "warning"
    assert result["failed_tasks"] == 1
