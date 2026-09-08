"""代理自身的环境变量配置，不复用数据湖项目的配置对象。"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Literal


class SettingsError(ValueError):
    """环境变量缺失或格式错误。"""


_ROOT_PATH_SEGMENT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._~-]*$")


def _positive_int(name: str, default: int, *, minimum: int = 1, maximum: int = 1_000_000) -> int:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        value = int(raw)
    except ValueError as exc:
        raise SettingsError(f"{name} 必须是整数") from exc
    if not minimum <= value <= maximum:
        raise SettingsError(f"{name} 必须介于 {minimum} 和 {maximum} 之间")
    return value


def _non_negative_float(name: str, default: float, *, maximum: float) -> float:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        value = float(raw)
    except ValueError as exc:
        raise SettingsError(f"{name} 必须是数字") from exc
    if not 0 <= value <= maximum:
        raise SettingsError(f"{name} 必须介于 0 和 {maximum} 之间")
    return value


def _validate_root_path(root_path: str) -> None:
    """限制反向代理路径前缀为可安全嵌入 URL 的规范路径。"""
    if not isinstance(root_path, str):
        raise SettingsError("root_path 必须是字符串")
    if not root_path:
        return
    if root_path != root_path.strip():
        raise SettingsError("root_path 不能包含首尾空白")
    if root_path == "/" or not root_path.startswith("/") or root_path.endswith("/"):
        raise SettingsError("root_path 必须为空或以 / 开头且不带尾随 / 的路径")

    segments = root_path[1:].split("/")
    if any(
        segment in {".", ".."} or not _ROOT_PATH_SEGMENT.fullmatch(segment) for segment in segments
    ):
        raise SettingsError("root_path 只能包含由字母、数字、点、连字符和下划线组成的路径段")


def _root_path_from_env() -> str:
    raw = os.getenv("CNEQUITY_PROXY_ROOT_PATH", "")
    if not raw.strip():
        return ""
    _validate_root_path(raw)
    return raw


@dataclass(frozen=True)
class ProxySettings:
    """查询代理的全部运行配置。"""

    data_root: Path
    api_key: str | None = None
    root_path: str = ""
    default_window_days: int = 365
    max_window_days: int = 3660
    default_minute_window_days: int = 5
    max_minute_window_days: int = 31
    default_five_minute_window_days: int = 20
    max_five_minute_window_days: int = 90
    default_limit: int = 1000
    max_bars: int = 5000
    max_files: int = 6000
    cache_ttl_seconds: float = 60.0
    cache_entries: int = 512
    max_concurrent_queries: int = 4
    duckdb_threads: int = 1

    def __post_init__(self) -> None:
        _validate_root_path(self.root_path)
        if self.default_window_days < 1 or self.max_window_days < self.default_window_days:
            raise SettingsError("默认窗口必须为正数且不大于最大窗口")
        if (
            self.default_minute_window_days < 1
            or self.max_minute_window_days < self.default_minute_window_days
        ):
            raise SettingsError("分钟线默认窗口必须为正数且不大于最大窗口")
        if (
            self.default_five_minute_window_days < 1
            or self.max_five_minute_window_days < self.default_five_minute_window_days
        ):
            raise SettingsError("5 分钟线默认窗口必须为正数且不大于最大窗口")
        if not 1 <= self.default_limit <= self.max_bars:
            raise SettingsError("默认每页条数必须介于 1 和最大条数之间")
        if (
            min(
                self.max_files,
                self.cache_entries,
                self.max_concurrent_queries,
                self.duckdb_threads,
            )
            < 1
        ):
            raise SettingsError("文件数、缓存数和并发配置必须为正数")
        if self.cache_ttl_seconds < 0:
            raise SettingsError("缓存 TTL 不能为负数")

    @property
    def daily_bars_root(self) -> Path:
        return self.data_root / "curated" / "daily_bars"

    @property
    def minute_bars_root(self) -> Path:
        return self.data_root / "curated" / "minute_bars"

    @property
    def minute_bars_5m_root(self) -> Path:
        return self.data_root / "curated" / "minute_bars_5m"

    @property
    def trading_calendar_root(self) -> Path:
        return self.data_root / "curated" / "trading_calendar"

    @property
    def trading_status_root(self) -> Path:
        return self.data_root / "curated" / "trading_status"

    @property
    def dragon_tiger_root(self) -> Path:
        return self.data_root / "curated" / "dragon_tiger"

    @property
    def valuation_metrics_root(self) -> Path:
        return self.data_root / "curated" / "valuation_metrics"

    @property
    def industry_members_root(self) -> Path:
        return self.data_root / "curated" / "industry_members"

    @property
    def sector_members_root(self) -> Path:
        return self.data_root / "curated" / "sector_members"

    @property
    def index_constituents_root(self) -> Path:
        return self.data_root / "curated" / "index_constituents"

    @property
    def fund_flow_root(self) -> Path:
        return self.data_root / "curated" / "fund_flow"

    @property
    def analyst_consensus_root(self) -> Path:
        return self.data_root / "curated" / "analyst_consensus"

    @property
    def hot_rank_root(self) -> Path:
        return self.data_root / "curated" / "hot_rank"

    @property
    def sentiment_scores_root(self) -> Path:
        return self.data_root / "curated" / "sentiment_scores"

    def intraday_bars_root(self, interval: Literal["1m", "5m"]) -> Path:
        """返回指定日内周期独立的 Parquet 根目录。"""
        if interval == "1m":
            return self.minute_bars_root
        return self.minute_bars_5m_root

    def intraday_window_days(self, interval: Literal["1m", "5m"]) -> tuple[int, int]:
        """返回指定日内周期的默认和最大自然日窗口。"""
        if interval == "1m":
            return self.default_minute_window_days, self.max_minute_window_days
        return self.default_five_minute_window_days, self.max_five_minute_window_days

    @property
    def adj_factors_root(self) -> Path:
        return self.data_root / "derived" / "adj_factors"

    @property
    def instruments_file(self) -> Path:
        """证券主数据的固定 canonical Parquet 文件。"""
        return self.data_root / "curated" / "instruments" / "part-merged.parquet"

    @classmethod
    def from_env(cls) -> ProxySettings:
        raw_root = os.getenv("CNEQUITY_PROXY_DATA_ROOT", "").strip()
        if not raw_root:
            raise SettingsError("必须设置 CNEQUITY_PROXY_DATA_ROOT")
        api_key = os.getenv("CNEQUITY_PROXY_API_KEY", "").strip() or None
        return cls(
            data_root=Path(raw_root).expanduser().resolve(),
            api_key=api_key,
            root_path=_root_path_from_env(),
            default_window_days=_positive_int("CNEQUITY_PROXY_DEFAULT_WINDOW_DAYS", 365),
            max_window_days=_positive_int("CNEQUITY_PROXY_MAX_WINDOW_DAYS", 3660),
            default_minute_window_days=_positive_int(
                "CNEQUITY_PROXY_DEFAULT_MINUTE_WINDOW_DAYS", 5
            ),
            max_minute_window_days=_positive_int("CNEQUITY_PROXY_MAX_MINUTE_WINDOW_DAYS", 31),
            default_five_minute_window_days=_positive_int(
                "CNEQUITY_PROXY_DEFAULT_5M_WINDOW_DAYS", 20
            ),
            max_five_minute_window_days=_positive_int("CNEQUITY_PROXY_MAX_5M_WINDOW_DAYS", 90),
            default_limit=_positive_int("CNEQUITY_PROXY_DEFAULT_LIMIT", 1000, maximum=100_000),
            max_bars=_positive_int("CNEQUITY_PROXY_MAX_BARS", 5000, maximum=100_000),
            max_files=_positive_int("CNEQUITY_PROXY_MAX_FILES", 6000, maximum=100_000),
            cache_ttl_seconds=_non_negative_float(
                "CNEQUITY_PROXY_CACHE_TTL_SECONDS", 60.0, maximum=3600.0
            ),
            cache_entries=_positive_int("CNEQUITY_PROXY_CACHE_ENTRIES", 512, maximum=100_000),
            max_concurrent_queries=_positive_int(
                "CNEQUITY_PROXY_MAX_CONCURRENT_QUERIES", 4, maximum=1024
            ),
            duckdb_threads=_positive_int("CNEQUITY_PROXY_DUCKDB_THREADS", 1, maximum=128),
        )
