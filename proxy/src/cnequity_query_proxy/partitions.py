"""只解析 daily_bars 的本地目录布局，用于在扫描前剪枝。"""

from __future__ import annotations

import threading
import time
from calendar import monthrange
from dataclasses import dataclass
from datetime import date
from pathlib import Path


@dataclass(frozen=True)
class PartitionFiles:
    """一个日、月或年分区及其 Parquet 文件。"""

    start: date
    end: date
    files: tuple[Path, ...]

    def overlaps(self, start: date, end: date) -> bool:
        return self.start <= end and start <= self.end


def _parse_partition(value: str) -> tuple[date, date] | None:
    """解析兼容旧布局的 YYYY-MM-DD、YYYY-MM 和 YYYY 目录名。"""
    pieces = value.split("-")
    try:
        if len(pieces) == 3:
            day = date.fromisoformat(value)
            return day, day
        if len(pieces) == 2:
            year, month = (int(piece) for piece in pieces)
            return date(year, month, 1), date(year, month, monthrange(year, month)[1])
        if len(pieces) == 1 and len(value) == 4:
            year = int(value)
            return date(year, 1, 1), date(year, 12, 31)
    except ValueError:
        return None
    return None


class PartitionIndex:
    """短 TTL 的分区目录索引；只缓存路径，绝不写入数据湖。"""

    def __init__(
        self,
        root: Path,
        *,
        ttl_seconds: float,
        dataset_name: str,
        partition_key: str = "trade_date",
    ):
        self._root = root
        self._ttl_seconds = ttl_seconds
        self._dataset_name = dataset_name
        self._partition_key = partition_key
        self._lock = threading.Lock()
        self._entries: tuple[PartitionFiles, ...] = ()
        self._root_files: tuple[Path, ...] = ()
        self._root_mtime_ns: int | None = None
        self._expires_at = 0.0

    def refresh(self) -> None:
        with self._lock:
            self._expires_at = 0.0

    def files_for(self, start: date, end: date) -> list[Path]:
        entries, root_files = self._snapshot()
        files = list(root_files)
        for entry in entries:
            if entry.overlaps(start, end):
                files.extend(entry.files)
        return files

    def latest_files(self) -> list[Path]:
        """返回最新分区的文件，绝不为摘要递归扫描历史分区。"""
        entries, root_files = self._snapshot()
        if entries:
            latest = max(entries, key=lambda entry: (entry.end, entry.start))
            return list(latest.files)
        return list(root_files)

    def snapshot_files_for(
        self, start: date, end: date, *, include_previous_snapshot: bool = False
    ) -> list[Path]:
        """返回窗口内完整日分区，可附加严格早于窗口的最近日分区。

        完整性依赖 curated 写入门禁；不按板块拼接不同日期，也不使用
        无日期文件或月、年分区推断历史快照。
        """
        entries, _ = self._snapshot()
        daily = [entry for entry in entries if entry.start == entry.end]
        selected = [entry for entry in daily if start <= entry.start <= end]
        if include_previous_snapshot:
            previous = [entry for entry in daily if entry.end < start]
            if previous:
                selected.insert(0, max(previous, key=lambda entry: entry.end))
        return [path for entry in selected for path in entry.files]

    def _snapshot(self) -> tuple[tuple[PartitionFiles, ...], tuple[Path, ...]]:
        if not self._root.is_dir():
            raise FileNotFoundError(f"{self._dataset_name} 目录不存在: {self._root}")

        root_mtime_ns = self._root.stat().st_mtime_ns
        now = time.monotonic()
        with self._lock:
            if now < self._expires_at and root_mtime_ns == self._root_mtime_ns:
                return self._entries, self._root_files

            entries: list[PartitionFiles] = []
            for child in self._root.iterdir():
                prefix = f"{self._partition_key}="
                if not child.is_dir() or not child.name.startswith(prefix):
                    continue
                bounds = _parse_partition(child.name.removeprefix(prefix))
                if bounds is None:
                    continue
                files = tuple(sorted(child.glob("*.parquet")))
                if files:
                    entries.append(PartitionFiles(*bounds, files))

            self._entries = tuple(sorted(entries, key=lambda entry: entry.start))
            self._root_files = tuple(sorted(self._root.glob("*.parquet")))
            self._root_mtime_ns = root_mtime_ns
            self._expires_at = now + self._ttl_seconds
            return self._entries, self._root_files
