"""全市场原始 Parquet 的流式批量下载。"""

from __future__ import annotations

import os
import stat
import tarfile
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date
from pathlib import Path

from cnequity_query_proxy.kline import LakeUnavailable, QueryFailed
from cnequity_query_proxy.partitions import PartitionIndex
from cnequity_query_proxy.settings import ProxySettings

_CHUNK_BYTES = 1024 * 1024


class NoBatchParquetFiles(LookupError):
    """所选窗口没有可下载的原始 Parquet 文件。"""


@dataclass(frozen=True)
class _ArchiveMember:
    path: Path
    archive_name: str
    size: int
    mtime: int
    mode: int


class ParquetBatchArchive:
    """按需产生 TAR 字节流的原始 Parquet 归档。"""

    def __init__(
        self,
        members: tuple[_ArchiveMember, ...],
        *,
        data_bytes: int,
        data_label: str,
    ):
        self._members = members
        self.data_bytes = data_bytes
        self._data_label = data_label

    @property
    def file_count(self) -> int:
        return len(self._members)

    def iter_bytes(self) -> Iterator[bytes]:
        """按 TAR 格式逐块输出原始 Parquet 文件，不将文件读入内存。"""
        for member in self._members:
            yield from self._iter_member(member, data_label=self._data_label)
        yield b"\0" * (tarfile.BLOCKSIZE * 2)

    @staticmethod
    def _iter_member(member: _ArchiveMember, *, data_label: str) -> Iterator[bytes]:
        try:
            with member.path.open("rb") as source:
                current = os.fstat(source.fileno())
                if current.st_size != member.size or int(current.st_mtime) != member.mtime:
                    raise QueryFailed(f"{data_label}文件在批量下载前发生变化，请重试")

                info = tarfile.TarInfo(member.archive_name)
                info.size = member.size
                info.mode = member.mode
                info.mtime = member.mtime
                info.type = tarfile.REGTYPE
                yield info.tobuf(format=tarfile.PAX_FORMAT)

                remaining = member.size
                while remaining:
                    chunk = source.read(min(_CHUNK_BYTES, remaining))
                    if not chunk:
                        raise QueryFailed(f"{data_label}文件在批量下载时被截断，请重试")
                    remaining -= len(chunk)
                    yield chunk

                padding = (-member.size) % tarfile.BLOCKSIZE
                if padding:
                    yield b"\0" * padding
        except OSError as exc:
            raise QueryFailed(f"{data_label}文件在批量下载时不可读，请重试") from exc


class ParquetBatchRepository:
    """选择一个受控数据集的原始 Parquet 文件，不执行 SQL 或数据重编码。"""

    def __init__(
        self,
        settings: ProxySettings,
        *,
        root: Path,
        dataset_name: str,
        data_label: str,
        partition_key: str = "trade_date",
    ):
        self._settings = settings
        self._root = root
        self._data_label = data_label
        self._partitions = PartitionIndex(
            root,
            ttl_seconds=settings.cache_ttl_seconds,
            dataset_name=dataset_name,
            partition_key=partition_key,
        )

    def prepare(self, *, start: date, end: date) -> tuple[tuple[_ArchiveMember, ...], int]:
        for attempt in range(2):
            try:
                paths = self._partitions.files_for(start, end)
            except FileNotFoundError as exc:
                raise LakeUnavailable(str(exc)) from exc
            if not paths:
                raise NoBatchParquetFiles(f"所选窗口没有{self._data_label} Parquet 文件")
            try:
                members = tuple(self._archive_member(path) for path in paths)
            except OSError as exc:
                if attempt == 0:
                    self._partitions.refresh()
                    continue
                raise QueryFailed("Parquet 文件在批量下载准备中变化，请重试") from exc

            data_bytes = sum(member.size for member in members)
            return members, data_bytes

        raise QueryFailed("Parquet 文件在批量下载准备中变化，请重试")  # pragma: no cover

    def _archive_member(self, path: Path) -> _ArchiveMember:
        resolved_root = self._settings.data_root.resolve()
        resolved_dataset_root = self._root.resolve()
        resolved_path = path.resolve(strict=True)
        try:
            resolved_path.relative_to(resolved_dataset_root)
            archive_name = resolved_path.relative_to(resolved_root).as_posix()
        except ValueError as exc:
            raise OSError(f"Parquet 文件不在受控目录内: {path}") from exc

        file_stat = resolved_path.stat()
        if not stat.S_ISREG(file_stat.st_mode):
            raise OSError(f"Parquet 文件不是普通文件: {path}")
        return _ArchiveMember(
            path=resolved_path,
            archive_name=archive_name,
            size=file_stat.st_size,
            mtime=int(file_stat.st_mtime),
            mode=stat.S_IMODE(file_stat.st_mode),
        )


class ParquetBatchService:
    """原始 Parquet 批量下载服务，不施加请求窗口或并发额度。"""

    def __init__(
        self,
        settings: ProxySettings,
        *,
        root: Path,
        dataset_name: str,
        data_label: str,
        partition_key: str = "trade_date",
    ):
        self._repository = ParquetBatchRepository(
            settings,
            root=root,
            dataset_name=dataset_name,
            data_label=data_label,
            partition_key=partition_key,
        )
        self._data_label = data_label

    def open_archive(self, *, start: date, end: date) -> ParquetBatchArchive:
        members, data_bytes = self._repository.prepare(start=start, end=end)
        return ParquetBatchArchive(
            members,
            data_bytes=data_bytes,
            data_label=self._data_label,
        )


class MarketDailyBarsService(ParquetBatchService):
    """全市场日线批量下载的兼容入口。"""

    def __init__(
        self,
        settings: ProxySettings,
    ):
        super().__init__(
            settings,
            root=settings.daily_bars_root,
            dataset_name="daily_bars",
            data_label="日线",
        )


# 兼容内部已使用的名称；新代码应使用 NoBatchParquetFiles。
NoMarketDailyBars = NoBatchParquetFiles
