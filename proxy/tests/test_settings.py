"""代理默认运行配置的契约。"""

from __future__ import annotations

from pathlib import Path

import pytest

from cnequity_query_proxy.settings import ProxySettings, SettingsError


def test_default_result_cache_is_sized_for_repeated_research_queries(tmp_path: Path):
    settings = ProxySettings(data_root=tmp_path)

    assert settings.cache_ttl_seconds == 60.0
    assert settings.cache_entries == 512


def test_environment_overrides_result_cache_defaults(monkeypatch, tmp_path: Path):
    monkeypatch.setenv("CNEQUITY_PROXY_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("CNEQUITY_PROXY_CACHE_TTL_SECONDS", "120")
    monkeypatch.setenv("CNEQUITY_PROXY_CACHE_ENTRIES", "1024")

    settings = ProxySettings.from_env()

    assert settings.cache_ttl_seconds == 120.0
    assert settings.cache_entries == 1024


def test_environment_configures_reverse_proxy_path_prefix(monkeypatch, tmp_path: Path):
    monkeypatch.setenv("CNEQUITY_PROXY_DATA_ROOT", str(tmp_path))
    monkeypatch.setenv("CNEQUITY_PROXY_ROOT_PATH", "/query")

    settings = ProxySettings.from_env()

    assert settings.root_path == "/query"


@pytest.mark.parametrize(
    "root_path",
    ("/", "query", "/query/", "/query//v1", "/query/../admin", "/query?debug=1"),
)
def test_root_path_requires_a_canonical_non_root_prefix(tmp_path: Path, root_path: str):
    with pytest.raises(SettingsError, match="root_path"):
        ProxySettings(data_root=tmp_path, root_path=root_path)
