"""代理默认运行配置的契约。"""

from __future__ import annotations

from pathlib import Path

from cnequity_query_proxy.settings import ProxySettings


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
