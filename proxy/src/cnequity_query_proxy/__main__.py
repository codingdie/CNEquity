"""独立进程入口。"""

from __future__ import annotations

import os

import uvicorn

from cnequity_query_proxy.app import create_app
from cnequity_query_proxy.settings import ProxySettings, SettingsError


def _port() -> int:
    raw = os.getenv("CNEQUITY_PROXY_PORT", "8790")
    try:
        value = int(raw)
    except ValueError as exc:
        raise SettingsError("CNEQUITY_PROXY_PORT 必须是整数") from exc
    if not 1 <= value <= 65535:
        raise SettingsError("CNEQUITY_PROXY_PORT 必须介于 1 和 65535 之间")
    return value


def _is_loopback(host: str) -> bool:
    return host.lower() in {"127.0.0.1", "::1", "localhost"}


def main() -> None:
    settings = ProxySettings.from_env()
    host = os.getenv("CNEQUITY_PROXY_HOST", "127.0.0.1").strip()
    if not host:
        raise SettingsError("CNEQUITY_PROXY_HOST 不能为空")
    if not _is_loopback(host) and not settings.api_key:
        raise SettingsError("非回环地址启动时必须设置 CNEQUITY_PROXY_API_KEY")
    uvicorn.run(create_app(settings), host=host, port=_port())


if __name__ == "__main__":
    main()
