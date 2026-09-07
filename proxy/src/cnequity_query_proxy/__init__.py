"""独立、只读的 CNEquity Parquet 查询代理。"""

from cnequity_query_proxy.app import create_app
from cnequity_query_proxy.settings import ProxySettings

__all__ = ["ProxySettings", "create_app"]
