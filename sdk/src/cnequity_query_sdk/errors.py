"""CNEquity Query Proxy SDK 的异常类型。"""

from __future__ import annotations


class QuerySDKError(Exception):
    """所有 SDK 异常的基类。"""


class TransportError(QuerySDKError):
    """请求没有获得 HTTP 响应，例如连接或超时失败。"""


class ResponseDecodeError(QuerySDKError):
    """服务返回了不符合公开 JSON 契约的成功响应。"""


class APIError(QuerySDKError):
    """服务返回的非成功 HTTP 响应。"""

    def __init__(self, status_code: int, detail: str, *, retry_after: float | None = None):
        self.status_code = status_code
        self.detail = detail
        self.retry_after = retry_after
        super().__init__(f"HTTP {status_code}: {detail}")


class AuthenticationError(APIError):
    """Bearer Token 缺失或无效。"""


class NotFoundError(APIError):
    """请求的资源不存在。"""


class ConflictError(APIError):
    """请求与数据覆盖或复权契约冲突。"""


class RequestTooLargeError(APIError):
    """请求超出代理文件扫描预算。"""


class RequestValidationError(APIError):
    """请求参数不符合代理的公开契约。"""


class RateLimitError(APIError):
    """代理当前没有空闲查询额度。"""


class ServiceUnavailableError(APIError):
    """代理或所需的本地数据湖暂时不可用。"""


__all__ = [
    "APIError",
    "AuthenticationError",
    "ConflictError",
    "NotFoundError",
    "QuerySDKError",
    "RateLimitError",
    "RequestTooLargeError",
    "RequestValidationError",
    "ResponseDecodeError",
    "ServiceUnavailableError",
    "TransportError",
]
