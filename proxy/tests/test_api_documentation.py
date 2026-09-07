"""确保人工 API 文档与实际公开路由同步。"""

from __future__ import annotations

import re
from pathlib import Path

from fastapi.routing import APIRoute

from cnequity_query_proxy.app import create_app

_DOCUMENTED_ROUTE = re.compile(r"^\|\s*`([A-Z]+ [^`]+)`\s*\|", re.MULTILINE)
_ENDPOINT_SECTION = re.compile(r"^###\s+`([A-Z]+ [^`]+)`\s*$", re.MULTILINE)
_PARAMETER_ROW = re.compile(r"^\|\s*`([^`]+)`\s*\|\s*(?:path|query)\s*\|", re.MULTILINE)


def _documented_parameters(content: str) -> dict[str, set[str]]:
    sections = list(_ENDPOINT_SECTION.finditer(content))
    parameters: dict[str, set[str]] = {}
    for index, section in enumerate(sections):
        next_start = sections[index + 1].start() if index + 1 < len(sections) else len(content)
        parameters[section.group(1)] = set(
            _PARAMETER_ROW.findall(content[section.end() : next_start])
        )
    return parameters


def test_api_documentation_lists_exactly_the_public_business_routes(settings):
    document = Path(__file__).parents[1] / "docs" / "API.md"
    content = document.read_text(encoding="utf-8")
    documented_routes = set(_DOCUMENTED_ROUTE.findall(content))
    documented_sections = set(_ENDPOINT_SECTION.findall(content))
    app = create_app(settings)
    actual_routes = {
        f"{method} {route.path}"
        for route in app.routes
        if isinstance(route, APIRoute)
        and (route.path == "/healthz" or route.path.startswith("/v1/"))
        for method in route.methods or ()
    }

    assert documented_routes == actual_routes
    assert documented_sections == actual_routes

    openapi = app.openapi()
    actual_parameters = {
        f"{method} {route.path}": {
            parameter["name"]
            for parameter in openapi["paths"]
            .get(route.path, {})
            .get(method.lower(), {})
            .get("parameters", [])
            if parameter["in"] in {"path", "query"}
        }
        for route in app.routes
        if isinstance(route, APIRoute)
        and (route.path == "/healthz" or route.path.startswith("/v1/"))
        for method in route.methods or ()
    }

    assert _documented_parameters(content) == actual_parameters
