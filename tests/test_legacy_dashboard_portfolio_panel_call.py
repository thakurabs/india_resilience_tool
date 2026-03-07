"""
Regression test: keep details_runtime's render_portfolio_panel call in sync
with portfolio_ui.render_portfolio_panel's required keyword-only arguments.
"""

from __future__ import annotations

import ast
import inspect
from pathlib import Path


def test_legacy_dashboard_render_portfolio_panel_call_includes_required_kwonly_args() -> None:
    from india_resilience_tool.app.portfolio_ui import render_portfolio_panel

    sig = inspect.signature(render_portfolio_panel)
    required_kwonly = {
        p.name
        for p in sig.parameters.values()
        if p.kind is inspect.Parameter.KEYWORD_ONLY and p.default is inspect.Parameter.empty
    }

    src_path = (
        Path(__file__).resolve().parents[1]
        / "india_resilience_tool"
        / "app"
        / "details_runtime.py"
    )
    tree = ast.parse(src_path.read_text(encoding="utf-8"), filename=str(src_path))

    calls: list[ast.Call] = []
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "render_portfolio_panel"
        ):
            calls.append(node)

    assert calls, "Expected at least one render_portfolio_panel(...) call in details_runtime.py"

    for call in calls:
        call_kwargs = {kw.arg for kw in call.keywords if kw.arg is not None}
        missing = required_kwonly - call_kwargs
        assert not missing, (
            "Missing required kw-only args in render_portfolio_panel call: "
            f"{sorted(missing)}"
        )
