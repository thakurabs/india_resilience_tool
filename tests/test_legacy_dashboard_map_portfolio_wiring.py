"""
Regression test: left_panel_runtime must pass `merged=` into the map inline
portfolio control so click-coordinates can be resolved to admin units.

This test avoids importing Streamlit by parsing the source AST directly.
"""

from __future__ import annotations

import ast
from pathlib import Path


def test_render_unit_add_to_portfolio_call_passes_merged() -> None:
    src_path = (
        Path(__file__).resolve().parents[1]
        / "india_resilience_tool"
        / "app"
        / "left_panel_runtime.py"
    )
    tree = ast.parse(src_path.read_text(encoding="utf-8"), filename=str(src_path))

    calls: list[ast.Call] = []
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "render_unit_add_to_portfolio"
        ):
            calls.append(node)

    assert calls, "Expected a render_unit_add_to_portfolio(...) call in left_panel_runtime.py"

    for call in calls:
        call_kwargs = {kw.arg for kw in call.keywords if kw.arg is not None}
        assert "merged" in call_kwargs, "render_unit_add_to_portfolio(...) must be called with merged=..."
