"""
Tier-1 portfolio UI guardrails.

These tests are intentionally lightweight and avoid importing Streamlit.
They protect key no-regression expectations:
  - Portfolio visualizations are lazy/opt-in.
  - Climate Profile is suppressed in any portfolio (multi-*) mode.
"""

from __future__ import annotations

from pathlib import Path


def _read(path: Path) -> list[str]:
    return path.read_text(encoding="utf-8").splitlines()


def test_portfolio_ui_visualizations_are_lazy_opt_in() -> None:
    src = (
        Path(__file__).resolve().parents[1]
        / "india_resilience_tool"
        / "app"
        / "portfolio_ui.py"
    )
    lines = _read(src)
    text = "\n".join(lines)

    assert "portfolio_show_visualizations_" in text, "Expected a stable session_state key for visualization toggle"

    call_line = next(
        (i for i, ln in enumerate(lines) if "render_portfolio_visualizations(" in ln),
        None,
    )
    assert call_line is not None, "Expected render_portfolio_visualizations(...) call"

    window_start = max(0, call_line - 40)
    window = "\n".join(lines[window_start : call_line + 1])

    assert "if show_viz" in window, "Expected visualizations to be gated behind a show_viz condition"


def test_legacy_dashboard_suppresses_climate_profile_in_any_portfolio_mode() -> None:
    src = (
        Path(__file__).resolve().parents[1]
        / "india_resilience_tool"
        / "app"
        / "details_runtime.py"
    )
    lines = _read(src)

    header_line = next(
        (i for i, ln in enumerate(lines) if 'st.header("Climate Profile")' in ln or "st.header('Climate Profile')" in ln),
        None,
    )
    assert header_line is not None, 'Expected st.header("Climate Profile") in details_runtime.py'

    window_start = max(0, header_line - 60)
    window = "\n".join(lines[window_start : header_line + 1])

    assert "is_portfolio_mode" in window, "Expected an is_portfolio_mode guard near Climate Profile header"
    assert "st.stop()" in window, "Expected portfolio-mode early stop to suppress Climate Profile rendering"


def test_portfolio_rhs_has_compare_add_units_tabs() -> None:
    src = (
        Path(__file__).resolve().parents[1]
        / "india_resilience_tool"
        / "app"
        / "portfolio_ui.py"
    )
    text = src.read_text(encoding="utf-8")
    assert "portfolio_rhs_tab_" in text
    assert "Add units" in text
    assert "Compare" in text
