"""
Smoke tests for app.views.rankings_view.

We intentionally avoid importing Streamlit at module import time; the module should
import cleanly in test environments.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

import pandas as pd

from india_resilience_tool.app.views import rankings_view


def test_rankings_view_exports_renderer() -> None:
    assert callable(rankings_view.render_rankings_view)


def test_format_rankings_numeric_columns_rounds_population_total_for_display() -> None:
    df = pd.DataFrame({"value": [1931513.75], "baseline": [1000.4], "delta_abs": [3.2]})

    out = rankings_view._format_rankings_numeric_columns(df, variable_slug="population_total")

    assert out["value"].tolist() == ["1,931,514"]
    assert out["baseline"].tolist() == ["1,000"]
    assert out["delta_abs"].tolist() == ["3"]
