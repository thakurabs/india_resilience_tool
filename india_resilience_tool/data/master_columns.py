"""
Master-column helpers (Streamlit-free).

These helpers interpret the wide master schema convention:
  {metric}__{scenario}__{period}__{stat}

They are used by the dashboard for baseline discovery and labeling.
"""

from __future__ import annotations

import re
from typing import Optional, Sequence


def find_baseline_column_for_metric(
    df_cols: Sequence[str],
    *,
    base_metric: str,
    preferred_period_tokens: Sequence[str] = ("1995-2014", "1995_2014", "1985-2014"),
) -> Optional[str]:
    """
    Find a historical baseline column for the given metric (legacy heuristic).

    Contract (legacy):
      - Only considers columns ending in `__mean`
      - Only considers `historical` scenario
      - Prefers a small set of historical periods when present
      - Otherwise returns the lexicographically earliest historical period

    Returns:
        Column name or None if no suitable baseline column is found.
    """
    metric = str(base_metric or "").strip()
    if not metric:
        return None

    pat = re.compile(
        rf"^{re.escape(metric)}__(?P<scenario>[^_]+)__(?P<period>[^_]+)__mean$"
    )

    candidates: list[tuple[str, str]] = []
    for c in df_cols:
        m = pat.match(str(c))
        if m and m.group("scenario").lower() == "historical":
            candidates.append((str(c), str(m.group("period"))))

    if not candidates:
        return None

    pref = {str(p).replace(" ", "") for p in preferred_period_tokens if str(p).strip()}
    for c, p in candidates:
        if p.replace(" ", "") in pref:
            return c

    candidates.sort(key=lambda x: x[1])
    return candidates[0][0]

