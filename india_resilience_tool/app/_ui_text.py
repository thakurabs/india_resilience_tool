"""
Direction-aware UI phrasing helpers.

Centralizes the human-facing copy that depends on a metric's
`rank_higher_is_worse` flag, so the language matches the methodology
(e.g. cold-magnitude metrics where lower values are worse).

Streamlit-free; pure functions only.
"""

from __future__ import annotations

from typing import NamedTuple


class RankPhrasing(NamedTuple):
    rank_1_meaning: str          # word substituted into "Rank 1 = {…} value."
    percentile_legend: str       # parenthetical for the percentile tooltip
    direction_summary: str       # one-line summary of risk direction


def rank_phrasing(higher_is_worse: bool) -> RankPhrasing:
    """Return direction-aware phrasing for rank / percentile narrative.

    Parameters
    ----------
    higher_is_worse:
        True (the registry default) when larger metric values indicate
        higher risk (e.g. heat days, drought severity). False for
        cold-magnitude metrics where smaller values are worse
        (e.g. tasmin_winter_min, tnn_annual_min).
    """
    if higher_is_worse:
        return RankPhrasing(
            rank_1_meaning="highest",
            percentile_legend="higher value = worse",
            direction_summary="Higher values indicate higher risk.",
        )
    return RankPhrasing(
        rank_1_meaning="lowest (coldest)",
        percentile_legend="lower value = worse",
        direction_summary="Lower values indicate higher risk.",
    )


__all__ = ["RankPhrasing", "rank_phrasing"]
