"""
Unit tests for direction-aware UI phrasing helper.

Guards the user-visible copy that switches between
"higher = worse" and "lower = worse" semantics based on the
registry `rank_higher_is_worse` flag.
"""

from __future__ import annotations

from india_resilience_tool.app._ui_text import RankPhrasing, rank_phrasing


def test_rank_phrasing_higher_is_worse_default() -> None:
    phr = rank_phrasing(True)
    assert isinstance(phr, RankPhrasing)
    assert phr.rank_1_meaning == "highest"
    assert phr.percentile_legend == "higher value = worse"
    assert "Higher values" in phr.direction_summary


def test_rank_phrasing_lower_is_worse_for_cold_metrics() -> None:
    phr = rank_phrasing(False)
    assert "lowest" in phr.rank_1_meaning
    assert "coldest" in phr.rank_1_meaning
    assert phr.percentile_legend == "lower value = worse"
    assert "Lower values" in phr.direction_summary


def test_rank_phrasing_no_bleed_between_branches() -> None:
    hot = rank_phrasing(True)
    cold = rank_phrasing(False)
    assert hot.rank_1_meaning != cold.rank_1_meaning
    assert hot.percentile_legend != cold.percentile_legend
    assert hot.direction_summary != cold.direction_summary
