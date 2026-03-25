"""
Unit tests for deterministic merge + session_state mtime caching.

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

import time
from pathlib import Path

import pandas as pd
import pytest

from india_resilience_tool.data.merge import get_or_build_merged_for_index_cached


def _alias(s: str) -> str:
    return str(s).strip().lower()


def test_merge_filters_states_and_joins_districts(tmp_path: Path) -> None:
    adm2 = pd.DataFrame(
        {
            "district_name": ["Alpha", "Beta", "Gamma"],
            "state_name": ["Telangana", "Telangana", "Karnataka"],
        }
    )
    master = pd.DataFrame(
        {
            "district": ["alpha", "beta"],
            "state": ["telangana", "telangana"],
            "some_metric__ssp585__2020-2040__mean": [1.0, 2.0],
        }
    )

    master_path = tmp_path / "master.csv"
    master_path.write_text("x\n1\n")

    session_state: dict = {}

    merged = get_or_build_merged_for_index_cached(
        adm2,
        master,
        slug="demo",
        master_path=master_path,
        session_state=session_state,
        alias_fn=_alias,
        adm2_state_col="state_name",
        master_state_col="state",
    )

    # Karnataka row should be filtered out because master only has Telangana
    assert merged.shape[0] == 2
    assert set(merged["district_name"].tolist()) == {"Alpha", "Beta"}

    # Joined metric should exist
    assert "some_metric__ssp585__2020-2040__mean" in merged.columns
    assert float(merged.loc[merged["district_name"] == "Alpha", "some_metric__ssp585__2020-2040__mean"].iloc[0]) == 1.0


def test_cache_by_mtime(tmp_path: Path) -> None:
    adm2 = pd.DataFrame({"district_name": ["Alpha"], "state_name": ["Telangana"]})
    master_v1 = pd.DataFrame({"district": ["alpha"], "state": ["telangana"], "m": [1]})
    master_v2 = pd.DataFrame({"district": ["alpha"], "state": ["telangana"], "m": [999]})

    master_path = tmp_path / "master.csv"
    master_path.write_text("x\n1\n")
    session_state: dict = {}

    out1 = get_or_build_merged_for_index_cached(
        adm2,
        master_v1,
        slug="demo",
        master_path=master_path,
        session_state=session_state,
        alias_fn=_alias,
    )
    assert int(out1["m"].iloc[0]) == 1

    # Same mtime -> should return cached result (still 1 even if master_df passed differs)
    out_cached = get_or_build_merged_for_index_cached(
        adm2,
        master_v2,
        slug="demo",
        master_path=master_path,
        session_state=session_state,
        alias_fn=_alias,
    )
    assert int(out_cached["m"].iloc[0]) == 1

    # Bump mtime -> should rebuild and reflect new master_df
    time.sleep(0.01)
    master_path.write_text("x\n2\n")

    out2 = get_or_build_merged_for_index_cached(
        adm2,
        master_v2,
        slug="demo",
        master_path=master_path,
        session_state=session_state,
        alias_fn=_alias,
    )
    assert int(out2["m"].iloc[0]) == 999


def test_cache_invalidates_when_multisource_signature_changes(tmp_path: Path) -> None:
    adm2 = pd.DataFrame({"district_name": ["Alpha"], "state_name": ["Telangana"]})
    master_v1 = pd.DataFrame({"district": ["alpha"], "state": ["telangana"], "m": [1]})
    master_v2 = pd.DataFrame({"district": ["alpha"], "state": ["telangana"], "m": [5]})

    master_a = tmp_path / "a.csv"
    master_b = tmp_path / "b.csv"
    master_a.write_text("x\n1\n")
    master_b.write_text("x\n1\n")
    session_state: dict = {}

    out1 = get_or_build_merged_for_index_cached(
        adm2,
        master_v1,
        slug="demo",
        master_path=(master_a, master_b),
        session_state=session_state,
        alias_fn=_alias,
    )
    assert int(out1["m"].iloc[0]) == 1

    out_cached = get_or_build_merged_for_index_cached(
        adm2,
        master_v2,
        slug="demo",
        master_path=(master_a, master_b),
        session_state=session_state,
        alias_fn=_alias,
    )
    assert int(out_cached["m"].iloc[0]) == 1

    time.sleep(0.01)
    master_b.write_text("x\n2\n")

    out2 = get_or_build_merged_for_index_cached(
        adm2,
        master_v2,
        slug="demo",
        master_path=(master_a, master_b),
        session_state=session_state,
        alias_fn=_alias,
    )
    assert int(out2["m"].iloc[0]) == 5


def test_block_merge_raises_when_master_states_are_missing_from_boundaries(tmp_path: Path) -> None:
    adm3 = pd.DataFrame(
        {
            "state_name": ["Karn<taka"],
            "district_name": ["Belag<vi"],
            "block_name": ["Athani"],
        }
    )
    master = pd.DataFrame(
        {
            "state": ["Karnataka"],
            "district": ["Belagavi"],
            "block": ["Athani"],
            "m": [1.0],
        }
    )

    master_path = tmp_path / "master.csv"
    master_path.write_text("x\n1\n")

    with pytest.raises(ValueError, match="missing master states"):
        get_or_build_merged_for_index_cached(
            adm3,
            master,
            slug="demo",
            master_path=master_path,
            session_state={},
            alias_fn=_alias,
            level="block",
            adm2_state_col="state_name",
            master_state_col="state",
        )


def test_merge_ignores_nullable_master_states(tmp_path: Path) -> None:
    adm2 = pd.DataFrame(
        {
            "district_name": ["Alpha"],
            "state_name": ["Telangana"],
        }
    )
    master = pd.DataFrame(
        {
            "district": ["alpha", "orphan"],
            "state": pd.Series(["Telangana", pd.NA], dtype="string"),
            "m": [1.0, 2.0],
        }
    )

    master_path = tmp_path / "master.csv"
    master_path.write_text("x\n1\n")

    merged = get_or_build_merged_for_index_cached(
        adm2,
        master,
        slug="demo",
        master_path=master_path,
        session_state={},
        alias_fn=_alias,
        adm2_state_col="state_name",
        master_state_col="state",
    )

    assert merged.shape[0] == 1
    assert merged["district_name"].tolist() == ["Alpha"]
    assert float(merged["m"].iloc[0]) == 1.0
