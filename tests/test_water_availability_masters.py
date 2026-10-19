"""Tests for the NITI water-availability district-master builder (CHG-0235).

Covers ordinal class encoding, curated alias resolution, worst-class collision
aggregation, fail-fast integrity gates, and the split source/coverage counters.
Reconciliation is exercised with small synthetic canonical + source frames so the
tests are deterministic and do not depend on the real workbook or boundary files.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from tools.geodata.build_water_availability_district_masters import (
    DETERIORATION_COL,
    SCARCITY_2025_COL,
    SCARCITY_2050_COL,
    _class_to_code,
    _normalize_district,
    _normalize_state,
    reconcile_water_availability,
)


def _canonical_df(rows: list[tuple[str, str, str]]) -> pd.DataFrame:
    """Build a synthetic canonical district frame like _build_canonical_districts."""
    records = []
    for state, district, key in rows:
        records.append(
            {
                "canonical_state": state,
                "canonical_district": district,
                "district_key": key,
                "canonical_state_norm": _normalize_state(state),
                "canonical_district_norm": _normalize_district(district),
            }
        )
    return pd.DataFrame.from_records(records)


def _source_df(rows: list[tuple[str, str, int, int]]) -> pd.DataFrame:
    return pd.DataFrame(
        rows, columns=["source_state", "source_district", "code_2025", "code_2050"]
    )


def test_class_to_code_exact_order_and_unicode_minus() -> None:
    assert _class_to_code("No Stress [>1700 m3]") == 1
    assert _class_to_code("Stress [1000−1700 m3]") == 2  # U+2212 minus
    assert _class_to_code("Scarcity [500−1000 m3]") == 3
    assert _class_to_code("Absolute scarcity [<500 m3]") == 4
    # "Absolute scarcity" must not collide with the "scarcity" prefix.
    assert _class_to_code("absolute scarcity [<500 m3]") == 4


def test_class_to_code_raises_on_unknown_class() -> None:
    with pytest.raises(ValueError):
        _class_to_code("Severe drought [something]")


def test_reconciliation_resolves_aliases_and_reports_split_counters(tmp_path: Path) -> None:
    canonical = _canonical_df(
        [
            ("Telangana", "Warangal", "TG-WGL"),
            ("Telangana", "Hanumakonda", "TG-HNK"),
            ("Telangana", "Hyderabad", "TG-HYD"),  # no-source coverage gap
            ("Andhra Pradesh", "Anantapur", "AP-ATP"),
        ]
    )
    source = _source_df(
        [
            ("Telangana", "Warangal Rural", 2, 2),   # alias -> Warangal
            ("Telangana", "Warangal Urban", 2, 3),   # alias -> Hanumakonda
            ("Andhra Pradesh", "Ananthapuramu", 3, 4),  # alias -> Anantapur
        ]
    )
    result = reconcile_water_availability(
        source_df=source, canonical_df=canonical, allow_unmatched=False, qa_dir=tmp_path
    )
    summary = result.summary_df.iloc[0]
    # All source rows resolve; coverage is separate and counts the NaN roster row.
    assert int(summary["source_rows_resolved"]) == 3
    assert int(summary["source_rows_total"]) == 3
    assert int(summary["unmatched_source_rows"]) == 0
    assert int(summary["canonical_rows_total"]) == 4
    assert int(summary["canonical_rows_with_source"]) == 3
    assert int(summary["no_source_canonical_rows"]) == 1  # Hyderabad, coverage gap not unmatched

    master = result.master_df.set_index("district")
    # Warangal Rural -> Warangal, Warangal Urban -> Hanumakonda (distinct targets).
    assert master.loc["Warangal", SCARCITY_2025_COL] == 2
    assert master.loc["Hanumakonda", SCARCITY_2050_COL] == 3
    assert master.loc["Hanumakonda", DETERIORATION_COL] == 1  # 3 - 2
    # No-source canonical district is present with NaN, not dropped.
    assert pd.isna(master.loc["Hyderabad", SCARCITY_2025_COL])


def test_reconciliation_worst_class_collision(tmp_path: Path) -> None:
    canonical = _canonical_df([("Karnataka", "Bengaluru Urban", "KA-BLR")])
    source = _source_df(
        [
            ("Karnataka", "Bengaluru Urban", 3, 3),   # exact
            ("Karnataka", "Bengaluru South", 3, 4),   # alias -> Bengaluru Urban (collision)
        ]
    )
    result = reconcile_water_availability(
        source_df=source, canonical_df=canonical, allow_unmatched=False, qa_dir=tmp_path
    )
    # One collision group, collapsed to worst (max) class per year.
    assert int(result.summary_df.iloc[0]["collision_groups"]) == 1
    row = result.master_df.iloc[0]
    assert row[SCARCITY_2025_COL] == 3
    assert row[SCARCITY_2050_COL] == 4  # worst of {3, 4}
    assert result.duplicate_targets_df.empty  # aggregation ran before the duplicate check


def test_reconciliation_fails_fast_on_unmatched(tmp_path: Path) -> None:
    canonical = _canonical_df([("Telangana", "Warangal", "TG-WGL")])
    source = _source_df([("Telangana", "Nowhere District", 2, 2)])
    with pytest.raises(ValueError, match="unmatched"):
        reconcile_water_availability(
            source_df=source, canonical_df=canonical, allow_unmatched=False, qa_dir=tmp_path
        )
    # --allow-unmatched downgrades the unmatched row to a warning (no raise).
    result = reconcile_water_availability(
        source_df=source, canonical_df=canonical, allow_unmatched=True, qa_dir=tmp_path
    )
    assert int(result.summary_df.iloc[0]["unmatched_source_rows"]) == 1


def test_reconciliation_fails_fast_on_monotonicity_violation(tmp_path: Path) -> None:
    canonical = _canonical_df([("Telangana", "Warangal", "TG-WGL")])
    source = _source_df([("Telangana", "Warangal", 3, 2)])  # 2050 better than 2025
    with pytest.raises(ValueError, match="monotone"):
        reconcile_water_availability(
            source_df=source, canonical_df=canonical, allow_unmatched=False, qa_dir=tmp_path
        )
