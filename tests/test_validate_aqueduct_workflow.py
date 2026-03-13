from __future__ import annotations

import pandas as pd
import pytest

from tools.geodata.build_aqueduct_hydro_masters import AQ_WATER_STRESS_COLUMN_MAP
from tools.geodata.validate_aqueduct_workflow import (
    FIELD_CONTRACT,
    build_crosswalk_conservation_summary,
    build_master_value_spotcheck,
    classify_reliability_tiers,
    select_sample_audit_units,
)


def test_field_contract_covers_all_current_aq_water_stress_columns() -> None:
    mapped_source_columns = {row["source_column"] for row in FIELD_CONTRACT}
    assert mapped_source_columns == set(AQ_WATER_STRESS_COLUMN_MAP.values())


def test_build_crosswalk_conservation_summary_flags_partial_coverage() -> None:
    crosswalk_df = pd.DataFrame(
        {
            "pfaf_id": ["P1", "P1", "P2"],
            "pfaf_area_km2": [10.0, 10.0, 5.0],
            "intersection_area_km2": [4.0, 4.0, 5.0],
            "pfaf_area_fraction_in_basin": [0.4, 0.4, 1.0],
            "basin_id": ["B1", "B2", "B3"],
            "basin_name": ["One", "Two", "Three"],
            "basin_area_km2": [8.0, 8.0, 5.0],
            "basin_area_fraction_in_pfaf": [0.5, 0.5, 1.0],
        }
    )

    summary = build_crosswalk_conservation_summary(crosswalk_df, hydro_level="basin")
    row_p1 = summary.loc[summary["pfaf_id"] == "P1"].iloc[0]
    row_p2 = summary.loc[summary["pfaf_id"] == "P2"].iloc[0]

    assert row_p1["status"] == "partial_target_coverage"
    assert row_p1["summed_pfaf_fraction"] == pytest.approx(0.8)
    assert row_p2["status"] == "near_full"


def test_classify_reliability_tiers_assigns_expected_bands() -> None:
    qa_df = pd.DataFrame(
        {
            "subbasin_id": ["S1", "S2", "S3"],
            "subbasin_name": ["High", "Moderate", "Low"],
            "subbasin_code": ["H", "M", "L"],
            "basin_id": ["B1", "B1", "B1"],
            "basin_name": ["Demo", "Demo", "Demo"],
            "source_pfaf_count": [2, 2, 1],
            "subbasin_coverage_fraction": [0.95, 0.6, 0.2],
        }
    )

    out = classify_reliability_tiers(qa_df, hydro_level="sub_basin")
    tiers = dict(zip(out["subbasin_id"], out["reliability_tier"]))
    assert tiers == {"S1": "high", "S2": "moderate", "S3": "low"}


def test_select_sample_audit_units_returns_deterministic_per_tier_rows() -> None:
    reliability_df = pd.DataFrame(
        {
            "basin_id": ["B1", "B2", "B3", "B4", "B5", "B6"],
            "basin_name": ["A", "B", "C", "D", "E", "F"],
            "source_pfaf_count": [1, 1, 1, 1, 1, 1],
            "basin_coverage_fraction": [0.99, 0.93, 0.75, 0.55, 0.40, 0.10],
            "reliability_tier": ["high", "high", "moderate", "moderate", "low", "low"],
        }
    )

    sample = select_sample_audit_units(reliability_df, hydro_level="basin", per_tier=1)
    assert sample["basin_id"].tolist() == ["B1", "B4", "B6"]
    assert sample["audit_reason"].tolist() == [
        "high_coverage_sample",
        "moderate_coverage_sample",
        "low_coverage_sample",
    ]


def test_build_master_value_spotcheck_recomputes_sampled_weighted_values() -> None:
    sample_df = pd.DataFrame(
        {
            "basin_id": ["B1"],
            "basin_name": ["Demo Basin"],
            "basin_coverage_fraction": [1.0],
            "source_pfaf_count": [2],
            "hydro_level": ["basin"],
            "audit_reason": ["high_coverage_sample"],
        }
    )
    crosswalk_df = pd.DataFrame(
        {
            "pfaf_id": ["P1", "P2"],
            "basin_id": ["B1", "B1"],
            "basin_name": ["Demo Basin", "Demo Basin"],
            "intersection_area_km2": [1.0, 3.0],
        }
    )
    source_df = pd.DataFrame(
        {
            "pfaf_id": ["P1", "P2"],
            "aq_water_stress__historical__1979-2019__mean": [1.0, 5.0],
            "aq_water_stress__bau__2030__mean": [2.0, 10.0],
        }
    )
    master_df = pd.DataFrame(
        {
            "basin_id": ["B1"],
            "basin_name": ["Demo Basin"],
            "aq_water_stress__historical__1979-2019__mean": [4.0],
            "aq_water_stress__bau__2030__mean": [8.0],
        }
    )

    out = build_master_value_spotcheck(
        sample_df=sample_df,
        crosswalk_df=crosswalk_df,
        source_df=source_df,
        master_df=master_df,
        hydro_level="basin",
    )

    hist = out.loc[out["metric_column"] == "aq_water_stress__historical__1979-2019__mean"].iloc[0]
    bau = out.loc[out["metric_column"] == "aq_water_stress__bau__2030__mean"].iloc[0]
    assert hist["recomputed_weighted_value"] == pytest.approx(4.0)
    assert bau["recomputed_weighted_value"] == pytest.approx(8.0)
    assert hist["dominant_pfaf_id"] == "P2"
