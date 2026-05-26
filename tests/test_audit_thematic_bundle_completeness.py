from __future__ import annotations

from pathlib import Path

import pandas as pd

from tools.diagnostics.audit_thematic_bundle_completeness import audit_thematic_bundles


def _repo_audit_doc() -> Path:
    return Path(__file__).resolve().parents[1] / "docs" / "bundle_calculation_audit.md"


def _write_master(tmp_path: Path, slug: str, state: str, level: str, columns: dict[str, list[object]]) -> None:
    root = tmp_path / "processed" / slug / state
    root.mkdir(parents=True, exist_ok=True)
    filename = f"master_metrics_by_{level}.csv"
    pd.DataFrame(columns).to_csv(root / filename, index=False)


def test_drought_audit_reports_complete_when_sources_and_composite_align(tmp_path: Path) -> None:
    state = "Telangana"
    id_cols = {
        "state": [state],
        "district": ["A"],
        "district_key": ["a"],
    }
    component_slugs = (
        "spi3_count_events_lt_minus1",
        "spi6_count_events_lt_minus1",
        "spi12_count_events_lt_minus1",
        "spi3_max_spell_lt_minus1",
        "spi6_max_spell_lt_minus1",
        "spi12_max_spell_lt_minus1",
    )
    for slug in component_slugs:
        _write_master(
            tmp_path,
            slug,
            state,
            "district",
            {
                **id_cols,
                f"{slug}__historical__1990-2010__mean": [1.0],
                f"{slug}__ssp245__2040-2060__mean": [2.0],
            },
        )
    _write_master(
        tmp_path,
        "composite_drought_risk",
        state,
        "district",
        {
            **id_cols,
            "composite_drought_risk__historical__1990-2010__mean": [10.0],
            "composite_drought_risk__ssp245__2040-2060__mean": [20.0],
        },
    )

    _, rows = audit_thematic_bundles(
        audit_doc_path=_repo_audit_doc(),
        data_dir=tmp_path,
        bundle_names=("Drought Risk",),
        levels=("district",),
        states=(state,),
    )

    assert len(rows) == 1
    row = rows[0]
    assert row.bundle_name == "Drought Risk"
    assert row.status == "complete"
    assert row.source_shared_pairs == ("historical:1990-2010", "ssp245:2040-2060")
    assert row.missing_composite_pairs == ()


def test_drought_audit_reports_missing_composite_master(tmp_path: Path) -> None:
    state = "Telangana"
    id_cols = {
        "state": [state],
        "district": ["A"],
        "district_key": ["a"],
    }
    component_slugs = (
        "spi3_count_events_lt_minus1",
        "spi6_count_events_lt_minus1",
        "spi12_count_events_lt_minus1",
        "spi3_max_spell_lt_minus1",
        "spi6_max_spell_lt_minus1",
        "spi12_max_spell_lt_minus1",
    )
    for slug in component_slugs:
        _write_master(
            tmp_path,
            slug,
            state,
            "district",
            {
                **id_cols,
                f"{slug}__historical__1990-2010__mean": [1.0],
                f"{slug}__ssp585__2040-2060__mean": [2.0],
            },
        )

    _, rows = audit_thematic_bundles(
        audit_doc_path=_repo_audit_doc(),
        data_dir=tmp_path,
        bundle_names=("Drought Risk",),
        levels=("district",),
        states=(state,),
    )

    assert len(rows) == 1
    row = rows[0]
    assert row.status == "missing_composite_master"
    assert row.source_shared_pairs == ("historical:1990-2010", "ssp585:2040-2060")
    assert row.composite_pairs == ()
