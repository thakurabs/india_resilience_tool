from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pandas as pd

from india_resilience_tool.app.ribbon import (
    _domain_display_label,
    _domain_options_for_context,
    _resolve_admin_master_source,
    _resolve_external_admin_master_sources,
    _selected_state_for_admin_master_loading,
)


def test_resolve_external_admin_master_sources_for_all_uses_all_valid_states(tmp_path: Path) -> None:
    (tmp_path / "Telangana").mkdir(parents=True)
    (tmp_path / "Telangana" / "master_metrics_by_district.csv").write_text("state,district\n", encoding="utf-8")
    (tmp_path / "Odisha").mkdir(parents=True)
    (tmp_path / "Odisha" / "master_metrics_by_district.csv").write_text("state,district\n", encoding="utf-8")
    (tmp_path / "hydro" / "DemoBasin" / "ssp245").mkdir(parents=True)

    paths = _resolve_external_admin_master_sources(
        tmp_path,
        level="district",
        selected_state="All",
    )

    assert paths == (
        tmp_path / "Odisha" / "master_metrics_by_district.csv",
        tmp_path / "Telangana" / "master_metrics_by_district.csv",
    )


def test_resolve_external_admin_master_sources_for_single_state_returns_one_path(tmp_path: Path) -> None:
    expected = tmp_path / "Telangana" / "master_metrics_by_block.csv"

    paths = _resolve_external_admin_master_sources(
        tmp_path,
        level="block",
        selected_state="Telangana",
    )

    assert paths == (expected,)


def test_resolve_admin_master_source_falls_back_to_legacy_when_optimized_missing(
    tmp_path: Path,
    monkeypatch,
) -> None:
    slug = "composite_life_livelihood_loss_risk"
    optimized_root = tmp_path / "processed_optimised" / "metrics" / slug
    (optimized_root / "masters" / "admin" / "district").mkdir(parents=True)

    legacy_root = tmp_path / "processed" / slug
    legacy_state_root = legacy_root / "Telangana"
    legacy_state_root.mkdir(parents=True)
    (legacy_state_root / "master_metrics_by_district.csv").write_text(
        "state,district,district_key,composite_life_livelihood_loss_risk__ssp245__2020-2040__mean\n"
        "Telangana,A,telangana|a,1.0\n",
        encoding="utf-8",
    )

    monkeypatch.delenv("IRT_PROCESSED_ROOT", raising=False)
    monkeypatch.delenv("IRT_PROCESSED_OPTIMISED_ROOT", raising=False)

    resolved_root, master_paths, legacy_checked = _resolve_admin_master_source(
        optimized_root,
        variable_slug=slug,
        level="district",
        selected_state="Telangana",
        data_dir=tmp_path,
        optimized_intent=True,
    )

    assert resolved_root == legacy_root.resolve()
    assert master_paths == ((legacy_root / "Telangana" / "master_metrics_by_district.csv").resolve(),)
    assert legacy_checked == legacy_root.resolve()


def test_resolve_admin_master_source_falls_back_to_legacy_when_optimized_root_is_absent(
    tmp_path: Path,
    monkeypatch,
) -> None:
    slug = "composite_life_livelihood_loss_risk"
    optimized_root = tmp_path / "processed_optimised" / "metrics" / slug

    legacy_root = tmp_path / "processed" / slug
    legacy_state_root = legacy_root / "Telangana"
    legacy_state_root.mkdir(parents=True)
    (legacy_state_root / "master_metrics_by_district.csv").write_text(
        "state,district,district_key,composite_life_livelihood_loss_risk__ssp245__2020-2040__mean\n"
        "Telangana,A,telangana|a,1.0\n",
        encoding="utf-8",
    )

    monkeypatch.delenv("IRT_PROCESSED_ROOT", raising=False)
    monkeypatch.delenv("IRT_PROCESSED_OPTIMISED_ROOT", raising=False)

    resolved_root, master_paths, legacy_checked = _resolve_admin_master_source(
        optimized_root,
        variable_slug=slug,
        level="district",
        selected_state="Telangana",
        data_dir=tmp_path,
        optimized_intent=True,
    )

    assert resolved_root == legacy_root.resolve()
    assert master_paths == ((legacy_root / "Telangana" / "master_metrics_by_district.csv").resolve(),)
    assert legacy_checked == legacy_root.resolve()


def test_resolve_admin_master_source_falls_back_to_legacy_when_optimized_root_has_no_runtime_dirs(
    tmp_path: Path,
    monkeypatch,
) -> None:
    slug = "composite_life_livelihood_loss_risk"
    optimized_root = tmp_path / "processed_optimised" / "metrics" / slug
    optimized_root.mkdir(parents=True)

    legacy_root = tmp_path / "processed" / slug
    legacy_state_root = legacy_root / "Telangana"
    legacy_state_root.mkdir(parents=True)
    (legacy_state_root / "master_metrics_by_district.csv").write_text(
        "state,district,district_key,composite_life_livelihood_loss_risk__ssp245__2020-2040__mean\n"
        "Telangana,A,telangana|a,1.0\n",
        encoding="utf-8",
    )

    monkeypatch.delenv("IRT_PROCESSED_ROOT", raising=False)
    monkeypatch.delenv("IRT_PROCESSED_OPTIMISED_ROOT", raising=False)

    resolved_root, master_paths, legacy_checked = _resolve_admin_master_source(
        optimized_root,
        variable_slug=slug,
        level="district",
        selected_state="Telangana",
        data_dir=tmp_path,
        optimized_intent=True,
    )

    assert resolved_root == legacy_root.resolve()
    assert master_paths == ((legacy_root / "Telangana" / "master_metrics_by_district.csv").resolve(),)
    assert legacy_checked == legacy_root.resolve()


def test_resolve_admin_master_source_for_all_falls_back_to_legacy_when_optimized_state_coverage_is_partial(
    tmp_path: Path,
    monkeypatch,
) -> None:
    slug = "composite_life_livelihood_loss_risk"
    optimized_root = tmp_path / "processed_optimised" / "metrics" / slug
    optimized_district_root = optimized_root / "masters" / "admin" / "district"
    optimized_district_root.mkdir(parents=True)
    pd.DataFrame(
        {
            "state": ["Telangana"],
            "district": ["A"],
            "district_key": ["telangana|a"],
            "composite_life_livelihood_loss_risk__ssp245__2020-2040__mean": [1.0],
        }
    ).to_parquet(optimized_district_root / "state=Telangana.parquet", index=False)

    legacy_root = tmp_path / "processed" / slug
    telangana_root = legacy_root / "Telangana"
    telangana_root.mkdir(parents=True)
    (telangana_root / "master_metrics_by_district.csv").write_text(
        "state,district,district_key,composite_life_livelihood_loss_risk__ssp245__2020-2040__mean\n"
        "Telangana,A,telangana|a,1.0\n",
        encoding="utf-8",
    )
    odisha_root = legacy_root / "Odisha"
    odisha_root.mkdir(parents=True)
    (odisha_root / "master_metrics_by_district.csv").write_text(
        "state,district,district_key,composite_life_livelihood_loss_risk__ssp245__2020-2040__mean\n"
        "Odisha,B,odisha|b,2.0\n",
        encoding="utf-8",
    )

    monkeypatch.delenv("IRT_PROCESSED_ROOT", raising=False)
    monkeypatch.delenv("IRT_PROCESSED_OPTIMISED_ROOT", raising=False)

    resolved_root, master_paths, legacy_checked = _resolve_admin_master_source(
        optimized_root,
        variable_slug=slug,
        level="district",
        selected_state="All",
        data_dir=tmp_path,
        optimized_intent=True,
    )

    assert resolved_root == legacy_root.resolve()
    assert master_paths == (
        (legacy_root / "Odisha" / "master_metrics_by_district.csv").resolve(),
        (legacy_root / "Telangana" / "master_metrics_by_district.csv").resolve(),
    )
    assert legacy_checked == legacy_root.resolve()


def test_resolve_admin_master_source_for_all_prefers_optimized_when_loaded_content_is_fuller(
    tmp_path: Path,
    monkeypatch,
) -> None:
    slug = "composite_life_livelihood_loss_risk"
    optimized_root = tmp_path / "processed_optimised" / "metrics" / slug
    optimized_district_root = optimized_root / "masters" / "admin" / "district"
    optimized_district_root.mkdir(parents=True)
    pd.DataFrame(
        {
            "state": ["Telangana"],
            "district": ["A"],
            "district_key": ["telangana|a"],
            "composite_life_livelihood_loss_risk__ssp245__2020-2040__mean": [1.0],
        }
    ).to_parquet(optimized_district_root / "state=Telangana.parquet", index=False)
    pd.DataFrame(
        {
            "state": ["Odisha"],
            "district": ["B"],
            "district_key": ["odisha|b"],
            "composite_life_livelihood_loss_risk__ssp245__2020-2040__mean": [2.0],
        }
    ).to_parquet(optimized_district_root / "state=Odisha.parquet", index=False)

    legacy_root = tmp_path / "processed" / slug
    telangana_root = legacy_root / "Telangana"
    telangana_root.mkdir(parents=True)
    (telangana_root / "master_metrics_by_district.csv").write_text(
        "state,district,district_key,composite_life_livelihood_loss_risk__ssp245__2020-2040__mean\n"
        "Telangana,A,telangana|a,1.0\n",
        encoding="utf-8",
    )
    odisha_root = legacy_root / "Odisha"
    odisha_root.mkdir(parents=True)
    (odisha_root / "master_metrics_by_district.csv").write_text(
        "state,district,district_key,composite_life_livelihood_loss_risk__ssp245__2020-2040__mean\n"
        "Odisha,B,odisha|b,\n",
        encoding="utf-8",
    )

    monkeypatch.delenv("IRT_PROCESSED_ROOT", raising=False)
    monkeypatch.delenv("IRT_PROCESSED_OPTIMISED_ROOT", raising=False)

    resolved_root, master_paths, legacy_checked = _resolve_admin_master_source(
        optimized_root,
        variable_slug=slug,
        level="district",
        selected_state="All",
        data_dir=tmp_path,
        optimized_intent=True,
    )

    assert resolved_root == optimized_root.resolve()
    assert master_paths == (
        (optimized_district_root / "state=Odisha.parquet").resolve(),
        (optimized_district_root / "state=Telangana.parquet").resolve(),
    )
    assert legacy_checked == legacy_root.resolve()


def test_domain_display_label_uses_grouped_dashboard_labels() -> None:
    assert _domain_display_label("Heat Risk") == "Thematic - Heat Risk"
    assert _domain_display_label("Health Risk") == "Sector-wise - Health Risk"
    assert _domain_display_label("Population Exposure") == "Population Exposure"


def test_domain_options_for_admin_climate_hazards_prioritize_valid_dashboard_bundles(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        "india_resilience_tool.app.ribbon.get_domains_for_pillar",
        lambda *args, **kwargs: [
            "Heat Risk",
            "Drought Risk",
            "Health Risk",
            "Rainfall Totals & Typical Wetness",
            "Temperature Variability",
        ],
    )
    monkeypatch.setattr(
        "india_resilience_tool.app.ribbon.available_dashboard_bundle_names",
        lambda *, level, data_dir: ["Heat Risk", "Drought Risk", "Health Risk"],
    )

    options = _domain_options_for_context(
        selected_pillar="Climate Hazards",
        spatial_family="admin",
        current_level="district",
        data_dir=tmp_path,
    )

    assert options == [
        "Heat Risk",
        "Drought Risk",
        "Health Risk",
        "Rainfall Totals & Typical Wetness",
        "Temperature Variability",
    ]


def test_domain_options_for_non_dashboard_context_preserve_existing_registry_order(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(
        "india_resilience_tool.app.ribbon.get_domains_for_pillar",
        lambda *args, **kwargs: ["Population Exposure", "Aqueduct Water Risk"],
    )

    # CHG-0273: the dashboard-bundle reordering only fires for admin district/block
    # under Climate Hazards; any other context returns registry order unchanged.
    # Exercise that fallback via the hydro/basin context (Exposure is no longer a pillar).
    options = _domain_options_for_context(
        selected_pillar="Climate Hazards",
        spatial_family="hydro",
        current_level="basin",
        data_dir=tmp_path,
    )

    assert options == ["Population Exposure", "Aqueduct Water Risk"]


def test_selected_state_for_admin_master_loading_clamps_to_supported_metric_states(monkeypatch) -> None:
    import india_resilience_tool.app.ribbon as ribbon

    monkeypatch.setattr(
        ribbon,
        "st",
        SimpleNamespace(
            session_state={
                "selected_var": "restricted_metric",
                "selected_state": "Maharashtra",
                "pending_selected_state": "Maharashtra",
            }
        ),
    )
    monkeypatch.setitem(ribbon.VARIABLES, "restricted_metric", {"supported_admin_states": ["Telangana"]})

    assert _selected_state_for_admin_master_loading() == "Telangana"
