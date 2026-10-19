"""Focused tests for map-pipeline performance guards and non-spatial helpers."""

from __future__ import annotations

import pandas as pd

from india_resilience_tool.app.map_pipeline import (
    _build_nonspatial_details_source_df,
    _filter_frame_by_selection_value,
    _filter_master_frame_for_geometry_merge,
    _resolve_map_color_domain,
    _stack_legend_blocks,
    _uses_fixed_class_scale,
    DOMAIN_CMAP_FAMILY,
    blocked_drilldown_message,
    details_require_geometry,
    evaluate_coverage_policy,
    resolve_metric_cmap_name,
)
from india_resilience_tool.data.admin_coverage import CoverageDiagnostics
from india_resilience_tool.app.overlays import RP100_FLOOD_DEPTH_BINS
from india_resilience_tool.viz.colors import build_rp100_flood_depth_legend_html


def test_blocked_drilldown_message_requires_narrowing_for_fine_grain_views() -> None:
    assert (
        blocked_drilldown_message(
            adm_level="block",
            selected_state="All",
        )
        == "Select a state to render block maps and rankings."
    )
    assert (
        blocked_drilldown_message(
            adm_level="sub_basin",
            selected_state="All",
        )
        is None
    )
    assert (
        blocked_drilldown_message(
            adm_level="district",
            selected_state="All",
        )
        is None
    )


def test_details_require_geometry_only_for_summary_flows() -> None:
    assert details_require_geometry(
        adm_level="district",
        selected_state="Telangana",
        selected_district="All",
        selected_block="All",
    )
    assert not details_require_geometry(
        adm_level="district",
        selected_state="Telangana",
        selected_district="Hyderabad",
        selected_block="All",
    )
    assert details_require_geometry(
        adm_level="block",
        selected_state="Telangana",
        selected_district="Hyderabad",
        selected_block="All",
    )
    assert not details_require_geometry(
        adm_level="sub_basin",
        selected_state="All",
        selected_district="All",
        selected_block="All",
    )


def test_build_nonspatial_details_source_df_normalizes_admin_and_hydro_columns() -> None:
    admin_df = pd.DataFrame(
        {
            "state": ["Telangana"],
            "district": ["Hyderabad"],
            "block": ["Shaikpet"],
            "value": [1.0],
        }
    )
    admin_out = _build_nonspatial_details_source_df(admin_df, level="block")

    assert admin_out.columns.tolist() == ["state_name", "district_name", "block_name", "value"]
    assert admin_out.loc[0, "state_name"] == "Telangana"
    assert admin_out.loc[0, "district_name"] == "Hyderabad"
    assert admin_out.loc[0, "block_name"] == "Shaikpet"


def test_filter_frame_by_selection_value_handles_case_and_alias_mismatch() -> None:
    df = pd.DataFrame(
        {
            "district_name": ["Adilabad", "Nirmal"],
            "block_name": ["Adilabad Rural", "Laxmanchanda"],
            "basin_name": ["Upper Godavari", "Krishna"],
        }
    )

    district_out = _filter_frame_by_selection_value(
        df,
        column="district_name",
        selected_value="ADILABAD",
    )
    assert district_out["district_name"].tolist() == ["Adilabad"]

    block_out = _filter_frame_by_selection_value(
        df,
        column="block_name",
        selected_value="ADILABAD RURAL",
    )
    assert block_out["block_name"].tolist() == ["Adilabad Rural"]

    basin_out = _filter_frame_by_selection_value(
        df,
        column="basin_name",
        selected_value="upper godavari",
    )
    assert basin_out["basin_name"].tolist() == ["Upper Godavari"]


def test_filter_master_frame_for_geometry_merge_scopes_state_shards() -> None:
    df = pd.DataFrame(
        {
            "state": ["Andhra Pradesh", "Assam", "Andhra Pradesh"],
            "district": ["Anakapalli", "Cachar", "NTR"],
            "value": [1.0, 2.0, 3.0],
        }
    )

    out = _filter_master_frame_for_geometry_merge(
        df,
        level="district",
        selected_state="andhra pradesh",
        selected_district="All",
    )

    assert out["state"].tolist() == ["Andhra Pradesh", "Andhra Pradesh"]
    assert out["district"].tolist() == ["Anakapalli", "NTR"]
    assert "state" in out.columns


def test_filter_master_frame_for_geometry_merge_scopes_block_district_shards() -> None:
    df = pd.DataFrame(
        {
            "state": ["Telangana", "Telangana", "Telangana", "Karnataka"],
            "district": ["Hyderabad", "Hyderabad", "Nirmal", "Belagavi"],
            "block": ["Shaikpet", "Amberpet", "Laxmanchanda", "Athani"],
        }
    )

    out = _filter_master_frame_for_geometry_merge(
        df,
        level="block",
        selected_state="Telangana",
        selected_district="HYDERABAD",
    )

    assert out["block"].tolist() == ["Shaikpet", "Amberpet"]


def test_resolve_map_color_domain_keeps_baseline_deltas_data_driven() -> None:
    assert _resolve_map_color_domain(
        variable_slug="composite_heat_risk",
        use_fixed_class_scale=False,
        baseline_mode_active=True,
        vmin_default=-2.0,
        vmax_default=8.0,
        class_labels={},
    ) == (-2.0, 8.0)

    assert _resolve_map_color_domain(
        variable_slug="composite_heat_risk",
        use_fixed_class_scale=False,
        baseline_mode_active=False,
        vmin_default=-2.0,
        vmax_default=8.0,
        class_labels={},
    ) == (0.0, 100.0)


def test_fixed_class_scale_is_structurally_exclusive_from_baseline_domain() -> None:
    cfg = {
        "slug": "water_scarcity_percapita",
        "class_labels": {1: "No Stress", 2: "Stress", 3: "Scarcity", 4: "Absolute scarcity"},
        "class_display_mode": "label_with_score",
        "supports_baseline_comparison": True,
    }

    assert _uses_fixed_class_scale("water_scarcity_percapita", cfg)
    assert _resolve_map_color_domain(
        variable_slug="water_scarcity_percapita",
        use_fixed_class_scale=True,
        baseline_mode_active=True,
        vmin_default=-2.0,
        vmax_default=8.0,
        class_labels=cfg["class_labels"],
    ) == (1.0, 4.0)

    from india_resilience_tool.app.ribbon import _supports_baseline_comparison

    assert not _supports_baseline_comparison(cfg)


def test_stack_legend_blocks_keeps_both_legend_fragments() -> None:
    html = _stack_legend_blocks('<div id="main"></div>', '<div id="rp100"></div>', map_height=560)

    assert 'id="main"' in html
    assert 'id="rp100"' in html
    assert "flex-direction:row" in html
    assert "gap:28px" in html
    assert "height:560px" in html


def test_rp100_overlay_legend_uses_responsive_map_height() -> None:
    html = build_rp100_flood_depth_legend_html(bins=RP100_FLOOD_DEPTH_BINS, map_height=560)

    assert "height: 419px" in html


def test_evaluate_coverage_policy_warns_for_legitimate_partial_nationwide_district_coverage() -> None:
    diagnostics = CoverageDiagnostics(
        total_feature_keys=3,
        matched_feature_keys=1,
        missing_master_row_keys=("odisha|cuttack", "punjab|amritsar"),
        null_value_keys=(),
        broken_join_keys=(),
        coverage_pct=33.333333,
    )

    warnings, block = evaluate_coverage_policy(
        adm_level="district",
        selected_state="All",
        diagnostics=diagnostics,
    )

    assert block is None
    assert len(warnings) == 1
    assert "without master rows" in warnings[0]


def test_evaluate_coverage_policy_warns_for_null_values_without_blocking() -> None:
    diagnostics = CoverageDiagnostics(
        total_feature_keys=2,
        matched_feature_keys=2,
        missing_master_row_keys=(),
        null_value_keys=("odisha|cuttack",),
        broken_join_keys=(),
        coverage_pct=100.0,
    )

    warnings, block = evaluate_coverage_policy(
        adm_level="district",
        selected_state="All",
        diagnostics=diagnostics,
    )

    assert block is None
    assert len(warnings) == 1
    assert "null rendered values" in warnings[0]


def test_evaluate_coverage_policy_blocks_nationwide_district_broken_joins() -> None:
    diagnostics = CoverageDiagnostics(
        total_feature_keys=2,
        matched_feature_keys=1,
        missing_master_row_keys=(),
        null_value_keys=(),
        broken_join_keys=("telangana|adilabad",),
        coverage_pct=50.0,
    )

    warnings, block = evaluate_coverage_policy(
        adm_level="district",
        selected_state="All",
        diagnostics=diagnostics,
    )

    assert warnings == ()
    assert block is not None
    assert "blocked" in block.lower()


def test_evaluate_coverage_policy_warns_for_block_level_broken_joins() -> None:
    diagnostics = CoverageDiagnostics(
        total_feature_keys=2,
        matched_feature_keys=1,
        missing_master_row_keys=(),
        null_value_keys=(),
        broken_join_keys=("telangana|adilabad|adilabad rural",),
        coverage_pct=50.0,
    )

    warnings, block = evaluate_coverage_policy(
        adm_level="block",
        selected_state="All",
        diagnostics=diagnostics,
    )

    assert block is None
    assert len(warnings) == 1
    assert "failed to join" in warnings[0]


def test_uses_fixed_class_scale_generalizes_to_water_metrics() -> None:
    from india_resilience_tool.app.map_pipeline import _uses_fixed_class_scale

    assert _uses_fixed_class_scale(
        "water_scarcity_percapita",
        {"class_display_mode": "label_with_score", "class_labels": {1: "a", 2: "b", 3: "c", 4: "d"}},
    )
    # label_only (deterioration) also uses the fixed class scale
    assert _uses_fixed_class_scale(
        "water_scarcity_deterioration_2050",
        {"class_display_mode": "label_only", "class_labels": {0: "a", 1: "b", 2: "c", 3: "d"}},
    )
    # a plain continuous metric does not
    assert not _uses_fixed_class_scale("tas_annual_mean", {})


def test_class_scale_palette_lengths_and_zero_based_offset() -> None:
    from india_resilience_tool.viz.colors import (
        WATER_DETERIORATION_CLASS_COLORS,
        WATER_SCARCITY_CLASS_COLORS,
        class_scale_palette,
    )

    # exact-length fixed palettes for the 4-class water metrics
    assert class_scale_palette("water_scarcity_percapita", 4) == WATER_SCARCITY_CLASS_COLORS
    assert class_scale_palette("water_scarcity_deterioration_2050", 4) == WATER_DETERIORATION_CLASS_COLORS
    # an out-of-registry k-class metric falls back to a sampled palette of the right length
    fallback = class_scale_palette("some_unknown_metric", 3)
    assert len(fallback) == 3
    # 0-based deterioration: code 0 maps to the FIRST color (min_code offset), not dropped
    codes = [0, 1, 2, 3]
    min_code = min(codes)
    palette = class_scale_palette("water_scarcity_deterioration_2050", len(codes))
    value_to_color = {c: palette[c - min_code] for c in codes}
    assert value_to_color[0] == WATER_DETERIORATION_CLASS_COLORS[0]
    assert value_to_color[3] == WATER_DETERIORATION_CLASS_COLORS[3]


def test_resolve_metric_cmap_name_maps_domains_to_sdg_families() -> None:
    # Domain-hue contract (palette scheme "A+B"): a metric resolves to its
    # domain's SDG-anchored ramp; multi-domain metrics resolve to the
    # physical-hazard family deterministically; unknown slugs fall back.
    assert resolve_metric_cmap_name("tas_annual_mean") == "irt:heat"
    assert resolve_metric_cmap_name("water_scarcity_percapita") == "irt:water"
    # In Extreme Rainfall (water family) AND several sectoral risk domains:
    # the physical-hazard hue must win.
    assert resolve_metric_cmap_name("pr_max_1day_precip") == "irt:water"
    assert resolve_metric_cmap_name("population_total") == "irt:exposure"
    assert resolve_metric_cmap_name("not_a_real_metric") == "irt:heat"


def test_domain_cmap_family_targets_exist_as_ramps() -> None:
    # Every family named in the mapping must be a resolvable ramp, otherwise
    # the map render raises at color time.
    from india_resilience_tool.viz.colors import IRT_RAMP_ANCHORS

    assert set(DOMAIN_CMAP_FAMILY.values()) <= set(IRT_RAMP_ANCHORS)


def test_legend_card_control_survives_streamlit_folium_extraction() -> None:
    # Regression guard: streamlit-folium rebuilds the page JS from the map's
    # child elements and silently drops figure-level get_root().script, which
    # made the first legend-control implementation invisible in the app. The
    # control must therefore ride the map's child tree and appear in the JS
    # string st_folium actually ships to the browser.
    import pytest

    folium = pytest.importorskip("folium")
    streamlit_folium = pytest.importorskip("streamlit_folium")

    from india_resilience_tool.app.map_pipeline import attach_legend_card_control

    m = folium.Map(location=[17.9, 79.5], zoom_start=7)
    attach_legend_card_control(m, '<div id="irt-compact-map-legend">rows</div>')
    m.get_root().render()
    m.render()

    js = streamlit_folium._get_map_string(m)
    assert "irt-map-legend-control" in js
    assert "irt-compact-map-legend" in js
    assert "L.control({position: 'bottomright'})" in js


def test_map_view_controls_survive_extraction_with_max_bounds() -> None:
    # Regression guard (CHG-0278): the pan clamp used to ride a figure-level
    # get_root().html script that streamlit-folium silently drops. It now lives
    # inside the IrtMapViewControls MacroElement and must appear in the JS
    # string st_folium ships, alongside the existing control cluster markers.
    import pytest

    folium = pytest.importorskip("folium")
    streamlit_folium = pytest.importorskip("streamlit_folium")

    from india_resilience_tool.app.views.map_view import attach_map_view_controls

    m = folium.Map(location=[22.5, 80.0], zoom_start=5)
    attach_map_view_controls(
        m,
        [[6.0, 68.0], [37.5, 97.5]],
        max_bounds=[[5.0, 65.0], [45.0, 100.0]],
    )
    m.get_root().render()
    m.render()

    js = streamlit_folium._get_map_string(m)
    assert "setMaxBounds" in js
    assert "[[5.0, 65.0], [45.0, 100.0]]" in js
    assert "irt-map-view-controls" in js
    assert "Fit to India" in js
    assert "Toggle fullscreen" in js


def test_portfolio_legend_survives_streamlit_folium_extraction() -> None:
    # Regression guard (CHG-0278): the portfolio legend used to attach via
    # get_root().html, which streamlit-folium drops. It must now be a
    # bottomleft L.control MacroElement in the map's child tree.
    import pytest

    folium = pytest.importorskip("folium")
    streamlit_folium = pytest.importorskip("streamlit_folium")

    from india_resilience_tool.app.views.map_view import add_portfolio_legend_to_map

    m = folium.Map(location=[22.5, 80.0], zoom_start=5)
    add_portfolio_legend_to_map(m, 3, level="district")
    m.get_root().render()
    m.render()

    js = streamlit_folium._get_map_string(m)
    assert "irt-portfolio-legend-control" in js
    assert "In portfolio (3 districts)" in js
    assert "L.control({position: 'bottomleft'})" in js


def test_portfolio_legend_skipped_for_empty_portfolio() -> None:
    import pytest

    folium = pytest.importorskip("folium")
    streamlit_folium = pytest.importorskip("streamlit_folium")

    from india_resilience_tool.app.views.map_view import add_portfolio_legend_to_map

    m = folium.Map(location=[22.5, 80.0], zoom_start=5)
    add_portfolio_legend_to_map(m, 0, level="district")
    m.get_root().render()
    m.render()

    js = streamlit_folium._get_map_string(m)
    assert "irt-portfolio-legend-control" not in js
