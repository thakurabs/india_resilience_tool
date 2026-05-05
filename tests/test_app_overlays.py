"""Tests for dashboard reference overlay resolution."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from india_resilience_tool.app.overlays import (
    OVERLAY_DEFINITIONS,
    POPULATION_COLOR_RAMP,
    POPULATION_EXPOSURE_OVERLAY_ID,
    RP100_FLOOD_OVERLAY_ID,
    RIVER_NETWORK_OVERLAY_ID,
    OverlayRenderLayer,
    build_overlay_render_layers,
    discover_population_exposure_overlay_artifact,
    discover_rp100_overlay_artifact,
    ensure_overlay_session_state,
    overlay_cache_signature,
    resolve_overlay_control_states,
    validate_population_exposure_overlay_metadata,
    validate_rp100_overlay_metadata,
)


def _write_valid_pair(root: Path, *, width: int = 2, overlay_id: str = RP100_FLOOD_OVERLAY_ID) -> tuple[Path, Path]:
    png = root / "rp100_depth_overlay.png"
    meta = root / "rp100_depth_overlay_meta.json"
    root.mkdir(parents=True, exist_ok=True)
    png.write_bytes(b"png")
    meta.write_text(
        json.dumps(
            {
                "overlay_id": overlay_id,
                "source_raster_name": "RP100_depth.tif",
                "source_crs": "EPSG:4326",
                "image_crs": "EPSG:3857",
                "bounds_latlon": [[10.0, 70.0], [20.0, 80.0]],
                "display_value_min_m": 0.0,
                "display_value_max_m": 10.0,
                "source_positive_max_m": 12.0,
                "clipped_above_display_max": True,
                "display_units": "meters",
                "width_px": width,
                "height_px": 2,
            }
        ),
        encoding="utf-8",
    )
    return png, meta


def _valid_population_metadata(*, width: int = 2, bounds=None) -> dict:
    return {
        "overlay_id": POPULATION_EXPOSURE_OVERLAY_ID,
        "source_raster_name": "ind_pop_2025_CN_1km_R2025A_UA_v1.tif",
        "source_crs": "EPSG:4326",
        "bounds_latlon": bounds or [[10.0, 70.0], [20.0, 80.0]],
        "display_units": "people per source cell",
        "display_transform": "binned_people_per_source_cell",
        "display_value_min_people_per_cell": 0.0,
        "display_value_max_people_per_cell": 10000.0,
        "source_positive_max_people_per_cell": 12000.0,
        "clipped_above_display_max": True,
        "width_px": width,
        "height_px": 2,
        "color_ramp": POPULATION_COLOR_RAMP,
    }


def _write_valid_population_pair(root: Path, *, width: int = 2, bounds=None) -> tuple[Path, Path]:
    png = root / "population_exposure_2025_overlay.png"
    meta = root / "population_exposure_2025_overlay_meta.json"
    root.mkdir(parents=True, exist_ok=True)
    png.write_bytes(b"png")
    meta.write_text(json.dumps(_valid_population_metadata(width=width, bounds=bounds)), encoding="utf-8")
    return png, meta


def test_overlay_session_state_defaults_clamps_and_migrates_legacy_river_key() -> None:
    ss = {"show_river_network": True, "overlay_rp100_flood_depth_raster_opacity_pct": 150}
    ensure_overlay_session_state(ss)
    assert ss["overlay_river_network_enabled"] is True
    assert ss["overlay_river_network_opacity_pct"] == 75
    assert ss["overlay_rp100_flood_depth_raster_opacity_pct"] == 100
    assert ss["overlay_population_exposure_2025_raster_enabled"] is False
    assert ss["overlay_population_exposure_2025_raster_opacity_pct"] == 50


def test_flood_visibility_availability_and_forced_off_for_non_telangana(tmp_path: Path) -> None:
    _write_valid_pair(tmp_path / "jrc_flood_depth" / "overlay")
    ss = {"overlay_rp100_flood_depth_raster_enabled": True}
    states = resolve_overlay_control_states(
        session_state=ss,
        spatial_family="admin",
        admin_level="district",
        selected_state="Maharashtra",
        selected_basin="All",
        river_display_geojson_path=tmp_path / "river_network_display.geojson",
        data_dir=tmp_path,
    )
    flood = states[RP100_FLOOD_OVERLAY_ID]
    assert flood.visible is True
    assert flood.available is False
    assert flood.active is False
    assert ss["overlay_rp100_flood_depth_raster_enabled"] is False


def test_river_visibility_requires_hydro_selected_basin(tmp_path: Path) -> None:
    river = tmp_path / "river_network_display.geojson"
    river.write_text("{}", encoding="utf-8")
    states = resolve_overlay_control_states(
        session_state={},
        spatial_family="hydro",
        admin_level="basin",
        selected_state="All",
        selected_basin="Godavari Basin",
        river_display_geojson_path=river,
        data_dir=tmp_path,
    )
    assert states[RIVER_NETWORK_OVERLAY_ID].visible is True
    assert states[RIVER_NETWORK_OVERLAY_ID].available is True
    assert states[RP100_FLOOD_OVERLAY_ID].visible is False


@pytest.mark.parametrize(
    ("family", "level"),
    [
        ("admin", "district"),
        ("admin", "block"),
        ("hydro", "basin"),
        ("hydro", "sub_basin"),
    ],
)
def test_population_overlay_visible_across_dashboard_levels(tmp_path: Path, family: str, level: str) -> None:
    _write_valid_population_pair(tmp_path / "population" / "overlay")
    states = resolve_overlay_control_states(
        session_state={},
        spatial_family=family,
        admin_level=level,
        selected_state="All",
        selected_basin="All",
        river_display_geojson_path=tmp_path / "river_network_display.geojson",
        data_dir=tmp_path,
    )
    population = states[POPULATION_EXPOSURE_OVERLAY_ID]
    assert population.visible is True
    assert population.available is True


def test_population_overlay_available_in_hydro_all_basin_while_river_is_not(tmp_path: Path) -> None:
    _write_valid_population_pair(tmp_path / "population" / "overlay")
    river = tmp_path / "river_network_display.geojson"
    river.write_text("{}", encoding="utf-8")
    states = resolve_overlay_control_states(
        session_state={},
        spatial_family="hydro",
        admin_level="basin",
        selected_state="All",
        selected_basin="All",
        river_display_geojson_path=river,
        data_dir=tmp_path,
    )
    assert states[POPULATION_EXPOSURE_OVERLAY_ID].available is True
    assert states[RIVER_NETWORK_OVERLAY_ID].available is False


def test_population_overlay_missing_artifacts_use_actionable_message(tmp_path: Path) -> None:
    states = resolve_overlay_control_states(
        session_state={},
        spatial_family="admin",
        admin_level="district",
        selected_state="All",
        selected_basin="All",
        river_display_geojson_path=tmp_path / "river_network_display.geojson",
        data_dir=tmp_path,
    )
    population = states[POPULATION_EXPOSURE_OVERLAY_ID]
    assert population.visible is True
    assert population.available is False
    assert "across all map levels" in str(population.unavailable_caption)
    assert population.availability_reason == (
        "Population overlay artifacts are not exported yet. Run the population exposure build to create the PNG and metadata."
    )
    assert "processed_optimised" not in str(population.availability_reason)


def test_rp100_artifact_precedence_and_invalid_fallback(tmp_path: Path) -> None:
    optimized = tmp_path / "processed_optimised" / "context" / "jrc_flood_depth" / "overlay"
    canonical = tmp_path / "jrc_flood_depth" / "overlay"
    opt_png, _ = _write_valid_pair(optimized)
    can_png, _ = _write_valid_pair(canonical)
    found, _meta, reason = discover_rp100_overlay_artifact(data_dir=tmp_path)
    assert found == opt_png
    assert reason is None

    (optimized / "rp100_depth_overlay_meta.json").write_text("{bad", encoding="utf-8")
    found, _meta, reason = discover_rp100_overlay_artifact(data_dir=tmp_path)
    assert found == can_png
    assert reason is None


def test_population_artifact_precedence_and_invalid_fallback(tmp_path: Path) -> None:
    optimized = tmp_path / "processed_optimised" / "context" / "population" / "overlay"
    canonical = tmp_path / "population" / "overlay"
    opt_png, _ = _write_valid_population_pair(optimized)
    can_png, _ = _write_valid_population_pair(canonical)
    found, meta, reason = discover_population_exposure_overlay_artifact(data_dir=tmp_path)
    assert found == opt_png
    assert meta is not None
    assert reason is None

    (optimized / "population_exposure_2025_overlay_meta.json").write_text("{bad", encoding="utf-8")
    found, _meta, reason = discover_population_exposure_overlay_artifact(data_dir=tmp_path)
    assert found == can_png
    assert reason is None


@pytest.mark.parametrize(
    "mutate",
    [
        lambda root: (root / "population_exposure_2025_overlay.png").unlink(),
        lambda root: (root / "population_exposure_2025_overlay_meta.json").unlink(),
        lambda root: (root / "population_exposure_2025_overlay_meta.json").write_text("{bad", encoding="utf-8"),
        lambda root: (root / "population_exposure_2025_overlay_meta.json").write_text(
            json.dumps(_valid_population_metadata(bounds=[[20.0, 70.0], [10.0, 80.0]])),
            encoding="utf-8",
        ),
        lambda root: _write_valid_population_pair(root, width=4097),
    ],
)
def test_population_artifact_invalid_cases_disable_pair(tmp_path: Path, mutate) -> None:
    root = tmp_path / "population" / "overlay"
    _write_valid_population_pair(root)
    mutate(root)
    png, meta, reason = discover_population_exposure_overlay_artifact(data_dir=tmp_path)
    assert png is None
    assert meta is None
    assert reason


def test_population_metadata_validator_returns_normalized_schema() -> None:
    normalized = validate_population_exposure_overlay_metadata(_valid_population_metadata())
    assert set(normalized) == {
        "overlay_id",
        "source_raster_name",
        "source_crs",
        "bounds_latlon",
        "display_units",
        "display_transform",
        "display_value_min_people_per_cell",
        "display_value_max_people_per_cell",
        "source_positive_max_people_per_cell",
        "clipped_above_display_max",
        "width_px",
        "height_px",
        "color_ramp",
    }
    assert normalized["color_ramp"] == POPULATION_COLOR_RAMP


def test_rp100_metadata_validator_requires_mercator_image_crs() -> None:
    raw = {
        "overlay_id": RP100_FLOOD_OVERLAY_ID,
        "source_raster_name": "RP100_depth.tif",
        "source_crs": "EPSG:4326",
        "image_crs": "EPSG:3857",
        "bounds_latlon": [[10.0, 70.0], [20.0, 80.0]],
        "display_value_min_m": 0.0,
        "display_value_max_m": 10.0,
        "source_positive_max_m": 12.0,
        "clipped_above_display_max": True,
        "display_units": "meters",
        "width_px": 2,
        "height_px": 2,
    }
    normalized = validate_rp100_overlay_metadata(raw)
    assert normalized["image_crs"] == "EPSG:3857"

    stale = dict(raw)
    stale.pop("image_crs")
    with pytest.raises(ValueError, match="image_crs must equal EPSG:3857"):
        validate_rp100_overlay_metadata(stale)


def test_active_population_overlay_zero_opacity_returns_image_layer(tmp_path: Path) -> None:
    png, _ = _write_valid_population_pair(tmp_path / "population" / "overlay")
    states = {
        POPULATION_EXPOSURE_OVERLAY_ID: resolve_overlay_control_states(
            session_state={
                "overlay_population_exposure_2025_raster_enabled": True,
                "overlay_population_exposure_2025_raster_opacity_pct": 0,
            },
            spatial_family="hydro",
            admin_level="basin",
            selected_state="All",
            selected_basin="All",
            river_display_geojson_path=tmp_path / "river_network_display.geojson",
            data_dir=tmp_path,
        )[POPULATION_EXPOSURE_OVERLAY_ID]
    }
    layers, messages, signature = build_overlay_render_layers(
        overlay_states=states,
        spatial_family="hydro",
        admin_level="basin",
        selected_basin="All",
        selected_subbasin="All",
        data_dir=tmp_path,
        river_display_geojson_path=tmp_path / "river_network_display.geojson",
        river_basin_reconciliation_path=tmp_path / "river_basin_name_reconciliation.csv",
        river_subbasin_diagnostics_path=tmp_path / "river_subbasin_diagnostics.csv",
        alias_fn=lambda value: str(value).strip().lower(),
    )
    assert messages == ()
    assert len(layers) == 1
    assert layers[0].image_path == png
    assert layers[0].opacity == 0.0
    assert layers[0].pane == "irt-population-raster"
    hash(signature)


@pytest.mark.parametrize(
    "mutate",
    [
        lambda root: (root / "rp100_depth_overlay.png").unlink(),
        lambda root: (root / "rp100_depth_overlay_meta.json").unlink(),
        lambda root: (root / "rp100_depth_overlay_meta.json").write_text("{bad", encoding="utf-8"),
        lambda root: (root / "rp100_depth_overlay_meta.json").write_text(
            json.dumps(
                {
                    "overlay_id": RP100_FLOOD_OVERLAY_ID,
                    "source_raster_name": "RP100_depth.tif",
                    "source_crs": "EPSG:4326",
                    "image_crs": "EPSG:3857",
                    "bounds_latlon": [[20.0, 70.0], [10.0, 80.0]],
                    "display_value_min_m": 0.0,
                    "display_value_max_m": 10.0,
                    "source_positive_max_m": 0.0,
                    "clipped_above_display_max": False,
                    "display_units": "meters",
                    "width_px": 2,
                    "height_px": 2,
                }
            ),
            encoding="utf-8",
        ),
        lambda root: _write_valid_pair(root, width=4097),
    ],
)
def test_rp100_artifact_invalid_cases_disable_pair(tmp_path: Path, mutate) -> None:
    root = tmp_path / "jrc_flood_depth" / "overlay"
    _write_valid_pair(root)
    mutate(root)
    png, meta, reason = discover_rp100_overlay_artifact(data_dir=tmp_path)
    assert png is None
    assert meta is None
    assert reason


def test_overlay_render_layer_invariants_and_zero_opacity() -> None:
    layer = OverlayRenderLayer(
        overlay_id=RP100_FLOOD_OVERLAY_ID,
        kind="image",
        name="RP-100 Flood Depth Raster",
        opacity=0.0,
        opacity_pct=0,
        image_path=Path("/tmp/rp100.png"),
        bounds_latlon=[[10.0, 70.0], [20.0, 80.0]],
    )
    assert layer.opacity == 0.0
    signature = overlay_cache_signature((layer,))
    hash(signature)
    assert signature[-1] == ((10.0, 70.0), (20.0, 80.0))
    with pytest.raises(ValueError):
        OverlayRenderLayer(
            overlay_id=RIVER_NETWORK_OVERLAY_ID,
            kind="geojson",
            name="River network",
            opacity=0.75,
            opacity_pct=75,
        )


def test_overlay_definition_sidebar_order() -> None:
    assert tuple(OVERLAY_DEFINITIONS) == (
        RP100_FLOOD_OVERLAY_ID,
        POPULATION_EXPOSURE_OVERLAY_ID,
        RIVER_NETWORK_OVERLAY_ID,
    )
