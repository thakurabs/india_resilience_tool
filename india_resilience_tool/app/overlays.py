"""Reference overlay contracts and resolution helpers for the dashboard."""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Literal, Mapping, MutableMapping, Optional


OverlayKind = Literal["image", "geojson"]

RP100_FLOOD_OVERLAY_ID = "rp100_flood_depth_raster"
POPULATION_EXPOSURE_OVERLAY_ID = "population_exposure_2025_raster"
RIVER_NETWORK_OVERLAY_ID = "river_network"

FLOOD_LABEL = "RP-100 flood depth"
POPULATION_LABEL = "Population exposure (2025)"
RIVER_LABEL = "River network"
FLOOD_UNAVAILABLE_CAPTION = (
    "Available for Telangana and All-state admin views when the RP-100 overlay artifact is present."
)
POPULATION_UNAVAILABLE_CAPTION = (
    "Available across all map levels when the population exposure overlay artifact is present."
)
RIVER_UNAVAILABLE_CAPTION = "Select a basin or sub-basin to enable the river network overlay."

POPULATION_COLOR_RAMP: list[dict[str, Any]] = [
    {"min_value_exclusive": None, "max_value_inclusive": 0.0, "color_hex": None, "transparent": True},
    {"min_value_exclusive": 0.0, "max_value_inclusive": 25.0, "color_hex": "#fff7bc", "transparent": False},
    {"min_value_exclusive": 25.0, "max_value_inclusive": 100.0, "color_hex": "#fee391", "transparent": False},
    {"min_value_exclusive": 100.0, "max_value_inclusive": 250.0, "color_hex": "#fec44f", "transparent": False},
    {"min_value_exclusive": 250.0, "max_value_inclusive": 500.0, "color_hex": "#fe9929", "transparent": False},
    {"min_value_exclusive": 500.0, "max_value_inclusive": 1000.0, "color_hex": "#ec7014", "transparent": False},
    {"min_value_exclusive": 1000.0, "max_value_inclusive": 2500.0, "color_hex": "#cc4c02", "transparent": False},
    {"min_value_exclusive": 2500.0, "max_value_inclusive": 5000.0, "color_hex": "#993404", "transparent": False},
    {"min_value_exclusive": 5000.0, "max_value_inclusive": 10000.0, "color_hex": "#7f1d1d", "transparent": False},
    {"min_value_exclusive": 10000.0, "max_value_inclusive": None, "color_hex": "#4c0519", "transparent": False},
]


@dataclass(frozen=True)
class OverlayDefinition:
    """Static registry entry for one reference overlay."""

    overlay_id: str
    label: str
    slider_label: str
    enabled_key: str
    opacity_key: str
    default_enabled: bool
    default_opacity_pct: int
    unavailable_caption: str


@dataclass(frozen=True)
class OverlayControlState:
    """Resolved sidebar/control state for one overlay in the current geography."""

    overlay_id: str
    label: str
    slider_label: str
    enabled_key: str
    opacity_key: str
    visible: bool
    available: bool
    enabled: bool
    opacity_pct: int
    unavailable_caption: Optional[str] = None
    availability_reason: Optional[str] = None

    @property
    def active(self) -> bool:
        """Return whether this overlay should be rendered."""
        return bool(self.visible and self.available and self.enabled)


@dataclass(frozen=True)
class OverlayRenderLayer:
    """Concrete layer payload consumed by the Folium map renderer."""

    overlay_id: str
    kind: OverlayKind
    name: str
    opacity: float
    opacity_pct: int
    image_path: Optional[Path] = None
    bounds_latlon: Optional[list[list[float]]] = None
    feature_collection: Optional[Mapping[str, Any]] = None
    tooltip_fields: Optional[tuple[str, ...]] = None
    pane: Optional[str] = None

    def __post_init__(self) -> None:
        if self.kind == "image":
            if self.image_path is None or self.bounds_latlon is None:
                raise ValueError("Image overlay layers require image_path and bounds_latlon.")
            if self.feature_collection is not None or self.tooltip_fields is not None:
                raise ValueError("Image overlay layers must not set feature_collection or tooltip_fields.")
        elif self.kind == "geojson":
            if self.feature_collection is None:
                raise ValueError("GeoJSON overlay layers require feature_collection.")
            if self.image_path is not None or self.bounds_latlon is not None:
                raise ValueError("GeoJSON overlay layers must not set image_path or bounds_latlon.")
            if self.tooltip_fields is None:
                raise ValueError("GeoJSON overlay layers require tooltip_fields.")
        else:
            raise ValueError(f"Unsupported overlay kind: {self.kind!r}")
        if not math.isfinite(float(self.opacity)):
            raise ValueError("Overlay opacity must be finite.")


OVERLAY_DEFINITIONS: dict[str, OverlayDefinition] = {
    RP100_FLOOD_OVERLAY_ID: OverlayDefinition(
        overlay_id=RP100_FLOOD_OVERLAY_ID,
        label=FLOOD_LABEL,
        slider_label="RP-100 flood depth opacity",
        enabled_key="overlay_rp100_flood_depth_raster_enabled",
        opacity_key="overlay_rp100_flood_depth_raster_opacity_pct",
        default_enabled=False,
        default_opacity_pct=65,
        unavailable_caption=FLOOD_UNAVAILABLE_CAPTION,
    ),
    POPULATION_EXPOSURE_OVERLAY_ID: OverlayDefinition(
        overlay_id=POPULATION_EXPOSURE_OVERLAY_ID,
        label=POPULATION_LABEL,
        slider_label="Population exposure opacity",
        enabled_key="overlay_population_exposure_2025_raster_enabled",
        opacity_key="overlay_population_exposure_2025_raster_opacity_pct",
        default_enabled=False,
        default_opacity_pct=50,
        unavailable_caption=POPULATION_UNAVAILABLE_CAPTION,
    ),
    RIVER_NETWORK_OVERLAY_ID: OverlayDefinition(
        overlay_id=RIVER_NETWORK_OVERLAY_ID,
        label=RIVER_LABEL,
        slider_label="River network opacity",
        enabled_key="overlay_river_network_enabled",
        opacity_key="overlay_river_network_opacity_pct",
        default_enabled=False,
        default_opacity_pct=75,
        unavailable_caption=RIVER_UNAVAILABLE_CAPTION,
    ),
}


def _clamp_opacity_pct(value: object, *, default: int) -> int:
    try:
        pct = int(round(float(value)))
    except (TypeError, ValueError):
        pct = int(default)
    return max(0, min(100, pct))


def ensure_overlay_session_state(session_state: MutableMapping[str, Any]) -> None:
    """Seed overlay session keys, clamp opacity, and migrate the legacy river key."""
    river_def = OVERLAY_DEFINITIONS[RIVER_NETWORK_OVERLAY_ID]
    if river_def.enabled_key not in session_state and "show_river_network" in session_state:
        session_state[river_def.enabled_key] = bool(session_state.get("show_river_network"))

    for definition in OVERLAY_DEFINITIONS.values():
        session_state.setdefault(definition.enabled_key, bool(definition.default_enabled))
        if definition.opacity_key not in session_state:
            session_state[definition.opacity_key] = int(definition.default_opacity_pct)
        session_state[definition.opacity_key] = _clamp_opacity_pct(
            session_state.get(definition.opacity_key),
            default=definition.default_opacity_pct,
        )


def _mtime_token(path: Path) -> Optional[float]:
    try:
        return float(path.stat().st_mtime)
    except OSError:
        return None


def _validate_bounds_latlon(value: object) -> list[list[float]]:
    if not (
        isinstance(value, list)
        and len(value) == 2
        and all(isinstance(row, list) and len(row) == 2 for row in value)
    ):
        raise ValueError("bounds_latlon must have shape [[south, west], [north, east]].")
    south = float(value[0][0])
    west = float(value[0][1])
    north = float(value[1][0])
    east = float(value[1][1])
    for number in (south, west, north, east):
        if not math.isfinite(number):
            raise ValueError("bounds_latlon values must be finite.")
    if not south < north:
        raise ValueError("bounds_latlon south must be less than north.")
    if not west < east:
        raise ValueError("bounds_latlon west must be less than east.")
    return [[round(south, 6), round(west, 6)], [round(north, 6), round(east, 6)]]


def validate_rp100_overlay_metadata(meta: Mapping[str, Any]) -> dict[str, Any]:
    """Validate RP-100 overlay metadata and return normalized values."""
    if str(meta.get("overlay_id") or "") != RP100_FLOOD_OVERLAY_ID:
        raise ValueError("metadata overlay_id must equal rp100_flood_depth_raster.")
    if str(meta.get("source_raster_name") or "") != "RP100_depth.tif":
        raise ValueError("metadata source_raster_name must equal RP100_depth.tif.")
    if str(meta.get("display_units") or "") != "meters":
        raise ValueError("metadata display_units must equal meters.")

    source_crs = str(meta.get("source_crs") or "").strip()
    if not source_crs:
        raise ValueError("metadata source_crs is required.")
    image_crs = str(meta.get("image_crs") or "").strip()
    if image_crs != "EPSG:3857":
        raise ValueError("metadata image_crs must equal EPSG:3857. Rebuild the RP-100 overlay artifacts.")
    bounds = _validate_bounds_latlon(meta.get("bounds_latlon"))

    display_min = float(meta.get("display_value_min_m"))
    display_max = float(meta.get("display_value_max_m"))
    source_positive_max = float(meta.get("source_positive_max_m"))
    width_px = int(meta.get("width_px"))
    height_px = int(meta.get("height_px"))
    for number in (display_min, display_max, source_positive_max):
        if not math.isfinite(number):
            raise ValueError("metadata numeric fields must be finite.")
    if display_min != 0.0 or display_max != 10.0:
        raise ValueError("metadata display scale must be 0.0 to 10.0 m.")
    if source_positive_max < 0.0:
        raise ValueError("metadata source_positive_max_m must be non-negative.")
    if width_px <= 0 or height_px <= 0 or width_px > 4096 or height_px > 4096:
        raise ValueError("metadata width_px and height_px must be in 1..4096.")
    clipped = meta.get("clipped_above_display_max")
    if not isinstance(clipped, bool):
        raise ValueError("metadata clipped_above_display_max must be boolean.")

    return {
        "overlay_id": RP100_FLOOD_OVERLAY_ID,
        "source_raster_name": "RP100_depth.tif",
        "source_crs": source_crs,
        "image_crs": "EPSG:3857",
        "bounds_latlon": bounds,
        "display_value_min_m": 0.0,
        "display_value_max_m": 10.0,
        "source_positive_max_m": float(source_positive_max),
        "clipped_above_display_max": bool(clipped),
        "display_units": "meters",
        "width_px": int(width_px),
        "height_px": int(height_px),
    }


def _finite_float(meta: Mapping[str, Any], key: str) -> float:
    value = float(meta.get(key))
    if not math.isfinite(value):
        raise ValueError(f"metadata {key} must be finite.")
    return value


def validate_population_exposure_overlay_metadata(meta: Mapping[str, Any]) -> dict[str, Any]:
    """Validate population overlay metadata and return normalized values."""
    if str(meta.get("overlay_id") or "") != POPULATION_EXPOSURE_OVERLAY_ID:
        raise ValueError("metadata overlay_id must equal population_exposure_2025_raster.")
    if str(meta.get("source_raster_name") or "") != "ind_pop_2025_CN_1km_R2025A_UA_v1.tif":
        raise ValueError("metadata source_raster_name must equal ind_pop_2025_CN_1km_R2025A_UA_v1.tif.")
    if str(meta.get("display_units") or "") != "people per source cell":
        raise ValueError("metadata display_units must equal people per source cell.")
    if str(meta.get("display_transform") or "") != "binned_people_per_source_cell":
        raise ValueError("metadata display_transform must equal binned_people_per_source_cell.")

    source_crs = str(meta.get("source_crs") or "").strip()
    if not source_crs:
        raise ValueError("metadata source_crs is required.")
    bounds = _validate_bounds_latlon(meta.get("bounds_latlon"))

    display_min = _finite_float(meta, "display_value_min_people_per_cell")
    display_max = _finite_float(meta, "display_value_max_people_per_cell")
    source_positive_max = _finite_float(meta, "source_positive_max_people_per_cell")
    width_px = int(meta.get("width_px"))
    height_px = int(meta.get("height_px"))
    if display_min != 0.0 or display_max != 10000.0:
        raise ValueError("metadata display scale must be 0.0 to 10000.0 people per source cell.")
    if source_positive_max < 0.0:
        raise ValueError("metadata source_positive_max_people_per_cell must be non-negative.")
    if width_px <= 0 or height_px <= 0 or width_px > 4096 or height_px > 4096:
        raise ValueError("metadata width_px and height_px must be in 1..4096.")
    clipped = meta.get("clipped_above_display_max")
    if not isinstance(clipped, bool):
        raise ValueError("metadata clipped_above_display_max must be boolean.")
    if meta.get("color_ramp") != POPULATION_COLOR_RAMP:
        raise ValueError("metadata color_ramp must match the canonical population ramp.")

    return {
        "overlay_id": POPULATION_EXPOSURE_OVERLAY_ID,
        "source_raster_name": "ind_pop_2025_CN_1km_R2025A_UA_v1.tif",
        "source_crs": source_crs,
        "bounds_latlon": bounds,
        "display_units": "people per source cell",
        "display_transform": "binned_people_per_source_cell",
        "display_value_min_people_per_cell": 0.0,
        "display_value_max_people_per_cell": 10000.0,
        "source_positive_max_people_per_cell": float(source_positive_max),
        "clipped_above_display_max": bool(clipped),
        "width_px": int(width_px),
        "height_px": int(height_px),
        "color_ramp": [dict(item) for item in POPULATION_COLOR_RAMP],
    }


def _load_valid_rp100_artifact_pair(png_path: Path, meta_path: Path) -> tuple[Path, dict[str, Any]]:
    if not png_path.exists():
        raise FileNotFoundError(f"RP-100 overlay PNG not found: {png_path}")
    if not meta_path.exists():
        raise FileNotFoundError(f"RP-100 overlay metadata not found: {meta_path}")
    try:
        raw_meta = json.loads(meta_path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise ValueError(f"RP-100 overlay metadata is malformed: {meta_path}") from exc
    if not isinstance(raw_meta, dict):
        raise ValueError("RP-100 overlay metadata must be a JSON object.")
    return png_path, validate_rp100_overlay_metadata(raw_meta)


def discover_rp100_overlay_artifact(*, data_dir: Path) -> tuple[Optional[Path], Optional[dict[str, Any]], Optional[str]]:
    """Return the first valid optimized/canonical RP-100 overlay artifact pair."""
    candidates = (
        (
            data_dir / "processed_optimised" / "context" / "jrc_flood_depth" / "overlay" / "rp100_depth_overlay.png",
            data_dir / "processed_optimised" / "context" / "jrc_flood_depth" / "overlay" / "rp100_depth_overlay_meta.json",
        ),
        (
            data_dir / "jrc_flood_depth" / "overlay" / "rp100_depth_overlay.png",
            data_dir / "jrc_flood_depth" / "overlay" / "rp100_depth_overlay_meta.json",
        ),
    )
    first_error: Optional[str] = None
    for png_path, meta_path in candidates:
        try:
            return (*_load_valid_rp100_artifact_pair(png_path, meta_path), None)
        except Exception as exc:
            if first_error is None:
                first_error = str(exc)
    return None, None, first_error or "RP-100 overlay artifact pair is unavailable."


def _load_valid_population_artifact_pair(png_path: Path, meta_path: Path) -> tuple[Path, dict[str, Any]]:
    if not png_path.exists():
        raise FileNotFoundError(f"Population overlay PNG not found: {png_path}")
    if not meta_path.exists():
        raise FileNotFoundError(f"Population overlay metadata not found: {meta_path}")
    try:
        raw_meta = json.loads(meta_path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise ValueError(f"Population overlay metadata is malformed: {meta_path}") from exc
    if not isinstance(raw_meta, dict):
        raise ValueError("Population overlay metadata must be a JSON object.")
    return png_path, validate_population_exposure_overlay_metadata(raw_meta)


def discover_population_exposure_overlay_artifact(
    *,
    data_dir: Path,
) -> tuple[Optional[Path], Optional[dict[str, Any]], Optional[str]]:
    """Return the first valid optimized/canonical population overlay artifact pair."""
    candidates = (
        (
            data_dir / "processed_optimised" / "context" / "population" / "overlay" / "population_exposure_2025_overlay.png",
            data_dir / "processed_optimised" / "context" / "population" / "overlay" / "population_exposure_2025_overlay_meta.json",
        ),
        (
            data_dir / "population" / "overlay" / "population_exposure_2025_overlay.png",
            data_dir / "population" / "overlay" / "population_exposure_2025_overlay_meta.json",
        ),
    )
    first_error: Optional[str] = None
    missing_count = 0
    for png_path, meta_path in candidates:
        try:
            return (*_load_valid_population_artifact_pair(png_path, meta_path), None)
        except Exception as exc:
            if isinstance(exc, FileNotFoundError):
                missing_count += 1
            if first_error is None:
                first_error = str(exc)
    if missing_count == len(candidates):
        return (
            None,
            None,
            "Population overlay artifacts are not exported yet. Run the population exposure build to create the PNG and metadata.",
        )
    return None, None, first_error or "Population overlay artifact pair is unavailable."


def resolve_overlay_control_states(
    *,
    session_state: MutableMapping[str, Any],
    spatial_family: str,
    admin_level: str,
    selected_state: str,
    selected_basin: str,
    river_display_geojson_path: Path,
    data_dir: Path,
) -> dict[str, OverlayControlState]:
    """Resolve visible, available, enabled, and opacity state for registered overlays."""
    ensure_overlay_session_state(session_state)
    family = str(spatial_family or "").strip().lower()
    level = str(admin_level or "").strip().lower()
    selected_state_norm = str(selected_state or "All").strip()
    selected_basin_norm = str(selected_basin or "All").strip()

    flood_visible = family == "admin" and level in {"district", "block"}
    flood_png, _flood_meta, flood_reason = discover_rp100_overlay_artifact(data_dir=data_dir)
    flood_available = (
        flood_visible
        and flood_png is not None
        and selected_state_norm in {"All", "Telangana"}
    )

    pop_visible = True
    pop_png, _pop_meta, pop_reason = discover_population_exposure_overlay_artifact(data_dir=data_dir)
    pop_available = pop_visible and pop_png is not None

    river_visible = family == "hydro" and level in {"basin", "sub_basin"}
    river_reason = None
    if not river_display_geojson_path.exists():
        river_reason = "River overlay unavailable: river_network_display.geojson not found."
    river_available = river_visible and river_display_geojson_path.exists() and selected_basin_norm != "All"

    state_specs = {
        RP100_FLOOD_OVERLAY_ID: (flood_visible, flood_available, flood_reason),
        POPULATION_EXPOSURE_OVERLAY_ID: (pop_visible, pop_available, pop_reason),
        RIVER_NETWORK_OVERLAY_ID: (river_visible, river_available, river_reason),
    }
    resolved: dict[str, OverlayControlState] = {}
    for overlay_id, (visible, available, reason) in state_specs.items():
        definition = OVERLAY_DEFINITIONS[overlay_id]
        if visible and not available:
            session_state[definition.enabled_key] = False
        opacity_pct = _clamp_opacity_pct(
            session_state.get(definition.opacity_key),
            default=definition.default_opacity_pct,
        )
        session_state[definition.opacity_key] = opacity_pct
        enabled = bool(session_state.get(definition.enabled_key, False))
        resolved[overlay_id] = OverlayControlState(
            overlay_id=overlay_id,
            label=definition.label,
            slider_label=definition.slider_label,
            enabled_key=definition.enabled_key,
            opacity_key=definition.opacity_key,
            visible=bool(visible),
            available=bool(available),
            enabled=bool(enabled and available),
            opacity_pct=opacity_pct,
            unavailable_caption=definition.unavailable_caption if visible and not available else None,
            availability_reason=reason if visible and not available else None,
        )
    return resolved


def overlay_cache_signature(layers: tuple[OverlayRenderLayer, ...]) -> tuple[Any, ...]:
    """Return cache-signature inputs for prepared overlay layers."""
    parts: list[Any] = []
    for layer in layers:
        path = layer.image_path
        parts.extend(
            [
                layer.overlay_id,
                True,
                int(layer.opacity_pct),
                str(layer.pane) if layer.pane is not None else None,
                str(path.resolve()) if path is not None else None,
                _mtime_token(path) if path is not None else None,
            ]
        )
        if layer.kind == "image":
            bounds = layer.bounds_latlon or []
            parts.append(tuple(tuple(float(value) for value in row) for row in bounds))
        else:
            feature_count = len(list((layer.feature_collection or {}).get("features", []) or []))
            parts.append(feature_count)
    return tuple(parts)


def _load_table_if_exists(path: Path, loader: Callable[[str], Any]) -> Any:
    return loader(str(path)) if path.exists() else None


def build_overlay_render_layers(
    *,
    overlay_states: Mapping[str, OverlayControlState],
    spatial_family: str,
    admin_level: str,
    selected_basin: str,
    selected_subbasin: str,
    data_dir: Path,
    river_display_geojson_path: Path,
    river_basin_reconciliation_path: Path,
    river_subbasin_diagnostics_path: Path,
    alias_fn: Callable[[str], str],
) -> tuple[tuple[OverlayRenderLayer, ...], tuple[str, ...], tuple[Any, ...]]:
    """Resolve active control states into concrete map layers and overlay messages."""
    from india_resilience_tool.app.geo_cache import (
        build_river_geojson_by_basin,
        build_river_geojson_by_subbasin,
        load_river_basin_reconciliation_cached,
        load_river_subbasin_diagnostics_cached,
    )
    from india_resilience_tool.data.river_loader import (
        resolve_river_basin_reconciliation,
        resolve_river_subbasin_diagnostics,
    )
    from india_resilience_tool.viz.folium_featurecollection import clone_featurecollection_for_patch

    layers: list[OverlayRenderLayer] = []
    messages: list[str] = []
    family = str(spatial_family or "").strip().lower()
    level = str(admin_level or "").strip().lower()

    flood_state = overlay_states.get(RP100_FLOOD_OVERLAY_ID)
    if flood_state and flood_state.active:
        png_path, meta, _reason = discover_rp100_overlay_artifact(data_dir=data_dir)
        if png_path is not None and meta is not None:
            layers.append(
                OverlayRenderLayer(
                    overlay_id=RP100_FLOOD_OVERLAY_ID,
                    kind="image",
                    name="RP-100 Flood Depth Raster",
                    opacity=float(max(0, min(100, flood_state.opacity_pct)) / 100.0),
                    opacity_pct=flood_state.opacity_pct,
                    image_path=png_path,
                    bounds_latlon=meta["bounds_latlon"],
                )
            )

    population_state = overlay_states.get(POPULATION_EXPOSURE_OVERLAY_ID)
    if population_state and population_state.active:
        png_path, meta, _reason = discover_population_exposure_overlay_artifact(data_dir=data_dir)
        if png_path is not None and meta is not None:
            layers.append(
                OverlayRenderLayer(
                    overlay_id=POPULATION_EXPOSURE_OVERLAY_ID,
                    kind="image",
                    name="Population Exposure 2025 Raster",
                    opacity=float(max(0, min(100, population_state.opacity_pct)) / 100.0),
                    opacity_pct=population_state.opacity_pct,
                    image_path=png_path,
                    bounds_latlon=meta["bounds_latlon"],
                    pane="irt-population-raster",
                )
            )

    river_state = overlay_states.get(RIVER_NETWORK_OVERLAY_ID)
    if river_state and river_state.active and family == "hydro" and level in {"basin", "sub_basin"}:
        river_fc: Optional[Mapping[str, Any]] = None
        if level == "sub_basin" and selected_subbasin != "All":
            diagnostics_df = _load_table_if_exists(
                river_subbasin_diagnostics_path,
                load_river_subbasin_diagnostics_cached,
            )
            resolution = resolve_river_subbasin_diagnostics(
                hydro_subbasin_name=selected_subbasin,
                diagnostics_df=diagnostics_df,
                alias_fn=alias_fn,
            )
            if resolution.get("message"):
                messages.append(str(resolution["message"]))
            if resolution.get("status") == "matched" and river_display_geojson_path.exists():
                river_mtime = _mtime_token(river_display_geojson_path)
                river_by_selector = build_river_geojson_by_subbasin(
                    path=str(river_display_geojson_path),
                    mtime=float(river_mtime or 0.0),
                )
                river_fc = clone_featurecollection_for_patch(
                    river_by_selector.get(alias_fn(selected_subbasin), {"type": "FeatureCollection", "features": []})
                )
        else:
            reconciliation_df = _load_table_if_exists(
                river_basin_reconciliation_path,
                load_river_basin_reconciliation_cached,
            )
            resolution = resolve_river_basin_reconciliation(
                hydro_basin_name=selected_basin,
                reconciliation_df=reconciliation_df,
                alias_fn=alias_fn,
            )
            if resolution.get("message"):
                messages.append(str(resolution["message"]))
            resolved_name = str(resolution.get("river_basin_name") or "").strip()
            if resolution.get("status") == "matched" and resolved_name and river_display_geojson_path.exists():
                river_mtime = _mtime_token(river_display_geojson_path)
                river_by_selector = build_river_geojson_by_basin(
                    path=str(river_display_geojson_path),
                    mtime=float(river_mtime or 0.0),
                )
                river_fc = clone_featurecollection_for_patch(
                    river_by_selector.get(alias_fn(resolved_name), {"type": "FeatureCollection", "features": []})
                )

        if river_fc and list((river_fc or {}).get("features", []) or []):
            layers.append(
                OverlayRenderLayer(
                    overlay_id=RIVER_NETWORK_OVERLAY_ID,
                    kind="geojson",
                    name="River network",
                    opacity=float(max(0, min(100, river_state.opacity_pct)) / 100.0),
                    opacity_pct=river_state.opacity_pct,
                    feature_collection=river_fc,
                    tooltip_fields=(
                        "river_name_clean",
                        "basin_name_clean",
                        "subbasin_name_clean",
                        "length_km_source",
                    ),
                )
            )

    layer_tuple = tuple(layers)
    signature = overlay_cache_signature(layer_tuple)
    return layer_tuple, tuple(messages), signature
