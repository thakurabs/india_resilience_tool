"""
Map view (Folium + st_folium) rendering and event extraction.

This module provides:
- Map rendering with st_folium
- Click event extraction
- Portfolio district highlighting (blue borders)
- Inline add-to-portfolio controls
- Saved point markers

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

from __future__ import annotations

from contextlib import nullcontext
from typing import Any, Callable, Mapping, Optional, Tuple

RESPONSIVE_MAP_MIN_HEIGHT = 420
RESPONSIVE_MAP_MAX_HEIGHT = 700


def clamp_map_height(
    available_height: float,
    *,
    min_height: int = RESPONSIVE_MAP_MIN_HEIGHT,
    max_height: int = RESPONSIVE_MAP_MAX_HEIGHT,
) -> int:
    """
    Clamp a computed viewport height into the supported map display range.

    Args:
        available_height: Candidate height derived from viewport space.
        min_height: Lower bound for readable map rendering.
        max_height: Upper bound to avoid oversizing the map area.

    Returns:
        Integer map height constrained to the configured bounds.
    """
    try:
        height = int(round(float(available_height)))
    except (TypeError, ValueError):
        height = min_height
    return max(min_height, min(max_height, height))


def _build_responsive_map_resizer_html(
    *,
    map_key: str,
    default_height: int,
    min_height: int = RESPONSIVE_MAP_MIN_HEIGHT,
    max_height: int = RESPONSIVE_MAP_MAX_HEIGHT,
    bottom_margin: int = 16,
) -> str:
    """Return a tiny component payload that resizes the map/legend iframes."""
    fallback_height = clamp_map_height(default_height, min_height=min_height, max_height=max_height)
    return f"""
    <script>
    (function() {{
      const selfFrame = window.frameElement;
      if (!selfFrame || !window.parent) {{
        return;
      }}

      const mapKey = {map_key!r};
      const minHeight = {int(min_height)};
      const maxHeight = {int(max_height)};
      const bottomMargin = {int(bottom_margin)};
      const fallbackHeight = {int(fallback_height)};

      function clampHeight(value) {{
        const rounded = Math.round(Number(value));
        if (!Number.isFinite(rounded)) {{
          return fallbackHeight;
        }}
        return Math.max(minHeight, Math.min(maxHeight, rounded));
      }}

      function updateInnerFrame(iframe, height) {{
        iframe.style.height = `${{height}}px`;
        iframe.height = String(height);
        try {{
          const childWindow = iframe.contentWindow;
          const childDocument = childWindow && childWindow.document ? childWindow.document : null;
          if (!childDocument) {{
            return;
          }}
          if (childDocument.documentElement) {{
            childDocument.documentElement.style.height = `${{height}}px`;
          }}
          if (childDocument.body) {{
            childDocument.body.style.height = `${{height}}px`;
            childDocument.body.style.overflow = "hidden";
          }}
          const mapNode = childDocument.querySelector(".folium-map, #map, .st_folium");
          if (mapNode) {{
            mapNode.style.height = `${{height}}px`;
          }}
          if (childWindow && typeof childWindow.dispatchEvent === "function") {{
            childWindow.dispatchEvent(new Event("resize"));
          }}
        }} catch (error) {{
          /* Ignore cross-frame access failures; the outer iframe height is still applied. */
        }}
      }}

      function findTargetIframes(block, markerTop) {{
        return Array.from(block.querySelectorAll("iframe")).filter((iframe) => {{
          if (iframe === selfFrame) {{
            return false;
          }}
          const rect = iframe.getBoundingClientRect();
          return Math.abs(rect.top - markerTop) < 220 && rect.height >= 0;
        }});
      }}

      function resizeTargets() {{
        const hostBlock = selfFrame.closest('[data-testid="stVerticalBlock"]');
        if (!hostBlock) {{
          return;
        }}
        const marker = hostBlock.querySelector(`.irt-responsive-map-marker[data-map-key="${{mapKey}}"]`);
        if (!marker) {{
          return;
        }}

        const markerTop = marker.getBoundingClientRect().top;
        const availableHeight = window.parent.innerHeight - markerTop - bottomMargin;
        const targetHeight = clampHeight(availableHeight);
        const iframes = findTargetIframes(hostBlock, markerTop);
        if (!iframes.length) {{
          return;
        }}
        iframes.forEach((iframe) => updateInnerFrame(iframe, targetHeight));
      }}

      function scheduleResize() {{
        if (window.parent && typeof window.parent.requestAnimationFrame === "function") {{
          window.parent.requestAnimationFrame(resizeTargets);
          return;
        }}
        window.setTimeout(resizeTargets, 0);
      }}

      scheduleResize();
      window.setTimeout(scheduleResize, 50);
      window.setTimeout(scheduleResize, 250);
      window.setTimeout(scheduleResize, 1000);

      if (window.parent && typeof window.parent.addEventListener === "function") {{
        window.parent.addEventListener("resize", scheduleResize);
      }}
    }})();
    </script>
    """


def _render_responsive_map_resizer(
    *,
    map_key: str,
    default_height: int,
    min_height: int = RESPONSIVE_MAP_MIN_HEIGHT,
    max_height: int = RESPONSIVE_MAP_MAX_HEIGHT,
) -> None:
    """Inject a zero-height component that resizes the map and legend after render."""
    import streamlit.components.v1 as components

    components.html(
        _build_responsive_map_resizer_html(
            map_key=map_key,
            default_height=default_height,
            min_height=min_height,
            max_height=max_height,
        ),
        height=0,
        width=0,
    )


def build_choropleth_map_with_geojson_layer(
    *,
    fc: Mapping[str, Any],
    map_center: list[float],
    map_zoom: float,
    bounds_latlon: list[list[float]],
    adm1: Any,
    selected_state: str,
    selected_district: str,
    layer_name: str,
    tooltip: Any = None,
    highlight_function: Optional[Callable[[dict], dict]] = None,
    reference_fc: Optional[Mapping[str, Any]] = None,
    reference_level: Optional[str] = None,
    reference_layer_name: Optional[str] = None,
) -> Any:
    """
    Build a Folium map and attach the patched GeoJSON FeatureCollection as a layer.

    This function is Streamlit-free; it only builds a Folium map object. The
    Streamlit rendering is done by `render_map_view`.

    Args:
        fc: GeoJSON FeatureCollection (already patched with fillColor/value fields).
        map_center/map_zoom: initial view.
        bounds_latlon: [[min_lat, min_lon], [max_lat, max_lon]] max-bounds clamp.
        adm1: ADM1 GeoDataFrame used to fit bounds when a state is selected.
        selected_state/selected_district: selection context (fit state bounds only).
        layer_name: layer label ("Districts"/"Blocks").
        tooltip: optional folium GeoJsonTooltip.
        highlight_function: optional folium highlight function.
        reference_fc: optional filtered FeatureCollection for a related-units overlay.
        reference_level: level of the related overlay (`district` or `sub_basin`).
        reference_layer_name: human-facing name for the overlay layer.
    """
    import folium

    m = folium.Map(
        location=map_center,
        zoom_start=map_zoom,
        tiles="CartoDB positron",
        control_scale=False,
        min_zoom=4,
        max_zoom=12,
        prefer_canvas=True,
        zoom_control=True,
        dragging=True,
        scrollWheelZoom=True,
    )

    # Fit state bounds when a state is selected but district is "All"
    try:
        if selected_state != "All" and selected_district == "All":
            row_state = adm1[adm1["shapeName"].astype(str).str.strip() == selected_state]
            if not row_state.empty:
                b = row_state.iloc[0].geometry.bounds
                fit_bounds = [[b[1], b[0]], [b[3], b[2]]]
                _name = m.get_name()
                bounds_js = f"<script>var {_name} = {_name}; {_name}.fitBounds({fit_bounds});</script>"
                m.get_root().html.add_child(folium.Element(bounds_js))
    except Exception:
        pass

    # Clamp panning to India-ish bounds (legacy)
    try:
        _name = m.get_name()
        bounds_js = (
            f"<script>var {_name} = {_name}; {_name}.setMaxBounds({bounds_latlon});</script>"
        )
        m.get_root().html.add_child(folium.Element(bounds_js))
    except Exception:
        pass

    def _style_fn(feature: dict) -> dict:
        props = (feature or {}).get("properties", {}) if isinstance(feature, dict) else {}
        return {
            "fillColor": props.get("fillColor", "#cccccc"),
            "color": "#666666",
            "weight": 0.3,
            "fillOpacity": 0.7,
        }

    folium.GeoJson(
        data=dict(fc),
        name=layer_name,
        style_function=_style_fn,
        tooltip=tooltip,
        highlight_function=highlight_function,
        smooth_factor=1.5,
        zoom_on_click=False,
        bubblingMouseEvents=False,
    ).add_to(m)

    if reference_fc and list((reference_fc or {}).get("features", []) or []):
        reference_level_norm = str(reference_level or "").strip().lower()

        def _reference_style(_feature: dict) -> dict:
            if reference_level_norm in {"district", "block"}:
                outline_color = "#1d4ed8"
            else:
                outline_color = "#0f766e"
            return {
                "fillColor": "transparent",
                "color": outline_color,
                "weight": 2.0,
                "fillOpacity": 0.0,
            }

        reference_tooltip = None
        if reference_level_norm == "district":
            reference_tooltip = folium.features.GeoJsonTooltip(
                fields=["district_name", "state_name"],
                aliases=["District", "State"],
                localize=True,
                sticky=True,
            )
        elif reference_level_norm == "block":
            reference_tooltip = folium.features.GeoJsonTooltip(
                fields=["block_name", "district_name", "state_name"],
                aliases=["Block", "District", "State"],
                localize=True,
                sticky=True,
            )
        elif reference_level_norm == "basin":
            reference_tooltip = folium.features.GeoJsonTooltip(
                fields=["basin_name"],
                aliases=["Basin"],
                localize=True,
                sticky=True,
            )
        elif reference_level_norm == "sub_basin":
            reference_tooltip = folium.features.GeoJsonTooltip(
                fields=["subbasin_name", "basin_name"],
                aliases=["Sub-basin", "Basin"],
                localize=True,
                sticky=True,
            )

        folium.GeoJson(
            data=dict(reference_fc),
            name=str(reference_layer_name or "Related units"),
            style_function=_reference_style,
            tooltip=reference_tooltip,
            smooth_factor=1.5,
            zoom_on_click=False,
            bubblingMouseEvents=False,
        ).add_to(m)

    return m


def extract_clicked_district_state(ret: Optional[Mapping[str, Any]]) -> Tuple[Optional[str], Optional[str]]:
    """
    Extract (district_name, state_name) from st_folium return payload.

    The payload can contain different keys depending on st_folium/folium versions.
    We scan common keys and then inspect properties for likely district/state fields.

    Returns:
        (district_name, state_name) if found else (None, None)
    """
    if not ret:
        return None, None

    candidates = (
        "last_object_clicked",
        "clicked_feature",
        "last_active_drawing",
        "last_object",
    )

    for key in candidates:
        feat = ret.get(key)
        if not isinstance(feat, dict):
            continue

        props = feat.get("properties") if isinstance(feat.get("properties"), dict) else feat
        if not isinstance(props, dict):
            continue

        for pk in ("district_name", "shapeName", "NAME", "name", "SHAPE_NAME"):
            val = props.get(pk)
            if val:
                state_val = (
                    props.get("state_name")
                    or props.get("state")
                    or props.get("STATE")
                    or props.get("shapeGroup")
                    or props.get("shapeName_0")
                )
                return str(val), (str(state_val) if state_val else None)

    return None, None


def extract_clicked_block_district_state(
    ret: Optional[Mapping[str, Any]],
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Extract (block_name, district_name, state_name) from st_folium return payload.

    Notes:
        st_folium payloads vary by version. We scan common keys and then inspect
        properties for likely block/district/state fields.

    Returns:
        (block_name, district_name, state_name) if found else (None, None, None)
    """
    if not ret:
        return None, None, None

    candidates = (
        "last_object_clicked",
        "clicked_feature",
        "last_active_drawing",
        "last_object",
    )

    for key in candidates:
        feat = ret.get(key)
        if not isinstance(feat, dict):
            continue

        props = feat.get("properties") if isinstance(feat.get("properties"), dict) else feat
        if not isinstance(props, dict):
            continue

        # Block name candidates (ADM3)
        block_val: Optional[str] = None
        for pk in (
            "block_name",
            "subdistrict_name",
            "adm3_name",
            "NAME_3",
            "name_3",
            "NAME3",
            "shapeName_3",
        ):
            v = props.get(pk)
            if v:
                block_val = str(v).strip()
                break

        if block_val:
            district_val = props.get("district_name") or props.get("district") or props.get("shapeName_1") or props.get("shapeName_2")
            state_val = props.get("state_name") or props.get("state") or props.get("shapeGroup") or props.get("shapeName_0")
            return block_val, (str(district_val).strip() if district_val else None), (str(state_val).strip() if state_val else None)

    return None, None, None


def extract_click_coordinates(ret: Optional[Mapping[str, Any]]) -> Tuple[Optional[float], Optional[float]]:
    """
    Extract (lat, lon) from st_folium return payload.
    
    Returns:
        (lat, lon) if found else (None, None)
    """
    if not ret:
        return None, None
    
    last_click = ret.get("last_object_clicked") or ret.get("last_clicked")
    if isinstance(last_click, dict):
        lat = last_click.get("lat")
        lng = last_click.get("lng") or last_click.get("lon")
        if lat is not None and lng is not None:
            try:
                return float(lat), float(lng)
            except (TypeError, ValueError):
                pass
    
    return None, None


def find_district_at_coordinates(
    merged: Any,  # GeoDataFrame
    lat: float,
    lon: float,
) -> Tuple[Optional[str], Optional[str]]:
    """
    Find district containing or nearest to given coordinates.
    
    Args:
        merged: GeoDataFrame with district geometries and district_name/state_name columns
        lat: Latitude
        lon: Longitude
    
    Returns:
        (district_name, state_name) if found else (None, None)
    """
    from shapely.geometry import Point
    
    try:
        pt = Point(float(lon), float(lat))
        
        # Try exact containment first
        mask = merged.geometry.contains(pt)
        if mask.any():
            row = merged[mask].iloc[0]
        else:
            # Fall back to nearest centroid
            dists = merged.geometry.centroid.distance(pt)
            row = merged.loc[dists.idxmin()]
        
        district = str(row.get("district_name", "")).strip()
        state = str(row.get("state_name", "")).strip()
        
        if district and state:
            return district, state
    except Exception:
        pass
    
    return None, None


def find_block_at_coordinates(
    merged: Any,  # GeoDataFrame
    lat: float,
    lon: float,
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Find block (ADM3) containing or nearest to given coordinates.

    Args:
        merged: GeoDataFrame with block geometries and block_name/district_name/state_name columns
        lat: Latitude
        lon: Longitude

    Returns:
        (block_name, district_name, state_name) if found else (None, None, None)
    """
    from shapely.geometry import Point

    try:
        pt = Point(float(lon), float(lat))

        # Try exact containment first
        mask = merged.geometry.contains(pt)
        if mask.any():
            row = merged[mask].iloc[0]
        else:
            # Fall back to nearest centroid
            dists = merged.geometry.centroid.distance(pt)
            row = merged.loc[dists.idxmin()]

        block = str(row.get("block_name", "")).strip()
        district = str(row.get("district_name", "")).strip()
        state = str(row.get("state_name", "")).strip()

        if block and district and state:
            return block, district, state
    except Exception:
        pass

    return None, None, None


def add_portfolio_legend_to_map(
    m: Any,
    portfolio_count: int,
    *,
    level: str = "district",
    portfolio_border_color: str = "#2563eb",
) -> None:
    """
    Add a legend item indicating portfolio units.
    """
    import folium

    if portfolio_count == 0:
        return

    unit_label = "block" if str(level).strip().lower() == "block" else "district"

    legend_html = f"""
    <div style="
        position: fixed;
        bottom: 50px;
        left: 10px;
        z-index: 1000;
        background: white;
        padding: 8px 12px;
        border-radius: 4px;
        box-shadow: 0 1px 4px rgba(0,0,0,0.3);
        font-size: 12px;
    ">
        <div style="display: flex; align-items: center; gap: 8px;">
            <div style="
                width: 20px;
                height: 14px;
                border: 3px solid {portfolio_border_color};
                background: #f0f0f0;
            "></div>
            <span>In portfolio ({portfolio_count} {unit_label}{'s' if portfolio_count != 1 else ''})</span>
        </div>
    </div>
    """
    m.get_root().html.add_child(folium.Element(legend_html))


def render_map_view(
    *,
    m: Any,
    variable_slug: str,
    map_mode: str,
    sel_scenario: str,
    sel_period: str,
    sel_stat: str,
    selected_state: str,
    selected_district: str,
    map_width: int,
    map_height: int,
    legend_block_html: Optional[str] = None,
    selected_block: str = "All",
    selected_basin: str = "All",
    selected_subbasin: str = "All",
    level: str = "district",
    perf_section: Optional[Callable[[str], Any]] = None,
) -> Tuple[Mapping[str, Any], Optional[str], Optional[str]]:
    """
    Render the folium map inside Streamlit using st_folium, and extract click info.

    This function preserves the legacy dashboard behavior:
      - Uses a deterministic st_folium key tied to variable/scenario/period/stat and selection
      - Adds portfolio-mode point markers from session_state if present
      - Adds portfolio legend in portfolio mode
      - Returns the st_folium payload and extracted clicked (district, state)

    Args:
        m: Pre-built folium.Map (including GeoJson layer, styles, etc.)
        variable_slug: current index slug
        map_mode: "Absolute value" / "Change from baseline"
        sel_scenario: selected scenario key
        sel_period: selected period string
        sel_stat: selected stat key
        selected_state: currently selected state
        selected_district: currently selected district
        map_width: width passed to st_folium
        map_height: height passed to st_folium
        perf_section: optional perf context manager factory

    Returns:
        returned: st_folium return dict (empty dict if None)
        clicked_district: extracted district name (if any)
        clicked_state: extracted state name (if any)
    """
    import streamlit as st
    import folium
    from streamlit_folium import st_folium

    analysis_mode = st.session_state.get("analysis_mode", "Single district focus")

    # In Multi-district/block portfolio mode, draw markers and legend
    if "Multi" in analysis_mode:
        # Saved point markers (blue)
        points = st.session_state.get("point_query_points", [])
        if isinstance(points, list):
            for idx, pt in enumerate(points, start=1):
                if not isinstance(pt, dict):
                    continue
                try:
                    lat_p = float(pt.get("lat"))
                    lon_p = float(pt.get("lon"))
                except (TypeError, ValueError):
                    continue
                
                label = pt.get("label") or f"Point {idx}"
                district = pt.get("district", "")
                tooltip_text = f"{label}: {district}" if district else f"{label}: {lat_p:.4f}, {lon_p:.4f}"

                folium.Marker(
                    location=[lat_p, lon_p],
                    tooltip=tooltip_text,
                    icon=folium.Icon(color="blue", icon="info-sign"),
                ).add_to(m)

        # Active point marker (legacy)
        point_query = st.session_state.get("point_query_latlon")
        if isinstance(point_query, dict):
            try:
                lat_a = float(point_query.get("lat"))
                lon_a = float(point_query.get("lon"))
                folium.Marker(
                    location=[lat_a, lon_a],
                    tooltip=f"Active: {lat_a:.4f}, {lon_a:.4f}",
                    icon=folium.Icon(color="red", icon="star"),
                ).add_to(m)
            except (TypeError, ValueError):
                pass
        
        # Single preview marker (red star) - from "Show on map" button
        preview_marker = st.session_state.get("map_preview_marker")
        if isinstance(preview_marker, dict):
            try:
                lat_m = float(preview_marker.get("lat"))
                lon_m = float(preview_marker.get("lon"))
                district = preview_marker.get("district", "")
                state = preview_marker.get("state", "")
                tooltip_text = f"Location: {district}, {state}" if district else f"Location: {lat_m:.4f}, {lon_m:.4f}"
                
                folium.Marker(
                    location=[lat_m, lon_m],
                    tooltip=tooltip_text,
                    popup=f"<b>{district}</b><br>{state}<br>({lat_m:.4f}, {lon_m:.4f})",
                    icon=folium.Icon(color="red", icon="star"),
                ).add_to(m)
            except (TypeError, ValueError):
                pass
        
        # Multiple preview markers (green) - from batch "Show all on map" button
        preview_markers = st.session_state.get("map_preview_markers")
        if isinstance(preview_markers, list):
            for idx, marker in enumerate(preview_markers, start=1):
                if not isinstance(marker, dict):
                    continue
                try:
                    lat_m = float(marker.get("lat"))
                    lon_m = float(marker.get("lon"))
                except (TypeError, ValueError):
                    continue
                
                label = marker.get("label") or f"#{idx}"
                district = marker.get("district", "")
                state = marker.get("state", "")
                tooltip_text = f"Location: {label}: {district}" if district else f"Location: {label}: {lat_m:.4f}, {lon_m:.4f}"
                
                folium.Marker(
                    location=[lat_m, lon_m],
                    tooltip=tooltip_text,
                    popup=f"<b>{label}</b><br>{district}, {state}<br>({lat_m:.4f}, {lon_m:.4f})",
                    icon=folium.Icon(color="green", icon="map-marker"),
                ).add_to(m)
        
        # Portfolio legend
        portfolio_state_key = "portfolio_blocks" if str(level).strip().lower() == "block" else "portfolio_districts"
        portfolio = st.session_state.get(portfolio_state_key, [])
        add_portfolio_legend_to_map(m, len(portfolio) if isinstance(portfolio, list) else 0, level=level)

    ctx = perf_section("map: render st_folium") if perf_section is not None else nullcontext()

    with ctx:
        overlay_spec = st.session_state.get("crosswalk_overlay") or {}
        overlay_signature = (
            f"{overlay_spec.get('level', 'none')}_"
            f"{overlay_spec.get('selected_name', 'none')}_"
            f"{len(list(overlay_spec.get('feature_keys', []) or []))}"
        )
        map_key = (
            f"map_{variable_slug}_{sel_scenario}_{sel_period}_{sel_stat}_"
            f"{selected_state}_{selected_district}_{selected_block}_"
            f"{selected_basin}_{selected_subbasin}_{str(level).strip().lower()}_{overlay_signature}"
        )
        st.markdown(
            (
                f'<div class="irt-responsive-map-marker" data-map-key="{map_key}" '
                'style="display:none;"></div>'
            ),
            unsafe_allow_html=True,
        )

        if legend_block_html:
            # Give the legend enough width so the colorbar and labels don't get clipped
            # on smaller screens.
            map_col, legend_col = st.columns([17, 4])
            with map_col:
                returned = st_folium(
                    m,
                    width=map_width,
                    height=map_height,
                    returned_objects=["last_object_clicked"],
                    use_container_width=True,
                    key=map_key,
                )
            with legend_col:
                # Render legend as raw HTML (not Markdown) to avoid the legend being
                # displayed as escaped text when Markdown parsing treats parts as code.
                import streamlit.components.v1 as components

                components.html(
                    legend_block_html,
                    height=map_height,
                    scrolling=False,
                )
        else:
            returned = st_folium(
                m,
                width=map_width,
                height=map_height,
                returned_objects=["last_object_clicked"],
                use_container_width=False,
                key=map_key,
            )
        _render_responsive_map_resizer(
            map_key=map_key,
            default_height=map_height,
        )

    if not isinstance(returned, dict):
        returned = {}

    # Default (district) click extraction
    clicked_district, clicked_state = extract_clicked_district_state(returned)

    # Block-aware click extraction (stores clicked_block in session_state)
    clicked_block: Optional[str] = None
    if str(level).strip().lower() == "block":
        b, d, s = extract_clicked_block_district_state(returned)
        clicked_block = b
        clicked_district = d or clicked_district
        clicked_state = s or clicked_state
        st.session_state["clicked_block"] = clicked_block
    else:
        # Ensure stale value isn't carried across toggles
        if "clicked_block" in st.session_state:
            st.session_state.pop("clicked_block")

    return returned, clicked_district, clicked_state


def render_unit_add_to_portfolio(
    *,
    clicked_district: Optional[str],
    clicked_state: Optional[str],
    clicked_block: Optional[str] = None,
    selected_state: str,
    portfolio_add_fn: Callable[..., None],
    portfolio_remove_fn: Callable[..., None],
    portfolio_contains_fn: Callable[..., bool],
    normalize_fn: Callable[[str], str],
    # Optional parameters for coordinate-based lookup
    returned: Optional[Mapping[str, Any]] = None,
    merged: Optional[Any] = None,  # GeoDataFrame
    level: str = "district",
) -> bool:
    """
    Render inline add/remove button for a clicked unit in portfolio mode.

    District mode:
        Uses (state, district)

    Block mode:
        Uses (state, district, block)
    """
    import streamlit as st

    level_norm = str(level).strip().lower()

    # Resolve click coordinates if needed
    resolved_district = clicked_district
    resolved_state = clicked_state
    resolved_block = clicked_block

    if level_norm == "block":
        if not resolved_block and returned is not None:
            b, d, s = extract_clicked_block_district_state(returned)
            resolved_block = b or resolved_block
            resolved_district = d or resolved_district
            resolved_state = s or resolved_state

        if (
            (merged is not None)
            and (returned is not None)
            and (
                (not resolved_block)
                or (not resolved_district)
                or (not resolved_state)
                or (str(resolved_state).strip() == "All")
            )
        ):
            lat, lon = extract_click_coordinates(returned)
            if lat is not None and lon is not None:
                b2, d2, s2 = find_block_at_coordinates(merged, lat, lon)
                if not resolved_block and b2:
                    resolved_block = b2
                if not resolved_district and d2:
                    resolved_district = d2
                if (not resolved_state or str(resolved_state).strip() == "All") and s2:
                    if (not resolved_district) or (d2 and normalize_fn(d2) == normalize_fn(resolved_district)):
                        resolved_state = s2
    else:
        if (
            (merged is not None)
            and (returned is not None)
            and (
                (not resolved_district)
                or (not resolved_state)
                or (str(resolved_state).strip() == "All")
            )
        ):
            lat, lon = extract_click_coordinates(returned)
            if lat is not None and lon is not None:
                d2, s2 = find_district_at_coordinates(merged, lat, lon)
                if not resolved_district and d2:
                    resolved_district = d2
                if (not resolved_state or str(resolved_state).strip() == "All") and s2:
                    if (not resolved_district) or (d2 and normalize_fn(d2) == normalize_fn(resolved_district)):
                        resolved_state = s2

    if level_norm == "block":
        if not resolved_block or not resolved_district:
            return False
    else:
        if not resolved_district:
            return False

    analysis_mode = st.session_state.get("analysis_mode", "Single district focus")
    if "Multi" not in analysis_mode:
        return False

    state_for_add = (resolved_state or selected_state or "").strip()
    if not state_for_add or state_for_add == "All":
        return False

    unit_label = "block" if level_norm == "block" else "district"
    name_for_display = resolved_block if level_norm == "block" else resolved_district

    # Portfolio membership check
    if level_norm == "block":
        is_in_portfolio = bool(portfolio_contains_fn(state_for_add, resolved_district, resolved_block))
        key_suffix = f"{normalize_fn(state_for_add)}_{normalize_fn(resolved_district)}_{normalize_fn(resolved_block)}"
    else:
        is_in_portfolio = bool(portfolio_contains_fn(state_for_add, resolved_district))
        key_suffix = f"{normalize_fn(state_for_add)}_{normalize_fn(resolved_district)}"

    st.markdown(f"**{name_for_display}** ({state_for_add})")

    col1, col2 = st.columns([2, 1])
    with col1:
        if is_in_portfolio:
            if st.button(
                f"Remove {unit_label} from portfolio",
                key=f"map_remove_{key_suffix}",
                type="secondary",
                use_container_width=True,
            ):
                if level_norm == "block":
                    portfolio_remove_fn(state_for_add, resolved_district, resolved_block)
                else:
                    portfolio_remove_fn(state_for_add, resolved_district)
                st.success(f"Removed {name_for_display}")
                st.rerun()
                return True
        else:
            if st.button(
                f"+ Add {unit_label} to portfolio",
                key=f"map_add_{key_suffix}",
                type="primary",
                use_container_width=True,
            ):
                if level_norm == "block":
                    portfolio_add_fn(state_for_add, resolved_district, resolved_block)
                else:
                    portfolio_add_fn(state_for_add, resolved_district)
                st.success(f"Added {name_for_display}")
                st.rerun()
                return True

    with col2:
        st.caption("Portfolio mode")

    return False
