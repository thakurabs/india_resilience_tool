from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import geopandas as gpd
from shapely.geometry import Polygon

from india_resilience_tool.app.geography_controls import (
    _analysis_mode_options,
    _build_admin_geography,
    _resolve_available_admin_states,
)


def test_resolve_available_admin_states_preserves_all_for_flat_admin_masters(tmp_path: Path) -> None:
    (tmp_path / "Telangana").mkdir(parents=True)
    (tmp_path / "Telangana" / "master_metrics_by_district.csv").write_text("state,district\n", encoding="utf-8")
    (tmp_path / "Odisha").mkdir(parents=True)
    (tmp_path / "Odisha" / "master_metrics_by_block.csv").write_text("state,district,block\n", encoding="utf-8")

    available_states, has_available_data = _resolve_available_admin_states(tmp_path)

    assert has_available_data is True
    assert available_states == ["All", "Odisha", "Telangana"]


def test_resolve_available_admin_states_returns_all_when_missing(tmp_path: Path) -> None:
    available_states, has_available_data = _resolve_available_admin_states(tmp_path / "missing")

    assert has_available_data is False
    assert available_states == ["All"]


def test_analysis_mode_options_include_hydro_portfolio_modes() -> None:
    assert _analysis_mode_options("hydro", "basin") == [
        "Single basin focus",
        "Multi-basin portfolio",
    ]
    assert _analysis_mode_options("hydro", "sub_basin") == [
        "Single sub-basin focus",
        "Multi-sub-basin portfolio",
    ]


def test_build_admin_geography_uses_optimized_block_index_before_loading_adm3(
    tmp_path: Path,
    monkeypatch,
) -> None:
    import india_resilience_tool.app.geo_cache as geo_cache
    import india_resilience_tool.app.geography_controls as geography_controls

    context_dir = tmp_path / "processed_optimised" / "context"
    context_dir.mkdir(parents=True)
    (context_dir / "admin_block_index.parquet").write_bytes(b"stub")

    session_state = {
        "selected_state": "Telangana",
        "selected_district": "Adilabad",
        "selected_block": "All",
        "selected_basin": "All",
        "selected_subbasin": "All",
        "admin_level": "block",
    }

    fake_st = SimpleNamespace(
        session_state=session_state,
        info=lambda *args, **kwargs: None,
        warning=lambda *args, **kwargs: None,
        error=lambda *args, **kwargs: None,
        caption=lambda *args, **kwargs: None,
        stop=lambda: (_ for _ in ()).throw(AssertionError("st.stop should not be called")),
        selectbox=lambda label, options, index=0, key=None, disabled=False: session_state.get(key, options[index]),
    )
    monkeypatch.setattr(geography_controls, "st", fake_st)
    monkeypatch.setattr(
        geography_controls,
        "_resolve_available_admin_states",
        lambda processed_root: (["All", "Telangana"], True),
    )
    monkeypatch.setattr(
        geography_controls,
        "load_admin_block_selector_index",
        lambda path: {"blocks_by_selector": {"telangana|adilabad": ["Bela", "Gudihathnoor"]}},
    )
    monkeypatch.setattr(
        geo_cache,
        "load_local_adm3",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("national ADM3 should not be loaded")),
    )

    adm1 = gpd.GeoDataFrame(
        {"shapeName": ["telangana"]},
        geometry=[Polygon([(0, 0), (2, 0), (2, 2), (0, 2)])],
        crs="EPSG:4326",
    )
    adm2 = gpd.GeoDataFrame(
        {
            "state_name": ["Telangana"],
            "district_name": ["Adilabad"],
        },
        geometry=[Polygon([(0.1, 0.1), (1.9, 0.1), (1.9, 1.9), (0.1, 1.9)])],
        crs="EPSG:4326",
    )

    selected_state, selected_district, selected_block, _ = _build_admin_geography(
        analysis_ready=True,
        analysis_mode="Single block focus",
        processed_root=tmp_path / "processed",
        adm1=adm1,
        adm2=adm2,
        adm3_geojson=tmp_path / "blocks_4326.geojson",
        simplify_tol_adm3=0.0001,
        admin_level="block",
    )

    assert selected_state == "Telangana"
    assert selected_district == "Adilabad"
    assert selected_block == "All"
