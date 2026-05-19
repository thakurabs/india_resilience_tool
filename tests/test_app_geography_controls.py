from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import geopandas as gpd
from shapely.geometry import Polygon

from india_resilience_tool.app.geography_controls import (
    _analysis_mode_options,
    _build_admin_geography,
    _districts_for_selected_state,
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
        data_dir=tmp_path,
        simplify_tol_adm3=0.0001,
        admin_level="block",
    )

    assert selected_state == "Telangana"
    assert selected_district == "Adilabad"
    assert selected_block == "All"


def test_districts_for_selected_state_prefers_exact_state_names_over_geometry_subset() -> None:
    adm1 = gpd.GeoDataFrame(
        {"shapeName": ["Telangana"]},
        geometry=[Polygon([(0, 0), (15, 0), (15, 1), (0, 1)])],
        crs="EPSG:4326",
    )
    adm2 = gpd.GeoDataFrame(
        {
            "state_name": ["Telangana"] * 33,
            "district_name": [f"D{idx:02d}" for idx in range(33)],
        },
        geometry=[
            Polygon([(idx, 0), (idx + 0.8, 0), (idx + 0.8, 0.8), (idx, 0.8)])
            for idx in range(33)
        ],
        crs="EPSG:4326",
    )

    districts = _districts_for_selected_state(adm2, adm1, "Telangana")

    assert len(districts) == 33
    assert sorted(districts["district_name"]) == [f"D{idx:02d}" for idx in range(33)]


def test_districts_for_selected_state_supplements_unknown_rows_by_representative_point() -> None:
    adm1 = gpd.GeoDataFrame(
        {"shapeName": ["Telangana"]},
        geometry=[Polygon([(0, 0), (4, 0), (4, 4), (0, 4)])],
        crs="EPSG:4326",
    )
    adm2 = gpd.GeoDataFrame(
        {
            "state_name": ["Telangana", "Unknown", "Maharashtra"],
            "district_name": ["Adilabad", "Nalgonda", "Pune"],
        },
        geometry=[
            Polygon([(0.2, 0.2), (1.0, 0.2), (1.0, 1.0), (0.2, 1.0)]),
            Polygon([(2.0, 2.0), (2.8, 2.0), (2.8, 2.8), (2.0, 2.8)]),
            Polygon([(5.0, 5.0), (5.8, 5.0), (5.8, 5.8), (5.0, 5.8)]),
        ],
        crs="EPSG:4326",
    )

    districts = _districts_for_selected_state(adm2, adm1, "Telangana")

    assert set(districts["district_name"]) == {"Adilabad", "Nalgonda"}


def test_districts_for_selected_state_preserves_empty_geodataframe_shape() -> None:
    adm1 = gpd.GeoDataFrame(
        {"shapeName": ["Telangana"]},
        geometry=[Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])],
        crs="EPSG:4326",
    )
    adm2 = gpd.GeoDataFrame(
        {"state_name": ["Maharashtra"], "district_name": ["Pune"]},
        geometry=[Polygon([(4, 4), (5, 4), (5, 5), (4, 5)])],
        crs="EPSG:4326",
    )

    districts = _districts_for_selected_state(adm2, adm1, "Telangana")

    assert isinstance(districts, gpd.GeoDataFrame)
    assert districts.empty
    assert districts.crs == adm2.crs
    assert "geometry" in districts.columns


def test_districts_for_selected_state_does_not_substring_match_state_names() -> None:
    adm1 = gpd.GeoDataFrame(
        {"shapeName": ["Pradesh"]},
        geometry=[Polygon([(10, 10), (11, 10), (11, 11), (10, 11)])],
        crs="EPSG:4326",
    )
    adm2 = gpd.GeoDataFrame(
        {
            "state_name": ["Arunachal Pradesh"],
            "district_name": ["Tawang"],
        },
        geometry=[Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])],
        crs="EPSG:4326",
    )

    districts = _districts_for_selected_state(adm2, adm1, "Pradesh")

    assert districts.empty


def test_build_admin_geography_loads_state_shard_when_adm2_is_deferred(
    tmp_path: Path,
    monkeypatch,
) -> None:
    import india_resilience_tool.app.geography_controls as geography_controls

    session_state = {
        "selected_state": "Telangana",
        "selected_district": "Nalgonda",
        "selected_block": "All",
        "selected_basin": "All",
        "selected_subbasin": "All",
        "admin_level": "district",
    }
    fake_st = SimpleNamespace(
        session_state=session_state,
        info=lambda *args, **kwargs: None,
        warning=lambda *args, **kwargs: None,
        caption=lambda *args, **kwargs: None,
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
        "_load_state_adm2_shard_for_sidebar",
        lambda *, selected_state, data_dir: gpd.GeoDataFrame(
            {"state_name": ["Telangana"], "district_name": ["Nalgonda"]},
            geometry=[Polygon([(0, 0), (1, 0), (1, 1), (0, 1)])],
            crs="EPSG:4326",
        ),
    )
    adm1 = gpd.GeoDataFrame(
        {"shapeName": ["Telangana"]},
        geometry=[Polygon([(0, 0), (2, 0), (2, 2), (0, 2)])],
        crs="EPSG:4326",
    )

    selected_state, selected_district, _, districts = _build_admin_geography(
        analysis_ready=True,
        analysis_mode="Single district focus",
        processed_root=tmp_path / "processed",
        adm1=adm1,
        adm2=None,
        adm3_geojson=tmp_path / "blocks_4326.geojson",
        data_dir=tmp_path,
        simplify_tol_adm3=0.0001,
        admin_level="district",
    )

    assert selected_state == "Telangana"
    assert selected_district == "Nalgonda"
    assert list(districts["district_name"]) == ["Nalgonda"]
