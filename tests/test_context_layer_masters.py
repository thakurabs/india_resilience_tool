"""Tests for the wave-2 context layer masters: WorldPop age, LGRIP30 and JRC GSW.

These cover the two things that would be expensive to discover later: the share
arithmetic (which denominator divides what) and the guardrails that stand between a
silently wrong raster and a published master.
"""

from __future__ import annotations

from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
import rasterio
from affine import Affine
from shapely.geometry import box

from tools.geodata import build_gsw_admin_masters as gsw
from tools.geodata import build_lgrip_admin_masters as lgrip
from tools.geodata import build_worldpop_agesex_admin_masters as agesex


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

RES = 0.01
ORIGIN_X = 77.0
ORIGIN_Y = 20.0


def _write_raster(path: Path, data: np.ndarray, *, dtype: str, nodata=None,
                  origin=(ORIGIN_X, ORIGIN_Y), res: float = RES) -> Path:
    transform = Affine(res, 0.0, origin[0], 0.0, -res, origin[1])
    profile = {
        "driver": "GTiff",
        "height": data.shape[0],
        "width": data.shape[1],
        "count": 1,
        "dtype": dtype,
        "crs": "EPSG:4326",
        "transform": transform,
    }
    if nodata is not None:
        profile["nodata"] = nodata
    path.parent.mkdir(parents=True, exist_ok=True)
    with rasterio.open(path, "w", **profile) as dst:
        dst.write(data.astype(dtype), 1)
    return path


def _district_gdf(n_cells: int = 10) -> gpd.GeoDataFrame:
    """One district covering the whole synthetic raster."""
    geom = box(ORIGIN_X, ORIGIN_Y - n_cells * RES, ORIGIN_X + n_cells * RES, ORIGIN_Y)
    return gpd.GeoDataFrame(
        {
            "state_name": ["Teststate"],
            "district_name": ["Testdistrict"],
            "district_key": ["Teststate::Testdistrict"],
            "geometry": [geom],
        },
        crs="EPSG:4326",
    )


def _age_master(pop: float, count_65: float, count_u5: float) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "state": "Teststate",
                "district": "Testdistrict",
                "district_key": "Teststate::Testdistrict",
                agesex.POPULATION_TOTAL_COL: pop,
                agesex.AGE_65PLUS_COUNT_COL: count_65,
                agesex.AGE_UNDER5_COUNT_COL: count_u5,
            }
        ]
    )


# ---------------------------------------------------------------------------
# Age structure: derived raster
# ---------------------------------------------------------------------------

def test_derived_age_raster_sums_its_bands(tmp_path):
    a = _write_raster(tmp_path / "a.tif", np.full((4, 4), 2.0), dtype="float32")
    b = _write_raster(tmp_path / "b.tif", np.full((4, 4), 5.0), dtype="float32")

    info = agesex.build_derived_age_raster(
        [a, b], tmp_path / "sum.tif", overwrite=False, dry_run=False
    )

    assert info["band_count"] == 2
    assert info["national_total"] == pytest.approx(16 * 7.0)
    with rasterio.open(tmp_path / "sum.tif") as src:
        assert np.allclose(src.read(1), 7.0)


def test_derived_age_raster_refuses_a_band_on_a_different_grid(tmp_path):
    """Cell-by-cell summing is only valid on one grid; anything else must not warp."""
    a = _write_raster(tmp_path / "a.tif", np.ones((4, 4)), dtype="float32")
    b = _write_raster(tmp_path / "b.tif", np.ones((4, 4)), dtype="float32", res=RES * 2)

    with pytest.raises(ValueError, match="transform|size"):
        agesex.build_derived_age_raster(
            [a, b], tmp_path / "sum.tif", overwrite=False, dry_run=False
        )


def test_derived_age_raster_treats_nodata_as_absent_not_zero(tmp_path):
    data = np.array([[1.0, -99999.0], [2.0, 3.0]], dtype="float32")
    a = _write_raster(tmp_path / "a.tif", data, dtype="float32", nodata=-99999.0)

    info = agesex.build_derived_age_raster(
        [a], tmp_path / "sum.tif", overwrite=False, dry_run=False
    )

    assert info["national_total"] == pytest.approx(6.0)
    assert info["valid_cells"] == 3


# ---------------------------------------------------------------------------
# Age structure: share arithmetic
# ---------------------------------------------------------------------------

def test_age_share_divides_by_unit_population(tmp_path):
    """The denominator is the unit's own population, aggregated the same way."""
    pop = _write_raster(tmp_path / "pop.tif", np.full((10, 10), 10.0), dtype="float32")
    r65 = _write_raster(tmp_path / "r65.tif", np.full((10, 10), 1.0), dtype="float32")
    ru5 = _write_raster(tmp_path / "ru5.tif", np.full((10, 10), 2.0), dtype="float32")

    master, _ = agesex.aggregate_age_structure_to_admin_units(
        _district_gdf(), level="district",
        population_raster=pop, raster_65plus=r65, raster_under5=ru5,
    )

    row = master.iloc[0]
    assert row[agesex.POPULATION_TOTAL_COL] == pytest.approx(1000.0)
    assert row[agesex.AGE_65PLUS_SHARE_COL] == pytest.approx(10.0)
    assert row[agesex.AGE_UNDER5_SHARE_COL] == pytest.approx(20.0)


def test_age_share_is_missing_not_zero_for_an_unpopulated_unit(tmp_path):
    """An empty unit has no age structure; zero would read as 'no elderly here'."""
    pop = _write_raster(tmp_path / "pop.tif", np.zeros((10, 10)), dtype="float32")
    r65 = _write_raster(tmp_path / "r65.tif", np.zeros((10, 10)), dtype="float32")
    ru5 = _write_raster(tmp_path / "ru5.tif", np.zeros((10, 10)), dtype="float32")

    master, _ = agesex.aggregate_age_structure_to_admin_units(
        _district_gdf(), level="district",
        population_raster=pop, raster_65plus=r65, raster_under5=ru5,
    )

    assert pd.isna(master.iloc[0][agesex.AGE_65PLUS_SHARE_COL])
    assert pd.isna(master.iloc[0][agesex.AGE_UNDER5_SHARE_COL])


# ---------------------------------------------------------------------------
# Age structure: guardrails
# ---------------------------------------------------------------------------

def _national_summary(share_65: float, share_u5: float) -> pd.DataFrame:
    return pd.DataFrame(
        [{"national_65plus_share_pct": share_65, "national_under5_share_pct": share_u5}]
    )


def test_age_count_above_population_fails():
    master = _age_master(pop=100.0, count_65=150.0, count_u5=5.0)
    with pytest.raises(ValueError, match="above total population"):
        agesex.assert_age_guardrails(
            district_master_df=master, block_master_df=None,
            national_summary_df=_national_summary(7.0, 8.0),
            allow_count_outlier=False, allow_share_outlier=False,
        )


def test_implausible_national_age_share_fails():
    """The signature of a wrong denominator or a dropped band."""
    master = _age_master(pop=100.0, count_65=7.0, count_u5=8.0)
    with pytest.raises(ValueError, match="national share"):
        agesex.assert_age_guardrails(
            district_master_df=master, block_master_df=None,
            national_summary_df=_national_summary(45.0, 8.0),
            allow_count_outlier=False, allow_share_outlier=False,
        )


def test_plausible_age_structure_passes():
    master = _age_master(pop=100.0, count_65=7.0, count_u5=8.0)
    agesex.assert_age_guardrails(
        district_master_df=master, block_master_df=None,
        national_summary_df=_national_summary(6.82, 7.77),
        allow_count_outlier=False, allow_share_outlier=False,
    )


# ---------------------------------------------------------------------------
# LGRIP: VRT mosaic
# ---------------------------------------------------------------------------

def _lgrip_tile(path: Path, data: np.ndarray, origin) -> Path:
    return _write_raster(path, data, dtype="uint8", origin=origin)


def test_vrt_mosaics_co_gridded_tiles(tmp_path):
    _lgrip_tile(tmp_path / "LGRIP30_2015_A.tif", np.full((4, 4), 2, "uint8"), (77.0, 20.0))
    _lgrip_tile(tmp_path / "LGRIP30_2015_B.tif", np.full((4, 4), 3, "uint8"), (77.04, 20.0))

    info = lgrip.build_tile_vrt(tmp_path, tmp_path / "m.vrt", overwrite=False, dry_run=False)

    assert info["tile_count"] == 2
    assert info["width"] == 8 and info["height"] == 4
    with rasterio.open(tmp_path / "m.vrt") as src:
        row = src.read(1)[0]
    assert list(row) == [2, 2, 2, 2, 3, 3, 3, 3]


def test_vrt_refuses_tiles_at_a_fractional_cell_offset(tmp_path):
    """Mosaicking these would require resampling a categorical raster."""
    _lgrip_tile(tmp_path / "LGRIP30_2015_A.tif", np.full((4, 4), 2, "uint8"), (77.0, 20.0))
    _lgrip_tile(tmp_path / "LGRIP30_2015_B.tif", np.full((4, 4), 3, "uint8"), (77.045, 20.0))

    with pytest.raises(ValueError, match="fractional cell offset"):
        lgrip.build_tile_vrt(tmp_path, tmp_path / "m.vrt", overwrite=False, dry_run=False)


def test_vrt_refuses_tiles_of_differing_resolution(tmp_path):
    _write_raster(tmp_path / "LGRIP30_2015_A.tif", np.full((4, 4), 2), dtype="uint8")
    _write_raster(tmp_path / "LGRIP30_2015_B.tif", np.full((4, 4), 3), dtype="uint8",
                  origin=(77.04, 20.0), res=RES * 2)

    with pytest.raises(ValueError, match="resolution"):
        lgrip.build_tile_vrt(tmp_path, tmp_path / "m.vrt", overwrite=False, dry_run=False)


# ---------------------------------------------------------------------------
# LGRIP: share arithmetic
# ---------------------------------------------------------------------------

def test_cropland_share_divides_by_unit_area_not_by_cropland(tmp_path):
    """Share of district area is what makes the stipple comparable between units.

    A quarter-irrigated, quarter-rainfed unit is 25% irrigated and 50% cropland of
    its own area -- not 50% irrigated as a share-of-cropland denominator would give.
    """
    # 100x100 rather than 10x10: the equal-area warp resamples onto its own grid,
    # so a small raster measures its own discretisation instead of the arithmetic.
    data = np.full((100, 100), lgrip.CLASS_NON_CROPLAND, dtype="uint8")
    data[:50, :] = lgrip.CLASS_IRRIGATED
    data[50:80, :] = lgrip.CLASS_RAINFED
    raster = _write_raster(tmp_path / "lgrip.tif", data, dtype="uint8")

    master, _ = lgrip.aggregate_lgrip_to_admin_units(
        _district_gdf(100), level="district", raster_path=raster
    )

    row = master.iloc[0]
    assert row[lgrip.IRRIGATED_SHARE_COL] == pytest.approx(50.0, abs=1.0)
    assert row[lgrip.RAINFED_SHARE_COL] == pytest.approx(30.0, abs=1.0)
    assert row[lgrip.CROPLAND_SHARE_COL] == pytest.approx(80.0, abs=1.0)
    # The share-of-cropland trap: irrigated is 62.5% OF CROPLAND but 50% of area.
    assert row[lgrip.IRRIGATED_SHARE_COL] < 60.0


def test_water_and_non_cropland_classes_are_not_counted_as_cropland(tmp_path):
    data = np.full((100, 100), lgrip.CLASS_WATER, dtype="uint8")
    data[:50, :] = lgrip.CLASS_NON_CROPLAND
    raster = _write_raster(tmp_path / "lgrip.tif", data, dtype="uint8")

    master, _ = lgrip.aggregate_lgrip_to_admin_units(
        _district_gdf(100), level="district", raster_path=raster
    )

    assert master.iloc[0][lgrip.CROPLAND_AREA_COL] == pytest.approx(0.0)


# ---------------------------------------------------------------------------
# LGRIP: coverage guardrail
# ---------------------------------------------------------------------------

def test_tile_coverage_detects_an_uncovered_unit():
    """A VRT returns 0 for uncovered area, and 0 is LGRIP's water class, so only
    the tile geometry can tell a missing tile from a genuinely wet district."""
    gdf = _district_gdf()
    full = [(ORIGIN_X, ORIGIN_Y - 0.1, ORIGIN_X + 0.1, ORIGIN_Y)]
    half = [(ORIGIN_X, ORIGIN_Y - 0.05, ORIGIN_X + 0.1, ORIGIN_Y)]

    assert lgrip.tile_coverage_pct(gdf, full).iloc[0] == pytest.approx(100.0, abs=0.01)
    assert lgrip.tile_coverage_pct(gdf, half).iloc[0] == pytest.approx(50.0, abs=0.5)


def _lgrip_master(coverage: float, irrigated_cells=1, rainfed_cells=1, support_cells=10,
                  cropland_share=20.0) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "state": "Teststate",
                "district": "Testdistrict",
                "district_key": "Teststate::Testdistrict",
                "tile_coverage_pct": coverage,
                "irrigated_cell_count": irrigated_cells,
                "rainfed_cell_count": rainfed_cells,
                "raster_extent_support_cell_count": support_cells,
                "cropland_exceeds_support": (irrigated_cells + rainfed_cells) > support_cells,
                lgrip.CROPLAND_SHARE_COL: cropland_share,
                lgrip.CROPLAND_AREA_COL: 10.0,
                "polygon_area_km2": 50.0,
            }
        ]
    )


def _lgrip_summary(cropland_km2: float = 1_500_000.0) -> pd.DataFrame:
    return pd.DataFrame([{"district_cropland_km2": cropland_km2}])


def test_incomplete_tile_coverage_fails_the_build():
    with pytest.raises(ValueError, match="tile coverage is incomplete"):
        lgrip.assert_lgrip_guardrails(
            district_master_df=_lgrip_master(coverage=61.0), block_master_df=None,
            national_summary_df=_lgrip_summary(), class_histogram={},
            allow_incomplete_coverage=False, allow_unexpected_values=False,
            allow_total_outlier=False, allow_share_outlier=False,
        )


def test_cropland_exceeding_raster_support_always_fails():
    """The one invariant with no override: cropland cells are a subset of support."""
    master = _lgrip_master(coverage=100.0, irrigated_cells=8, rainfed_cells=5, support_cells=10)
    with pytest.raises(ValueError, match="exceeds the raster support"):
        lgrip.assert_lgrip_guardrails(
            district_master_df=master, block_master_df=None,
            national_summary_df=_lgrip_summary(), class_histogram={},
            allow_incomplete_coverage=True, allow_unexpected_values=True,
            allow_total_outlier=True, allow_share_outlier=True,
        )


def test_quantisation_overshoot_just_above_100_pct_is_tolerated():
    """A fully-cropped small unit measures marginally over 100% by cell counting.

    Regression guard: a 100.01% block once failed the build, which is noise, not a
    fault. The subset invariant is what catches real errors.
    """
    master = _lgrip_master(coverage=100.0, irrigated_cells=10, rainfed_cells=0,
                           support_cells=10, cropland_share=100.01)
    lgrip.assert_lgrip_guardrails(
        district_master_df=master, block_master_df=None,
        national_summary_df=_lgrip_summary(), class_histogram={},
        allow_incomplete_coverage=False, allow_unexpected_values=False,
        allow_total_outlier=False, allow_share_outlier=False,
    )


def test_unexpected_class_value_fails():
    with pytest.raises(ValueError, match="outside"):
        lgrip.assert_lgrip_guardrails(
            district_master_df=_lgrip_master(coverage=100.0), block_master_df=None,
            national_summary_df=_lgrip_summary(), class_histogram={0: 5, 9: 3},
            allow_incomplete_coverage=False, allow_unexpected_values=False,
            allow_total_outlier=False, allow_share_outlier=False,
        )


def test_implausible_national_cropland_total_fails():
    with pytest.raises(ValueError, match="cropland total"):
        lgrip.assert_lgrip_guardrails(
            district_master_df=_lgrip_master(coverage=100.0), block_master_df=None,
            national_summary_df=_lgrip_summary(cropland_km2=50_000.0), class_histogram={},
            allow_incomplete_coverage=False, allow_unexpected_values=False,
            allow_total_outlier=False, allow_share_outlier=False,
        )


# ---------------------------------------------------------------------------
# JRC Global Surface Water: sea mask
# ---------------------------------------------------------------------------

def _states_file(path: Path, geom) -> Path:
    gpd.GeoDataFrame({"state_name": ["Teststate"], "geometry": [geom]}, crs="EPSG:4326").to_file(
        path, driver="GeoJSON"
    )
    return path


def _coast_scene(tmp_path: Path, *, cells: int = 60):
    """An ocean, a landmass, and a lagoon joined to the ocean by a one-cell mouth.

    This is the shape the sea mask exists to handle. Vembanad and Chilika are
    physically connected to the sea, so pure connectivity removes them along with
    the ocean; only an opening wide enough to sever the mouth keeps them.
    """
    data = np.zeros((cells, cells), dtype="uint8")
    half = cells // 2
    data[:, :half] = 100                      # ocean, west
    data[20:40, half + 5:cells - 5] = 100     # lagoon, inland
    data[29:30, half:half + 5] = 100          # one-cell mouth joining the two
    raster = _write_raster(tmp_path / "occurrence.tif", data, dtype="uint8")

    land = box(ORIGIN_X + half * RES, ORIGIN_Y - cells * RES, ORIGIN_X + cells * RES, ORIGIN_Y)
    states = _states_file(tmp_path / "states.geojson", land)
    return raster, states, half


def test_sea_fill_without_an_opening_swallows_a_connected_lagoon(tmp_path):
    raster, states, half = _coast_scene(tmp_path)
    sea, _transform, stats = gsw.build_sea_mask(
        raster, states, decimation=1, opening_cells=0
    )
    assert sea[:, :half].all()                       # the ocean is removed
    assert sea[25, half + 10]                        # and so is the lagoon
    assert stats["opening_cells"] == 0


def test_one_cell_opening_severs_the_mouth_and_keeps_the_lagoon(tmp_path):
    """The measured reason for the default: at opening 0 Alappuzha loses 2.4
    percentage points of water and Puri 3.2, all of it genuine inland lagoon."""
    raster, states, half = _coast_scene(tmp_path)
    sea, _transform, stats = gsw.build_sea_mask(
        raster, states, decimation=1, opening_cells=1
    )
    assert sea[:, :half].any()                       # the ocean is still removed
    assert not sea[25, half + 10]                    # the lagoon is not
    assert stats["seed_component_count"] >= 1


def test_sea_mask_refuses_a_scene_with_no_water(tmp_path):
    raster = _write_raster(
        tmp_path / "occurrence.tif", np.zeros((20, 20), dtype="uint8"), dtype="uint8"
    )
    states = _states_file(tmp_path / "states.geojson", box(ORIGIN_X, ORIGIN_Y - 0.2,
                                                           ORIGIN_X + 0.2, ORIGIN_Y))
    with pytest.raises(ValueError, match="No permanent water"):
        gsw.build_sea_mask(raster, states, decimation=1, opening_cells=0)


def test_sea_mask_refuses_a_scene_whose_water_is_all_inland(tmp_path):
    """No seed means no correction, which must be an error rather than a silent
    pass: it is what a CRS mismatch between raster and boundaries looks like."""
    data = np.zeros((40, 40), dtype="uint8")
    data[10:20, 10:20] = 100
    raster = _write_raster(tmp_path / "occurrence.tif", data, dtype="uint8")
    land = box(ORIGIN_X, ORIGIN_Y - 40 * RES, ORIGIN_X + 40 * RES, ORIGIN_Y)
    states = _states_file(tmp_path / "states.geojson", land)
    with pytest.raises(ValueError, match="no seed"):
        gsw.build_sea_mask(raster, states, decimation=1, opening_cells=0)


# ---------------------------------------------------------------------------
# JRC Global Surface Water: zonal arithmetic
# ---------------------------------------------------------------------------

def _sea_raster(tmp_path: Path, shape, *, marine_cols: int = 0) -> Path:
    mask = np.zeros(shape, dtype="uint8")
    if marine_cols:
        mask[:, :marine_cols] = 1
    return _write_raster(tmp_path / "sea.tif", mask, dtype="uint8")


def test_occurrence_splits_into_permanent_and_seasonal_by_threshold(tmp_path):
    data = np.zeros((100, 100), dtype="uint8")
    data[:40, :] = 90      # permanent
    data[40:70, :] = 50    # seasonal
    data[70:90, :] = 10    # ephemeral, counted as neither
    raster = _write_raster(tmp_path / "occurrence.tif", data, dtype="uint8")

    master, _ = gsw.aggregate_surface_water_to_admin_units(
        _district_gdf(100), level="district", raster_path=raster,
        sea_mask_path=_sea_raster(tmp_path, data.shape),
    )

    row = master.iloc[0]
    assert row[gsw.PERMANENT_SHARE_COL] == pytest.approx(40.0, abs=1.0)
    assert row[gsw.SEASONAL_SHARE_COL] == pytest.approx(30.0, abs=1.0)


def test_no_observation_is_neither_water_nor_dry(tmp_path):
    """255 means the pixel was never validly observed. Folding it into either
    class invents a measurement; it is counted separately and reported."""
    data = np.full((100, 100), gsw.NODATA_VALUE, dtype="uint8")
    data[:20, :] = 90
    raster = _write_raster(tmp_path / "occurrence.tif", data, dtype="uint8")

    master, _ = gsw.aggregate_surface_water_to_admin_units(
        _district_gdf(100), level="district", raster_path=raster,
        sea_mask_path=_sea_raster(tmp_path, data.shape),
    )

    row = master.iloc[0]
    assert row[gsw.PERMANENT_SHARE_COL] == pytest.approx(20.0, abs=1.0)
    assert row[gsw.SEASONAL_SHARE_COL] == pytest.approx(0.0, abs=0.5)
    # nodata is the overwhelming majority of the unit and must be visible as such
    assert row["nodata_pct_of_support"] == pytest.approx(80.0, abs=1.0)
    assert bool(row["high_nodata"]) is True


def test_marine_cells_are_removed_from_water_and_recorded(tmp_path):
    """The Nicobars case: uncorrected, the sea is published as district water."""
    data = np.full((100, 100), 100, dtype="uint8")
    raster = _write_raster(tmp_path / "occurrence.tif", data, dtype="uint8")

    master, _ = gsw.aggregate_surface_water_to_admin_units(
        _district_gdf(100), level="district", raster_path=raster,
        sea_mask_path=_sea_raster(tmp_path, data.shape, marine_cols=60),
    )

    row = master.iloc[0]
    assert row[gsw.PERMANENT_SHARE_COL] == pytest.approx(40.0, abs=1.5)
    assert row["marine_removed_pct_of_polygon"] == pytest.approx(60.0, abs=1.5)
    assert bool(row["marine_corrected"]) is True


def test_water_share_divides_by_unit_area_not_by_water(tmp_path):
    data = np.zeros((100, 100), dtype="uint8")
    data[:25, :] = 90     # permanent
    data[25:50, :] = 50   # seasonal
    raster = _write_raster(tmp_path / "occurrence.tif", data, dtype="uint8")

    master, _ = gsw.aggregate_surface_water_to_admin_units(
        _district_gdf(100), level="district", raster_path=raster,
        sea_mask_path=_sea_raster(tmp_path, data.shape),
    )

    row = master.iloc[0]
    # permanent is 50% OF WATER but 25% of unit area; the wrong denominator doubles it
    assert row[gsw.PERMANENT_SHARE_COL] == pytest.approx(25.0, abs=1.0)
    assert row[gsw.PERMANENT_SHARE_COL] < 40.0


# ---------------------------------------------------------------------------
# JRC Global Surface Water: guardrails
# ---------------------------------------------------------------------------

def _gsw_master(coverage: float, permanent_cells=1, seasonal_cells=1, support_cells=10,
                permanent_share=5.0, seasonal_share=5.0) -> pd.DataFrame:
    total = permanent_share + seasonal_share
    return pd.DataFrame(
        [
            {
                "state": "Teststate",
                "district": "Testdistrict",
                "district_key": "Teststate::Testdistrict",
                "tile_coverage_pct": coverage,
                "permanent_cell_count": permanent_cells,
                "seasonal_cell_count": seasonal_cells,
                "raster_extent_support_cell_count": support_cells,
                "water_exceeds_support": (permanent_cells + seasonal_cells) > support_cells,
                "share_out_of_range": total > gsw.SHARE_OUTLIER_MAX_PCT,
                gsw.PERMANENT_SHARE_COL: permanent_share,
                gsw.SEASONAL_SHARE_COL: seasonal_share,
                "polygon_area_km2": 50.0,
            }
        ]
    )


def _gsw_summary(permanent_km2: float = 17_000.0) -> pd.DataFrame:
    return pd.DataFrame([{"district_permanent_km2": permanent_km2}])


def test_gsw_incomplete_tile_coverage_fails_the_build():
    """Same trap as LGRIP: an uncovered area reads as occurrence 0, which is a
    legitimate value meaning "never water", so only tile geometry detects it."""
    with pytest.raises(ValueError, match="tile coverage is incomplete"):
        gsw.assert_surface_water_guardrails(
            district_master_df=_gsw_master(coverage=61.0), block_master_df=None,
            national_summary_df=_gsw_summary(), value_histogram={},
            allow_incomplete_coverage=False, allow_unexpected_values=False,
            allow_total_outlier=False, allow_share_outlier=False,
        )


def test_gsw_water_exceeding_raster_support_always_fails():
    with pytest.raises(ValueError, match="exceeds the raster support"):
        gsw.assert_surface_water_guardrails(
            district_master_df=_gsw_master(coverage=100.0, permanent_cells=8,
                                           seasonal_cells=5, support_cells=10),
            block_master_df=None, national_summary_df=_gsw_summary(), value_histogram={},
            allow_incomplete_coverage=True, allow_unexpected_values=True,
            allow_total_outlier=True, allow_share_outlier=True,
        )


def test_gsw_occurrence_outside_0_100_and_255_fails():
    with pytest.raises(ValueError, match="outside"):
        gsw.assert_surface_water_guardrails(
            district_master_df=_gsw_master(coverage=100.0), block_master_df=None,
            national_summary_df=_gsw_summary(), value_histogram={0: 5, 100: 2, 255: 1, 137: 3},
            allow_incomplete_coverage=False, allow_unexpected_values=False,
            allow_total_outlier=False, allow_share_outlier=False,
        )


def test_gsw_no_observation_value_is_not_treated_as_unexpected():
    gsw.assert_surface_water_guardrails(
        district_master_df=_gsw_master(coverage=100.0), block_master_df=None,
        national_summary_df=_gsw_summary(), value_histogram={0: 5, 100: 2, 255: 9},
        allow_incomplete_coverage=False, allow_unexpected_values=False,
        allow_total_outlier=False, allow_share_outlier=False,
    )


def test_a_sea_mask_that_failed_open_trips_the_national_total():
    """Marine water is roughly two thirds of the permanent water in the India box,
    so a correction that stops working shows up as a several-fold national total."""
    with pytest.raises(ValueError, match="permanent water total"):
        gsw.assert_surface_water_guardrails(
            district_master_df=_gsw_master(coverage=100.0), block_master_df=None,
            national_summary_df=_gsw_summary(permanent_km2=250_000.0), value_histogram={},
            allow_incomplete_coverage=False, allow_unexpected_values=False,
            allow_total_outlier=False, allow_share_outlier=False,
        )


def test_sea_mask_at_a_coarser_resolution_lands_within_one_coarse_cell(tmp_path):
    """The mask is built at ~550 m and applied to a ~27 m grid, so the index
    arithmetic that replaced the per-window warp has to register to within one
    coarse cell -- and must not drift consistently to one side, which is what a
    half-cell origin error looks like.

    Here one coarse cell is 5% of the district, so the bound is 5 percentage
    points; at the real 550 m against district-sized polygons it is a fraction of
    one percent. Measured errors across ratios 1, 2, 5 and 10 and four boundary
    positions stay inside half a coarse cell and change sign, so the residual is
    discretisation rather than registration.
    """
    data = np.full((100, 100), 100, dtype="uint8")
    raster = _write_raster(tmp_path / "occurrence.tif", data, dtype="uint8")

    results = []
    for marine_coarse_cols, expected in ((4, 20.0), (8, 40.0), (12, 60.0), (16, 80.0)):
        coarse = np.zeros((20, 20), dtype="uint8")     # one coarse cell = five fine cells
        coarse[:, :marine_coarse_cols] = 1
        sea = _write_raster(tmp_path / f"sea_{marine_coarse_cols}.tif", coarse,
                            dtype="uint8", res=RES * 5)
        master, _ = gsw.aggregate_surface_water_to_admin_units(
            _district_gdf(100), level="district", raster_path=raster, sea_mask_path=sea
        )
        row = master.iloc[0]
        got = float(row["marine_removed_pct_of_polygon"])
        assert got == pytest.approx(expected, abs=5.0)
        # the two classes partition the unit: what is not marine is water
        assert row[gsw.PERMANENT_SHARE_COL] == pytest.approx(100.0 - got, abs=1.0)
        results.append(got - expected)

    # not all on one side, which a half-cell origin error would be
    assert min(results) < 0 < max(results)
