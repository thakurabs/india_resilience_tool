# verify_jk_west.py — READ ONLY.  Run: python -m tools.diagnostics.verify_jk_west
import geopandas as gpd
from tools.geodata.build_admin_boundaries_from_lgd import (
    prepare_admin_boundaries, _default_source_shapefile,
    _canonicalize_states, _fix_invalid_geometries,
)
from india_resilience_tool.data.adm3_loader import ensure_epsg4326

shp = _default_source_shapefile()

# (1) ArcGIS-equivalent "truth": every block, no block_name filter
raw = ensure_epsg4326(_fix_invalid_geometries(gpd.read_file(shp)))
raw["state_name"] = _canonicalize_states(raw["state"])[0].astype(str)
jk_raw = raw.loc[raw["state_name"] == "Jammu & Kashmir"]
try:
    geom_raw = jk_raw.geometry.union_all()
except AttributeError:
    geom_raw = jk_raw.geometry.unary_union
area_raw = gpd.GeoSeries([geom_raw], crs=4326).to_crs(6933).area.iloc[0] / 1e6

# (2) Our post-fix builder
_, _, states, qa = prepare_admin_boundaries(shp)
jk = states.loc[states["state_name"] == "Jammu & Kashmir"]
geom_b = jk.geometry.iloc[0]
area_b = float(jk["area_km2"].iloc[0])

print(f"synthesized_block_names      : {qa['synthesized_block_names']}")
print(f"ALL-blocks  J&K  minx={geom_raw.bounds[0]:.4f}  area_km2={area_raw:,.0f}")
print(f"builder     J&K  minx={geom_b.bounds[0]:.4f}  area_km2={area_b:,.0f}")
print(f"minx diff (deg): {abs(geom_raw.bounds[0]-geom_b.bounds[0]):.6f}  "
    f"area diff (km2): {abs(area_raw-area_b):,.0f}")