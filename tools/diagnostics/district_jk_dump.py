# jk_dump.py — READ ONLY.  Run: python -m tools.diagnostics.jk_dump
import json
import geopandas as gpd
import pandas as pd

from tools.geodata.build_admin_boundaries_from_lgd import (
    _default_source_shapefile, _canonicalize_states,
    _strip_district_suffix, _invalid_identity_mask, _fix_invalid_geometries,
)
from india_resilience_tool.data.adm3_loader import (
    ensure_epsg4326, repair_adm3_identity_columns,
)
from paths import DISTRICTS_PATH

pd.set_option("display.max_rows", 500); pd.set_option("display.width", 220)
pd.set_option("display.max_colwidth", 40)

raw = gpd.read_file(_default_source_shapefile())
g = ensure_epsg4326(_fix_invalid_geometries(raw))
g["state_name"] = _canonicalize_states(g["state"])[0].astype(str)
g["raw_district"] = g["district"].astype("string").str.strip()
g["raw_block"]    = g["block_name"].astype("string").str.strip()
g["district_name"] = _strip_district_suffix(g["district"])
g["block_name"]    = g["block_name"].astype("string").str.strip()
g["dist_lgd_n"]    = pd.to_numeric(g.get("dist_lgd"), errors="coerce")
g = repair_adm3_identity_columns(g)

g["bad_state"]    = _invalid_identity_mask(g["state_name"])
g["bad_district"] = _invalid_identity_mask(g["district_name"])
g["bad_block"]    = _invalid_identity_mask(g["block_name"])
g["dropped"]      = g["bad_state"] | g["bad_district"] | g["bad_block"]

jk = g[g["state_name"] == "Jammu & Kashmir"]

print("J&K raw blocks:", len(jk), " dropped rows:", int(jk["dropped"].sum()))
print("\nPER RAW DISTRICT (raw_district | dist_lgd | n_blocks | n_dropped | repaired_name):")
summ = (jk.groupby("raw_district")
        .agg(dist_lgd=("dist_lgd_n", lambda s: sorted(set(s.dropna().astype(int)))),
                n_blocks=("raw_block", "size"),
                n_dropped=("dropped", "sum"),
                repaired=("district_name", lambda s: sorted(set(s))[:2]))
        .reset_index())
print(summ.to_string(index=False))

print("\nDROPPED ROWS in J&K (the smoking gun):")
cols = ["raw_district", "raw_block", "block_name", "district_name",
        "bad_state", "bad_district", "bad_block", "dist_lgd_n"]
drp = jk.loc[jk["dropped"], cols]
print(drp.to_string(index=False) if len(drp) else "  (none)")

# output J&K names for the final diff
with open(DISTRICTS_PATH, encoding="utf-8") as fh:
    out = json.load(fh)
out_jk = sorted(f["properties"]["district_name"]
                for f in out["features"]
                if f["properties"]["state_name"] == "Jammu & Kashmir")
raw_jk = sorted(set(jk["district_name"]))
print("\noutput J&K districts (", len(out_jk), "):", out_jk)
print("\nraw J&K repaired names (", len(raw_jk), "):", raw_jk)
print("\nIN RAW BUT NOT OUTPUT:", sorted(set(raw_jk) - set(out_jk)))