# district_merge_audit.py  — READ ONLY.  Run from repo root:
#   python -m tools.diagnostics.district_merge_audit
#   python -m tools.diagnostics.district_merge_audit "C:\path\to\bharatlas_districts.csv"
import sys, json
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

pd.set_option("display.max_rows", 2000); pd.set_option("display.width", 200)

raw = gpd.read_file(_default_source_shapefile())
g = ensure_epsg4326(_fix_invalid_geometries(raw))

# canonical state for both stages
g["state_name"] = _canonicalize_states(g["state"])[0].astype(str)

# RAW district = canonical state + raw district (whitespace only)
g["raw_district"] = g["district"].astype("string").str.strip()
raw_pairs = set(zip(g["state_name"], g["raw_district"]))

# REPAIRED district = full builder transform
g["district_name"] = _strip_district_suffix(g["district"])
g["block_name"] = g["block_name"].astype("string").str.strip()
g = repair_adm3_identity_columns(g)
invalid = (_invalid_identity_mask(g["state_name"])
            | _invalid_identity_mask(g["district_name"])
            | _invalid_identity_mask(g["block_name"]))
gv = g.loc[~invalid].copy()
rep_pairs = set(zip(gv["state_name"], gv["district_name"]))

print("=" * 78)
print(f"raw distinct (canon-state, raw-district) pairs : {len(raw_pairs)}")
print(f"repaired distinct (state_name, district_name)  : {len(rep_pairs)}")
print("=" * 78)

# ---- THE MERGE: repaired names produced from >1 distinct raw name ----
m = (gv[["state_name", "raw_district", "district_name"]].drop_duplicates()
        .groupby(["state_name", "district_name"])["raw_district"]
        .agg(lambda s: sorted(set(s))))
merges = m[m.map(len) > 1]
print("\nREPAIR MERGES  (distinct raw spellings -> one repaired name):")
if len(merges):
    for (st, di), raws in merges.items():
        print(f"  {st} / {di!r}  <-  {raws}")
else:
    print("  (none)")

# ---- per-state, properly joined on canonical state ----
src = pd.Series({s: len({d for (ss, d) in raw_pairs if ss == s})
                for s in {x[0] for x in raw_pairs}}, name="src_names")
with open(DISTRICTS_PATH, encoding="utf-8") as fh:
    out = json.load(fh)
out_names = {}
out_pairs = set()
for f in out["features"]:
    p = f["properties"]; out_pairs.add((p["state_name"], p["district_name"]))
    out_names[p["state_name"]] = out_names.get(p["state_name"], 0) + 1
outs = pd.Series(out_names, name="out")
tbl = pd.concat([src, outs], axis=1).fillna(0).astype(int)
tbl["delta"] = tbl["src_names"] - tbl["out"]
print("\nPER-STATE (canon-state | raw distinct names | output | delta):")
print(tbl.sort_values("delta", ascending=False))

# ---- optional diff against bharatlas 785 master ----
if len(sys.argv) > 1:
    bl = pd.read_csv(sys.argv[1])
    print("\nbharatlas CSV columns:", list(bl.columns))
    print("Map state/district columns below, then we diff. (rows:", len(bl), ")")