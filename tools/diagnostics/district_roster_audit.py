# district_roster_audit.py  — READ ONLY (no writes). Run: python district_roster_audit.py
import json
import geopandas as gpd
import pandas as pd

from tools.geodata.build_admin_boundaries_from_lgd import (
    _default_source_shapefile,
    _canonicalize_states,
    _strip_district_suffix,
    _invalid_identity_mask,
    _fix_invalid_geometries,
)
from india_resilience_tool.data.adm3_loader import (
    ensure_epsg4326,
    repair_adm3_identity_columns,
)
from paths import DISTRICTS_PATH

pd.set_option("display.max_rows", 1000)
pd.set_option("display.width", 200)

shp = _default_source_shapefile()
print(f"source shp : {shp}")
print(f"districts  : {DISTRICTS_PATH}\n")

# ---- 1. RAW ground truth (before any filtering) -------------------------------
raw = gpd.read_file(shp)
raw["_dist_lgd"] = pd.to_numeric(raw.get("dist_lgd"), errors="coerce")
n_raw_rows = len(raw)
n_raw_codes = raw["_dist_lgd"].dropna().nunique()
n_raw_namepairs = raw.dropna(subset=["state", "district"]).groupby(
    [raw["state"].astype(str).str.strip(), raw["district"].astype(str).str.strip()]
).ngroups

# ---- 2. Replicate the builder transform up to (but not into) the dissolve -----
g = _fix_invalid_geometries(raw)                      # drops null/empty geom rows
n_after_geom = len(g)
g = ensure_epsg4326(g)
state_mapped, unmapped = _canonicalize_states(g["state"])
g["state_name"] = state_mapped.astype(str)            # unmapped -> 'nan' (caught below)
g["district_name"] = _strip_district_suffix(g["district"])
g["block_name"] = g["block_name"].astype("string").str.strip()
g = repair_adm3_identity_columns(g)
g["_dist_lgd"] = pd.to_numeric(g.get("dist_lgd"), errors="coerce")

invalid = (
    _invalid_identity_mask(g["state_name"])
    | _invalid_identity_mask(g["district_name"])
    | _invalid_identity_mask(g["block_name"])
)
g_valid = g.loc[~invalid].copy()

# ---- 3. Output file actually on disk -----------------------------------------
with open(DISTRICTS_PATH, "r", encoding="utf-8") as fh:
    out = json.load(fh)
out_feats = out["features"]
n_out = len(out_feats)
out_pairs = {
    (f["properties"].get("state_name"), f["properties"].get("district_name"))
    for f in out_feats
}

# ---- 4. Decomposition ---------------------------------------------------------
codes_raw   = set(raw["_dist_lgd"].dropna().astype(int))
codes_valid = set(g_valid["_dist_lgd"].dropna().astype(int))
namepairs_valid = set(
    zip(g_valid["state_name"].astype(str), g_valid["district_name"].astype(str))
)

codes_dropped_invalid = codes_raw - codes_valid          # mechanism A
collisions = len(codes_valid) - len(namepairs_valid)     # mechanism B (count)

print("=" * 78)
print("TOTALS")
print("=" * 78)
print(f"raw block rows                         : {n_raw_rows}")
print(f"  rows w/ null/empty geom dropped      : {n_raw_rows - n_after_geom}")
print(f"raw distinct dist_lgd (ground truth)   : {n_raw_codes}")
print(f"raw distinct (state,district) names    : {n_raw_namepairs}")
print(f"valid distinct dist_lgd after filter   : {len(codes_valid)}")
print(f"valid distinct (state,district) names  : {len(namepairs_valid)}")
print(f"districts in output geojson on disk     : {n_out}")
print()
print(f"LOSS A  dist_lgd codes dropped by invalid-identity filter : {len(codes_dropped_invalid)}")
print(f"LOSS B  distinct districts merged by name collision        : {collisions}")
print(f"        (expected output = {len(namepairs_valid)}; on disk = {n_out})")

# ---- 5. Per-state table ------------------------------------------------------
src = (
    raw.dropna(subset=["_dist_lgd"])
    .assign(_st=lambda d: d["state"].astype(str).str.strip())
    .groupby("_st")["_dist_lgd"].nunique()
    .rename("src_dist_lgd")
)
outdf = pd.Series(
    pd.value_counts([f["properties"].get("state_name") for f in out_feats]),
    name="out_districts",
)
tbl = pd.concat([src, outdf], axis=1).fillna(0).astype(int)
tbl["delta"] = tbl["src_dist_lgd"] - tbl["out_districts"]
print("\n" + "=" * 78)
print("PER-STATE  (src_dist_lgd uses raw state spelling; out uses canonical)")
print("=" * 78)
print(tbl.sort_values("delta", ascending=False))
print("\nStates with delta != 0 are where districts were lost or renamed across the\n"
    "state-name canonicalization. Rows that don't line up by name are usually just\n"
    "the raw-vs-canonical state spelling; focus on the detail lists below.\n")

# ---- 6. Detail: codes dropped by invalid-identity filter ---------------------
print("=" * 78)
print("DETAIL A — dist_lgd codes present in source but DROPPED (invalid identity)")
print("=" * 78)
if codes_dropped_invalid:
    d = raw[raw["_dist_lgd"].isin(codes_dropped_invalid)]
    rep = (
        d.assign(
            state=d["state"].astype(str).str.strip(),
            district=d["district"].astype(str).str.strip(),
            block=d["block_name"].astype(str).str.strip(),
        )
        .groupby("_dist_lgd")
        .agg(state=("state", "first"),
            district=("district", "first"),
            n_blocks=("district", "size"),
            distinct_districts=("district", lambda s: sorted(set(s))[:3]))
        .reset_index()
    )
    print(rep.to_string(index=False))
else:
    print("(none)")

# ---- 7. Detail: name collisions ---------------------------------------------
print("\n" + "=" * 78)
print("DETAIL B — distinct dist_lgd codes that COLLIDE onto one (state,district) name")
print("=" * 78)
coll = (
    g_valid.dropna(subset=["_dist_lgd"])
    .groupby(["state_name", "district_name"])["_dist_lgd"]
    .nunique()
)
coll = coll[coll > 1]
if len(coll):
    for (st, di), n in coll.items():
        codes = sorted(set(
            g_valid[(g_valid["state_name"] == st) & (g_valid["district_name"] == di)]["_dist_lgd"]
            .dropna().astype(int)
        ))
        print(f"  {st} / {di!r}: {n} codes -> {codes}")
else:
    print("(none)")

# ---- 8. Unmapped state names (would become 'nan' and be dropped) -------------
print("\n" + "=" * 78)
print("DETAIL C — source state spellings with NO canonical mapping (all rows dropped)")
print("=" * 78)
print(unmapped if unmapped else "(none)")