# import geopandas as gpd
# from pathlib import Path
# from india_resilience_tool.config.paths import get_paths_config
# from india_resilience_tool.data.adm3_loader import ensure_adm3_columns
# from india_resilience_tool.utils.naming import alias

# cfg = get_paths_config()
# g = ensure_adm3_columns(gpd.read_file(cfg.blocks_path))
# g = g[(g.state_name.map(alias) == alias("Telangana")) &
#     (g.district_name.map(alias) == alias("Adilabad"))]
# roster = sorted(alias(b) for b in g.block_name.astype(str))
# print(f"ROSTER Adilabad blocks ({len(roster)}):", roster)

# blocks = Path(cfg.districts_path).parent / "processed/txge35_extreme_heat_days/Telangana/blocks/ADILABAD"
# dirs = sorted(p.name for p in blocks.iterdir() if p.is_dir())
# populated = [d for d in dirs if any(blocks.joinpath(d).rglob("*.parquet")) or any(blocks.joinpath(d).rglob("*.csv"))]
# print(f"\nPOPULATED dirs ({len(populated)}):", populated)
# print("POPULATED → alias:", sorted(alias(d.replace('_',' ')) for d in populated))
# print("\nPOPULATED keys IN roster:   ", sorted(set(alias(d.replace('_',' ')) for d in populated) & set(roster)))
# print("POPULATED keys NOT in roster:", sorted(set(alias(d.replace('_',' ')) for d in populated) - set(roster)))

cd /mnt/d/projects/india_resilience_tool
"/mnt/c/Users/22015611/AppData/Local/miniconda3/envs/irt/python.exe" -X utf8 - <<'PY'
from pathlib import Path
import pandas as pd
from india_resilience_tool.config.composite_metrics import COMPOSITES_BY_SLUG
from india_resilience_tool.config import proposal_bundles as pb
from india_resilience_tool.config.paths import get_paths_config, resolve_processed_root
from india_resilience_tool.utils.naming import alias
from tools.optimized.build_processed_optimised import _canonical_admin_keys

STATE = "Telangana"
LIVE = {
    "composite_drought_risk":            "thematic",   # 6 SPI
    "composite_agricultural_risk":       "proposal",   # 7
    "composite_asset_risk_thermal_power":"proposal",   # 3
    # composite_agriculture_growing_conditions = RETIRED, intentionally excluded
}

def comps(slug, kind):
    if kind == "thematic":
        return COMPOSITES_BY_SLUG[slug].component_metric_slugs
    return pb.get_proposal_bundle_source_metric_slugs(slug)

union = {}
for slug, kind in LIVE.items():
    for c in comps(slug, kind):
        union.setdefault(c, []).append(slug)

cfg = get_paths_config()
canon = _canonical_admin_keys("district", str(cfg.districts_path))
print(f"canonical district roster: {len(canon)} keys  (expect 33)\n")

bad = []
print(f"{'component':42} {'verdict':8} {'rows':>4}  detail")
for slug in sorted(union):
    f = Path(resolve_processed_root(slug, mode="portfolio")) / STATE / "master_metrics_by_district.csv"
    if not f.exists():
        print(f"{slug:42} {'MISSING':8} {0:>4}  no master file  <-- HEAL"); bad.append(slug); continue
    df = pd.read_csv(f)
    keys = set(df["state"].astype(str).map(alias) + "|" + df["district"].astype(str).map(alias))
    offenders = sorted(keys - canon)   # stale / old-spelling rows
    missing   = sorted(canon - keys)   # roster districts absent
    if offenders:   verdict, detail = "STALE", "stale=" + ",".join(o.split('|')[-1] for o in offenders[:6])
    elif missing:   verdict, detail = "SHORT", "missing=" + ",".join(m.split('|')[-1] for m in missing[:6])
    elif len(df) == len(canon): verdict, detail = "GREEN", ""
    else:           verdict, detail = f"DUP?", f"{len(df)} rows vs {len(canon)} roster"
    if verdict != "GREEN": bad.append(slug)
    flag = "" if verdict == "GREEN" else "  <-- HEAL"
    print(f"{slug:42} {verdict:8} {len(df):>4}  {detail}{flag}")

print()
print("NEEDS HEAL: " + " ".join(bad) if bad else "ALL GREEN — safe to run composites.")