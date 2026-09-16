"""Block-name-granularity orphan audit (read-only).

District-level quarantine only catches renamed DISTRICT dirs. Block staleness lives at
(district, block) granularity — old block spellings and pre-LGD 1->many splits that a
district-name sweep misses. This walks every slug's Telangana block tree, keys each
(district_dir, block_dir) leaf the same way the publish gate does (alias of the boundary
name), and flags any pair absent from the current blocks_4326.geojson roster.

Nothing is moved. Output is a per-slug orphan report + the distinct orphan (district|block)
set, suitable for feeding a block-name-granularity quarantine.
"""
from __future__ import annotations

from collections import defaultdict
from pathlib import Path

import geopandas as gpd

from india_resilience_tool.config.paths import get_paths_config
from india_resilience_tool.data.adm3_loader import ensure_adm3_columns
from india_resilience_tool.utils.naming import alias

STATE = "Telangana"
STATE_KEY = alias(STATE)


def _name_from_dir(dirname: str) -> str:
    """Invert the compute's dir encoding (spaces -> underscores) before aliasing."""
    return dirname.replace("_", " ")


def _canonical_block_keys() -> tuple[frozenset[str], frozenset[str]]:
    """Return (block_keys, district_keys) for STATE from the live LGD boundaries.

    block key   = alias(district)|alias(block)
    district key = alias(district)
    Keyed identically to build_processed_optimised._admin_keys(level='block').
    """
    cfg = get_paths_config()
    gdf = ensure_adm3_columns(gpd.read_file(cfg.blocks_path))
    gdf = gdf[gdf["state_name"].astype(str).map(alias) == STATE_KEY]
    dkeys = gdf["district_name"].astype(str).map(alias)
    bkeys = dkeys.str.cat(gdf["block_name"].astype(str).map(alias), sep="|")
    return frozenset(bkeys.tolist()), frozenset(dkeys.tolist())


def _iter_block_leaves(blocks_dir: Path):
    """Yield (district_dir, block_dir, leaf_path) over both periods and ensembles subtrees."""
    if not blocks_dir.is_dir():
        return
    for district_dir in sorted(p for p in blocks_dir.iterdir() if p.is_dir()):
        # 'ensembles' is a sibling subtree mirroring district/block one level deeper
        roots = [district_dir]
        if district_dir.name.lower() == "ensembles":
            roots = [p for p in district_dir.iterdir() if p.is_dir()]
        for droot in roots:
            for block_dir in sorted(p for p in droot.iterdir() if p.is_dir()):
                yield droot.name, block_dir.name, block_dir


def main() -> None:
    cfg = get_paths_config()
    processed_root = Path(cfg.districts_path).parent / "processed"

    block_keys, district_keys = _canonical_block_keys()
    print(f"canonical {STATE}: roster blocks={len(block_keys)} districts={len(district_keys)}")

    per_slug: dict[str, dict[str, int]] = {}
    distinct_orphans: set[str] = set()
    stale_districts: set[str] = set()

    for slug_dir in sorted(p for p in processed_root.iterdir() if p.is_dir()):
        if slug_dir.name.startswith("_"):  # skip _stale_prelgd_bak/ etc.
            continue
        blocks_dir = slug_dir / STATE / "blocks"
        if not blocks_dir.is_dir():
            continue
        total = orphan = 0
        for ddir, bdir, _leaf in _iter_block_leaves(blocks_dir):
            dkey = alias(_name_from_dir(ddir))
            bkey = f"{dkey}|{alias(_name_from_dir(bdir))}"
            total += 1
            if bkey not in block_keys:
                orphan += 1
                distinct_orphans.add(f"{ddir}|{bdir}")
                if dkey not in district_keys:
                    stale_districts.add(ddir)
        if orphan:
            per_slug[slug_dir.name] = {"total": total, "orphan": orphan}

    print(f"\nslugs with block orphans: {len(per_slug)}")
    for slug in sorted(per_slug):
        s = per_slug[slug]
        print(f"  {slug}: {s['orphan']}/{s['total']} orphan leaf blocks")
    print(f"\ndistinct orphan (district|block) pairs: {len(distinct_orphans)}")
    print(f"stale (non-canonical) district dirs seen: {sorted(stale_districts)}")
    for pair in sorted(distinct_orphans)[:40]:
        print(f"    {pair}")
    if len(distinct_orphans) > 40:
        print(f"    … +{len(distinct_orphans) - 40} more")


if __name__ == "__main__":
    main()
