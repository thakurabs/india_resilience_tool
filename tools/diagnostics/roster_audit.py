"""Canonical-roster audit + boundary-migration housekeeping for IRT (CHG-0089).

Promoted from the throwaway ``tools/_scratch_roster_inventory.py`` into a documented,
parametrized tool for the LGD boundary-migration rounds (Telangana now, Maharashtra next).

It does three jobs, all keyed off the **published per-state geometry shard** as the
canonical admin roster:

  AUDIT (default, read-only)
    - A) canonical roster (district_key / block_key from the geometry shard)
    - B) master-staleness: published masters carrying keys absent from the roster
    - C) orphan-dir inventory in the raw tree, covering BOTH directory-name-keyed source
         trees: ``<level>/<UNIT>`` (periods, the master source) AND
         ``<level>/ensembles/<UNIT>`` (the yearly_ensemble source) — the latter was the
         GAP-7 blind spot in the scratch tool.
    - E) COMPLETENESS gate: for each keeper, assert ``canonical_keys ⊆ published_keys`` over
         BOTH the master parquet and (where it exists) the yearly_ensemble parquet. Exits
         non-zero if any keeper is missing any canonical unit in either artifact. This is the
         machine-checkable "33/33" the roster gate alone cannot prove (it only flags extras).

  --quarantine-processed (apply mode; --dry-run is the default)
    Move old-named raw dirs (periods + ensembles) out of the ensemble glob path into
    ``processed/_stale_prelgd_bak/`` so the per-slug ensemble input glob can no longer pick
    up a stale-named unit. At district level a file-level NEW-NAME INTERLOCK refuses to move
    an old dir unless the renamed unit's new-named periods+ensembles files are present (the
    district path re-masters from existing periods, so a missing new copy would strand the
    unit). Block level is reported but not interlocked: block compute is regenerated from raw
    grids downstream, so moving old block dirs cannot strand source data.

  --prune-optimised (apply mode; --dry-run is the default)
    Move deferred-stale published masters (carry stale keys AND are not in-scope keepers AND
    not gw_*) into an OUT-OF-BUNDLE sidecar ``_stale_optimised_prelgd_bak/`` so the runtime
    disk-scan inventory stops surfacing them. A keeper-component guard refuses to prune any
    slug that is a declared component of any keeper bundle.

Every apply mode writes a JSON move-manifest (old -> new per path) for mechanical reversal.

Nothing is moved unless ``--apply`` is passed; the default for every mode is a dry-run.
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# Allow running both as a module (-m tools.diagnostics.roster_audit) and as a script.
_REPO = Path(__file__).resolve().parents[2]
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from india_resilience_tool.utils.naming import alias  # noqa: E402

# The 16 in-scope thematic + sectoral keepers (see plan P3 / CHG-0080..0091).
KEEPERS_16: tuple[str, ...] = (
    # composites (5)
    "composite_agricultural_risk",
    "composite_agriculture_growing_conditions",
    "composite_asset_risk_thermal_power",
    "composite_drought_risk",
    "composite_flood_jrc_depth",
    # components (11)
    "jrc_flood_depth_index_rp100",
    "jrc_flood_depth_rp100",
    "jrc_flood_extent_rp100",
    "r95p_interannual_variability",
    "spi3_count_events_lt_minus1",
    "spi6_count_events_lt_minus1",
    "spi12_count_events_lt_minus1",
    "spi3_max_spell_lt_minus1",
    "spi6_max_spell_lt_minus1",
    "spi12_max_spell_lt_minus1",
    "spi3_count_months_lt_minus1",
)

LEVELS = ("district", "block")


# --------------------------------------------------------------------------------------
# Path resolution
# --------------------------------------------------------------------------------------
def resolve_data_dir(explicit: Optional[str] = None) -> Path:
    """Resolve the IRT data dir (parent of ``processed`` / ``processed_optimised``)."""
    if explicit:
        return Path(explicit)
    env = os.environ.get("IRT_DATA_DIR")
    if env:
        return Path(env)
    try:
        from india_resilience_tool.config.paths import get_paths_config

        return Path(get_paths_config().base_output_root).parent
    except Exception:  # noqa: BLE001
        return Path(r"D:\projects\irt_data")


class Paths:
    def __init__(self, data_dir: Path, state: str) -> None:
        self.data = data_dir
        self.state = state
        self.processed = data_dir / "processed"
        self.optimised = data_dir / "processed_optimised"
        self.metrics = self.optimised / "metrics"

    def geometry_shard(self, level: str) -> Path:
        return self.optimised / "geometry" / "admin" / level / f"state={self.state}.geojson"

    def master_parquet(self, slug: str, level: str) -> Path:
        return self.metrics / slug / "masters" / "admin" / level / f"state={self.state}.parquet"

    def yearly_parquet(self, slug: str, level: str) -> Path:
        return self.metrics / slug / "yearly_ensemble" / "admin" / level / f"state={self.state}.parquet"

    def raw_periods_root(self, slug: str, level: str) -> Path:
        sub = "districts" if level == "district" else "blocks"
        return self.processed / slug / self.state / sub

    def raw_ensembles_root(self, slug: str, level: str) -> Path:
        return self.raw_periods_root(slug, level) / "ensembles"

    @property
    def quarantine_root(self) -> Path:
        return self.processed / "_stale_prelgd_bak"

    @property
    def prune_sidecar_root(self) -> Path:
        # OUT-OF-BUNDLE on purpose: parity audit allowed_markers would flag an in-bundle sidecar.
        return self.data / "_stale_optimised_prelgd_bak"


# --------------------------------------------------------------------------------------
# Canonical roster (from the published geometry shard)
# --------------------------------------------------------------------------------------
def canonical_keys(paths: Paths, level: str) -> tuple[set[str], set[str]]:
    """Return (key_set, district_alias_set) for ``level`` from the geometry shard.

    key_set: the ``<level>_key`` values (``alias(state)|alias(district)[|alias(block)]``) —
      the exact form the publish roster gate uses. Used for the completeness gate.
    district_alias_set: ``{alias(district_name)}`` — used to classify raw orphan dirs
      (which are keyed by district name at both levels, since block_key embeds the district).
    """
    shard = paths.geometry_shard(level)
    if not shard.exists():
        raise SystemExit(f"[roster_audit] geometry shard missing: {shard}")
    obj = json.loads(shard.read_text(encoding="utf-8"))
    feats = obj.get("features", [])
    key_field = f"{level}_key"
    keys: set[str] = set()
    dist_aliases: set[str] = set()
    for f in feats:
        props = f.get("properties", {})
        k = props.get(key_field)
        if k:
            keys.add(str(k).strip())
        dn = props.get("district_name")
        if dn:
            dist_aliases.add(alias(str(dn)))
    return keys, dist_aliases


# --------------------------------------------------------------------------------------
# Published-artifact key readers
# --------------------------------------------------------------------------------------
def _read_key_column(pq: Path, level: str) -> Optional[set[str]]:
    """Return the set of ``<level>_key`` values from a published parquet, or None if absent."""
    if not pq.exists():
        return None
    import pandas as pd

    key_col = f"{level}_key"
    try:
        df = pd.read_parquet(pq, columns=[key_col])
    except Exception:
        # Column may not be projectable on some engines; fall back to full read.
        try:
            df = pd.read_parquet(pq)
        except Exception as e:  # noqa: BLE001
            print(f"   [read-error] {pq}: {e}")
            return set()
    if key_col not in df.columns:
        return set()
    return {str(x).strip() for x in df[key_col].dropna().unique()}


def published_master_keys(paths: Paths, slug: str, level: str) -> Optional[set[str]]:
    return _read_key_column(paths.master_parquet(slug, level), level)


def published_yearly_keys(paths: Paths, slug: str, level: str) -> Optional[set[str]]:
    return _read_key_column(paths.yearly_parquet(slug, level), level)


# --------------------------------------------------------------------------------------
# Raw-tree orphan inventory (periods + ensembles)
# --------------------------------------------------------------------------------------
_SKIP_DIRS = {".markers"}


def _orphan_unit_dirs(root: Path, canon_dist_aliases: set[str]) -> list[Path]:
    """List immediate child dirs of ``root`` whose alias is not a canonical district.

    Both periods (``<level>/<UNIT>``) and ensembles (``<level>/ensembles/<UNIT>``) are keyed
    by the *district* directory name (block_key embeds the district), so a renamed parent
    district is the orphan at either level — matching plan refinement R3.
    """
    if not root.exists():
        return []
    out: list[Path] = []
    for d in sorted(root.iterdir()):
        if not d.is_dir() or d.name in _SKIP_DIRS or d.name == "ensembles":
            continue
        if alias(d.name) not in canon_dist_aliases:
            out.append(d)
    return out


def orphan_dirs_for_slug(paths: Paths, slug: str, level: str, canon_dist_aliases: set[str]) -> dict[str, list[Path]]:
    """Return {"periods": [...], "ensembles": [...]} orphan dirs for one slug/level."""
    return {
        "periods": _orphan_unit_dirs(paths.raw_periods_root(slug, level), canon_dist_aliases),
        "ensembles": _orphan_unit_dirs(paths.raw_ensembles_root(slug, level), canon_dist_aliases),
    }


def active_slugs(paths: Paths) -> list[str]:
    """List active processed/<slug> dirs (exclude backups / hidden / sidecars)."""
    if not paths.processed.exists():
        return []
    out = []
    for d in sorted(paths.processed.iterdir()):
        if not d.is_dir():
            continue
        n = d.name
        if n.startswith(".") or n.startswith("_") or ".bak" in n or "_bak" in n:
            continue
        out.append(n)
    return out


# --------------------------------------------------------------------------------------
# District new-name interlock (GAP 1 / R1 / R2)
# --------------------------------------------------------------------------------------
def _canonical_new_dir(root: Path, canon_alias: str) -> Optional[Path]:
    """Find the raw dir under ``root`` whose alias matches a canonical district alias."""
    if not root.exists():
        return None
    for d in root.iterdir():
        if d.is_dir() and d.name not in _SKIP_DIRS and d.name != "ensembles" and alias(d.name) == canon_alias:
            return d
    return None


def district_interlock(paths: Paths, slug: str, canon_dist_aliases: set[str], canon_names: dict[str, str]) -> list[str]:
    """Return a list of interlock-failure messages for a keeper at DISTRICT level.

    For each canonical district, assert the new-named periods dir has a ``*_periods.csv`` and
    the new-named ensembles dir has a scenario ``*.csv``. Empty list == interlock passes.
    Only meaningful where the keeper has *some* district data (a fully-uncomputed keeper is a
    Step-2 from-scratch case, not a strand risk — flagged separately by the caller).
    """
    failures: list[str] = []
    periods_root = paths.raw_periods_root(slug, "district")
    ens_root = paths.raw_ensembles_root(slug, "district")
    for canon_alias in sorted(canon_dist_aliases):
        name = canon_names.get(canon_alias, canon_alias)
        pdir = _canonical_new_dir(periods_root, canon_alias)
        if pdir is None or not any(pdir.rglob("*_periods.csv")):
            failures.append(f"{slug}: district '{name}' MISSING new-named periods (*_periods.csv)")
        edir = _canonical_new_dir(ens_root, canon_alias)
        if edir is None or not any(edir.rglob("*.csv")):
            failures.append(f"{slug}: district '{name}' MISSING new-named ensembles (ensembles/<new>/*/*.csv)")
    return failures


# --------------------------------------------------------------------------------------
# Keeper-component guard set (GAP 6)
# --------------------------------------------------------------------------------------
def keeper_component_guard_set(keepers: tuple[str, ...]) -> set[str]:
    """Slugs that must NEVER be pruned: keepers + their declared components + gw_* sentinel.

    Conservative superset: unions all in-scope bundle component registries so the guard
    refuses to prune anything a keeper bundle could depend on, even indirectly.
    """
    guard: set[str] = set(keepers)
    try:
        from india_resilience_tool.config.bundle_weights import LANDING_BUNDLE_WEIGHTS

        for entries in LANDING_BUNDLE_WEIGHTS.values():
            for e in entries:
                guard.add(str(e.metric_slug))
    except Exception as e:  # noqa: BLE001
        print(f"   [guard] LANDING_BUNDLE_WEIGHTS unavailable: {e}")
    try:
        from india_resilience_tool.config.dashboard_bundles import dashboard_composite_slugs
        from india_resilience_tool.config.proposal_bundles import get_proposal_bundle_source_metric_slugs

        for cs in dashboard_composite_slugs():
            for ms in get_proposal_bundle_source_metric_slugs(cs):
                guard.add(str(ms))
    except Exception as e:  # noqa: BLE001
        print(f"   [guard] proposal_bundles unavailable: {e}")
    try:
        from india_resilience_tool.config.composite_metrics import COMPOSITES_BY_SLUG

        for spec in COMPOSITES_BY_SLUG.values():
            for ms in getattr(spec, "component_metric_slugs", ()):  # type: ignore[attr-defined]
                guard.add(str(ms))
    except Exception as e:  # noqa: BLE001
        print(f"   [guard] composite_metrics unavailable: {e}")
    return guard


def _is_gw(slug: str) -> bool:
    return slug.startswith("gw_")


# --------------------------------------------------------------------------------------
# Move-manifest helper
# --------------------------------------------------------------------------------------
def _write_manifest(moves: list[tuple[Path, Path]], out_path: Path, *, mode: str, applied: bool) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "mode": mode,
        "applied": applied,
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "moves": [{"old": str(src), "new": str(dst)} for src, dst in moves],
        "count": len(moves),
    }
    out_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(f"   move-manifest written: {out_path} ({len(moves)} entr{'y' if len(moves)==1 else 'ies'})")


def _do_moves(moves: list[tuple[Path, Path]], *, apply: bool) -> None:
    for src, dst in moves:
        print(f"   MOVE  {src}\n     ->  {dst}")
        if apply:
            dst.parent.mkdir(parents=True, exist_ok=True)
            if dst.exists():
                print(f"   [skip] destination exists, not overwriting: {dst}")
                continue
            shutil.move(str(src), str(dst))


# --------------------------------------------------------------------------------------
# Modes
# --------------------------------------------------------------------------------------
def run_audit(paths: Paths, levels: tuple[str, ...], keepers: tuple[str, ...]) -> int:
    slugs = active_slugs(paths)
    exit_code = 0
    for level in levels:
        canon_keys, canon_dist_aliases = canonical_keys(paths, level)
        print("=" * 80)
        print(f"[{level.upper()}] A) CANONICAL ROSTER — {len(canon_keys)} {level} keys "
              f"({len(canon_dist_aliases)} districts) from {paths.geometry_shard(level).name}")

        # B) master staleness
        stale_masters: dict[str, set[str]] = {}
        no_master: list[str] = []
        for slug in slugs:
            keys = published_master_keys(paths, slug, level)
            if keys is None:
                no_master.append(slug)
                continue
            bad = keys - canon_keys
            if bad:
                stale_masters[slug] = bad
        print(f"[{level.upper()}] B) MASTER STALENESS — {len(stale_masters)} slugs with stale keys; "
              f"{len(no_master)} slugs without a {level} master")
        for slug in sorted(stale_masters):
            print(f"     [STALE-MASTER] {slug}: {sorted(stale_masters[slug])[:8]}")

        # C) orphan dirs (periods + ensembles)
        orphan_summary: dict[str, dict[str, list[Path]]] = {}
        n_periods = n_ens = 0
        for slug in slugs:
            od = orphan_dirs_for_slug(paths, slug, level, canon_dist_aliases)
            if od["periods"] or od["ensembles"]:
                orphan_summary[slug] = od
                n_periods += len(od["periods"])
                n_ens += len(od["ensembles"])
        print(f"[{level.upper()}] C) ORPHAN DIRS — {len(orphan_summary)} slugs; "
              f"{n_periods} periods dirs + {n_ens} ensembles dirs (GAP-7 coverage)")

        # E) completeness gate (master AND yearly_ensemble) for keepers
        print(f"[{level.upper()}] E) COMPLETENESS GATE (keepers) — canonical ⊆ published?")
        for slug in keepers:
            mkeys = published_master_keys(paths, slug, level)
            if mkeys is None:
                print(f"     [n/a]   {slug}: no {level} master published")
            else:
                missing = canon_keys - mkeys
                stale = mkeys - canon_keys
                status = "OK" if not missing and not stale else "FAIL"
                if status == "FAIL":
                    exit_code = 1
                print(f"     [{status}] {slug} master: missing={len(missing)} stale={len(stale)}"
                      + (f" missing_sample={sorted(missing)[:4]}" if missing else "")
                      + (f" stale_sample={sorted(stale)[:4]}" if stale else ""))
            ykeys = published_yearly_keys(paths, slug, level)
            if ykeys is not None:
                ymissing = canon_keys - ykeys
                ystale = ykeys - canon_keys
                ystatus = "OK" if not ymissing and not ystale else "FAIL"
                if ystatus == "FAIL":
                    exit_code = 1
                print(f"     [{ystatus}] {slug} yearly: missing={len(ymissing)} stale={len(ystale)}"
                      + (f" missing_sample={sorted(ymissing)[:4]}" if ymissing else "")
                      + (f" stale_sample={sorted(ystale)[:4]}" if ystale else ""))
        print()
    if exit_code:
        print("[roster_audit] COMPLETENESS GATE FAILED — at least one keeper is missing/stale "
              "in master or yearly_ensemble. (exit 1)")
    else:
        print("[roster_audit] completeness gate: all keepers clean across requested levels.")
    return exit_code


def run_quarantine(paths: Paths, levels: tuple[str, ...], keepers: tuple[str, ...], *, apply: bool, manifest: Path) -> int:
    slugs = active_slugs(paths)
    moves: list[tuple[Path, Path]] = []
    interlock_failures: list[str] = []

    for level in levels:
        canon_keys, canon_dist_aliases = canonical_keys(paths, level)
        canon_names = {}
        # name lookup for nicer messages
        shard = json.loads(paths.geometry_shard(level).read_text(encoding="utf-8"))
        for f in shard.get("features", []):
            dn = f.get("properties", {}).get("district_name")
            if dn:
                canon_names[alias(str(dn))] = str(dn)

        # District interlock (only for keepers that have some district data).
        if level == "district":
            for slug in keepers:
                proot = paths.raw_periods_root(slug, "district")
                has_any = proot.exists() and any(
                    d.is_dir() and d.name not in _SKIP_DIRS and d.name != "ensembles"
                    for d in proot.iterdir()
                )
                if not has_any:
                    print(f"   [interlock] {slug}: no district raw data → Step-2 from-scratch case, interlock skipped")
                    continue
                fails = district_interlock(paths, slug, canon_dist_aliases, canon_names)
                interlock_failures.extend(fails)

        # Build orphan move list (periods + ensembles), all slugs.
        for slug in slugs:
            od = orphan_dirs_for_slug(paths, slug, level, canon_dist_aliases)
            for kind, dirs in od.items():
                for src in dirs:
                    rel = src.relative_to(paths.processed)
                    dst = paths.quarantine_root / rel
                    moves.append((src, dst))

    print("=" * 80)
    print(f"[quarantine] {'APPLY' if apply else 'DRY-RUN'} — {len(moves)} orphan dirs to move "
          f"→ {paths.quarantine_root}")
    if interlock_failures:
        print("=" * 80)
        print(f"[quarantine] DISTRICT INTERLOCK FAILED ({len(interlock_failures)} issue(s)) — "
              "a keeper is missing new-named periods/ensembles for a canonical district.")
        for m in interlock_failures:
            print(f"   [INTERLOCK] {m}")
        print("   Remediation (R2): regenerate the missing unit from raw grids, e.g.")
        print("     compute_indices_multiprocess --state <STATE> --level district --metrics <slug> --overwrite")
        print("   then re-run the interlock. NOT moving anything (hard stop).")
        return 2

    _do_moves(moves, apply=apply)
    _write_manifest(moves, manifest, mode="quarantine-processed", applied=apply)
    if not apply:
        print("   (dry-run; pass --apply to perform the moves)")
    return 0


def run_prune(paths: Paths, levels: tuple[str, ...], keepers: tuple[str, ...], *, apply: bool, manifest: Path) -> int:
    slugs = active_slugs(paths)
    guard = keeper_component_guard_set(keepers)
    print("=" * 80)
    print(f"[prune] keeper-component guard set: {len(guard)} protected slug(s)")

    # A slug is prune-eligible if it carries stale keys at ANY requested level AND is not
    # guarded AND is not gw_*.
    eligible: dict[str, set[str]] = {}
    for level in levels:
        canon_keys, _ = canonical_keys(paths, level)
        for slug in slugs:
            keys = published_master_keys(paths, slug, level)
            if keys is None:
                continue
            if keys - canon_keys:
                eligible.setdefault(slug, set()).add(level)

    guard_hits = sorted(s for s in eligible if s in guard or _is_gw(s))
    prune_set = sorted(s for s in eligible if s not in guard and not _is_gw(s))

    if guard_hits:
        print(f"[prune] GUARD/carve-out protected {len(guard_hits)} stale slug(s) (NOT pruned):")
        for s in guard_hits:
            why = "gw_* carve-out" if _is_gw(s) else "keeper-component guard"
            print(f"     [protected:{why}] {s}")

    moves: list[tuple[Path, Path]] = []
    for slug in prune_set:
        src = paths.metrics / slug
        dst = paths.prune_sidecar_root / "metrics" / slug
        moves.append((src, dst))

    print("=" * 80)
    print(f"[prune] {'APPLY' if apply else 'DRY-RUN'} — {len(prune_set)} deferred-stale "
          f"metric dir(s) to move → {paths.prune_sidecar_root}/metrics/")
    for slug in prune_set:
        print(f"     [PRUNE] {slug}  (stale at: {','.join(sorted(eligible[slug]))})")
    _do_moves(moves, apply=apply)
    _write_manifest(moves, manifest, mode="prune-optimised", applied=apply)
    if not apply:
        print("   (dry-run; pass --apply to perform the moves)")
    return 0


# --------------------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------------------
def main(argv: Optional[list[str]] = None) -> int:
    p = argparse.ArgumentParser(
        prog="roster_audit",
        description="Canonical-roster audit + boundary-migration quarantine/prune (CHG-0089).",
    )
    p.add_argument("--state", default=os.environ.get("IRT_PILOT_STATE", "Telangana"))
    p.add_argument("--level", choices=("district", "block", "all"), default="all")
    p.add_argument("--data-dir", default=None, help="Override IRT data dir (default: paths config / IRT_DATA_DIR).")
    p.add_argument("--keepers", default=None, help="Comma-separated keeper slugs (default: the 16 in-scope).")
    mode = p.add_mutually_exclusive_group()
    mode.add_argument("--quarantine-processed", action="store_true", help="Quarantine old-named raw dirs (periods+ensembles).")
    mode.add_argument("--prune-optimised", action="store_true", help="Prune deferred-stale published masters to out-of-bundle sidecar.")
    p.add_argument("--apply", action="store_true", help="Perform moves (default: dry-run).")
    p.add_argument("--manifest", default=None, help="Path for the JSON move-manifest (apply modes).")
    args = p.parse_args(argv)

    data_dir = resolve_data_dir(args.data_dir)
    paths = Paths(data_dir, args.state)
    levels = LEVELS if args.level == "all" else (args.level,)
    keepers = tuple(s.strip() for s in args.keepers.split(",")) if args.keepers else KEEPERS_16

    print(f"[roster_audit] state={args.state} levels={levels} data_dir={data_dir}")

    default_manifest = paths.optimised / "logs" / "roster_audit" / (
        f"moves_{'quarantine' if args.quarantine_processed else 'prune'}_"
        f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.json"
    )
    manifest = Path(args.manifest) if args.manifest else default_manifest

    if args.quarantine_processed:
        return run_quarantine(paths, levels, keepers, apply=args.apply, manifest=manifest)
    if args.prune_optimised:
        return run_prune(paths, levels, keepers, apply=args.apply, manifest=manifest)
    return run_audit(paths, levels, keepers)


if __name__ == "__main__":
    raise SystemExit(main())
