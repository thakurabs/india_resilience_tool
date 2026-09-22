# tools/diagnostics/verify_admin_join_consistency.py
"""Cross-level admin *join* consistency check for the boundary GeoJSONs.

Complements ``verify_states_geojson.py`` and ``verify_districts_blocks_geojson.py``
(which validate one layer at a time). This script focuses on the two properties
that matter when the three layers are used together as a nested hierarchy:

  1. NAMING consistency
     - every block ``(state_name, district_name)`` maps to a district row,
     - every district/block ``state_name`` maps to a state row,
     - ``state_lgd_code`` <-> ``state_name`` agree where both are carried.

  2. JOIN (geometry) consistency
     - the union (dissolve) of a district's blocks reproduces the district
       polygon, and the union of a state's districts reproduces the state
       polygon. Reported per unit as IoU (intersection / union) and residual
       (symmetric-difference area as a fraction of the parent), computed in an
       equal-area CRS.

Optionally (``--figures-dir``) it renders a few diagnostic figures:
  - ``example_district_nesting.png`` — one district's blocks tiling it, with the
    union-of-blocks outline overlaid on the district outline (+ residual slivers),
  - ``example_state_nesting.png`` — same, districts tiling one state,
  - ``area_parity_scatter.png`` — union-of-children vs parent area for ALL units,
  - ``iou_consistency_bands.png`` — population of IoU values by band.

The script is read-only w.r.t. the boundary data; it only ever writes figures
under ``--figures-dir``. Exit code is non-zero if any unit's IoU falls below
``--min-iou`` (default 0.999), so it can double as a CI / pre-ingest guard.

Examples
--------
    # Text report against the configured data dir
    python -m tools.diagnostics.verify_admin_join_consistency

    # Point at an arbitrary folder of the three *_4326.geojson files + figures
    python -m tools.diagnostics.verify_admin_join_consistency \
        --geojson-dir /mnt/c/Users/.../updated_geojsons \
        --figures-dir ./_join_figs
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from pyproj import datadir


def _configure_pyproj_data_dir() -> None:
    """Point pyproj at a valid proj.db before geopandas/shapely import.

    Mirrors the other diagnostics scripts: the env may ship a pip-installed
    geo stack whose bundled proj data is missing/stale.
    """
    candidates = [os.environ.get("PROJ_DATA"), os.environ.get("PROJ_LIB")]
    conda_prefix = os.environ.get("CONDA_PREFIX")
    if conda_prefix:
        candidates.append(str(Path(conda_prefix) / "Library" / "share" / "proj"))
        candidates.append(str(Path(conda_prefix) / "share" / "proj"))
    candidates.append(str(Path(sys.prefix) / "Library" / "share" / "proj"))
    candidates.append(str(Path(sys.prefix) / "share" / "proj"))
    for candidate in candidates:
        if not candidate:
            continue
        if (Path(candidate) / "proj.db").exists():
            datadir.set_data_dir(candidate)
            return


_configure_pyproj_data_dir()


import geopandas as gpd  # noqa: E402  (must import after pyproj data-dir is set)
import pandas as pd  # noqa: E402
from shapely.ops import unary_union  # noqa: E402
from shapely.validation import make_valid  # noqa: E402

EQUAL_AREA_CRS = "EPSG:6933"  # World Cylindrical Equal Area (metres) for area/overlay
DISPLAY_CRS = "EPSG:4326"     # geographic, for the example maps

STATE_KEYS = ["state_name"]
DISTRICT_KEYS = ["state_name", "district_name"]


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------

def _resolve_paths(geojson_dir: Path | None) -> dict[str, Path]:
    """Resolve the three layer paths, defaulting to the configured data dir."""
    if geojson_dir is not None:
        base = geojson_dir
    else:
        from paths import DATA_DIR  # noqa: E402  (lazy; needs repo root on path)
        base = Path(DATA_DIR)
    paths = {
        "states": base / "states_4326.geojson",
        "districts": base / "districts_4326.geojson",
        "blocks": base / "blocks_4326.geojson",
    }
    missing = [str(p) for p in paths.values() if not p.exists()]
    if missing:
        raise FileNotFoundError("missing GeoJSON layer(s):\n  " + "\n  ".join(missing))
    return paths


def load_layer(path: Path, label: str) -> gpd.GeoDataFrame:
    """Read a layer, assert EPSG:4326, repair invalid geometries."""
    print(f"  reading {label}: {path}", flush=True)
    gdf = gpd.read_file(path)
    assert gdf.crs is not None and str(gdf.crs).upper().endswith("4326"), (
        f"{label}: CRS is not EPSG:4326 ({gdf.crs})"
    )
    n_invalid = int((~gdf.geometry.is_valid).sum())
    if n_invalid:
        print(f"    {label}: {n_invalid} invalid geometries -> make_valid", flush=True)
        gdf["geometry"] = gdf.geometry.apply(make_valid)
    return gdf


# ---------------------------------------------------------------------------
# (1) Naming consistency
# ---------------------------------------------------------------------------

def check_naming(
    states: gpd.GeoDataFrame,
    districts: gpd.GeoDataFrame,
    blocks: gpd.GeoDataFrame,
) -> list[str]:
    """Exact-match name mapping up the hierarchy. Returns a list of problems."""
    print("\n=== (1) NAMING CONSISTENCY ===", flush=True)
    problems: list[str] = []

    state_names = set(states["state_name"])
    dist_pairs = set(zip(districts["state_name"], districts["district_name"]))
    blk_pairs = set(zip(blocks["state_name"], blocks["district_name"]))

    orphan_blocks = sorted(blk_pairs - dist_pairs)
    childless_dists = sorted(dist_pairs - blk_pairs)
    bad_dist_states = sorted(set(districts["state_name"]) - state_names)
    bad_blk_states = sorted(set(blocks["state_name"]) - state_names)

    print(f"  block (state,district) pairs NOT in districts : {len(orphan_blocks)}", flush=True)
    for x in orphan_blocks[:20]:
        print(f"      orphan block district: {x}", flush=True)
    print(f"  districts with NO blocks                       : {len(childless_dists)}", flush=True)
    for x in childless_dists[:20]:
        print(f"      childless district: {x}", flush=True)
    print(f"  district state_name NOT in states             : {bad_dist_states}", flush=True)
    print(f"  block state_name NOT in states                : {bad_blk_states}", flush=True)

    # code <-> name agreement where both columns exist
    n_code_mismatch = 0
    if "state_lgd_code" in states.columns and "state_lgd_code" in blocks.columns:
        code2name = dict(zip(states["state_lgd_code"], states["state_name"]))
        mism = blocks[blocks["state_lgd_code"].map(code2name) != blocks["state_name"]]
        n_code_mismatch = len(mism)
        print(f"  blocks where state_lgd_code's state != name   : {n_code_mismatch}", flush=True)
        if n_code_mismatch:
            print(mism[["state_lgd_code", "state_name"]].drop_duplicates().to_string(index=False), flush=True)
    else:
        print("  (state_lgd_code not on both states+blocks; skipping code<->name check)", flush=True)

    if orphan_blocks:
        problems.append(f"{len(orphan_blocks)} block district(s) not present in districts layer")
    if bad_dist_states:
        problems.append(f"{len(bad_dist_states)} district state_name(s) not present in states layer")
    if bad_blk_states:
        problems.append(f"{len(bad_blk_states)} block state_name(s) not present in states layer")
    if n_code_mismatch:
        problems.append(f"{n_code_mismatch} block(s) with state_lgd_code/state_name disagreement")
    return problems


# ---------------------------------------------------------------------------
# (2) Join (geometry) consistency
# ---------------------------------------------------------------------------

def join_consistency(
    parent: gpd.GeoDataFrame,
    child: gpd.GeoDataFrame,
    keys: list[str],
    label: str,
) -> pd.DataFrame:
    """Dissolve ``child`` by ``keys`` and compare to ``parent`` per unit.

    Returns a DataFrame with one row per matched unit:
    ``keys + [parent_km2, child_km2, iou, resid_frac, resid_km2, area_diff_pct]``.
    """
    print(f"\n=== (2) JOIN GEOMETRY: {label} ===", flush=True)
    child_d = child.dissolve(by=keys, as_index=False)[keys + ["geometry"]].to_crs(EQUAL_AREA_CRS)
    parent_m = parent[keys + ["geometry"]].to_crs(EQUAL_AREA_CRS)
    merged = parent_m.merge(child_d, on=keys, suffixes=("_p", "_c"), how="outer", indicator=True)

    only_p = int((merged["_merge"] == "left_only").sum())
    only_c = int((merged["_merge"] == "right_only").sum())
    both = merged[merged["_merge"] == "both"]
    print(
        f"  parents={len(parent_m)}  child-groups={len(child_d)}  matched={len(both)}  "
        f"parent_only={only_p}  child_only={only_c}",
        flush=True,
    )

    rows = []
    for _, r in both.iterrows():
        gp, gc = r["geometry_p"], r["geometry_c"]
        if gp is None or gc is None or gp.is_empty or gc.is_empty:
            continue
        ap, ac = gp.area, gc.area
        inter = gp.intersection(gc).area
        union = gp.union(gc).area
        iou = inter / union if union else float("nan")
        resid_area = gp.symmetric_difference(gc).area
        rows.append(
            tuple(r[k] for k in keys)
            + (ap / 1e6, ac / 1e6, iou, (resid_area / ap) if ap else float("nan"), resid_area / 1e6)
        )
    df = pd.DataFrame(rows, columns=keys + ["parent_km2", "child_km2", "iou", "resid_frac", "resid_km2"])
    df["area_diff_pct"] = (df["child_km2"] - df["parent_km2"]) / df["parent_km2"] * 100.0

    if df.empty:
        print("  WARNING: no matched units to compare.", flush=True)
        return df

    print(f"  IoU      : min={df.iou.min():.4f}  median={df.iou.median():.4f}  mean={df.iou.mean():.4f}", flush=True)
    for thr in (0.999, 0.99, 0.95, 0.90):
        print(f"      IoU >= {thr:<5}: {int((df.iou >= thr).sum())}/{len(df)}", flush=True)
    print(f"  |area %| : median={df.area_diff_pct.abs().median():.4f}  max={df.area_diff_pct.abs().max():.4f}", flush=True)
    print(f"  residual : max={df.resid_km2.max():.4f} km^2", flush=True)
    worst = df.sort_values("iou").head(10)
    print("  worst 10 by IoU:", flush=True)
    with pd.option_context("display.max_columns", None, "display.width", 200,
                           "display.float_format", lambda v: f"{v:.4f}"):
        print(worst[keys + ["parent_km2", "child_km2", "iou", "resid_km2"]].to_string(index=False), flush=True)
    return df


# ---------------------------------------------------------------------------
# Example pickers
# ---------------------------------------------------------------------------

def _pick_example_district(blocks: gpd.GeoDataFrame, override: str | None) -> tuple[str, str]:
    """Choose a (state, district) with a moderate block count for a clear map."""
    if override:
        state, _, dist = override.partition("::")
        return state.strip(), dist.strip()
    counts = blocks.groupby(["state_name", "district_name"]).size()
    mid = counts[(counts >= 8) & (counts <= 25)]
    pick = (mid if not mid.empty else counts).sort_values()
    # median-ish entry of the eligible band
    return tuple(pick.index[len(pick) // 2])  # type: ignore[return-value]


def _pick_example_state(districts: gpd.GeoDataFrame, override: str | None) -> str:
    """Choose a state with a moderate district count for a clear map."""
    if override:
        return override.strip()
    counts = districts.groupby("state_name").size()
    mid = counts[(counts >= 8) & (counts <= 25)]
    pick = (mid if not mid.empty else counts).sort_values()
    return str(pick.index[len(pick) // 2])


# ---------------------------------------------------------------------------
# Figures
# ---------------------------------------------------------------------------

def _nesting_map(ax, children_4326, parent_geom_4326, child_label: str, title: str) -> None:
    """Plot children tiling a parent + union-of-children outline + residual slivers."""
    import matplotlib.pyplot as plt  # noqa: F401  (ensures backend wired)

    children_4326 = children_4326.reset_index(drop=True)
    children_4326.assign(_i=range(len(children_4326))).plot(
        ax=ax, column="_i", cmap="tab20", edgecolor="white", linewidth=0.5, alpha=0.75, legend=False
    )
    parent_series = gpd.GeoSeries([parent_geom_4326], crs=DISPLAY_CRS)
    parent_series.boundary.plot(ax=ax, color="black", linewidth=2.2, zorder=5)

    union = unary_union(list(children_4326.geometry.values))
    gpd.GeoSeries([union], crs=DISPLAY_CRS).boundary.plot(
        ax=ax, color="red", linewidth=1.0, linestyle=(0, (4, 3)), zorder=6
    )
    # residual slivers (should be ~empty for consistent data) -> bright magenta fill
    resid = parent_geom_4326.symmetric_difference(union)
    if not resid.is_empty:
        gpd.GeoSeries([resid], crs=DISPLAY_CRS).plot(ax=ax, color="magenta", zorder=7)

    ax.set_title(title, fontsize=10)
    ax.set_axis_off()
    from matplotlib.lines import Line2D
    ax.legend(
        handles=[
            Line2D([0], [0], color="black", lw=2.2, label="parent boundary"),
            Line2D([0], [0], color="red", lw=1.0, ls="--", label=f"union of {child_label}"),
            Line2D([0], [0], color="magenta", lw=6, label="residual (sym-diff)"),
        ],
        loc="lower left", fontsize=7, framealpha=0.9,
    )


def make_figures(
    figures_dir: Path,
    states: gpd.GeoDataFrame,
    districts: gpd.GeoDataFrame,
    blocks: gpd.GeoDataFrame,
    df_blk: pd.DataFrame,
    df_dist: pd.DataFrame,
    example_district: tuple[str, str],
    example_state: str,
) -> list[Path]:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    figures_dir.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []

    # --- Fig 1: example district (blocks -> district) ---
    es, ed = example_district
    sub = blocks[(blocks.state_name == es) & (blocks.district_name == ed)]
    dpoly = districts[(districts.state_name == es) & (districts.district_name == ed)]
    if not sub.empty and not dpoly.empty:
        row = df_blk[(df_blk.state_name == es) & (df_blk.district_name == ed)]
        iou = float(row.iou.iloc[0]) if not row.empty else float("nan")
        resid = float(row.resid_km2.iloc[0]) if not row.empty else float("nan")
        fig, ax = plt.subplots(figsize=(7, 7))
        _nesting_map(
            ax, sub, unary_union(list(dpoly.geometry.values)), "blocks",
            f"{es} / {ed}\n{len(sub)} blocks  |  IoU={iou:.4f}  residual={resid:.3f} km²",
        )
        out = figures_dir / "example_district_nesting.png"
        fig.tight_layout(); fig.savefig(out, dpi=150); plt.close(fig)
        written.append(out)

    # --- Fig 2: example state (districts -> state) ---
    sub = districts[districts.state_name == example_state]
    spoly = states[states.state_name == example_state]
    if not sub.empty and not spoly.empty:
        row = df_dist[df_dist.state_name == example_state]
        iou = float(row.iou.iloc[0]) if not row.empty else float("nan")
        resid = float(row.resid_km2.iloc[0]) if not row.empty else float("nan")
        fig, ax = plt.subplots(figsize=(7, 7))
        _nesting_map(
            ax, sub, unary_union(list(spoly.geometry.values)), "districts",
            f"{example_state}\n{len(sub)} districts  |  IoU={iou:.4f}  residual={resid:.3f} km²",
        )
        out = figures_dir / "example_state_nesting.png"
        fig.tight_layout(); fig.savefig(out, dpi=150); plt.close(fig)
        written.append(out)

    # --- Fig 3: area-parity scatter (ALL units) ---
    fig, ax = plt.subplots(figsize=(6.5, 6.5))
    for d, lbl, mk in ((df_dist, "districts→state", "s"), (df_blk, "blocks→district", "o")):
        if not d.empty:
            ax.scatter(d.parent_km2, d.child_km2, s=14, marker=mk, alpha=0.5, label=lbl)
    lim_lo = min(df_blk.parent_km2.min(), df_dist.parent_km2.min()) * 0.8
    lim_hi = max(df_blk.parent_km2.max(), df_dist.parent_km2.max()) * 1.2
    ax.plot([lim_lo, lim_hi], [lim_lo, lim_hi], color="black", ls="--", lw=1, label="y = x")
    ax.set_xscale("log"); ax.set_yscale("log")
    ax.set_xlabel("parent polygon area (km², log)")
    ax.set_ylabel("union-of-children area (km², log)")
    max_err = max(df_blk.area_diff_pct.abs().max(), df_dist.area_diff_pct.abs().max())
    ax.set_title(f"Area parity: children dissolve vs parent\nmax |area error| = {max_err:.4f}%")
    ax.legend(fontsize=8); ax.grid(True, which="both", ls=":", alpha=0.4)
    out = figures_dir / "area_parity_scatter.png"
    fig.tight_layout(); fig.savefig(out, dpi=150); plt.close(fig)
    written.append(out)

    # --- Fig 4: IoU consistency bands ---
    bands = [(-0.01, 0.90, "<0.90"), (0.90, 0.95, "0.90–0.95"),
             (0.95, 0.99, "0.95–0.99"), (0.99, 0.999, "0.99–0.999"), (0.999, 1.01, "≥0.999")]
    labels = [b[2] for b in bands]
    blk_counts = [int(((df_blk.iou > lo) & (df_blk.iou <= hi)).sum()) for lo, hi, _ in bands]
    dist_counts = [int(((df_dist.iou > lo) & (df_dist.iou <= hi)).sum()) for lo, hi, _ in bands]
    import numpy as np
    x = np.arange(len(labels)); w = 0.38
    fig, ax = plt.subplots(figsize=(7.5, 4.5))
    b1 = ax.bar(x - w / 2, blk_counts, w, label=f"blocks→district (n={len(df_blk)})")
    b2 = ax.bar(x + w / 2, dist_counts, w, label=f"districts→state (n={len(df_dist)})")
    ax.bar_label(b1, fontsize=7); ax.bar_label(b2, fontsize=7)
    ax.set_xticks(x); ax.set_xticklabels(labels)
    ax.set_ylabel("number of units"); ax.set_yscale("symlog")
    ax.set_title(f"Join-consistency (IoU) distribution\nmin IoU: blocks={df_blk.iou.min():.4f}, districts={df_dist.iou.min():.4f}")
    ax.legend(fontsize=8)
    out = figures_dir / "iou_consistency_bands.png"
    fig.tight_layout(); fig.savefig(out, dpi=150); plt.close(fig)
    written.append(out)

    return written


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_cli() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument(
        "--geojson-dir", type=Path, default=None,
        help="Directory holding states_4326/districts_4326/blocks_4326.geojson "
             "(default: configured IRT data dir).",
    )
    p.add_argument(
        "--figures-dir", type=Path, default=None,
        help="If set, write diagnostic figures (PNG) into this directory.",
    )
    p.add_argument("--example-district", default=None,
                   help='Example district for the map as "State::District" (default: auto-pick).')
    p.add_argument("--example-state", default=None,
                   help="Example state for the map (default: auto-pick).")
    p.add_argument("--min-iou", type=float, default=0.999,
                   help="Fail (exit 1) if any unit's IoU is below this (default 0.999).")
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_cli().parse_args(argv)
    paths = _resolve_paths(args.geojson_dir)

    print("LOADING", flush=True)
    states = load_layer(paths["states"], "states")
    districts = load_layer(paths["districts"], "districts")
    blocks = load_layer(paths["blocks"], "blocks")

    naming_problems = check_naming(states, districts, blocks)
    df_blk = join_consistency(districts, blocks, DISTRICT_KEYS, "blocks → district")
    df_dist = join_consistency(states, districts, STATE_KEYS, "districts → state")

    if args.figures_dir is not None:
        ex_d = _pick_example_district(blocks, args.example_district)
        ex_s = _pick_example_state(districts, args.example_state)
        print(f"\n=== FIGURES ===\n  example district: {ex_d[0]} / {ex_d[1]}\n  example state   : {ex_s}", flush=True)
        written = make_figures(args.figures_dir, states, districts, blocks, df_blk, df_dist, ex_d, ex_s)
        for w in written:
            print(f"  wrote {w}", flush=True)

    # --- verdict ---
    print("\n=== VERDICT ===", flush=True)
    geom_ok = (not df_blk.empty and df_blk.iou.min() >= args.min_iou) and \
              (not df_dist.empty and df_dist.iou.min() >= args.min_iou)
    if naming_problems:
        for prob in naming_problems:
            print(f"  NAMING FAIL: {prob}", flush=True)
    else:
        print("  NAMING: OK (clean block→district→state mapping)", flush=True)
    if geom_ok:
        print(f"  GEOMETRY: OK (all units IoU >= {args.min_iou})", flush=True)
    else:
        print(f"  GEOMETRY FAIL: some units IoU < {args.min_iou}", flush=True)

    return 0 if (geom_ok and not naming_problems) else 1


if __name__ == "__main__":
    sys.exit(main())
