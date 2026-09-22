"""Spatial maps of the gridded ERA5 WBGT method comparison.

Reads ``per_cell.csv`` written by ``wbgt_era5_grid_compare`` and renders the
fields and biases as maps, so that the spatial structure behind the summary
scores is visible rather than collapsed into a single mean.

Four figures:

``fields.png``      the eight WBGT fields (IRT shade / IRT sWBGT / Bernard /
                    Liljegren, each as day-mean and day-peak) on one shared
                    colour scale, so the level differences between the shade
                    and outdoor families are readable off the panels.
``deployed.png``    the two AS-DEPLOYED IRT fields -- the formulas evaluated on
                    daily-MEAN inputs, which is what ``heat_stress_gridfirst``
                    actually ships -- beside their peak references.
``bias.png``        the seven scored comparisons as candidate-minus-reference
                    maps on a shared symmetric diverging scale. This is the
                    figure that shows the bias is not spatially uniform.
``drivers.png``     Ta day-mean, Ta day-peak, the peak-minus-mean swing and
                    day-mean RH, plus a scatter of the swing against the
                    as-deployed shade bias.

Reads only ``per_cell.csv`` and, if present, the state boundary layer under
``DATA_DIR`` for orientation. Writes nothing under ``IRT_DATA_DIR``.

    python -m tools.diagnostics.wbgt_era5_grid_maps --dry-run
"""

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import xarray as xr

from india_resilience_tool.config.paths import DATA_DIR
from tools.diagnostics.wbgt_era5_grid_compare import COMPARISON_PLAN

DEFAULT_IN_CSV = Path("docs/diagnostics/wbgt_era5_grid/per_cell.csv")
DEFAULT_OUT_DIR = Path("docs/diagnostics/wbgt_era5_grid")

STATES_GEOJSON = DATA_DIR / "states_4326.geojson"

#: column -> panel title, for the absolute-field figure
FIELD_PANELS = (
    ("irt_shade_day_mean", "IRT shade, day mean"),
    ("irt_shade_day_peak", "IRT shade, day peak"),
    ("bernard_day_mean", "Bernard indoor, day mean"),
    ("bernard_day_peak", "Bernard indoor, day peak"),
    ("irt_swbgt_day_mean", "IRT sWBGT, day mean"),
    ("irt_swbgt_day_peak", "IRT sWBGT, day peak"),
    ("liljegren_day_mean", "Liljegren outdoor, day mean"),
    ("liljegren_day_peak", "Liljegren outdoor, day peak"),
)

DEPLOYED_PANELS = (
    ("irt_shade_deployed", "IRT shade AS DEPLOYED\n(formula on daily-mean inputs)"),
    ("bernard_day_peak", "Bernard indoor, day peak\n(its reference)"),
    ("irt_swbgt_deployed", "IRT sWBGT AS DEPLOYED\n(formula on daily-mean inputs)"),
    ("liljegren_day_peak", "Liljegren outdoor, day peak\n(its reference)"),
)

DRIVER_PANELS = (
    ("t_c_day_mean", "Ta day mean", "degC"),
    ("t_c_day_peak", "Ta day peak", "degC"),
    ("ta_peak_minus_mean", "Ta peak - day mean\n(diurnal swing proxy)", "degC"),
    ("rh_day_mean", "RH day mean", "%"),
)


def load_grid(csv_path: Path) -> xr.Dataset:
    """Read ``per_cell.csv`` and pivot it back onto the (lat, lon) grid.

    The comparison writes one row per cell per complete local day. Only one
    day survives the 24-step completeness filter for a two-UTC-day fetch, so a
    later multi-day run would need a day selector here; this asserts the
    assumption rather than silently averaging across days.
    """
    df = pd.read_csv(csv_path)
    days = sorted(df["local_day"].unique())
    if len(days) != 1:
        raise ValueError(
            "per_cell.csv holds more than one complete local day "
            f"({days}); these maps are written for a single day."
        )
    df["ta_peak_minus_mean"] = df["t_c_day_peak"] - df["t_c_day_mean"]
    for cand, ref, label in COMPARISON_PLAN:
        df[f"bias::{cand}::{ref}"] = df[cand] - df[ref]
    ds = df.set_index(["latitude", "longitude"]).to_xarray()
    ds.attrs["local_day"] = str(days[0])
    return ds


def load_states():
    """State outlines for orientation, or ``None`` if the layer is absent.

    The maps are legible without it -- it is an overlay, not data -- so a
    missing boundary file degrades to no outline rather than failing a run
    that has nothing to do with boundaries.
    """
    if not STATES_GEOJSON.exists():
        return None
    try:
        import geopandas as gpd

        return gpd.read_file(STATES_GEOJSON)
    except Exception as exc:  # pragma: no cover - overlay is optional
        print(f"state overlay unavailable ({exc}); drawing without it")
        return None


def draw(ax, ds: xr.Dataset, column: str, title: str, states, **kwargs) -> object:
    """Paint one field as a pcolormesh panel with the state outline on top."""
    mesh = ds[column].plot.pcolormesh(
        ax=ax, add_colorbar=False, add_labels=False, **kwargs
    )
    if states is not None:
        states.boundary.plot(ax=ax, color="white", linewidth=0.6, zorder=3)
    ax.set_xlim(float(ds["longitude"].min()) - 0.125, float(ds["longitude"].max()) + 0.125)
    ax.set_ylim(float(ds["latitude"].min()) - 0.125, float(ds["latitude"].max()) + 0.125)
    ax.set_title(title, fontsize=9)
    ax.set_aspect("equal")
    ax.tick_params(labelsize=7)
    return mesh


def figure_fields(ds: xr.Dataset, states, out_path: Path) -> None:
    cols = [c for c, _ in FIELD_PANELS]
    vmin = float(min(ds[c].min() for c in cols))
    vmax = float(max(ds[c].max() for c in cols))

    fig, axes = plt.subplots(2, 4, figsize=(15, 8), constrained_layout=True)
    for ax, (col, title) in zip(axes.ravel(), FIELD_PANELS):
        mesh = draw(ax, ds, col, title, states, cmap="inferno", vmin=vmin, vmax=vmax)
    fig.colorbar(mesh, ax=axes, shrink=0.8, label="WBGT (degC)")
    fig.suptitle(
        f"WBGT fields, ERA5 hourly, local day {ds.attrs['local_day']} (IST)\n"
        "top row: shade family | bottom row: outdoor family | one shared colour scale",
        fontsize=11,
    )
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


def figure_deployed(ds: xr.Dataset, states, out_path: Path) -> None:
    cols = [c for c, _ in DEPLOYED_PANELS]
    vmin = float(min(ds[c].min() for c in cols))
    vmax = float(max(ds[c].max() for c in cols))

    fig, axes = plt.subplots(1, 4, figsize=(16, 4.6), constrained_layout=True)
    for ax, (col, title) in zip(axes.ravel(), DEPLOYED_PANELS):
        mesh = draw(ax, ds, col, title, states, cmap="inferno", vmin=vmin, vmax=vmax)
    fig.colorbar(mesh, ax=axes, shrink=0.85, label="WBGT (degC)")
    fig.suptitle(
        "What IRT ships, beside what it should be compared against "
        f"-- local day {ds.attrs['local_day']} (IST)",
        fontsize=11,
    )
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


def figure_bias(ds: xr.Dataset, states, out_path: Path) -> None:
    keys = [f"bias::{c}::{r}" for c, r, _ in COMPARISON_PLAN]
    span = float(max(abs(ds[k]).max() for k in keys))

    fig, axes = plt.subplots(2, 4, figsize=(15, 8), constrained_layout=True)
    flat = axes.ravel()
    for ax, key, (cand, ref, label) in zip(flat, keys, COMPARISON_PLAN):
        mean = float(ds[key].mean())
        lo, hi = float(ds[key].min()), float(ds[key].max())
        mesh = draw(
            ax,
            ds,
            key,
            f"{label}\nmean {mean:+.2f} degC  (cells {lo:+.2f} .. {hi:+.2f})",
            states,
            cmap="RdBu_r",
            vmin=-span,
            vmax=span,
        )
    for ax in flat[len(keys) :]:
        ax.axis("off")
    fig.colorbar(mesh, ax=axes, shrink=0.8, label="candidate - reference (degC)")
    fig.suptitle(
        "Method bias, candidate minus reference "
        f"-- local day {ds.attrs['local_day']} (IST), one shared symmetric scale",
        fontsize=11,
    )
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


def figure_drivers(ds: xr.Dataset, states, out_path: Path) -> None:
    fig, axes = plt.subplots(1, 5, figsize=(19, 4.2), constrained_layout=True)
    for ax, (col, title, unit) in zip(axes[:4], DRIVER_PANELS):
        mesh = draw(ax, ds, col, title, states, cmap="viridis")
        fig.colorbar(mesh, ax=ax, shrink=0.85, label=unit)

    swing = ds["ta_peak_minus_mean"].to_numpy().ravel()
    bias = ds["bias::irt_shade_deployed::bernard_day_peak"].to_numpy().ravel()
    good = np.isfinite(swing) & np.isfinite(bias)
    r = float(np.corrcoef(swing[good], bias[good])[0, 1])

    ax = axes[4]
    ax.scatter(swing[good], bias[good], s=10, alpha=0.7, color="#b2182b")
    ax.set_xlabel("Ta peak - day mean (degC)", fontsize=8)
    ax.set_ylabel("as-deployed shade bias (degC)", fontsize=8)
    ax.set_title(f"Aggregation bias tracks the\ndiurnal swing (r = {r:+.2f})", fontsize=9)
    ax.tick_params(labelsize=7)
    ax.grid(alpha=0.3)

    fig.suptitle(
        f"Drivers and why the bias varies in space -- local day {ds.attrs['local_day']} (IST)",
        fontsize=11,
    )
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


FIGURES_MD = """# Spatial maps of the gridded ERA5 WBGT comparison

Local day {day} (IST). Rendered by `tools/diagnostics/wbgt_era5_grid_maps.py` from
`per_cell.csv`; the scores these illustrate are in `README.md`. Nothing here is
recomputed, so the maps cannot drift from the table.

## The fields

![WBGT fields](fields.png)

All eight on one colour scale. The outdoor family sits visibly hotter than the
shade family, and the day-peak panels hotter than the day-mean panels -- the two
axes the comparison separates.

## What IRT actually ships

![As deployed](deployed.png)

The AS-DEPLOYED panels are the IRT formulas evaluated on daily-**mean** Ta and RH,
which is what `heat_stress_gridfirst` receives. Each sits beside the day-peak
reference it should be graded against.

## Method bias

![Bias](bias.png)

Candidate minus reference, one shared symmetric scale. The two `formula only`
shade panels are near-blank at this scale: that is the finding, not a rendering
fault, so every panel carries its own mean and per-cell range. Read across:

- the shade formula reproduces Bernard to within hundredths of a degree;
- evaluating that same formula on daily means costs about two degrees, and the
  cost is **not spatially uniform**;
- sWBGT's small mean outdoor bias is two larger errors cancelling -- a
  radiation-blind formula reading hot, and daily-mean aggregation reading cold --
  which the panel shows as a sign change across the box.

## Drivers

![Drivers](drivers.png)

The as-deployed shade bias tracks the diurnal swing: cells that swing further
lose more, because their daily mean sits further below their peak. That is why
the bias is a **ranking** problem for a frozen national CDF ruler and not only a
level problem.
"""


def write_index(ds: xr.Dataset, out_path: Path) -> None:
    """Write the figure index beside the PNGs.

    Kept separate from the comparison's own ``README.md``, which is regenerated
    on every compare run and would overwrite anything appended to it.
    """
    out_path.write_text(FIGURES_MD.format(day=ds.attrs["local_day"]), encoding="utf-8")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--in-csv", type=Path, default=DEFAULT_IN_CSV)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    targets = ["fields.png", "deployed.png", "bias.png", "drivers.png", "FIGURES.md"]
    if args.dry_run:
        print(f"would read  {args.in_csv}")
        print(f"would read  {STATES_GEOJSON} (optional overlay)")
        for name in targets:
            print(f"would write {args.out_dir / name}")
        return 0

    ds = load_grid(args.in_csv)
    states = load_states()
    args.out_dir.mkdir(parents=True, exist_ok=True)

    figure_fields(ds, states, args.out_dir / "fields.png")
    figure_deployed(ds, states, args.out_dir / "deployed.png")
    figure_bias(ds, states, args.out_dir / "bias.png")
    figure_drivers(ds, states, args.out_dir / "drivers.png")
    write_index(ds, args.out_dir / "FIGURES.md")

    for name in targets:
        print(f"wrote {args.out_dir / name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
