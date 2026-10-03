"""Spatial maps of the gridded ERA5 WBGT method comparison.

Reads ``per_cell.csv`` written by ``wbgt_era5_grid_compare`` and renders the
fields and biases as maps, so that the spatial structure behind the summary
scores is visible rather than collapsed into a single mean.

Every field figure is a 2x2 laid out **candidate beside its own reference**:
IRT on the left column, the reference method on the right, day mean on the top
row and day max on the bottom. Reading across a row is method error at a fixed
aggregation; reading down a column is aggregation error at a fixed method.

``shade.png``       IRT shade vs Bernard indoor, mean and max.
``outdoor.png``     IRT sWBGT vs Liljegren outdoor, mean and max.
``deployed.png``    the two AS-DEPLOYED IRT fields -- the formulas evaluated on
                    daily-MEAN inputs, which is what ``heat_stress_gridfirst``
                    actually ships -- each beside the day-max reference it
                    should be graded against.
``bias.png``        four candidate-minus-reference maps on one shared symmetric
                    diverging scale: shade mean and max on the top row, outdoor
                    mean and max on the bottom.
``drivers.png``     Ta day-mean, Ta day-max, the max-minus-mean swing and
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

#: (candidate column, reference column, row label) per row of a field figure.
SHADE_ROWS = (
    ("irt_shade_day_mean", "bernard_day_mean", "day mean"),
    ("irt_shade_day_peak", "bernard_day_peak", "day max"),
)

OUTDOOR_ROWS = (
    ("irt_swbgt_day_mean", "liljegren_day_mean", "day mean"),
    ("irt_swbgt_day_peak", "liljegren_day_peak", "day max"),
)

#: same shape, but the left column is the formula on daily-MEAN inputs, so the
#: two columns of a row carry different aggregation labels.
DEPLOYED_ROWS = (
    ("irt_shade_deployed", "bernard_day_peak", ("daily-mean inputs", "day max")),
    ("irt_swbgt_deployed", "liljegren_day_peak", ("daily-mean inputs", "day max")),
)

#: the four biases of the summary figure, in reading order.
BIAS_PLAN = (
    ("irt_shade_day_mean", "bernard_day_mean", "Shade, day mean\nIRT - Bernard indoor"),
    ("irt_shade_day_peak", "bernard_day_peak", "Shade, day max\nIRT - Bernard indoor"),
    ("irt_swbgt_day_mean", "liljegren_day_mean", "Outdoor, day mean\nIRT sWBGT - Liljegren"),
    ("irt_swbgt_day_peak", "liljegren_day_peak", "Outdoor, day max\nIRT sWBGT - Liljegren"),
)

DRIVER_PANELS = (
    ("t_c_day_mean", "Ta day mean", "degC"),
    ("t_c_day_peak", "Ta day max", "degC"),
    ("ta_peak_minus_mean", "Ta max - day mean\n(diurnal swing proxy)", "degC"),
    ("rh_day_mean", "RH day mean", "%"),
)

#: column -> the name used in panel titles.
PRETTY = {
    "irt_shade_day_mean": "IRT shade",
    "irt_shade_day_peak": "IRT shade",
    "irt_shade_deployed": "IRT shade AS DEPLOYED",
    "bernard_day_mean": "Bernard indoor (reference)",
    "bernard_day_peak": "Bernard indoor (reference)",
    "irt_swbgt_day_mean": "IRT sWBGT",
    "irt_swbgt_day_peak": "IRT sWBGT",
    "irt_swbgt_deployed": "IRT sWBGT AS DEPLOYED",
    "liljegren_day_mean": "Liljegren outdoor (reference)",
    "liljegren_day_peak": "Liljegren outdoor (reference)",
}


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
    for cand, ref, label in BIAS_PLAN:
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


def field_scale(ds: xr.Dataset, row_specs) -> tuple[float, float]:
    """Common colour limits across every panel of a set of rows."""
    cols = [c for spec in row_specs for c in spec[:2]]
    return (
        float(min(ds[c].min() for c in cols)),
        float(max(ds[c].max() for c in cols)),
    )


def figure_pairs(
    ds: xr.Dataset,
    states,
    rows,
    out_path: Path,
    suptitle: str,
    vmin: float,
    vmax: float,
) -> None:
    """Render a 2x2 of candidate (left) beside reference (right), one row per
    aggregation, on a shared colour scale so both axes are readable directly."""
    fig, axes = plt.subplots(2, 2, figsize=(9.0, 8.4), constrained_layout=True)
    for (cand, ref, row_label), ax_row in zip(rows, axes):
        labels = row_label if isinstance(row_label, tuple) else (row_label, row_label)
        for ax, col, label in zip(ax_row, (cand, ref), labels):
            mesh = draw(
                ax,
                ds,
                col,
                f"{PRETTY[col]}, {label}\nmean {float(ds[col].mean()):.2f} degC",
                states,
                cmap="inferno",
                vmin=vmin,
                vmax=vmax,
            )
    fig.colorbar(mesh, ax=axes, shrink=0.85, label="WBGT (degC)")
    fig.suptitle(suptitle, fontsize=11)
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


def figure_bias(ds: xr.Dataset, states, out_path: Path) -> None:
    """The four scored biases: shade on the top row, outdoor on the bottom,
    day mean left and day max right, on one shared symmetric scale."""
    keys = [f"bias::{c}::{r}" for c, r, _ in BIAS_PLAN]
    span = float(max(abs(ds[k]).max() for k in keys))

    fig, axes = plt.subplots(2, 2, figsize=(9.0, 8.4), constrained_layout=True)
    for ax, key, (cand, ref, label) in zip(axes.ravel(), keys, BIAS_PLAN):
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
    fig.colorbar(mesh, ax=axes, shrink=0.85, label="IRT - reference (degC)")
    fig.suptitle(
        "Method bias at a fixed aggregation, IRT minus reference\n"
        f"local day {ds.attrs['local_day']} (IST), one shared symmetric scale",
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
    ax.set_xlabel("Ta max - day mean (degC)", fontsize=8)
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

Every field figure is laid out the same way: **IRT on the left, its reference on
the right, day mean on top, day max below**. Read across a row for method error
at a fixed aggregation; read down a column for aggregation error at a fixed
method.

## Shade family

![Shade](shade.png)

IRT shade beside Bernard indoor. The two columns are visually indistinguishable
at both aggregations -- the formula is the method. The top-to-bottom step is the
aggregation gap.

## Outdoor family

![Outdoor](outdoor.png)

IRT sWBGT beside Liljegren outdoor. Here the columns differ: sWBGT carries no
radiation term, so it cannot reproduce the outdoor field cell by cell.

## What IRT actually ships

![As deployed](deployed.png)

The AS-DEPLOYED panels are the IRT formulas evaluated on daily-**mean** Ta and RH,
which is what `heat_stress_gridfirst` receives. Each sits beside the day-max
reference it should be graded against.

## Method bias

![Bias](bias.png)

IRT minus reference, one shared symmetric scale. The two shade panels are
near-blank at this scale: that is the finding, not a rendering fault, so every
panel carries its own mean and per-cell range.

## Drivers

![Drivers](drivers.png)

The as-deployed shade bias tracks the diurnal swing: cells that swing further
lose more, because their daily mean sits further below their max. That is why
the bias is a **ranking** problem for a frozen national CDF ruler and not only a
level problem.
"""


def write_index(ds: xr.Dataset, out_path: Path) -> None:
    """Write the figure index beside the PNGs.

    Kept separate from the comparison's own ``README.md``, which is regenerated
    on every compare run and would overwrite anything appended to it.
    """
    out_path.write_text(FIGURES_MD.format(day=ds.attrs["local_day"]), encoding="utf-8")


#: figures this script owns; anything else matching *.png in the out dir is left
#: alone, but a stale figure from an earlier layout is removed by name.
TARGETS = ("shade.png", "outdoor.png", "deployed.png", "bias.png", "drivers.png", "FIGURES.md")
RETIRED = ("fields.png",)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--in-csv", type=Path, default=DEFAULT_IN_CSV)
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    if args.dry_run:
        print(f"would read  {args.in_csv}")
        print(f"would read  {STATES_GEOJSON} (optional overlay)")
        for name in RETIRED:
            print(f"would remove {args.out_dir / name} (superseded layout)")
        for name in TARGETS:
            print(f"would write {args.out_dir / name}")
        return 0

    ds = load_grid(args.in_csv)
    states = load_states()
    args.out_dir.mkdir(parents=True, exist_ok=True)

    for name in RETIRED:
        stale = args.out_dir / name
        if stale.exists():
            stale.unlink()
            print(f"removed {stale} (superseded layout)")

    # One scale across both family figures so the level gap between the shade
    # and outdoor families stays readable when the two figures sit side by side.
    vmin, vmax = field_scale(ds, SHADE_ROWS + OUTDOOR_ROWS)
    day = ds.attrs["local_day"]

    figure_pairs(
        ds,
        states,
        SHADE_ROWS,
        args.out_dir / "shade.png",
        f"Shade family: IRT vs Bernard indoor -- local day {day} (IST)",
        vmin,
        vmax,
    )
    figure_pairs(
        ds,
        states,
        OUTDOOR_ROWS,
        args.out_dir / "outdoor.png",
        f"Outdoor family: IRT sWBGT vs Liljegren -- local day {day} (IST)",
        vmin,
        vmax,
    )
    figure_pairs(
        ds,
        states,
        DEPLOYED_ROWS,
        args.out_dir / "deployed.png",
        "What IRT ships, beside the day-max reference it should be graded against\n"
        f"local day {day} (IST)",
        vmin,
        vmax,
    )
    figure_bias(ds, states, args.out_dir / "bias.png")
    figure_drivers(ds, states, args.out_dir / "drivers.png")
    write_index(ds, args.out_dir / "FIGURES.md")

    for name in TARGETS:
        print(f"wrote {args.out_dir / name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
