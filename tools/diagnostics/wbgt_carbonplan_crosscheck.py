"""Cross-check IRT's published WBGT day counts against CarbonPlan extreme-heat v1.0.

CarbonPlan's ``extreme-heat`` dataset (MIT code, CC BY 4.0 data) is built on the
same NEX-GDDP-CMIP6 archive IRT uses, which makes it the closest available
external reference for IRT's shipped WBGT day-count metrics.

What this script does and does not establish
--------------------------------------------
It compares **distributions**, not places. Agreement between two national means
is a weak test: two distributions can share a mean and still disagree at every
location. So the report here is built on the full quantile curve and on the
shape statistics that a mean hides.

A per-region **spatial** correlation is *not* produced, and this is deliberate
rather than an oversight. CarbonPlan keys its regions by Climate Impact Lab
``hierid`` (``IND.<state>.<district>.<region>``); the published CSVs and summary
Zarrs carry no coordinates, and the 592 distinct district-level prefixes do not
line up with IRT's 784 LGD districts. A name-based join across that gap would
manufacture agreement or disagreement out of matching errors. Unblocking it
requires the CIL impact-region geometry, after which the join should be done by
point-in-polygon against IRT's own boundaries, never by name.

Two differences are stated in the report rather than corrected for, because
correcting for either would require rerunning one of the two products:

* **Window.** CarbonPlan's "historical" is ``slice("1985", "2014")``
  (``notebooks/09_summarize.ipynb``); IRT's is 1990-2010.
* **Driver.** CarbonPlan evaluates WBGT at ``tasmax`` with RH computed at
  ``tasmax``; IRT evaluates its formulas on daily-**mean** ``tas`` and ``hurs``.
  That difference is quantified directly, against physical references, by
  ``tools/diagnostics/wbgt_deployed_vs_reference.py``.

Usage
-----
    python -m tools.diagnostics.wbgt_carbonplan_crosscheck --dry-run
    python -m tools.diagnostics.wbgt_carbonplan_crosscheck
"""

from __future__ import annotations

import argparse
import glob
import json
import sys
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

# --------------------------------------------------------------------------
# Constants
# --------------------------------------------------------------------------

CARBONPLAN_CSV_TEMPLATE = (
    "https://carbonplan-climate-impacts.s3.us-west-2.amazonaws.com/"
    "extreme-heat/v1.0/outputs/csv/carbonplan-extreme-heat-{stat}-WBGT-{exposure}.csv"
)

#: The only threshold both products publish. CarbonPlan offers 29 / 30.5 / 32 /
#: 35 degC; IRT offers 28 / 30 / 32. Comparing anything else would be comparing
#: different questions.
COMMON_STAT = "days-over-32-degC"

CARBONPLAN_HISTORICAL_COL = "days over 32 degC - CarbonPlan - historical"

#: CarbonPlan's historical window, pinned from notebooks/09_summarize.ipynb.
CARBONPLAN_HISTORICAL_WINDOW = "1985-2014"

#: IRT's historical window as published in processed_optimised.
IRT_HISTORICAL_WINDOW = "1990-2010"

IRT_PROCESSED_TEMPLATE = (
    "{root}/{slug}/masters/admin/district/*.parquet"
)

#: (label, CarbonPlan exposure, IRT slug, IRT value column)
COMPARISON_PAIRS: tuple[tuple[str, str, str, str], ...] = (
    (
        "shade",
        "shade",
        "wbgt_shade_stull_days_ge_32",
        "wbgt_shade_stull_days_ge_32_days",
    ),
    (
        "sun/outdoor",
        "sun",
        "swbgt_empirical_days_ge_32",
        "swbgt_empirical_days_ge_32_days",
    ),
)

QUANTILES = (0.0, 0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99, 1.0)

DEFAULT_OUT_DIR = Path("docs/diagnostics/wbgt_carbonplan_crosscheck")
DEFAULT_CACHE_DIR = Path("scratch/wbgt_carbonplan_cache")


# --------------------------------------------------------------------------
# Data loading
# --------------------------------------------------------------------------


def load_carbonplan_india(
    exposure: str,
    *,
    cache_dir: Path | None,
    verbose: bool = True,
) -> pd.Series:
    """Return CarbonPlan's historical days-over-32C for the Indian regions.

    Only rows whose ``hierid`` begins with ``IND`` are kept. Those rows carry no
    ``ID_HDC_G0``, i.e. they are the climatically-similar *regions*, not the
    urban-centre subset -- so this comparison is not urban-biased.
    """

    url = CARBONPLAN_CSV_TEMPLATE.format(stat=COMMON_STAT, exposure=exposure)
    cache_path = (
        None if cache_dir is None else cache_dir / f"carbonplan-{exposure}.csv"
    )

    if cache_path is not None and cache_path.exists():
        frame = pd.read_csv(cache_path)
        if verbose:
            print(f"  CarbonPlan {exposure}: {len(frame)} rows (cached)")
    else:
        frame = pd.read_csv(url)
        if verbose:
            print(f"  CarbonPlan {exposure}: {len(frame)} rows (downloaded)")
        if cache_path is not None:
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            frame.to_csv(cache_path, index=False)

    if CARBONPLAN_HISTORICAL_COL not in frame.columns:
        raise RuntimeError(
            f"Expected column {CARBONPLAN_HISTORICAL_COL!r}; got {list(frame.columns)}"
        )

    hierid = frame["hierid"].astype("string").fillna("")
    india = frame.loc[hierid.str.startswith("IND")].copy()
    india["hierid"] = hierid.loc[india.index]

    series = pd.Series(
        india[CARBONPLAN_HISTORICAL_COL].to_numpy(dtype=float),
        index=india["hierid"].to_numpy(),
        name=f"carbonplan_{exposure}",
    )
    if verbose:
        print(f"    -> {len(series)} Indian regions")
    return series


def load_irt_districts(
    slug: str,
    value_col: str,
    processed_root: str,
    *,
    verbose: bool = True,
) -> pd.Series:
    """Return IRT's published historical district values for ``slug``."""

    pattern = IRT_PROCESSED_TEMPLATE.format(root=processed_root, slug=slug)
    files = sorted(glob.glob(pattern))
    if not files:
        raise FileNotFoundError(f"No IRT master parquet files matched {pattern!r}")

    column = f"{value_col}__historical__{IRT_HISTORICAL_WINDOW}__mean"
    frames = [
        pd.read_parquet(path, columns=["district", "state", column]) for path in files
    ]
    combined = pd.concat(frames, ignore_index=True)

    series = pd.Series(
        combined[column].to_numpy(dtype=float),
        index=(combined["state"] + " | " + combined["district"]).to_numpy(),
        name=f"irt_{slug}",
    )
    if verbose:
        print(f"  IRT {slug}: {len(series)} districts from {len(files)} state files")
    return series


# --------------------------------------------------------------------------
# Distribution comparison
# --------------------------------------------------------------------------


def describe(series: pd.Series) -> dict[str, float]:
    """Shape statistics for one distribution, including the tail."""

    values = series.dropna()
    stats: dict[str, float] = {
        "n": float(len(values)),
        "n_zero": float((values == 0.0).sum()),
        "frac_zero": float((values == 0.0).mean()) if len(values) else float("nan"),
        "mean": float(values.mean()),
        "std": float(values.std()),
        "skew": float(values.skew()),
    }
    for q in QUANTILES:
        stats[f"q{q:g}"] = float(values.quantile(q))
    return stats


def compare_distributions(
    carbonplan: pd.Series,
    irt: pd.Series,
) -> dict[str, object]:
    """Compare two distributions without assuming any unit correspondence.

    Because the two products use different spatial units, nothing here is a
    paired statistic. The comparison is between the *shapes* of the two
    distributions and, in particular, between their tails -- which is where a
    threshold-count metric is actually decided.
    """

    cp = carbonplan.dropna()
    ir = irt.dropna()

    result: dict[str, object] = {
        "carbonplan": describe(cp),
        "irt": describe(ir),
    }

    # Quantile-by-quantile ratio: a single number per quantile saying how much
    # higher or lower IRT reads at that point of the distribution.
    ratios: dict[str, float] = {}
    for q in QUANTILES:
        cp_q = float(cp.quantile(q))
        ir_q = float(ir.quantile(q))
        ratios[f"q{q:g}"] = (ir_q / cp_q) if cp_q > 0 else float("nan")
    result["irt_over_carbonplan_by_quantile"] = ratios

    # Mean agreement is reported so it can be seen *next to* the tail, which is
    # the point: the two can differ by an order of magnitude.
    cp_mean = float(cp.mean())
    result["mean_ratio"] = (float(ir.mean()) / cp_mean) if cp_mean > 0 else float("nan")
    result["mean_pct_diff"] = (
        100.0 * (float(ir.mean()) - cp_mean) / cp_mean if cp_mean > 0 else float("nan")
    )

    # Two-sample Kolmogorov-Smirnov: are these plausibly the same distribution?
    try:
        from scipy.stats import ks_2samp

        ks = ks_2samp(cp.to_numpy(), ir.to_numpy())
        result["ks_statistic"] = float(ks.statistic)
        result["ks_pvalue"] = float(ks.pvalue)
    except Exception as exc:  # pragma: no cover - scipy optional
        result["ks_statistic"] = float("nan")
        result["ks_pvalue"] = float("nan")
        result["ks_error"] = str(exc)

    return result


# --------------------------------------------------------------------------
# Reporting
# --------------------------------------------------------------------------


def render_markdown(results: dict[str, dict[str, object]]) -> str:
    """Render the cross-check findings document."""

    lines = [
        "# IRT WBGT day counts vs CarbonPlan extreme-heat v1.0",
        "",
        f"Generated {pd.Timestamp.utcnow():%Y-%m-%d %H:%M} UTC.",
        "",
        "**Metric:** days per year with WBGT >= 32 degC (the only threshold both "
        "products publish).",
        "",
        "## Stated differences, not corrected for",
        "",
        "| | CarbonPlan | IRT |",
        "| --- | --- | --- |",
        f"| Historical window | {CARBONPLAN_HISTORICAL_WINDOW} | {IRT_HISTORICAL_WINDOW} |",
        "| Spatial unit | CIL impact region (2,257 in India) | LGD district (784) |",
        "| Driver | `tasmax`, RH at `tasmax` | daily-mean `tas`, daily-mean `hurs` |",
        "| Bias correction | against ERA5 (notebook 06) | none |",
        "",
        "> The spatial units do not correspond, so **nothing below is a paired "
        "statistic**. These are two distributions over the same country, compared "
        "by shape. A per-place correlation is blocked -- see *What this does not "
        "establish* at the end.",
        "",
    ]

    for label, result in results.items():
        cp = result["carbonplan"]
        ir = result["irt"]
        ratios = result["irt_over_carbonplan_by_quantile"]

        lines += [
            f"## {label}",
            "",
            "| statistic | CarbonPlan | IRT | IRT / CP |",
            "| --- | ---: | ---: | ---: |",
        ]
        for key in ("n", "mean", "std", "skew", "frac_zero"):
            cp_v, ir_v = cp[key], ir[key]
            ratio = (ir_v / cp_v) if cp_v not in (0.0, float("nan")) else float("nan")
            lines.append(
                f"| {key} | {cp_v:.3f} | {ir_v:.3f} | "
                + (f"{ratio:.2f}" if np.isfinite(ratio) else "n/a")
                + " |"
            )

        lines += [
            "",
            "### Quantile curve",
            "",
            "| quantile | CarbonPlan | IRT | IRT / CP |",
            "| --- | ---: | ---: | ---: |",
        ]
        for q in QUANTILES:
            key = f"q{q:g}"
            ratio = ratios[key]
            lines.append(
                f"| {q:.2f} | {cp[key]:.2f} | {ir[key]:.2f} | "
                + (f"{ratio:.2f}" if np.isfinite(ratio) else "n/a")
                + " |"
            )

        ks_stat = result.get("ks_statistic", float("nan"))
        ks_p = result.get("ks_pvalue", float("nan"))
        lines += [
            "",
            f"Mean agreement: **{result['mean_pct_diff']:+.1f}%** "
            f"(ratio {result['mean_ratio']:.3f}).  ",
            f"Two-sample KS: D = {ks_stat:.3f}, p = {ks_p:.3g}.",
            "",
        ]

    lines += [
        "## What this does not establish",
        "",
        "- **No per-place agreement.** CarbonPlan keys regions by CIL `hierid` "
        "(`IND.<state>.<district>.<region>`); the published CSVs and summary Zarrs "
        "carry no coordinates, and the 592 district-level prefixes do not "
        "correspond to IRT's 784 LGD districts. Unblocking this needs the CIL "
        "impact-region geometry, joined to IRT boundaries by point-in-polygon -- "
        "never by name.",
        "- **A matching mean is not validation.** Read the quantile table, not the "
        "mean row: the tail is where a threshold-count metric is decided, and it is "
        "where these two products diverge most.",
        "- **Neither product is ground truth here.** For a comparison against "
        "physical references rather than against another model's output, see "
        "`docs/diagnostics/wbgt_deployed_vs_reference/`.",
        "",
    ]
    return "\n".join(lines)


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------


def default_processed_root() -> str:
    """Resolve IRT's processed_optimised metrics root."""

    from india_resilience_tool.config.paths import resolve_processed_optimised_root

    # Any slug resolves to <root>/metrics/<slug>; take the parent twice.
    sample = Path(str(resolve_processed_optimised_root("swbgt_empirical_days_ge_32")))
    return str(sample.parent)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Compare IRT's published WBGT >= 32 degC day counts against "
            "CarbonPlan extreme-heat v1.0 by distribution shape."
        )
    )
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    parser.add_argument("--cache-dir", type=Path, default=DEFAULT_CACHE_DIR)
    parser.add_argument(
        "--no-cache", action="store_true", help="Always re-download the CSVs."
    )
    parser.add_argument(
        "--processed-root",
        default=None,
        help="Override the processed_optimised metrics root.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the plan and exit without downloading or writing anything.",
    )
    return parser


def main(argv: Iterable[str] | None = None) -> int:
    args = build_parser().parse_args(list(argv) if argv is not None else None)

    processed_root = args.processed_root or default_processed_root()
    cache_dir = None if args.no_cache else args.cache_dir

    if args.dry_run:
        print("DRY RUN -- nothing downloaded, nothing written.")
        print(f"  metric        : {COMMON_STAT}")
        print(f"  CarbonPlan    : {CARBONPLAN_HISTORICAL_WINDOW}")
        print(f"  IRT           : {IRT_HISTORICAL_WINDOW}")
        print(f"  processed root: {processed_root}")
        print(f"  cache dir     : {cache_dir if cache_dir else '(disabled)'}")
        print(f"  out dir       : {args.out_dir}")
        for label, exposure, slug, _ in COMPARISON_PAIRS:
            print(f"  pair          : {label:12s} CarbonPlan/{exposure} vs IRT/{slug}")
        return 0

    args.out_dir.mkdir(parents=True, exist_ok=True)

    results: dict[str, dict[str, object]] = {}
    for label, exposure, slug, value_col in COMPARISON_PAIRS:
        print(f"[{label}]")
        carbonplan = load_carbonplan_india(exposure, cache_dir=cache_dir)
        irt = load_irt_districts(slug, value_col, processed_root)
        results[label] = compare_distributions(carbonplan, irt)

    report_path = args.out_dir / "README.md"
    report_path.write_text(render_markdown(results), encoding="utf-8")

    json_path = args.out_dir / "distribution_stats.json"
    json_path.write_text(json.dumps(results, indent=2), encoding="utf-8")

    print()
    print(f"Wrote {report_path}")
    print(f"      {json_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
