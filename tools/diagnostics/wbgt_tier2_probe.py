"""Does the Tier-2 sun adjustment actually fix the outdoor WBGT metric?

Stage A of the Tier-2 plan was built in CHG-0538 and never ran, because its
intended driver (ARCO-ERA5) turned out to cost days per site. The six-site
Open-Meteo pull from CHG-0546 left 6 sites x 10 years of hourly ERA5 on disk --
including ``shortwave_radiation`` and ``wind_speed_10m`` -- so the Tier-2 chain
can now be scored against hourly Liljegren for free, with no new download.

The chain under test, exactly as committed in ``wbgt_method_validation``::

    adjustment = -2.1564 - 0.005375 * rsds_max + 1.0424 * sfcWind
    WBGT_sun   = WBGT_shade - adjustment

The question is not "does the mean agree". CHG-0544 and CHG-0550 both showed a
mean that agrees while the distribution does not, because a hot formula and a
cold aggregation cancel. So the chain is scored in four variants that separate
the two error sources, every one of them against the same reference
(``liljegren_daily_max``, hourly physics then the daily peak):

``as_shipped_agg``  shade on daily-MEAN tas/hurs, adjustment from *disaggregated*
                    rsds_max and daily-mean wind. Tier 2 bolted onto today's
                    pipeline, changing nothing about aggregation.
``stage_a_spec``    shade on **tasmax** + daily-mean hurs, same adjustment. What
                    ``tier2_outdoor_wbgt_c`` was actually written to do.
``true_rsds``       ``stage_a_spec`` but fed the TRUE hourly rsds maximum, which
                    removes the solar-disaggregation error.
``ceiling``         hourly shade WBGT then daily max, plus the adjustment on true
                    rsds_max. The best the Tier-2 idea can possibly do, since
                    every input error other than the adjustment itself is gone.

Reading the four in order prices each error source separately: the gap from
``ceiling`` down to ``as_shipped_agg`` is aggregation, and ``ceiling`` itself is
the adjustment model's own ceiling. The shipped ``swbgt_empirical`` rows are
carried through unchanged as the incumbent to beat.

Acceptance is the bar Stage A fixed before any result was seen -- median
absolute bias < 1 C and RMSE < 1.5 C in EVERY regime -- and is applied here
without adjustment.

Reads only ``scratch/wbgt_deployed_vs_reference_cache``. Writes nothing under
``IRT_DATA_DIR``.

    python -m tools.diagnostics.wbgt_tier2_probe --dry-run
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable, Sequence

import numpy as np
import pandas as pd

from tools.diagnostics.wbgt_deployed_vs_reference import (
    DAY_COUNT_THRESHOLDS_C,
    build_daily_frame,
    build_hourly_frame,
    compare_series,
    irt_wbgt_shade_stull_c,
    load_site_hourly,
    parse_years,
    select_sites,
)
from tools.diagnostics.wbgt_method_validation import (
    ACCEPT_MEDIAN_ABS_BIAS_C,
    ACCEPT_RMSE_C,
    ADJ_RSDS_CLIP,
    ADJ_WIND_CLIP,
    DEFAULT_PEAK_LAG_FACTOR,
    daily_peak_rsds_from_mean,
    sun_adjustment_c,
)

DEFAULT_YEARS = "2005-2014"
DEFAULT_OUT_DIR = Path("docs/diagnostics/wbgt_tier2_probe")
DEFAULT_CACHE_DIR = Path("scratch/wbgt_deployed_vs_reference_cache")

REFERENCE = "liljegren_daily_max"

#: candidate column -> label used in the report, in reading order.
CANDIDATES: tuple[tuple[str, str], ...] = (
    ("irt_swbgt_daily_mean_in", "INCUMBENT swbgt_empirical, AS DEPLOYED"),
    ("tier2_as_shipped_agg", "Tier 2 on daily-mean inputs (today's aggregation)"),
    ("tier2_stage_a_spec", "Tier 2 as specified (tasmax-driven shade)"),
    ("tier2_true_rsds", "Tier 2, true hourly rsds_max (no disaggregation error)"),
    ("tier2_ceiling", "Tier 2 CEILING (hourly shade max + true rsds_max)"),
)


def build_tier2_frame(daily: pd.DataFrame, *, lat: float, lon: float) -> pd.DataFrame:
    """Attach every Tier-2 variant, and the adjustment term itself, to ``daily``.

    The adjustment is kept as its own column because a candidate can land on the
    right mean by adding the right amount of heat to the wrong shade value; the
    size of the term is what shows whether that is happening.
    """

    frame = daily.copy()
    days = pd.DatetimeIndex(frame.index)

    rsds_disagg = daily_peak_rsds_from_mean(
        frame["rsds_daily_mean"].to_numpy(dtype=float),
        lat_deg=lat,
        lon_deg=lon,
        days=days,
        peak_lag_factor=DEFAULT_PEAK_LAG_FACTOR,
    )
    frame["rsds_max_disaggregated"] = rsds_disagg
    rsds_true = frame["rsds_true_max"].to_numpy(dtype=float)
    wind = frame["wind_daily_mean_ms"].to_numpy(dtype=float)

    frame["adjustment_disagg_c"] = sun_adjustment_c(rsds_disagg, wind)
    frame["adjustment_true_c"] = sun_adjustment_c(rsds_true, wind)

    hurs_mean = frame["hurs_daily_mean_pct"].to_numpy(dtype=float)
    shade_tasmax = irt_wbgt_shade_stull_c(
        frame["tasmax_c"].to_numpy(dtype=float), hurs_mean
    )
    frame["shade_tasmax_c"] = shade_tasmax

    frame["tier2_as_shipped_agg"] = (
        frame["irt_shade_daily_mean_in"] - frame["adjustment_disagg_c"]
    )
    frame["tier2_stage_a_spec"] = shade_tasmax - frame["adjustment_disagg_c"]
    frame["tier2_true_rsds"] = shade_tasmax - frame["adjustment_true_c"]
    frame["tier2_ceiling"] = (
        frame["irt_shade_hourly_max"] - frame["adjustment_true_c"]
    )
    return frame


def extend_daily(hourly: pd.DataFrame, daily: pd.DataFrame) -> pd.DataFrame:
    """Add the solar and wind daily aggregates the Tier-2 chain needs."""

    grouped = hourly.groupby("local_day")
    extra = pd.DataFrame(
        {
            "rsds_daily_mean": grouped["shortwave_radiation"].mean(),
            "rsds_true_max": grouped["shortwave_radiation"].max(),
            "wind_daily_mean_ms": grouped["wind_speed_10m"].mean(),
        }
    )
    extra.index = pd.to_datetime(extra.index)
    extra.index.name = "local_day"
    return daily.join(extra, how="left")


def score_site(site_name: str, frame: pd.DataFrame) -> list[dict[str, object]]:
    """Score every candidate for one site, and record the adjustment size."""

    rows: list[dict[str, object]] = []
    for column, label in CANDIDATES:
        comparison = compare_series(
            site_name,
            frame[REFERENCE],
            frame[column],
            reference_name=REFERENCE,
            candidate_name=label,
        )
        row = comparison.to_row()
        # Uplift is the negated adjustment: what the chain adds to shade WBGT.
        if column.startswith("tier2_"):
            term = (
                "adjustment_true_c"
                if column in {"tier2_true_rsds", "tier2_ceiling"}
                else "adjustment_disagg_c"
            )
            row["mean_uplift_c"] = float(-frame[term].mean())
        else:
            row["mean_uplift_c"] = np.nan
        rows.append(row)
    return rows


def acceptance_flag(median_abs: float, rmse: float) -> str:
    """Stage A's pre-registered bar, applied verbatim."""

    passed = median_abs < ACCEPT_MEDIAN_ABS_BIAS_C and rmse < ACCEPT_RMSE_C
    return "PASS" if passed else "FAIL"


def render_markdown(rows: pd.DataFrame, *, years: Sequence[int]) -> str:
    """Render the findings document."""

    lines = [
        "# Does the Tier-2 sun adjustment fix the outdoor WBGT metric?",
        "",
        f"Generated {pd.Timestamp.utcnow():%Y-%m-%d %H:%M} UTC.",
        "",
        f"**Window:** {min(years)}-{max(years)}, all months.  ",
        "**Driver:** ERA5 hourly via the Open-Meteo archive, cached by CHG-0546.  ",
        "**Reference:** Liljegren et al. (2008), hourly, then the daily maximum.  ",
        "**Chain under test:** `WBGT_sun = WBGT_shade - "
        "(-2.1564 - 0.005375*rsds_max + 1.0424*sfcWind)`, clipped to "
        f"rsds_max {ADJ_RSDS_CLIP[0]:g}-{ADJ_RSDS_CLIP[1]:g} W/m2 and wind "
        f"{ADJ_WIND_CLIP[0]:g}-{ADJ_WIND_CLIP[1]:g} m/s.",
        "",
        "`bias` is candidate minus reference. `p99 bias` is the same difference",
        "restricted to the hottest 1% of reference days. `uplift` is the mean",
        "degrees the adjustment adds to shade WBGT -- a candidate with a good",
        "bias and a large uplift is cancelling errors, not measuring heat.",
        "",
        "**Acceptance (pre-registered in CHG-0538, not tuned here):** median",
        f"absolute bias < {ACCEPT_MEDIAN_ABS_BIAS_C:g} C **and** RMSE < "
        f"{ACCEPT_RMSE_C:g} C, in every regime.",
        "",
        "## Scores",
        "",
        "| site | candidate | n | bias C | med abs C | RMSE C | r | p99 bias C "
        "| uplift C | verdict |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | :--- |",
    ]
    for _, r in rows.iterrows():
        uplift = "--" if pd.isna(r["mean_uplift_c"]) else f"{r['mean_uplift_c']:+.2f}"
        lines.append(
            f"| {r['site']} | {r['candidate']} | {int(r['n_days'])} "
            f"| {r['bias_c']:+.2f} | {r['median_abs_bias_c']:.2f} "
            f"| {r['rmse_c']:.2f} | {r['pearson_r']:.3f} "
            f"| {r['p99_bias_c']:+.2f} | {uplift} | {r['verdict']} |"
        )

    lines += ["", "## Day counts at the shipped thresholds", ""]
    header = "| site | candidate |" + "".join(
        f" >={t:g} ref | >={t:g} cand |" for t in DAY_COUNT_THRESHOLDS_C
    )
    lines.append(header)
    lines.append("| --- | --- |" + " ---: | ---: |" * len(DAY_COUNT_THRESHOLDS_C))
    for _, r in rows.iterrows():
        cells = "".join(
            f" {int(r[f'days_ge_{t:g}_ref'])} | {int(r[f'days_ge_{t:g}_cand'])} |"
            for t in DAY_COUNT_THRESHOLDS_C
        )
        lines.append(f"| {r['site']} | {r['candidate']} |{cells}")

    lines += [
        "",
        "## Reading this table",
        "",
        "- The four Tier-2 rows differ only in how much input error is removed.",
        "  Compare them downward: `CEILING` is the adjustment model judged on",
        "  perfect inputs, and everything above it is the cost of an input",
        "  approximation the production pipeline would actually make.",
        "- A row that passes on `bias` while `r` is low and `uplift` is large is",
        "  the sWBGT failure mode repeating: the right average heat added to the",
        "  wrong day-to-day signal.",
        "- `p99 bias` decides the shipped `*_days_ge_*` slugs. A tail bias with",
        "  the opposite sign to the mean bias means threshold counts will be",
        "  wrong even where the annual mean looks right.",
    ]
    return "\n".join(lines) + "\n"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--years", default=DEFAULT_YEARS)
    parser.add_argument("--sites", default=None, help="comma-separated site names")
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR)
    parser.add_argument("--cache-dir", type=Path, default=DEFAULT_CACHE_DIR)
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main(argv: Iterable[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    years = parse_years(args.years)
    sites = select_sites(args.sites)

    if args.dry_run:
        print(f"would read  {args.cache_dir} ({len(sites)} sites x {len(years)} years)")
        print(f"would write {args.out_dir / 'README.md'}")
        print(f"would write {args.out_dir / 'per_site.csv'}")
        for _, label in CANDIDATES:
            print(f"  candidate: {label}")
        return 0

    rows: list[dict[str, object]] = []
    for site in sites:
        raw = load_site_hourly(site, years, cache_dir=args.cache_dir)
        hourly = build_hourly_frame(site, raw)
        daily = extend_daily(hourly, build_daily_frame(hourly))
        frame = build_tier2_frame(daily, lat=site.lat, lon=site.lon)
        rows.extend(score_site(site.name, frame))
        print(f"  {site.name}: {len(frame)} days scored")

    table = pd.DataFrame(rows)
    table["verdict"] = [
        acceptance_flag(m, r)
        for m, r in zip(table["median_abs_bias_c"], table["rmse_c"])
    ]

    args.out_dir.mkdir(parents=True, exist_ok=True)
    table.to_csv(args.out_dir / "per_site.csv", index=False)
    (args.out_dir / "README.md").write_text(
        render_markdown(table, years=years), encoding="utf-8"
    )
    print(f"wrote {args.out_dir / 'per_site.csv'}")
    print(f"wrote {args.out_dir / 'README.md'}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
