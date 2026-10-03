"""District-fill vs block-fill, side by side, for a handful of states (CHG-0370).

Read-only diagnostic. The frozen Heat Risk ruler makes district scores and block
scores directly comparable, which is what allows a State view to paint blocks on
the same colour scale the national view uses for districts. How much that
actually changes the picture varies enormously by state: most of the within-state
variance already sits *between* districts in Uttar Pradesh, and almost none of it
does in Goa. This page shows that difference rather than asserting it.

It also demonstrates the colourbar question. The fill can be read against the
frozen 0-100 domain (recommended) or against a domain rescaled to whatever is on
screen. The second mode is included precisely so its failure is visible: under
rescaling, a state with a two-point spread is painted across the full ramp.

The ruler is *read*, never refitted: ``cdf_support.csv`` from the national pilot
run is the frozen artifact, one knot per distinct pooled value with its mid-rank
score.

Usage
-----
    python -m tools.diagnostics.build_resolution_comparison \
        --states "Uttar Pradesh,Kerala,Goa" \
        --out docs/diagnostics/heat_risk_pilot/resolution_comparison.html
"""

from __future__ import annotations

import argparse
import html
import json
import sys
from pathlib import Path
from typing import Optional, Sequence

import numpy as np
import pandas as pd

from india_resilience_tool.compute.composite_metrics import _bundle_metric_specs
from india_resilience_tool.config.composite_metrics import get_composite_metric_for_bundle
from india_resilience_tool.config.paths import get_paths_config
from tools.diagnostics.build_heat_risk_frozen_map import (
    CANVAS_WIDTH,
    MISSING_COLOR,
    _geom_path,
    _project,
    ramp_hex,
)
from tools.diagnostics.heat_risk_national_ruler_pilot import (
    BASELINE_REFERENCED_SLUGS,
    BUNDLE_DOMAIN,
    SLICES,
    load_national_long_frame,
)

DEFAULT_STATES = ("Uttar Pradesh", "Kerala", "Goa")
DEFAULT_SUPPORT = Path("docs/diagnostics/heat_risk_pilot/cdf_support.csv")
DEFAULT_OUT = Path("docs/diagnostics/heat_risk_pilot/resolution_comparison.html")
DEFAULT_SIMPLIFY = 0.004  # finer than the national map: these are single-state panels

#: Paths are projected by :func:`_project`, which scales to ``CANVAS_WIDTH``; the
#: viewBox has to match it or the panel clips. On-screen size is set in CSS, and
#: ``preserveAspectRatio`` letterboxes each state into the same box so a long
#: narrow state and a squat one are shown at comparable scale.
PANEL_WIDTH = CANVAS_WIDTH


# ---------------------------------------------------------------------------
# The frozen ruler, read from the pilot artifact
# ---------------------------------------------------------------------------


def load_frozen_support(path: Path) -> dict[str, tuple[np.ndarray, np.ndarray]]:
    """Per-metric ``(knot_values, midrank_scores)`` for the frozen ``cdf`` ruler."""
    frame = pd.read_csv(path)
    frame = frame.loc[frame["ruler"] == "cdf"]
    if frame.empty:
        raise ValueError(f"{path} carries no cdf support rows")
    out: dict[str, tuple[np.ndarray, np.ndarray]] = {}
    for slug, block in frame.groupby("metric_slug", sort=False):
        block = block.sort_values("knot_value")
        out[str(slug)] = (
            block["knot_value"].to_numpy(dtype=float),
            block["midrank_score"].to_numpy(dtype=float),
        )
    return out


def score_against(values: pd.Series, knots: tuple[np.ndarray, np.ndarray]) -> pd.Series:
    """Score a series on the frozen ruler, clamping outside the knot range.

    Blocks legitimately fall outside the district-fitted range — a block can be
    colder or hotter than any district mean — so clamping is expected here and
    was measured at 0.52% of block rows nationally.
    """
    knot_values, knot_scores = knots
    numeric = pd.to_numeric(values, errors="coerce")
    arr = numeric.to_numpy(dtype=float, na_value=np.nan)
    out = np.full(arr.shape, np.nan)
    finite = np.isfinite(arr)
    if finite.any():
        out[finite] = np.clip(np.interp(arr[finite], knot_values, knot_scores), 0.0, 100.0)
    return pd.Series(out, index=numeric.index)


def composite_frame(
    long_frame: pd.DataFrame,
    *,
    slugs: Sequence[str],
    weights: pd.Series,
    support: dict[str, tuple[np.ndarray, np.ndarray]],
) -> pd.Series:
    """Weighted mean of the 9 absolute-half metric scores, renormalized per row."""
    scores = pd.DataFrame(
        {slug: score_against(long_frame[slug], support[slug]) for slug in slugs},
        index=long_frame.index,
    )
    available = scores.notna().mul(weights, axis=1).sum(axis=1)
    return scores.mul(weights, axis=1).sum(axis=1, skipna=True) / available.where(available > 0)


# ---------------------------------------------------------------------------
# Geometry
# ---------------------------------------------------------------------------


def state_shapes(state: str, *, data_dir: Path, simplify: float, precision: int) -> dict:
    """District and block paths for one state, on one shared projection."""
    import geopandas as gpd

    root = Path(data_dir) / "processed_optimised" / "geometry" / "admin"
    layers = {}
    frames = {}
    for level in ("district", "block"):
        path = root / level / f"state={state}.geojson"
        if not path.exists():
            raise FileNotFoundError(f"no {level} geometry for {state!r} at {path}")
        gdf = gpd.read_file(path)
        if gdf.crs is not None and gdf.crs.to_epsg() != 4326:
            gdf = gdf.to_crs(4326)
        if simplify > 0:
            gdf["geometry"] = gdf.geometry.simplify(simplify, preserve_topology=True)
        frames[level] = gdf

    # One projection for both layers, so the thin and thick strokes overlay exactly.
    stacked = np.vstack([f.total_bounds for f in frames.values()])
    bounds = (
        float(stacked[:, 0].min()),
        float(stacked[:, 1].min()),
        float(stacked[:, 2].max()),
        float(stacked[:, 3].max()),
    )
    _, _, height = _project(np.array([bounds[0]]), np.array([bounds[1]]), bounds)

    for level, gdf in frames.items():
        key = f"{level}_key"
        items = []
        for row in gdf.itertuples(index=False):
            path_d = _geom_path(row.geometry, bounds, precision)
            if not path_d:
                continue
            items.append(
                {
                    "k": getattr(row, key, ""),
                    "n": getattr(row, f"{level}_name", ""),
                    "d": path_d,
                }
            )
        layers[level] = items

    # The state silhouette, for the coarse layer of the district panel. Dissolving
    # is not the same as stacking every district path: a shared internal border
    # would otherwise be drawn at the thick weight.
    silhouette = _geom_path(
        frames["district"].geometry.union_all(), bounds, precision
    )

    return {
        "district": layers["district"],
        "block": layers["block"],
        "silhouette": silhouette,
        "height": float(height),
    }


# ---------------------------------------------------------------------------
# Page
# ---------------------------------------------------------------------------


def within_district_share(block_scores: pd.DataFrame) -> dict[str, float]:
    """Share of within-state block variance that lies inside districts, per slice.

    The part a district-resolution map physically cannot show.
    """
    out: dict[str, float] = {}
    for (scenario, period), group in block_scores.groupby(["scenario", "period"], sort=False):
        values = group["comp"].dropna()
        if len(values) < 3:
            continue
        total = float(values.var(ddof=0))
        if not np.isfinite(total) or total == 0.0:
            out[f"{scenario}|{period}"] = 100.0
            continue
        parent = group.loc[values.index].groupby("district_key")["comp"].transform("mean")
        within = float(((values - parent) ** 2).mean())
        out[f"{scenario}|{period}"] = round(100.0 * within / total, 1)
    return out


def build_page(states: list[dict], *, source: Path) -> str:
    payload = {
        "states": states,
        "slices": [list(s) for s in SLICES],
        "ramp": ramp_hex(),
        "missing": MISSING_COLOR,
        "panelWidth": PANEL_WIDTH,
    }
    blob = json.dumps(payload, separators=(",", ":"))
    names = ", ".join(html.escape(s["name"]) for s in states)

    return f"""<title>District Fill vs Block Fill</title>
<style>
  :root {{
    --paper:#fbfcfd; --panel:#eef2f6; --ink:#101922; --muted:#5a6b7b;
    --rule:#ccd6e0; --accent:#12506e; --warn:#a8261f; --stroke:#7a8794;
  }}
  @media (prefers-color-scheme: dark) {{
    :root:not([data-theme="light"]) {{
      --paper:#0d1319; --panel:#161e27; --ink:#e8eef4; --muted:#8ea0b0;
      --rule:#26303b; --accent:#6fb2d6; --warn:#e0736a; --stroke:#8e9ba8;
    }}
  }}
  :root[data-theme="dark"] {{
    --paper:#0d1319; --panel:#161e27; --ink:#e8eef4; --muted:#8ea0b0;
    --rule:#26303b; --accent:#6fb2d6; --warn:#e0736a; --stroke:#8e9ba8;
  }}
  html {{ --sans: ui-sans-serif, system-ui, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
          --mono: ui-monospace, "Cascadia Mono", "SF Mono", Consolas, "Liberation Mono", monospace; }}
  body {{ background:var(--paper); color:var(--ink); font:15px/1.55 var(--sans);
    margin:0; padding:26px clamp(18px,4vw,46px) 60px; }}
  .page {{ max-width:1240px; margin:0 auto; display:flex; flex-direction:column; gap:24px; }}
  .eyebrow {{ font:600 11px/1 var(--mono); letter-spacing:.14em; text-transform:uppercase;
    color:var(--accent); margin:0 0 8px; }}
  h1 {{ font-size:26px; font-weight:620; letter-spacing:-.02em; margin:0 0 6px; text-wrap:balance; }}
  .sub {{ color:var(--muted); font-size:13px; margin:0; max-width:70ch; }}
  .controls {{ display:flex; gap:26px; flex-wrap:wrap; align-items:flex-end;
    border-top:1px solid var(--rule); border-bottom:1px solid var(--rule); padding:14px 0; }}
  label {{ display:block; font:600 10.5px/1 var(--mono); letter-spacing:.12em;
    text-transform:uppercase; color:var(--muted); margin-bottom:6px; }}
  select {{ font:14px var(--mono); padding:8px 11px; border-radius:5px;
    border:1px solid var(--rule); background:var(--panel); color:var(--ink); min-width:170px; }}
  select:focus-visible, input:focus-visible {{ outline:2px solid var(--accent); outline-offset:2px; }}
  .radios {{ display:flex; gap:16px; align-items:center; }}
  .radios span {{ display:flex; align-items:center; gap:6px; font-size:13.5px; }}
  .warnbox {{ background:var(--panel); border-left:3px solid var(--warn); padding:10px 14px;
    font-size:13px; color:var(--muted); display:none; }}
  .warnbox.on {{ display:block; }}
  .warnbox b {{ color:var(--warn); }}

  .state {{ border-top:1px solid var(--rule); padding-top:20px; }}
  .stateHead {{ display:flex; justify-content:space-between; align-items:baseline;
    gap:20px; flex-wrap:wrap; margin-bottom:12px; }}
  .stateHead h2 {{ font-size:19px; font-weight:600; margin:0; letter-spacing:-.01em; }}
  .metric {{ font:12.5px var(--mono); color:var(--muted); }}
  .metric b {{ color:var(--ink); font-size:15px; }}
  .pair {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(300px,1fr)); gap:26px; }}
  .panel h3 {{ font:600 10.5px/1 var(--mono); letter-spacing:.12em; text-transform:uppercase;
    color:var(--muted); margin:0 0 8px; }}
  .panel svg.map {{ width:100%; height:420px; display:block; background:none; }}
  .fine {{ stroke:var(--stroke); stroke-opacity:.45; fill:none; stroke-linejoin:round;
    pointer-events:none; }}
  .coarse {{ stroke:var(--stroke); stroke-opacity:.95; fill:none; stroke-linejoin:round;
    pointer-events:none; }}
  .cell {{ stroke:none; }}
  .scalewrap {{ margin-top:10px; }}
  .scale {{ position:relative; height:13px; border-radius:2px; border:1px solid var(--rule); }}
  .bracket {{ position:absolute; top:-4px; bottom:-4px; border-left:2px solid var(--ink);
    border-right:2px solid var(--ink); }}
  .bracket::after {{ content:""; position:absolute; left:0; right:0; top:-3px;
    border-top:2px solid var(--ink); }}
  .scaleticks {{ display:flex; justify-content:space-between; font:11px var(--mono);
    color:var(--muted); margin-top:4px; }}
  .obs {{ font:11.5px var(--mono); color:var(--muted); margin-top:5px; }}
  .obs b {{ color:var(--ink); }}
  .note {{ font-size:13px; color:var(--muted); max-width:74ch; }}
  .note b {{ color:var(--ink); font-weight:600; }}
</style>

<div class="page">
  <header>
    <p class="eyebrow">Heat Risk &middot; frozen CDF ruler &middot; absolute half</p>
    <h1>What changes when a State view paints blocks</h1>
    <p class="sub">Left: the state as the national map already draws it, districts filled from
      district values. Right: the same state filled from block values. Same ruler, same ramp.
      {names}. Source <code>{html.escape(str(source))}</code>.</p>
  </header>

  <div class="controls">
    <div><label for="scenario">Scenario</label><select id="scenario"></select></div>
    <div><label for="period">Period</label><select id="period"></select></div>
    <div>
      <label>Colour domain</label>
      <div class="radios">
        <span><input type="radio" name="dom" id="dom-fixed" value="fixed" checked>
          <label for="dom-fixed" style="text-transform:none;letter-spacing:0;font:inherit;margin:0;color:inherit">Frozen 0&ndash;100</label></span>
        <span><input type="radio" name="dom" id="dom-auto" value="auto">
          <label for="dom-auto" style="text-transform:none;letter-spacing:0;font:inherit;margin:0;color:inherit">Rescale to what&rsquo;s on screen</label></span>
      </div>
    </div>
  </div>

  <div class="warnbox" id="warn">
    <b>Rescaling is on.</b> Each panel now stretches its own observed range across the full ramp,
    so identical colours no longer mean identical scores &mdash; not between the two panels, not
    between states, and not between one selection and the next. Watch Goa: a two-point spread
    painted from blue to red.
  </div>

  <div id="states"></div>

  <p class="note">The bracket on each colourbar marks the range actually present in that panel.
    Under the frozen domain it is the honest way to show a state that genuinely occupies a narrow
    slice of the national scale &mdash; the fill stays comparable and the reader still sees the
    spread.</p>
</div>

<script>
const D = {blob};
const NS = "http://www.w3.org/2000/svg";
const host = document.getElementById("states");
const scenarioSel = document.getElementById("scenario");
const periodSel = document.getElementById("period");
const warn = document.getElementById("warn");

const periods = {{}};
D.slices.forEach(([sc, pe]) => (periods[sc] = periods[sc] || []).push(pe));
Object.keys(periods).forEach(sc => scenarioSel.add(new Option(sc, sc)));

function color(v, lo, hi) {{
  if (v === undefined || v === null || Number.isNaN(v)) return D.missing;
  const t = hi > lo ? (v - lo) / (hi - lo) : 0.5;
  const i = Math.max(0, Math.min(D.ramp.length - 1, Math.round(t * (D.ramp.length - 1))));
  return D.ramp[i];
}}

// One block per state: heading, two panels, a colourbar under each.
const built = D.states.map(st => {{
  const wrap = document.createElement("section");
  wrap.className = "state";
  wrap.innerHTML = `
    <div class="stateHead">
      <h2>${{st.name}}</h2>
      <div class="metric">${{st.districts.length}} districts &middot; ${{st.blocks.length}} blocks &middot;
        within-district share of variance <b class="wds">&ndash;</b></div>
    </div>
    <div class="pair">
      <div class="panel" data-level="district">
        <h3>District fill &mdash; thick state, thin district</h3>
        <svg class="map" viewBox="0 0 ${{D.panelWidth}} ${{st.height}}" preserveAspectRatio="xMidYMid meet"></svg>
        <div class="scalewrap"><div class="scale"><div class="bracket"></div></div>
          <div class="scaleticks"><span class="t0">0</span><span class="t1">50</span><span class="t2">100</span></div>
          <div class="obs">observed <b class="rng">&ndash;</b></div></div>
      </div>
      <div class="panel" data-level="block">
        <h3>Block fill &mdash; thick district, thin block</h3>
        <svg class="map" viewBox="0 0 ${{D.panelWidth}} ${{st.height}}" preserveAspectRatio="xMidYMid meet"></svg>
        <div class="scalewrap"><div class="scale"><div class="bracket"></div></div>
          <div class="scaleticks"><span class="t0">0</span><span class="t1">50</span><span class="t2">100</span></div>
          <div class="obs">observed <b class="rng">&ndash;</b></div></div>
      </div>
    </div>`;
  host.appendChild(wrap);

  const panels = {{}};
  wrap.querySelectorAll(".panel").forEach(p => {{
    const level = p.dataset.level;
    const svg = p.querySelector("svg");
    // fill layer = this view's unit; outline layer = the coarser unit, drawn on top
    const fillUnits = level === "district" ? st.districts : st.blocks;
    const coarse = level === "district" ? st.outline : st.districts;
    const cells = fillUnits.map(u => {{
      const el = document.createElementNS(NS, "path");
      el.setAttribute("d", u.d);
      el.setAttribute("class", "cell");
      el.setAttribute("stroke", "var(--stroke)");
      el.setAttribute("stroke-opacity", ".45");
      el.setAttribute("stroke-width", level === "district" ? "0.7" : "0.45");
      svg.appendChild(el);
      return {{ key: u.k, el }};
    }});
    coarse.forEach(u => {{
      const el = document.createElementNS(NS, "path");
      el.setAttribute("d", u.d);
      el.setAttribute("class", "coarse");
      el.setAttribute("stroke-width", "1.9");
      svg.appendChild(el);
    }});
    panels[level] = {{
      cells,
      scale: p.querySelector(".scale"),
      bracket: p.querySelector(".bracket"),
      rng: p.querySelector(".rng"),
      ticks: [p.querySelector(".t0"), p.querySelector(".t1"), p.querySelector(".t2")],
    }};
  }});
  return {{ st, wrap, panels, wds: wrap.querySelector(".wds") }};
}});

function fillPeriods() {{
  const keep = periodSel.value;
  periodSel.innerHTML = "";
  periods[scenarioSel.value].forEach(p => periodSel.add(new Option(p, p)));
  if ([...periodSel.options].some(o => o.value === keep)) periodSel.value = keep;
}}

function draw() {{
  const key = `${{scenarioSel.value}}|${{periodSel.value}}`;
  const auto = document.getElementById("dom-auto").checked;
  warn.classList.toggle("on", auto);

  built.forEach(({{ st, panels, wds }}) => {{
    wds.textContent = (st.wds[key] ?? "–") + "%";
    ["district", "block"].forEach(level => {{
      const scores = st.scores[level][key] || {{}};
      const P = panels[level];
      const vals = P.cells.map(c => scores[c.key]).filter(v => v !== undefined);
      const obsLo = vals.length ? Math.min(...vals) : 0;
      const obsHi = vals.length ? Math.max(...vals) : 100;
      const lo = auto ? obsLo : 0, hi = auto ? obsHi : 100;

      P.cells.forEach(c => c.el.setAttribute("fill", color(scores[c.key], lo, hi)));
      P.scale.style.background = `linear-gradient(to right, ${{D.ramp.join(",")}})`;
      const span = hi > lo ? hi - lo : 1;
      P.bracket.style.left = (100 * (obsLo - lo) / span) + "%";
      P.bracket.style.width = Math.max(0.8, 100 * (obsHi - obsLo) / span) + "%";
      P.rng.textContent = vals.length
        ? `${{obsLo.toFixed(1)}} – ${{obsHi.toFixed(1)}}  (${{(obsHi - obsLo).toFixed(1)}} wide)` : "–";
      P.ticks[0].textContent = lo.toFixed(auto ? 1 : 0);
      P.ticks[1].textContent = ((lo + hi) / 2).toFixed(auto ? 1 : 0);
      P.ticks[2].textContent = hi.toFixed(auto ? 1 : 0);
    }});
  }});
}}

scenarioSel.addEventListener("change", () => {{ fillPeriods(); draw(); }});
periodSel.addEventListener("change", draw);
document.querySelectorAll('input[name="dom"]').forEach(r => r.addEventListener("change", draw));
fillPeriods();
scenarioSel.value = "ssp585"; fillPeriods(); periodSel.value = "2040-2060";
draw();
</script>
"""


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--states", default=",".join(DEFAULT_STATES),
                        help="comma-separated states to compare (default: %(default)s)")
    parser.add_argument("--support", type=Path, default=DEFAULT_SUPPORT,
                        help="frozen cdf_support.csv from the pilot run (default: %(default)s)")
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--data-dir", type=Path, default=None)
    parser.add_argument("--simplify", type=float, default=DEFAULT_SIMPLIFY)
    parser.add_argument("--precision", type=int, default=1)
    args = parser.parse_args(argv)

    data_dir = Path(args.data_dir) if args.data_dir else get_paths_config().data_dir
    wanted = [name.strip() for name in args.states.split(",") if name.strip()]

    support = load_frozen_support(args.support)
    specs = _bundle_metric_specs(get_composite_metric_for_bundle(BUNDLE_DOMAIN))
    absolute = [s for s in specs if s.slug not in BASELINE_REFERENCED_SLUGS]
    slugs = [s.slug for s in absolute]
    missing = [slug for slug in slugs if slug not in support]
    if missing:
        raise ValueError(f"frozen support has no ruler for: {', '.join(missing)}")
    weights = pd.Series({s.slug: float(s.weight) for s in absolute}, dtype=float)
    weights = weights / weights.sum()

    payload_states: list[dict] = []
    for state in wanted:
        print(f"[{state}] loading masters", file=sys.stderr)
        frames = {}
        for level in ("district", "block"):
            long_frame = load_national_long_frame(
                slugs, level=level, states=[state], data_dir=data_dir, verbose=False
            )
            if long_frame.empty:
                raise RuntimeError(f"no {level} master rows for {state!r}")
            long_frame = long_frame.copy()
            long_frame["comp"] = composite_frame(
                long_frame, slugs=slugs, weights=weights, support=support
            )
            frames[level] = long_frame

        frames["block"]["district_key"] = (
            frames["block"]["block_key"].str.rsplit("|", n=1).str[0]
        )

        scores: dict[str, dict[str, dict[str, float]]] = {}
        for level, key in (("district", "district_key"), ("block", "block_key")):
            per_slice: dict[str, dict[str, float]] = {}
            for (scenario, period), block in frames[level].groupby(
                ["scenario", "period"], sort=False
            ):
                per_slice[f"{scenario}|{period}"] = {
                    str(k): round(float(v), 2)
                    for k, v in zip(block[key], block["comp"])
                    if pd.notna(v)
                }
            scores[level] = per_slice

        print(f"[{state}] building geometry", file=sys.stderr)
        shapes = state_shapes(
            state, data_dir=data_dir, simplify=args.simplify, precision=args.precision
        )
        payload_states.append(
            {
                "name": state,
                "districts": shapes["district"],
                "blocks": shapes["block"],
                "outline": [{"k": "", "n": state, "d": shapes["silhouette"]}],
                "height": round(shapes["height"], 2),
                "scores": scores,
                "wds": within_district_share(frames["block"]),
            }
        )

    page = build_page(payload_states, source=args.support)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(page, encoding="utf-8")
    print(f"wrote {args.out} ({args.out.stat().st_size / 1e6:.2f} MB, "
          f"{len(payload_states)} states)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
