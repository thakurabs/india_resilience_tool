"""Frozen Heat Risk district map — self-contained interactive HTML (CHG-0368).

Read-only diagnostic viewer. It renders the *decided* Heat Risk configuration
(2026-09-09) as a single standalone HTML file with two selectors, scenario and
period, and nothing else to choose:

- **Ruler** frozen to ``cdf`` — the exact pooled mid-rank empirical CDF fitted by
  ``tools.diagnostics.heat_risk_national_ruler_pilot``. A score answers "how
  unusual is this district within the pooled national sample?", not "how far up
  India's physical range is it?".
- **Headline** frozen to ``composite_absolute_threshold`` — the absolute half
  only: the 9 metrics scored against absolute physical thresholds or levels,
  their weights renormalized 0.633 -> 1.000 within the half. The 5
  baseline-referenced metrics are excluded from the headline by decision; they
  remain in the pilot outputs as a separate lens.
- **Domain** frozen to 0-100. Every slice is painted on the same scale, so the
  selectors move the map, never the ruler.
- **Colour ramp** frozen to the vendored NCL ``WhiteBlueGreenYellowRed`` table.

The input is the pilot's ``district_scores.csv``; this tool re-scores nothing.
Geometry comes from the same canonical district shards the pilot uses, is
simplified for the browser, and is embedded as SVG path data so the output file
has no network dependencies at all.

Usage
-----
    python -m tools.diagnostics.build_heat_risk_frozen_map \
        --scores docs/diagnostics/heat_risk_pilot/district_scores.csv \
        --out docs/diagnostics/heat_risk_pilot/heat_risk_frozen_map.html
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

from india_resilience_tool.config.paths import get_paths_config
from tools.diagnostics.heat_risk_national_ruler_pilot import (
    SLICES,
    _load_cmap,
    load_district_geometry,
)

# ---------------------------------------------------------------------------
# Frozen settings (the two decisions of 2026-09-09; not flags on purpose)
# ---------------------------------------------------------------------------

FROZEN_RULER = "cdf"
FROZEN_FIELD = "composite_absolute_threshold"
FROZEN_CMAP = "WhiteBlueGreenYellowRed"
FROZEN_VMIN = 0.0
FROZEN_VMAX = 100.0

#: The WhiteBlueGreenYellowRed table bottoms out at pure white, which collides
#: with the grey "no data" convention: a near-zero district would read as a hole
#: in the map. The ramp therefore starts a step up the table, so score 0 is a
#: faint blue-white that is still visibly *painted*. Missing districts keep an
#: explicit grey with a hatch, and their own legend swatch.
CMAP_FLOOR = 0.045
MISSING_COLOR = "#d5d8dc"

DEFAULT_SCORES = Path("docs/diagnostics/heat_risk_pilot/district_scores.csv")
DEFAULT_OUT = Path("docs/diagnostics/heat_risk_pilot/heat_risk_frozen_map.html")

#: Douglas-Peucker tolerance in degrees, and SVG canvas width in user units.
DEFAULT_SIMPLIFY = 0.01
CANVAS_WIDTH = 1000.0


# ---------------------------------------------------------------------------
# Data
# ---------------------------------------------------------------------------


def load_frozen_scores(path: Path) -> pd.DataFrame:
    """Rows of the pilot score table for the frozen ruler, one per district-slice.

    Raises when the frozen ruler or the frozen headline field is absent, rather
    than silently falling back to the composite: a viewer that quietly paints a
    different quantity than its caption claims is worse than no viewer.
    """
    frame = pd.read_csv(path)
    for column in ("ruler", "district_key", "scenario", "period", FROZEN_FIELD):
        if column not in frame.columns:
            raise ValueError(f"{path} has no '{column}' column; not a pilot score table")

    frame = frame.loc[frame["ruler"] == FROZEN_RULER].copy()
    if frame.empty:
        raise ValueError(f"{path} carries no rows for ruler '{FROZEN_RULER}'")

    frame[FROZEN_FIELD] = pd.to_numeric(frame[FROZEN_FIELD], errors="coerce")
    keep = ["district_key", "state", "district", "scenario", "period", FROZEN_FIELD]
    keep = [c for c in keep if c in frame.columns]
    frame = frame.loc[:, keep]

    present = set(map(tuple, frame.loc[:, ["scenario", "period"]].drop_duplicates().values))
    missing = [s for s in SLICES if s not in present]
    if missing:
        raise ValueError(
            "score table is missing frozen slices: "
            + ", ".join(f"{sc}/{pe}" for sc, pe in missing)
        )
    return frame


# ---------------------------------------------------------------------------
# Geometry -> SVG
# ---------------------------------------------------------------------------


def _project(lon: np.ndarray, lat: np.ndarray, bounds: tuple[float, float, float, float]):
    """Equirectangular projection with a cos(mean latitude) width correction.

    Sufficient for a national reference map and it keeps the output dependency
    free; nothing downstream measures area off these pixels.
    """
    minx, miny, maxx, maxy = bounds
    lat0 = np.deg2rad((miny + maxy) / 2.0)
    span_x = (maxx - minx) * np.cos(lat0)
    span_y = maxy - miny
    scale = CANVAS_WIDTH / span_x
    x = (lon - minx) * np.cos(lat0) * scale
    y = (maxy - lat) * scale
    return x, y, span_y * scale


def _ring_path(coords, bounds, precision: int) -> str:
    lon = np.asarray([c[0] for c in coords], dtype=float)
    lat = np.asarray([c[1] for c in coords], dtype=float)
    x, y, _ = _project(lon, lat, bounds)
    x = np.round(x, precision)
    y = np.round(y, precision)
    parts = [f"M{x[0]} {y[0]}"]
    parts.extend(f"L{px} {py}" for px, py in zip(x[1:], y[1:]))
    parts.append("Z")
    return "".join(parts)


def _geom_path(geom, bounds, precision: int) -> str:
    """SVG path data for a (Multi)Polygon, exterior and interior rings alike."""
    if geom is None or geom.is_empty:
        return ""
    polygons = list(getattr(geom, "geoms", [geom]))
    rings: list[str] = []
    for polygon in polygons:
        if polygon.is_empty:
            continue
        rings.append(_ring_path(list(polygon.exterior.coords), bounds, precision))
        for interior in polygon.interiors:
            rings.append(_ring_path(list(interior.coords), bounds, precision))
    return "".join(rings)


def build_shapes(
    data_dir: Path,
    *,
    simplify: float,
    precision: int,
    states: Optional[Sequence[str]] = None,
) -> tuple[list[dict], list[str], float]:
    """District paths, state-outline paths, and the SVG canvas height.

    Both layers are projected against the *same* bounds so they overlay exactly.
    """
    gdf = load_district_geometry(data_dir, states=states)
    if gdf.crs is not None and gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(4326)

    if simplify > 0:
        gdf["geometry"] = gdf.geometry.simplify(simplify, preserve_topology=True)

    bounds = tuple(float(v) for v in gdf.total_bounds)  # type: ignore[assignment]
    _, _, height = _project(np.array([bounds[0]]), np.array([bounds[1]]), bounds)

    districts: list[dict] = []
    for row in gdf.itertuples(index=False):
        path = _geom_path(row.geometry, bounds, precision)
        if not path:
            continue
        districts.append(
            {
                "k": getattr(row, "district_key", ""),
                "n": getattr(row, "district_name", ""),
                "s": getattr(row, "state_name", ""),
                "d": path,
            }
        )

    outlines = gdf.dissolve(by="state_name") if "state_name" in gdf.columns else None
    state_paths: list[str] = []
    if outlines is not None:
        for geom in outlines.geometry:
            path = _geom_path(geom, bounds, precision)
            if path:
                state_paths.append(path)

    return districts, state_paths, float(height)


# ---------------------------------------------------------------------------
# Colour ramp
# ---------------------------------------------------------------------------


def ramp_hex(steps: int = 101) -> list[str]:
    """`steps` hex colours sampled from the frozen table, starting above white."""
    from matplotlib.colors import to_hex

    cmap = _load_cmap(FROZEN_CMAP)
    fractions = CMAP_FLOOR + (1.0 - CMAP_FLOOR) * np.linspace(0.0, 1.0, steps)
    return [to_hex(cmap(float(f))) for f in fractions]


# ---------------------------------------------------------------------------
# HTML
# ---------------------------------------------------------------------------


def build_html(
    scores: pd.DataFrame,
    districts: list[dict],
    state_paths: list[str],
    height: float,
    *,
    source: Path,
) -> str:
    """One self-contained page: inline SVG, inline data, no network calls."""
    by_slice: dict[str, dict[str, float]] = {}
    for (scenario, period), block in scores.groupby(["scenario", "period"], sort=False):
        values = block.set_index("district_key")[FROZEN_FIELD]
        by_slice[f"{scenario}|{period}"] = {
            str(key): round(float(value), 2)
            for key, value in values.items()
            if pd.notna(value)
        }

    periods_by_scenario: dict[str, list[str]] = {}
    for scenario, period in SLICES:
        periods_by_scenario.setdefault(scenario, []).append(period)

    payload = {
        "districts": districts,
        "states": state_paths,
        "scores": by_slice,
        "slices": [list(s) for s in SLICES],
        "periods": periods_by_scenario,
        "ramp": ramp_hex(),
        "height": round(height, 2),
        "width": CANVAS_WIDTH,
        "missing": MISSING_COLOR,
        "vmin": FROZEN_VMIN,
        "vmax": FROZEN_VMAX,
    }
    blob = json.dumps(payload, separators=(",", ":"))
    covered = sum(len(v) for v in by_slice.values())
    caption = (
        f"{len(districts)} districts &middot; {len(SLICES)} slices &middot; "
        f"{covered} scored values &middot; source "
        f"<code>{html.escape(str(source))}</code>"
    )

    return f"""<title>Heat Risk Frozen Map</title>
<style>
  /* Palette: blue-biased neutrals off the cold end of the WhBlGrYeRe table, so
     the chrome sits under the map instead of competing with it. */
  :root {{
    --paper:#fbfcfd; --panel:#eef2f6; --ink:#101922; --muted:#5a6b7b;
    --rule:#ccd6e0; --accent:#12506e; --hot:#a8261f; --shadow:rgba(16,25,34,.08);
  }}
  @media (prefers-color-scheme: dark) {{
    :root:not([data-theme="light"]) {{
      --paper:#0d1319; --panel:#161e27; --ink:#e8eef4; --muted:#8ea0b0;
      --rule:#26303b; --accent:#6fb2d6; --hot:#e0736a; --shadow:rgba(0,0,0,.4);
    }}
  }}
  :root[data-theme="dark"] {{
    --paper:#0d1319; --panel:#161e27; --ink:#e8eef4; --muted:#8ea0b0;
    --rule:#26303b; --accent:#6fb2d6; --hot:#e0736a; --shadow:rgba(0,0,0,.4);
  }}

  /* No webfonts on purpose: this file is opened from the repo, often offline.
     The sans/mono split does the typographic work instead — every number,
     identifier and axis label is set in mono. */
  html {{ --sans: ui-sans-serif, system-ui, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
          --mono: ui-monospace, "Cascadia Mono", "SF Mono", Consolas, "Liberation Mono", monospace; }}

  body {{ background:var(--paper); color:var(--ink); font:15px/1.55 var(--sans);
    margin:0; padding:26px clamp(18px,4vw,46px) 56px; }}
  .page {{ max-width:1180px; margin:0 auto; display:flex; flex-direction:column; gap:22px; }}

  .eyebrow {{ font:600 11px/1 var(--mono); letter-spacing:.14em; text-transform:uppercase;
    color:var(--accent); margin:0 0 8px; }}
  h1 {{ font-size:26px; font-weight:620; letter-spacing:-.02em; margin:0 0 6px;
    text-wrap:balance; }}
  .sub {{ color:var(--muted); font-size:13px; margin:0; }}
  .sub code {{ font:12px var(--mono); }}

  /* Spec strip, not a card: four frozen facts divided by hairlines. */
  .spec {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(190px,1fr));
    border-top:1px solid var(--rule); border-bottom:1px solid var(--rule); }}
  .spec div {{ padding:13px 20px 14px; border-left:1px solid var(--rule); }}
  .spec div:first-child {{ border-left:0; padding-left:0; }}
  .spec dt {{ font:600 10.5px/1 var(--mono); letter-spacing:.12em; text-transform:uppercase;
    color:var(--muted); margin:0 0 6px; }}
  .spec dd {{ margin:0; font-size:13px; line-height:1.45; }}
  .spec dd b {{ font-weight:640; }}
  .spec dd span {{ color:var(--muted); }}

  .controls {{ display:flex; gap:20px; flex-wrap:wrap; align-items:flex-end; }}
  label {{ display:block; font:600 10.5px/1 var(--mono); letter-spacing:.12em;
    text-transform:uppercase; color:var(--muted); margin-bottom:6px; }}
  select {{ font:14px var(--mono); padding:8px 11px; border-radius:5px;
    border:1px solid var(--rule); background:var(--panel); color:var(--ink); min-width:180px; }}
  select:focus-visible {{ outline:2px solid var(--accent); outline-offset:2px; }}

  .layout {{ display:flex; gap:34px; align-items:flex-start; flex-wrap:wrap; }}
  .mapwrap {{ flex:1 1 460px; min-width:300px; max-width:720px; position:relative; }}
  svg.map {{ width:100%; height:auto; display:block; }}
  path.dist {{ stroke:var(--paper); stroke-width:.3; stroke-linejoin:round; cursor:crosshair; }}
  path.dist:hover {{ stroke:var(--ink); stroke-width:1.4; }}
  path.state {{ fill:none; stroke:var(--muted); stroke-width:.8; stroke-linejoin:round;
    pointer-events:none; opacity:.55; }}
  .tip {{ position:absolute; pointer-events:none; background:var(--ink); color:var(--paper);
    padding:7px 10px; border-radius:5px; font:12.5px var(--mono); white-space:nowrap;
    opacity:0; transition:opacity .09s; z-index:5; box-shadow:0 2px 10px var(--shadow); }}
  @media (prefers-reduced-motion:reduce) {{ .tip {{ transition:none; }} }}

  .rail {{ flex:0 0 258px; display:flex; flex-direction:column; gap:22px; }}
  .bar {{ height:14px; border-radius:2px; border:1px solid var(--rule); }}
  .ticks {{ display:flex; justify-content:space-between; font:11px var(--mono);
    color:var(--muted); margin-top:4px; }}
  .swatch {{ display:flex; align-items:center; gap:8px; margin-top:10px;
    font-size:12.5px; color:var(--muted); }}
  .swatch i {{ width:14px; height:14px; border-radius:2px; border:1px solid var(--rule);
    display:inline-block; background:{MISSING_COLOR}; }}
  svg.hist {{ width:100%; height:78px; display:block; }}
  .stats {{ display:flex; flex-direction:column; }}
  .stats div {{ display:flex; justify-content:space-between; align-items:baseline;
    gap:12px; padding:8px 0; border-bottom:1px solid var(--rule); }}
  .stats div:last-child {{ border-bottom:0; }}
  .stats dt {{ font-size:12.5px; color:var(--muted); margin:0; }}
  .stats dd {{ margin:0; font:14px var(--mono); font-variant-numeric:tabular-nums; }}
  .chip {{ background:var(--hot); color:#fff; border-radius:3px; padding:2px 7px;
    font:13px var(--mono); }}
</style>

<div class="page">
  <header>
    <p class="eyebrow">Frozen configuration &middot; 2026-09-09</p>
    <h1>Heat Risk across India, district by district</h1>
    <p class="sub">{caption}</p>
  </header>

  <dl class="spec">
    <div><dt>Ruler</dt><dd><b>Pooled mid-rank CDF</b><br>
      <span>a score is a national percentile, not a place on India&rsquo;s physical range</span></dd></div>
    <div><dt>Headline</dt><dd><b>Absolute half only</b><br>
      <span>9 threshold metrics, weights renormalized 0.633&nbsp;&rarr;&nbsp;1.000</span></dd></div>
    <div><dt>Scale</dt><dd><b>Fixed 0&ndash;100</b><br>
      <span>identical across all 7 slices, so the selectors move the map, never the ruler</span></dd></div>
    <div><dt>Ramp</dt><dd><b>WhiteBlueGreenYellowRed</b><br>
      <span>NCL table, started above pure white so no district reads as missing</span></dd></div>
  </dl>

  <div class="controls">
    <div><label for="scenario">Scenario</label><select id="scenario"></select></div>
    <div><label for="period">Period</label><select id="period"></select></div>
  </div>

  <div class="layout">
    <div class="mapwrap">
      <svg class="map" id="map" viewBox="0 0 0 0" role="img"
           aria-label="Choropleth of Heat Risk score by Indian district"></svg>
      <div class="tip" id="tip"></div>
    </div>
    <div class="rail">
      <div>
        <label>Heat Risk score</label>
        <div class="bar" id="bar"></div>
        <div class="ticks"><span>0</span><span>50</span><span>100</span></div>
        <div class="swatch"><i></i> no data</div>
      </div>
      <div>
        <label>Distribution, this slice</label>
        <svg class="hist" id="hist" viewBox="0 0 258 78" role="img"
             aria-label="Histogram of district scores for the selected slice"></svg>
      </div>
      <dl class="stats">
        <div><dt>districts painted</dt><dd id="s-n">&ndash;</dd></div>
        <div><dt>median</dt><dd id="s-med">&ndash;</dd></div>
        <div><dt>min &ndash; max</dt><dd id="s-rng">&ndash;</dd></div>
        <div><dt>score &ge; 80</dt><dd><span class="chip" id="s-hi">&ndash;</span></dd></div>
      </dl>
    </div>
  </div>
</div>

<script>
const D = {blob};
const NS = "http://www.w3.org/2000/svg";
const svg = document.getElementById("map");
const tip = document.getElementById("tip");
const hist = document.getElementById("hist");

svg.setAttribute("viewBox", `0 0 ${{D.width}} ${{D.height}}`);

function color(v) {{
  if (v === undefined || v === null || Number.isNaN(v)) return D.missing;
  const t = (v - D.vmin) / (D.vmax - D.vmin);
  const i = Math.max(0, Math.min(D.ramp.length - 1, Math.round(t * (D.ramp.length - 1))));
  return D.ramp[i];
}}

const nodes = D.districts.map(d => {{
  const p = document.createElementNS(NS, "path");
  p.setAttribute("d", d.d);
  p.setAttribute("class", "dist");
  p.addEventListener("mousemove", e => {{
    const box = svg.parentElement.getBoundingClientRect();
    const v = p.dataset.v;
    tip.textContent = `${{d.n}}, ${{d.s}}  ${{v === "" ? "no data" : v}}`;
    tip.style.left = Math.min(e.clientX - box.left + 14, box.width - 20) + "px";
    tip.style.top = (e.clientY - box.top + 14) + "px";
    tip.style.opacity = 1;
  }});
  p.addEventListener("mouseleave", () => {{ tip.style.opacity = 0; }});
  svg.appendChild(p);
  return {{ key: d.k, el: p }};
}});
D.states.forEach(d => {{
  const p = document.createElementNS(NS, "path");
  p.setAttribute("d", d);
  p.setAttribute("class", "state");
  svg.appendChild(p);
}});

document.getElementById("bar").style.background =
  `linear-gradient(to right, ${{D.ramp.join(",")}})`;

const scenarioSel = document.getElementById("scenario");
const periodSel = document.getElementById("period");
Object.keys(D.periods).forEach(s => scenarioSel.add(new Option(s, s)));

function fillPeriods() {{
  const keep = periodSel.value;
  periodSel.innerHTML = "";
  D.periods[scenarioSel.value].forEach(p => periodSel.add(new Option(p, p)));
  if ([...periodSel.options].some(o => o.value === keep)) periodSel.value = keep;
}}

const BINS = 20, HW = 258, HH = 78, PAD = 12;
function drawHist(vals) {{
  hist.innerHTML = "";
  const counts = new Array(BINS).fill(0);
  vals.forEach(v => counts[Math.min(BINS - 1, Math.floor(v / 100 * BINS))]++);
  const peak = Math.max(1, ...counts);
  const bw = (HW - 2) / BINS;
  counts.forEach((n, i) => {{
    const h = (HH - PAD) * n / peak;
    const r = document.createElementNS(NS, "rect");
    r.setAttribute("x", (1 + i * bw).toFixed(2));
    r.setAttribute("y", (HH - PAD - h).toFixed(2));
    r.setAttribute("width", (bw - 1).toFixed(2));
    r.setAttribute("height", Math.max(h, n ? 1 : 0).toFixed(2));
    r.setAttribute("fill", color((i + 0.5) / BINS * 100));
    hist.appendChild(r);
  }});
  const axis = document.createElementNS(NS, "line");
  axis.setAttribute("x1", 1); axis.setAttribute("x2", HW - 1);
  axis.setAttribute("y1", HH - PAD); axis.setAttribute("y2", HH - PAD);
  axis.setAttribute("stroke", "currentColor");
  axis.setAttribute("stroke-opacity", ".35");
  hist.appendChild(axis);
  const cap = document.createElementNS(NS, "text");
  cap.setAttribute("x", 1); cap.setAttribute("y", HH - 2);
  cap.setAttribute("fill", "currentColor"); cap.setAttribute("fill-opacity", ".6");
  cap.setAttribute("font-size", "10");
  cap.setAttribute("font-family", "ui-monospace, Consolas, monospace");
  cap.textContent = `peak ${{peak}} districts per 5-point bin`;
  hist.appendChild(cap);
}}

function draw() {{
  const scores = D.scores[`${{scenarioSel.value}}|${{periodSel.value}}`] || {{}};
  const vals = [];
  nodes.forEach(n => {{
    const v = scores[n.key];
    n.el.setAttribute("fill", color(v));
    n.el.dataset.v = (v === undefined) ? "" : v.toFixed(1);
    if (v !== undefined) vals.push(v);
  }});
  vals.sort((a, b) => a - b);
  const dash = "\u2013";
  document.getElementById("s-n").textContent = vals.length;
  document.getElementById("s-med").textContent =
    vals.length ? vals[Math.floor(vals.length / 2)].toFixed(1) : dash;
  document.getElementById("s-rng").textContent = vals.length
    ? `${{vals[0].toFixed(1)}} ${{dash}} ${{vals[vals.length - 1].toFixed(1)}}` : dash;
  document.getElementById("s-hi").textContent =
    vals.length ? `${{vals.filter(v => v >= 80).length}} districts` : dash;
  drawHist(vals);
}}

scenarioSel.addEventListener("change", () => {{ fillPeriods(); draw(); }});
periodSel.addEventListener("change", draw);
fillPeriods();
draw();
</script>
"""


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--scores", type=Path, default=DEFAULT_SCORES,
                        help="pilot district_scores.csv to read (default: %(default)s)")
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT,
                        help="HTML file to write (default: %(default)s)")
    parser.add_argument("--data-dir", type=Path, default=None,
                        help="override IRT data dir used to find district geometry")
    parser.add_argument("--simplify", type=float, default=DEFAULT_SIMPLIFY,
                        help="Douglas-Peucker tolerance in degrees (default: %(default)s)")
    parser.add_argument("--precision", type=int, default=1,
                        help="decimal places kept in SVG coordinates (default: %(default)s)")
    args = parser.parse_args(argv)

    data_dir = Path(args.data_dir) if args.data_dir else get_paths_config().data_dir

    scores = load_frozen_scores(args.scores)
    districts, state_paths, height = build_shapes(
        data_dir, simplify=args.simplify, precision=args.precision
    )

    geometry_keys = {d["k"] for d in districts}
    score_keys = set(scores["district_key"].astype(str))
    orphans = sorted(score_keys - geometry_keys)
    unpainted = sorted(geometry_keys - score_keys)
    if orphans:
        print(f"[warn] {len(orphans)} scored district_key(s) have no geometry, "
              f"e.g. {orphans[:3]}", file=sys.stderr)
    if unpainted:
        print(f"[warn] {len(unpainted)} geometry district_key(s) have no score, "
              f"e.g. {unpainted[:3]}", file=sys.stderr)

    page = build_html(scores, districts, state_paths, height, source=args.scores)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(page, encoding="utf-8")
    print(f"wrote {args.out} ({args.out.stat().st_size / 1e6:.2f} MB, "
          f"{len(districts)} districts, {len(SLICES)} slices)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
