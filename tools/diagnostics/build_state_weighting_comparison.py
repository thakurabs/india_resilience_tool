"""Area-weighted vs population-weighted State means, side by side (CHG-0374).

Read-only diagnostic for one open decision: which weight the State/UT headline
number uses once `Elevated Bundle-Score Concentration (%)` is retired and the
State statistic becomes a weighted mean of district composite scores.

Both maps paint the same quantity on the same frozen 0-100 ramp; only the weight
differs. Area weighting answers "how hot is this state's land", population
weighting answers "how hot is this state where people are". Neither is a
correction of the other, which is why the page shows both rather than picking.

The district scores come from the frozen `cdf` ruler read out of the pilot's
`cdf_support.csv`; nothing is refitted. Population is the 2025 snapshot, so the
population weights are held constant across every slice - stated on the page,
because a 2060-2080 map weighted by 2025 people is an assumption, not a fact.

Usage
-----
    python -m tools.diagnostics.build_state_weighting_comparison \
        --out docs/diagnostics/heat_risk_pilot/state_weighting.html
"""

from __future__ import annotations

import argparse
import glob
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
from tools.diagnostics.build_resolution_comparison import (
    composite_frame,
    load_frozen_support,
)
from tools.diagnostics.heat_risk_national_ruler_pilot import (
    BASELINE_REFERENCED_SLUGS,
    BUNDLE_DOMAIN,
    SLICES,
    discover_states,
    load_national_long_frame,
)

DEFAULT_SUPPORT = Path("docs/diagnostics/heat_risk_pilot/cdf_support.csv")
DEFAULT_OUT = Path("docs/diagnostics/heat_risk_pilot/state_weighting.html")
DEFAULT_SIMPLIFY = 0.02  # national extent, 36 polygons: coarser than a state panel


def district_context(data_dir: Path) -> pd.DataFrame:
    """District key -> parent state, area, and 2025 population."""
    rows: list[dict] = []
    root = Path(data_dir) / "processed_optimised"
    for path in sorted((root / "geometry" / "admin" / "district").glob("state=*.geojson")):
        for feature in json.loads(path.read_text(encoding="utf-8"))["features"]:
            rows.append(feature["properties"])
    area = pd.DataFrame(rows).loc[:, ["district_key", "state_name", "area_m2"]]

    shards = sorted(
        (root / "metrics" / "population_total" / "masters" / "admin" / "district").glob(
            "state=*.parquet"
        )
    )
    if not shards:
        raise FileNotFoundError("no district population_total masters; cannot weight by people")
    pop = pd.concat([pd.read_parquet(p) for p in shards], ignore_index=True)
    column = next(c for c in pop.columns if c.startswith("population_total__"))
    pop = pop.loc[:, ["district_key", column]].rename(columns={column: "population"})
    return area.merge(pop, on="district_key", how="left")


def state_means(scored: pd.DataFrame) -> dict[str, dict[str, dict[str, float]]]:
    """Per slice, per state: the two weighted means and their difference."""
    out: dict[str, dict[str, dict[str, float]]] = {}
    for (scenario, period), block in scored.groupby(["scenario", "period"], sort=False):
        per_state: dict[str, dict[str, float]] = {}
        for state, group in block.groupby("state_name", sort=True):
            values = pd.to_numeric(group["comp"], errors="coerce")
            mask = values.notna()
            if not mask.any():
                continue
            areas = pd.to_numeric(group["area_m2"], errors="coerce").fillna(0.0)
            people = pd.to_numeric(group["population"], errors="coerce").fillna(0.0)
            area_ok = mask & (areas > 0)
            pop_ok = mask & (people > 0)
            area_mean = (
                float(np.average(values[area_ok], weights=areas[area_ok]))
                if area_ok.any() else float("nan")
            )
            pop_mean = (
                float(np.average(values[pop_ok], weights=people[pop_ok]))
                if pop_ok.any() else float("nan")
            )
            per_state[str(state)] = {
                "area": round(area_mean, 2),
                "pop": round(pop_mean, 2),
                "delta": round(area_mean - pop_mean, 2),
                "n": int(mask.sum()),
            }
        out[f"{scenario}|{period}"] = per_state
    return out


def state_shapes(data_dir: Path, *, simplify: float, precision: int) -> tuple[list[dict], float]:
    """One SVG path per State/UT, from the canonical adm1 layer."""
    import geopandas as gpd

    path = Path(data_dir) / "processed_optimised" / "geometry" / "admin" / "adm1.geojson"
    gdf = gpd.read_file(path)
    if gdf.crs is not None and gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(4326)
    if simplify > 0:
        gdf["geometry"] = gdf.geometry.simplify(simplify, preserve_topology=True)

    bounds = tuple(float(v) for v in gdf.total_bounds)  # type: ignore[assignment]
    _, _, height = _project(np.array([bounds[0]]), np.array([bounds[1]]), bounds)

    shapes = []
    for row in gdf.itertuples(index=False):
        d = _geom_path(row.geometry, bounds, precision)
        if d:
            shapes.append({"k": getattr(row, "state_name", ""), "d": d})
    return shapes, float(height)


def build_page(
    shapes: list[dict], height: float, values: dict, *, source: Path, districts: int
) -> str:
    payload = {
        "shapes": shapes,
        "values": values,
        "slices": [list(s) for s in SLICES],
        "ramp": ramp_hex(),
        "missing": MISSING_COLOR,
        "width": CANVAS_WIDTH,
        "height": round(height, 2),
    }
    blob = json.dumps(payload, separators=(",", ":"))

    return f"""<title>Area or People</title>
<style>
  :root {{
    --paper:#fbfcfd; --panel:#eef2f6; --ink:#101922; --muted:#5a6b7b;
    --rule:#ccd6e0; --accent:#12506e; --stroke:#7a8794;
  }}
  @media (prefers-color-scheme: dark) {{
    :root:not([data-theme="light"]) {{
      --paper:#0d1319; --panel:#161e27; --ink:#e8eef4; --muted:#8ea0b0;
      --rule:#26303b; --accent:#6fb2d6; --stroke:#8e9ba8;
    }}
  }}
  :root[data-theme="dark"] {{
    --paper:#0d1319; --panel:#161e27; --ink:#e8eef4; --muted:#8ea0b0;
    --rule:#26303b; --accent:#6fb2d6; --stroke:#8e9ba8;
  }}
  html {{ --sans: ui-sans-serif, system-ui, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
          --mono: ui-monospace, "Cascadia Mono", "SF Mono", Consolas, "Liberation Mono", monospace; }}
  body {{ background:var(--paper); color:var(--ink); font:15px/1.55 var(--sans);
    margin:0; padding:26px clamp(18px,4vw,46px) 60px; }}
  .page {{ max-width:1240px; margin:0 auto; display:flex; flex-direction:column; gap:24px; }}
  .eyebrow {{ font:600 11px/1 var(--mono); letter-spacing:.14em; text-transform:uppercase;
    color:var(--accent); margin:0 0 8px; }}
  h1 {{ font-size:26px; font-weight:620; letter-spacing:-.02em; margin:0 0 6px; text-wrap:balance; }}
  .sub {{ color:var(--muted); font-size:13px; margin:0; max-width:72ch; }}
  .controls {{ display:flex; gap:26px; flex-wrap:wrap; align-items:flex-end;
    border-top:1px solid var(--rule); border-bottom:1px solid var(--rule); padding:14px 0; }}
  label {{ display:block; font:600 10.5px/1 var(--mono); letter-spacing:.12em;
    text-transform:uppercase; color:var(--muted); margin-bottom:6px; }}
  select {{ font:14px var(--mono); padding:8px 11px; border-radius:5px;
    border:1px solid var(--rule); background:var(--panel); color:var(--ink); min-width:170px; }}
  select:focus-visible {{ outline:2px solid var(--accent); outline-offset:2px; }}
  .maps {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(320px,1fr)); gap:30px; }}
  .panel h2 {{ font-size:16px; font-weight:600; margin:0 0 2px; }}
  .panel p {{ font-size:12.5px; color:var(--muted); margin:0 0 10px; }}
  .panel {{ position:relative; }}
  svg.map {{ width:100%; height:460px; display:block; }}
  path.st {{ stroke:var(--stroke); stroke-opacity:.95; stroke-width:1.1;
    vector-effect:non-scaling-stroke; stroke-linejoin:round; }}
  path.st:hover {{ stroke:var(--ink); stroke-width:2.2; }}
  .tip {{ position:absolute; pointer-events:none; background:var(--ink); color:var(--paper);
    padding:8px 10px; border-radius:5px; font:12.5px/1.5 var(--mono); white-space:pre;
    opacity:0; transition:opacity .09s; z-index:5; }}
  @media (prefers-reduced-motion:reduce) {{ .tip {{ transition:none; }} }}
  .legend {{ display:flex; align-items:center; gap:14px; flex-wrap:wrap; }}
  .bar {{ flex:1 1 240px; max-width:420px; }}
  .bar .grad {{ height:13px; border-radius:2px; border:1px solid var(--rule); }}
  .bar .ticks {{ display:flex; justify-content:space-between; font:11px var(--mono);
    color:var(--muted); margin-top:4px; }}
  .swatch {{ display:flex; align-items:center; gap:7px; font-size:12.5px; color:var(--muted); }}
  .swatch i {{ width:14px; height:14px; border-radius:2px; border:1px solid var(--rule);
    background:{MISSING_COLOR}; display:inline-block; }}

  .delta h2 {{ font-size:16px; font-weight:600; margin:0 0 2px; }}
  .delta p {{ font-size:12.5px; color:var(--muted); margin:0 0 14px; max-width:70ch; }}
  .rows {{ display:flex; flex-direction:column; }}
  .row {{ display:grid; grid-template-columns:190px 1fr 74px; gap:12px; align-items:center;
    padding:3px 0; border-bottom:1px solid var(--rule); }}
  .row:last-child {{ border-bottom:0; }}
  .row .nm {{ font-size:12.5px; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }}
  .row .val {{ font:12.5px var(--mono); font-variant-numeric:tabular-nums; text-align:right;
    color:var(--muted); }}
  .track {{ position:relative; height:15px; }}
  .track .axis {{ position:absolute; left:50%; top:-2px; bottom:-2px; width:1px;
    background:var(--rule); }}
  .track .b {{ position:absolute; top:2px; height:11px; border-radius:2px; }}
  .axlabels {{ display:grid; grid-template-columns:190px 1fr 74px; gap:12px;
    font:10.5px var(--mono); letter-spacing:.06em; text-transform:uppercase;
    color:var(--muted); margin-bottom:6px; }}
  .axlabels span:nth-child(2) {{ display:flex; justify-content:space-between; }}
</style>

<div class="page">
  <header>
    <p class="eyebrow">Heat Risk &middot; frozen CDF ruler &middot; absolute half</p>
    <h1>Area or people</h1>
    <p class="sub">The State/UT headline is a weighted mean of its district composite scores.
      Both maps show that mean on the same frozen 0&ndash;100 scale; only the weight differs.
      {districts} districts, 36 States and UTs. Population is the 2025 snapshot and is held
      constant across slices &mdash; a 2060&ndash;2080 map weighted by today&rsquo;s people is an
      assumption. Scores from <code>{html.escape(str(source))}</code>.</p>
  </header>

  <div class="controls">
    <div><label for="scenario">Scenario</label><select id="scenario"></select></div>
    <div><label for="period">Period</label><select id="period"></select></div>
  </div>

  <div class="maps">
    <div class="panel">
      <h2>Weighted by area</h2>
      <p>How hot the state&rsquo;s land is. Empty terrain counts as much as a city.</p>
      <svg class="map" id="map-area" preserveAspectRatio="xMidYMid meet"></svg>
      <div class="tip" id="tip-area"></div>
    </div>
    <div class="panel">
      <h2>Weighted by population</h2>
      <p>How hot the state is where people live. Uninhabited terrain drops out.</p>
      <svg class="map" id="map-pop" preserveAspectRatio="xMidYMid meet"></svg>
      <div class="tip" id="tip-pop"></div>
    </div>
  </div>

  <div class="legend">
    <div class="bar">
      <label>Heat Risk score</label>
      <div class="grad" id="grad"></div>
      <div class="ticks"><span>0</span><span>25</span><span>50</span><span>75</span><span>100</span></div>
    </div>
    <div class="swatch"><i></i> no data</div>
  </div>

  <div class="delta">
    <h2>Where the two disagree</h2>
    <p>Area-weighted minus population-weighted, in score points. Bars to the right mean the
      land reads hotter than the people do; bars to the left mean the population sits in the
      hotter part of the state.</p>
    <div class="axlabels"><span>State / UT</span><span><span>people hotter</span><span>land hotter</span></span><span>&Delta;</span></div>
    <div class="rows" id="rows"></div>
  </div>
</div>

<script>
const D = {blob};
const NS = "http://www.w3.org/2000/svg";
const scenarioSel = document.getElementById("scenario");
const periodSel = document.getElementById("period");
const rowsHost = document.getElementById("rows");

const periods = {{}};
D.slices.forEach(([sc, pe]) => (periods[sc] = periods[sc] || []).push(pe));
Object.keys(periods).forEach(sc => scenarioSel.add(new Option(sc, sc)));

function color(v) {{
  if (v === undefined || v === null || Number.isNaN(v)) return D.missing;
  const i = Math.max(0, Math.min(100, Math.round(v)));
  return D.ramp[i];
}}

const panels = ["area", "pop"].map(kind => {{
  const svg = document.getElementById("map-" + kind);
  const tip = document.getElementById("tip-" + kind);
  svg.setAttribute("viewBox", `0 0 ${{D.width}} ${{D.height}}`);
  const cells = D.shapes.map(s => {{
    const p = document.createElementNS(NS, "path");
    p.setAttribute("d", s.d);
    p.setAttribute("class", "st");
    p.addEventListener("mousemove", e => {{
      const box = svg.parentElement.getBoundingClientRect();
      tip.textContent = p.dataset.tip || s.k;
      tip.style.left = Math.min(e.clientX - box.left + 14, box.width - 30) + "px";
      tip.style.top = (e.clientY - box.top + 14) + "px";
      tip.style.opacity = 1;
    }});
    p.addEventListener("mouseleave", () => {{ tip.style.opacity = 0; }});
    svg.appendChild(p);
    return {{ key: s.k, el: p }};
  }});
  return {{ kind, cells }};
}});

document.getElementById("grad").style.background =
  `linear-gradient(to right, ${{D.ramp.join(",")}})`;

function fillPeriods() {{
  const keep = periodSel.value;
  periodSel.innerHTML = "";
  periods[scenarioSel.value].forEach(p => periodSel.add(new Option(p, p)));
  if ([...periodSel.options].some(o => o.value === keep)) periodSel.value = keep;
}}

function draw() {{
  const vals = D.values[`${{scenarioSel.value}}|${{periodSel.value}}`] || {{}};

  panels.forEach(({{ kind, cells }}) => {{
    cells.forEach(c => {{
      const v = vals[c.key];
      c.el.setAttribute("fill", color(v ? v[kind] : undefined));
      c.el.dataset.tip = v
        ? `${{c.key}}\\narea  ${{v.area.toFixed(1)}}\\npeople ${{v.pop.toFixed(1)}}\\n` +
          `delta ${{v.delta > 0 ? "+" : ""}}${{v.delta.toFixed(1)}}  ·  ${{v.n}} districts`
        : `${{c.key}}\\nno data`;
    }});
  }});

  const entries = Object.entries(vals)
    .filter(([, v]) => Number.isFinite(v.delta))
    .sort((a, b) => a[1].delta - b[1].delta);
  const span = Math.max(1, ...entries.map(([, v]) => Math.abs(v.delta)));
  rowsHost.innerHTML = "";
  entries.forEach(([name, v]) => {{
    const row = document.createElement("div");
    row.className = "row";
    const half = 50 * Math.abs(v.delta) / span;
    const left = v.delta >= 0 ? 50 : 50 - half;
    row.innerHTML =
      `<span class="nm">${{name}}</span>` +
      `<span class="track"><span class="axis"></span>` +
      `<span class="b" style="left:${{left}}%;width:${{half}}%;` +
      `background:${{color(v.delta >= 0 ? v.area : v.pop)}}"></span></span>` +
      `<span class="val">${{v.delta > 0 ? "+" : ""}}${{v.delta.toFixed(1)}}</span>`;
    rowsHost.appendChild(row);
  }});
}}

scenarioSel.addEventListener("change", () => {{ fillPeriods(); draw(); }});
periodSel.addEventListener("change", draw);
fillPeriods();
scenarioSel.value = "ssp585"; fillPeriods(); periodSel.value = "2040-2060";
draw();
</script>
"""


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--support", type=Path, default=DEFAULT_SUPPORT)
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--data-dir", type=Path, default=None)
    parser.add_argument("--simplify", type=float, default=DEFAULT_SIMPLIFY)
    parser.add_argument("--precision", type=int, default=1)
    args = parser.parse_args(argv)

    data_dir = Path(args.data_dir) if args.data_dir else get_paths_config().data_dir
    support = load_frozen_support(args.support)
    specs = _bundle_metric_specs(get_composite_metric_for_bundle(BUNDLE_DOMAIN))
    absolute = [s for s in specs if s.slug not in BASELINE_REFERENCED_SLUGS]
    slugs = [s.slug for s in absolute]
    weights = pd.Series({s.slug: float(s.weight) for s in absolute}, dtype=float)
    weights = weights / weights.sum()

    print("loading district masters", file=sys.stderr)
    states = discover_states(slugs, data_dir=data_dir)
    frame = load_national_long_frame(
        slugs, level="district", states=states, data_dir=data_dir, verbose=False
    )
    frame["comp"] = composite_frame(frame, slugs=slugs, weights=weights, support=support)
    frame = frame.merge(district_context(data_dir), on="district_key", how="left")

    orphans = int(frame["state_name"].isna().sum())
    if orphans:
        print(f"[warn] {orphans} scored district rows have no geometry context", file=sys.stderr)
    no_pop = sorted(frame.loc[frame["population"].isna(), "district_key"].unique())
    if no_pop:
        print(f"[warn] {len(no_pop)} districts have no population, e.g. {no_pop[:3]}",
              file=sys.stderr)

    values = state_means(frame)
    print("building geometry", file=sys.stderr)
    shapes, height = state_shapes(data_dir, simplify=args.simplify, precision=args.precision)

    painted = {s["k"] for s in shapes}
    scored = set(next(iter(values.values())))
    if painted - scored:
        print(f"[warn] states with geometry but no value: {sorted(painted - scored)}",
              file=sys.stderr)
    if scored - painted:
        print(f"[warn] states with a value but no geometry: {sorted(scored - painted)}",
              file=sys.stderr)

    page = build_page(
        shapes, height, values,
        source=args.support, districts=int(frame["district_key"].nunique()),
    )
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(page, encoding="utf-8")
    print(f"wrote {args.out} ({args.out.stat().st_size / 1e6:.2f} MB, {len(shapes)} states)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
