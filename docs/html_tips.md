# How the colorbar-comparison HTML was built

Reference notes on the construction of the palette-decision artifact
(“IRT colorbar options — Option A vs A+B”, 2026-07-17). The same techniques
apply to any future self-contained HTML mockup for design decisions.

Files involved:

- `scratch/_palette_mockup.py` — data generator (runs under the Windows `irt`
  conda env; needs geopandas/matplotlib/pyarrow)
- `scratch/results/palette_mockup/mockup_data.json` — intermediate payload
- page builder (session scratchpad) — pure-stdlib Python that templates the
  JSON into one HTML file, published as a Claude artifact

---

## 1. Two-stage pipeline: heavy data step, cheap render step

The build is split so the expensive geospatial work runs once and the visual
iteration loop is instant:

1. **Generator (Windows conda env).** Reads the *real* inputs — the
   Telangana district master parquet
   (`processed_optimised/metrics/composite_heat_risk/masters/admin/district/state=Telangana.parquet`,
   column `composite_heat_risk__ssp585__2020-2040__mean`) and the real ADM2
   geometry (`irt_data/districts_4326.geojson`) — and emits a single JSON with
   values, class edges, palette hex lists, ramp-quality diagnostics, and
   pre-projected SVG path strings. Everything slow or environment-fragile
   (parquet, shapely, matplotlib colormap sampling) ends here.
2. **Page builder (any Python).** Pure `json` + f-string templating. No
   dependencies, so layout/CSS iterations never touch the geo stack.

Using real data mattered for the decision: every candidate palette was shown
on the *same* districts with the *same* 7 equal-interval bins, so the only
visible variable was color.

## 2. Color science: OKLab ramps, validated not eyeballed

- **OKLab/OKLCh conversion** was implemented from Björn Ottosson's reference
  math (sRGB → linear → LMS → cube root → Lab; ~20 lines each way). OKLab's
  L axis tracks perceived lightness far better than sRGB or HSL, which is what
  makes “does the ramp *look* ordered?” a computable question.
- **SDG-anchored ramps** hold the anchor's hue angle constant and interpolate
  only lightness (L) and chroma (C): light tint (L≈0.955, low C) → anchor →
  deepened anchor (L = anchor·0.72, C = anchor·0.82). Two fixes discovered by
  measurement, not by eye:
  - deepen the dark end *proportionally* to the anchor's own lightness, so
    light anchors (SDG orange `#F89D2A`) still get enough travel;
  - place the anchor at its *natural lightness position*
    `t = (L_light − L_anchor)/(L_light − L_dark)` instead of a fixed stop, so
    ΔL is spread evenly across classes (a fixed stop crowded the light end and
    the orange ramp's weakest step fell to 0.041).
- **Acceptance gate:** each 7-class ramp must have strictly decreasing OKLab
  L with min adjacent step ΔL ≥ 0.06 (adjacent classes stay distinguishable).
  The generator prints this per ramp; the page shows it as a pass/warn chip.
  This gate is now also a pytest guard
  (`tests/test_viz_colors.py::test_irt_domain_ramps_are_monotone_in_oklab_lightness`).
- **magma_r** was sampled over [0.04, 0.86] to drop the near-white and
  near-black extremes while keeping its perceptually uniform interior.

## 3. Choropleth as inline SVG (no map library)

For a static comparison, Leaflet is overkill. The generator turns each
district polygon into an SVG path:

- **Projection:** plain equirectangular over the state's bounding box, with
  the height scaled by `cos(mean latitude)` so shapes aren't visibly
  squashed. Adequate at state scale; do not reuse for all-India views where
  a real projection matters.
- **Geometry diet:** `shapely.simplify(0.008°, preserve_topology=True)`
  before path-building keeps all 33 districts around ~200 KB of path data.
- **Path building:** exterior ring + interior rings each become
  `M x,y L … Z` with coordinates rounded to 0.1 px — rounding is most of the
  size savings.
- **Reuse:** the same path set is rendered N times (once per palette), only
  the `fill` attribute differing — computed by binning each district's value
  into `searchsorted`-style class edges shared across all maps.
- **Hover:** a `<title>` child per `<path>` gives native tooltips
  (district + value) with zero JS; a CSS rule
  (`svg:hover path:not(:hover) { opacity:.75 }`) dims siblings so the hovered
  district pops. `prefers-reduced-motion` disables the transition.
- Strokes use `stroke="var(--hairline)"` — a CSS token, not a literal — so
  hairlines stay correct in both themes.

## 4. Theming: token-level light/dark

All colors live as CSS custom properties on `:root`. Dark mode redefines
*only the tokens* in three blocks, in this order:

```css
:root { --bg:…; --ink:…; }                       /* light default */
@media (prefers-color-scheme: dark) { :root {…} } /* OS preference */
:root[data-theme="dark"] {…}                      /* viewer toggle wins */
:root[data-theme="light"] {…}                     /* toggle back wins too */
```

Components never mention a raw hex — they use tokens — so the media query
and the `data-theme` override both work without touching component CSS. The
data swatches (palette hexes) intentionally stay identical across themes;
only ground, ink, and hairlines flip.

## 5. Layout and typography choices

- Cards on `display:grid; grid-template-columns:repeat(auto-fit, minmax(300px,1fr))`
  — responsive without media queries; maps reflow 3-across → 1-across.
- Each card is a flex column with `gap`; the pass/warn chip uses
  `margin-top:auto` to pin to the card foot so rows align.
- `font-variant-numeric: tabular-nums` on legend labels so class-edge
  numbers align.
- The trade-off table sits in its own `overflow-x:auto` wrapper so a narrow
  viewport scrolls the table, never the page body.
- Serif display face (Charter/Georgia stack) for headings against a system
  sans for data/labels; an uppercase letter-spaced eyebrow labels the page's
  role. No webfonts: the artifact CSP blocks external requests, so
  everything (fonts, styles, data) must be inline/system.

## 6. Publishing constraints (Claude artifacts)

- One self-contained file: no CDN scripts, stylesheets, fonts, or remote
  images (strict CSP). Anything external must be inlined or dropped.
- No `<!DOCTYPE>/<html>/<head>/<body>` wrapper — the artifact host wraps the
  fragment; provide `<title>` + `<style>` + content directly.
- HTML-escape all data-derived strings (`html.escape` on names/labels in the
  generator) — district names come from external rosters.

## 7. Reproducing / extending

```bash
# 1. regenerate the data payload (Windows-side env)
<irt-python> scratch/_palette_mockup.py

# 2. re-render the page from mockup_data.json (any python3)
#    (page builder lives in the session scratchpad; it is ~150 lines of
#     f-string templating over the JSON — trivial to recreate or inline)
```

To compare different palettes, edit only the `palettes` section of the
generator; to use a different metric/state, change `PARQUET`/`VALUE_COL` and
the geometry filter. The gate (monotone L, min ΔL ≥ 0.06) should be kept for
any candidate sequential ramp.
