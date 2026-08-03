# Figure Correction Instructions — Essential Batch (post-review)

Applies to the SVGs and generator in `docs/figures/technical_guidance_note/`
(`generate_essential_figures.py`, producing FIG-01, 06, 08, 09, 12, 14, 18, 20).
Source of truth for content remains `docs/technical_guidance_note.md` and
`docs/figure_gen_instructions.md`; **this file only fixes rendering quality, not
methodology**. All content/number checks in the prior review passed — do not
change any value, formula, weight, threshold, or label wording while fixing layout.

Correction priority:
- **P0 — FIG-01** (broken: text overflow, hidden boxes, stubby/crossing arrows, duplicate composite node).
- **P1 — FIG-20** (callout text overflows its box), **FIG-12** (annotation text spills past the plot).
- **P2 — global generator hardening** (arrowheads, text-fit guard, PNG export) so the class of bug cannot recur.
- **Visual-QA only — FIG-06, 08, 09, 14, 18** (geometrically clean; eyeball once rendered).

---

## Part 1 — Global generator defects (fix once, benefits every figure)

### G-1. Arrowheads scale with stroke width and look like fat wedges
The `<marker id="arrow">` uses the default `markerUnits="strokeWidth"`, so the head
size is multiplied by the `.arrow` stroke width (2.2×). Combined with short
connectors this produces the "peculiar" stubby triangles in FIG-01.

**Fix:** make the marker a fixed physical size and give it a crisp shape.
```xml
<marker id="arrow" viewBox="0 0 12 12" refX="10" refY="6"
        markerWidth="12" markerHeight="12" markerUnits="userSpaceOnUse"
        orient="auto-start-reverse">
  <path d="M 1 1 L 11 6 L 1 11 z" fill="#52606d"/>
</marker>
```
Keep `.arrow` stroke at ~2.0 and **never** let a connector be shorter than ~3× the
head length (≥ 36 px of visible line). Where two boxes are close, either widen the
gap or draw the connector as an orthogonal elbow (see G-2), not a 17 px stub.

### G-2. Connectors are too short and are drawn box-edge to box-edge
Straight arrows squeezed into a 32 px gap read as noise. Adopt one connector style
and use it everywhere:
- **Horizontal flow:** start 2 px right of the source box, end 2 px left of the
  target box, minimum 40 px gap between boxes so the line is clearly visible.
- **Branch / vertical routing:** use **orthogonal elbows** (H→V→H via `<path d="M … L … L … L …"/>` with the arrow marker only on the final segment), not diagonal
  arrows across the diagram. Diagonal arrows crossing other elements (as the current
  FIG-01 rejoin arrow does) are not allowed.

### G-3. Boxes have no text-fit guarantee → labels overflow
`box()` and the timeline labels place fixed-size text into fixed-width boxes with no
measurement, so long strings run outside the box (9 overflows in FIG-01, 1 in FIG-20).
Add a **width-aware text layer** to the generator and route all in-box text through it:

1. Add a monospace-free width estimate (Arial):
   `approx_px(s, font_px, bold) = len(s) * font_px * (0.60 if bold else 0.53)`.
2. Add `wrap(text, max_px, font_px, bold)` that greedily breaks on spaces into lines
   each ≤ `max_px`; return the list of lines.
3. In `box()`: wrap the **label** to `w - 24` (may become 2 lines) and wrap each
   **subline** to `w - 20`; compute the total text block height and either (a) grow
   `h` to fit, or (b) vertically center the block. Never emit a line wider than the box.
4. Add an **assertion pass**: after building each figure, parse its own text vs the
   box it sits in and `raise` if any line exceeds its box (turn the review's audit
   into a build-time guard — see G-5).

Interim rule if wrapping is deferred: no box may be narrower than the longest line it
carries at its font size. Practically that means stage boxes in FIG-01 need `w ≥ 180`
**and** 2-line titles, or shorter labels.

### G-4. Opaque boxes are painted over other content (z-order collisions)
The FIG-01 "Scope note" is an opaque fill drawn *after* the Thematic/Sectoral/Composite
nodes, so it hides them. **Rule:** reserve a clear rectangle for every annotation box
and assert it does not intersect any other drawn box (the G-5 self-check covers this).
Annotation/legend/note boxes go in whitespace, never on top of flow nodes.

### G-5. Add a self-check and a raster export (prevents regressions)
- **Self-check (`validate()`):** reuse the review's geometry audit inside the module —
  after generating each figure string, parse `<rect fill=…>` boxes and `<text>`, and
  `raise AssertionError` on (a) any text line whose estimated extent exits its enclosing
  box, (b) any two *flow/annotation* boxes overlapping by > 3 px (exclude chart bars,
  which are legitimately inside panels — tag those rects with `class="bar"` so the check
  can skip them), (c) any element outside the canvas. Run it in `main()`.
- **PNG/PDF export:** `figure_gen_instructions.md` asks for 300-dpi PNG / vector PDF.
  Add an optional export step (e.g. `cairosvg` if available; otherwise document the
  `rsvg-convert -w 2400` command) writing `fig_nn_*.png` beside each SVG.
  **When PNG export is added, also add `!docs/figures/technical_guidance_note/*.png`
  to `.gitignore`** (the current `**` exception is being narrowed to `*.svg`/`*.py`
  per the CHG-0203 review item, so new extensions must be opted in explicitly).

---

## Part 2 — Per-figure corrections

### FIG-01 — End-to-End Pipeline Flow  (P0, redesign)

Four defects: (1) 5 stage boxes overflow their titles/sublines; (2) the Scope-note box
hides the Thematic and Sectoral boxes; (3) a redundant "Composite" node sits below the
row in addition to the "Composite output" stage — the "two boxes between bundle
construction and composite" you saw are the half-hidden Thematic/Sectoral pair, and the
duplicate Composite adds to the confusion; (4) arrows are stubby and one crosses the
scope note diagonally.

**Redesign — inline the thematic/sectoral split, remove the lower sub-diagram entirely.**

1. **Canvas & spacing.** Widen to `width=1320` (keep `height=760`). Stage geometry:
   `x0=36, w=182, gap=42, y=150, h=140`. (6 stages → `36 + 6*182 + 5*42 = 1338`? no —
   recompute so the last box right edge ≤ 1300: use `w=178, gap=38` →
   `36 + 6*178 + 5*38 = 36+1068+190 = 1294`, right margin 26. Good.)
2. **Two-line titles.** Wrap every stage title to ≤ 2 lines via G-3, e.g.
   `Grid-first index compute` → `Grid-first index / compute`;
   `Spatial + temporal aggregation` → `Spatial + temporal / aggregation`. Shorten
   sublines: `daily -> annual -> period -> ensemble` → two lines
   `daily → annual →` / `period → ensemble`.
3. **Stage 5 becomes the split, not a single box.** Replace the single "Bundle
   construction" box with a labelled column header ("Bundle construction") over **two
   stacked half-height boxes** in that column: **Thematic** (top) and **Sectoral**
   (bottom), each ~ `w × (h/2 − 6)`. Both draw a short horizontal arrow into stage 6.
4. **Stage 6 is the only composite.** Keep one "Composite output" box (0–100 higher =
   worse; district & block views). **Delete the separate floating "Composite / hazard
   pressure" node** — it duplicated stage 6.
5. **Scope note relocation.** Move it to clear whitespace **below** the pipeline row,
   left-aligned and clear of everything: a full-width slim banner at `x=36, y=330,
   w=1258, h=64` ("Composite scores are hazard-pressure indices; exposure and
   vulnerability are not inputs."). It must not intersect any flow box (assert via G-5).
6. **Arrows.** Use the G-1 marker and G-2 spacing. Between stages 1→4 use straight
   horizontal connectors in the 38 px gaps. For the stage-5 split, draw the two
   Thematic/Sectoral→Composite arrows horizontally (they are already adjacent columns) —
   no diagonal rejoin, no vertical drop.
7. **Section strip** stays but move to `y=470` so it clears the relocated scope banner;
   keep its own arrows on the G-1 marker.
8. Re-run the G-5 self-check; expect zero overflow/overlap findings.

### FIG-20 — District A vs B  (P1)
The callout box `box(755, 625, 350, 66, …)` carries the line *"Fast-warming and newly
above onset; blended score rises from 20 to 47."* (~445 px) in a 350 px box → ~47 px
overflow each side. **Fix:** widen the callout to `w=470` (and shift `x` to `695` so it
stays centered under Panel B and within the canvas), **or** wrap the sentence to two
lines via G-3. Nothing else in this figure needs changing (bars, axes, and the worked
values are correct).

### FIG-12 — DOY Percentile Threshold Curve  (P1)
The two legend lines `tau_d: baseline 90th percentile` and
`orange points: evaluation-year exceedances` are left-anchored at `x0+w-240` and run
~39 px / ~27 px past the plot's right border (into the white margin). **Fix:** right-
anchor them at the plot's inner right edge (`anchor="end"`, `x = x0+w-16`) **or** place
them in a small legend box inside the upper-right of the plot with a light fill. Keep
them clear of the τ_d curve. Everything else (window, DOY 121, exceedance dots, method
note) is correct.

### FIG-06, 08, 09, 14, 18 — geometrically clean (visual-QA only)
No overflow/overlap in the audit. Once a rasterizer is available, eyeball each for the
items below and adjust only if they read poorly; otherwise leave as-is:
- **FIG-06:** the two CDF curves (`F_mod`, `F_obs`) must be visually distinct and their
  `F_mod`/`F_obs` labels sit on the correct curve; confirm the coarse 3×3 and fine 8×8
  grids read as "coarse → fine".
- **FIG-08:** district polygon vs block sub-polygons vs grid lines must be separable by
  value, not hue alone; the block-zoom highlighted block (rose outline) should be obvious.
- **FIG-09:** confirm the 35 °C threshold dashed line, the city (5 crossings) vs valley
  (0) series, and the "2.5 hot days" result box all read at a glance; check the series
  legend text doesn't collide with the day labels.
- **FIG-14:** confirm panel-3 vertical axis (SPI z) and the SPI<−1 shaded tail line up
  with the normal-quantile curve; check the three inter-panel arrows are level.
- **FIG-18:** three arrows fan **in** to the rule-score box and three fan **out** to the
  rule nodes, plus three fan **out** from Inputs — verify these clusters don't tangle;
  if they do, convert to short orthogonal elbows (G-2).

---

## Part 3 — Re-acceptance checklist (run before re-committing)

1. `python3 -m py_compile generate_essential_figures.py` passes.
2. `main()` regenerates all 8 SVGs **and** the new `validate()` self-check raises nothing.
3. Re-run the geometry audit (text-in-box, box-overlap, off-canvas) → zero findings on
   FIG-01/12/20; unchanged-clean on the rest.
4. Rasterize to PNG and eyeball FIG-01 (branch legible, no hidden boxes, crisp arrows),
   FIG-20 (callout fits), FIG-12 (legend inside plot).
5. Confirm no content value changed vs the prior review (diff only touches geometry,
   text-wrapping, arrows, and canvas size).
6. `git add -n docs/figures/` shows only `*.svg`, `*.py` (and `*.png` if export added) —
   never `__pycache__/*.pyc` (CHG-0203).
