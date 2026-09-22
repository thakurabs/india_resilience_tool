# IRT Visual Design Benchmark — vs. WRI Aqueduct, Global Forest Watch, CRAVIS.ai

**Scope:** Visual design language only — how the tool *looks and feels* (color, typography, spacing, iconography, hierarchy). This review does not assess features, analytics, or data correctness.

**Audience considered:** Policymakers, district officials, corporate donors, researchers, general public, hydrology/industry practitioners.

**Method:** Comparative visual audit of IRT Figma screens (17) and the live IRT landing page against three reference tools (WRI Aqueduct, Global Forest Watch, CRAVIS.ai).

---

## How to read this report (for non-designers)

Design language is the equivalent of "tone of voice" for a software product. Two tools can offer the same data and the same features, yet feel very different — one feels like a polished, trustworthy product from a serious institution; the other feels like an internal admin screen. That difference is created almost entirely by five visual ingredients:

1. **Color** — which colors are used, where, and how much of each. Good tools use color sparingly for the UI itself and save bright colors for the data.
2. **Typography** — the fonts, sizes, and weights of text. Good tools use a small, consistent set of text sizes, so the eye knows what is a title, what is a label, and what is a number.
3. **Spacing** — how much breathing room sits between elements. Good tools use *consistent* spacing; cramped or excessively empty layouts both feel amateurish.
4. **Iconography** — the small pictograms used for layers, info, filters, downloads. Good tools have a single, consistent icon set; missing icons make a tool feel text-heavy and unwelcoming.
5. **Hierarchy** — which element on the screen is largest/loudest, and which recede. Good tools make the *content* (the map, the data) the loudest thing, and make the *controls* (filters, panels) quieter.

The rest of this document scores IRT against the three benchmark tools on each of these five ingredients, then converts the findings into a prioritized list the designers can act on.

---

## 1. Executive summary

**The primary concern of this review is the look and feel of the tool.** A climate intelligence tool used by policymakers, donors, researchers and the public will be judged within the first five seconds — long before any user evaluates its analytics. If the look and feel is sub-standard, even an excellent feature set will fail to build trust. The benchmarks (Aqueduct, GFW, CRAVIS.ai) all look like products from credible institutions; **IRT currently does not.**

The dominant impression of the IRT screens is of a 2015-era admin template:

- **Three different shades of blue** are used inside the chrome (navy header text, bright cyan filter bar, lighter teal section headers in the left panel) with no clear rule for which means what.
- **Large flat blocks of saturated cyan** dominate the screen, leaving little visual room for the map or the data.
- **Text sizes are inconsistent** — the tool title, panel headings, dropdown labels, and helper text all read at similar visual weight.
- **The landing page is mostly empty grey space** with a fixed-width filter cluster pinned to the top-left; the layout does not respond to the viewport.
- **Icons are almost entirely absent**, leaving technical terms like "Risk Domain", "Scenario", "Statistic", "Map Mode" unexplained.

By contrast, all three benchmark tools share three traits IRT lacks:

- A **restrained primary palette** (one neutral chrome color + one accent) with bright color reserved exclusively for the data.
- A **disciplined typographic scale** (clear, predictable text sizes) and a modern font family (Inter, IBM Plex, Manrope or similar).
- A **map-first layout** where the map is the dominant canvas and controls collapse to thin rails or icon menus.

### Top five fixes that will move the needle the most

1. Replace the solid cyan panel fills with a neutral surface; reserve cyan for accent only. Settle on **one** blue, not three.
2. Adopt a modern sans-serif font family (Inter, IBM Plex Sans, or Barlow) and define a strict size/weight scale.
3. Re-architect the screen so the **map is the dominant element**; collapse control rails and let panels respond to viewport width.
4. Introduce a consistent icon set and place an info (`?`) icon next to every technical term.
5. Replace the traffic-light risk legend with a scientific sequential color ramp (e.g., ColorBrewer YlOrRd) that is colorblind-safe.

---

## 2. Stakeholder feedback (raw observations from the project team)

The following four observations were raised by the project team during initial review and are amplified throughout the dimension-by-dimension analysis in section 3.

### F1. The landing page violates basic layout principles

> *"Basic layout design principles have not been followed. Excessive white/negative space. The resilience filters are also static — the size should adjust dynamically."*

**Context.** On the live landing page (before any geography is selected), roughly **50% of the canvas is empty grey space** to the right of the filter cluster. When the left "Geography Selection" rail is collapsed via the toggle arrow, the filter cluster does **not** expand to fill the freed width — it stays pinned at a fixed size in the top-left corner, exposing even more empty area.

This is a clear violation of two foundational layout principles:

- **Responsive proportionality:** primary controls should occupy a proportion of the screen that reflects their importance, and should reflow when adjacent panels open or close.
- **Visual completeness on first paint:** a user landing on the tool for the first time should see something that looks "complete." Large empty regions read as a broken page or a slow load, not an intentional design.

None of the benchmark tools have this problem: Aqueduct shows a full world map on first paint; CRAVIS shows a full India map; GFW shows a full continent map. The user immediately understands "this is a map tool."

**Recommendation.** On first paint, IRT should already render a full India map behind the filter panel (greyed-out, illustrative, or with a "Select a state to begin" prompt overlay). The filter panel should be a **fluid container** that expands and contracts with the available width, not a fixed-width box.

### F2. The font family is dated and can be much better

> *"Font family can be much better — sans-serif family looks promising (Inter / IBM Plex / Barlow Condensed)."*

**Context.** The current font appears to be a default system sans-serif (Arial or similar). This signals "unstyled" to design-literate users. Modern climate/geospatial tools have all moved to humanist or geometric open-source sans-serifs that carry an institutional, modern feel.

**Recommendation.** Adopt **one** of the following as the single product font:

| Font | Personality | Best for |
|------|-------------|----------|
| **Inter** | Neutral, highly legible at small sizes | Default safe choice; what most modern data products use |
| **IBM Plex Sans** | Slightly more institutional/editorial | Strong fit if IRT wants a "research credibility" tone |
| **Barlow** / **Barlow Condensed** | Modern, slightly condensed | Best when screen real-estate is tight (which IRT panels are) |

All three are free, open-source, and load reliably via Google Fonts. Pick one for body + headings, and optionally pair with a tabular numeric variant for the ranking table.

### F3. Inconsistent use of color — three shades of blue look odd

> *"Inconsistent usage of colours — three shades of blue looks very odd."*

**Context.** A quick scan of the landing screen shows at least three distinct blues:

- A **navy/dark blue** used for the "India Resilience Tool" title text.
- A **saturated bright cyan** used as the filter-bar background.
- A **slightly lighter teal/cyan** used for section headings inside the left "Geography Selection" panel.

There is no apparent rule for which blue means what. This is the visual equivalent of writing a paragraph in three different fonts — readers don't consciously notice the rule violation, but they do feel the lack of polish.

**Recommendation.** Define exactly **one** primary brand blue, plus a defined hover/active variant (lighter and darker tints from the same hue). Every other blue in the system must be either the primary, a tint of it, or removed. Document this in a single color-token table.

### F4. Inconsistent font sizing across the dashboard

> *"Font sizing is inconsistent. A consistent choice has to be made across the dashboard."*

**Context.** Across the IRT screens, the tool title, panel section headers ("Geography Selection", "Coordinate Panel"), filter labels ("Risk Domain", "Metric"), dropdown placeholder text, helper text, and ranking-table column headers all sit within a narrow size band — making it hard for the eye to know which is the most important element on the screen. By contrast, CRAVIS.ai uses very large numbers (24.9, 143) for headline data and very small labels for everything else; the hierarchy is unmistakable.

**Recommendation.** Lock in a strict type scale (see section 3.2 for the proposed scale) and audit every screen against it. No text in the product should use a size that is not on the scale.

---

## 3. Dimension-by-dimension comparison

### 3.1 Color

| Tool | Primary chrome | Data palette | Verdict |
|------|----------------|--------------|---------|
| **IRT** | Three uncoordinated blues (navy title + bright cyan filter bar + teal section heads). Saturated cyan used as **large flat fills** on filter bar, side panel, buttons. White body. | Risk band: pure green → yellow → orange → red → dark red (traffic-light primaries). Selected geography outlined in **bright red**. | Chrome competes with data; the multiple blues create visual noise; red selection clashes with red risk band; palette feels alarmist and non-scientific. |
| **Aqueduct** | Deep navy left panel, white map, single yellow accent for active state. | Sequential & diverging cartographic ramps (ColorBrewer-style). Restrained. | Chrome recedes; user's eye goes to the map. |
| **GFW** | Off-white panel, brand green only as accent, dark text. | High-saturation magenta for tree-cover-loss is intentional and mono-purpose. | Strong brand presence without overwhelming. |
| **CRAVIS.ai** | Dark slate basemap with white labels; left rail is dark; map is the bright surface. | Sequential yellow→orange→red choropleth; blue for water bodies. | Premium, modern, "intelligence-platform" feel. |

**Findings for IRT designers**

- **Resolve the three-blues problem first** (stakeholder feedback F3). Define one primary blue plus a defined hover/active tint and apply consistently. This single change will visibly raise the perceived quality of the tool.
- The **solid cyan filter bar + cyan side panel** create ~30% of the screen as a single non-data color. None of the benchmarks do this. Move to a neutral surface (e.g., `#F4F6F8` or a navy left rail) and use cyan only for interactive accents (button, active tab, focus ring).
- The **red boundary stroke** around the selected geography conflicts with the red end of the risk legend. Use a neutral high-contrast outline (e.g., black or a dashed dark stroke) — Aqueduct and CRAVIS handle selection via outline weight, not hue.
- The risk legend uses **fully saturated primaries** (`#00C800`-ish green, pure red) which read as alarming and aren't colorblind-safe. Adopt a **ColorBrewer YlOrRd or RdYlBu** ramp; this is industry standard across all three benchmarks.
- Define color tokens explicitly: `surface/default`, `surface/raised`, `border/subtle`, `text/primary|secondary|muted`, `accent/primary`, `data/seq-1..5`, `data/div-…`. None of these appear to be systematized in the current screens.

### 3.2 Typography

| Tool | Treatment |
|------|-----------|
| **IRT** | Appears to use a default system sans-serif. Single weight, single size for nearly all labels. "India Resilience Tool" header is sized similarly to filter dropdown labels. Dropdown labels, section headers, and ranking-table column heads are visually indistinguishable (stakeholder feedback F4). |
| **Aqueduct** | Clear scale: section caps, indicator label, sub-parameter, helper `?`. Generous line-height. |
| **GFW** | Section caps in tracked uppercase, layer title in semibold, subtitle in regular muted, metadata small. Five clearly discernible levels in a tight rail. |
| **CRAVIS.ai** | Display numerics ("24.9", "143") at very large weight with small metadata below — uses size to tell the data story. Modern geometric sans (Inter / Manrope family). |

**Findings for IRT designers**

- Adopt a single modern open-source font family (stakeholder feedback F2) — recommended: **Inter** as the default safe choice, **IBM Plex Sans** for a more institutional/research tone, or **Barlow / Barlow Condensed** if panel real-estate stays tight.
- Establish an explicit type scale, and audit every screen against it. Recommended starting point:

| Role | Size / Weight | Use |
|------|---------------|-----|
| Display (data callouts) | 32–40px / 600 | Big numbers on the map (district headline metrics) |
| H1 (page / tool title) | 22–24px / 600 | "India Resilience Tool" header |
| H2 (panel section) | 14px / 600 / UPPERCASE / +0.04em tracking | "GEOGRAPHY SELECTION", "RESILIENCE FILTERS" |
| Body label | 13–14px / 500 | Dropdown labels, button text |
| Helper / metadata | 12px / 400 / muted | "Choose a geography to begin analysis" |
| Numeric (tabular) | 13–14px / 600 / tabular-nums | Ranking-table values |

- The header lockup "India Resilience Tool" currently sits in a thin white bar at modest weight. It should be the most confident piece of type on the screen — increase weight and size, and pair it with a small "Climate Risk & Resilience Atlas" tagline for first-time visitors (donors, public).
- **Ranking Table** screen: column heads, district names, and numeric values all read at similar visual weight. Numbers should be tabular and visually heavier than headers; headers should be smaller tracked caps. Compare to how Aqueduct/CRAVIS treat tabular numerics.

### 3.3 Spacing & layout density

| Tool | Density |
|------|---------|
| **IRT** | On the landing page, ~50% of the canvas is empty grey space; the filter cluster is fixed-width and does not expand when the left rail collapses (stakeholder feedback F1). On the analysis screens, the filter bar packs 6 dropdowns into a tight cyan row, the left panel stacks 8+ controls with minimal vertical rhythm, and the right "My Analysis" panel adds a third column — **three vertical rails competing around a relatively small map.** |
| **Aqueduct** | One left panel, full-bleed map. ~12–16px padding inside cards; ~24–32px between panel sections. |
| **GFW** | One left rail + thin icon nav. Generous vertical rhythm even though list is long. |
| **CRAVIS.ai** | Collapsible left rail; map takes 75%+ of canvas. Data callouts float over map with strong shadow. |

**Findings for IRT designers**

- **Fix the landing page first.** On first paint, render a full India map (greyed or illustrative) behind the filter panel, with a clear "Select a state to begin" overlay. Make the filter panel a **fluid container** that expands and contracts with viewport width.
- On the analysis screens, the map currently occupies roughly **45–55% of the canvas** when both side panels are open. Benchmarks consistently give the map **65–80%**. Consider:
  - Collapse the left "Geography Selection" panel into a top search bar + collapsible accordion.
  - Move "My Analysis" / "Compare Portfolio" panels to a slide-over drawer triggered by an icon, not a permanent rail.
- Introduce an **8-point spacing scale** (4 / 8 / 12 / 16 / 24 / 32 / 48). The current screens look like everything sits on a 4-pixel grid with inconsistent application.
- The Quick Guide modal floats centered over the map and partially obscures the geography being selected — move it to a non-blocking side coach-mark with a pointer line.

### 3.4 Iconography

| Tool | Icon system |
|------|-------------|
| **IRT** | Almost none. No layer icons, no info `?` next to filters (Domain, Metric, Scenario, Period — all unexplained), no locate/zoom controls visible in chrome, no download icon. |
| **Aqueduct** | `?` info icon next to every indicator; ▾ disclosure arrows; map control stack on the right. |
| **GFW** | Vertical icon nav (Land Cover, Land Use, Climate, Biodiversity, Explore), per-layer thumbnail, info `i`, toggle pill. |
| **CRAVIS.ai** | Compact icon rail (Atlas, Layers, Climate Metrics, Precipitation, Hot Weather, Risk Index), info icons, expand carets, locate. |

**Findings for IRT designers**

- This is **the single biggest visual gap**. IRT has technical concepts (Risk Domain, Metric, Scenario, Period, Map Mode) that *demand* inline `?` affordances. New users (public, donors) cannot proceed without methodology context.
- Adopt a single icon library (Phosphor, Lucide, or Tabler — all open-source, 1.5px stroke, consistent grid). Define stroke weight, size (16 / 20 / 24px), and color tokens.
- Required icon vocabulary at minimum: search, location pin, layers, info, filter, download, share, expand/collapse, settings, help, close, chevron, eye (visibility toggle), check.
- Replace the "Resilience Actions" multicolored bar logo with a cleaner mark when used inside the tool chrome — the current logo competes visually with the data legend due to its rainbow stripe.

### 3.5 Visual hierarchy

| Tool | Dominant element | Secondary | Tertiary |
|------|------------------|-----------|----------|
| **IRT** | Cyan filter bar + cyan panels (chrome) | Map | Data callouts barely present |
| **Aqueduct** | Map | Left indicator panel | Tooltip legend (bottom-right) |
| **GFW** | Map | Layer rail | Icon nav (thinnest) |
| **CRAVIS.ai** | Map + data callout cards | Left icon rail | Time/period selector top |

**Findings for IRT designers**

- IRT's hierarchy is **inverted**: chrome dominates, map is supporting, and the actual data story (district values, trends, comparisons) is buried inside the right "My Analysis" panel rather than surfaced on the map. The benchmarks all do the opposite.
- Borrow from CRAVIS: when a district is selected, surface a **floating data callout card** on the map (district name, headline metric, trend sparkline, "open profile" CTA). This makes the tool feel insight-led, not control-led.
- The **Quick Guide tour** is visually identical in weight to the data panels — a tour overlay should clearly recede (lower z-index visual treatment, softer shadow, distinct accent color) so it doesn't compete with the live UI it's explaining.
- The Compare Portfolio / Refine Filters / Saved Analysis stack on the right is a flat list of similar-looking blue blocks. Use a tabbed or accordion pattern with clear active/inactive states so users perceive a single workspace, not a wall of buttons.

---

## 4. Audience-specific gaps

| Audience | What benchmarks do well that IRT misses |
|----------|------------------------------------------|
| **General public / donors** | Aqueduct & CRAVIS open with a clean map and one strong headline — no login wall, no empty grey landing. IRT's sign-in-first flow + dense, half-empty control surface is intimidating. Visually, the tool should feel exploratory before it feels analytical. |
| **Policymakers / district officials** | CRAVIS surfaces a single big number per district (24.9°C, 143 mm). IRT buries the equivalent inside table rows. Big-number callouts read as authoritative. |
| **Researchers / hydrology consultants** | Aqueduct provides per-indicator `?` with full methodology link. IRT exposes Domain/Metric/Scenario/Period with no inline explanation. |
| **Corporate donors** | All three benchmarks feel "branded and trustworthy" through restraint. IRT's saturated blocks, three blues, and primary-color risk legend feel utilitarian and reduce perceived trust — which directly affects fundraising conversations. |

---

## 5. Prioritized recommendations (designer-actionable)

| Priority | Recommendation | Where it lands |
|----------|----------------|----------------|
| **P0** | Fix the landing page: render a full India map on first paint; make the filter panel a fluid container that responds to viewport width. | Landing screen |
| **P0** | Resolve to a single brand blue (+ defined tints). Remove the three-shades-of-blue inconsistency. | All screens |
| **P0** | Adopt a modern open-source font (Inter / IBM Plex Sans / Barlow) and document a strict type scale. Audit every screen against it. | All screens |
| **P0** | Re-skin chrome: replace solid cyan fills with neutral surface (`#F6F8FA` or navy rail). Reserve the brand blue for accents only. | Filter bar, side panels, header |
| **P0** | Adopt a 16/20/24px icon set; place `?` info icons next to every methodology-bearing control. | Filters, panels, table headers |
| **P0** | Replace risk-band traffic-light palette with a ColorBrewer-style sequential ramp; verify colorblind safety (deuteranopia / protanopia). | Map choropleth, ranking table, legend |
| **P1** | Re-architect to map-first: collapse "My Analysis" to a slide-over; merge "Geography Selection" and "Coordinate Panel" into one tabbed left rail. | All map screens |
| **P1** | Add floating data callout cards on district select (headline metric + trend + CTA). | Map screens |
| **P1** | Differentiate selection outline color from risk-band red. | Map screens |
| **P2** | Redesign Quick Guide as non-blocking coach-marks with pointer lines. | Onboarding overlay |
| **P2** | Strengthen header lockup: bigger weight, tagline, cleaner co-branding with Resilience Actions. | All screens |
| **P2** | Establish 8pt spacing scale and audit panel padding against it. | All screens |

---

## 6. Closing note for the design team

 **The risk is not the feature set — it is that a sub-standard look and feel will undermine credibility before users get to the features.** Each of the recommendations above is a known, well-documented pattern in the modern web-product community; none requires invention, only application. The fastest visible win for the project will come from the four P0 items: one font, one blue, a real landing page, and a quieter chrome.
