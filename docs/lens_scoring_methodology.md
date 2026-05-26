# Lens-Based Scoring Methodology for Sectoral Climate-Risk Bundles

This document defines the **lens-based scoring methodology** used to convert
climate-index metrics into 0-100 sector hazard-pressure scores in the India
Resilience Tool (IRT). It is the methodological companion to the per-bundle
dossiers in `docs/bundle_calculation_audit.md`: the audit doc records *what each
bundle computes today*; this doc defines *the scoring framework, its scientific
basis, and the per-metric lens reasoning* for the sectoral bundles.

Scope of this document:
- Sections 1-5 define the framework (purpose, the three lenses, scientific
  basis, impact-band provenance policy, score-decomposition / persistence
  schema).
- Section 6 onward give the **metric-by-metric lens dossiers** per sector
  bundle. Health Risk (Section 6) is the worked template; the remaining sector
  bundles follow the same structure.
- Extension of the lens machinery to the **thematic** bundles is deferred until
  the sectoral bundles are complete (see Section 8).

Implementation references (current code):
- Rule catalog: `india_resilience_tool/config/proposal_bundles.py`
- Proposal builder: `india_resilience_tool/compute/proposal_bundles.py`
- Dashboard catalog: `india_resilience_tool/config/dashboard_bundles.py`
- Metric registry: `india_resilience_tool/config/metrics_registry.py`

---

## 1. Purpose and Scope

### 1.1 What these scores are

The sectoral bundles produce a **0-100 climate hazard-pressure score** for each
admin geography (district / block), scenario (`ssp245`, `ssp585`), and future
period (`2020-2040`, `2040-2060`, `2060-2080`). A higher score always means
higher climate hazard pressure relevant to that sector.

### 1.2 What these scores are NOT (Phase-1 caveat)

These are **hazard-pressure** scores, not full sectoral *risk* scores. The IPCC
AR6 risk framing defines risk as the interaction of three determinants:

```text
Risk = f(Hazard, Exposure, Vulnerability)
```

The current compute path models the **hazard** determinant only. It does not yet
incorporate:
- **Exposure** (population, assets, economic value, cropped area in the path of
  the hazard); or
- **Vulnerability / adaptive capacity** (sensitivity, coping capacity,
  infrastructure quality, socio-economic resilience).

This is a deliberate Phase-1 scope. Sector labels such as "Health Risk" or
"Investment / Financial Risk" should therefore be read as *"climate hazard
pressure relevant to that sector"*, and any UI presentation should make the
hazard-only scope explicit. Promoting these to true *risk* scores requires
adding exposure and vulnerability layers, which is out of scope for this
methodology.

Reference: IPCC (2021), *The concept of risk in the IPCC Sixth Assessment
Report* (see Section 3).

---

## 2. The Three Lenses

Every metric in a sector bundle can be scored through up to three **lenses**.
Each lens answers a different, complementary question about the same underlying
climate metric. The lens names — **absolute**, **change**, **impact** — are
retained because they are the most representative of what each measures.

| Lens | Question it answers | Anchored to |
|---|---|---|
| **absolute** | "How extreme is the projected value compared with its peers?" | The peer cohort's distribution (relative) |
| **change** | "How much worse is the projected value than its own history?" | The historical baseline (anomaly) |
| **impact** | "How far into a physically/operationally dangerous range is the value?" | A fixed, externally-justified threshold band (absolute physical meaning) |

A rule's final score is the **weighted mean of its active lenses**. A rule may
use one, two, or all three lenses; lenses with zero weight are inactive for that
rule.

### 2.1 Absolute lens

The absolute lens scores **where the projected value sits within its peer
cohort**. The cohort is the set of geographies sharing the same
`state x level x scenario x period`. Concretely, a **district** is scored
against all other districts **in the same state**, and a **block** is scored
against all other blocks **in the same state** — *not* against the blocks of its
own district only. A state-wide block cohort is used deliberately: it keeps the
p10/p90 bounds robust (a single district often has too few blocks for a stable
distribution), keeps blocks comparable across districts, and leaves the
"is this block locally dangerous?" question to the cohort-independent impact lens.
Scoring uses a robust min-max rescaling between the cohort's 10th and 90th
percentiles:

```text
absolute_score = clip( (value - p10) / (p90 - p10), 0, 1 ) * 100
```

- p10/p90 (rather than min/max) bound the influence of single-cell outliers, as
  recommended for composite indicators.
- If the cohort's p10 and p90 are equal (no spread), valid rows receive a flat
  mid-scale score and a flatness diagnostic is emitted.
- Direction: for `higher_worse` metrics the formula is as above; for
  `lower_worse` metrics the scaled value is inverted (`1 - x`).

**What it captures:** relative prioritization — which geographies are worst-off
*right now in the selected future*.

**Structural limitation:** because the cohort is rebuilt per scenario/period,
the absolute lens is **blind to uniform escalation**. If every geography in a
state warms by the same amount between two periods, the p10 and p90 both shift
and the normalized scores are unchanged. The absolute lens can rank places
against each other; it cannot, by itself, show that the future is worse than the
present. This is the gap the change and impact lenses fill.

### 2.2 Change lens

The change lens scores the **climate-change signal**: the anomaly of the
projected value relative to the historical baseline, then rescaled across the
cohort's anomalies.

```text
delta            = value(future) - value(baseline)        # absolute_delta mode
delta            = 100 * (value(future) - value(baseline)) / |value(baseline)|   # relative_pct mode
change_score     = clip( (delta - p10_delta) / (p90_delta - p10_delta), 0, 1 ) * 100
```

- `absolute_delta` is used for temperature-like metrics (a degree is a degree);
  `relative_pct` is used for quantities where proportional change is the
  meaningful signal (e.g. dry-spell length).
- The baseline must be the **reconciled historical baseline** (see Section 2.5).

**What it captures:** trajectory / deterioration. A geography that is only
moderately hazardous today but is deteriorating fastest is surfaced by this lens
even when the absolute lens rates it low. This is the "fast-warming, not-yet-
extreme" signal (see the worked example in Section 2.6).

### 2.3 Impact lens

The impact lens scores the value against a **fixed threshold band** with real
physical, physiological, or engineering meaning — not a relative percentile.

```text
impact_score = clip( (value - band_low) / (band_high - band_low), 0, 1 ) * 100   # higher_worse
```

- Below `band_low` the score is 0 (no material impact yet); above `band_high`
  the score saturates at 100 (severe regime).
- The band must be justified — see the provenance policy in Section 4. Every
  band carries a cited source and a date.

**What it captures:** absolute physical danger. Unlike the absolute lens, a value
of 0 or 100 here means something real ("below the harm threshold" / "in the
severe regime"), independent of how peers are doing. This is also the only lens
that is fully comparable across cohorts, periods, and states.

**What each lens lets you compare.** Because two of the three lenses are
cohort-relative, not every comparison of the composite is valid. The blended
score is a *hybrid* of relative ranking (absolute, change) and absolute danger
(impact), so users must read it accordingly:

| Comparison | absolute | change | impact | Net interpretation |
|---|:--:|:--:|:--:|---|
| Units **within** one `state x level x scenario x period` | yes | yes | yes | fully comparable — a true ranking of units |
| **Across periods** (e.g. 2020-40 vs 2060-80), same state | re-normalised each period | re-normalised | **yes (fixed band)** | only the impact component is comparable; a flat absolute trend means "same rank", not "no warming" |
| **Across states** | each state normalised to itself | itto | **yes (fixed band)** | only the impact component is cross-state comparable |

The practical consequence: the **impact lens is the only carrier of absolute
escalation** over time and space (the District-B reason it exists, Section 2.6).
This is why metrics where escalation matters should retain an impact lens
wherever a defensible danger band can be sourced or derived (Section 4).

### 2.4 Combining lenses into a rule score

```text
rule_score = sum(lens_score_i * lens_weight_i for active lenses)
           / sum(lens_weight_i for active lenses)
```

Missing components are dropped and the remaining weights renormalize row-wise, so
a rule still produces a partial, explainable score when one lens is unavailable
(e.g. a missing baseline disables only the change lens).

### 2.5 Reconciled baseline (prerequisite for the change lens)

The change lens compares the future against a historical baseline. To stay
consistent with the dashboard's historical-delta columns, **all change-lens
baselines must use the `1990-2010` historical period** used elsewhere in the
tool. Any deviation (e.g. the legacy `1995-2014` / `1985-2014` proposal-builder
tokens) must be reconciled before the change lens is treated as trustworthy. The
absolute and impact lenses do not depend on a baseline.

### 2.6 Worked example — why the lenses matter (Districts A and B)

This example makes the trade-off tangible. Consider two districts in the same
state, scenario `ssp585`, period `2060-2080`, for the metric **TXx** (annual
maximum daytime temperature). Suppose within this cohort the projected TXx ranges
from p10 = 41 deg C to p90 = 46 deg C, and the warming-anomaly (vs the
`1990-2010` baseline) ranges from p10 = +1.0 deg C to p90 = +3.5 deg C. We score
this through the Health Risk `txx` rule, which uses all three lenses
(absolute 0.40, change 0.25, impact 0.35) with an impact band of 40-45 deg C
(IMD plains heatwave envelope, Section 4).

| District | TXx 2060-80 | Anomaly vs baseline | absolute | change | impact | **Rule score (blended)** | **Pure-absolute only** |
|---|---:|---:|---:|---:|---:|---:|---:|
| **A** | 45.5 deg C | +1.5 deg C | 90 | 20 | 100 | **76** | **90** |
| **B** | 42.0 deg C | +3.5 deg C | 20 | 100 | 40 | **47** | **20** |

How each cell is derived:
- District A absolute = (45.5 - 41) / (46 - 41) = 0.90 -> 90. District B
  absolute = (42.0 - 41) / 5 = 0.20 -> 20.
- District A change = (1.5 - 1.0) / (3.5 - 1.0) = 0.20 -> 20. District B
  change = (3.5 - 1.0) / 2.5 = 1.00 -> 100.
- District A impact = (45.5 - 40) / (45 - 40), clipped to 1.0 -> 100. District B
  impact = (42.0 - 40) / 5 = 0.40 -> 40.
- Blended A = 0.40*90 + 0.25*20 + 0.35*100, renormalized = 76. Blended
  B = 0.40*20 + 0.25*100 + 0.35*40, renormalized = 47.

**Interpretation.**

- **District A** is the already-hot district: it is the hottest among its peers
  (absolute 90) and is well past the 45 deg C severe-heat threshold (impact
  100), but it is warming only modestly (change 20). It is correctly scored as a
  serious, established heat hazard under both methods.

- **District B** is the dangerous case that pure-absolute scoring *hides*. In
  relative terms it looks unremarkable (absolute 20 — it is near the bottom of
  its cohort). But it is **warming faster than any of its peers** (change 100)
  and it has **just crossed the 40 deg C heatwave-declaration threshold** (impact
  40). The blended score lifts it to 47 — a mid-range hazard that warrants
  attention. Pure-absolute scoring leaves it at 20, effectively labelling a
  fast-warming, newly-dangerous district as low-priority.

The blend therefore preserves three distinct, decision-relevant signals that the
absolute lens alone discards:
1. **Trajectory** (change): District B's rapid deterioration, which matters for
   populations that are not yet acclimatized to heat that is rising quickly.
2. **Physical danger anchoring** (impact): the fact that District B has crossed a
   physiologically meaningful, externally-defined threshold (40 deg C), which a
   purely relative score cannot represent.
3. **Escalation visibility over time and space**: because the absolute lens is
   cohort-relative per period, it cannot show that conditions worsen across
   periods or differ in absolute danger across states; the change and impact
   lenses restore that.

The cost of the blend is interpretability — a single 0-100 number now mixes
three signals. Section 5 addresses this directly by **persisting and exposing the
lens decomposition** so a user can see that District B's 47 = absolute 20 +
change 100 + impact 40.

### 2.7 Ensemble central estimate and model uncertainty

Each metric is produced by a 20+ model ensemble. Two distinct choices arise
before and during scoring.

**Central estimate per geography.** Before any lens runs, the ensemble is reduced
to one value per geography. We recommend the **ensemble median** rather than the
mean: a single divergent model shifts the mean but barely moves the median, and
multi-model median is standard practice in IPCC assessments. Note this is a
different axis from the absolute lens's p10-p90 bounds: the median provides
robustness to *ensemble* (model) outliers per geography, while the cohort p10-p90
provides robustness to *spatial* outliers across geographies. Both are warranted.

> Implementation status: the current builder reads the ensemble **mean**
> (`SUPPORTED_STAT = "mean"`), as do the thematic scores and the dashboard
> statistic selector. Migrating the score's central estimate to the **median** is
> a tool-wide methodology change (to keep the score, the thematic bundles, and
> the displayed statistic consistent) and is pending its own approval and tests.

**Model uncertainty (spread).** The ensemble spread (`std`, `p05`, `p95`,
`n_models`) is deliberately **not folded into** the 0-100 score. Collapsing a
central estimate and an uncertainty range into a single number reduces
interpretability without a clear analytical gain — a "70 with wide model
disagreement" and a "70 with tight agreement" should not be forced to look
identical or be merged into one shifted score. Instead, spread / model agreement
is surfaced as a **separate confidence annotation** in the deep-dive (read from
the existing source-master ensemble columns), mirroring the IPCC convention of
reporting a central estimate alongside a separate confidence statement. This
keeps the score legible and the uncertainty visible.

---

## 3. Scientific Basis and References

Each lens corresponds to an established methodological tradition. The lens
framework is a structured combination of these traditions, not a novel scoring
invention.

### 3.1 Absolute lens — composite-indicator normalization

Rescaling indicators of differing units onto a common 0-100 scale via min-max
(or robust quantile) normalization, with explicit treatment of outliers, is
standard composite-indicator practice. Our use of p10-p90 bounds (rather than
raw min-max) follows the Handbook's guidance that extreme values can unduly
influence a normalized index and should be handled deliberately.

- OECD & Joint Research Centre (2008). *Handbook on Constructing Composite
  Indicators: Methodology and User Guide.* Nardo M., Saisana M., Saltelli A.,
  Tarantola S., Hoffmann A., Giovannini E. OECD Publishing, Paris.
  ISBN 978-92-64-04345-9.

### 3.2 Change lens — climate-change signal / change-factor method

Expressing the climate-change signal as the anomaly ("delta") between a future
and a historical baseline period is the foundation of the delta-change /
change-factor approach widely used in climate-impact assessment. The additive
form is appropriate for temperature; the multiplicative / percentage form is
appropriate for precipitation-like quantities — mirroring our `absolute_delta`
vs `relative_pct` change modes.

- Anandhi A., Frei A., Pierson D.C., Schneiderman E.M., Zion M.S., Lounsbury D.,
  Matonse A.H. (2011). "Examination of change factor methodologies for climate
  change impact assessment." *Water Resources Research* 47(3), W03501.
  doi:10.1029/2010WR009104.
- IPCC (2021). *The concept of risk in the IPCC Sixth Assessment Report: a
  summary of cross-Working Group discussions.* IPCC, Geneva. (Risk =
  hazard x exposure x vulnerability; basis for the Phase-1 hazard-only caveat.)

### 3.3 Impact lens — threshold / dose-response impact functions

Mapping a hazard variable onto harm via physiologically or operationally
meaningful thresholds is the basis of impact-function and dose-response
epidemiology, and of engineering design standards. Two findings shape our
policy that bands must be **locally justified and cited** (Section 4):

- Temperature-mortality relationships are **location- and climate-specific**: the
  minimum-mortality temperature ranges from roughly the 60th percentile in
  tropical climates to the 80-90th in temperate ones. A single global "danger
  temperature" is therefore not defensible.
  - Gasparrini A. et al. (2015). "Mortality risk attributable to high and low
    ambient temperature: a multicountry observational study." *The Lancet*
    386(9991): 369-375.
- Physiological heat limits carry **evolving evidence**: the widely-cited 35 deg
  C wet-bulb survivability limit has been empirically revised downward (to
  ~30-31 deg C for young healthy adults), underscoring that any threshold must be
  cited with a date and treated as revisable.
  - Sherwood S.C., Huber M. (2010). "An adaptability limit to climate change due
    to heat stress." *PNAS* 107(21): 9552-9555.
  - Vecellio D.J., Wolf S.T., Cottle R.M., Kenney W.L. (2022). "Evaluating the
    35 deg C wet-bulb temperature adaptability threshold for young, healthy
    subjects (PSU HEAT Project)." *Journal of Applied Physiology* 132(2):
    340-345.

### 3.4 Underlying climate-index definitions (ETCCDI) and drought (SPI)

The source metrics themselves follow standard index definitions, which gives the
inputs to the lenses their own provenance:

- Zhang X., Alexander L., Hegerl G.C., Jones P., Klein Tank A., Peterson T.C.,
  Trewin B., Zwiers F.W. (2011). "Indices for monitoring changes in extremes
  based on daily temperature and precipitation data." *WIREs Climate Change*
  2(6): 851-870. (Defines WSDI, Rx1day, Rx5day, CWD, CDD, TXx, TNx, etc., via
  the WMO Expert Team on Climate Change Detection and Indices, ETCCDI.)
- McKee T.B., Doesken N.J., Kleist J. (1993). "The relationship of drought
  frequency and duration to time scales." *Proc. 8th Conference on Applied
  Climatology*, American Meteorological Society, Anaheim, CA. (Defines SPI and
  its drought classes; drought onset at SPI <= -1.0.)

### 3.5 India-context threshold provenance

- India Meteorological Department (IMD), *FAQ on Heat Wave* and Heat Wave
  Guidance; National Disaster Management Authority (NDMA), *Heat Wave
  guidelines*. Plains heatwave criteria: heatwave declared at Tmax >= 40 deg C;
  by the actual-temperature method, heatwave at Tmax >= 45 deg C and severe
  heatwave at Tmax >= 47 deg C; by the departure method, +4.5 to 6.4 deg C above
  normal (heatwave) and > 6.4 deg C (severe). IMD also defines a **warm night**
  (departure-based): minimum temperature 4.5-6.4 deg C above normal (severe /
  very warm night > 6.4 deg C), conditional on Tmax >= 40 deg C. This is an
  anomaly criterion, not an absolute night-temperature band, and therefore
  informs the change lens rather than the impact lens (see Section 6.3).
- IMD daily rainfall classification (24-hour): heavy rainfall 64.5-115.5 mm;
  very heavy rainfall 115.6-204.4 mm; extremely heavy rainfall >= 204.5 mm.

### 3.6 Sector impact-pathway references (used in the per-bundle dossiers)

- Hot-night mortality (lack of overnight physiological recovery): multi-country
  analyses report hot-night mortality associations, with location-specific
  thresholds frequently above 20 deg C. (e.g. multi-country hot-nights analysis
  across 178 locations; night-time warming mortality-burden modelling, *Lancet
  Planetary Health*.)
- Heatwave **duration** has an added mortality effect beyond single hot days,
  because consecutive hot days prevent physiological recovery; effects typically
  emerge after ~2-4 consecutive days. (Heatwave-mortality reviews; e.g. *The
  impact of heat waves on mortality*.)
- Flooding and extreme precipitation are leading triggers of waterborne and
  vector-borne disease outbreaks via contamination of water supplies and
  creation of vector breeding sites. (World Health Organization, *Floods*;
  systematic reviews of extreme water-related weather events and waterborne
  disease.)

> Note on citations: peer-reviewed and institutional sources above were verified
> against primary publishers / agencies. Where a per-bundle dossier introduces a
> new band (especially for rainfall design storms, cooling-water, or hydropower
> operations), the specific source and date are recorded inline in that dossier
> and added to this bibliography.

---

## 4. Impact-Band Provenance Policy (Hybrid)

The impact lens is only as defensible as the band behind it. Policy:

1. **The impact lens scores danger, not unusualness.** A band marks proximity to
   a physical, physiological, or operational *harm* regime. Emergence /
   "how unusual vs history" is the job of the **change** lens; the impact lens
   must not be built from a percentile or a standardized-anomaly-from-history
   threshold, because that would duplicate the change lens and stops measuring
   danger.

2. **Sourcing, in preference order.** Every band is either:
   - **External provenance** — an authoritative published danger threshold
     (e.g. IMD heatwave criteria, IMD rainfall categories, NDMA/CWC/BIS codes,
     peer-reviewed dose-response thresholds); preferred wherever it exists and is
     appropriate to the Indian/local context; or
   - **Self-derived** — a danger band the methodology authors construct via the
     derivation protocol below, used **only** where no adequate external band
     exists. We do **not** drop the impact lens merely because no ready-made
     external band is published.

3. **Self-derived band derivation protocol.** A self-derived band must be a
   transparent derivation, not a picked number. Record, in the dossier:
   1. **Harm mechanism** — the physical/physiological/operational pathway by
      which high (or low) values harm the sector receptor.
   2. **Closest anchors** — the nearest external evidence (dose-response
      inflections, analogous standards, related thresholds), cited.
   3. **Cut points** — `band_low` = mechanistic onset of *material* harm;
      `band_high` = severe / saturation regime; each justified from the anchors.
   4. **Confidence rating** — high / medium / low, by how directly the anchors
      support the cut points.
   5. **Provenance** — author, date, assumptions, revisable flag.
   Confidence feeds the weight: a low-confidence self-derived band is given a
   **smaller within-rule impact weight** than a high-confidence external band, so
   a weak band cannot drive the rule score.

4. **Every band is justified and cited.** Each impact band records its two cut
   points and units, its source (citation or "self-derived" + the protocol
   fields above), a date, and the rationale for sector/India appropriateness.

5. **Local specificity over universal thresholds.** Because dose-response
   relationships are climate- and population-specific (Gasparrini et al. 2015),
   prefer India-context thresholds (IMD/NDMA/CWC/BIS) over globally-transferred
   ones. Document any global threshold's applicability explicitly.

6. **Bands are revisable.** Record bands as versioned, dated assumptions (the
   35 deg C wet-bulb revision is the cautionary precedent). A band change is a
   methodology change and must be called out and tested.

7. **No phantom thresholds.** A rule slug or label that names a number (e.g.
   `..._ge_200`) must either implement that number as a real impact band with
   provenance, or be renamed. Labels must not imply thresholds the math does not
   apply.

8. **Bands are expressed in absolute physical units, applied to the value.** An
   impact band thresholds the metric *value* (e.g. TXx 40-45 deg C), not a
   departure from baseline. A departure-from-baseline construction was considered
   for TNx (the IMD warm-night "+4.5 to +6.4 deg C above normal" criterion) and
   **rejected**: that standard is defined only jointly with a same-day
   Tmax >= 40 deg C co-condition and against a *daily climatological* normal, so
   applying it to an annual-maximum night value against an annual-maximum baseline
   would silently change what the threshold means. The lesson encoded here: a
   borrowed standard may only be used in the construction its source defines.
   Emergence-type signals belong to the change lens (point 1).

9. **Geography-zone specificity (deferred).** Institutional danger standards are
   physiography-specific — IMD's heatwave trigger is 40 / 37 / 30 deg C for
   plains / coastal / hilly zones, with a physical basis (coastal humidity lowers
   the dry-bulb danger threshold). The bands in this document are therefore
   **plains / national defaults**, which is correct for the Telangana pilot
   (plateau/plains, no coast, negligible hill terrain). Refining bands per zone
   requires a per-geography zone label; no ready-made all-India
   district -> {plains/coastal/hilly} classification matching IMD's taxonomy
   exists off-the-shelf, so this is deferred (see `docs/BACKLOG.md` BL-0020;
   candidate sources: ICAR/Planning-Commission agro-climatic regions, NBSS&LUP
   agro-ecological zones, or a DEM + coastal-district classification).

---

## 5. Score Decomposition and Persistence Schema

To make the blended score transparent (and thereby credible), the per-lens
components are **computed, persisted, and made available for display**. The
guiding UX contract:

- **Glance view:** show only the **composite bundle score** (and, optionally, the
  top contributing rules as drivers). One number, easy to read.
- **Deep-dive view:** let the user **expand any rule into its lens
  decomposition** — the absolute, change, and impact components, the combined
  rule score, and the rule's weight in the bundle.

### 5.1 Persistence schema

Persisted per `scenario x period` in the bundle master, for each rule:

```text
{rule_slug}__{scenario}__{period}__score              # combined rule score (existing)
{rule_slug}__absolute__{scenario}__{period}__score    # absolute-lens component (new)
{rule_slug}__change__{scenario}__{period}__score       # change-lens component (new)
{rule_slug}__impact__{scenario}__{period}__score       # impact-lens component (new)
```

And per bundle (existing, retained):

```text
{composite_slug}__{scenario}__{period}__mean
{composite_slug}__{scenario}__{period}__available_rule_count
{composite_slug}__{scenario}__{period}__available_rule_weight_fraction
```

Rules:
- **Only active lenses are persisted.** For an absolute-only rule, the `absolute`
  component equals the combined `__score`; the `change` and `impact` columns are
  omitted (not written as empty), keeping the schema honest and the file size
  proportional to the lenses actually used.
- The combined `__score` column name is unchanged, so the schema extension is
  **backward-compatible** with existing readers and the composite roll-up.
- The deep-dive UI reads the per-lens columns; the Glance view reads only the
  composite mean.

### 5.2 Storage rationale

Sector bundles are small offline admin masters (e.g. Telangana = 33 districts,
620 blocks). Adding active-lens columns is at most ~3x the rule-score columns —
kilobytes per state, with no dashboard-runtime cost. The transparency gain
(showing *why* a geography scored as it did) is the point: it converts an opaque
0-100 into an auditable decomposition.

> Implementation status: the per-lens persistence columns are a **proposed
> extension** to `compute/proposal_bundles.py` and the column helpers in
> `config/proposal_bundles.py`. They are documented here as the target schema;
> the code change is tracked separately and requires its own approval and tests
> (the builder currently persists only the combined `__score` plus the three
> bundle-level columns).

### 5.3 Coverage gate (retained)

The composite is only published for a geography when the rules that have data
cover at least a minimum fraction of total rule weight
(`min_available_rule_weight_fraction`). Below the gate, the composite is set to
NaN rather than reported from a thin subset of rules. This gate should be applied
consistently across all sector bundles (Agricultural Risk uses 0.70; the
standard is to adopt the same gate for the other sector bundles unless a bundle
documents a different, justified value).

---

## 6. Health Risk — Metric-by-Metric Lens Dossier (template)

Bundle: `Sector-wise - Health Risk` | composite slug: `composite_health_risk`
| levels: district, block | scenarios: `ssp245`, `ssp585`.

Conceptual scope: climate hazards most directly tied to **human health
outcomes** — acute heat (mortality/morbidity), persistent heat (loss of
overnight recovery and cumulative stress), and extreme rainfall (flood injury and
waterborne / vector-borne disease). Per Section 1.2 this is hazard pressure, not
health *risk*: it does not include population exposure or health-system
vulnerability.

The table summarizes the lens decision per metric; each subsection gives the
reasoning and any band provenance.

| Rule (metric) | absolute | change | impact (band) | Rationale summary |
|---|:--:|:--:|:--:|---|
| TXx — extreme daytime heat (`txx_annual_max`) | yes | yes | yes — external, 40-45 deg C (plains) | Acute heat mortality; IMD plains heatwave envelope |
| WSDI — warm-spell persistence (`wsdi_warm_spell_days`) | yes | yes | yes — self-derived (low conf), 6-18 days/yr | Added mortality effect of heat duration; heatwave-duration epidemiology |
| TNx — night-time heat (`tnx_annual_max`) | yes | yes | yes — self-derived (med conf), 28-32 deg C | Loss of overnight recovery; India hot-night mortality inflection |
| Rx1day — 1-day rainfall (`pr_max_1day_precip`) | yes | yes | yes — external, 115.6-204.5 mm | Flood injury + waterborne disease; IMD very-heavy to extremely-heavy band |
| CWD — consecutive wet days (`cwd_consecutive_wet_days`) | yes | yes | yes — self-derived (low conf), 7-15 days | Saturation / waterlogging / vector breeding; prolonged-saturation pathway |

### 6.1 TXx — Extreme daytime heat (`txx_annual_max`)

- **Lenses:** absolute (yes), change (yes), impact (yes).
- **absolute:** Keep. Identifies which districts face the most extreme projected
  daytime heat relative to peers — the base prioritization signal.
- **change:** Keep. Surfaces districts warming fastest vs the `1990-2010`
  baseline. Health-relevant because populations adapt slowly; rapid warming
  raises mortality risk even where absolute levels are not yet the highest
  (the District B case, Section 2.6). Mode: `absolute_delta` (degrees).
- **impact:** Keep. Band **40-45 deg C**, external provenance, **high
  confidence**. IMD declares a plains heatwave at Tmax >= 40 deg C and, by the
  actual-temperature method, a heatwave at >= 45 deg C (severe at >= 47 deg C).
  The 40-45 band therefore maps onset-of-concern to the heatwave-declaration
  floor and saturation to the heatwave threshold. Source: IMD *FAQ on Heat Wave*
  / NDMA Heat Wave guidelines; date 2024. Rationale: heat mortality rises steeply
  through this range, and the band is the nationally-recognized Indian *plains*
  standard rather than a transferred global value.
  - **Zone caveat:** this is the **plains default**. IMD's coastal trigger is
    37 deg C and hilly 30 deg C; zone-specific bands are deferred (Section 4.9,
    BL-0020). Correct for the Telangana pilot.
- **Per-lens weights:** absolute 0.40 / change 0.25 / impact 0.35 (as in the
  worked example, Section 2.6).
- **Why not exclude any lens:** TXx is the metric where all three lenses are most
  defensible for health — relative prioritization, trajectory, and a cited
  national danger band.

### 6.2 WSDI — Warm-spell persistence (`wsdi_warm_spell_days`)

- **Lenses:** absolute (yes), change (yes), impact (no).
- **absolute:** Keep. Districts with the most persistent warm spells relative to
  peers. WSDI is the ETCCDI count of days in spells of >= 6 consecutive days
  above the day-of-year 90th percentile (Zhang et al. 2011).
- **change:** Keep. Lengthening warm spells vs baseline are health-relevant
  because the **added effect of heat duration** on mortality comes precisely from
  consecutive days that prevent overnight/physiological recovery (heatwave-
  mortality literature; effects emerge after ~2-4 consecutive days). Mode:
  `relative_pct` (proportional change in spell days).
- **impact:** Keep, **self-derived band 6-18 days/yr, low confidence** (Section 4
  protocol).
  - **Mechanism:** consecutive hot days compound mortality because they prevent
    overnight/physiological recovery.
  - **Anchors:** the "added" duration effect on mortality appears after ~4
    consecutive heatwave days (multi-country evidence) and escalates steeply
    (Beijing CVD study: ~10% excess at day 4, ~51% at day 5 for Tmax > 35 deg C).
  - **Cut points:** onset **6 days** — WSDI's own minimum qualifying spell (>= 6
    consecutive days > day-of-year 90th percentile, Zhang et al. 2011), already
    past the ~4-day added-effect threshold; saturation **18 days/yr** — a high
    annual warm-spell burden (~three qualifying spells).
  - **Confidence: low.** WSDI is a percentile-based *annual tally*, not a single
    spell length, so the band is a pragmatic annual-burden proxy, not a physical
    threshold. It is therefore given a **small within-rule impact weight** so it
    cannot dominate the rule. The primary duration signal stays with the absolute
    and change lenses.
- **Per-lens weights:** absolute 0.45 / change 0.40 / impact 0.15.

### 6.3 TNx — Night-time heat (`tnx_annual_max`)

- **Lenses:** absolute (yes), change (yes), impact (yes — departure band).
- **absolute:** Keep. Districts with the hottest projected nights relative to
  peers. TNx is the annual maximum of daily minimum temperature (ETCCDI).
- **change:** Keep. Warming nights vs the `1990-2010` baseline, normalized across
  the cohort's anomalies (i.e. "warming faster than peers"). Night-time warming
  has a distinct, growing mortality burden because it removes the overnight relief
  the body needs to recover from daytime heat. Mode: `absolute_delta` (degrees).
- **impact:** Keep, **self-derived absolute band 28-32 deg C, medium
  confidence** (Section 4 protocol). The band thresholds the night-temperature
  *level* directly:

  ```text
  impact_score = clip( (TNx - 28) / (32 - 28), 0, 1 ) * 100
  ```

  - **Mechanism:** warm nights remove the overnight cooling the body needs to
    recover from daytime heat, sustaining cardiovascular strain and disrupting
    sleep — a mortality burden *independent of* daytime heat.
  - **Anchors:** an India case-crossover analysis finds that above a night
    minimum of **~28 deg C**, each +1 deg C raises mortality **~9.8%** —
    marginally *more* than the daytime per-degree effect (~9.6%) — giving a
    defensible India-specific **onset = 28 deg C**. The tropical-night convention
    (Tmin >= 20 deg C classic; the IRT Heat Risk already uses TN > 25 deg C as a
    "warm night") confirms cooling-relief is lost well below 28 deg C, so taking
    28 deg C as the *steep-harm* onset is conservative. **Saturation = 32 deg C**
    (~+4 deg C above onset) sits near the upper envelope of observed Indian
    warmest nights, where overnight recovery effectively fails.
  - **Confidence: medium** — India-specific epidemiological anchor for the onset;
    the saturation point is reasoned from the observed envelope.
  - **Why not the departure band:** an earlier draft used the IMD warm-night
    "+4.5 to +6.4 deg C above normal" departure criterion. It was **withdrawn**
    (Section 4.8): IMD's warm night is defined only jointly with a same-day
    Tmax >= 40 deg C co-condition and against a *daily climatological* normal,
    neither of which holds for an annual-maximum TNx against an annual-maximum
    baseline — so the borrowed threshold would not mean what IMD intends. With an
    absolute-level band, TNx is now structurally like TXx (absolute and impact
    both act on the level; change carries the departure).
  - **Zone caveat:** plains/national default (Section 4.9, BL-0020).
- **Per-lens weights:** absolute 0.40 / change 0.25 / impact 0.35 (mirrors TXx,
  since the band now acts on the level). This supersedes the earlier
  0.50/0.25/0.25 split, which existed only to avoid double-counting the withdrawn
  departure band.

### 6.4 Rx1day — One-day rainfall (`pr_max_1day_precip`)

- **Lenses:** absolute (yes), change (yes), impact (yes).
- **absolute:** Keep. Districts facing the most intense single-day rainfall
  relative to peers (Rx1day, ETCCDI).
- **change:** Keep. Intensifying extreme rainfall vs baseline. Mode:
  `relative_pct` (precipitation change is conventionally multiplicative).
- **impact:** Keep. Band **115.6-204.5 mm/day**, external provenance. IMD's
  daily rainfall classification places "very heavy rainfall" at 115.6-204.4 mm
  and "extremely heavy rainfall" at >= 204.5 mm in 24 hours. Onset-of-concern is
  therefore set at the very-heavy threshold and saturation at the
  extremely-heavy threshold — the range over which flood injury, drowning, and
  waterborne / vector-borne disease outbreaks are most strongly associated with
  rainfall (WHO *Floods*; waterborne-disease reviews). Source: IMD daily
  rainfall classification, verified against the IMD *Heavy Rain Warning Services*
  brochure (heavy 64.5-115.5; very heavy 115.6-204.4; extremely heavy
  >= 204.5 mm); date 2024. **High confidence.**
  - **Zone note:** IMD rainfall categories are **national, not
    physiography-specific**, so no zone caveat applies (unlike the heat bands).
- **Why not the "heavy" 64.5 mm floor:** the health-impact pathway (flooding,
  contamination) is most defensible at the very-heavy-and-above range; using the
  heavy-rain floor would over-trigger the impact lens for routine monsoon days.
- **Per-lens weights:** absolute 0.40 / change 0.25 / impact 0.35 (mirrors TXx;
  change mode `relative_pct`, as precipitation change is multiplicative).

### 6.5 CWD — Consecutive wet days (`cwd_consecutive_wet_days`)

- **Lenses:** absolute (yes), change (yes), impact (no).
- **absolute:** Keep. Districts with the longest projected wet spells relative to
  peers (CWD, ETCCDI). Long wet spells drive ground saturation, waterlogging,
  and standing water that supports vector breeding.
- **change:** Keep. Lengthening wet spells vs baseline. Mode: `relative_pct`.
- **impact:** Keep, **self-derived band 7-15 days, low confidence** (Section 4
  protocol).
  - **Mechanism:** prolonged consecutive rain drives soil saturation,
    waterlogging, and standing water -> flood injury plus mosquito breeding
    habitat (vector-disease cases lag rainfall by ~4-6 weeks).
  - **Cut points:** onset **7 days** — a continuous week sustains standing water
    across a mosquito aquatic-development cycle (~7-10 days) and drives the onset
    of waterlogging; saturation **15 days** — a fortnight-plus of continuous wet
    days indicates a prolonged-saturation regime.
  - **Confidence: low.** The health pathway depends heavily on local drainage and
    hydrology — a genuine zone/hydrology refinement (Section 4.9, BL-0020) — so
    the band is given a **small within-rule impact weight**. The persistence
    signal is carried mainly by the absolute and change lenses.
- **Per-lens weights:** absolute 0.45 / change 0.40 / impact 0.15.

### 6.6 Health Risk — bundle assembly notes

**Rule weights (explicit, sum to 1.0).** Weighting is an evidence-informed
expert elicitation, not a derived constant; the recommended default reflects the
relative climate-health burden in India and is recorded as a revisable
assumption.

| Cluster | Cluster weight | Rule | Rule weight | Why |
|---|---:|---|---:|---|
| Heat | 0.60 | TXx (acute daytime heat) | 0.30 | Acute heat is the dominant, best-evidenced climate-health mortality driver in India; aligns with the IMD heatwave definition |
| Heat | 0.60 | TNx (night-time heat) | 0.18 | Independent, growing mortality burden from loss of overnight recovery; India hot-night mortality evidence (self-derived 28-32 deg C band) |
| Heat | 0.60 | WSDI (heat persistence) | 0.12 | Added duration effect on mortality; partly correlated with TXx, so weighted below it |
| Rainfall | 0.40 | Rx1day (acute 1-day rainfall) | 0.25 | Strongest rainfall-flood injury and waterborne-outbreak association |
| Rainfall | 0.40 | CWD (wet-spell persistence) | 0.15 | Saturation / waterlogging / vector breeding; persistence rather than acute intensity |

**How these weights were derived.** The weights are an expert elicitation
structured in two stages, so the reasoning is auditable rather than ad hoc:

1. **Cluster split first (heat vs rainfall).** The bundle's five metrics fall
   into two correlated hazard clusters — heat (TXx, TNx, WSDI) and rainfall
   (Rx1day, CWD). We assign weight between clusters before splitting within them.
   The heat cluster receives the majority (0.60) because:
   - India's documented climate-attributable health burden is heat-dominated —
     heatwave mortality is the largest and best-evidenced climate-health signal
     nationally; and
   - the rainfall -> health pathway (flood injury, waterborne / vector disease)
     is real but more strongly mediated by **exposure and sanitation / drainage**
     — determinants this hazard-only Phase-1 score does not model — so on hazard
     alone it is weighted below heat rather than equally.
   The 0.60 / 0.40 split is the single most consequential judgment and the main
   lever for revision.

2. **Within-cluster split by evidence strength and inter-metric correlation.**
   - Heat: TXx (0.30) > TNx (0.18) > WSDI (0.12). TXx (acute daytime heat) has
     the strongest direct mortality evidence and aligns with the IMD heatwave
     definition. TNx (night-time heat) is an independent and growing burden (loss
     of overnight recovery). WSDI (persistence) is an added duration effect and
     is partly correlated with TXx, so it is weighted below both to avoid
     over-counting the daytime-heat signal.
   - Rainfall: Rx1day (0.25) > CWD (0.15). Acute single-day intensity has the
     strongest association with flood injury and waterborne-disease outbreaks;
     CWD (persistence / waterlogging) is a secondary, slower pathway.

3. **Sanity checks.** Weights are positive, sum to 1.0, and no single rule
   dominates (max 0.30). Correlated metrics within a cluster are deliberately not
   given equal weight, so the composite is not driven by one over-represented
   mechanism.

These weights are revisable expert assumptions, not derived constants; any change
is a methodology change to be recorded and tested.

**Per-lens weights within each rule** (absolute / change / impact) are recorded
in `config/proposal_bundles.py` and shown per metric in Sections 6.1-6.5. The
high-confidence external-band metrics (TXx, Rx1day) and the medium-confidence
TNx use a 0.40 / 0.25 / 0.35 split; the low-confidence self-derived bands (WSDI,
CWD) use 0.45 / 0.40 / 0.15, deliberately giving the weak impact band a small
share so it cannot dominate the rule (Section 4.3).

- **Coverage gate:** adopt the standard 0.70 available-rule-weight gate
  (Section 5.3).
- **Source masters:** all five source metrics must resolve to grid-first
  district/block masters (compute the index per grid cell, then area-weight to
  the polygon), consistent with the spatial-aggregation recommendation in
  `docs/bundle_calculation_audit.md`.

---

## 7. Industrial Risk — Metric-by-Metric Lens Dossier

Bundle: `Sector-wise - Industrial Risk` | composite slug:
`composite_industrial_risk` | levels: district, block | scenarios: `ssp245`,
`ssp585`.

Conceptual scope: climate hazards most directly tied to **industrial operations
and continuity** — extreme heat (worker productivity, equipment derating),
extreme rainfall (factory inundation, transport and supply-chain disruption),
and prolonged dry spells (cooling-water and process-water stress for
water-intensive sectors). Per Section 1.2 this is hazard pressure, not full
industrial risk: it does not include sector-specific exposure (where industrial
assets sit), adaptive capacity (cooling-water reuse, on-site storage,
flood-proofing), or vulnerability (insurance, supply-chain redundancy).

The table summarizes the lens decision per metric; each subsection gives the
reasoning and any band provenance. Rule slugs marked "renamed from" reflect the
CHG-0015 rename of phantom slugs (Section 4.7); the dossier presents the
renamed slugs because the bands and labels now reflect the actual scoring math.

| Rule (metric) | absolute | change | impact (band) | Rationale summary |
|---|:--:|:--:|:--:|---|
| Rx1day — 1-day rainfall (`pr_max_1day_precip`) | yes | yes | yes — external, 115.6-204.5 mm | IMD very-heavy / extremely-heavy categories; factory inundation, transport disruption, power outages |
| Rx5day — 5-day rainfall (`pr_max_5day_precip`) | yes | yes | yes — self-derived (low conf), 250-500 mm | Multi-day basin-scale flood pressure; no external categorical band exists at 5-day scale |
| CDD — consecutive dry days (`pr_consecutive_dry_days_lt1mm`) | yes | yes | yes — self-derived (med conf, IMD-anchored), 30-90 days | Cooling- and process-water stress; IMD Agricultural Drought (4 Drought Weeks) anchors onset |
| TXx — extreme daytime heat (`txx_annual_max`) | yes | yes | yes — external, 40-45 deg C (plains) | Worker productivity loss + equipment derating; IMD plains heatwave envelope |

### 7.1 Rx1day — One-day rainfall (`pr_max_1day_precip`)

- **Lenses:** absolute (yes), change (yes), impact (yes).
- **absolute:** Keep. Districts facing the most intense projected single-day
  rainfall relative to peers (Rx1day, ETCCDI). The acute disruption signal — a
  single very-heavy day forces operational shutdowns, inundates factory floors,
  and breaks road / rail logistics regardless of multi-day context.
- **change:** Keep. Intensifying one-day extremes vs the `1990-2010` baseline
  matter to industry because facilities are designed against historical norms
  (drainage capacity, plinth height, storm-water sizing). Mode: `relative_pct`
  (precipitation change is conventionally multiplicative).
- **impact:** Keep. Band **115.6-204.5 mm/day**, **external provenance, high
  confidence** — same band Health Risk uses (Section 6.4). IMD's daily rainfall
  classification: very heavy 115.6-204.4 mm, extremely heavy >= 204.5 mm. The
  industrial harm pathway (drainage failure, factory inundation, transport
  paralysis, power-network outages) is most strongly associated with this range,
  and the band is national / not physiography-specific.
  - **Why the same band as Health Risk:** the hazard *value* threshold for
    drainage failure does not change by sector receptor — it is set by the
    rainfall intensity. Health and Industrial differ in *consequence* (flood
    injury vs operational shutdown), not in the rainfall amount that triggers
    failure of urban / industrial drainage.
- **Per-lens weights:** absolute 0.40 / change 0.25 / impact 0.35 (mirrors
  TXx — high-confidence external band lets the impact lens carry meaningful
  share without dominating).

### 7.2 Rx5day — Five-day rainfall (`pr_max_5day_precip`)

- **Lenses:** absolute (yes), change (yes), impact (yes — self-derived).
- **absolute:** Keep. Districts with the heaviest projected 5-day accumulated
  rainfall relative to peers. Captures **basin-scale** flood pressure that a
  single-day extreme can miss — sustained multi-day rainfall builds upstream
  runoff, saturates catchments, and overwhelms regional drainage even when no
  single day reaches the very-heavy IMD threshold.
- **change:** Keep. Intensifying multi-day accumulations vs baseline. Mode:
  `relative_pct`.
- **impact:** Keep, **self-derived band 250-500 mm/5 days, low confidence**
  (Section 4 protocol). **No external categorical band exists at 5-day scale**:
  IMD publishes daily rainfall categories only, CWC uses station-specific gauge
  danger levels (not universal mm thresholds), and NDMA SOPs reference IMD
  daily categories. The band is therefore derived rather than borrowed.
  - **Mechanism:** sustained multi-day rainfall produces saturation-driven
    flooding, drainage backlog across the catchment, and regional supply-chain
    paralysis distinct from the single-day acute signal.
  - **Anchors:**
    - Five consecutive IMD "heavy" days (>= 64.5 mm/day per IMD daily
      classification) sum to **>= 322 mm** — onset of sustained heavy-rain
      regime; **250 mm** is a conservative floor capturing the onset of plausible
      drainage failure at plains scale.
    - Observed Indian catastrophic-event 5-day cumulative magnitudes anchor the
      saturation point: Kerala 2018 ~350-400 mm in 3 days regionally, Mumbai
      2005 cumulative ~944 mm, and multi-day extreme precipitation events
      causing major Indian floods during the 2024 monsoon had 3-day return
      periods of **>75 to >200 years** (Chuphal et al. 2025) — confirming the
      **500 mm / 5-day** regime is statistically extreme and operationally
      catastrophic.
  - **Cut points:** onset **250 mm** (plausible drainage-failure regime);
    saturation **500 mm** (regional flood-event regime).
  - **Confidence: low.** Anchored only on derivation (IMD daily category × 5)
    and observed disaster magnitudes; no institutional categorical band exists
    at 5-day scale. The 5-day signal partially overlaps Rx1day (high-1-day
    events often dominate 5-day totals), so the rule's bundle weight is also
    smaller than Rx1day's.
- **Per-lens weights:** absolute 0.45 / change 0.40 / impact 0.15 — small
  within-rule impact weight because the band is self-derived and low confidence
  (Section 4.3). The persistence signal is carried mainly by absolute and
  change.

### 7.3 CDD — Consecutive dry days (`pr_consecutive_dry_days_lt1mm`)

- **Lenses:** absolute (yes), change (yes), impact (yes — IMD-anchored).
- **absolute:** Keep. Districts with the longest projected dry spells relative
  to peers (CDD, ETCCDI: maximum consecutive days with precipitation < 1 mm).
  Long dry spells stress municipal and industrial water supply, with
  water-intensive sub-sectors (textiles, beverages, paper, thermal-plant
  cooling, semiconductors) facing operational interruption.
- **change:** Keep. Lengthening dry spells vs baseline shift industrial water
  planning beyond historical operating envelopes. Mode: `relative_pct`.
- **impact:** Keep, **self-derived band 30-90 days, medium confidence**
  (Section 4 protocol). The onset is anchored on an **operational Indian
  institutional definition** (IMD Agricultural Drought) rather than a picked
  number, which raises the confidence over a purely-reasoned band.
  - **Mechanism:** prolonged dry-spell -> reservoir / groundwater drawdown ->
    municipal water rationing -> curtailed industrial draws (cooling, process,
    washing). Water-intensive industries experience this as forced derate or
    shutdown.
  - **Anchors:**
    - IMD agromet practice defines a **Drought Week** as 7 days with rainfall
      <= 50% of normal; **four consecutive Drought Weeks** between May and
      October constitutes IMD **Agricultural Drought** — i.e. **~28 days of
      dry-spell conditions** is the operational onset of drought in Indian
      institutional practice.
    - The Indian summer monsoon (JJAS) is ~120 days; a 90-day dry spell during
      this window indicates monsoon failure and corresponds to SPI-12 severe
      meteorological drought territory (McKee et al. 1993).
  - **Cut points:** onset **30 days** (IMD Agricultural Drought, rounded from
    28 to align with the existing slug threshold and operational rounding);
    saturation **90 days** (¾-season dry regime; monsoon-failure / severe-
    drought regime).
  - **Confidence: medium.** IMD-anchored onset (operational institutional
    definition) raises confidence above a purely self-derived band. Saturation
    is reasoned from monsoon length and SPI severe-drought convention rather
    than an institutional category.
  - **Local mediation caveat:** actual industrial impact is heavily mediated by
    local groundwater status, reservoir storage, and on-site water-reuse
    infrastructure — Phase-1 hazard pressure does not model these. A real
    industrial water-stress refinement would couple CDD with Aqueduct
    base-water-stress or CGWB groundwater data (deferred).
- **Per-lens weights:** absolute 0.40 / change 0.30 / impact 0.30 — the
  medium-confidence IMD-anchored band justifies a larger within-rule impact
  weight than the low-confidence Rx5day band, but still slightly below the
  high-confidence external bands (TXx, Rx1day).

### 7.4 TXx — Extreme daytime heat (`txx_annual_max`)

- **Lenses:** absolute (yes), change (yes), impact (yes).
- **absolute:** Keep. Same definition as in Health Risk (Section 6.1). Districts
  facing the most extreme projected daytime heat relative to peers.
- **change:** Keep. Warming daytime extremes vs the `1990-2010` baseline matter
  to industry because facilities (HVAC, transformers, cooling towers, on-site
  power) are sized against historical extremes. Mode: `absolute_delta` (degrees).
- **impact:** Keep. Band **40-45 deg C**, **external provenance, high
  confidence** — same IMD plains heatwave band as Health Risk (Section 6.1).
  - **Industrial harm pathway:** daytime heat affects industry through two
    distinct channels — **(i) worker productivity loss** (ILO 2019: India
    projected to lose ~5.8% of total working hours by 2030, equivalent to ~34
    million full-time jobs, the largest national loss globally) and
    **(ii) equipment derating** (transformer / motor / HVAC efficiency declines,
    grid stress, semiconductor and food-processing line shutdowns).
  - **Why the IMD heatwave band rather than a WBGT band:** the IRT's TXx is a
    dry-bulb temperature, and the IMD heatwave definition is the
    nationally-recognized plains threshold for the dry-bulb regime. The
    labor-productivity literature (Sahu et al. 2013; ILO 2019) is anchored on
    **wet-bulb globe temperature (WBGT)**, which becomes adverse for outdoor
    Indian workers above ~26 deg C WBGT — corresponding to lower dry-bulb
    values when humidity is high, but higher dry-bulb values when air is dry.
    The IMD 40-45 dry-bulb band is therefore a **coarser** proxy than a
    WBGT-conditioned band would be: it under-triggers in humid coastal regions
    and may slightly over-trigger in dry-arid regions. For Phase-1 (Telangana
    plateau pilot, semi-arid climate), this is acceptable and the band is
    consistent with the Health and (future) other sectoral bundles that use the
    same dry-bulb TXx.
  - **WBGT-conditioned rule:** a future industrial-heat refinement would add a
    `twb_*` source metric (e.g. `twb_days_ge_28` already produced by the Heat
    Stress v2 grid-first compute) as a separate rule, so labor-productivity
    impact can be scored against the appropriate physiological band
    (deferred — BL-0021, see Section 7.5).
  - **Zone caveat:** plains/national default (Section 4.9, BL-0020).
- **Per-lens weights:** absolute 0.40 / change 0.25 / impact 0.35 — mirrors
  Health Risk's TXx (high-confidence external band).

### 7.5 Industrial Risk — bundle assembly notes

**Rule weights (explicit, sum to 1.0).** Weighting is an evidence-informed
expert elicitation, not a derived constant; the recommended default reflects
the relative industrial-sector climate burden in India and is recorded as a
revisable assumption.

| Cluster | Cluster weight | Rule | Rule weight | Why |
|---|---:|---|---:|---|
| Heat | 0.40 | TXx (extreme heat operations) | 0.40 | Heat is the only *continuous* sector-wide pressure with a quantified India-specific dose-response (ILO ~5.8% working-hours loss by 2030; Sahu et al. ~5% productivity loss per deg C WBGT > 26); external high-confidence band |
| Rainfall (acute) | 0.40 | Rx1day (1-day rainfall) | 0.25 | High-confidence IMD band; well-documented major-event losses ($1-5B per event: Mumbai 2005 ~$3B, Chennai 2015 ~$1.3B, Kerala 2018 ~$3.5B) |
| Rainfall (multi-day) | 0.40 | Rx5day (5-day rainfall) | 0.15 | Low-confidence self-derived band; structurally overlaps Rx1day's signal; captures basin-scale pressure independent of single-day extreme |
| Water stress | 0.20 | CDD (consecutive dry days) | 0.20 | Medium-confidence IMD-anchored band; affects water-intensive sub-sectors through a slow-onset pathway structurally independent of rainfall |

**How these weights were derived.** Same two-stage elicitation as Health
Risk's, with the recipe stated up front so the reasoning is auditable:
**rule_weight reflects (continuous sector burden) x (band confidence) x
(structural independence)**.

1. **Cluster split first (heat vs rainfall vs water-stress).** The bundle's
   four metrics fall into three hazard clusters — heat (TXx), rainfall (Rx1day,
   Rx5day), and slow-onset water stress (CDD). Cluster weights:
   - **Heat 0.40** — heat is the only continuous, sector-wide pressure with a
     **quantified India-specific dose-response**: ILO (2019) projects ~5.8% of
     India's total working hours lost to heat stress by 2030 (~34 million
     jobs equivalent — the largest national loss globally); Sahu et al. (2013)
     measured ~5% productivity loss per deg C of WBGT above ~26 deg C in southern
     India outdoor workers. No other industrial hazard has comparably-anchored
     continuous-pressure quantification, so heat receives the single largest
     cluster weight and the single largest rule weight.
   - **Rainfall 0.40** — major Indian flood events impose $1-5B in industrial
     and infrastructure losses per event (Mumbai 2005, Chennai 2015, Kerala
     2018) with an annual average around **$7.4B**, projected to grow ~6x by
     2070. Rainfall pressure is *episodic* rather than continuous, but its
     consequence magnitude justifies a cluster weight equal to heat.
   - **Water stress 0.20** — CDD captures a slow-onset, sector-specific
     pathway that is structurally independent of single- and multi-day rainfall
     and matters distinctly to water-intensive industry. It receives less
     weight than heat or rainfall because its impact is mediated by local
     groundwater / reservoir storage / on-site water-reuse infrastructure that
     this Phase-1 score does not model.
   The 0.40 / 0.40 / 0.20 split is the single most consequential judgment and
   the main lever for revision.

2. **Within-cluster split by evidence strength and inter-metric correlation.**
   - Heat: TXx alone (0.40). Only one heat rule in this bundle; WBGT-conditioned
     refinement deferred (BL-0021).
   - Rainfall: Rx1day (0.25) > Rx5day (0.15). Rx1day has a high-confidence IMD
     categorical band and the strongest direct disruption association at the
     acute scale; Rx5day's band is self-derived and partially structurally
     redundant with Rx1day (5-day totals are often dominated by a single
     extreme day), so it carries less weight.
   - Water stress: CDD alone (0.20). Only one water-stress rule.

3. **Sanity checks.** Weights are positive, sum to 1.0, no single rule
   dominates (max 0.40 for TXx, justified by the only continuous-pressure
   quantification in the bundle). Low-confidence rules (Rx5day) carry the
   smallest weight. Bands rated low or medium confidence carry smaller
   within-rule impact weights (Section 4.3); high-confidence bands (TXx,
   Rx1day) carry the standard 0.35 impact weight.

These weights are revisable expert assumptions, not derived constants; any
change is a methodology change to be recorded and tested.

**Per-lens weights within each rule** (absolute / change / impact) are recorded
in `config/proposal_bundles.py` and shown per metric in Sections 7.1-7.4. The
high-confidence external-band rules (TXx, Rx1day) use **0.40 / 0.25 / 0.35**;
the medium-confidence IMD-anchored CDD uses **0.40 / 0.30 / 0.30** (slightly
smaller impact weight than the external-band rules); the low-confidence
self-derived Rx5day uses **0.45 / 0.40 / 0.15**, deliberately giving the weak
impact band a small share so it cannot dominate the rule (Section 4.3).

- **Coverage gate:** adopt the standard 0.70 available-rule-weight gate
  (Section 5.3) — matching Health and Agricultural.
- **Source masters:** all four source metrics must resolve to grid-first
  district/block masters (compute the index per grid cell, then area-weight to
  the polygon), consistent with the spatial-aggregation recommendation in
  `docs/bundle_calculation_audit.md`. TXx and Rx1day already route through
  grid-first paths; Rx5day and CDD need verification (likely the same paths but
  not confirmed for this bundle).
- **Phantom-slug renames (CHG-0015):** the dossier presents the renamed slugs
  (`rx1day_ge_200`, `rx5day_accumulated_pressure`, `cdd_water_stress_pressure`)
  to satisfy Section 4.7 (no labels naming thresholds the math does not apply).
  Migration is a data-contract change tracked separately.
- **Deferred refinements:** WBGT-conditioned industrial-heat rule (BL-0021);
  Aqueduct / CGWB groundwater coupling for CDD impact (BL-0022); JRC flood
  depth ingestion for direct flood-pressure measurement rather than the
  rainfall proxy (BL-0023). All three are Phase-2 additions, not corrections.

---

## 8. Thematic Bundles — Deferred Extension

The lens machinery is intended to extend to the thematic bundles (Heat Risk,
Drought Risk, Extreme Rainfall, Riverine Flood, Heat Stress, Cold Risk), where
the same "District B" blind spot applies: the live thematic scorer
(`analysis/bundle_scores.py`) uses plain min-max normalization within the
current-period cohort, which is escalation-blind and has no physical-danger
anchoring.

This extension is **deferred until the sectoral bundles are complete**, because
the thematic blast radius is larger (live dashboard scoring, six bundles,
persisted composites, and all downstream rankings / percentiles / risk classes /
drivers) and because moving the thematics off purely-relative screening reverses
a currently-stated design intent. It should be undertaken as a deliberate,
separately-tested methodology change with the science owner.

---

## 9. References (consolidated)

1. OECD & Joint Research Centre (2008). *Handbook on Constructing Composite
   Indicators: Methodology and User Guide.* OECD Publishing, Paris.
2. Anandhi A. et al. (2011). "Examination of change factor methodologies for
   climate change impact assessment." *Water Resources Research* 47(3), W03501.
3. IPCC (2021). *The concept of risk in the IPCC Sixth Assessment Report.*
4. Zhang X. et al. (2011). "Indices for monitoring changes in extremes based on
   daily temperature and precipitation data." *WIREs Climate Change* 2(6):
   851-870.
5. McKee T.B., Doesken N.J., Kleist J. (1993). "The relationship of drought
   frequency and duration to time scales." *Proc. 8th Conf. on Applied
   Climatology*, AMS.
6. Gasparrini A. et al. (2015). "Mortality risk attributable to high and low
   ambient temperature: a multicountry observational study." *The Lancet*
   386(9991): 369-375.
7. Sherwood S.C., Huber M. (2010). "An adaptability limit to climate change due
   to heat stress." *PNAS* 107(21): 9552-9555.
8. Vecellio D.J. et al. (2022). "Evaluating the 35 deg C wet-bulb temperature
   adaptability threshold for young, healthy subjects (PSU HEAT Project)."
   *Journal of Applied Physiology* 132(2): 340-345.
9. India Meteorological Department — *FAQ on Heat Wave* / Heat Wave Guidance;
   NDMA — *Heat Wave guidelines* (plains heatwave and departure-based warm-night
   criteria).
10. India Meteorological Department — daily rainfall classification (heavy /
    very heavy / extremely heavy).
11. World Health Organization — *Floods* health-topic guidance; systematic
    reviews of extreme water-related weather events and waterborne disease.
12. International Labour Organization (2019). *Working on a warmer planet: The
    impact of heat stress on labour productivity and decent work.* ILO, Geneva.
    (~5.8% India working-hours loss by 2030, ~34M full-time jobs equivalent —
    largest national loss globally.)
13. Sahu S., Sett M., Kjellstrom T. (2013). "Heat exposure, cardiovascular
    stress and productivity in rice harvesting workers in India: implications
    for a climate change future." *Industrial Health* 51(4): 424-431.
    (~5% productivity loss per deg C of WBGT above ~26 deg C, Southern India
    outdoor workers.)
14. Chuphal D.S. et al. (2025). "Multi-Day Extreme Precipitation Caused Major
    Floods in India During Summer Monsoon of 2024." *Earth's Future* 13.
    (3-day extreme precipitation return periods >75 to >200 years for the
    three regions with the largest 2024 floods.)
15. Swiss Re Institute. *Billion-dollar Rain: Why India can't afford to ignore
    urban flood risk.* Swiss Re, Zurich.
    (Mumbai 2005 ~USD 3B economic loss; Chennai 2015 ~USD 1.3B insured;
    Kerala 2018 ~USD 3.5B recovery cost.)
16. India Meteorological Department — agromet operational definitions:
    **Dry Day** (rainfall < 2.5 mm or < 6.3 mm in IMD agromet studies);
    **Drought Week** (7 days with rainfall <= 50% of long-period normal);
    **Agricultural Drought** (4 consecutive Drought Weeks between May and
    October, i.e. ~28 days). Anchors the CDD onset for Industrial Risk
    Section 7.3.
17. Central Water Commission — flood-forecasting framework (station-specific
    warning and danger gauge levels in consultation with state authorities);
    no universal mm-based 5-day rainfall categorical threshold is published,
    motivating the self-derived Industrial Risk Rx5day band in Section 7.2.
