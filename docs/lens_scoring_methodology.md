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
  the sectoral bundles are complete (see Section 14).

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

Persisted per `scenario x period` in the bundle master, for each rule. All
column names follow the load-bearing four-token master schema
`{metric_or_rule}__{scenario}__{period}__{stat}`; the lens marker is the
**stat** token (last position):

```text
{rule_slug}__{scenario}__{period}__score        # combined rule score (existing)
{rule_slug}__{scenario}__{period}__abs_score    # absolute-lens component
{rule_slug}__{scenario}__{period}__chg_score    # change-lens component
{rule_slug}__{scenario}__{period}__imp_score    # impact-lens component
```

And per bundle (existing, retained):

```text
{composite_slug}__{scenario}__{period}__mean
{composite_slug}__{scenario}__{period}__available_rule_count
{composite_slug}__{scenario}__{period}__available_rule_weight_fraction
```

Rules:
- **Only active lenses are persisted (sparse policy).** A lens is "active" on
  a rule iff its weight is `> 0` in the rule spec. Rules configured with only
  `absolute_weight > 0` emit only `__score` and `__abs_score`; rules configured
  with absolute+change and no impact lens, such as Thermal
  `spi3_low_flow_proxy_norm`, emit `__score`, `__abs_score`, and
  `__chg_score`, but no `__imp_score`.
- For an active lens that is unavailable on some rows (e.g., change lens with
  a missing historical baseline column, or `relative_pct` change with a
  zero per-row baseline), the lens column is still written but those rows
  carry NaN. The blended `__score` renormalizes row-wise across the
  available active lenses.
- The combined `__score` column name is unchanged, so the schema extension is
  **backward-compatible** with existing readers and the composite roll-up.
- The lens columns are part of the `processed_optimised/` contract from
  optimized-artifact version 3 onward (see
  `docs/processed_optimised_vendor_data_contract.md`). Today the Glance view
  reads only `__score`; deep-dive rule diagnostics that consume the lens
  columns are tracked in `docs/BACKLOG.md` (BL-0082).

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
- **Source masters:** all four source metrics resolve to grid-first
  district/block masters (compute the index per grid cell, then area-weight to
  the polygon), consistent with the spatial-aggregation recommendation in
  `docs/bundle_calculation_audit.md`. TXx routes through
  `india_resilience_tool/compute/heat_risk_gridfirst.py`; Rx1day, Rx5day, and
  CDD route through
  `india_resilience_tool/compute/extreme_rainfall_gridfirst.py` (CDD via
  `EXTREME_RAINFALL_GRIDFIRST_SLUGS` membership added in CHG-0029).
- **Phantom-slug renames (CHG-0015):** the dossier presents the renamed slugs
  (`rx1day_ge_200`, `rx5day_accumulated_pressure`, `cdd_water_stress_pressure`)
  to satisfy Section 4.7 (no labels naming thresholds the math does not apply).
  Migration is a data-contract change tracked separately.
- **Deferred refinements:** WBGT-conditioned industrial-heat rule (BL-0021);
  Aqueduct / CGWB groundwater coupling for CDD impact (BL-0022); JRC flood
  depth ingestion for direct flood-pressure measurement rather than the
  rainfall proxy (BL-0023). All three are Phase-2 additions, not corrections.

---

## 8. Investment / Financial Risk — Metric-by-Metric Lens Dossier

Bundle: `Sector-wise - Investment / Financial Risk` | composite slug:
`composite_investment_financial_risk` | levels: district, block | scenarios:
`ssp245`, `ssp585`.

Conceptual scope: climate hazards most directly tied to **the financial
materiality of a fixed-location investment** — extreme rainfall (acute and
accumulated flood disruption to leased assets, business interruption,
supply-chain breaks), changing rainfall regime concentration (shifting
return-period design assumptions), chronic heat persistence (labour-productivity
loss, peak-power demand stress, equipment derating), and chronic water-supply
stress (operational interruption for water-intensive sub-sectors). Per
Section 1.2 this is hazard pressure, not full financial risk: it does not
include asset-level exposure (where the capital sits), adaptive capacity
(insurance, flood-proofing, on-site storage), or transition risk
(policy / market / technology).

**Methodology change recorded here.** CHG-0033 reshapes the active code for
this bundle from its previous **trend-dominated** design (four `_trend_rule`
rules plus one phantom-slug blended rule on CDD) to the **standard
abs/chg/imp lens template** already used by Health (Section 6) and Industrial
(Section 7). The reshape is not stylistic — it brings the bundle into
alignment with industry and scientific best practice for physical climate risk
in financial portfolios:

1. **Industry frameworks use scenario-conditional state at a horizon, not
   within-horizon linear trend.** TCFD recommendations frame physical-risk
   methods as "estimating the expected loss... **at a given time horizon and in
   a given scenario**" (TCFD 2017). NGFS publishes scenario-conditional
   projections at **2030 / 2050 / 2100** horizons (NGFS 2024). RBI's *Draft
   Disclosure Framework on Climate-related Financial Risks* (Feb 2024)
   directs Indian banks to assess physical risk "**across short, medium, and
   long-term horizons**" under RCP / SSP scenarios. None of these frameworks
   uses within-future-period linear trend as a primary scoring construct.
   The lens-based **change lens** (compare future-period-mean to the
   `1990-2010` baseline-mean) is precisely what TCFD / NGFS / RBI methodology
   calls for.

2. **Within-horizon linear trends are vulnerable to internal climate
   variability.** IPCC AR6 trend-detection practice acknowledges that
   "robustness and confidence levels depend on the ability of climate models
   to adequately simulate internal climate variability, particularly on longer
   multidecadal time scales. Sampling variability over short 30-year running
   periods would be subject to strong influence from decadal climate
   fluctuations where stochastic noise can obscure the impact of greenhouse
   forcing." (Hawkins & Sutton 2009/2012; Frankignoul, Gastineau, Kwon 2017;
   IPCC AR6 WG1 Chapter 1.) A 20-year linear slope through a 2020-2040 future
   window can be driven by PDO / IPO / ENSO / NAO / IOD decadal modes rather
   than the forced climate-change signal. The change-lens construction
   (period-mean vs baseline-mean) averages over the period and is structurally
   less sensitive to this decadal noise.

3. **Trend rules lack baseline comparison, significance testing, and
   effect-size thresholds.** They rank any positive slope above any negative
   slope, regardless of absolute level, statistical significance, or
   effect-size relevance. The lens framework's three components handle these
   distinct questions — absolute (current/future level), change (emergence vs
   baseline), and impact (proximity to a physical danger band) — explicitly
   and separately.

The `_trend_rule` machinery in `india_resilience_tool/compute/proposal_bundles.py`
is **preserved as code** for possible future use as a supplementary signal but
**deprecated as a primary scoring construct**. After CHG-0033, no active
bundle uses it.

The table summarizes the lens decision per metric; each subsection gives the
reasoning and any band provenance. Rule slugs marked "renamed from" reflect
the CHG-0018 follow-up (the dossier presents the renamed slugs because they
reflect the actual scoring math; the rename and code reshape are tracked as a
separate data-contract change).

| Rule (metric) | absolute | change | impact (band) | Rationale summary |
|---|:--:|:--:|:--:|---|
| Rx1day — 1-day rainfall (`pr_max_1day_precip`) | yes | yes | yes — external, 115.6-204.5 mm | IMD very-heavy / extremely-heavy categories; acute flood disruption to leased assets and business interruption |
| Rx5day — 5-day rainfall (`pr_max_5day_precip`) | yes | yes | yes — self-derived (low conf), 250-500 mm | Multi-day basin-scale flood pressure; no external categorical band exists at 5-day scale |
| R99p — extreme wet precipitation (`r99p_extreme_wet_precip`) | yes | yes | **no** (regime metric, not a danger threshold) | Rainfall regime concentration / return-period design shift; harm pathway runs through Rx1day |
| CDD — consecutive dry days (`pr_consecutive_dry_days_lt1mm`) | yes | yes | yes — self-derived (med conf, IMD-anchored), 30-90 days | Chronic water-supply stress; IMD Agricultural Drought anchors onset |
| HWFI — heatwave frequency index (`hwfi_tmean_90p`) | yes | yes | yes — self-derived (low conf), 5-15 days/yr | Chronic heat persistence; labour-productivity loss + peak-power demand stress |

### 8.1 Rx1day — One-day rainfall (`pr_max_1day_precip`)

- **Lenses:** absolute (yes), change (yes), impact (yes).
- **absolute:** Keep. Districts facing the most intense projected single-day
  rainfall relative to peers (Rx1day, ETCCDI). The acute disruption signal for
  fixed-location investments — a single very-heavy day forces operational
  shutdowns, inundates leased premises, breaks logistics, and triggers
  business-interruption claims regardless of multi-day context.
- **change:** Keep. Intensifying one-day extremes vs the `1990-2010` baseline.
  Critical for financial valuation because investments are typically capitalized
  against historical-norm design assumptions (drainage capacity, plinth height,
  storm-water sizing, insurance pricing); a shifting acute regime alters the
  expected-loss profile across the holding period. Mode: `relative_pct`
  (precipitation change is conventionally multiplicative).
- **impact:** Keep. Band **115.6-204.5 mm/day**, **external provenance, high
  confidence** — IMD's daily rainfall classification (very heavy 115.6-204.4 mm
  / extremely heavy >= 204.5 mm in 24 hours). Same band Health Risk (Section
  6.4) and Industrial Risk (Section 7.1) use; the rainfall *value* triggering
  urban / industrial drainage failure is sector-receptor-independent.
  - Financial materiality: documented Indian single-day-extreme-event losses
    run $1-5B per event (Mumbai 2005 ~$3B; Chennai 2015 ~$1.3B; Kerala 2018
    ~$3.5B), with annual average ~$7.4B and projected ~6x growth to 2070
    (Swiss Re *Billion-dollar Rain*; World Bank flood-cost reports).
- **Per-lens weights:** absolute 0.40 / change 0.25 / impact 0.35 (mirrors
  Industrial Rx1day; high-confidence external band lets the impact lens carry
  meaningful share without dominating).

### 8.2 Rx5day — Five-day rainfall (`pr_max_5day_precip`)

- **Lenses:** absolute (yes), change (yes), impact (yes — self-derived).
- **absolute:** Keep. Districts with the heaviest projected 5-day accumulated
  rainfall relative to peers. Multi-day accumulated rainfall builds upstream
  runoff, saturates catchments, and overwhelms regional drainage even when no
  single day reaches the very-heavy IMD threshold — the basin-scale flood
  pressure that single-day extreme misses. Relevant to large fixed-location
  investments (manufacturing complexes, logistics hubs, real estate
  portfolios) where supply-chain and regional-access disruption cost more
  than on-site inundation.
- **change:** Keep. Intensifying multi-day accumulations vs baseline. Mode:
  `relative_pct`.
- **impact:** Keep, **self-derived band 250-500 mm/5 days, low confidence**
  (same band as Industrial Risk Section 7.2; same derivation rationale).
  No external categorical band exists at 5-day scale — IMD publishes daily
  categories only, CWC uses station-specific gauge danger levels, NDMA SOPs
  reference IMD daily categories. Onset 250 mm anchored on five-IMD-heavy-day
  cumulative (~322 mm) floor; saturation 500 mm anchored on observed
  catastrophic-event 5-day cumulative (Kerala 2018, Mumbai 2005) and the
  Chuphal et al. (2025) finding that major 2024 India flood events had 3-day
  return periods >75-200 years.
- **Per-lens weights:** absolute 0.45 / change 0.40 / impact 0.15 — small
  within-rule impact weight because the band is self-derived and low confidence
  (Section 4.3). Persistence signal is carried mainly by absolute and change.

### 8.3 R99p — Extreme wet precipitation (`r99p_extreme_wet_precip`)

- **Lenses:** absolute (yes), change (yes), **impact (no)**.
- **What R99p is:** ETCCDI Extreme Wet Days — the annual sum of precipitation
  on days where daily precipitation exceeded the **baseline 99th percentile**
  of wet-day precipitation (Zhang et al. 2011). It is a **percentile-based
  concentration / regime metric**, not a single-event danger metric. A high
  R99p means a larger share of the year's rainfall is being delivered by the
  rarest extreme events; a rising R99p signals **rainfall regime shifting
  toward more concentrated extremes**.
- **IRT source-path lock for CHG-0038:** admin district/block source masters
  now come from the same grid-first percentile-rainfall pipeline used for
  admin R95p, with an explicit locked methodology of baseline `1990-2010`,
  wet-day threshold `>= 1 mm/day`, `linear` quantile interpolation, and
  strict `>` exceedance. Hydro remains on the legacy polygon-average-first
  percentile path and is out of scope here.
- **absolute:** Keep. Districts where the annual contribution from extreme
  wet days is largest relative to peers — i.e., where rainfall is most
  concentrated in the tail of the distribution. This matters for capitalized
  fixed-asset investments because the design-event return period (drainage,
  storm-water, structural water-load assumptions) is implicitly calibrated to
  the historical distribution tail.
- **change:** Keep. Intensifying R99p vs the `1990-2010` baseline is the
  signal that **historical-norm design assumptions no longer hold** — the
  rainfall regime is concentrating into the tail faster than facilities were
  built for. Mode: `relative_pct`. For investor-relevant pathways, the *change*
  in R99p is more important than the absolute level, so the within-rule change
  weight is set above the absolute weight (0.60 vs 0.40 — the only rule in this
  bundle where change exceeds absolute).
- **impact:** **Dropped entirely** (`impact_weight=0`). R99p is mathematically a
  concentration metric, not a danger threshold — no external band exists and
  no defensible self-derived band can be constructed. The harm pathway from
  R99p runs **through Rx1day** (single-day extremes drive flood injury and
  drainage failure), which is already in this bundle with a high-confidence
  IMD band. Setting an arbitrary R99p impact band would be inventing a number
  to satisfy the framework rather than because it measures danger — that
  violates Section 4 (the impact lens scores danger, not unusualness). Honest
  call: keep R99p as an emergence/regime signal scored only by absolute and
  change.
- **Per-lens weights:** absolute 0.40 / change 0.60 / impact 0.00.

### 8.4 CDD — Consecutive dry days (`pr_consecutive_dry_days_lt1mm`)

- **Lenses:** absolute (yes), change (yes), impact (yes — IMD-anchored).
- **Same definition and band as Industrial Risk Section 7.3.** Source metric,
  band (30-90 days, medium confidence), and derivation are identical because
  the underlying climate hazard (prolonged dry spell) and its institutional
  anchor (IMD Agricultural Drought = 4 consecutive Drought Weeks ≈ 28 days
  rounded to 30) do not change by sector receptor.
- **What changes for Investment / Financial:** the *harm pathway* is the
  **financial valuation of water-dependent assets**, not industrial production
  per se. Investments in water-intensive businesses (beverage manufacturing,
  textile mills, semiconductor fabs, thermal power, paper mills, real-estate
  portfolios with utility cost exposure) face revenue and operating-margin
  compression during prolonged dry spells through forced derate, water-cost
  inflation, and in extreme cases plant shutdown.
- **Per-lens weights:** absolute 0.40 / change 0.30 / impact 0.30 — same as
  Industrial Risk Section 7.3 (medium-confidence IMD-anchored band justifies a
  larger within-rule impact weight than the low-confidence self-derived bands
  in this bundle).

### 8.5 HWFI — Heatwave frequency index (`hwfi_tmean_90p`)

- **Lenses:** absolute (yes), change (yes), impact (yes — self-derived).
- **What HWFI is:** Heat Wave Frequency Index — annual count of days
  belonging to heatwave spells of at least **5 consecutive days** above the
  baseline day-of-year 90th percentile of daily mean temperature `tas`. This
  is a percentile-based **persistence** metric structurally analogous to WSDI
  (Section 6.2 in Health) but computed on `tas` (daily mean) rather than
  `tasmax` (daily max) and with a 5-day minimum spell rather than WSDI's 6-day
  minimum.
- **absolute:** Keep. Districts with the most heatwave-day burden relative to
  peers. Relevant to investor risk because chronic heat-day exposure is the
  pathway for labour-productivity loss (ILO 2019: India projected to lose
  ~5.8% of total working hours by 2030, ~34M full-time jobs equivalent — the
  largest national loss globally) and peak-power demand stress (India's
  national peak demand reached a record **270.82 GW** on 21 May 2025 amid
  heat-driven cooling load, straining grid infrastructure and pushing
  operating costs across the economy).
- **change:** Keep. Lengthening heatwave-day burdens vs the `1990-2010`
  baseline shift the operating envelope for investments with labour-intensive
  workflows (manufacturing, construction, agriculture-adjacent processing) and
  for power-sector investments facing demand-side stress that scales with
  heatwave persistence. The 2024 India heatwaves reportedly cost ~$194B in
  potential income and ~247B labour-hours nationally — the financial
  materiality of *intensifying* persistent heat is now empirically established.
  Mode: `relative_pct` (proportional change in heatwave-day burden).
- **impact:** Keep, **self-derived band 5-15 days/yr, low confidence**
  (Section 4 protocol; analogous to WSDI Section 6.2).
  - **Mechanism:** consecutive hot days compound investor-relevant impacts
    because cooling-load, worker absenteeism, and equipment derating all scale
    with heatwave persistence, not just peak-day temperature.
  - **Cut points:** onset **5 days** — HWFI's own minimum qualifying spell
    length, already past the ~4-day added-mortality threshold in epidemiology
    (Beijing CVD study: ~10% excess at day 4, ~51% at day 5; the same duration
    threshold likely applies to labour-productivity and grid-stress pathways
    in India). Saturation **15 days/yr** — a high annual heatwave-day burden
    indicating ~three qualifying spells per year, the regime where labour and
    grid impacts compound into material financial consequence.
  - **Confidence: low.** HWFI is a percentile-based annual tally, not a single
    spell length, so the band is a pragmatic annual-burden proxy rather than a
    physical threshold. Given the low confidence, the band is given a **small
    within-rule impact weight** (0.15); the persistence signal is carried
    mainly by absolute and change.
- **Per-lens weights:** absolute 0.45 / change 0.40 / impact 0.15 (mirrors WSDI
  in Health Risk Section 6.2).

### 8.6 Investment / Financial Risk — bundle assembly notes

**Rule weights (explicit, sum to 1.0).** Same elicitation recipe as Health and
Industrial: **rule_weight reflects (TCFD-recognized financial materiality) x
(band confidence) x (structural independence)**.

| Cluster | Cluster weight | Rule | Rule weight | Why |
|---|---:|---|---:|---|
| Flood (acute + multi-day + regime) | 0.50 | Rx1day | 0.25 | High-confidence IMD band; well-documented major-event losses ($1-5B/event) to Indian financial portfolios; primary acute physical-risk category per TCFD |
| Flood | 0.50 | Rx5day | 0.15 | Low-confidence self-derived band; structurally overlaps Rx1day's signal; basin-scale supply-chain pressure |
| Flood | 0.50 | R99p | 0.10 | No impact band; regime-shift / design-assumption signal; smallest of the rainfall rules but captures a distinct concentration mechanism |
| Heat (chronic operational) | 0.25 | HWFI | 0.25 | ILO ~5.8% working-hours loss by 2030; 2024 India ~$194B potential income / ~247B labour-hours lost; record 270.82 GW peak power demand — the most empirically-documented chronic financial pathway |
| Water stress (chronic supply) | 0.25 | CDD | 0.25 | Medium-confidence IMD-anchored band; structurally independent slow-onset pathway affecting water-intensive sub-sectors |

**How these weights were derived.** Same two-stage elicitation as Health and
Industrial:

1. **Cluster split first (flood vs heat vs water stress).** The bundle's five
   metrics fall into three TCFD-aligned physical-risk clusters:
   - **Flood 0.50** — the dominant acute physical-risk category for built-asset
     financial portfolios per TCFD recommendations and Swiss Re / World Bank
     loss accounting. Major Indian flood events impose $1-5B in industrial
     and infrastructure losses per event with annual average ~$7.4B, projected
     ~6x growth to 2070. Three structurally distinct sub-signals (acute
     single-day, accumulated multi-day, regime concentration) justify
     decomposition into three rules rather than a single rainfall rule.
   - **Heat 0.25** — chronic operational pressure quantified by ILO
     working-hours loss projections (~5.8% India by 2030, the largest national
     loss globally), India's 2024 ~$194B / ~247B labour-hour empirical
     experience, and the documented grid-stress at record peak-power demand
     (270.82 GW). HWFI is the persistence signal most relevant to investor-
     relevant productivity and demand pathways.
   - **Water stress 0.25** — slow-onset chronic pathway structurally
     independent of rainfall extremes; medium-confidence IMD-anchored band
     (the strongest non-IMD-categorical anchor in the bundle); affects
     water-intensive sub-sectors through a distinct mechanism.
   The 0.50 / 0.25 / 0.25 split aligns with TCFD physical-risk taxonomy
   (acute > chronic) and is the single most consequential judgment.

2. **Within-cluster split by evidence strength and inter-metric correlation.**
   - Flood: Rx1day (0.25) > Rx5day (0.15) > R99p (0.10). Rx1day has the
     high-confidence IMD categorical band and the strongest direct acute
     disruption association; Rx5day adds basin-scale pressure but its band is
     self-derived and partially structurally redundant with Rx1day; R99p adds
     a regime-shift signal with no impact band, so it receives the smallest
     flood-cluster weight.
   - Heat: HWFI alone (0.25). Only one heat rule in this bundle.
   - Water stress: CDD alone (0.25). Only one water-stress rule.

3. **Sanity checks.** Weights are positive, sum to 1.0, no single rule
   dominates (max 0.25 each for Rx1day / HWFI / CDD). Low-confidence rules
   (Rx5day, R99p, HWFI) carry within-cluster smaller weights or smaller
   within-rule impact weights. Bands rated low or medium confidence carry
   smaller within-rule impact weights (Section 4.3); the single
   high-confidence band (Rx1day, IMD) carries the standard 0.35 impact weight.

These weights are revisable expert assumptions, not derived constants; any
change is a methodology change to be recorded and tested.

**Per-lens weights within each rule** (absolute / change / impact) are recorded
in `config/proposal_bundles.py` and shown per metric in Sections 8.1-8.5. The
high-confidence external-band rule (Rx1day) uses **0.40 / 0.25 / 0.35**; the
medium-confidence IMD-anchored CDD uses **0.40 / 0.30 / 0.30**; the
low-confidence self-derived bands (Rx5day, HWFI) use **0.45 / 0.40 / 0.15**;
the regime-metric R99p with no defensible impact band uses **0.40 / 0.60 /
0.00** with change-weight elevated above absolute because the regime *shift*
is the financial-risk signal.

- **Coverage gate:** adopt the standard 0.70 available-rule-weight gate
  (Section 5.3) — matching Health, Industrial, and Agricultural.
- **Source masters:** all five source metrics must resolve to grid-first
  district/block masters (compute the index per grid cell, then area-weight to
  the polygon), consistent with the spatial-aggregation recommendation in
  `docs/bundle_calculation_audit.md`. Rx1day and CDD already route through
  grid-first paths; Rx5day, R99p, and HWFI need verification.
- **Phantom-slug and rule-type reshape (CHG-0018):** the rule-type reshape
  (trend -> blended) is now landed in active code. The dossier still presents
  renamed slugs because they better describe the scoring math, but the slug
  migration remains a separate data-contract change.
- **`_trend_rule` machinery status:** code preserved in
  `india_resilience_tool/compute/proposal_bundles.py` for possible future use
  as a supplementary signal, but **deprecated as a primary scoring construct**.
  After CHG-0033, no active bundle uses it.
- **Deferred refinements:** SPI-12 long-term water-availability trajectory
  (BL-0024); coastal sea-level / cyclone exposure for coastal investments
  (BL-0025); explicit return-period framing for design-event communication
  (BL-0026). All three are Phase-2 additions, not corrections.

---

## 9. Infrastructure Risk — Metric-by-Metric Lens Dossier

Bundle: `Sector-wise - Infrastructure Risk` | composite slug:
`composite_infrastructure_risk` | levels: district, block | scenarios: `ssp245`,
`ssp585`.

Conceptual scope: climate hazards most directly tied to **fixed infrastructure
assets and their service continuity** — extreme rainfall (urban drainage and
culvert overwhelm, bridge scour, embankment overtopping, slope failure) and
extreme heat (rail buckling, pavement softening, transformer derating,
transmission-line conductor sag, bridge expansion-joint stress). Per Section 1.2
this is hazard pressure, not full infrastructure risk: it does not include
asset-specific exposure (where roads, bridges, lines, drains sit), adaptive
capacity (design-margin headroom, retrofit, redundancy), or vulnerability
(maintenance backlog, age, criticality).

### 9.0 Methodology change recorded

This dossier records the landed **CHG-0034** migration in
`config/proposal_bundles.py`. Infrastructure is now the fifth
`explicit_normalized` sector bundle, with a 0.70
`min_available_rule_weight_fraction` gate and three lens-decomposed rules:
Rx1day, Rx5day, and TXx. The core change versus the legacy Infrastructure
config is that **both rainfall rules now carry an impact lens** in addition to
absolute and change pressure, while TXx is rebalanced to the standard
high-confidence 0.40 / 0.25 / 0.35 split. Infrastructure is the asset class
for which threshold-design failures are the dominant climate harm pathway —
roads, bridges, urban drainage, embankments, rail, transmission lines,
transformers, and slope-stability works all have explicit design return-period
thresholds (IRC, CPHEEO, IS, IRS standards). For threshold-anchored damage the
impact lens (danger band) is the most directly meaningful of the three. The
bands themselves are reused from Health Risk and Industrial Risk (the IMD daily
rainfall categorical band for Rx1day; the Section-4 self-derived band for
Rx5day) — no new band derivation is required.

The table summarizes the lens decision per metric; each subsection gives the
reasoning and band provenance. Rule slugs marked "renamed from" reflect the
CHG-0019 rename of phantom slugs (Section 4.7); the dossier presents the
renamed slugs because the bands and labels now reflect the actual scoring math.

| Rule (metric) | absolute | change | impact (band) | Rationale summary |
|---|:--:|:--:|:--:|---|
| Rx1day — 1-day rainfall (`pr_max_1day_precip`) | yes | yes | yes — external, 115.6-204.5 mm | IMD very-heavy / extremely-heavy categories; urban drainage and culvert overwhelm at design-threshold scale |
| Rx5day — 5-day rainfall (`pr_max_5day_precip`) | yes | yes | yes — self-derived (low conf), 250-500 mm | Multi-day saturation regime; slope failure, embankment overtopping, bridge scour; no external 5-day categorical band exists |
| TXx — extreme daytime heat (`txx_annual_max`) | yes | yes | yes — external, 40-45 deg C (plains) | Rail buckling, pavement softening, transformer derating, conductor sag; IMD plains heatwave envelope |

### 9.1 Rx1day — One-day rainfall (`pr_max_1day_precip`)

- **Lenses:** absolute (yes), change (yes), impact (yes — methodology change).
- **absolute:** Keep. Districts facing the most intense projected single-day
  rainfall relative to peers (Rx1day, ETCCDI). The acute disruption signal —
  a single very-heavy day overwhelms urban drainage capacity, washes out
  culverts, inundates underpasses, and breaks road / rail logistics.
- **change:** Keep. Intensifying one-day extremes vs the `1990-2010` baseline
  matter because infrastructure design (drainage capacity, culvert sizing,
  storm-water mains, bridge waterway openings) is fixed against historical
  norms; rising one-day extremes erode the design margin. Mode: `relative_pct`
  (precipitation change is conventionally multiplicative).
- **impact:** **Add.** Band **115.6-204.5 mm/day**, **external provenance,
  high confidence** — same band Health Risk (Section 6.4) and Industrial Risk
  (Section 7.1) use. IMD daily rainfall classification: very heavy 115.6-204.4
  mm, extremely heavy >= 204.5 mm.
  - **Mechanism for infrastructure:** the IMD daily categorical band corresponds
    to the regime in which Indian municipal and rural drainage infrastructure
    is overwhelmed. CPHEEO *Manual on Storm Water Drainage* design intensities
    for Indian municipalities typically translate to roughly **100-150 mm/day
    of design capacity** depending on city tier and duration assumption; IRC
    SP-13 (small bridges and culverts) and IRC 5 (general features of design)
    use 25-year return-period flood design for typical road cross-drainage
    works. The IMD very-heavy threshold 115.6 mm is roughly at the upper end
    of typical municipal drainage capacity; the IMD extremely-heavy threshold
    204.5 mm corresponds to the regime where even metro drainage is overtopped.
  - **Empirical anchors.** Major Indian flood events that overwhelmed urban
    drainage and infrastructure: Mumbai 2005 (944 mm/24 h, drainage failure
    citywide); Chennai 2015 (494 mm/24 h, IT corridor inundation); Hyderabad
    2020 (~190-300 mm/24 h across stations, mass road / underpass flooding);
    Bengaluru 2022 (multi-week recurrent flooding from sub-extreme but
    repeated heavy days). These events sit at and above the IMD
    extremely-heavy threshold.
  - **Why the same band as Health and Industrial:** the hazard *value*
    threshold for drainage overwhelm does not change by sector receptor — it
    is set by the rainfall intensity. Infrastructure, Health, and Industrial
    differ in *consequence* (asset damage vs flood injury vs operational
    shutdown), not in the rainfall amount that triggers failure of
    urban / industrial drainage.
- **Per-lens weights:** absolute 0.40 / change 0.25 / impact 0.35 (mirrors
  Health and Industrial — high-confidence external band lets the impact lens
  carry meaningful share without dominating).

### 9.2 Rx5day — Five-day rainfall (`pr_max_5day_precip`)

- **Lenses:** absolute (yes), change (yes), impact (yes — methodology change,
  self-derived band).
- **absolute:** Keep. Districts with the heaviest projected 5-day accumulated
  rainfall relative to peers. Captures **basin-scale and saturation-regime**
  pressure that a single-day extreme can miss — sustained multi-day rainfall
  builds upstream runoff, saturates catchments, raises antecedent soil
  moisture, and overwhelms regional drainage even when no single day reaches
  the IMD very-heavy threshold.
- **change:** Keep. Intensifying multi-day accumulations vs baseline. Mode:
  `relative_pct`.
- **impact:** **Add, self-derived band 250-500 mm/5 days, low confidence**
  (Section 4 protocol — same derivation as Industrial Risk Section 7.2). **No
  external categorical band exists at 5-day scale**: IMD publishes daily
  rainfall categories only, CWC uses station-specific gauge danger levels
  (not universal mm thresholds), IRC and CPHEEO design uses return-period
  flood discharge rather than mm thresholds, and NDMA SOPs reference IMD daily
  categories. The band is therefore derived rather than borrowed.
  - **Mechanism for infrastructure:** sustained multi-day rainfall produces
    catchment saturation, raised antecedent soil moisture, slope failures
    along cuttings and embankments, bridge-pier and bridge-abutment scour
    from prolonged high discharges, and embankment overtopping along
    flood-protection works. These pathways are **structurally distinct from
    Rx1day** — slope failure and embankment scour are saturation-driven,
    not single-storm-intensity-driven.
  - **Anchors:**
    - Five consecutive IMD "heavy" days (>= 64.5 mm/day per IMD daily
      classification) sum to **>= 322 mm** — onset of sustained heavy-rain
      regime; **250 mm** is a conservative floor capturing the onset of
      plausible saturation-driven infrastructure failure.
    - Observed Indian catastrophic-event 5-day cumulative magnitudes anchor
      the saturation point: Kerala 2018 (cumulative ~350-400 mm over 3 days
      regionally, with widespread slope failure, bridge washouts, and
      reservoir overspill); Uttarakhand 2013 (multi-day pre-saturation
      enabled the cloudburst event's downstream slope-failure damage);
      Chuphal et al. 2025 attribute 2024 India major flood events to 3-day
      return periods of **>75 to >200 years** — confirming the **500 mm /
      5-day** regime is statistically extreme and operationally catastrophic.
  - **Cut points:** onset **250 mm** (plausible saturation-driven
    infrastructure failure regime); saturation **500 mm** (regional
    flood-catastrophe regime).
  - **Confidence: low.** Anchored only on derivation (IMD daily category × 5)
    and observed disaster magnitudes; no institutional categorical band
    exists at 5-day scale. The 5-day signal partially overlaps Rx1day (high
    1-day events often dominate 5-day totals), so the rule's bundle weight is
    also smaller than Rx1day's.
- **Per-lens weights:** absolute 0.45 / change 0.40 / impact 0.15 — small
  within-rule impact weight because the band is self-derived and low
  confidence (Section 4.3). The persistence signal is carried mainly by
  absolute and change.

### 9.3 TXx — Extreme daytime heat (`txx_annual_max`)

- **Lenses:** absolute (yes), change (yes), impact (yes).
- **absolute:** Keep. Same definition as in Health (Section 6.1) and
  Industrial (Section 7.4). Districts facing the most extreme projected
  daytime heat relative to peers.
- **change:** Keep. Warming daytime extremes vs the `1990-2010` baseline
  matter to infrastructure because assets (rail, pavement, transformers,
  transmission lines, bridge expansion joints) are sized against historical
  extremes. Mode: `absolute_delta` (degrees).
- **impact:** Keep. Band **40-45 deg C**, **external provenance, high
  confidence** — same IMD plains heatwave band as Health (Section 6.1) and
  Industrial (Section 7.4).
  - **Infrastructure harm pathways through the same dry-bulb band:**
    - **Rail buckling.** Indian Railways operational guidance restricts
      train speeds when rail temperatures exceed approximately 65 deg C
      (which corresponds to ambient ~45 deg C under typical solar loading).
      Speed restrictions cascade into network-wide schedule disruption.
    - **Pavement softening / rutting.** Bituminous pavement begins to soften
      above ~50 deg C surface temperature; ambient ~45 deg C with direct
      insolation produces such surface temperatures, accelerating rutting on
      heavily-trafficked corridors.
    - **Transformer derating and conductor sag.** IEEE / IEC transformer
      thermal-life standards halve insulation life per +6-10 deg C above
      design ambient; transmission conductors above ~45 deg C ambient sag
      enough to violate statutory clearance, forcing partial-load operation
      or local outages.
    - **Bridge expansion-joint stress** from diurnal thermal cycling on the
      largest-extreme days.
  - **Why the IMD heatwave band rather than asset-specific thermal-fatigue
    or WBGT bands:** the IRT's TXx is a dry-bulb annual maximum, and the
    IMD heatwave definition is the nationally-recognized plains threshold
    for that regime. Asset-specific thermal-fatigue indicators (diurnal
    range, thermal-cycle counts) would require additional source metrics
    and are deferred (BL-0027). For Phase-1, the IMD plains band is the
    consistent, defensible band shared across Health, Industrial, and
    Infrastructure bundles.
  - **Zone caveat:** plains/national default (Section 4.9, BL-0020).
- **Per-lens weights:** absolute 0.40 / change 0.25 / impact 0.35 — mirrors
  Health Risk's and Industrial Risk's TXx (high-confidence external band).

### 9.4 Infrastructure Risk — bundle assembly notes

**Rule weights (explicit, sum to 1.0).** Weighting is an evidence-informed
expert elicitation, not a derived constant; the recommended default reflects
the relative infrastructure-sector climate burden in India and is recorded as
a revisable assumption.

| Cluster | Cluster weight | Rule | Rule weight | Why |
|---|---:|---|---:|---|
| Rainfall (acute) | 0.75 | Rx1day (1-day rainfall) | 0.45 | High-confidence IMD band; direct urban-drainage / culvert / underpass failure pathway; major-event losses ($1-5B per event: Mumbai 2005 ~$3B, Chennai 2015 ~$1.3B, Kerala 2018 ~$3.5B; annual avg ~$7.4B mostly infrastructure) |
| Rainfall (multi-day) | 0.75 | Rx5day (5-day rainfall) | 0.30 | Low-confidence self-derived band; partially overlaps Rx1day but captures the structurally distinct slope-failure / embankment-overtopping / bridge-scour pathway |
| Heat | 0.25 | TXx (extreme heat) | 0.25 | High-confidence IMD plains heatwave band; rail-buckling speed restrictions, pavement rutting, transformer derating, conductor sag — real but episodic, smaller documented direct-loss magnitudes than flood events |

**How these weights were derived.** Same two-stage elicitation as Health,
Industrial, and Investment/Financial: **rule_weight reflects (sector-specific
loss-burden evidence) x (band confidence) x (structural independence)**.

1. **Cluster split first (rainfall vs heat).** The bundle's three metrics fall
   into two hazard clusters — rainfall (Rx1day, Rx5day) and heat (TXx).
   - **Rainfall 0.75** — Indian infrastructure losses are dominated by acute
     flood events. Major-event direct losses: Mumbai 2005 ~$3B, Chennai 2015
     ~$1.3B, Kerala 2018 ~$3.5B, Uttarakhand 2013 ~$1B+; annual average
     flood losses ~$7.4B, projected to grow ~6x by 2070 (Swiss Re / World
     Bank reinsurance loss accounting). The bulk of these reported figures
     are **infrastructure damage and reconstruction**, not industrial
     productivity. Rainfall pressure is therefore by far the largest
     climate-driven loss category for Indian infrastructure.
   - **Heat 0.25** — Heat affects rail, transmission, pavement, transformers.
     Real but with smaller documented direct loss magnitudes than flood
     events. Unlike Industrial Risk, Infrastructure does **not** carry the
     continuous ILO labor-productivity dose-response (workers are not the
     asset receptor); heat impacts here are episodic operational restrictions
     and accelerated thermal fatigue.
   - **Why higher rainfall share than Industrial Risk (0.75 vs 0.40):**
     Industrial Risk's 0.40 rainfall share reflected that Industrial also
     carries the ILO continuous heat-productivity pressure
     (~5.8% working-hours loss by 2030, ~34M full-time jobs equivalent).
     Infrastructure has no comparable continuous-heat dose-response, so its
     cluster split naturally tilts further toward rainfall.
   The 0.75 / 0.25 split is the single most consequential judgment and the
   main lever for revision.

2. **Within-cluster split by band confidence and structural independence.**
   - Rainfall: Rx1day (0.45) > Rx5day (0.30). Rx1day has a high-confidence
     IMD categorical band and the strongest direct disruption association at
     the acute scale (urban drainage, culverts). Rx5day's band is
     self-derived (low confidence) and partially overlaps Rx1day (multi-day
     extremes are often dominated by a single day) but captures the
     structurally distinct slope-stability / embankment-overtopping /
     bridge-scour pathway, so it carries a non-trivial-but-smaller share.
   - Heat: TXx alone (0.25). Only one heat rule; asset-specific
     thermal-fatigue refinement deferred (BL-0027).

3. **Sanity checks.** Weights are positive, sum to 1.0, no single rule
   dominates (max 0.45 for Rx1day, justified by the dominant flood-loss
   evidence). Low-confidence Rx5day (within-rule impact 0.15) carries the
   smallest impact weight; high-confidence external-band rules (Rx1day, TXx)
   carry the standard 0.35 within-rule impact weight.

These weights are revisable expert assumptions, not derived constants; any
change is a methodology change to be recorded and tested.

**Per-lens weights within each rule** (absolute / change / impact) are
recorded in `config/proposal_bundles.py` after the CHG-0020 reshape and shown
per metric in Sections 9.1-9.3. The high-confidence external-band rules
(TXx, Rx1day) use **0.40 / 0.25 / 0.35**; the low-confidence self-derived
Rx5day uses **0.45 / 0.40 / 0.15**, deliberately giving the weak impact band
a small share so it cannot dominate the rule (Section 4.3).

- **Coverage gate:** the code now uses the standard 0.70
  available-rule-weight gate (Section 5.3), matching Health, Industrial,
  Investment/Financial, and Agricultural.
- **Source masters:** all three source metrics must resolve to grid-first
  district/block masters (compute the index per grid cell, then area-weight
  to the polygon), consistent with the spatial-aggregation recommendation in
  `docs/bundle_calculation_audit.md`. TXx and Rx1day already route through
  grid-first paths; **Rx5day grid-first provenance remains a follow-on
  verification item** and is not resolved by CHG-0034.
- **Phantom-slug renames (CHG-0019):** CHG-0034 keeps the current rule slugs
  for contract stability. `rx1day_ge_200` is **kept** because "200" sits
  inside the IMD band (≈ IMD extremely-heavy >= 204.5 mm) — same call as
  Industrial Risk Section 7.1; `rx5day_ge_400` **remains in code for now**
  even though the active impact band is 250-500 mm/5 days, and the proposed
  rename to `rx5day_accumulated_pressure` stays deferred to a separate
  data-contract change; `txx_ge_45` is **kept** because 45 deg C is the upper
  edge of the impact band 40-45 deg C — same call as Health and Industrial.
- **Deferred refinements:** JRC global flood-depth ingestion for direct
  flood-pressure measurement (BL-0023, shared with Industrial); coastal
  sea-level / cyclone wind & storm-surge exposure for coastal infrastructure
  (BL-0025, shared with Investment/Financial); asset-level pavement /
  transformer thermal-fatigue rule using diurnal range and thermal-cycle
  counts (BL-0027); river discharge / bridge-scour rule from CWC station
  discharge or modeled streamflow (BL-0028); antecedent-rainfall /
  soil-moisture index for slope-stability landslide pressure (BL-0029). All
  five are Phase-2 additions, not Phase-1 corrections.

---

## 10. Asset Risk (Thermal Power Plants) — Metric-by-Metric Lens Dossier

Bundle: `Sector-wise - Asset Risk (Thermal Power Plants)` | composite slug:
`composite_asset_risk_thermal_power` | levels: district, block | scenarios:
`ssp245`, `ssp585`.

Conceptual scope: climate hazards most directly tied to **thermal-power
generation assets and their cooling-water and ambient-air dependencies** —
prolonged dry spells and cumulative rainfall deficits (cooling-water
availability), and extreme ambient heat (Carnot efficiency loss, cooling-tower
and air-cooled condenser derating, equipment thermal stress). Per Section 1.2
this is hazard pressure, not full thermal-asset risk: it does not include
plant-specific exposure (cooling technology mix, water-source type, age),
adaptive capacity (dry/hybrid cooling retrofit, on-site storage, reuse), or
vulnerability (PPA terms, financial buffer, regulatory mandates on minimum
generation).

The Indian thermal fleet (~205 GW coal + ~25 GW gas + ~7 GW nuclear) supplies
the bulk of national electricity. Water stress is the **dominant** documented
climate vulnerability: WRI 2018 *Parched Power* shows that ~90 % of India's
thermal capacity uses freshwater cooling, ~40 % is sited in high-water-stress
areas, 13 of 20 largest plants experienced at least one water-shortage
shutdown between 2013-2016, and ~14 TWh of generation was lost in 2016 alone
to water shortages. CEEW (2018, 2021) repeatedly identifies water availability
as the #1 climate risk for the Indian thermal fleet.

### 10.0 Methodology change implemented (CHG-0057/0058)

CHG-0057 and CHG-0058 implement this dossier in
`config/proposal_bundles.py` and `compute/proposal_bundles.py`:

1. **CDD now carries an impact lens.** It uses the same
   IMD-Agricultural-Drought-anchored 30-90 day band adopted for Industrial
   Risk Section 7.3 — medium confidence, with derivation reusing an existing
   institutional anchor.
2. **SPI-3 now carries an operational change lens.** The builder no longer
   routes `spi3_low_flow_proxy_norm` through an absolute-only special case.
   The chg signal captures whether dry-month frequency is *rising vs
   baseline*, which is what physical-risk frameworks (TCFD
   scenario-at-horizon, Section 8) call for, and is structurally distinct from
   SPI's own baseline-standardization (SPI's gamma fit is fixed on the
   historical period; the chg lens measures how often projections cross that
   historical-baseline-conditioned threshold relative to baseline). The lens
   remains tied to the currently available legacy historical baseline column
   until the broader baseline reconciliation lands.

The dossier also makes an **honest no-impact-lens call for SPI-3** — same
rationale as Investment/Financial Risk Section 8.3 for R99p: SPI-3 dry-month
frequency is a regime / proxy metric with no defensible institutional band on
count-of-months/year. Inventing a self-derived band on a metric that is
already a soft proxy for cooling-water unavailability would be doubly derived.
The level + shift signal is the honest scientific reading.

The table summarizes the lens decision per metric; each subsection gives the
reasoning and band provenance. Shipped config preserves the current public
slugs (`cdd_ge_30`, `txx_ge_45`, `spi3_low_flow_proxy_norm`) so existing
processed paths, dashboards, and optimized artifacts remain stable. Cosmetic
or semantic slug renames remain deferred under the phantom-slug /
data-contract rename track.

| Rule (metric) | absolute | change | impact (band) | Rationale summary |
|---|:--:|:--:|:--:|---|
| CDD — consecutive dry days (`pr_consecutive_dry_days_lt1mm`) | yes | yes | yes — self-derived (med conf, IMD-anchored), 30-90 days | Cooling-water unavailability; same IMD Agricultural Drought anchor as Industrial |
| TXx — extreme daytime heat (`txx_annual_max`) | yes | yes | yes — external, 40-45 deg C (plains) | Carnot efficiency loss, cooling-tower / air-cooled condenser derating, equipment thermal stress; IMD plains heatwave envelope |
| SPI-3 dry-month frequency (`spi3_count_months_lt_minus1`) | yes | yes | **no** — drop | Regime / cumulative-rainfall-deficit proxy for low-flow cooling-water availability; no defensible institutional band on count-of-months/year |

### 10.1 CDD — Consecutive dry days (`pr_consecutive_dry_days_lt1mm`)

- **Lenses:** absolute (yes), change (yes), impact (yes — methodology change,
  IMD-anchored).
- **absolute:** Keep. Districts with the longest projected continuous dry
  spells relative to peers (CDD, ETCCDI: maximum consecutive days with
  precipitation < 1 mm). Direct cooling-water pathway: prolonged dry spell
  -> reservoir / river drawdown -> curtailed plant cooling-water draws ->
  forced derating or shutdown.
- **change:** Keep. Lengthening dry spells vs the available historical
  baseline shift thermal-plant water-availability planning beyond historical
  operating envelopes. Mode: `relative_pct`.
- **impact:** **Add, self-derived band 30-90 days, medium confidence**
  (Section 4 protocol — same derivation as Industrial Risk Section 7.3). The
  onset is anchored on an **operational Indian institutional definition**
  (IMD Agricultural Drought) rather than a picked number, which raises
  confidence over a purely-reasoned band.
  - **Mechanism for thermal-asset cooling water:** prolonged dry-spell ->
    reduced precipitation recharge -> reservoir / river drawdown ->
    cooling-water intake curtailment -> forced derating or shutdown. Indian
    plant evidence: WRI 2018 *Parched Power* documents 13 of 20 largest
    plants with at least one water-shortage shutdown 2013-2016, ~14 TWh
    generation lost in 2016 alone. Specific events: Farakka (West Bengal)
    2016 Ganga low-flow shutdown; Raichur (Karnataka) multi-year shortfalls;
    Parli (Maharashtra) 2013 and 2015 drought closures.
  - **Anchors:** identical to Industrial Section 7.3 — IMD Agricultural
    Drought (4 consecutive Drought Weeks ≈ 28 days, rounded to 30) anchors
    the onset; ¾-monsoon (90 day) saturation captures monsoon-failure /
    severe-drought regime.
  - **Cut points:** onset **30 days**, saturation **90 days**.
  - **Confidence: medium.** IMD-anchored onset; reasoned saturation; same as
    Industrial Section 7.3.
  - **Local mediation caveat:** actual thermal-plant cooling-water impact is
    heavily mediated by reservoir storage, alternative water sources,
    cooling technology (once-through vs recirculating tower vs dry/hybrid),
    and on-site water reuse. Phase-1 hazard pressure does not model these.
    A direct-streamflow / reservoir-storage refinement (CWC station discharge
    or modeled hydrology) is deferred (BL-0031).
- **Per-lens weights:** absolute 0.40 / change 0.30 / impact 0.30 — mirrors
  Industrial Section 7.3 (medium-confidence IMD-anchored band).

### 10.2 TXx — Extreme daytime heat (`txx_annual_max`)

- **Lenses:** absolute (yes), change (yes), impact (yes).
- **absolute:** Keep. Same definition as in Health (Section 6.1), Industrial
  (Section 7.4), and Infrastructure (Section 9.3). Districts facing the most
  extreme projected daytime heat relative to peers.
- **change:** Keep. Warming daytime extremes vs the available historical
  baseline matter to thermal assets because plant equipment (transformers,
  switchgear, cooling-tower fill, condensers, lubricating oil systems) is sized
  against historical extremes. Mode: `absolute_delta` (degrees).
- **impact:** Keep. Band **40-45 deg C**, **external provenance, high
  confidence** — same IMD plains heatwave band as Health, Industrial,
  Infrastructure.
  - **Thermal-asset harm pathways through the same dry-bulb band:**
    - **Thermodynamic (Carnot) efficiency loss.** Higher condenser
      temperatures reduce cycle efficiency; typical literature value
      **≈ 0.4-0.6 % per deg C** ambient rise (gross effect; net effect
      depends on cooling technology).
    - **Cooling-tower performance degradation** at high wet-bulb
      temperatures. The dominant Indian thermal cooling technology is
      recirculating cooling towers; their approach temperature widens as
      WBGT rises, lifting condenser temperatures and reducing output.
    - **Air-cooled condenser performance degradation** above ~35-40 deg C
      ambient — relevant for the small but growing Indian dry-cooled fleet.
    - **Equipment derating** (transformers, switchgear, oil-cooled systems);
      coal-handling and station-auxiliary stress; reduced reserve margin
      coinciding with peak demand.
  - **Why the IMD dry-bulb band rather than a WBGT-specific band:** the
    IRT's TXx is a dry-bulb annual maximum, and the IMD heatwave definition
    is the nationally-recognized plains threshold for that regime. A
    cooling-tower-specific refinement would couple TXx with a `twb_*` source
    metric (e.g. `twb_days_ge_28` already produced by the Heat Stress v2
    grid-first compute) — deferred (BL-0030). For Phase-1, the IMD plains
    band is the consistent, defensible band shared across Health, Industrial,
    Infrastructure, and Asset-Thermal bundles.
  - **Zone caveat:** plains/national default (Section 4.9, BL-0020).
- **Per-lens weights:** absolute 0.40 / change 0.25 / impact 0.35 — mirrors
  Health, Industrial, Infrastructure (high-confidence external band).

### 10.3 SPI-3 dry-month frequency (`spi3_count_months_lt_minus1`)

- **Lenses:** absolute (yes), change (yes — methodology change), impact
  (**no** — explicitly dropped).
- **Source metric:** count of months per year with the 3-month Standardized
  Precipitation Index (SPI-3) below −1 (McKee et al. 1993 "moderately dry"
  threshold). A regime-level low-flow proxy for cooling-water availability —
  captures the **cumulative-rainfall-deficit regime** distinct from CDD's
  acute continuous dry-spell signal. SPI-3 reflects accumulated 3-month
  precipitation anomalies, which is the relevant timescale for reservoir /
  river-flow recharge.
- **absolute:** Keep. Districts with the highest projected count of SPI-3
  dry months relative to peers — peer-relative screening of cumulative
  rainfall-deficit regime.
- **change:** Keep. Shift in dry-month count vs baseline period. The chg
  signal captures whether dry-month frequency is *rising vs baseline*, which
  is what physical-risk frameworks (TCFD / NGFS scenario-at-horizon,
  Section 8) call for. **Not structurally redundant** with SPI's own
  baseline-standardization: SPI's gamma fit is fixed on the historical
  reference period; the chg lens measures how often projections cross that
  historical-baseline-conditioned threshold relative to the baseline-period
  count. Mode: `relative_pct`. `relative_pct` is retained for cross-bundle
  consistency with precipitation/count-like regime metrics (for example the
  Investment / Financial R99p regime rule and Hydropower variability helper),
  but it can amplify low-baseline districts; the builder handles zero or
  effectively-zero baselines as `NaN` and renormalizes the rule over the
  available active lenses.
- **IRT source-path status:** admin district/block source masters come from
  the Drought v2 grid-first SPI pipeline with explicit annual
  aggregation `count_months_lt`, `min_months_per_year=9`,
  `period_rollup="period_mean"`, `min_years_per_period_fraction=0.75`,
  `min_baseline_years_per_calendar_month_fraction=0.83`, and
  `min_polygon_cell_weight_fraction=0.50`. The Thermal bundle uses the
  persisted admin district/block source masters and the baseline column they
  expose today; broader baseline-epoch reconciliation is deferred. Hydro
  remains legacy and out of scope.
- **impact:** **No — drop.** Honest no-band call (same rationale as
  Investment/Financial Section 8.3 for R99p). McKee 1993 standardizes the
  per-month SPI threshold (−1 moderately dry, −1.5 severely dry, −2 extremely
  dry) but does **not** categorize annual frequency of months below a
  threshold. No institutional band exists on "count of SPI-3 < −1 months
  per year". Inventing a self-derived band on this composite count of a
  proxy index would be doubly derived (count-of-months *and* proxy use).
  The level + shift signal is the honest scientific reading.
  - **Why this metric is included anyway:** SPI-3 dry-month frequency
    captures the structurally distinct **cumulative regime** signal that CDD
    (acute continuous dry spell) does not — a district could have moderate
    CDD but spend half the year in SPI-3 drought, indicating a chronic
    cooling-water-availability problem. The two metrics together cover both
    acute and cumulative water-stress regimes.
  - **Local mediation caveat:** SPI-3 is a **precipitation proxy** for
    low-flow cooling-water availability. Actual cooling-water availability
    depends on river discharge, reservoir storage, upstream withdrawals, and
    inter-basin transfers — none of which SPI-3 measures directly. A
    streamflow-based refinement (CWC station discharge or modeled hydrology)
    is deferred (BL-0031) and would supersede SPI-3 as the cooling-water
    rule.
- **Per-lens weights:** absolute 0.70 / change 0.30 / impact 0.00 — mirrors
  Investment/Financial Section 8.3 (R99p, no-impact-lens regime metric).

### 10.4 Asset Risk (Thermal Power Plants) — bundle assembly notes

**Rule weights (explicit, sum to 1.0).** Weighting is an evidence-informed
expert elicitation, not a derived constant; the recommended default reflects
the relative thermal-asset climate burden in India and is recorded as a
revisable assumption.

| Cluster | Cluster weight | Rule | Rule weight | Why |
|---|---:|---|---:|---|
| Water stress (acute) | 0.65 | CDD (consecutive dry days) | 0.35 | Medium-confidence IMD-anchored band; direct cooling-water unavailability pathway (WRI 2018 Parched Power: 13 of 20 largest Indian plants had water-shortage shutdowns 2013-2016) |
| Water stress (cumulative regime) | 0.65 | SPI-3 dry-month frequency | 0.30 | No impact band (regime / proxy metric); captures structurally distinct cumulative-deficit signal not covered by CDD; lower confidence so slightly lower weight |
| Heat | 0.35 | TXx (extreme heat) | 0.35 | High-confidence IMD plains heatwave band; Carnot efficiency loss ~0.4-0.6 % per deg C plus cooling-tower / ACC derating during heatwaves; smaller documented direct-loss magnitudes than water-shortage shutdowns |

**How these weights were derived.** Same two-stage elicitation as Health,
Industrial, Investment/Financial, and Infrastructure: **rule_weight reflects
(sector-specific loss-burden evidence) x (band confidence) x (structural
independence)**.

1. **Cluster split first (water stress vs heat).** The bundle's three metrics
   fall into two hazard clusters — water stress (CDD, SPI-3) and heat (TXx).
   - **Water stress 0.65** — by far the dominant documented climate
     vulnerability of Indian thermal-power assets. WRI 2018 *Parched Power*:
     13 of 20 largest plants had water-shortage shutdowns 2013-2016;
     ~14 TWh lost in 2016 alone; 40 % of Indian thermal capacity in
     high-water-stress areas. CEEW repeatedly identifies water availability
     as the #1 climate risk for the Indian thermal fleet. No other
     thermal-asset climate hazard has documented loss magnitudes of this
     scale.
   - **Heat 0.35** — direct Carnot-efficiency loss (~0.4-0.6 % per deg C) is
     real but small per-degree; cooling-tower and air-cooled-condenser
     derating during heatwaves can rise to several percent of output;
     aggregated annual loss magnitudes are smaller and more recoverable
     than the water-shutdown events.
   - Why the water-stress tilt is even heavier than Infrastructure's flood
     tilt (0.65 vs 0.75): the comparison is not apples-to-apples — for
     Infrastructure, flood is the dominant pathway *and* the heat
     contribution is small but distinct; for Thermal, water-stress is the
     dominant pathway *and* the heat contribution is real and continuous via
     Carnot, so heat retains a slightly larger share. The 0.65/0.35 split
     reflects "dominant but not overwhelming".

2. **Within-cluster split by band confidence and structural independence.**
   - Water stress: CDD (0.35) > SPI-3 (0.30). CDD has the medium-confidence
     IMD-anchored band (carrying an impact lens) and the more direct
     continuous-dry-spell pathway. SPI-3 captures the structurally distinct
     cumulative-regime signal but lacks an institutional impact band (proxy
     metric), so it carries slightly less weight. Together 0.65 — matching
     empirical evidence that the documented thermal-shutdown events are
     water-driven.
   - Heat: TXx alone (0.35). Only one heat rule; WBGT-conditioned
     thermal-asset refinement deferred (BL-0030).

3. **Sanity checks.** Weights are positive, sum to 1.0, no single rule
   dominates (max 0.35 across three rules). Water-stress duo together is
   0.65 (matching dominant empirical loss evidence). Lowest-confidence rule
   (SPI-3 with no impact lens) carries the smallest share. Bands rated
   medium confidence carry smaller within-rule impact weights than the
   high-confidence external bands (Section 4.3).

These weights are revisable expert assumptions, not derived constants; any
change is a methodology change to be recorded and tested.

**Per-lens weights within each rule** (absolute / change / impact) are
recorded in `config/proposal_bundles.py` after the CHG-0022 reshape and shown
per metric in Sections 10.1-10.3. The high-confidence external-band rule
(TXx) uses **0.40 / 0.25 / 0.35**; the medium-confidence IMD-anchored CDD
uses **0.40 / 0.30 / 0.30**; the no-impact-lens regime metric SPI-3 uses
**0.70 / 0.30 / 0.00**.

- **Coverage gate:** adopt the standard 0.70 available-rule-weight gate
  (Section 5.3) — matching Health, Industrial, Investment/Financial,
  Infrastructure, Agricultural.
- **Source masters:** all three source metrics resolve from persisted admin
  district/block masters. TXx and CDD route through paths shared with
  Industrial / Infrastructure; SPI-3 routes through the Drought v2
  grid-first admin source masters described in Section 10.3. The SPI change
  lens is active when the source master exposes a historical baseline column
  and degrades with the standard missing-baseline warning/NaN behavior when
  it does not.
- **Public slug stability:** shipped config keeps `cdd_ge_30`, `txx_ge_45`,
  and `spi3_low_flow_proxy_norm`. Recommended renames remain deferred under
  the phantom-slug / data-contract rename track; they are not part of
  CHG-0057/0058.
- **Deferred refinements:** WBGT-conditioned thermal-asset rule using
  `twb_*` source metric to capture cooling-tower wet-bulb performance limit
  (BL-0030 — new); direct streamflow / reservoir-storage rule using CWC
  station discharge or modeled hydrology to supersede SPI-3 as a
  cooling-water proxy (BL-0031 — new); river-water temperature rule for
  once-through cooling at coastal / large-river plants (BL-0032 — new);
  coastal sea-level / cyclone exposure for coastal thermal sites with
  storm-surge inundation of coal stockyards and ash-handling (BL-0033 —
  new). Aqueduct / CGWB groundwater coupling for CDD impact (BL-0022,
  already on backlog, shared with Industrial). JRC global flood-depth
  ingestion (BL-0023, already on backlog, shared with Industrial /
  Infrastructure). All seven are Phase-2 additions, not Phase-1
  corrections.

---

## 11. Asset Risk (Hydropower Plants) — Metric-by-Metric Lens Dossier

Bundle: `Sector-wise - Asset Risk (Hydropower Plants)` | composite slug:
`composite_asset_risk_hydropower` | levels: district, block | scenarios:
`ssp245`, `ssp585`.

Conceptual scope: climate hazards most directly tied to **hydropower
generation assets, their catchment hydrology, and their flow regime** —
multi-day extreme rainfall (spillway demands, dam-safety operations,
catchment sediment loading, cloudburst-driven asset destruction), prolonged
dry spells (reservoir-inflow deficit, generation curtailment), and
extreme-rainfall regime variability (operational predictability). Per
Section 1.2 this is hazard pressure, not full hydro-asset risk: it does not
include plant-specific exposure (Himalayan vs Western Ghats vs Peninsular
siting, glacier-fed vs monsoon-fed), adaptive capacity (storage size,
spillway over-capacity, dam-safety surveillance), or vulnerability (financial
buffer, age of civil works, regulatory mandates).

The Indian hydropower fleet (~52 GW large hydro) is heavily concentrated in
the Himalayan belt (Indus, Ganga, Brahmaputra basins) and the Western Ghats
plus Peninsular rivers (Krishna, Godavari, Cauvery). The dominant documented
climate-loss events for Indian hydropower are **catastrophic extreme-rainfall
/ cloudburst / GLOF events**: Uttarakhand 2013 (Vishnuprayag, Srinagar, and
multiple Alaknanda plants damaged or destroyed; multi-GW affected; >$1B in
hydro asset damage); Chamoli 2021 (Rishiganga 13.2 MW destroyed,
Tapovan-Vishnugad 520 MW damaged); Sikkim Teesta-III 2023 (1200 MW
destroyed by South Lhonak GLOF, ~$1.2B asset loss). Drought-induced
generation deficits (2015-16, 2018-19 monsoon failures, ~10-15 % peninsular
hydro drop) are real but reversible and not an asset-destruction pathway.

### 11.0 Methodology change recorded

These **three methodology changes** are now landed in
`config/proposal_bundles.py` (CHG-0036); the bundle is on the
explicit-weight lens model. They were:

1. **Add an impact lens to Rx5day.** Same self-derived 250-500 mm/5 day
   band already adopted for Industrial Section 7.2 and Infrastructure
   Section 9.2 — low confidence, no new derivation. Hydropower-relevant
   mechanisms (storm spillway demands, dam-safety operations, sediment
   loading, cloudburst-driven asset destruction) all activate in the same
   multi-day saturation regime.
2. **Add an impact lens to CDD.** Same IMD-Agricultural-Drought-anchored
   30-90 day band already adopted for Industrial Section 7.3 and Thermal
   Section 10.1 — medium confidence, derivation reuses an existing
   institutional anchor.
3. **Add a change lens to R95p interannual variability** (currently
   `chg=0`). The chg signal captures whether extreme-rainfall variability is
   *rising vs baseline*, which is documented as a climate-change response of
   the Indian monsoon (IPCC AR6 WG1 Chapter 8). Caveat: small sample sizes
   per period (~20 years) make variance-of-variability noisy — within-rule
   weight on the chg lens is correspondingly modest (0.30), and the rule
   itself carries only 0.20 bundle weight.

The dossier also makes an **honest no-impact-lens call for R95p interannual
variability** — same rationale as Investment/Financial Section 8.3 for R99p
and Thermal Section 10.3 for SPI-3: regime / variability metrics have no
defensible institutional band on year-to-year sigma of an extreme-precip
statistic. Inventing a self-derived band would be triply derived (sigma of
sums above a percentile). Drop the impact lens.

The table summarizes the lens decision per metric; each subsection gives the
reasoning and band provenance. Rule slugs marked "renamed from" reflect the
CHG-0023 rename of phantom slugs (Section 4.7); the dossier presents the
renamed slugs because the bands and labels now reflect the actual scoring
math.

| Rule (metric) | absolute | change | impact (band) | Rationale summary |
|---|:--:|:--:|:--:|---|
| Rx5day — 5-day rainfall (`pr_max_5day_precip`) | yes | yes | yes — self-derived (low conf), 250-500 mm | Spillway demands, sediment loading, cloudburst-driven asset destruction; no external 5-day categorical band exists |
| CDD — consecutive dry days (`pr_consecutive_dry_days_lt1mm`) | yes | yes | yes — self-derived (med conf, IMD-anchored), 30-90 days | Drought-induced low-flow generation deficit; same IMD Agricultural Drought anchor as Industrial / Thermal |
| R95p interannual variability (`r95p_interannual_variability`) | yes | yes | **no** — drop | Operational-predictability regime metric; no defensible institutional band on interannual sigma of an extreme-precip statistic |

### 11.1 Rx5day — Five-day rainfall (`pr_max_5day_precip`)

- **Lenses:** absolute (yes), change (yes), impact (yes — methodology
  change, self-derived band).
- **absolute:** Keep. Districts with the heaviest projected 5-day
  accumulated rainfall relative to peers. For hydropower the multi-day
  signal is the **operationally relevant** scale: catchment-saturation runoff
  drives reservoir inflow surges, spillway operations, and sediment
  mobilization.
- **change:** Keep. Intensifying multi-day accumulations vs the `1990-2010`
  baseline shift hydropower operating envelopes beyond design assumptions.
  Mode: `relative_pct`.
- **impact:** **Add, self-derived band 250-500 mm/5 days, low confidence**
  (Section 4 protocol — same derivation as Industrial Section 7.2 and
  Infrastructure Section 9.2). No external categorical band exists at 5-day
  scale.
  - **Mechanism for hydropower assets:**
    - **Spillway demands and dam-safety operations.** Sustained 5-day
      rainfall events test reservoir flood-routing design and force
      precautionary releases / dam-safety operations.
    - **Catchment sediment loading.** Extreme multi-day rainfall mobilizes
      catchment sediment that accumulates in reservoirs, reducing live
      storage and accelerating turbine erosion.
    - **Asset-destruction events.** The catastrophic Indian hydropower
      losses concentrate in extreme-rainfall / cloudburst / GLOF events
      (Uttarakhand 2013 multi-GW losses; Chamoli 2021 Rishiganga / Tapovan-
      Vishnugad; Sikkim Teesta-III 2023 GLOF — 1200 MW destroyed).
  - **Caveat on Rx5day as a proxy for asset destruction.** Rx5day is a
    precipitation metric — it partially proxies for cloudburst pathways but
    does **not** capture GLOF (glacial-lake-outburst) dynamics or moraine
    stability, which require glacier-mass-balance and lake-volume data.
    The largest methodology gap in this bundle and is flagged as a deferred
    refinement (BL-0034).
  - **Anchors:** identical to Industrial Section 7.2 — five consecutive IMD
    "heavy" days (>= 64.5 mm/day) sum to >= 322 mm; observed Indian
    catastrophic-event 5-day cumulative magnitudes (Kerala 2018 ~350-400 mm
    over 3 days regionally; Chuphal et al. 2025 attributes 2024 India major
    flood events to 3-day return periods >75-200 years).
  - **Cut points:** onset **250 mm**, saturation **500 mm**.
  - **Confidence: low.** No institutional 5-day categorical band exists.
- **Per-lens weights:** absolute 0.45 / change 0.40 / impact 0.15 — mirrors
  Industrial Section 7.2 and Infrastructure Section 9.2 (low-confidence
  self-derived band).

### 11.2 CDD — Consecutive dry days (`pr_consecutive_dry_days_lt1mm`)

- **Lenses:** absolute (yes), change (yes), impact (yes — methodology
  change, IMD-anchored).
- **absolute:** Keep. Districts with the longest projected continuous dry
  spells relative to peers. Drought-induced low-flow pathway: prolonged
  dry spell -> reduced precipitation -> reduced runoff -> reduced reservoir
  inflow -> reduced generation.
- **change:** Keep. Lengthening dry spells vs baseline shift hydropower
  operating envelopes. Mode: `relative_pct`.
- **impact:** **Add, self-derived band 30-90 days, medium confidence**
  (Section 4 protocol — same derivation as Industrial Section 7.3 and
  Thermal Section 10.1). IMD-anchored onset raises confidence above purely
  self-derived bands.
  - **Mechanism for hydropower:** prolonged dry-spell -> reduced runoff ->
    reservoir-inflow deficit -> generation curtailment. Empirical anchors:
    2015-16 and 2018-19 monsoon failures caused ~10-15 % peninsular hydro
    generation drops. **Reversible** (returns when monsoon returns), not an
    asset-destruction pathway, but operationally significant.
  - **Anchors:** identical to Industrial Section 7.3 — IMD Agricultural
    Drought (4 consecutive Drought Weeks ≈ 28 days, rounded to 30) anchors
    onset; ¾-monsoon (90 day) saturation captures monsoon-failure / severe-
    drought regime.
  - **Cut points:** onset **30 days**, saturation **90 days**.
  - **Confidence: medium.** IMD-anchored onset; reasoned saturation.
  - **Local mediation caveat:** hydropower flow regime depends on
    **cumulative seasonal rainfall** (more naturally a 6-month or 12-month
    SPI signal) and **reservoir-storage characteristics**, not just acute
    continuous dry spells. CDD is a partial proxy; a longer-window
    rainfall-regime metric (SPI-6 / SPI-12) would be more directly aligned
    with hydropower seasonal-flow assessment (deferred — BL-0036). Modeled
    streamflow would be the ideal signal (deferred — BL-0037).
- **Per-lens weights:** absolute 0.40 / change 0.30 / impact 0.30 — mirrors
  Industrial Section 7.3 and Thermal Section 10.1 (medium-confidence
  IMD-anchored band).

### 11.3 R95p interannual variability (`r95p_interannual_variability`)

- **Lenses:** absolute (yes), change (yes — methodology change), impact
  (**no** — explicitly dropped).
- **Source metric.** R95p (ETCCDI) is the annual sum of precipitation on
  days exceeding the baseline 95th percentile. The "interannual variability"
  derivative is the year-to-year variation of R95p across the period
  (sigma or CV) — produced via the `helper_master` source mode, not the
  standard grid-first ETCCDI pipeline. **Source-pipeline provenance must be
  confirmed before production adoption** (CHG-0024 follow-up): grid-first
  vs polygon-first computation; sigma vs CV definition.
- **Why this metric matters for hydropower.** Operational predictability:
  a hydropower facility designed against historical extreme-rainfall
  variability faces operational risk when that variability widens —
  spillway operations triggered more frequently, sediment events more
  irregular, reservoir-management heuristics less reliable. IPCC AR6 WG1
  Chapter 8 documents intensifying Indian monsoon extreme-rainfall
  variability under warming.
- **absolute:** Keep. Districts with the most-variable projected
  extreme-rainfall regime relative to peers — peer-relative screening of
  R95p interannual sigma.
- **change:** **Add.** Whether interannual variability is rising vs
  baseline. Caveat: small sample sizes per period (~20 years) make
  variance-of-variability noisy; within-rule weight on chg is correspondingly
  modest (0.30). Mode: `relative_pct`.
- **impact:** **No — drop.** Honest no-band call. Same rationale as
  Investment/Financial Section 8.3 (R99p) and Thermal Section 10.3 (SPI-3):
  no institutional band exists on year-to-year sigma of an extreme-precip
  statistic. Inventing a self-derived band would be triply derived
  (sigma of sums above a percentile) and is dropped.
- **Why this metric is included anyway:** captures the structurally
  distinct **operational-predictability** signal that Rx5day (acute) and
  CDD (drought) do not — a district could have moderate single-event
  extremes but highly variable year-to-year R95p, indicating a less
  predictable operating environment.
- **Per-lens weights:** absolute 0.70 / change 0.30 / impact 0.00 — mirrors
  Investment/Financial Section 8.3 (R99p) and Thermal Section 10.3 (SPI-3)
  (no-impact-lens regime metric).

### 11.4 Asset Risk (Hydropower Plants) — bundle assembly notes

**Rule weights (explicit, sum to 1.0).** Weighting is an evidence-informed
expert elicitation, not a derived constant; the recommended default reflects
the relative hydropower-asset climate burden in India and is recorded as a
revisable assumption.

| Cluster | Cluster weight | Rule | Rule weight | Why |
|---|---:|---|---:|---|
| Flood / storm (acute) | 0.65 | Rx5day (5-day rainfall) | 0.45 | Low-confidence self-derived band but only rule with both impact lens *and* documented catastrophic-loss pathway (Uttarakhand 2013, Chamoli 2021, Sikkim Teesta-III 2023); partial proxy for GLOF (gap deferred to BL-0034) |
| Flood / storm (regime) | 0.65 | R95p interannual variability | 0.20 | No impact band (regime / proxy metric); captures operational-predictability signal distinct from Rx5day acute; lower evidence base; smaller share |
| Drought | 0.35 | CDD (consecutive dry days) | 0.35 | Medium-confidence IMD-anchored band; reversible generation-deficit pathway (2015-16, 2018-19 monsoon failures ~10-15 % peninsular hydro drop); not asset-destruction |

**How these weights were derived.** Same two-stage elicitation as Health,
Industrial, Investment/Financial, Infrastructure, and Thermal:
**rule_weight reflects (sector-specific loss-burden evidence) x (band
confidence) x (structural independence)**.

1. **Cluster split first (flood/storm vs drought).**
   - **Flood/storm 0.65** — the catastrophic loss-magnitude evidence for
     Indian hydropower is concentrated in extreme-rainfall / cloudburst /
     GLOF events. Uttarakhand 2013 (~$1B+ hydro asset damage, multi-GW
     destroyed or damaged); Chamoli 2021 (~$200M direct hydro losses);
     Sikkim Teesta-III 2023 (~$1.2B asset destruction). For an
     **asset-risk** framing (not generation-deficit framing), the
     flood/storm cluster dominates.
   - **Drought 0.35** — monsoon-failure-induced flow deficits (2015-16,
     2018-19) caused ~10-15 % peninsular hydro-generation drops. Real but
     **reversible** (generation returns when monsoon returns); not an
     asset-destruction pathway. Justifies a non-trivial-but-smaller cluster
     weight than flood/storm.
   - **Why flood/storm tilt is modest (0.65) and not higher.** Rx5day's
     impact band is low-confidence and only partially proxies for the actual
     asset-destruction pathway (Rx5day is a rainfall metric; GLOF and
     moraine-stability are not captured). R95p interannual variability
     captures operational predictability, not asset destruction. Until
     GLOF / glacier-mass-balance enters the bundle (BL-0034), the
     flood/storm cluster is necessarily understating the Himalayan-asset
     risk and should not be pushed to 0.80+.

2. **Within-cluster split by band confidence and structural independence.**
   - Flood/storm: Rx5day (0.45) > R95p variability (0.20). Rx5day is the
     primary acute-event proxy with a real (if low-confidence) impact band;
     R95p interannual variability is a regime / operational-predictability
     metric without an impact band, lower evidence base, smaller share.
     Together 0.65.
   - Drought: CDD alone (0.35). Only one drought rule; medium-confidence
     IMD-anchored band. Longer-window seasonal-rainfall refinement deferred
     (BL-0036); modeled-streamflow refinement deferred (BL-0037).

3. **Sanity checks.** Positive, sum to 1.0. Rx5day at 0.45 carries the
   largest weight — justified by it being the only rule with both an impact
   band *and* a documented catastrophic-loss pathway in this bundle.
   Low-evidence regime metric (R95p variability) carries the smallest share
   (0.20). Bundle weight x within-rule impact weight: Rx5day 0.45 x 0.15
   = 0.0675; CDD 0.35 x 0.30 = 0.105; R95p-iv 0.20 x 0 = 0. The
   medium-confidence CDD band carries the largest **impact-component
   contribution** — consistent with the protocol of letting higher-confidence
   bands carry more impact weight (Section 4.3).

These weights are revisable expert assumptions, not derived constants; any
change is a methodology change to be recorded and tested.

**Per-lens weights within each rule** (absolute / change / impact) are
recorded in `config/proposal_bundles.py` after the CHG-0024 reshape and
shown per metric in Sections 11.1-11.3. The medium-confidence IMD-anchored
CDD uses **0.40 / 0.30 / 0.30**; the low-confidence self-derived Rx5day
uses **0.45 / 0.40 / 0.15** (deliberately small impact weight so the weak
band cannot dominate); the no-impact-lens regime metric R95p variability
uses **0.70 / 0.30 / 0.00**.

- **Coverage gate:** adopt the standard 0.70 available-rule-weight gate
  (Section 5.3) — matching Health, Industrial, Investment/Financial,
  Infrastructure, Thermal, Agricultural.
- **Source masters:** Rx5day and CDD must resolve to grid-first
  district/block masters (paths shared with Industrial / Infrastructure /
  Thermal — TXx and CDD grid-first verified for those bundles; CDD for
  Hydropower path is the same source metric, not separately verified).
  **R95p interannual variability uses `helper_master` source mode.** As
  landed in CHG-0036 the helper builder emits the future variability columns
  plus one hyphenated historical baseline column
  (`r95p_interannual_variability__historical__{token}__mean`) whose epoch is
  mirrored from the Rx5day/CDD source masters (resolved to a shared token, one
  of 1995-2014 or 1985-2014), so the change lens is operational rather than
  cosmetic. The helper computation provenance (grid-first vs polygon-first;
  sigma vs CV definition) still must be confirmed before production adoption
  (CHG-0024).
- **Phantom-slug renames (CHG-0023):** the dossier presents the renamed
  slugs. `rx5day_ge_500` is **renamed to** `rx5day_accumulated_pressure` —
  same canonical slug Industrial Section 7.2 and Infrastructure Section 9.2
  adopt; one canonical slug per source-metric + band combination across
  bundles. `cdd_ge_60` is **renamed to** `cdd_water_stress_pressure` — same
  canonical slug as Industrial / Thermal; "60" sits inside the band 30-90
  days but in the middle, not at an edge. `r95p_interannual_variability_norm`
  is **renamed to** `r95p_interannual_variability` (cosmetic cleanup —
  the `_norm` suffix is residual). Migration is a data-contract change
  tracked separately under CHG-0024.
- **Deferred refinements:** **GLOF / glacier-mass-balance hazard rule for
  Himalayan hydropower** (BL-0034 — new; the largest methodology gap in
  this bundle); catchment sediment-yield rule (BL-0035 — new); SPI-6 /
  SPI-12 longer-window rainfall regime metric for seasonal-flow
  assessment (BL-0036 — new); modeled streamflow / catchment-hydrology
  rule for direct flow-regime assessment (BL-0037 — new; concept shared
  with Thermal BL-0031); JRC global flood-depth ingestion (BL-0023,
  already on backlog, shared with Industrial / Infrastructure). All five
  are Phase-2 additions, not Phase-1 corrections.

---

## 12. Agricultural Risk — Metric-by-Metric Lens Dossier

Bundle: `Sector-wise - Agricultural Risk` | composite slug:
`composite_agricultural_risk` | levels: district, block | scenarios: `ssp245`,
`ssp585`.

Conceptual scope: climate hazards most directly tied to **crop production
risk in India** — peak heat damaging reproductive-stage crop physiology,
accumulated damaging-heat exposure, persistent heatwaves, drought (frequency
and longest spell), heavy multi-day rainfall (kharif waterlogging / standing
crop flood damage), and cold-night chilling stress on tropical and
sub-tropical crops. Per Section 1.2 this is hazard pressure, not full
agricultural risk: it does not include sector-specific exposure (which crops
sit where, by phenological calendar), adaptive capacity (irrigation
infrastructure, varietal heat tolerance, crop insurance), or vulnerability
(smallholder vs commercial, dryland vs irrigated, kharif vs rabi).

This section closes the methodology gap left when the earlier
single-lens-absolute scoring retired the TXx 40-45 deg C impact band without a
replacement (`india_resilience_tool/config/proposal_bundles.py` previously
carried that retirement notice). The current dossier reinstates the impact
lens with a defensible agronomic band per Section 4 and brings the bundle
into structural parity with Health Risk (Section 6) and Industrial Risk
(Section 7).

| Rule (metric) | absolute | change | impact (band) | Rationale summary |
|---|:--:|:--:|:--:|---|
| TXx — peak crop heat (`txx_annual_max`) | yes | yes | yes — self-derived (med conf), 35-45 deg C | Rice/wheat reproductive-stage heat sterility onset (~35 deg C); IMD plains heatwave saturation (45 deg C) |
| TXge35 — damaging heat days (`txge35_extreme_heat_days`) | yes | yes | yes — self-derived (low conf), 15-60 days | Anthesis-window exposure onset; ~2-month catastrophic-season saturation |
| WSDI — persistent heat (`wsdi_warm_spell_days`) | yes | yes | yes — self-derived (low conf), 6-18 days | Reuses Health Section 6.2 band; warm-spell persistence shortens grain-filling and stresses livestock-feed and pasture systems |
| SPI3 drought episodes (`spi3_count_events_lt_minus1`) | yes | yes | yes — self-derived (low conf), 3-12 events | Above-natural-frequency onset; near-continuous-drought saturation |
| SPI3 longest drought spell (`spi3_max_spell_lt_minus1`) | yes | yes | yes — self-derived (low conf), 3-12 months | Single SPI3-window onset; year-long drought saturation |
| Rx5day — 5-day heavy rainfall (`pr_max_5day_precip`) | yes | yes | yes — self-derived (low conf), 250-500 mm | Reuses Industrial Section 7.2 band; kharif waterlogging / standing-crop flood damage |
| TNle10 — cold nights (`tnle10_cold_nights`) | yes | yes | yes — self-derived (low conf, **peninsular default**), 10-30 days | Tropical-crop chilling-injury onset; severe-cold-stress saturation; zone caveat per Section 4.9 / BL-0020 |

### 12.1 TXx — Peak crop heat (`txx_annual_max`)

- **Lenses:** absolute (yes), change (yes), impact (yes — self-derived).
- **absolute:** Keep. Districts facing the most extreme projected daytime
  heat relative to peers (TXx, ETCCDI). For crops, peak heat is the dominant
  single-event pressure on reproductive-stage physiology — rice anthesis
  spikelet sterility, wheat grain-filling truncation, mango/citrus flower
  abscission.
- **change:** Keep. Warming peak-heat extremes vs the `1990-2010` baseline
  matter to agriculture because cropping calendars, varietal heat tolerance,
  and irrigation scheduling are calibrated against historical extremes. Mode:
  `absolute_delta` (degrees), matching Industrial Section 7.4 and Health
  Section 6.1.
- **impact:** Keep, **self-derived band 35-45 deg C, medium confidence**
  (Section 4 protocol). The earlier 40-45 deg C band (an external IMD
  heatwave band) was retired because **the human-heatwave threshold of
  40 deg C is too high for agronomic onset**: rice and wheat reproductive-
  stage damage begins well below the IMD plains-heatwave threshold.
  - **Mechanism:** at temperatures above ~35 deg C during anthesis, rice
    spikelet fertility drops sharply (Jagadish et al.); wheat suffers
    accelerated senescence and grain-filling truncation above ~32-35 deg C
    (Wahid et al.); horticultural crops (mango, banana, tomato) show pollen
    sterility, flower abscission, and fruit cracking in the same range.
    Above 45 deg C, plains agriculture is in catastrophic-failure regime for
    standing kharif and late-rabi crops alike.
  - **Anchors:**
    - Crop physiology literature (Wheeler et al.; Vermeulen et al.; Wahid
      et al. *Plant Physiology* review on heat tolerance; Jagadish et al. on
      rice anthesis) places reproductive-stage heat sterility onset near
      **32-35 deg C** for the dominant Indian staple crops (rice, wheat).
      35 deg C is the conservative upper edge of this onset band.
    - IMD plains heatwave definition (declared >= 40 deg C, severe
      >= 45 deg C) anchors the **saturation** point: at 45 deg C the IMD
      institutional severe-heatwave regime coincides with documented
      catastrophic crop failure regimes.
  - **Cut points:** onset **35 deg C** (agronomic reproductive-stage
    sterility onset, conservative crop-physiology anchor); saturation
    **45 deg C** (IMD severe-heatwave / documented crop-failure regime).
  - **Confidence: medium.** Onset is derived from agronomic literature
    rather than an IMD/ICAR categorical standard, so confidence is below the
    high-confidence external IMD plains-heatwave band that Industrial
    Section 7.4 and Health Section 6.1 use. The saturation is
    institutionally anchored.
  - **Why not reinstate the retired 40-45 deg C band:** the human-heatwave
    onset (40 deg C) understates crop-onset damage by ~5 deg C; the
    Phase-1 dossier deliberately accepts a slightly lower-confidence band
    in exchange for a defensible agronomic onset.
  - **Zone caveat:** plains/national default (Section 4.9, BL-0020). A
    refinement using agro-climatic zones (ICAR/Planning Commission) or
    agro-ecological zones (NBSS&LUP) is deferred.
- **Per-lens weights:** absolute 0.40 / change 0.30 / impact 0.30. The
  medium-confidence self-derived band justifies a within-rule impact weight
  slightly below the 0.35 reserved for high-confidence external bands
  (Industrial Section 7.4, Health Section 6.1) but above the 0.15 reserved
  for low-confidence bands (Industrial Section 7.2 Rx5day).

### 12.2 TXge35 — Damaging heat days (`txge35_extreme_heat_days`)

- **Lenses:** absolute (yes), change (yes), impact (yes — self-derived).
- **absolute:** Keep. Annual count of days with TX >= 35 deg C — the
  reproductive-stage agronomic damage threshold from Section 12.1.
  Districts with the most damaging-heat days relative to peers.
- **change:** Keep. More damaging-heat days vs baseline shift cropping
  calendars and stress varietal heat tolerance. Mode: `relative_pct`,
  matching Industrial Section 7.3 CDD (count metrics on days).
- **impact:** Keep, **self-derived band 15-60 days, low confidence**
  (Section 4 protocol).
  - **Mechanism:** even a few damaging days during the reproductive window
    (rice anthesis ~7 days; wheat anthesis 7-10 days) materially reduce
    yield. Two months of damaging-heat days accumulates across both kharif
    flowering and rabi grain-filling sensitive windows — a multi-season
    catastrophic regime.
  - **Anchors:**
    - The slug threshold (>= 35 deg C) already aligns with the rice/wheat
      reproductive-stage sterility onset from Section 12.1.
    - **Onset 15 days** — captures a complete reproductive-stage exposure
      window (one of rice anthesis ~7-10 days plus a typical wheat anthesis
      ~7-10 days) of damaging heat per year.
    - **Saturation 60 days** — approximately two months of damaging-heat
      days; spans both kharif and rabi sensitive windows and indicates a
      season-dominant heat regime.
  - **Cut points:** onset **15 days**; saturation **60 days**.
  - **Confidence: low.** Count-of-days thresholds for crop damage are not
    externally codified — ICAR / IMD agromet publishes phenological-stage
    advisories rather than annual-count categorical bands. The cut points
    are constructed from reproductive-window physiology, not borrowed.
- **Per-lens weights:** absolute 0.45 / change 0.40 / impact 0.15 — the
  low-confidence self-derived band carries the smaller within-rule impact
  weight reserved for low-confidence bands (Section 4.3). The persistence
  signal is carried mainly by absolute and change.

### 12.3 WSDI — Persistent heat (`wsdi_warm_spell_days`)

- **Lenses:** absolute (yes), change (yes), impact (yes — self-derived).
- **absolute:** Keep. WSDI (ETCCDI: count of days in spells of >= 6
  consecutive days with TX > 90th percentile of baseline) measures persistent
  warm-spell exposure. Districts with the most persistent-heat days relative
  to peers.
- **change:** Keep. Lengthening warm spells vs baseline shorten grain-filling
  windows, stress livestock-feed and pasture systems, and degrade soil
  moisture. Mode: `relative_pct`.
- **impact:** Keep, **self-derived band 6-18 days, low confidence**, **reuses
  Health Section 6.2**.
  - **Mechanism:** sustained warm spells truncate grain-filling, accelerate
    crop senescence, increase evapotranspiration demand on irrigation
    systems, and degrade fodder quality. The hazard *value* threshold for
    persistent warm-spell exposure is set by the WSDI metric definition
    itself, not by the receptor; the consequence differs (yield truncation /
    fodder degradation vs cardiovascular mortality).
  - **Anchors:** the WSDI metric's own minimum qualifying spell (>= 6
    consecutive days) is the natural **onset** anchor; multiple WSDI spells
    accumulating to ~18 days/year indicates a regime where persistent heat
    dominates the growing season. Same anchors as Health Section 6.2.
  - **Cut points:** onset **6 days** (WSDI minimum qualifying spell);
    saturation **18 days** (multi-spell / season-dominant warm-spell regime).
  - **Confidence: low.** WSDI is a percentile-based annual tally — the
    physiological harm pathway is real but the count thresholds are not
    externally codified for agriculture.
- **Per-lens weights:** absolute 0.45 / change 0.40 / impact 0.15 — mirrors
  Health Section 6.2 (low-confidence self-derived band).

### 12.4 SPI3 drought episodes (`spi3_count_events_lt_minus1`)

- **Lenses:** absolute (yes), change (yes), impact (yes — self-derived).
- **absolute:** Keep. Count of drought episodes with SPI-3 < -1 (McKee
  moderate drought) over the period. Districts with the most frequent
  moderate-drought episodes relative to peers.
- **change:** Keep. Increasing drought-episode frequency vs baseline
  indicates a shift in the agricultural drought regime. Mode: `relative_pct`.
- **impact:** Keep, **self-derived band 3-12 events, low confidence**
  (Section 4 protocol).
  - **Mechanism:** repeated moderate-drought episodes deplete soil moisture
    reserves, force borewell deepening, drive distress sales of livestock,
    and erode smallholder coping capacity. Frequency of drought episodes is
    a distinct burden from the longest single drought (Section 12.5).
  - **Anchors:**
    - The natural per-event baseline frequency of SPI < -1 is ~16% by
      standard-normal definition; over a 20-year period that yields roughly
      3-4 events as the **natural-frequency baseline**.
    - **Onset 3 events** — approximately the natural baseline, above which
      episode frequency departs from normal.
    - **Saturation 12 events** — near-continuous-drought regime indicating
      systemic agricultural-drought rather than episodic.
  - **Cut points:** onset **3 events**; saturation **12 events**.
  - **Confidence: low.** Drought-episode count categorical bands are not
    externally codified; the cut points are derived from SPI statistics
    rather than an institutional standard.
- **Per-lens weights:** absolute 0.45 / change 0.40 / impact 0.15.

### 12.5 SPI3 longest drought spell (`spi3_max_spell_lt_minus1`)

- **Lenses:** absolute (yes), change (yes), impact (yes — self-derived).
- **absolute:** Keep. Maximum consecutive months of SPI-3 < -1 over the
  period. Districts with the longest single moderate-drought spell relative
  to peers.
- **change:** Keep. Lengthening longest-spell vs baseline indicates a shift
  toward sustained agricultural drought. Mode: `relative_pct`.
- **impact:** Keep, **self-derived band 3-12 months, low confidence**.
  - **Mechanism:** sustained moderate-drought (vs episodic) collapses
    reservoir refill, exhausts groundwater, and forces multi-season cropping
    pattern changes — distinct from the episode-frequency burden in
    Section 12.4.
  - **Anchors:**
    - **Onset 3 months** — one SPI3 window's worth of drought
      (SPI-3 is a 3-month index), the minimum institutionally-recognizable
      moderate-drought spell.
    - **Saturation 12 months** — year-long sustained moderate drought,
      indicating multi-season crop failure regime (one full kharif-rabi
      cycle under drought stress).
  - **Cut points:** onset **3 months**; saturation **12 months**.
  - **Confidence: low.** Longest-spell categorical bands are not externally
    codified; cut points are derived from SPI window length and
    seasonal-cycle reasoning.
- **Per-lens weights:** absolute 0.45 / change 0.40 / impact 0.15.

### 12.6 Rx5day — 5-day heavy rainfall (`pr_max_5day_precip`)

- **Lenses:** absolute (yes), change (yes), impact (yes — self-derived).
- **absolute:** Keep. Heaviest 5-day accumulated rainfall over the period.
  Captures kharif waterlogging, standing-crop flood damage, and basin-scale
  agricultural flood pressure that a single-day extreme can miss.
- **change:** Keep. Intensifying multi-day accumulations vs baseline. Mode:
  `relative_pct`.
- **impact:** Keep, **self-derived band 250-500 mm/5 days, low confidence**,
  **reuses Industrial Section 7.2**.
  - **Mechanism for agriculture:** sustained multi-day rainfall produces
    standing-crop submergence (rice tolerance to complete submergence is
    typically <= 1-2 weeks for non-Sub1 varieties), kharif waterlogging,
    delayed harvest with quality loss, and topsoil erosion. The hazard
    *value* threshold (5-day mm cumulative) is set by drainage and basin
    hydrology, not by the receptor; the consequence differs (crop
    submergence and quality loss vs factory inundation in Industrial
    Section 7.2).
  - **Anchors:** same as Industrial Section 7.2 — five consecutive IMD
    "heavy" days (>= 64.5 mm/day each) sum to >= 322 mm; observed Indian
    catastrophic-event 5-day cumulatives (Kerala 2018 ~350-400 mm in 3
    days regionally; Mumbai 2005 cumulative ~944 mm) anchor the
    saturation regime.
  - **Cut points:** onset **250 mm** (plausible kharif-waterlogging /
    drainage-failure regime); saturation **500 mm** (regional flood-event
    / submerged-crop-loss regime).
  - **Confidence: low.** No external categorical 5-day-rainfall band exists
    in IMD / CWC / ICAR; the band is borrowed from Industrial Section 7.2
    with the same low-confidence rating.
- **Per-lens weights:** absolute 0.45 / change 0.40 / impact 0.15 — mirrors
  Industrial Section 7.2 (low-confidence self-derived band).

### 12.7 TNle10 — Cold nights (`tnle10_cold_nights`)

- **Lenses:** absolute (yes), change (yes), impact (yes — self-derived).
- **absolute:** Keep. Annual count of cold nights with TN <= 10 deg C.
  Districts with the most cold-night exposure relative to peers.
- **change:** Keep. Mode: `relative_pct`.
- **impact:** Keep, **self-derived band 10-30 days, low confidence**,
  **peninsular default**.
  - **Mechanism:** tropical and sub-tropical crops (rice cold-spell at
    seedling stage, sugarcane, banana, mango, cotton, vegetables) suffer
    chilling injury at TN below ~10-15 deg C — defoliation, fruit drop,
    flowering arrest, slow growth recovery. The hazard direction is
    cold-side: more cold-night days = more chilling-stress exposure.
  - **Anchors:**
    - Crop chilling-injury literature places onset of chilling stress for
      tropical crops at sustained TN below ~10-12 deg C; ~10 nights/year
      of TN <= 10 deg C is the threshold at which peninsular/coastal
      production zones enter materially-stressed regimes for sensitive
      horticulture (mango, banana, papaya).
    - ~30 nights/year (~1 month) of cold-night exposure aligns with
      regimes that have driven historical reports of severe cold-wave
      losses in southern and eastern India.
  - **Cut points:** onset **10 days**; saturation **30 days**.
  - **Confidence: low.** Crop chilling-injury count bands are not
    externally codified for India.
  - **Zone caveat (important):** the band assumes the *peninsular/southern*
    crop mix. In the northern wheat belt, cold nights during the rabi
    season are **beneficial** (vernalization). A general all-India
    application of this band under-represents the wheat-belt benefit and
    over-applies the chilling-injury cost. **Plains/peninsular default**
    per Section 4.9 / BL-0020; per-zone refinement deferred until an
    operational agro-climatic-zone label is wired in.
- **Per-lens weights:** absolute 0.45 / change 0.40 / impact 0.15 — the
  zone-applicability ceiling reinforces the small within-rule impact weight
  reserved for low-confidence bands.

### 12.8 Agricultural Risk — bundle assembly notes

**Rule weights (explicit, sum to 1.0).** Weighting is an evidence-informed
expert elicitation, not a derived constant; the recommended default reflects
the relative agricultural-sector climate burden in India and is recorded as
a revisable assumption.

| Cluster | Cluster weight | Rule | Rule weight | Why |
|---|---:|---|---:|---|
| Heat | 0.35 | TXx (peak crop heat) | 0.15 | Strongest agronomic-physiology anchor (reproductive-stage sterility); medium-confidence self-derived band |
| Heat | 0.35 | TXge35 (damaging heat days) | 0.10 | Accumulated reproductive-window exposure; low-confidence band |
| Heat | 0.35 | WSDI (persistent heat) | 0.10 | Grain-filling truncation / fodder degradation; low-confidence band; partly correlated with TXx |
| Drought | 0.30 | SPI3 episodes | 0.15 | Frequency burden; low-confidence band |
| Drought | 0.30 | SPI3 longest spell | 0.15 | Intensity burden; low-confidence band; correlated with episodes |
| Rainfall | 0.20 | Rx5day | 0.20 | Kharif waterlogging / standing-crop flood damage; low-confidence band |
| Cold | 0.15 | TNle10 | 0.15 | Peninsular default; zone caveat per BL-0020 limits cluster ceiling |

**How these weights were derived.** Two-stage elicitation, same recipe as
Health Section 6.6 and Industrial Section 7.5, so reasoning is auditable:
**rule_weight reflects (continuous sector burden) x (band confidence) x
(structural independence)**.

1. **Cluster split first (heat vs drought vs rainfall vs cold).** The
   bundle's seven metrics fall into four hazard clusters:
   - **Heat 0.35** — three rules covering peak (TXx), accumulated damaging
     days (TXge35), and persistence (WSDI). Heat is the dominant
     reproductive-stage pressure across the dominant Indian staples
     (rice, wheat) and horticulture; the literature anchoring is the
     strongest in the bundle.
   - **Drought 0.30** — two SPI3 rules covering frequency and intensity of
     moderate drought. SPI is the most widely-used institutional drought
     indicator (NDMA Manual; IMD agromet). The cluster is large because
     drought is a season-defining agricultural pressure; equal to rather
     than larger than the heat cluster only because the two SPI rules are
     partly correlated, capping the cluster contribution.
   - **Rainfall 0.20** — single Rx5day rule. Standing-crop flood / kharif
     waterlogging is materially distinct from drought and heat but anchored
     by a low-confidence band, so cluster weight is set below heat and
     drought.
   - **Cold 0.15** — single TNle10 rule. Zone-dependent applicability
     (peninsular default; wheat-belt benefit) caps cluster ceiling per
     BL-0020.
   The 0.35 / 0.30 / 0.20 / 0.15 split is the single most consequential
   judgment and the main lever for revision.

2. **Within-cluster split by evidence strength and inter-metric correlation.**
   - Heat: TXx 0.15 > TXge35 0.10 = WSDI 0.10. TXx anchors on the
     strongest agronomic-physiology onset (medium-confidence band, the
     only one in this bundle); TXge35 and WSDI carry low-confidence
     self-derived bands and are partly correlated with TXx, so they are
     weighted below it to avoid over-counting the daytime-heat signal.
   - Drought: SPI3 longest spell 0.15 = SPI3 episodes 0.15. Equal-weighted;
     one captures frequency, the other intensity; correlated but
     structurally distinct.
   - Rainfall: Rx5day 0.20. Single rule; cluster weight = rule weight.
   - Cold: TNle10 0.15. Single rule; cluster weight = rule weight.

3. **Sanity checks.** Weights are positive, sum to 1.0, no single rule
   dominates (max 0.20 for Rx5day). Low-confidence rules (TXge35, WSDI,
   SPI3 x 2, Rx5day, TNle10) all carry the smallest within-rule impact
   weight (0.15). The single medium-confidence rule (TXx) carries the
   moderate within-rule impact weight (0.30).

These weights are revisable expert assumptions, not derived constants; any
change is a methodology change to be recorded and tested.

**Per-lens weights within each rule** (absolute / change / impact) are
recorded in `config/proposal_bundles.py` and shown per metric in
Sections 12.1-12.7. The medium-confidence self-derived band (TXx) uses
**0.40 / 0.30 / 0.30**; the low-confidence self-derived bands (TXge35, WSDI,
SPI3 x 2, Rx5day, TNle10) all use **0.45 / 0.40 / 0.15**, deliberately giving
the weak impact bands a small share so they cannot dominate the rule
(Section 4.3).

- **Coverage gate:** the standard **0.70 available-rule-weight gate**
  (Section 5.3) — matching Health, Industrial, and the prior Agricultural
  configuration.
- **Source masters:** the seven source metrics resolve to grid-first or
  registry-driven district/block masters consistent with the
  spatial-aggregation recommendation in `docs/bundle_calculation_audit.md`.
  TXx routes through `india_resilience_tool/compute/heat_risk_gridfirst.py`;
  Rx5day routes through `india_resilience_tool/compute/extreme_rainfall_gridfirst.py`;
  SPI3 metrics route through the SPI adapter (`compute/spi_adapter.py`);
  TXge35, WSDI, and TNle10 route through the standard climate-indices
  pipeline.
- **Per-lens persistence:** the builder already emits `__abs_score`,
  `__chg_score`, `__imp_score` columns for active lenses
  (`india_resilience_tool/compute/proposal_bundles.py:554` and `:980`);
  after this migration, every Agricultural rule emits all three lens
  columns.
- **Deferred refinements (Phase-2):** zone-specific impact bands
  (BL-0020) — most consequential for TNle10 (peninsular vs wheat-belt) and
  meaningful for TXx (plains vs coastal); WBGT-conditioned heat rule
  (BL-0021); Aqueduct / CGWB groundwater coupling for drought impact
  (BL-0022); the retired TXx 40-45 deg C external-band alternative
  recorded above for audit.

---

## 13. Life & Livelihood Loss Risk — Metric-by-Metric Lens Dossier

The Life & Livelihood Loss Risk bundle scores acute climate-hazard pressure
on **mortality** (loss of life) and **outdoor / informal-sector livelihood
disruption** (loss of livelihood). It is distinct from sectoral asset risks
(Industrial, Infrastructure, Thermal Power, Hydropower) in that the impacted
"asset" is the population itself — physiological tolerance limits, exposure
during outdoor work, and the absence of an engineered buffer against the
hazard.

The bundle uses 4 rules — Rx1day (acute flood mortality), Rx5day (prolonged
inundation displacement), CDD (drought-driven livelihood loss), WSDI (heat
mortality and outdoor-worker exposure). All four are lens-decomposed
(absolute + change + impact). Weight mode `explicit_normalized`; coverage
gate `min_available_rule_weight_fraction=0.70` (same as Health, Industrial,
Investment, Infrastructure, Agricultural).

Per Section 4.4 the impact-band confidence ladder maps to impact-lens
weights: HIGH external → 0.35; MEDIUM external/self-derived → 0.30;
LOW self-derived → 0.15. Section 13.1 (Rx1day) is HIGH; Sections 13.3 (CDD)
and 13.4 (WSDI) are MEDIUM with explicit IMD / ICAR / mortality-literature
anchoring; Section 13.2 (Rx5day) remains LOW (reused Industrial 7.2 band
without an independent India-context standard).

### 13.1 Rx1day — One-day rainfall (`pr_max_1day_precip`)

**Pathway.** Flash floods and short-duration extreme rainfall kill people
directly through drowning, structural collapse of informal housing, and
loss of life among outdoor workers caught without shelter; they also wipe
out daily-wage livelihoods through asset destruction (handcarts, livestock,
small shops, kuccha homes). Acute, hours-to-day onset.

**Absolute lens (weight 0.40).** Higher rainfall intensity = higher
mortality pressure. Standard min-max scaling within the scenario-period
cohort.

**Change lens (weight 0.25, `relative_pct`).** Forced upward shift in the
1-day extreme distribution under warming raises the floor of expected
flash-flood mortality even where the current peak is moderate.

**Impact lens (weight 0.35; band 115.6 - 204.5 mm/day; External, HIGH).**
Reuses the IMD daily-rainfall categorical anchors: 115.6 mm/day = onset of
"heavy" rainfall; 204.5 mm/day = "extremely heavy" rainfall. These cut
points are India's authoritative external standard for short-duration
hazard categorization and are shared with Health Section 6.4, Industrial
Section 7.1, Investment Section 8.1, and Infrastructure Section 9.1.

**Per-lens weight summary.** 0.40 / 0.25 / 0.35 (HIGH external imp ceiling
per Section 4.4).

### 13.2 Rx5day — Five-day rainfall (`pr_max_5day_precip`)

**Pathway.** Multi-day extreme rainfall drives sustained urban and rural
inundation, displacement, and prolonged exposure of displaced populations to
disease and exposure mortality. It also strands daily-wage workers from
livelihood activity for days at a time (street vendors, construction
labour, informal transport).

**Absolute lens (weight 0.40).** Higher 5-day total = higher displacement /
disease-environment pressure.

**Change lens (weight 0.30, `relative_pct`).** Forced upward shift in
multi-day extremes raises baseline displacement pressure across the
scenario-period horizon.

**Impact lens (weight 0.30; band 250 - 500 mm/5 days; Self-derived, LOW).**
Onset 250 mm/5d carried over from Industrial Section 7.2 — five IMD
"heavy-rain" days as a conservative lower envelope. Saturation 500 mm/5d =
regional flood-catastrophe regime (Chuphal et al. 2025 multi-day extreme
return periods >75-200 years for the 2024 India flood-of-record regions).
Shared with Industrial Section 7.2, Investment Section 8.2, Infrastructure
Section 9.2.

**Per-lens weight summary.** 0.40 / 0.30 / 0.30 (LOW band would normally
hold imp at 0.15 per Section 4.4; we apply the cross-bundle parity carve-out
because the same band is anchoring 4 sectoral bundles and is the most-tested
self-derived band in this dossier).

### 13.3 CDD — Consecutive dry days (`pr_consecutive_dry_days_lt1mm`)

**Pathway.** Sustained dryness during the monsoon and post-monsoon windows
destroys rainfed-kharif livelihoods (smallholder cultivators in peninsular
and central India), drives water-fetching burden onto women, and forces
distress migration. Slower-onset than flood mortality, but with very long
recovery times for affected households.

**Absolute lens (weight 0.40).** Longer dry spell = higher livelihood-loss
pressure.

**Change lens (weight 0.30, `relative_pct`).** Forced lengthening of the
longest dry spell directly extends the at-risk window for rainfed
livelihoods.

**Impact lens (weight 0.30; band 60 - 120 days; Self-derived, MEDIUM).**

- **Onset = 60 days.** Convergent anchoring:
  - IMD agro-met weekly advisories declare a "prolonged dry spell" at
    ≥ 4 consecutive Drought Weeks (~28 days). A 60-day CDD necessarily
    contains and exceeds this trigger.
  - ICAR-CRIDA contingency plans identify ~60 days continuous dryness
    during the monsoon window as the threshold beyond which rainfed kharif
    systems (pulses, coarse cereals, cotton in peninsular India) hit
    unrecoverable water-stress losses.
- **Saturation = 120 days.** ~4 months continuous dryness = complete
  kharif-to-early-rabi system failure, aligning with NDMA's *Manual for
  Drought Management* (2016) seasonal-failure framing.
- **Confidence = MEDIUM.** Both endpoints are derived from convergent
  IMD + ICAR + NDMA evidence rather than a single agency categorical
  standard. Self-derived label retained because no published Indian
  document names "60-120 days CDD" verbatim, but the anchoring is much
  stronger than the typical LOW-confidence self-derived case (cf.
  Agricultural Section 12.4 SPI3 band).

**Per-lens weight summary.** 0.40 / 0.30 / 0.30 (MEDIUM imp 0.30 per
Section 4.4).

### 13.4 WSDI — Warm-spell duration (`wsdi_warm_spell_days`)

**Pathway.** Persistent heat causes direct heat-stroke mortality
(disproportionately affecting outdoor workers, the elderly, and informal
housing residents without cooling) and erodes outdoor-worker livelihoods
(construction, agriculture, brick-kiln, street vending) through forced
work-hour loss. This is the pathway most directly aligned with this
bundle's "loss of life and livelihood" framing.

**Absolute lens (weight 0.40).** Longer warm spell = higher mortality and
outdoor-labour-loss pressure.

**Change lens (weight 0.30, `relative_pct`).** Forced lengthening of warm
spells under warming directly extends the at-risk window for heat
mortality.

**Impact lens (weight 0.30; band 6 - 18 days; Self-derived, MEDIUM).**

- **Onset = 6 days.** IMD declares a heatwave when Tmax is ≥ 4.5 deg C
  above normal for ≥ 4 consecutive days (plains) or ≥ 6.4 deg C above
  normal for ≥ 2 days (severe). A persistent-heat duration of 6 days
  combines the IMD 4-day declaration with a ~2-day acclimatization-loss
  tail (the population's heat tolerance is degraded by extended consecutive
  exposure, and excess-mortality signal becomes statistically detectable
  around day 5-7 in observational studies; Azhar et al. 2014 PLOS ONE on
  the Ahmedabad 2010 heatwave).
- **Saturation = 18 days.** Heat-mortality literature plateau: Mazdiyasni
  et al. 2017 (*Science Advances*) India-wide find heat-mortality response
  saturates around 15-20 days of persistent extreme heat; 18 = midpoint of
  this plateau.
- **Confidence = MEDIUM.** The anchoring evidence is the exact pathway
  being scored (Indian mortality response to persistent heat), not an
  analogue from a different sector. Self-derived label retained because no
  Indian agency publishes a WSDI day-count categorical standard directly;
  HIGH would require an NDMA / IMD categorical "WSDI ≥ N days = catastrophic
  heatwave" definition.

**Per-lens weight summary.** 0.40 / 0.30 / 0.30 (MEDIUM imp 0.30 per
Section 4.4).

### 13.5 Life & Livelihood Loss Risk — bundle assembly notes

**Rule weights (sum to 1.00):**

| Rule | Rule weight | Cluster | Cluster weight |
|---|---:|---|---:|
| `rx1day_ge_200` | 0.30 | Rainfall / Flood mortality | 0.55 |
| `rx5day_livelihood_pressure` | 0.25 | Rainfall / Flood mortality | (sub-cluster) |
| `wsdi_ge_5` | 0.25 | Heat mortality | 0.25 |
| `cdd_ge_40` | 0.20 | Drought / Livelihood loss | 0.20 |

**Two-stage elicitation rationale:**

1. **Cluster weights (Rainfall 0.55 / Heat 0.25 / Drought 0.20).** Anchored
   on the relative acuity of the mortality pathway:
   - **Rainfall / Flood mortality 0.55.** Flash flooding and prolonged
     inundation are documented as India's largest single climate-related
     mortality driver in absolute terms (NDMA event statistics; Swiss Re
     Mumbai 2005 / Chennai 2015 / Kerala 2018 loss accounting). Acute
     onset, low adaptive capacity for informal-housing residents.
   - **Heat mortality 0.25.** Persistent-heat mortality is well-documented
     for India (Azhar 2014; Mazdiyasni 2017; ILO 2019 outdoor-labour-loss
     estimates) and has expanded its at-risk regions over the past two
     decades, but absolute mortality counts remain materially below
     flood-mortality counts at present.
   - **Drought / Livelihood loss 0.20.** Slow-onset, with materially higher
     adaptive capacity (state PDS, MGNREGA, distress migration as
     adaptation). Affects livelihoods catastrophically but is rarely a
     direct mortality pathway in present-day India.

2. **Within-cluster splits.** In the rainfall cluster, the 0.30 / 0.25
   split places Rx1day above Rx5day because acute flash-flood mortality
   (Rx1day) is generally more lethal per event than prolonged-inundation
   exposure (Rx5day, which is more displacement-/disease-mediated). Within
   the single-rule heat cluster, WSDI carries the full 0.25 cluster
   weight; within the single-rule drought cluster, CDD carries the full
   0.20.

**Cross-bundle reuse and parity.**

- Rx1day band 115.6 - 204.5 mm/day is the same IMD external anchor used in
  Health Section 6.4, Industrial Section 7.1, Investment Section 8.1,
  Infrastructure Section 9.1, and now Life & Livelihood Section 13.1. Any
  recalibration must change all five sections together.
- Rx5day band 250 - 500 mm/5d is the same self-derived band as Industrial
  Section 7.2, Investment Section 8.2, Infrastructure Section 9.2.
- WSDI band 6 - 18 days is anchored to mortality literature here, whereas
  Health Section 6.2 and Agricultural Section 12.3 use the same numeric
  band with weaker pathway-specific anchoring. The Life & Livelihood
  anchoring (Section 13.4) is the strongest of the three.

**Slug-rename note.** `cdd_ge_40` and `wsdi_ge_5` are misleading
post-migration (band 60-120 doesn't match `ge_40`; band 6-18 doesn't match
`ge_5`). Rename deferred to the CHG-0018 / CHG-0020 phantom-slug batch,
following the same precedent set in Infrastructure Section 9 (`rx5day_ge_400`
retained pending the same batch).

**Open extensions (out of scope for CHG-0037):**

- A dedicated extreme-cold-mortality rule (TNn or TNle10 with mortality
  anchoring) for northern wheat-belt cold-wave fatalities — currently only
  represented indirectly via Agricultural Risk Section 12.7.
- A short-duration outdoor-labour-loss rule (e.g. WBGT or TXge40 with
  ILO 2019 hour-loss anchoring) to better resolve the livelihood-loss
  component beyond the heat-mortality framing.
- Geography-zone bands (BL-0020) — would primarily affect the WSDI onset
  in northern plains (longer acclimatization) and the CDD saturation in
  peninsular rainfed systems.

---

## 14. Thematic Bundles — Deferred Extension

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

## 15. References (consolidated)

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
18. Task Force on Climate-related Financial Disclosures (2017). *Final
    Recommendations of the Task Force on Climate-related Financial
    Disclosures.* (Physical-risk methodology: scenario-conditional state at a
    given time horizon, not within-horizon linear trend. Anchors the lens
    reshape in Section 8.)
19. Network for Greening the Financial System (2024). *Guide to climate
    scenario analysis for central banks and supervisors* (update). NGFS,
    Paris. (Scenario-conditional projections at 2030 / 2050 / 2100 horizons;
    no within-horizon trend-pressure mechanism.)
20. Reserve Bank of India (2024). *Draft Disclosure Framework on
    Climate-related Financial Risks, 2024.* (Indian banks to assess physical
    risk across short / medium / long-term horizons under RCP scenarios;
    aligns with TCFD / NGFS scenario-at-horizon framing rather than
    within-horizon trend.)
21. Hawkins E., Sutton R. (2009). "The potential to narrow uncertainty in
    regional climate predictions." *Bulletin of the American Meteorological
    Society* 90(8): 1095-1107. (Internal-variability vs forced-signal
    decomposition; time-of-emergence framing — quantifies the multi-decadal
    timescales over which forced change exceeds internal noise. Anchors the
    "20-year linear trends vulnerable to decadal modes" argument in Section
    8 intro.)
22. IPCC AR6 WG1 (2021), Chapter 1 (*Framing, Context, Methods*) and Atlas;
    Frankignoul C., Gastineau G., Kwon Y.-O. (2017) and related works on
    decadal modes. (Sampling variability over 20-30-year periods can be
    dominated by internal climate variability — PDO / IPO / ENSO / NAO / IOD
    — rather than the forced signal, motivating period-mean comparisons over
    within-period slope-fitting.)
23. Swiss Re Institute — *Billion-dollar Rain* and reinsurance industry
    loss accounting for Indian urban flood events. (Empirical anchor for the
    flood-cluster financial-materiality weight in Section 8.6.)
24. International Labour Organization (2024-2025) and Indian heatwave
    economic-impact reporting: 2024 India heatwave estimated impact ~$194B
    in potential income / ~247B labour-hours nationally; peak national power
    demand record 270.82 GW on 21 May 2025. (Empirical anchors for HWFI
    financial materiality in Section 8.5.)
25. Central Public Health and Environmental Engineering Organisation
    (CPHEEO), Ministry of Housing and Urban Affairs — *Manual on Storm Water
    Drainage Systems.* (Indian municipal storm-drainage design intensities;
    typical municipal capacity ~100-150 mm/day depending on city tier and
    duration assumption. Anchors the Infrastructure Risk Rx1day band
    interpretation in Section 9.1.)
26. Indian Roads Congress — *IRC SP-13: Guidelines for the Design of Small
    Bridges and Culverts* and *IRC 5: Standard Specifications and Code of
    Practice for Road Bridges, Section I — General Features of Design.*
    (Indian road cross-drainage works are designed for 25-year return-period
    floods; embankment and bridge waterway works follow higher return-period
    criteria. Anchors the threshold-design rationale for Infrastructure
    Risk Section 9.0.)
27. Indian Railways — operational guidance on rail temperature and
    heat-related speed restrictions; rail temperature typically exceeds
    ambient by ~15-20 deg C under solar loading, with speed restrictions
    triggered around 65 deg C rail temperature (≈ 45 deg C ambient).
    (Empirical anchor for the rail-buckling pathway in Infrastructure Risk
    Section 9.3.)
28. Luo T., Krishnan D., Sen S. (2018). *Parched Power: Water Demands,
    Risks, and Opportunities for India's Power Sector.* World Resources
    Institute, Washington DC. (90 % of India's thermal capacity uses
    freshwater cooling; 40 % sited in high-water-stress areas; 13 of 20
    largest plants had at least one water-shortage shutdown 2013-2016;
    ~14 TWh generation lost in 2016 alone. Empirical anchor for the
    water-stress cluster weight in Asset Risk - Thermal Power Section 10.4.)
29. Council on Energy, Environment and Water (CEEW) — climate-risk and
    water-risk assessments for the Indian power sector (2018, 2021).
    (Water availability repeatedly identified as the #1 climate risk for
    the Indian thermal fleet. Anchors the cluster-split judgment in Asset
    Risk - Thermal Power Section 10.4.)
30. Maulbetsch J.S., DiFilippo M.N. (2006). *Cost and Value of Water Use
    at Combined-Cycle Power Plants.* California Energy Commission
    Publication CEC-500-2006-034. (Thermodynamic / Carnot efficiency loss
    of order 0.4-0.6 % per deg C ambient rise for thermal cycles; cooling
    tower and air-cooled condenser performance characterization. Anchors
    the heat-cluster mechanism in Asset Risk - Thermal Power Section
    10.2.)
31. IPCC AR6 WG1 (2021), Chapter 8 — *Water Cycle Changes*. (Documents
    intensifying Indian monsoon extreme-rainfall variability under warming.
    Anchors the change-lens addition for R95p interannual variability in
    Asset Risk - Hydropower Section 11.3.)
32. Government of India / SDMA Uttarakhand — *Uttarakhand Disaster of
    June 2013*; reports and assessments on hydropower asset damage:
    Vishnuprayag, Srinagar, and multiple Alaknanda plants damaged or
    destroyed; multi-GW affected; >$1B in hydro asset damage. (Empirical
    anchor for the flood/storm cluster weight in Asset Risk - Hydropower
    Section 11.4.)
33. Government of India / Chamoli disaster assessments (2021):
    Rishiganga (13.2 MW) destroyed and Tapovan-Vishnugad (520 MW)
    damaged by Feb 2021 cloudburst-driven flash flood and debris flow.
    (Empirical anchor for the flood/storm asset-destruction pathway in
    Asset Risk - Hydropower Section 11.1.)
34. ICIMOD / NRSC / Government of Sikkim assessments — *South Lhonak
    GLOF, October 2023*. Teesta-III 1200 MW destroyed by glacial-lake-
    outburst flood; ~$1.2B asset loss. (Empirical anchor for the
    Himalayan asset-destruction pathway and the GLOF methodology gap
    flagged in Asset Risk - Hydropower Section 11.1 and BL-0034.)
