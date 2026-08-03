# Findings — Humid heat and India's data-centre clusters

**Status:** DRAFT · **Date:** 2026-07-30
**Source:** `dev.resilience.org.in`, Compare Portfolio flow, **Heat Stress** bundle
**Level:** block · **Scenario:** SSP5-8.5 · **Baseline:** 1990–2010
**Coverage:** 7 state exports → 18 blocks → 5 metro clusters

---

## 1. Scope and provenance

Six Compare Portfolio exports, all dated 2026-07-30, all Heat Stress bundle at
block level under SSP5-8.5:

| State | Export file | Blocks returned |
|---|---|---|
| Tamil Nadu | `portfolio-comparison-table-20260730-1403.xlsx` | Chennai, Thiruporur |
| Telangana | `...-1358.xlsx` | Gandipet, Shabad, Shaikpet |
| Karnataka | `...-1414.xlsx` | Bengaluru East, Bengaluru South |
| Uttar Pradesh | `...-1420.xlsx` | Bisrakh, Dankaur |
| Maharashtra | `...-1527.xlsx` | Mumbai Suburban, Thane, Panvel, Pune City, Mulshi, Nashik |
| Haryana | `...-1543.xlsx` | Gurgaon |
| Delhi | `...-1600.xlsx` | New Delhi, South East |

Blocks were resolved by the tool from the uploaded coordinates in
`../upload/by_state/*.csv`. Note that Ambattur resolved to **Chennai** district
rather than Tiruvallur — boundary vintage, not a file error (flagged in the
handoff risk list).

Dev-environment data can be republished. All values here are as of 2026-07-30.

---

## 2. Method constraint: why no composite score appears below

**The composite is excluded from all findings.** It is not merely intra-state
comparable — it is intra-state *and* intra-period, and its trends run backwards.
Evidence from the same exports:

| Cluster | SSP2-4.5 early → mid → end | SSP5-8.5 early → mid → end |
|---|---|---|
| Chennai | 61.8 → 63.6 → 65.9 | 62.1 → 64.7 → 69.4 |
| Hyderabad | 29.1 → 32.0 → **31.3** | 27.3 → 30.1 → **27.2** |
| Bengaluru | 16.4 → 15.5 → **15.3** | 16.1 → 15.7 → 17.0 |
| Delhi NCR (Bisrakh) | 38.0 → 42.5 → **41.0** | 40.5 → 39.7 → **36.7** |

Three of four clusters score **lower** under SSP5-8.5 than SSP2-4.5, and
Bengaluru's risk *falls* toward end-century. This is the relative-percentile
artefact: the composite reports a block's position among its state peers, so as
an entire state warms a block can slide down the ranking while its absolute
conditions worsen. Baseline is `N/A` throughout, consistent with the
"percentile inert historical" flag in `docs/metric_distribution_review.md`.

Everything below therefore rests on **physical metrics only** — days per year,
degrees Celsius — which are absolute and nationally comparable by construction.

---

## 3. Headline findings

### F1 — Humid heat separates the clusters by roughly 50×

At mid-century, days per year above 28 °C wet-bulb range from **74.5 (Chennai)
to 1.4 (Bengaluru East)** — a 54× spread within one country and one decade.
This is not a marginal separation requiring an index to become visible.

### F2 — The clusters fall into two tiers with almost nothing between them

| Tier | Blocks | Days ≥28 °C, mid-century |
|---|---|---|
| **Exposed** | Chennai 74.5 · Mumbai Suburban 69.1 · Thane 68.6 · Panvel 67.2 · Dankaur 66.8 · Bisrakh 65.4 · Thiruporur 64.2 · South East Delhi 63.7 · New Delhi 59.0 · Gurgaon 51.5 | **51–75** |
| **Favourable** | Pune City 9.2 · Gandipet 7.6 · Shaikpet 7.6 · Nashik 6.8 · Mulshi 6.8 · Shabad 5.5 | **5–9** |
| **Best** | Bengaluru South 1.9 · Bengaluru East 1.4 | **1–2** |

A ~6× gap separates the tiers, and no block occupies the middle. The siting
question is therefore **binary, not a continuous ranking** — which is the more
useful form for a decision-maker.

Mid-century is both the decision-relevant horizon and the horizon of widest
separation: by end-century the exposed clusters approach threshold saturation
(Chennai 137.9 days), which *compresses* the ratios.

### F3 — Maharashtra splits in half across the Western Ghats

One state, six blocks, mid-century days ≥28 °C:

| Block (district) | Baseline | Mid | End |
|---|---:|---:|---:|
| Mumbai Suburban | 13.2 | **69.1** | 112.7 |
| Thane | 13.6 | **68.6** | 111.5 |
| Panvel (Raigad) | 13.6 | **67.2** | 111.8 |
| Pune City (Pune) | 2.9 | **9.2** | 22.9 |
| Mulshi (Pune) | 2.0 | **6.8** | 18.1 |
| Nashik | 2.0 | **6.8** | 16.1 |

**7.5× between Mumbai Suburban and Pune City at mid-century**, ~150 km apart,
same state, same power market, same policy regime. The Ghats escarpment is a
humid-heat boundary.

This is the most actionable finding in the set. It requires no one to leave
Maharashtra — Pune is already ~9% of national operational capacity and sits on
the favourable side. It is also a within-state comparison, so it survives every
comparability constraint.

### F4 — Delhi NCR fails differently from the coastal clusters

NCR is the **driest cluster on average and the most extreme at its peak**:

| Metric | Chennai | Delhi NCR (Dankaur) |
|---|---:|---:|
| Mean Twb, baseline | **24.69 °C** (highest) | **18.54 °C** (lowest) |
| Summer mean Twb, baseline | **25.38 °C** | **17.34 °C** |
| Annual max Twb, baseline | 28.64 °C | **29.36 °C** |
| Days ≥30 °C, mid-century | 6.4 | **17.7** (highest) |

NCR carries the worst ≥30 °C exposure at **every** horizon (baseline 2.7 → mid
17.7 → end 37.9), roughly 2× Chennai throughout, so this is not a single-snapshot
artefact. Two distinct engineering problems follow:

- **Chennai / MMR = chronic load.** Evaporative and adiabatic cooling operate
  with permanently reduced headroom. Structurally higher WUE and PUE year-round.
- **Delhi NCR = acute spikes.** Cheap to cool most of the year, then monsoon-break
  events push wet-bulb past 30 °C more often than anywhere else — a design-day and
  peak-capacity problem that coincides with the regional grid peak.

This also disposes of the objection that wet-bulb is temperature repackaged: NCR
is the hottest cluster on air temperature and the *coolest* on mean wet-bulb.

### F5 — NCR carries an internal west–east humid-heat gradient across three states

With Delhi NCT added, NCR resolves into five blocks spanning Haryana, Delhi and
Uttar Pradesh — and they order **monotonically from west to east**:

| Block (district, state) | Approx. easting | Days ≥28, mid | Days ≥30, mid |
|---|---|---:|---:|
| Gurgaon / Manesar (Gurugram, **HR**) | westmost | **51.5** | 12.0 |
| New Delhi (New Delhi, **Delhi**) | ↓ | 59.0 | 14.3 |
| South East (South East, **Delhi**) | ↓ | 63.7 | 16.4 |
| Bisrakh (Gautam Buddha Nagar, **UP**) | ↓ | 65.4 | 17.2 |
| Dankaur (Gautam Buddha Nagar, **UP**) | eastmost | **66.8** | 17.7 |

A **~30% spread across roughly 60 km**, rising steadily eastward — the drier
Aravalli/Thar-influenced western edge grading into the moister Ganga–Yamuna
plain. This is not a state-boundary artefact; it is a continuous physical
gradient that happens to cross two state lines.

Two things follow:

- **It is impossible to see on composite score**, which cannot be compared across
  Haryana, Delhi and Uttar Pradesh. It exists only because the physical metrics
  are absolute. This is the clearest live demonstration of why the
  physical-metric spine was the right choice.
- **It is the one cluster where sub-cluster siting genuinely matters.** Choosing
  the western edge of NCR over the eastern edge buys ~15 fewer humid-heat days a
  year at mid-century, within the same labour market and grid region.

### F6 — The ranking is set by present-day climate, not by scenario

Warming increment in mean wet-bulb, baseline → mid-century, across all 18 blocks
spans just **+1.36 °C to +1.72 °C** (a 0.36 °C spread). Baseline values span
**18.03 °C to 24.69 °C** — a 6.66 °C spread, roughly **18× larger** than the
spread in warming.

The cluster ordering is therefore almost entirely a property of existing climate.
Projections determine *when* thresholds are crossed, not *who* is exposed.

Two consequences:

- The SSP2-4.5 vs SSP5-8.5 choice largely dissolves — the ordering does not
  depend on it.
- The siting recommendation can rest on observed baseline climate, which is far
  more defensible than a recommendation resting on a high-emissions scenario.

---

## 4. Full data table

SSP5-8.5, block level, baseline 1990–2010. Sorted by mid-century days ≥28 °C.

| Cluster | Block (district) | Mean Twb base | Mean Twb mid | Max Twb base | Days ≥28 base | mid | end | Days ≥30 base | mid | end |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| Chennai | Chennai (Chennai) | 24.69 | 26.05 | 28.64 | 14.52 | 74.51 | 137.88 | 0.55 | 6.43 | 24.18 |
| MMR | Mumbai Suburban | 22.25 | 23.81 | 29.54 | 13.18 | 69.09 | 112.65 | 2.51 | 7.94 | 23.89 |
| MMR | Thane (Thane) | 22.11 | 23.68 | 29.56 | 13.56 | 68.59 | 111.49 | 2.57 | 8.22 | 24.06 |
| MMR | Panvel (Raigad) | 22.14 | 23.69 | 29.54 | 13.57 | 67.24 | 111.80 | 2.46 | 8.06 | 23.53 |
| Delhi NCR | Dankaur (G.B. Nagar) | 18.54 | 20.24 | 29.36 | 26.91 | 66.83 | 85.85 | 2.69 | 17.72 | 37.88 |
| Delhi NCR | Bisrakh (G.B. Nagar) | 18.58 | 20.28 | 29.29 | 25.74 | 65.42 | 84.55 | 2.45 | 17.17 | 36.75 |
| Chennai | Thiruporur (Chengalpattu) | 24.65 | 26.01 | 28.51 | 12.59 | 64.21 | 127.24 | 0.49 | 5.53 | 20.57 |
| Delhi NCR | South East (Delhi) | 18.51 | 20.21 | 29.22 | 23.63 | 63.67 | 83.21 | 2.26 | 16.43 | 35.32 |
| Delhi NCR | New Delhi (Delhi) | 18.35 | 20.07 | 29.03 | 18.05 | 58.95 | 79.93 | 1.71 | 14.32 | 31.37 |
| Delhi NCR | Gurgaon (Gurugram) | 18.03 | 19.75 | 28.81 | 13.22 | 51.51 | 74.94 | 1.17 | 12.02 | 26.87 |
| Pune | Pune City (Pune) | 19.88 | 21.38 | 27.54 | 2.87 | 9.21 | 22.93 | 0.46 | 1.72 | 3.92 |
| Hyderabad | Gandipet (Ranga Reddy) | 20.20 | 21.67 | 26.70 | 1.16 | 7.63 | 23.04 | 0.02 | 0.57 | 2.60 |
| Hyderabad | Shaikpet (Hyderabad) | 20.20 | 21.67 | 26.70 | 1.16 | 7.63 | 23.04 | 0.02 | 0.57 | 2.60 |
| Nashik | Nashik (Nashik) | 18.85 | 20.46 | 26.93 | 1.96 | 6.84 | 16.08 | 0.23 | 1.26 | 2.85 |
| Pune | Mulshi (Pune) | 19.83 | 21.32 | 27.07 | 2.01 | 6.79 | 18.10 | 0.35 | 1.22 | 2.89 |
| Hyderabad | Shabad (Ranga Reddy) | 20.08 | 21.55 | 26.40 | 0.79 | 5.51 | 18.17 | 0.01 | 0.37 | 1.70 |
| Bengaluru | Bengaluru South | 20.98 | 22.44 | 24.95 | 0.43 | 1.90 | 5.98 | 0.01 | 0.12 | 0.49 |
| Bengaluru | Bengaluru East | 20.75 | 22.21 | 24.74 | 0.23 | 1.39 | 4.91 | 0.00 | 0.06 | 0.32 |

---

## 5. Implication for the case study

Against Cushman & Wakefield's H1 2025 capacity shares:

| | Clusters | Share of operational capacity | Share of pipeline |
|---|---|---:|---:|
| **Exposed tier** | Mumbai 46% + Chennai 15% + Delhi NCR 11% | **~72%** | **~62%** |
| **Favourable tier** | Hyderabad 11% + Pune 9% + Bengaluru 6% | **~26%** | ~19% (Hyderabad) |

Roughly **72% of India's operational data-centre capacity sits in the three
highest humid-heat clusters, and the pipeline only marginally corrects it** —
about 62% of announced capacity is still going into the exposed three.

This is an argument about the *next* decision, not a verdict on past ones. The
industry sited for fibre, power, land and customer proximity — all rational. Humid
heat was not on the scorecard because nobody had it at this resolution.

### Intra-cluster vs inter-cluster

Within most clusters, block-level spread is modest: Shabad sits ~28% below
Gandipet/Shaikpet; Chennai block ~16% above Thiruporur; MMR's three blocks are
within 3% of each other. Against 6–50× between tiers, that variation is second
order.

**Delhi NCR is the exception.** It spans 51.5 to 66.8 days — a ~30% internal
spread across three states (F5), the only cluster where the sub-cluster choice
carries real weight.

Block resolution is therefore best presented as a **capability demonstration**,
plus the two cases where it produces a genuine finding: the Ghats split (F3),
which is really two clusters inside one state, and the NCR gradient (F5).
Everywhere else, the cluster is the decision unit.

---

## 6. Caveats and limitations

- **SSP5-8.5 only.** All detail sheets carry the high-emissions scenario. F6
  argues the ordering is scenario-insensitive, but the deck should state the
  scenario plainly or add an SSP2-4.5 pass.
- **Composite excluded** for the reasons in §2. No slide should show composite
  trends, scenario comparisons, or cross-state composite values.
- **Merged upload points score clusters, not campuses.** Seven sites sit behind
  three coordinates (SIPCOT Siruseri ×3, Ambattur ×2, Electronic City ×2).
- **Locality-precise, not campus-precise** coordinates. Adequate for block and
  district resolution; not a site-level verdict.
- **Absent hazards.** Coastal inundation, sea-level rise, cyclone and storm surge
  are not in the tool and are first-order for Chennai and MMR — together ~61% of
  national capacity. Grid resilience (SAIDI/SAIFI, dual feed, substation flood
  exposure) is also absent. Water/groundwater bundles are built but not live.
- **Data flag, not for presentation:** Bengaluru tropical nights are
  non-monotonic — 6.13 baseline → **5.78** mid → 12.71 end (East) and 6.63 →
  **6.20** → 13.37 (South). Bengaluru is the only cluster showing this. Low-count
  metric at ~900 m elevation, but it needs a check before appearing anywhere.
- **Sample basis.** 34 sites against a reported national population of ~271
  facilities (CEEW, Jan 2026). This is a capacity-weighted sample of the major
  clusters, not a census.

---

## 7. Open items

1. ~~Delhi NCT has no sites.~~ **RESOLVED 2026-07-30.** Three real campuses added
   (DC35–DC37): STT Delhi 1 at Videsh Sanchar Bhavan, Bangla Sahib Road (New
   Delhi district) and STT Delhi 2 and 3 sharing the Greater Kailash I complex
   (South East district, merged to one upload point). `upload/by_state/delhi.csv`
   now exists with 2 points. No centroid was invented. **Delhi exported
   2026-07-30** (`analysis/delhi/...-1600.xlsx`); both blocks landed inside the
   predicted Gurugram–Noida bracket and upgraded F5 from a two-point split to a
   five-block gradient.

   Worth carrying into the narrative: Delhi NCT proper hosts only ~12 MW of
   commercial colocation, all STT. The NCR cluster is really Noida plus Gurugram,
   which is why the original inventory was not materially wrong.
2. **Riverine Flood (RP100) and Extreme Rainfall exports** not yet run. These are
   Act 1 beats 1 and 2; only the heat beat is evidenced so far.
3. **Heat ∩ drought coincidence cut** — the intersection of high Heat Stress and
   high Drought, which the deep-research report identifies as the Hyderabad and
   NCR "peak coincidence" problem. Cheap once Drought exports exist.
4. **Cluster capacity table** — C&W H1 2025 MW figures per cluster, so every
   cluster claim carries weight and a citation.
5. **SSP2-4.5 pass** if the deck needs a lower-emissions comparison.
