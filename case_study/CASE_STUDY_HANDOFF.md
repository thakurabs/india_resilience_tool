# PERFECT HANDOFF POINT — data-centre case study

**Session:** `irt_demo_case_study` · **Date:** 2026-07-30
**Working Snapshot:** `GIT:add_flood_depth@1da5524` · working tree dirty (untracked `case_study/`)
**Next session's job:** build the case-study narrative. No results gathered yet.

---

## 1. Objective

Build a demo case study for the vendor web app at **dev.resilience.org.in** (NOT
this repo's Streamlit prototype). Title framing:

> **Where should India's next data centre *not* go?**

Audience is mixed — **investor pitch + corporate/private sector + donor/DFI** — and
the case study must prove **the decision the tool unlocks**, not its data breadth or
methodology. It is a **generic showcase**, not tuned to a named prospect.

Chosen over the alternative idea (**NITI aspirational districts**), which survives as
a closing coda: same engine, swap the question, public-sector buyer, drop to block
level to match the Aspirational *Blocks* Programme.

---

## 2. Decisions locked (do not re-litigate)

| Decision | Choice |
|---|---|
| Hero case | Data centres. Aspirational districts = closing coda |
| What it proves | The decision it unlocks (narrative-led) |
| Decision shape | **Both** — stress-test the existing footprint, then screen for new ground |
| Demo path | **Coordinate upload flow** — it is the product moment |
| Site set | ~30 **real named campuses**, publicly sourced |
| No data-centre bundle exists | **Compose from existing bundles, and make that the point** (extensibility/TAM argument) |
| Riverine Flood is snapshot-only | **Present-day flood = hard exclusion filter; 2050 heat = ranking.** Narrate explicitly as method |
| Build effort | Light overlay — no new pipeline work in the tool |

---

## 3. The deployed tool's actual surface

Only **thematic + sectoral bundles** are live. Groundwater / per-capita water
scarcity are in this repo but **uncommitted and NOT live** — do not build on them.

| Group | Bundles | Scenarios |
|---|---|---|
| Thematic | Heat Risk, **Heat Stress**, Drought, **Extreme Rainfall / Flash Flood**, **Riverine Flood**, Cold | SSP245/585, **except Riverine Flood = snapshot only** |
| Sector-wise | Agricultural, Health, **Industrial**, **Investment/Financial**, **Infrastructure**, Asset Risk (Thermal Power), Asset Risk (Hydropower), Life & Livelihood Loss | SSP245/585 |

**JRC riverine flood = RP100 only** on the deployed app.

**Heat Stress is a pure wet-bulb bundle** — verified in `bundle_weights.py`:
`twb_annual_mean`, `twb_summer_mean`, `twb_annual_max`, `twb_days_ge_28`,
`twb_days_ge_30`, `tasmin_tropical_nights_gt28`. This is the case study's strongest
asset: wet-bulb governs evaporative/adiabatic cooling capacity, so it maps directly
onto PUE and WUE. Almost nobody in India puts this on a district map.

---

## 4. ⚠ THE CONSTRAINT THAT SHAPES EVERYTHING

**The composite is only intra-state comparable** (user-confirmed, 2026-07-30).

Consequences:

- ❌ Cross-state ranking of the portfolio on composite score is **invalid**. Never
  write "Chennai scores worse than Hyderabad" from composite values.
- ✅ **Physical metrics are the quantitative spine** — nationally comparable by
  construction: `twb_days_ge_30` (days/yr), `twb_annual_max` (°C), `Rx1day` (mm),
  `jrc_flood_depth_rp100`.
- ✅ **Within-state percentile / rank is valid and useful** — "Panvel is the *n*th
  most flood-exposed of Maharashtra's districts" is defensible.
- ⚠ Composite is demoted to **map visual + within-state ranking only**.
- ⚠ Act 2 (screening nationally for better ground) **requires** physical metrics.

Editorial upside: "Chennai runs X days/yr above 30 °C wet-bulb; Hyderabad runs Y" is
a stronger investor line than any composite score — it is a fact about cooling rather
than a request to trust a weighting.

---

## 5. Act structure

### Act 1 — Stress-test what exists (present day)

1. **Riverine Flood (RP100)** — the hard filter. Which campuses sit in high-depth districts.
2. **Extreme Rainfall / Flash Flood** — the pluvial layer. Different districts light up
   than in step 1. The beat: *most flood maps conflate the two mechanisms; this one
   doesn't.* Chennai 2015 was drainage, not river.
3. **Heat Stress (wet-bulb)** — the money slide. Cooling-cost and WUE/PUE story.

Output: portfolio ranked, each site tagged with the hazard that actually threatens it.

### Act 2 — Screen for new ground (2050)

- RP100 flood as a **present-day exclusion**.
- Heat Stress **2050** ranks what survives (physical metric, not composite).
- Industrial + Infrastructure + Investment/Financial for the operating environment.
- Narrate the asymmetry openly: *"flood eliminates on today's evidence; heat ranks on tomorrow's."*

### Act 3 — The composability close

"We never built a data-centre module. The question decomposed into five bundles that
already existed. Every sector with a physical footprint decomposes the same way."
→ ADP coda: swap Industrial for Health + Agricultural + Life & Livelihood Loss, swap
the corporate buyer for a state government, drop to block level.

### Framing that protects the claim

Position as **"eliminates bad options early," not "picks the site."** There is no
power, fibre, land-cost or grid layer — this narrows a longlist. Claiming less makes
the actual claim unassailable.

**Honest-limitations slide (include unprompted):** water/groundwater in build not
live; riverine flood present-day only; hazard screen, not a siting model.

---

## 6. What is built and committed-ready

`case_study/data_centres/` — **34 sites, 19 districts, 11 states.**

```
case_study/data_centres/
├── upload/                          exact tool format: id,custom_name,lat,long
│   ├── dc_sites_operational.csv     26 points from 30 sites  ← Act 1
│   ├── dc_sites_announced.csv        4 points                ← Act 2
│   └── by_state/                    10 files, operating sites only
├── reference/dc_sites_all.csv       34 rows, full provenance
├── build_site_csvs.py               regenerates everything, deterministic
└── README.md                        provenance + caveats
```

**Provenance:** locality verified from public sources (operator location pages, DCD,
TechCrunch, aggregator listings), then geocoded via **Nominatim/OpenStreetMap** (ODbL).
No coordinate invented. Locality-precise, not campus-precise — adequate for district
resolution.

**Upload format** matches `sample_coordinates.xlsx` exactly: `id,custom_name,lat,long`;
`id` restarts at 1 per file; `custom_name` = `operator - facility`, ASCII-only, never
leading with `= + - @`.

**Coordinates are unique per file** — the app rejects duplicates. Co-located campuses
are **merged into one labelled point** rather than dropped: SIPCOT Siruseri (3),
Ambattur (2), Electronic City (2). A merged point therefore scores a **cluster, not one
operator's campus** — never imply STT and AdaniConneX were assessed independently at
Siruseri.

### Harvest sizing — the result set is small

**30 operational sites → 26 points → 16 distinct districts.**

| State | Sites | Points | Districts |
|---|---|---|---|
| Maharashtra | 11 | 11 | 5 |
| Tamil Nadu | 5 | 2 | 2 |
| Karnataka | 3 | 2 | 1 |
| Telangana | 3 | 3 | 2 |
| Uttar Pradesh | 3 | 3 | 1 |
| Haryana / West Bengal / Odisha / Rajasthan / Gujarat | 1 each | 1 each | 1 each |

Notable: **Tamil Nadu's entire footprint sits on two points.** Whatever Ambattur and
Siruseri score lands on five campuses from four operators — that is a
**concentration-risk finding about the Chennai market**, a stronger Act 1 beat than any
single site's rank.

### Rejected data source

`Global-Data-Center-Map` (github.com/Ringmast4r) — **do not use.** 342 India rows but
238 lack coordinates; the 104 with coordinates collapse to 19 **city centroids**
(Mumbai 19.0760,72.8777 ×34, etc.). Column corruption (emails/phones in City), no
entity normalisation, stale (lists insolvent Net4 / Reliance Communications), 36 stars,
5 commits, no stated sources or methodology. Its **address column** (337/342) is the
only salvageable part — usable as a coverage checklist to find missed campuses
(Tata Communications, Iron Mountain, MetaEdge, Ensono absent from our 30).

---

## 7. Result-gathering strategy (agreed, not yet executed)

**Split the two goals — do not share a workflow:**

| Goal | Vehicle |
|---|---|
| Numbers for the deck | **Ranking Table → export.** One export returns every district in a state, so all 16 arrive in ~10 exports, with within-state rank for free |
| The demo motion | **Coordinate upload** of `by_state/*.csv`, captured as screenshots/recording, separately |

Harvesting via Compare Portfolio would be slower and hits **N25** (out-of-state sites
silently dropped from the report) — which is why the per-state files exist.

**Fixed sequence per run** (UI is order-sensitive):
`select State → select bundle → select scenario + period → Table view → export`

| Phase | Scope | Exports | Covers |
|---|---|---|---|
| 1 | 5 states (MH, TN, TS, KA, UP) × 3 bundles, present-day | 15 | 25/30 sites (83%) |
| 2 | 5 singleton states (HR, WB, OD, RJ, GJ) × 3 bundles | 15 | all 30 |
| 3 | Heat Stress + Extreme Rainfall, mid-century scenarios | ~20 | Act 2 |
| 4 | Industrial / Infrastructure / Investment on shortlist only | ~6 | Act 3 |
| 5 | Demo-motion capture via `by_state/` upload | — | screenshots |

Core bundles for phases 1–2: **Heat Stress**, **Extreme Rainfall / Flash Flood**,
**Riverine Flood** (snapshot only).

**Record provenance per export:** bundle, scenario, period, level, state, date,
filename. This is a dev environment whose data can be republished — date-stamp
everything.

---

## 8. Traps already documented in `qa/reports/` — do not rediscover

- **N25** — uploaded site resolving outside the selected state is **silently dropped**
  from the Compare Portfolio report, no warning. Work state-by-state.
- **Resilience filters persist and are non-idempotent** — set once per session;
  re-applying hangs (~30s).
- **Mode switch (admin ↔ coordinate) clears geography *and* portfolio** — re-select
  state after every switch.
- **M5** — composites fire HTTP 500 on Resilience Profile trend/scenario-comparison
  (composites have no time series). For a per-site trend chart, select a **single
  metric**.
- **N4** — ranking caption leaks internal slugs (`composite_heat_risk • ssp245`) —
  crop before any deck screenshot.
- **B1 (ranking 500) is FIXED** as of 2026-07-27 — ranking table loads.
- **M7 (upload value validation) MOSTLY FIXED** — invalid/out-of-range now reject.
- **N23** — formula injection stored verbatim but latent (no live export sink). Our
  `custom_name` values are already ASCII and non-formula-leading.
- Map click box needs a real GL context (`QA_SOFTWARE_GL=1` / headed) — irrelevant for
  manual driving.

---

## 9. Next steps, ordered

1. **Test the untested hypothesis (~15 min).** Export Heat Stress present-day for
   **Tamil Nadu** and **Telangana**; compare `twb_days_ge_30` / `twb_annual_max` for
   Tiruvallur + Chengalpattu vs Ranga Reddy + Hyderabad. Use **physical metrics**, not
   composite. **Act 1 rests entirely on this separation existing.** If coastal-humid
   and plateau don't separate, the narrative needs rework before anything is built.
2. **Answer the two open questions** in §10.
3. Build the **run-log template** + **ingestion script** (reads exported ranking files,
   joins to the 16 districts in `reference/dc_sites_all.csv`, emits Act 1 / Act 2
   tables) so analysis is reproducible rather than hand-assembled.
4. Run **Phase 1** (15 exports).
5. Build the narrative from real numbers. Phases 2–4. Demo-motion capture last.

---

## 10. Open questions / risks

**Open questions (blocking scale-up, not step 1):**
- **Level:** district only, or district + block for a zoom-in? (Default: district only.)
- **Scenarios:** SSP245 only, or SSP245 + SSP585? (Default: SSP245 only. Both defaults
  roughly halve the work.)

**Risks:**
- 🔴 **Act 1's core hypothesis is untested** — that humid heat separates coastal from
  plateau. Everything narrative rests on it. Step 1 exists to settle this.
- 🟠 **No cross-state composite comparability** (§4) — any slide comparing composite
  scores across states is wrong. Physical metrics only for national claims.
- 🟠 **Merged points score clusters, not campuses** — 7 sites behind 3 points.
- 🟠 **Announced sites (DC22/32/33/34) are not operating assets** — keep visually
  distinct; a risk score on an unbuilt facility invites a fair objection.
- 🟡 **Locality-not-campus precision** — Navi Mumbai cluster straddles Thane/Raigad.
- 🟡 **Ambattur district ambiguity** — municipality inside Greater Chennai Corporation,
  revenue district Tiruvallur. If the tool returns Chennai, that's boundary vintage,
  not a file error.
- 🟡 **Dev-environment data can be republished** — date-stamp all exports.
- 🟡 Repo-side flag from `docs/metric_distribution_review.md`: "percentile inert
  historical" — sanity-check that present-day/baseline composite values aren't
  degenerate when they first appear.

---

## 11. CHG ledger (this session)

| Change ID | File(s) | Summary | Status |
|-----------|---------|---------|--------|
| CHG-0310 | `data_centres/build_site_csvs.py` | Verified site table + CSV generator | `APPLIED (user-confirmed)` |
| CHG-0311 | `data_centres/README.md` | Provenance, sources, caveats | `APPLIED (user-confirmed)` |
| CHG-0312 | `data_centres/*.csv` | Generated site CSVs | `SUPERSEDED` by 0313–0315 |
| CHG-0313 | `data_centres/build_site_csvs.py` | Emit `upload/` in tool format + `reference/` | `APPLIED (user-confirmed)` |
| CHG-0314 | `data_centres/upload/*.csv` | Regenerated to `id,custom_name,lat,long` | `APPLIED (user-confirmed)` |
| CHG-0315 | `data_centres/README.md` | Document upload contract + state-file scope | `APPLIED (user-confirmed)` |
| CHG-0316 | old `dc_sites_*.csv`, `by_state/` | Superseded layout removed | `APPLIED (complete)` |
| CHG-0317 | `data_centres/build_site_csvs.py` | Dedupe coordinates; merge co-located operators | `APPLIED (user-confirmed)` |
| CHG-0318 | `data_centres/upload/*.csv` (13) | Regenerated unique-coordinate files | `APPLIED (user-confirmed)` |
| CHG-0319 | `data_centres/README.md` | Dedupe/merge + cluster-scoring caveat | `APPLIED (user-confirmed)` |
| CHG-0320 | `case_study/CASE_STUDY_HANDOFF.md` | This handoff | `APPLIED (user-confirmed)` |

**Tests run:** none. Per `CLAUDE.md` §3 this is case-study input data, not scientific
compute or a tool data contract. Verification was by inspection: all 13 upload files
confirmed to have header `id,custom_name,lat,long`, zero duplicate coordinates, and
`id` running 1..n.

**`MANIFEST.md` / `README.md`:** not updated. `MANIFEST.md` likely wants a line for the
new top-level `case_study/` directory — **left for explicit approval.**

---

## 12. Git handoff

```bash
git add case_study/CASE_STUDY_HANDOFF.md case_study/data_centres/build_site_csvs.py case_study/data_centres/README.md case_study/data_centres/reference case_study/data_centres/upload
git commit -m "Add data-centre case study site list, upload CSVs and handoff"
```
