# Outdoor WBGT engineering pilot — Kerala, Rajasthan, Himachal Pradesh

**Verdict: `ENGINEERING CONDITIONAL`.**

The pipeline is correct and cheap. The blocker is input data, not code.

Milestone 4. Frozen specification: [`SPEC.md`](SPEC.md), written before any result existed.
Runner: `tools/diagnostics/wbgt_outdoor_pilot.py`. Tests:
`tests/test_wbgt_outdoor_pilot.py`.

> **Everything in this directory is DIAGNOSTIC-ONLY.** No value here enters the dashboard,
> the production metric registry, composites, frozen rulers or published maps. An engineering
> pass is not permission for national publication, and the scientific accuracy work listed in
> §7 still blocks it.

---

## Decision

| Question (SPEC.md §1, §14E) | Answer |
|---|---|
| Does the actual NEX input path work correctly? | **Yes.** All 18 variable-years verified; all six variables on one identical grid; `rsds` and `sfcWind` enter numerically. |
| Does the pilot reproduce W1? | **Yes, exactly.** `max abs diff 0.000e+00 °C` over 367 days against `m2.nex_candidate_daily_max`, for both W1 and C1. |
| Are district/block aggregates correct? | **Yes.** 36/36 admin checks pass; blocks re-aggregate to districts to `1.4e-14 °C`. |
| What data-support problems remain? | **12 of 784 cell-years lost (1.5 %)**, concentrated in Himachal; no DEM for cell elevation. |
| What is the measured cost? | **331 s wall, 0.78 GB peak RSS, 1.1 MB artifacts**, at one worker, alongside the running shade rebuild. |
| Is broader engineering validation justified? | **Yes**, once the completeness rule and the elevation input are settled. |

`CONDITIONAL` rather than `PASS` for two reasons, both measured below: a single marginally
invalid input day destroys a whole cell-year, which costs Lahul and Spiti 39.5 % of its area
(§3); and there is no elevation source, so every cell runs at sea level (§4).

---

## 1. Completed scope

| Item | Value |
|---|---|
| States | Kerala, Rajasthan, Himachal Pradesh — **all three completed** |
| Levels | district **and** block |
| Units | **75 districts, 581 blocks** |
| Model / member / scenario / year | ACCESS-CM2 / `r1i1p1f1` / `historical` / **2005** |
| Climate cells computed | **784** intersecting cells (Kerala 86, Rajasthan 582, Himachal 116) |
| Candidates | W1 (the frozen method) and C1 (method-sensitivity comparator) |
| Second model | MRI-ESM2-0, Himachal Pradesh only, as predeclared in SPEC.md §13 |

2005 was chosen because it is interior to the local `rsds`/`sfcWind` coverage (1990–2010), so
both padding days exist, and it is not a leap year. Padding days 2004-12-31 and 2006-01-01
were loaded and verified present; no target year was lost to absent padding.

---

## 2. Parity — the engineering rewrite changed no number

Run **before** any state was scaled, on a real Kerala pilot cell, per SPEC.md §9.

| Check | Result | Tolerance |
|---|---|---|
| Pilot path vs `m2.nex_candidate_daily_max`, **W1** | `0.000e+00 °C` over 367 days | ≤ 1e-9 |
| Pilot path vs `m2.nex_candidate_daily_max`, **C1** | `0.000e+00 °C` over 367 days | ≤ 1e-9 |
| Target-year-only hours vs full 3-year span | `0.000e+00 °C` over 365 days | ≤ 1e-9 |
| First and last target day finite (padding works) | **yes** | — |
| One cell alone vs inside a batch | **bit-identical** (`27.901223638732365`) | exact |
| Chunk sizes 1, 3 and 4 vs whole | **bit-identical**, counts exactly equal | exact |
| Resume vs uninterrupted | **bit-identical** (asserted in tests) | exact |

The third row licenses the runner's main saving: hours are built only for the target year,
because each reconstructed hour reads daily values from its own day and its two neighbours
only. That is a 3× reduction in solver work for zero change in output.

W1 and C1 are also asserted to genuinely differ (`> 0.01 °C`), so the parity checks cannot
pass by the two candidates being the same computation.

---

## 3. Data support — the reason this is CONDITIONAL

### 3.1 Twelve cell-years lost to marginal input defects

`cell_invalidity.csv` lists every one.

| State | Cells lost | Cause | Worst value |
|---|---|---|---|
| Himachal Pradesh | **11 of 116** | `hurs` above 100 % | **101.208 %** |
| Kerala | **1 of 86** | `tasmin > tasmax` | `tasmax − tasmin = −0.758 °C` |
| Rajasthan | 0 of 582 | — | — |

These are real defects in the ACCESS-CM2 NEX product — relative humidity above saturation and
a reversed daily temperature range — not pipeline errors. SPEC.md §5 froze the rule that such
days are **marked invalid, never repaired**, and §10 froze complete-365. Applied together,
**one bad day out of 365 destroys the entire cell-year.**

The cost is concentrated, because the affected cells are contiguous:

| Unit | Valid area fraction | Cells valid / total |
|---|---|---|
| Lahul and Spiti (district) | **0.605** | 27 / 38 |
| Spiti (block) | **0.550** | 13 / 21 |
| Lahul (block) | 0.665 | 17 / 23 |
| Naggar (block) | 0.717 | 5 / 7 |
| Kullu (district) | 0.929 | 16 / 18 |
| Kasargod (block) | 0.948 | 3 / 4 |

Only **5 of 581 blocks** and **3 of 75 districts** fall below 0.99. Nothing was filled
spatially, no unit became NaN, and no count became zero — but a Lahul and Spiti value now
rests on 60.5 % of its area, which a consumer must be told.

**This is the decision the next milestone owes**, and it is a policy question, not a coding
one: whether a `hurs` overshoot of 0.005–1.208 percentage points should cost a whole year.
The rule was frozen before the result was known and has been applied as written; changing it
now in response to the result is exactly what SPEC.md §15 forbids.

### 3.2 No coastal or land-mask support loss

Of 784 intersecting cells, **zero** fall outside the NEX land mask. Kerala's 51,380
non-finite cell-days are all ocean cells inside the bounding box but outside every polygon,
so they never reach an aggregate. Coastal support is not a problem here.

---

## 4. Elevation — a declared, bounded gap

`SiteStatic.elevation_m` drives ISA pressure, and **no local per-cell elevation source
exists**: no DEM, no `orog` field, no elevation column on the boundaries. Every cell
therefore runs at **0 m**, declared in the method signature as
`elev-sea-level-constant-no-dem` so no future elevation-aware cache can be confused with this
one.

Measured consequence, at fixed drivers (35 °C, 40 % RH, 2 m s-1, 850 W m-2):

| Elevation | ISA pressure | Liljegren WBGT |
|---|---|---|
| 0 m | 1013.25 hPa | 31.992 °C |
| 1000 m | 898.75 hPa | 31.876 °C |
| 2276 m (Shimla) | 768.09 hPa | **31.742 °C** |

So the error is bounded by about **0.25 °C** at the highest Indian elevations and is far
smaller in Kerala and most of Rajasthan. It is real, it is one-signed, and it is small
relative to the reconstruction errors milestones 1–3 already reported — but it is a genuine
missing input, and acquiring an elevation field is a precondition for any national work.

---

## 5. Results

`district_values.csv`, `block_values.csv`. W1, ACCESS-CM2, 2005.

**Annual mean of daily maximum outdoor WBGT** (never "daily mean WBGT"), district range:

| State | Annual mean (°C) | `days_ge_32` range |
|---|---|---|
| Himachal Pradesh | 8.25 – 26.03 | 0.00 – 55.39 |
| Kerala | 27.53 – 30.86 | 1.17 – 96.34 |
| Rajasthan | 25.73 – 28.26 | 5.38 – 137.65 |

Kerala's internal ordering is physically sensible without having been targeted: the highland
districts Idukki (27.53 °C) and Wayanad (27.98 °C) are coldest, the coastal lowlands
Alappuzha (30.86 °C) and Ernakulam (30.81 °C) hottest. Himachal's 8.25 °C floor is high
Himalaya. Per SPEC.md §11 the six reference sites' observed annual-mean range is **not**
used as a validity bound, so this range is reported, not gated.

Every threshold count is labelled **"area-weighted mean annual cell exceedance days"** and is
fractional. It is *not* the number of days every location in the polygon exceeded the
threshold, *not* the number of days any location did, and *not* the number of days the
polygon-average WBGT did.

### 5.1 Admin correctness — 36 of 36 checks pass

`admin_checks.csv`. Unique canonical keys; `days_ge_32 <= days_ge_30 <= days_ge_28` on every
row; all counts within [0, 365]; no NaN value paired with a zero count; unsupported units
retained as NaN with a reason; every weighted mean inside the range of its contributing
valid cells.

### 5.2 Hierarchy reconciles exactly

`district_vs_block_rollup.csv`. Re-aggregating blocks to districts, weighted by **valid
intersected area** (not whole-block area), reproduces the direct district aggregation to a
maximum absolute difference of **1.4e-14 °C** across all 75 districts — floating-point zero.

That is a real structural result, not a tautology: it holds only because the blocks tile
their districts without gaps or overlaps over the climate grid. Had the boundary set been
inconsistent, the difference would have been visible. Weighting by whole-block area instead
would have been wrong, and the test suite asserts the two give different answers.

---

## 6. Ranking sensitivity — diagnostic only

Both comparisons were predeclared in SPEC.md §13 before execution.

### 6.1 Method sensitivity: W1 vs C1 (all three states)

`ranking_sensitivity.csv`. W1 is **0.395 °C cooler** than C1 on average (range −0.506 to
−0.181 °C).

| Level | Field | n | Spearman | Mean shift | Max shift | Largest change |
|---|---|---|---:|---:|---:|---|
| district | annual mean | 75 | 0.999 | 0.64 | 5 | Idukki 27.525 → 27.799 |
| district | `days_ge_28` | 75 | 0.981 | 2.37 | 21 | Wayanad 174.9 → 201.8 |
| district | `days_ge_32` | 75 | 0.997 | 1.04 | 8 | Thrissur 84.6 → 111.8 |
| block | annual mean | 581 | 0.999 | 2.78 | 51 | Idukki\|\|Azhutha 27.555 → 27.798 |
| block | `days_ge_28` | 581 | 0.989 | 14.68 | **226** | Wayanad\|\|Mananthavady 193.1 → 221.0 |
| block | `days_ge_32` | 581 | 0.994 | 12.78 | 74 | Thrissur\|\|Kodakara 91.6 → 125.5 |

The level field is highly stable; the **counts are not**. A block can move 226 places on
`days_ge_28` purely from the within-day wind shape. Since W1's wind shape is a declared
assumption rather than an established reconstruction, block-level count rankings must be
treated as method-dependent.

### 6.2 Model sensitivity: ACCESS-CM2 vs MRI-ESM2-0 (Himachal Pradesh only)

`model_sensitivity.csv`. MRI-ESM2-0 is **0.270 °C cooler** (range −0.468 to −0.173 °C).

| Level | Field | n | Spearman | Mean shift | Max shift |
|---|---|---:|---:|---:|---:|
| district | annual mean | 12 | 1.000 | 0.00 | 0 |
| district | `days_ge_32` | 12 | 0.928 | 0.67 | 3 |
| block | annual mean | 81 | 1.000 | 0.40 | 2 |
| block | `days_ge_32` | 81 | 0.978 | 1.90 | **26.5** |

The two are reported separately and are **not** merged, because they answer different
questions. Their magnitudes are also not directly comparable: 581 block ranks versus 81.

Neither result is evidence of accuracy. **Rank stability is not rank accuracy**, and
agreement between two reconstructions validates neither against reality. Spatial ranking
accuracy remains unestablished and is the central open scientific question.

---

## 7. What still blocks publication

Carried in every manifest and repeated here:

- Diagnostic-only; uncorrected NEX inputs; no bias correction applied.
- Six-site reconstruction evidence does not establish national accuracy.
- Remaining NEX distribution and count errors are unresolved — milestone 3 measured NEX
  under-counting `>= 32 °C` by 78–116 d/yr at Kochi against a 189 d/yr reference.
- W1 may benefit from compensating reconstruction errors; its wind shape is a declared
  assumption, not an established physical reconstruction.
- `>= 32 °C` outcomes include reference-sensitive results (milestone 3: all four of B's
  count failures were reference-sensitive).
- Rare-event regimes remain insufficiently evaluated.
- **Spatial ranking accuracy is not established** — and §6.1 shows block count ranks are
  method-dependent.
- Daily time-boundary semantics remain **inferred**; no NEX file publishes `time_bnds`.
- Cell elevation is a sea-level constant; no DEM available (§4).
- **All thresholds, including `>= 28` and `>= 30 °C`, remain diagnostic-only on NEX.** An
  ERA5 reconstruction pass does not promote them.

---

## 8. Measured cost

| Stage | Measurement |
|---|---|
| Total wall (all three states, both candidates) | **331.0 s** |
| Peak process RSS | **0.78 GB** (budget 4 GB) |
| Artifacts (scratch + evidence) | **1.1 MB** (budget 5 GB) |
| Workers | **1** throughout, while shade was active |
| Solver rate | **0.13 s/cell-year** (Kerala, Rajasthan), 0.21 (Himachal) |
| Per-state fixed setup | 36–45 s, dominated by reading the national block GeoJSON |
| Input load per state | ~0.95 s |

`timings.csv`. Every budget ceiling in SPEC.md §12 was respected with a wide margin; no
checkpoint or continuation was needed.

**No national extrapolation is offered.** The setup cost is per-state and boundary-dominated,
the solver cost scales with intersecting cells, and both would need measuring on a larger
state before any national figure could be quoted. Milestone 1–3's six-point runtimes are not
used for this either.

---

## 9. Isolation — shade and production untouched

- Tracked-file changes across the whole milestone: **`MANIFEST.md` (+2), `tools/README.md`
  (+1)**. Nothing else.
- `git status` over `india_resilience_tool/`, all three predecessor evidence directories and
  the shade release directory: **no modifications**.
- No production module imports this tool; this tool imports production helpers read-only and
  changed none of them.
- The write guard resolves symlinks and refuses `docs/diagnostics/wbgt_outdoor_selection`,
  `docs/diagnostics/wbgt_shade_release`, `irt_data/processed`, `processed_optimised`,
  `scratch/wbgt_shade_national` and `irt_data` — demonstrated live and asserted in tests.
- No existing process was stopped, restarted or signalled. One worker throughout.
- `tests/test_wbgt_shade_release.py` still green.

---

## 10. Tests

`tests/test_wbgt_outdoor_pilot.py` — **70 passed, 1 skipped** (the symlink case needs a
privilege not available here and skips cleanly).

Affected suites together: **265 passed, 1 skipped** across
`test_wbgt_outdoor_pilot`, `test_wbgt_outdoor_feasibility`, `test_wbgt_outdoor_selection`,
`test_wbgt_outdoor_humidity` and `test_wbgt_shade_release`. No broad unrelated suite was run.

Coverage follows SPEC.md §13: protected-path refusals including symlink destinations; unit,
calendar, duplicate-date, missing-date and grid-mismatch rejection; W1 and C1 parity;
chunk and boundary-day parity; complete-year and all-NaN behaviour; threshold ordering and
bounds; grid-first aggregation on a heterogeneous fixture; hand-checked area weighting;
unsupported-unit retention; resume invalidation on every signature element; resume round-trip
without duplication; and the ranking helpers.

Two tests exist specifically to stop this pilot degrading into something cheaper:
`test_grid_first_differs_from_averaging_weather_first` asserts cell-first and polygon-first
genuinely disagree, and `test_w1_coefficients_are_imported_never_restated` asserts W1's slope
is never re-declared here.

---

## 11. Exact next action

Decide the completeness policy for marginally invalid NEX cell-days (§3.1) — specifically
whether `hurs` in (100, 102] % should invalidate a day, a year, or be clamped with a recorded
flag. That single decision governs 11 of the 12 lost cell-years and is the only thing between
this pilot and `ENGINEERING PASS`.

Then acquire a per-cell elevation field (§4) before any further geographic expansion.

Not next: more states, more years, or another method candidate. None of those is blocked by
anything this pilot measured.

---

## 12. Files

| File | Contents |
|---|---|
| `SPEC.md` | Frozen specification, written before any result |
| `district_values.csv` | 150 rows — 75 districts × W1/C1 |
| `block_values.csv` | 1162 rows — 581 blocks × W1/C1 |
| `cell_invalidity.csv` | Every lost cell-year, with cause and consequence |
| `coverage.csv` | Per-state cell-day validity and load cost |
| `input_checks.csv` | All 18 variable-years verified |
| `parity_checks.csv` | The seven §9 checks |
| `admin_checks.csv` | 36 structural checks |
| `district_vs_block_rollup.csv` | Hierarchy reconciliation |
| `ranking_sensitivity.csv` | W1 vs C1, method sensitivity |
| `model_sensitivity.csv` | ACCESS-CM2 vs MRI-ESM2-0, Himachal only |
| `timings.csv` | Measured per-state cost |
| `run_manifest.json` | Git snapshot, versions, command, signatures, budget, limitations |

Bulk caches live under `scratch/wbgt_outdoor_pilot/` and are not part of the evidence.

Root `README.md` needs **no** update: no metric slug is registered, no deployed methodology
changes, no new operator command enters the documented workflow, and nothing in the dashboard
setup or run instructions is affected. `tools/README.md` and `MANIFEST.md` carry the runner.
`docs/HANDOFF.md` and `docs/BACKLOG.md` are unchanged under their authorization rules.
