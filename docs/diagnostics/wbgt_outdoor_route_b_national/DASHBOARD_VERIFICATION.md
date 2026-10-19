# Staged dashboard verification — two-model Route B bundle

Run against the isolated staged release, not production.

* processed tree `C:\irt_rb_s2` (ACCESS-CM2 + ACCESS-ESM1-5, 595,155 files, 386 s)
* runtime bundle `C:\irt_rb_bundle2` (357,496 tasks, 6,971 s; geometry and context
  copied from production so the shared artifacts are present)
* scripts `scratch/wbgt_route_b_national/verify_dashboard.py`, `verify_extra.py`,
  `verify_gap.py`; raw results in `verify_dashboard.json`

## 1. Strict parity audit

`tools.optimized.audit_processed_optimised_parity --strict` over the five slugs:

| Run | Issues | Exit |
|---|---|---|
| After first build | 30 (20 yearly-ensemble, 10 state-values) | 1 |
| After the ensemble empty-state fix (CHG-0671) | 10 (state-values warnings) | 1 |
| After `build_state_values` | 23 (context/geometry, skipped at build) | 1 |
| After copying shared context and geometry | **0** | **0** |

The twenty `yearly-models` errors reported earlier are gone. Nothing was suppressed
and no check was weakened; the artifacts the audit asks for now exist.

## 2. The app reads the staged location

`resolve_optimized_bundle_root()` returns `C:\irt_rb_bundle2` under
`IRT_PROCESSED_OPTIMISED_ROOT`, and all five metric roots satisfy
`is_optimized_metric_root`. **39 of 39 reader checks pass.**

## 3. The four state cases

| State | Case | District master | Block master | Exact zeros |
|---|---|---|---|---|
| Telangana | normally covered | 33 rows, 231/231 finite | 588 rows, 4116/4116 finite | **0** |
| Gujarat | partially excluded coastal | 33 rows, 203/231 finite | 251 rows, 1631/1757 finite | **0** |
| Andaman & Nicobar | all units ineligible | 3 rows, **0 finite** | 9 rows, **0 finite** | **0** |
| Lakshadweep | all units ineligible | 1 row, **0 finite** | 10 rows, **0 finite** | **0** |

Every excluded unit stays in the roster carrying a missing value. **No value anywhere
is an exact zero**, so a blank can never be read as a measured zero.

Gujarat's four blank districts are Anand, Devbhumi Dwarka, Gir Somnath and Surat;
the other 29 carry values. An eligible unit in a partially excluded state is
unaffected: Ahmadabad returns 21 historical and 61 scenario years for both models.

## 4. Trends truncate honestly

| Unit | historical | ssp245 | ssp585 |
|---|---|---|---|
| Ahmadabad | 1990–2010 | 2020–2080 | 2020–2080 |
| Surat (excluded) | none | none | none |
| Leh Ladakh (thinned) | 1990–2010 | **2020–2059** | **2020–2059** |

Leh Ladakh is the most thinned unit in the roster. Its loss is **contiguous at the
tail** — whole QDM application windows drop from 2060–2080 backwards — so the series
ends early rather than carrying an interior hole, and nothing is interpolated across
a gap. Within ssp245, ACCESS-CM2 covers 2020–2039 and ACCESS-ESM1-5 covers 2020–2059,
so 2040–2059 rests on one model. The published ensemble CSV records that as
`n_models=1` for those years.

## 5. Empty states do not crash the application

Streamlit started headless against the staged bundle with
`IRT_PILOT_STATE="Andaman & Nicobar Islands"` — the all-missing territory as the
opening state. The server answered **HTTP 200** on `/` and `/healthz` and its log
carried **no traceback, error or exception** over 90 seconds.

This is evidence that the empty-state path does not raise. It is not a substitute
for clicking through every view: Streamlit renders over a websocket, and only a
scripted browser session would exercise each widget.

## 6. One-model summaries are not labelled as a full roster

The per-model reader returns exactly `["ACCESS-CM2", "ACCESS-ESM1-5"]` for every
unit tested — the models actually staged, never the configured 18. State headline
values carry `n_units`, which is **0** for both island territories against a `NaN`
value.

## 7. The contributing-model count — closed by CHG-0673

**The gap, as found.** `_normalize_legacy_ensemble_df` kept only `year`, `mean`
and `median`, so the `n_models` column written beside each ensemble series never
reached the runtime bundle, and the optimised master carried only `mean` and
`median` per period. That was the **bundle-wide contract**, identical for the
incumbent `swbgt_empirical_*` metrics, not a Route B regression — but it matters
more here, because this roster genuinely thins and a viewer could not tell a
2-model year from an 18-model year from the bundle alone.

**Closed on 2026-10-07, user-approved, with no shared-schema change.** The count
is declared per metric through `retained_master_statistics`, default empty. See
`PUBLICATION_PLAN.md` §7 for the definition at each tier and the two defects the
inspection turned up (an area-weighted state count, and an empty-ensemble frame
missing `median`).

**Verified end to end against the staged bundle** by
`scratch/wbgt_route_b_national/verify_model_counts.py`, 7/7 PASS:

| Check | Observed |
|---|---|
| covered state: counts sit beside values | Telangana, 33 districts, all counts `2`, matching the 2 staged models |
| island: zero contributors, no zero values | A&N, 3 districts, every value missing, every count `0`, no zeros published |
| tooltip: count rendered for the displayed slice | `['2','2','2']`, advertised among 6 tooltip fields |
| island tooltip: no data, not zero | value rows `—`, count rows `—` |
| ribbon: Statistic dropdown unchanged | `['mean','median']` |
| master schema parser ignores the count | 14 schema items, `stats=['mean','median']` |
| existing metric unaffected | `swbgt_empirical_annual_mean` district master: no `__n_models` columns |

Measured in the rebuilt staged bundle: the master carries 7 count columns (one
historical period plus three periods × two scenarios), `int8`; the yearly
ensemble carries one per unit-year-scenario; the legitimately empty island
ensemble now carries the full populated column set,
`['year','mean','median','n_models','scenario','<level>_key']`.

Note the staged counts read `2` because only 2 of the 18 models were fitted when
this was verified. **A count of 2 is the correct report for a 2-model staged
bundle, not a defect** — and it is exactly the thinning the column exists to make
visible. The final release must show 18 where all models contribute, which is a
publication-time check, not a staging one.
