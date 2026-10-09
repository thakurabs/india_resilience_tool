# Route B outdoor WBGT — production publication plan and rollback

Prepared from a measured dry-run against the production bundle. Nothing has been
written to production. This document is the approval gate.

## 1. Exact affected production paths

`tools.optimized.build_processed_optimised --dry-run` with
`IRT_PROCESSED_ROOT=C:\irt_rb_s2` and
`IRT_PROCESSED_OPTIMISED_ROOT=D:\projects\irt_data\processed_optimised`:

```
write_target  1081
delete_target    0
```

| Path | Files | Shared? |
|---|---|---|
| `metrics\wbgt_outdoor_route_b_annual_mean\{masters,yearly_models,yearly_ensemble}` | 216 | new |
| `metrics\wbgt_outdoor_route_b_annual_max\{…}` | 216 | new |
| `metrics\wbgt_outdoor_route_b_days_ge_28\{…}` | 216 | new |
| `metrics\wbgt_outdoor_route_b_days_ge_30\{…}` | 216 | new |
| `metrics\wbgt_outdoor_route_b_days_ge_32\{…}` | 216 | new |
| `bundle_manifest.json` | 1 | **shared** |

72 files per tier = 36 states × 2 admin levels. A following `build_state_values`
run adds 10 more files, all inside the five new metric directories
(`state_values/admin/{district,block}/all_states.parquet`).

**Exactly one shared file is touched, and nothing is deleted.** Every other write
lands inside a directory that does not exist in production today.

## 2. What the shared manifest rewrite does

`_bundle_inventory_summaries` enumerates **every metric directory on disk**, not
only the selected ones, so the rewritten manifest keeps all existing entries and
gains five. The payload also recomputes `glance_view_model`, `frozen_rulers` and
`shade_method_signature`.

`_shade_release_signature` reads only `SHADE_SLUGS` — the four `wbgt_shade_stull_*`
metrics. The Route B slugs are not in that set, so the shade signature cannot move.
`colour_scale.json` is written only by `tools/pipeline/fit_frozen_ruler.py` and is
not touched by this build.

Composite scoring is unchanged: neither the incumbent WBGT slugs nor the new slugs
appear in `bundle_weights.py`, `proposal_bundles.py` or `dashboard_bundles.py`.

## 3. The incumbent metrics are retained

`swbgt_empirical_*` and `wbgt_shade_stull_*` keep their own directories, model
rosters, spatial coverage and years. No path under either is a write target. The
registry carries the new slugs as `source_type="external"` display-only metrics
with no `compute` key, so they cannot enter a composite without a separate change.

## 4. Pre-publication checklist

| Gate | State |
|---|---|
| All 18 supported models fitted, assembled and validated | **in progress** — campaign running |
| Release-gate validation PASS per model | 2 of 18 so far |
| Strict parity audit on the staged bundle | **PASS, 0 issues, exit 0** (2-model) |
| Dashboard reader checks, four state cases | **39 of 39 PASS** |
| Empty-state territories do not crash the app | **PASS** (HTTP 200, no traceback) |
| Island exclusions explained and documented | **done** |
| Exact affected paths and dry-run | **done, this document** |
| Rollback established | **done, below** |
| Final user approval | **not requested yet** |

## 5. Rollback

Before publishing:

```bash
cp "D:/projects/irt_data/processed_optimised/bundle_manifest.json" \
   "D:/projects/irt_data/processed_optimised/bundle_manifest.json.bak-preRouteB-<timestamp>"
```

To roll back completely:

1. Restore `bundle_manifest.json` from that copy.
2. Remove the five `metrics/wbgt_outdoor_route_b_*` directories.

No other production artifact changes, so no further restoration is needed. The
staged bundle at `C:\irt_rb_bundle2` and the staged processed tree at `C:\irt_rb_s2`
remain available for re-verification after a rollback.

The code changes (registry entries, builder empty-state contract, publisher
ensemble tier) are reverted through git independently of the data.

## 6. Open decisions

* **`n_models` in the runtime bundle — resolved, CHG-0673.** Carried through, with
  no shared-schema change. See §7.
* **`--overwrite` on republication.** The first publication needs no overwrite: all
  1,080 metric files are new. A later republication with more models would rewrite
  them in place and therefore needs explicit confirmation under `tools/CLAUDE.md`.

## 7. Application and configuration changes (CHG-0673)

Approved under instruction 4: carry actual contributing-model counts into the
runtime outputs and make them visible. The inspection it asked for found one
concrete incompatibility and one latent defect, both avoided rather than accepted.

### The incompatibility, and what was done instead

The obvious route — adding `n_models` to the metric's `supported_statistics` —
does not work. `india_resilience_tool/app/ribbon.py:943` builds the user-facing
**Statistic** dropdown directly from that list, so the count would have appeared
as a selectable statistic to map. It is provenance about a value, not a value.

The compatible alternative is a separate declaration,
`retained_master_statistics`, which the builder consults for extra
`__<stat>` columns to retain and nothing else reads. It defaults to empty, so:

* no existing metric's published columns change;
* no existing metric's dropdown changes;
* `parse_master_schema_obj` already parses only `mean|median`, so the extra
  column is invisible to metric/scenario/period discovery — verified, not assumed;
* `master_loader` already lists `n_models` among the suffixes it normalises, so
  the reader needed no change at all.

### The latent defect found by the same inspection

`tools/optimized/build_state_values.py` skips only `models` and
`values_per_model`, so it would have area-weighted the per-unit counts and
published a state headline of "17.3 models". Counts are now aggregated as the
**minimum over the units that published a value** for the same slice, which stays
an integer and reads as "every unit behind this headline rests on at least N
models". A state with no published value reports a missing count, never 0.

The same inspection found `_empty_yearly_ensemble_frame` omitted `median`, which
both populated load paths produce. A legitimately empty state therefore answered a
different column set from its populated siblings, and the required-column audit
does not check for that. Fixed in the same change.

### Definition of the count

| Tier | Count is | Aggregation level |
|---|---|---|
| master | models with **every year** of the period | unit × scenario × period |
| yearly ensemble | models with a finite value **that year** | unit × scenario × year |
| state values | **minimum** over contributing units | state × scenario × period |

Never the nominal roster size, and never an average of annual counts. Zero
contributors renders as "—" in the tooltip rather than "0": a unit excluded for
insufficient reference support has every model available and is simply ineligible,
so a literal 0 would assert something false about the roster.

### Affected non-data paths

| Path | Change |
|---|---|
| `india_resilience_tool/config/metrics_registry.py` | `retained_master_statistics` field; declared on the five Route B slugs; the stale "19 models" claim in all five descriptions corrected to 18 |
| `tools/optimized/build_processed_optimised.py` | retain declared stats in masters; carry/compute `n_models` in both ensemble load paths; empty-ensemble schema parity; manifest `stats_contract` corrected |
| `tools/optimized/build_state_values.py` | minimum aggregation for count stats |
| `india_resilience_tool/analysis/map_enrichment.py` | `_tooltip_n_models`, opt-in |
| `india_resilience_tool/viz/folium_featurecollection.py` | tooltip row, opt-in via `has_model_count` |
| `india_resilience_tool/app/map_layer_runtime.py` | passes the flag from the patched frame |
| `india_resilience_tool/app/map_pipeline.py` | derives the count column from the selected metric column |
| `tests/test_contributing_model_counts.py` | new, 32 tests |

No colour-scale asset changes. The five slugs already resolve to the `irt:heat`
ramp through their registered **Heat Stress** domain, identically to the eight
incumbent WBGT metrics — verified by calling `resolve_metric_cmap_name`. The
robust p2–p98 scale for non-composite metrics needs no per-metric configuration,
and `colour_scale.json` is written only by `fit_frozen_ruler.py`, which is not run.

Rollback for these is `git checkout` of the listed files; they are independent of
the published data, which rolls back as described in §5.

## 8. The publication runbook

> **Superseded in three places by §9 and §10, kept for the measurements it
> records.** Step 1's roster check is now by name, not by count (a count of 18
> could be satisfied by the wrong eighteen). Step 2 now hashes contents by
> default, and `--fast-inventory` is refused with `--run`. The rollback paragraph
> at the end was wrong about the application files: most of them are untracked, so
> `git checkout` cannot restore them — see §10.4.

`scratch/wbgt_route_b_national/publish_production.py` executes the publication as
one audited sequence, read-only unless `--run` is passed:

1. Refuse unless every assembled model carries a `PASS` validation report **and**
   the count equals the approved roster. Verified: with 4 models assembled it
   prints them and exits with *"roster is 4 models, not the approved 18; refusing
   to publish a partial ensemble"*.
2. Fingerprint every production file **outside** the five new metric directories —
   18,887 files, measured — immediately before writing, by size and mtime. Not a
   content hash: the bundle is 8.7 GB and this runs twice, so hashing would read
   17 GB and contend with the fitting campaign for the same disk. The one shared
   file that is *expected* to change is sha256'd before and after.
3. Back up `bundle_manifest.json` and both inventories to
   `scratch/wbgt_route_b_national/production_backup/<timestamp>/`.
4. Build the five metric directories with `--skip-geometry --skip-context`, so the
   shared geometry and context artifacts are not rewritten at all.
5. Precompute state values for those five metrics only.
6. Re-fingerprint and report every unexpected modification, disappearance or new
   non-metric file, exiting non-zero if any exist, with the restore path named.
7. Run the strict parity audit.

Step 6 is the point of it: the dry-run measured exactly one shared file changing,
and this verifies that measurement still held at publication time rather than
assuming it.

Rollback, in order: restore `bundle_manifest.json` from the backup directory, then
delete the five `metrics/wbgt_outdoor_route_b_*` directories. Both are reversible
without touching any other metric. The application half is **not** a `git checkout`
— most of those files are untracked and git holds no earlier version of them; it
is a copy-back from the snapshot the script takes, and §10.4 separates restoring
the working tree from removing the integration.

## 9. Release gates and the automatic path (CHG-0676)

### 9.1 Execution order

Exactly one step writes to production, and it is last. Everything before it runs
against staged roots.

| # | Stage | Writes to | Gate artifact |
|---|---|---|---|
| A | 18 approved models fitted, assembled, validated | `scratch/.../assembly/` | `campaign_gate_verdict.json` |
| B | Publisher builds the staged processed tree | `--processed-root` (staged) | tree present, per metric |
| C | Staged optimized bundle + state values | `IRT_PROCESSED_OPTIMISED_ROOT` (staged) | build log, task counts |
| D | Required tests | — | `gate_tests.json` |
| E | Strict parity audit on the staged bundle | — | `gate_staged_audit.json` |
| F | Staged dashboard verification | — | `gate_staged_dashboard.json` |
| G | Back up production shared + application artifacts | `production_backup/<ts>/` | `ROLLBACK.md`, `application_snapshot.json` |
| H | **Additive publication** | **production `processed_optimised`** | `inventory_before/after.json` |
| I | Production dashboard smoke check | — | `gate_production_dashboard.json` |

"Publisher" is `tools.pipeline.publish_route_b_outdoor`, which names the staged
processed tree through a required `--processed-root` and has no production path at
all. It cannot write production whether or not the gates pass. The orchestrator
that can is `scratch/wbgt_route_b_national/publish_production.py`, and it writes
nothing before gates A and D–F pass.

### 9.2 The gate, and why process exit is not one

The campaign runs with `--continue-on-failure`, so it exits 0 after skipping a
broken model; a killed process also "finishes". `campaign_gate.py` is therefore
the only release signal, and `publish_production.py` re-runs it rather than
trusting a stored verdict. Six independent gates:

| Gate | Requires |
|---|---|
| G1 roster | every approved model assembled, by name, and no unapproved extras |
| G2 stages | all seven campaign stages recorded `PASS` per model |
| G3 validation | `validation_report.json` verdict `PASS` with empty `failed_checks` |
| G4 method | identical `method`, `aggregation`, `reference`, `source_lock` and both `code_sha256` entries across the roster |
| G5 signature | identical `support.signature_sha256`; and a **distinct** run `signature_sha256` per model |
| G6 artifacts | every file matches the sha256 its own run manifest recorded |

G5 is split deliberately. The support signature covers the shared inputs
(boundaries, grid, elevation, reference) and must agree, or the models were not
fitted against the same data. The top-level run signature folds in each model's
own driver `source_hash`, so it is per-model by construction; requiring it to
agree would be wrong, and requiring it to be *distinct* catches a copied or
relabelled run. The gate's first version got this backwards and was corrected
against the seven real manifests.

Every failure mode was demonstrated, not assumed, against a synthetic 18-model
roster: a stage marked `FAIL`, a validation verdict `FAIL`, a drifted
`code_sha256`, a truncated `yearly_metrics.csv`, and two models sharing one run
signature. Each produced the expected named refusal and a non-zero exit.

`--expected-models` is gone. The roster is compared to `ENSEMBLE_MODELS` by name;
a count could be satisfied by the wrong eighteen. `--override-roster` exists for a
human and is reachable from no automatic path.

### 9.3 Preservation: hashes, not timestamps

`_inventory` now takes sha256 contents by default, and `--fast-inventory` (size +
mtime) is **refused together with `--run`**. Size and mtime detect a write but
cannot prove contents unchanged, because a same-length rewrite that restores the
mtime would pass. The content hash costs two full reads of the ~8.7 GB bundle,
which is acceptable only once fitting has finished — which is exactly when this
runs.

Concurrent writers: checked at 2026-10-07 15:00 UTC. No `build_processed_optimised`,
`build_state_values`, `prepare_dashboard`, `compute_indices` or Streamlit process
was running, and no file under production `processed_optimised` had been modified
in the preceding 24 hours. The campaign writes only under `scratch/` and `C:\`.
`bundle_manifest.json` is the single shared file the release rewrites; it is hashed
and copied before the build and re-hashed after. If a legitimate concurrent update
appears between the two inventories, the run reports it by path and exits non-zero
rather than deciding on its own whether it was damage.

### 9.4 Recovery, with the artifact each path reads

Detection and recovery are separate. `inventory_before.json` proves what changed;
these are what put it back.

| What | Restore path | Reads from |
|---|---|---|
| The five metric directories | delete them | nothing — they did not exist before; `inventory_before.json` contains no path under them |
| `bundle_manifest.json` | copy back | `production_backup/<ts>/bundle_manifest.json`, a byte copy with a recorded sha256 |
| Application + config changes | copy back | `production_backup/<ts>/application/`, plus `application_snapshot.json` with git HEAD and per-file sha256 |
| Existing metric data | not applicable | nothing in this release deletes or rewrites it |

The application snapshot closes a real gap: **nothing is committed**, so several
of these files are untracked and git could not have restored them. A rollback that
said "revert the code" would have had nothing to revert to. `verify_rollback.py`
re-hashes every snapshotted file afterwards, so a restore is confirmed rather than
assumed. If a modification appears outside the five new directories, this backup
cannot repair it — it holds only the manifest and the application files — so the
procedure says to stop rather than improvise.

### 9.5 Dashboard evidence

`verify_release.py` drives the application's own loaders and the pure builders
behind each surface, for all five slugs × both administrative levels × four
coverage cases, and emits a `verdict` plus a non-zero exit. It checks registry
label/units/statistics, the colour scale through `resolve_metric_cmap_name`, the
master's value columns and per-slice `__n_models`, the three provenance columns and
the rendered "No data" message, the rankings table and its download bytes, the
legend range and labels, the yearly ensemble series with its counts, the per-model
selection, and the state summaries with integral counts. It also checks two
incumbent metrics still load — resolved against **production**, since a
metric-scoped staged bundle does not contain them.

It does not render Streamlit. `build_map_and_rankings` takes about forty
widget-coupled arguments and calls `st.stop()`, so driving it headlessly would
test a harness rather than the app. What is verified is every input contract those
widgets consume. The render itself is checked by hand once against the staged
bundle and once against production, and recorded as such.

The same script is the production smoke check, via
`verify_release_production.bat`, which leaves `IRT_PROCESSED_OPTIMISED_ROOT` unset
so the app resolves production exactly as a user's launch does, and asserts the
resolved root — a check that silently read the staged bundle would prove nothing.

### 9.6 A gap this found: the count never reached the yearly chart (CHG-0675)

`load_district_yearly` and its block twin requested a fixed column list
(`key, scenario, year, mean, median`), so `n_models` was dropped before the chart
could see it. The count reached the bundle (CHG-0673) and the map tooltip, but not
the yearly view the directive named. Both loaders now pass
`optional_columns=["n_models"]`, resolved against the Parquet schema so a bundle
written before the field existed reads byte-identically and never forces a
full-table re-read. Verified both ways on real bundles: the post-CHG-0673 metric
returns a count range, and a pre-CHG-0673 metric returns the same frame it always
did.

### 9.7 End-to-end ETA

From 2026-10-07 15:09 UTC. Measured figures are marked; the rest are scaled from a
measurement, with the scaling stated.

| Stage | Estimate | Basis |
|---|---|---|
| Remaining campaign | **~21.8 h** | measured 2.05 h/model over five consecutive models (start-to-start 2.10, 2.12, 2.07, 1.95, 1.99 h); GFDL-CM4 ~1.2 h to go, then 10 × 2.05 h |
| Publisher → staged processed tree | ~1–2 h | 18 models over the same unit roster; to be measured |
| Staged bundle + state values | **~11.7 h** | measured 6,970.8 s for 5 metrics at 2 models; the yearly-models tier is 61.5% and scales linearly with roster, so ×9 on that tier |
| Full test suite | ~0.5–0.8 h | uncontended; the 2h48m figure was under contention with fitting and is not the clean cost |
| Strict parity audit | ~0.2 h | 5 metrics, read-only |
| Staged dashboard verification | <0.1 h | `verify_release.py` runs in seconds |
| Backup + content hashing | ~0.5–1.0 h | two sha256 passes over ~8.7 GB on D: |
| Production build + state values | ~2–12 h | same tiers as the staged build; D: writes measured at 36 files/s against 636 on C:, so this is the widest band and will be measured on the staged run first |
| Production dashboard verification | <0.1 h | same script |

**Time to the five metrics being selectable in production: roughly 39–50 hours**,
of which ~22 h is fitting. The ~22 h campaign figure alone is not the time to
dashboard availability, and should not be quoted as one. The band is dominated by
two unmeasured write paths, both of which get measured before they are relied on.

---

## 10. Review findings closed before publication (CHG-0677)

A read-only review of §9 raised three findings. All three were real; this section
records what each one changed, and two further defects the work on them exposed.

### 10.1 Evidence is now bound to the release candidate

A verdict is evidence about the thing that was examined. `gate_tests.json` saying
`PASS` meant nothing about *which* code, fits or bundle it passed against: refit a
model, edit a release file, or rebuild the staged bundle after that file was
written, and it still reads `PASS` while describing something else.

`release_candidate.py` now computes the candidate's identity as four separately
digested components, so a mismatch names what drifted rather than only that
something did:

| Component | Contents | What a change means |
|---|---|---|
| `code` | sha256 of 17 files: the 9 application/builder files the release changes, plus the 8 Route B fitting modules | the code that produces or renders the metrics is not the code that was tested |
| `roster` | the approved 18 names, from `ENSEMBLE_MODELS` | the ensemble is not the approved one |
| `campaign` | per model, the run signature and every recorded artifact hash | a model was refitted, or an artifact changed |
| `bundle` | per metric directory: file count, total bytes, and a digest over every `(path, size, mtime_ns)` | this is a different build of the staged bundle |

Evidence is only ever written by `gate_evidence.py`, which stamps the fingerprint
at the moment the gate ran. `publish_production.py` recomputes it and compares
component by component. Demonstrated, in both directions:

| Evidence | Result |
|---|---|
| matching candidate | accepted, with the recording time printed |
| `code` digest differs | `evidence describes a different release candidate -- code changed since this evidence was recorded (deadbeefdead -> afaa5e5002c1); re-run this gate` |
| `campaign` digest differs (a refit) | same form, naming `campaign` |
| `bundle` digest differs (a rebuild) | same form, naming `bundle` |
| no fingerprint at all | `carries no release-candidate fingerprint at all` |
| `verdict: FAIL` | `verdict is 'FAIL', not PASS` |
| tests gate, recorded with `bundle: null` | accepted — see below |

The one asymmetry is deliberate. The test suite runs before the staged bundle
exists, so its evidence records `bundle: null`, which is a claim about code and
fits only; that is accepted against any bundle. The reverse — evidence asserting a
bundle when the candidate has none — is a mismatch and is rejected.

What the `bundle` component does **not** claim: it detects a different build,
because every rebuild moves the files' mtimes. It does not prove the bytes inside
the parquet files are unchanged, which would mean hashing the whole bundle twice.
That guarantee comes from the strict parity audit, which reads the data itself and
is one of the gates this binding protects.

`gate_evidence.py` also closes the gap §9 left open: the three evidence files had
no producer at all, so `--run` would have refused for want of files nothing wrote.
First run of the tests gate: **126 passed in 14.65 s**, evidence written and bound.

### 10.2 Target directories are checked directly, not inferred

The preservation inventory deliberately *excludes* the five new metric
directories, so it can say nothing about them — "absent from the inventory" is how
every one of them looks whether or not it exists on disk. Inferring absence from it
would be a reading error.

`_check_targets` now stat-s each target path before anything is written, prints
what it found, and records it in the publication journal. An existing directory
means this is a replacement, not the additive publication that was approved, so
the script stops. Checked against production: all five absent, publication is
genuinely additive. Checked against a bundle where one target was populated: the
one slug reported `EXISTS with 1 files` and the run refused.

### 10.3 Partial publication has a defined retry

`publication_state.json` now records every phase as it starts and as it finishes,
so an interrupted run is diagnosable rather than reconstructed from file listings.
The script detects a previous attempt that never recorded completion and says so
before doing anything. `ROLLBACK.md` gains a retry section keyed to the phase it
stopped in:

| Stopped in | State of production | Action |
|---|---|---|
| before `build bundle` | nothing written | re-run unchanged |
| `build bundle` | one or more directories possibly half-written, and indistinguishable from complete by listing | delete all five, re-run |
| `build state values` | metric directories complete, some state values missing | delete all five, re-run — a metric without state values renders missing headlines rather than an error, which is the failure mode least likely to be noticed |
| after `inventory after` | complete release, audit outstanding | re-run the audit alone, do not republish |

A retry recomputes the candidate and re-checks the evidence against it, so a retry
after any code change or refit refuses until the gates are re-run. That is the
intended behaviour.

### 10.4 `timeseries.py` is in the snapshot, and restore is separated from revert

`india_resilience_tool/analysis/timeseries.py` is now in the snapshot list. Both
the snapshot and the candidate's `code` component read that one list, defined in
`release_candidate.py`, so a file cannot be fingerprinted without being backed up
or vice versa.

The rollback document now separates two operations it previously blurred:

- **Section 3 — restore the pre-publication working tree.** Copy `application/`
  back. This returns the tree to exactly what ran the publication, which *still
  contains the outdoor integration*. This is what an undo-and-retry needs.
- **Section 5 — remove the outdoor integration.** Tracked files go back to their
  committed versions with `git checkout HEAD --`; the untracked ones are new in
  this release, git has no earlier version of them, and reverting means deleting
  them. The snapshot is then the only way back.

The snapshot records which files git tracks, computed at backup time with
`git ls-files`, so the document names the right files for each half rather than
asserting that "revert the code" is possible.

### 10.5 A defect this found: the builder deletes `parity_report.json`

Working on 10.2 and 10.3 exposed a false-alarm bug in my own preservation check. A
build that is not state-scoped calls `_invalidate_bundle_metadata`, which unlinks
**both** `bundle_manifest.json` and `parity_report.json` before regenerating the
manifest from a fresh inventory. `parity_report.json` exists in production
(1,543 bytes, 2026-09-18) and was not in `EXPECTED_SHARED_WRITES`, so publication
would have deleted a file it is entitled to delete and then reported itself as
having damaged production — a false alarm raised *after* writing, which is the
worst possible time for one. It is now an expected shared write, documented as a
report about the bundle rather than bundle data: after a rollback it is
regenerated by re-running the audit, not restored from a copy.

### 10.6 Manifest regeneration is a full rescan, not a merge

Also established while checking the above, and relevant to §10.7: the builder
deletes the manifest and rebuilds it from `_bundle_inventory_summaries`, a scan of
whatever the bundle actually contains. So the manifest can never describe a metric
that is not there. It equally will not flag one that is only half there — which is
what the five-directory delete in the retry table is for.

## 11. Promoting the staged artifacts instead of rebuilding (assessment)

The question is whether the validated staged bundle can be copied into production
rather than built a second time there. Three things had to be established.

**Portability — confirmed.** Each metric directory is 218 parquet files across
four tiers (`masters`, `yearly_ensemble`, `yearly_models`, `state_values`) and
nothing else: no JSON, no CSV, no sidecars. A scan for embedded absolute paths
found zero occurrences of the staged root name `irt_rb_bundle2`; the four `C:\`
byte matches are random sequences inside compressed parquet pages, each surrounded
by binary noise. The runtime resolves these files purely by convention —
`resolve_optimized_metric_root(slug)` is `<root>/metrics/<slug>` — so a directory
is readable wherever it is placed.

**Manifest handling — the staged manifest must never be copied.** It is
root-bound: `glance_view_model.root` holds the absolute path
`C:\irt_rb_bundle2\context\glance`, its `bundles` list is empty where production
has 15, every composite ruler's `published_slices` is empty, and `summaries` has
the 5 Route B entries where production has 100 other metrics. Copying it would
destroy production metadata. The merge is small, though, because the five entries
it would need are path-free inventory flags:

    {"slug": "...", "has_masters": true, "has_yearly_ensemble": true,
     "has_yearly_models": true, "source_type": "external"}

and because **nothing in `india_resilience_tool/` reads the manifest at all**. The
only caller of `bundle_manifest_path` outside `optimized_bundle.py` is the builder;
`summaries` and `has_masters` appear nowhere in the application. The manifest is a
build inventory, not a runtime contract. (The knowledge-graph query for this
returned an 850-node dump, so this was settled by grep for the literal keys, and
reported as the fallback.)

**Identical contents — not yet established, and this decides it.** Promotion is
only sound if a copy is indistinguishable from a build, which requires the builder
to be byte-deterministic for a given processed tree. That is not yet measured and
cannot be measured honestly while fitting competes for the same disk. The plan,
after the campaign:

1. Build **one** metric (`days_ge_32`, the smallest) into production from the
   staged processed tree, as the publication would.
2. Byte-compare its 218 files against the same metric in the staged bundle.
3. If identical, promote the remaining four by copy and re-run the strict audit
   against production. If not, rebuild all five and record what differs.

Why this is worth doing: the five staged metric directories total ~126 MB and
1,090 files at the current 2-model roster. The production rebuild's cost is
recomputation from the processed tree, not the write — so if the artifacts are
byte-identical, promotion replaces the widest band in the ETA (2–12 h) with a
file copy of a few minutes, and removes the second opportunity for the production
build to diverge from the bundle that was actually audited and verified. The
18-model staged bundle will be larger, since `yearly_models` scales with the
roster, but the comparison is unaffected.

One consequence to carry: a copy-promotion rewrites **no** shared file, where a
rebuild rewrites two. The preservation check treats the expected shared writes as
permitted, not required, so fewer writes than expected is not an error — but the
manifest would then need its five `summaries` entries appended surgically, and
that step has to be explicit rather than assumed.

## 12. ETA, re-based 2026-10-08 07:00 UTC

§9.7 was written at 15:09 UTC on 2026-10-07, when 7 of 18 models were done. 15 are
now complete and validated, and the measured cadence held: consecutive completions
at 02:01, 03:57 and 05:50 UTC, within minutes of 2.05 h apart over 27 hours. The
fitting share has therefore shrunk from ~22 h to ~5 h, and the long pole is now
the staged bundle build and the production write.

| Stage | Estimate | Basis |
|---|---|---|
| Remaining campaign (NorESM2-LM, NorESM2-MM, TaiESM1) | **~5.0 h** | measured 2.05 h/model; NorESM2-LM entered `fit` at 05:55 UTC |
| Publisher → staged processed tree | ~1–2 h | still unmeasured |
| Staged bundle + state values | **~11.7 h** | measured 6,970.8 s at 2 models; `yearly_models` is 61.5% of it and scales with the roster |
| Full test suite | ~0.5–0.8 h | uncontended. The release-only subset is measured at **14.65 s / 126 tests** |
| Strict parity audit (staged) | ~0.2 h | |
| Staged dashboard verification + Streamlit check | ~0.2 h | |
| Backup + content hashing | ~0.5–1.0 h | two sha256 passes over ~8.7 GB on D: |
| Production artifacts | ~0.7–2.5 h promoted, or 2–12 h rebuilt | §11: one metric is built and byte-compared; if identical the other four are copied |
| Production dashboard + Streamlit check | ~0.2 h | |

**Remaining time to the five metrics being selectable in production: ~20–23 h if
the artifacts prove promotable, ~22–33 h if they must be rebuilt.** Either way the
publication lands on 2026-10-09. The 5 h of fitting that is left is not the time to
dashboard availability and should not be quoted as one.

## 13. The remaining sequence, as commands

Everything below runs from the repository root. The `.bat` wrappers exist because
environment variables do not cross WSL into the Windows interpreter.

| # | Command | Gate it satisfies |
|---|---|---|
| 1 | *(automatic)* the armed waiter runs `campaign_gate.py` when the campaign exits | A |
| 2 | `python -m tools.pipeline.publish_route_b_outdoor --run --processed-root C:\irt_rb_s2` | B |
| 3 | `build_s2.bat` then `state_values_s2.bat` | C |
| 4 | `gate_evidence.bat tests` | D → `gate_tests.json` |
| 5 | `gate_evidence.bat audit --bundle-root C:\irt_rb_bundle2 --processed-root C:\irt_rb_s2` | E → `gate_staged_audit.json` |
| 6 | `gate_evidence.bat dashboard --bundle-root C:\irt_rb_bundle2` | F → `gate_staged_dashboard.json`, covering both `verify_release.py` and `verify_streamlit.py` |
| 7 | `build_prod_dryrun.bat` for one metric, then `compare_bundles.bat --left C:\irt_rb_bundle2 --right D:\projects\irt_data\processed_optimised --metric wbgt_outdoor_route_b_days_ge_32` | §11 promotion decision |
| 8 | `publish_production.bat --run --processed-root C:\irt_rb_s2 --staged-bundle-root C:\irt_rb_bundle2` | G, H |
| 9 | `gate_evidence.bat dashboard --bundle-root D:\projects\irt_data\processed_optimised --label production --expect-root irt_data` | I |

Step 4 must run before step 6 and after step 3, because the dashboard gate's
evidence is bound to the staged bundle and the test gate's is not. Steps 5 and 6
are rerun, not patched, if the bundle is rebuilt for any reason: the candidate
binding refuses evidence from the previous build by design.

Nothing in steps 1–7 can write to production. Step 8 is the only writer, and it
re-runs gate A itself and re-checks the evidence from steps 4–6 against a freshly
computed candidate before it writes anything.

## 14. The Streamlit check (CHG-0678)

§9.5 said the Streamlit render would be checked by hand because
`build_map_and_rankings` takes ~40 widget-coupled arguments and calls `st.stop()`.
That reasoning was wrong: it argued against calling the function directly, and
then concluded the app could not be driven at all. `streamlit.testing.v1.AppTest`
runs `main.py` as a script, so the real widgets are instantiated and `st.stop()`
does exactly what it does in production.

`verify_streamlit.py` now does that, and it is run as part of gate F alongside
`verify_release.py` — the two answer different questions ("is the data the app
reads correct" and "does the app actually run on it") and the gate fails on
either.

Per metric, per state, per level it asserts: the script completes with no
exception and no `st.error`; the metric is selectable; the map, rankings and
yearly surfaces appear as elements; the download button carries bytes; and for
Lakshadweep and Andaman & Nicobar the rendered text contains a "No data" message
rather than an empty surface or a traceback. It also runs the landing view and an
incumbent metric.

Two things were learned building it, both worth recording because they shape what
the check can claim:

- **Metric selectability is asserted twice, and the second way is the stronger.**
  AppTest exposes a selectbox's `options` already passed through the widget's
  `format_func`, so they are display labels, not slugs — comparing slugs against
  them fails for every metric, including ones that work. The behavioural check is
  better: the ribbon resets `selected_var` to its placeholder whenever the held
  slug is not in the options it just built, so a slug that survives a run was in
  that list. Verified directly: with the slug seeded, the app came back holding
  `selected_var == 'wbgt_outdoor_route_b_annual_mean'`.
- **One AppTest instance per case.** Driving one instance through several runs
  fails on the second run: AppTest rebuilds the widget state of every widget the
  previous run created, and one multiselect raises inside AppTest's own element
  tree — `ValueError: content: "D"\n is not in list`, from `element_tree.py:744`,
  where it formats each value and looks it up in the options. That is a limitation
  of the harness, not a defect in the app: the widget renders correctly and it is
  reading the element tree that fails. A single run per instance never enters that
  code path. Instances share one process, so the caches stay warm — a map-view run
  costs ~16 s against ~126 s for the landing view.

What this still does not claim: no browser is involved, so it does not prove the
Folium map paints or that a tooltip appears under a cursor. It proves the app
script runs to completion on these metrics and emits the elements those surfaces
are built from. The one manual pass remains, and is recorded separately.

## 15. What the Streamlit check found (CHG-0679)

Running the app rather than its loaders found two things no data-level check could
have, and settled a third.

### 15.1 A fully excluded selection showed a red error, not a "No data" message

Selecting Lakshadweep produced:

    st.error: No numeric values found for the current map selection.

followed by `st.stop()`. For these two territories the absence of values is the
documented, evidenced outcome — `ISLAND_EXCLUSION.md` records Lakshadweep's
maximum cell support as 0.12 against a 0.50 gate — so an error is the wrong
presentation twice over: it reads as a malfunction, and `st.stop()` kills the rest
of the page, including the per-unit explanation CHG-0674 went to the trouble of
carrying into the master.

`summarise_missing_selection` (in `analysis/map_enrichment.py`, Streamlit-free)
now turns a fully excluded selection into its reason, and `map_pipeline.py` shows
that as `st.info` instead:

    No data for this selection. reference coverage below the required share of
    this unit's area (36 of 36 units). Highest retained coverage in this
    selection is 12%.

The conservative half matters more than the message. It returns the empty string —
leaving the error in place — whenever the data does not account for the absence:
no `exclusion_reason` column, no rows, or **any** unit marked `eligible` yet
carrying no value. That last case is a real anomaly and must keep surfacing as an
error rather than being explained away. Four tests cover it, including the
eligible-unit guard and the mixed-reason case (42 pass in that file; 95 pass
across the map and registry suites).

### 15.2 The five metrics will have no case-study export, by existing design

The right panel's case-study PDF/ZIP export is withheld for every metric with
`source_type: "external"` — `details_panel.py:1646`, commented "pipeline-backed
metrics only". All five Route B metrics are external, so they do not get it.

This is **not** a regression introduced here, but it is a user-visible difference
from the incumbent metrics they sit beside: `swbgt_empirical_*` and
`wbgt_shade_stull_*` carry no `source_type` at all, so they are pipeline metrics
and do get the export. The rankings table and its CSV still work and are verified
at byte level by `verify_release.py`, and the landing view's downloads are checked
in the app.

Changing this would mean enabling the export for every external metric, which is a
feature decision beyond this release and is not being made here. `source_type` is
not the lever either: `external` is what routes bundle discovery and the rebuild
path correctly, and relabelling these as pipeline metrics would assert that the
climate pipeline produces them, which it does not. **Flagged for a decision rather
than silently absorbed.** The check now asserts the contract as it is — no export
expander for an external metric — so this cannot drift unnoticed.

### 15.3 A latent defect outside this release

With the staged bundle selected, choosing an incumbent metric triggered the app's
auto-rebuild path, which failed with:

    Auto-rebuild failed: rebuild failed: [WinError 123] The filename, directory
    name, or volume label syntax is incorrect:
    "(WindowsPath('D:\projects\irt_data\processed\swbgt_empirical_annual_mean\Telangana"

The path has a stray `(` and quote: a tuple's repr is being used where a path
string belongs, in the rebuild command assembly reached from `ribbon.py:781`. It
only fires when a metric is missing from the resolved bundle, which is why no one
has hit it. Out of scope for this release and **not fixed here**; recorded so it is
not rediscovered as a Route B problem. The Streamlit incumbent check now runs only
against production, where the incumbents exist, so it does not trip over this.

## 16. The campaign finished; the first staged chain failed at the test gate

All 18 models are fitted, validated and assembled. **GATE A PASSED** at
2026-10-08 17:23 UTC with `release_ready: true`, 18 of 18 assembled, one shared
method, one shared support signature `f9843c3a57f0e72e`, and every artifact
matching its recorded hash. The staged chain then ran unattended and stopped, as
designed, at the test gate.

### 16.1 Measured stage costs, against the estimates

| Stage | Estimated | **Measured** |
|---|---|---|
| Publisher → staged processed tree | 1–2 h | **45 min** |
| Staged optimised bundle | 11.7 h | **4 h 08 min** |
| Staged state values | (inside the above) | **83 s** |
| Full test suite | 0.5–0.8 h | **7 h 31 min** contended by the env leak; **2 h 15 min** clean |

Two of my three estimates were wrong, in both directions. The bundle build is
~3× faster than predicted: scaling the `yearly_models` tier ×9 from a 2-model
measurement over-predicted, because that tier does not scale linearly with the
roster.

The suite needed two corrections, not one. The 7h31m figure was measured **with
the leaked staged roots** (§16.2); run clean it is **8,093 s / 2h15m**, which is
the number to plan with. And I guessed wrong about *why* it is slow: I wrote that
"the cost is concentrated somewhere specific", and `--durations=25` disproves it —
the slowest test in the suite is 6.16 s and the top 25 together are ~90 s of
8,093. The cost is per-test overhead spread across 2,214 tests at ~3.6 s each
(the geo stack imports on this interpreter), not a few pathological tests. So
there is nothing to optimise by fixing one test; the lever would be parallelism,
and `pytest-xdist` is not installed. Adding it is a dependency change and needs
approval, so the suite stays serial at ~2h15m.

The staged bundle is 487 MB across the five metrics (218 files each, 95–102 MB),
up from 25.8 MB per metric at 2 models.

### 16.2 The 101 test failures, accounted for

| Count | Cause |
|---|---|
| **82** | my own defect: the chain leaked staged roots into the test step |
| **15** | pre-existing at HEAD, unrelated to this release |
| **4** | real regressions from this release, now fixed |

**The 82.** `build_s2.bat` and `state_values_s2.bat` set `IRT_DATA_DIR`,
`IRT_PROCESSED_ROOT` and `IRT_PROCESSED_OPTIMISED_ROOT` without `setlocal`, and
the chain `call`s them, so those roots leaked into every later step. The suite
therefore ran against a metric-scoped Route B staging bundle, and 82 unrelated
tests in `test_proposal_bundle_*`, `test_optimized_bundle`, `test_composite_metrics`
and `test_glance_view_model` failed for that reason alone. Re-running the same 19
files with the three variables cleared: **101 → 19**. All four staged `.bat`
wrappers are now `setlocal`-scoped, and the chain clears the three variables
before the test step as well, because the cost of being wrong there is a 7.5 hour
run.

**The 15.** Measured in `.scratch/wbgt_head_baseline`, a pristine checkout of
HEAD `0cb621f`, with the same interpreter and a clean environment: **15 failed,
461 passed**. The identical 15 fail in the working tree. They are missing
boundary fixtures in pytest tmp dirs, an emoji in `docs/REGEN_RUNBOOK.md`, a
colour-range floor assertion, and similar — the repository's weather on this
machine.

**The 4 regressions** (CHG-0680), all from the same cause and all now fixed —
see §16.3.

### 16.3 A regression this found: the first external metric in the pipeline registry

These four failed in the working tree and pass at HEAD:

    test_compute_indices_synthetic.py::TestTierARegistryConsistency::test_all_pipeline_metrics_have_compute_functions
    test_compute_indices_synthetic.py::TestTierARegistryConsistency::test_all_bundle_metrics_have_valid_compute_functions
    test_compute_indices_synthetic_comprehensive.py::TestTierARegistryConsistency::test_all_pipeline_metrics_have_compute_functions
    test_compute_indices_synthetic_comprehensive.py::TestBundleMetricSmokeCoverage::test_every_bundle_metric_is_exercised

Cause: the five Route B metrics are the **first and only** entries in
`PIPELINE_METRICS_RAW` with `source_type: "external"`, and they are exactly the
five without a `compute` function — because nothing in the climate pipeline
computes them. Those tests require every registered metric to name a compute
function in `compute_indices_multiprocess.py` and to survive a synthetic smoke
call.

The fix is a contract, not an exemption. `_is_externally_produced` keys strictly
on `source_type == "external"`, and in exchange the tests now assert something
they did not before: **an external metric must declare an
`admin_rebuild_command`**, so there is a recorded way to reproduce it. Verified
that this bites rather than decorates — stripping the rebuild command from one
external metric is caught and names it. 177 pass in those two files.

This is the risk the directive's "do not suppress audit failures to obtain a
clean release report" names, so it is worth being explicit: the exemption is
narrow, keyed on a declared field, documented in the test, and paid for with a
stricter requirement in its place.

### 16.4 The test gate now fails on regressions, not on the repository's weather

The suite is not green at HEAD, so a gate demanding exit 0 could never pass, and
the temptation would then be to quietly lower it. The contract is explicit
instead: `tests_baseline_failures.json` records the 15 known failures with their
provenance (where measured, which commit, which interpreter, and the caveat that
it covers those 19 files only), and the gate passes only when nothing **outside**
that set fails. Both sets — every failure, and the regressions — go into
`gate_tests.json`, so the bar is auditable rather than asserted, and the output
log is kept.

An absent baseline file deliberately means "tolerate nothing". Widening the list
to make a gate pass is the thing it exists to prevent; extending it requires the
same kind of evidence that produced it.

Confirmed after the fix: the same 19 files now give **15 failed, 516 passed —
zero regressions against the recorded baseline.**

### 16.5 The staged bundle carries what the release needs

Rebuilt at 18 models, checked directly:

| State | Units | Provenance columns | Finite values | Model counts | Reasons |
|---|---|---|---|---|---|
| Telangana | 33 | all three | 231 | 16–18 | 33 eligible |
| Gujarat | 33 | all three | 203 | 0–18 | 29 eligible, 4 excluded |
| Andaman & Nicobar | 3 | all three | 0 | 0 | 3 coverage-below-gate |
| Lakshadweep | 1 | all three | 0 | 0 | 1 coverage-below-gate |

So the two island failures in the earlier dashboard runs were the stale 2-model
bundle, exactly as diagnosed: that bundle's masters carried **no** provenance
columns at all (`prov=[]`), so `summarise_missing_selection` correctly declined
to explain the absence and the error stood. The rebuilt masters carry the reason,
and the counts stop at 18 rather than claiming a model that did not contribute.

---

## 17. Second review round: the gate could pass without running (CHG-0681, CHG-0682)

Two findings from the focused review, both about the *instrument* rather than the
release. Closed before any final evidence was generated, and the verification code
was then frozen, so the evidence below describes the rules now in force.

### 17.1 CHG-0681 — a test gate that cannot distinguish "no failures" from "no run"

The gate derived its verdict from the `FAILED`/`ERROR` node ids parsed out of
pytest's output, forgave any that appeared in `tests_baseline_failures.json`, and
passed when nothing remained. The flaw is directional and severe: **every way of
failing to run the suite also produces zero `FAILED` lines.** A mistyped flag, an
interrupt, a collection error, an internal error or an empty selection each yield
no parsed failures, therefore no regressions, therefore `PASS`.

This had already happened in this campaign and I did not notice at the time:

```
ERROR: usage: __main__.py [options] [file_or_dir]
__main__.py: error: unrecognized arguments: --timeout=900
```

pytest exited **4** and executed nothing. Under the old logic that run would have
been stamped as a passing test gate, and the baseline mechanism — whose whole
purpose is to forgive failures — would have made the stamp look principled.

Admissibility is now established **positively**, from three independent signals,
and a baseline failure is forgiven only when all three hold:

| Signal | Requirement | Rejects |
|---|---|---|
| Exit code | 0 or 1 only, per pytest's documented codes | 2 interrupted, 3 internal error, 4 usage error, 5 nothing collected, anything unknown |
| Summary line | present, and accounting for ≥ 1 executed test | truncated or killed runs; `0 passed, 12 deselected` |
| Reconciliation | summary's failing count == parsed node-id count | truncated or interleaved logs, where the parsed set is not the whole story |

Reconciliation earns its place because the two numbers come from different parts
of pytest's output. If they disagree, the parsed set — the thing compared against
the baseline — cannot be trusted to be complete.

`publish_production.py` additionally refuses test evidence that is not
**full-suite** (`--release-only` evidence is recorded as a narrow scope and
rejected at publication) and refuses evidence carrying no outcome
classification at all, which is how pre-CHG-0681 evidence is identified.

Covered by `tests/test_route_b_gate_evidence.py` — 18 tests, each rejection case
plus the two admissible shapes, and one asserting directly that **a baseline
cannot forgive a run that did not happen**. All 18 pass.

### 17.2 CHG-0682 — binding the instrument, not just the subject

The candidate pinned the application code, the roster, the fits, the staged bundle
and (from CHG-0676) the test suite. It did not pin the thing that *decides* a
verdict. A PASS written by a gate that was later made more permissive still reads
PASS; so does one judged against a baseline that has since gained an entry, or
collected under a config that has since started ignoring a directory. None of
those edits touch any existing component, so every component would agree the
evidence was current while the standard had moved underneath it.

A `verification` component now digests the gate scripts, `verify_release.py`,
`verify_streamlit.py`, `publish_production.py`, `campaign_gate.py`, the recorded
baseline, the `.bat` wrappers (they decide the environment a gate inherits) and
the test-configuration files. The config files are listed although **none exists**
in this repo (pytest reports `inifile: None`); `ABSENT` is a recorded value, so
adding a `pytest.ini` moves the digest and invalidates the gate — correct, since a
new ini file can silently change what is collected.

Separately, each gate is now fingerprinted **before and after** it runs. A gate
takes hours; if anything in the candidate is edited while it is in flight, the
evidence describes neither the candidate it started on nor the one it finished
on, and both stamps are internally consistent, so comparing them is the only way
to notice. Drift is a `FAIL`, and `stable_during_run: null` — evidence predating
the check — is refused at publication.

Verified not to false-positive: no file under `metrics/<slug>/` changed during the
audit or the AppTest run (0 files modified in 12 h; newest write is the build
itself at 2026-10-08 23:14), so gates E and F do not perturb what they fingerprint.

### 17.3 Gate F's Telangana failure was staged-bundle geometry, not the release

Gate F's data-contract checker passed 143/143. The AppTest aborted on the first
case:

```
ValueError: District boundaries are missing master states after normalization: ['Telangana']
```

Not an app defect and not an environment leak — `DATA_DIR` resolves identically
with and without the gate's `IRT_*` exports, and cmd's `set VAR=` genuinely
unsets rather than leaving an empty string. The app loads district boundaries from
a **per-state geometry shard** inside the optimised bundle, and in the shard branch
it does not run `enrich_adm2_with_state_names`. Measured, staged vs production:

| Artifact | Staged | Production |
|---|---|---|
| `geometry/admin/district/state=Telangana.geojson` | 1 feature, 334 B | 33 features, 303 KB |
| `geometry/admin/block/state=Telangana.geojson` | 1 feature, 364 B | 588 features, 1.28 MB |
| `geometry/admin/adm1.geojson` | 1 feature, 256 B | 36 features, 402 KB |
| **every other geometry file (98 of 101)** | **byte-identical to production** | — |

Residue of an early smoke build run against a one-district boundary fixture, which
later builds never overwrote. Repaired in the staged bundle from production's
copies — justified by evidence, not assertion: 98 of 101 files produced by the same
builder from the same boundary source are byte-identical, so production's version
of those three is exactly what a correct staged build writes. Staged geometry is now
101/101 identical to production.

**The production risk this raised was checked and is nil.** Geometry is built from
`cfg.districts_path`, not from the metrics, so metric scope cannot degrade it — and
the publisher passes `--skip-geometry --skip-context --skip-audit`, so it never
writes geometry, context or shared audit artifacts into production at all.

**A free result for §11.** Two independently executed builds agreeing byte-for-byte
on 98 of 101 geometry files is direct evidence that this builder is
byte-deterministic. That was the one unmeasured input to the promotion-versus-rebuild
decision. It does not settle it — geometry is GeoJSON and the metrics are Parquet,
whose writers can embed timestamps or differing row-group boundaries — so the
one-metric Parquet comparison still runs before the decision. But the prior has
moved toward promotion being sound.

### 17.4 Corrections to this document

Two claims in §16.1 are withdrawn.

*Test-suite runtime.* The 7 h 31 min figure was measured **with the leaked staged
roots**. Clean, the suite is **8,100 s (2 h 15 min)**. Plan with the clean number.

*Why it is slow.* §16.1 previously said the cost was "concentrated somewhere
specific". `--durations=25` refutes that: the slowest single test is **6.16 s** and
the top 25 together are **~90 s of 8,100**, so the cost is spread across 2,214
tests at roughly 3.6 s each. The earlier replacement text attributed that overhead
to geo-stack imports. **That attribution is also withdrawn** — it is consistent
with import or fixture cost, but a duration summary measures per-test wall time and
nothing more, and no measurement here isolates a cause. Parallelism would be the
only lever; `pytest-xdist` is not installed, and adding a dependency is out of
scope for this release.

### 17.5 The test gate's result, stated precisely

The suite is not green at HEAD. The correct report of gate D is **"zero
regressions against the recorded baseline"** — 15 failures, all 15 present in
`tests_baseline_failures.json`, measured in the pristine `0cb621f` tree. It is
**not** "all tests passed", and this document should not be read as claiming that.

### 17.6 Staged dashboard gate rerun after geometry repair

The repaired Telangana and ADM1 geometry files match the production copies by
SHA-256. A first rerun passed the data-contract verifier (143/143) and exposed
three independent issues in the AppTest harness: printing a label containing
`≥` raised a Windows console encoding error; block cases seeded the district
analysis-focus option, which the block selector discarded; and the landing check
expected Glance view models in a staged bundle containing only the five Route B
metrics. The harness now prints escaped diagnostic details, seeds the block
analysis-focus option for block cases, and explicitly defers the landing check
to the production dashboard gate, where the Glance assets exist.

The full staged dashboard gate was rerun after those corrections. Its
`verify_release.py` check passed **143/143**; its `verify_streamlit.py` check
passed **47/47**, including mainland and excluded-island district/block cases.
`gate_staged_dashboard.json` records **PASS**, with a candidate fingerprint
stable across the gate run. The test and staged-audit evidence files still carry
older candidate fingerprints and must be rerun before publication; this
dashboard PASS alone is not a publication authorization.
