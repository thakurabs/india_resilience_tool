# Outdoor WBGT (Route B) — staging, publication and rollback package

CHG-0665. Companion to `RELEASE_SPEC.md`. Measured values are filled in as the
campaign produces them; every unmeasured number is marked PENDING, never
estimated into the table as if it had been measured.

## 1. Scope, and why "additive" is not the same as "safe"

Under the user's ruling the five `wbgt_outdoor_route_b_*` slugs are published
**alongside** `swbgt_empirical_*`, which is retired in this release. Nothing under
`metrics/swbgt_empirical_*` or `metrics/wbgt_shade_stull_*` is read, moved or
deleted, and no existing metric's values change.

That is genuinely lower risk than a replacement, but it is **not** merely adding
five directories, and this document previously said so too casually. A scoped
build still touches shared state:

| Shared artifact | How this release touches it | Check |
|---|---|---|
| `bundle_manifest.json` | rewritten by the build | diff before/after; back it up |
| `metrics/<slug>/state_values/.../all_states.parquet` | one per metric, so new files only | confirm no existing slug's file changes mtime |
| `colour_scale.json` | not touched; these slugs carry no ruler | confirm unchanged hash |
| `frozen_rulers` | not touched; display-only metrics | confirm unchanged hash |
| adm1 / `admin_block_index` / Glance | only rebuilt with `--include-shared-admin-artifacts`, which is **not** passed | confirm unchanged hashes |
| metric discovery | directory-based, so five new directories become visible | dashboard check |

`--full-rebuild` is **not** used. `--prune-scope` is **not** used: there are no
stale files inside a metric root that has never existed. The only destructive flag
in the plan is `--overwrite`, which `tools/CLAUDE.md` requires explicit
confirmation for.

## 2. Staging happens in a full replica, not in production

The whole release is built and verified in a complete standalone bundle on `C:`
before anything is written to `D:`. The production bundle is 8.7 GB, of which
geometry is 45 MB and context 239 MB, so a replica is affordable and removes any
need to verify against live data.

```
C:\irt_rb_s1       staged processed tree   (input to the build)
C:\irt_rb_bundle   staged processed_optimised replica + the five new metrics
```

Environment variables **do not cross the WSL to Windows boundary** — verified for
`PYTHONDONTWRITEBYTECODE` and documented for `PYTHONPATH` — so staged roots are
passed by generating a small `.bat` that sets them and invokes the module. A
command that merely prefixes `VAR=value` from a WSL shell silently runs against
production paths instead.

Two further operational facts, both learned the hard way during this session:

- **A Windows-interop process does not survive `nohup ... &` from a WSL shell.**
  It is killed when the launching shell exits, leaving a half-written tree and an
  empty log. Long Windows-side jobs must be launched so that the launching shell
  stays alive for their duration. Linux-side jobs under
  `/tmp/irt-carbonplan-py310` are unaffected and survive `nohup` normally.
- **Each relaunch begins with `rm -rf` of the staging root**, so two overlapping
  attempts silently wipe each other's output. Confirm no staging process is alive
  before starting another.

## 3. Ordered commands

Every command states what it writes. Steps 1 to 8 write nothing under `D:`.

**1. Clear stale bytecode.** On the `/mnt/d` 9p mount a `.pyc` can shadow an
edited source for the Windows interpreter, and `-B` only stops writing bytecode,
not loading it. Symptom: traceback line numbers that do not match the file.

```bash
find tools india_resilience_tool -name __pycache__ -type d -exec rm -rf {} + 2>/dev/null
```

**2. Validate every assembled model.** Read-only; writes `validation_report.json`
beside each model's artifacts.

```bash
for M in $(ls scratch/wbgt_route_b_national/assembly); do "/mnt/c/Users/22015611/AppData/Local/miniconda3/envs/irt/python.exe" -B -m tools.diagnostics.wbgt_route_b_validate --model-dir "scratch/wbgt_route_b_national/assembly/$M" --work-dir scratch/wbgt_route_b_national --report; done
```

**3. Predict and record missingness.** Read-only; writes
`exposure_prediction.json`. Verified exact against a fitted model, so it is
evidence rather than an estimate.

```bash
/tmp/irt-carbonplan-py310/bin/python -B -m tools.diagnostics.wbgt_route_b_exposure --workers 6
```

**4. Write the staged processed tree.** About 4.3 million small CSVs for 18
models. **Keep it on `C:`**: measured from the Windows geo environment on
2026-10-06, 36 small files/s to `D:` against 636 to `C:`. One model measured
238,710 files in 161 s, so the full roster is roughly 48 minutes.

```bash
"/mnt/c/Users/22015611/AppData/Local/miniconda3/envs/irt/python.exe" -u -B -m tools.pipeline.publish_route_b_outdoor --run --models all --processed-root C:/irt_rb_s1 --workers 8
```

**5. Build the staged bundle replica.** Geometry, context and the two shared JSON
files are copied (284 MB measured). The 8.4 GB of existing metric trees are
**junctioned**, not copied: a `cp -r` from `/mnt/d` measured 0.75 MB/s over the 9p
mount, which is hours, while 100 NTFS junctions are instant and resolve natively
for both the Windows builder and the dashboard.

```bash
mkdir -p /mnt/c/irt_rb_bundle && cp -r /mnt/d/projects/irt_data/processed_optimised/geometry /mnt/d/projects/irt_data/processed_optimised/context /mnt/c/irt_rb_bundle/ && cp /mnt/d/projects/irt_data/processed_optimised/bundle_manifest.json /mnt/d/projects/irt_data/processed_optimised/colour_scale.json /mnt/c/irt_rb_bundle/
mkdir -p /mnt/c/irt_rb_bundle/metrics
# then one `mklink /J` per existing slug, generated into a .bat and run through cmd.exe
```

**A junction reads and writes through to production.** That is safe here only
because every staged build is scoped with `--metric` to the five new slugs, which
are real directories rather than junctions. **Never run an unscoped or
`--full-rebuild` build against the staged bundle** — it would write through 100
junctions into live production. The staged bundle is for reading and for the five
new trees, nothing else.

**6. Dry-run the staged build.** Writes nothing; prints the resolved write and
delete plan. Generate the `.bat` so the staged roots actually take effect.

```bash
{ printf '@echo off\r\n'; printf 'set IRT_PROCESSED_ROOT=C:\\irt_rb_s1\r\n'; printf 'set IRT_PROCESSED_OPTIMISED_ROOT=C:\\irt_rb_bundle\r\n'; printf 'C:\\Users\\22015611\\AppData\\Local\\miniconda3\\envs\\irt\\python.exe -u -B -m tools.optimized.build_processed_optimised --metric wbgt_outdoor_route_b_annual_mean --metric wbgt_outdoor_route_b_annual_max --metric wbgt_outdoor_route_b_days_ge_28 --metric wbgt_outdoor_route_b_days_ge_30 --metric wbgt_outdoor_route_b_days_ge_32 --level all --skip-geometry --skip-context --dry-run\r\n'; } > .scratch/staged_dryrun.bat && /mnt/c/Windows/System32/cmd.exe /c "D:\projects\india_resilience_tool\.scratch\staged_dryrun.bat"
```

**7. Build into the staged bundle.** Same command without `--dry-run` and with a
staged `--report-path`. Writes only under `C:\irt_rb_bundle`.

**8. Generate staged state values, then audit.** `build_state_values` reads the
staged bundle's masters and writes one `all_states.parquet` per new metric. Both
commands need the staged roots, so both go through a generated `.bat`. Flags were
resolved from `--help`, not inferred:

```
python -m tools.optimized.build_state_values --metric <each of the five> --level all --strict
python -m tools.optimized.audit_processed_optimised_parity --metric <each of the five> --level all --skip-geometry --skip-context --require-block-yearly-models --strict --report-path docs/diagnostics/wbgt_outdoor_route_b_national/audit_staged.json
```

`--require-block-yearly-models` is passed deliberately: without it an audit can
pass while block yearly model series are absent, which is exactly the kind of
silent gap this release must not ship. `--strict` makes an error-severity issue a
non-zero exit rather than a line in a report nobody reads. The audit compares the
optimised bundle against the legacy processed inputs, so `IRT_PROCESSED_ROOT`
must point at the staged tree for it to mean anything.

**9. Verify the dashboard against the staged bundle.** The replica means this is a
real end-to-end check with no production exposure.

```bash
IRT_PROCESSED_OPTIMISED_ROOT=C:/irt_rb_bundle streamlit run main.py
```

Record what was actually looked at, not that the builder exited zero:

- metric availability and label under Heat Stress, beside the empirical ones;
- district and block selection; historical and SSP period selection;
- map values, legend, and the missing-data presentation for an excluded unit;
- rankings and detail panels; model and ensemble trends; state headline values;
- CSV download values and units;
- **that the configured periods match the available years** — only
  `1990-2010`, `2020-2040`, `2040-2060`, `2060-2080` should be offered, because
  periods are discovered from master column names rather than configured;
- **that the yearly trend truncates honestly** across the 2011-2019 gap between
  the historical and SSP series, showing no interpolated line and no apparent
  zeros outside coverage;
- at least one mainland state, one island territory, one supported coastal unit,
  one excluded coastal unit, and **Leh Ladakh**, whose end-century values rest on
  11 of 18 models.

**--- APPROVAL GATE: everything below writes into production `processed_optimised` ---**

**10. Record the rollback point.**

```bash
mkdir -p /mnt/d/projects/irt_data/_backup_route_b_outdoor_20261006 && cp /mnt/d/projects/irt_data/processed_optimised/bundle_manifest.json /mnt/d/projects/irt_data/processed_optimised/colour_scale.json /mnt/d/projects/irt_data/_backup_route_b_outdoor_20261006/ && find /mnt/d/projects/irt_data/processed_optimised -maxdepth 1 -type f -exec sha256sum {} + > /mnt/d/projects/irt_data/_backup_route_b_outdoor_20261006/shared_file_hashes.txt
```

**11. Publish.** The same build pointed at production, with a scoped report path
so a five-metric run cannot overwrite the whole-bundle parity verdict.

**12. Generate production state values, re-audit, and verify the live dashboard.**
Then confirm the shared artifacts in the §1 table are unchanged except
`bundle_manifest.json`, by comparing against `shared_file_hashes.txt`.

## 3a. Conversion fidelity, measured

The published master must carry the validated diagnostic value, not a
recomputation of it. Traced end to end on the staged bundle, ACCESS-CM2 only:

| Unit | Diagnostic source (mean of 21 years) | Published master | Difference |
|---|---|---|---|
| Kerala / Idukki (district) | 11.902903092824303 | 11.90290355682373 | 4.64e-07 |
| Kerala / Devikulam (block) | 0.8966489929078605 | 0.8966490030288696 | 1.01e-08 |

The difference is **float32 storage precision, not a conversion error**. The
optimised bundle stores master values as `float32` for every metric, the
incumbent included: `swbgt_empirical_days_ge_30` for Alappuzha is
273.913105812145 in its processed master and 273.9131164550781 as published, a
1.06e-05 difference at the same relative precision. Both the new and incumbent
columns are `float32`.

So the claim this release can make is **"preserved to the bundle's float32
storage precision"** — not bit-exact, and saying otherwise would overstate it.

The roster and missingness survive the conversion intact. Kerala's published
district master carries all 14 districts, with the five excluded coastal ones
(Alappuzha, Ernakulam, Kasaragod, Kozhikode, Thiruvananthapuram) as `NaN` rather
than zero or absent, and `district_key` generated in the established
`kerala|alappuzha` form.

## 3b. Staged build and audit result, one model

Measured on the staged bundle with ACCESS-CM2 alone:

| Stage | Result |
|---|---|
| Staged processed tree | 238,710 files, 485 s (161 s uncontended) |
| Optimised build | 119,536 tasks, 2,718.9 s — masters 93.6 s (3.4%), yearly-models 2,625.3 s (96.6%) |
| Metrics written | 5, all with yearly artifacts |
| State values | 10 files, 504 rows each across 36 states |
| Strict parity audit | **20 errors, all of one kind** (see below); zero warnings |

The build runs effectively single-threaded through the yearly stage, which is 96.6%
of its time. For 18 models that stage will dominate and should be re-measured
rather than scaled linearly from this run.

### The one open audit condition

**Andaman & Nicobar Islands has 0 of 12 eligible units, and Lakshadweep 0 of 11.
Both UTs are entirely unsupported by the UHE reference.** This was understated in
earlier reporting, which counted them among the 169 excluded units without saying
that two whole territories have no coverage at all.

The consequence is coherent but not clean:

- their **masters are written** with the full roster — 3 A&N districts, 1
  Lakshadweep district — carrying `NaN`, so a map correctly shows them as missing;
- their **yearly_models parquet is not written at all**, because every row is
  missing and the builder drops missing rows;
- the strict audit expects a yearly file wherever a master exists, so it reports
  `missing=district_key,model,scenario,value,year` — 2 states x 5 metrics x 2
  levels = **20 errors**, and `--strict` exits 1.

These errors are **correct signals about genuinely empty states**, not false
alarms, and they are not suppressed. Two ways to resolve them, both needing a
decision:

1. **Write an empty but schema-carrying parquet** for a state with no eligible
   units. The file then says "this state is known and has no data" rather than
   being absent. This changes `tools/optimized/build_processed_optimised.py`,
   which every one of the 100 metrics shares, so its blast radius is wider than
   this release.
2. **Accept and document the 20 errors**, recording them as an expected condition
   for these two UTs in the release manifest, and relying on the masters to carry
   the missing values.

Either way the dashboard's behaviour for those two UTs must be checked before
publication: showing "no data" is correct, erroring is a hard blocker. That check
is step 9 and has not been run.

## 4. Rollback

```bash
rm -rf /mnt/d/projects/irt_data/processed_optimised/metrics/wbgt_outdoor_route_b_annual_mean /mnt/d/projects/irt_data/processed_optimised/metrics/wbgt_outdoor_route_b_annual_max /mnt/d/projects/irt_data/processed_optimised/metrics/wbgt_outdoor_route_b_days_ge_28 /mnt/d/projects/irt_data/processed_optimised/metrics/wbgt_outdoor_route_b_days_ge_30 /mnt/d/projects/irt_data/processed_optimised/metrics/wbgt_outdoor_route_b_days_ge_32
cp /mnt/d/projects/irt_data/_backup_route_b_outdoor_20261006/bundle_manifest.json /mnt/d/projects/irt_data/_backup_route_b_outdoor_20261006/colour_scale.json /mnt/d/projects/irt_data/processed_optimised/
```

Reverting the registry is a separate `git checkout` of
`india_resilience_tool/config/metrics_registry.py` and
`tests/test_metrics_registry.py`. A registered slug with no data must degrade to
"unavailable" rather than erroring; confirm that in step 9 against the staged
bundle before publishing.

## 5. Mixed-release protection

Each metric tree is written under its own ownership root, so a failure mid-run
leaves some of the five present and others absent — five independent additive
metrics, not one mixed metric. No existing metric is touched, so there is no
window in which a reader sees a blend of old and new values under one label. If a
run fails part-way, roll back the partial trees and re-run rather than resuming
into them.

## 6. Measurements before the approval gate

| Quantity | Status |
|---|---|
| Model roster | **18**, verified by file existence and calendar; KACE-1-0-G excluded (360_day) |
| Models assembled and validated | ACCESS-CM2 PASS; ACCESS-ESM1-5 in progress; 16 held pending the window decision |
| Predicted missingness, all 18 models | measured: 44,320 of 19,953,648 model-unit-years, 0.22% |
| Staged tree, one model | measured: 238,710 files, 161.4 s |
| Staged tree, 18 models | PENDING — roughly 48 min at the measured rate |
| Staged bundle replica | measured: 284 MB geometry and context; 8.7 GB total production bundle |
| Optimised dry-run plan | exercised against the staged tree; 5 metric trees plus `bundle_manifest.json` |
| Free space | measured: 1.5 TB free on `C:`, 5.8 TB free on `D:` |
| Staged state values | PENDING |
| Staged strict audit | PENDING |
| Dashboard evidence | PENDING |
| Release manifest checksums | PENDING |

No approval should be requested while any of these is PENDING.
