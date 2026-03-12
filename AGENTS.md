# AGENTS.md — India Resilience Tool (IRT)

This repository is a climate-oriented risk/resilience assessment tool (Python-heavy; Streamlit UI; geospatial + climate analytics).
This file defines **how Codex should work here**.

Design principle: keep root instructions **short, command-first**, and put domain-specific rules in **nested** `AGENTS.md` files
near the code they govern.

Source-of-truth docs:
- `README.md` (human-facing setup/run/test)
- `MANIFEST.md` (AI-facing repo map: modules, entry points, file structure)

---

## 0) Non-negotiable: Approval Gate

**Do only tasks explicitly approved by the user.**

Allowed without extra approval (read-only):
- reading files
- `ls`, `find`, `rg`, `cat`, `sed -n`, `python -c`
- `git status`, `git diff`, `git log -1`, `git rev-parse --short HEAD`
- proposing patches (diffs/replace-blocks), explaining code, writing test plans

Requires explicit approval:
- modifying any files
- running expensive/long pipelines
- adding/upgrading dependencies
- creating commits/branches/tags/pushing

If approval is unclear: produce a patch + plan, but **do not apply changes**.

Common approval phrases to accept:
- “approved”, “go ahead”, “apply”, “implement”, “make the changes”, “do it”

Tip to reduce ambiguity: ask the user to use one of:
- `APPROVED: APPLY`
- `APPROVED: RUN TESTS`

---

## 1) Always start by establishing current state

Before code edits:
1) Read `README.md` and `MANIFEST.md` for repo-specific commands and navigation.
2) Determine snapshot + working tree:

Suggested commands:
- `git status --short --branch`
- `git rev-parse --short HEAD` (if git checkout)
- `git diff --name-only` (if needed)

In every technical response, include a **Status Header**:

- **Working Snapshot:** `GIT:<branch>@<sha>` or `ZIP:<name>` or `UNKNOWN`
- **Working Tree:** clean/dirty (+ changed files if dirty)
- **Assumptions:** only if needed

---

## 2) Patch / edit standards (production-ready)

For any modified function/class/module:
- Provide complete, runnable code (no placeholders)
- Preserve existing structure; avoid drive-by refactors
- Keep diffs minimal and task-scoped
- Include required imports; remove unused imports introduced by your change
- Use type hints for non-trivial functions and public APIs
- Add docstrings for public functions/classes
- Treat I/O robustly (clear exceptions / warnings)

When not applying changes directly, output patches as:
- **Unified diff**, or
- **Targeted Replace Blocks** with clear anchors.

---

## 3) Tests & validation

Tests live in `tests/`.

Preferred fast command:
- `python -m pytest -q`

If additional checks are configured, prefer whatever `README.md` specifies (do not guess).

For non-trivial logic changes, always include:
- a short manual test plan
- a suggested pytest test (path + assertion)
- edge cases: empty inputs, all-NaN, single-point, extremes

If you cannot run tests (env/data missing), say exactly what blocked you and provide a manual validation checklist.

After any **accepted/applied** code, contract, or workflow change, explicitly check whether:
- `README.md` needs updating
- `MANIFEST.md` needs updating

In your response, either:
- update them, or
- state clearly why no update is needed

---

## 4) Climate / geospatial / risk guardrails

- Assume NaNs/masked values; document behavior (error vs NaN vs partial+warning)
- Be explicit about units, baselines, aggregation windows
- Do not change methodology silently (ranking, thresholds, baselines, aggregation)
- Any methodology-impacting change must be called out and tested

See nested instructions:
- `india_resilience_tool/analysis/AGENTS.md`
- `india_resilience_tool/viz/AGENTS.md`
- `india_resilience_tool/app/AGENTS.md`

---

## 5) Change IDs + Ledger (CHG-xxxx)

Every proposed change gets a Change ID: `CHG-0001`, `CHG-0002`, ...

Maintain an **in-chat ledger** in every code-change response:
- Change ID
- file(s)
- 1–2 line summary
- status: `SUGGESTED` / `APPLIED (user-confirmed)` / `REJECTED` / `SUPERSEDED`

---

## 6) Persistent HANDOFF ledger: docs/HANDOFF.md

The persistent ledger lives in `docs/HANDOFF.md`.

**Critical rule**
- Do **not** update `docs/HANDOFF.md` unless the user explicitly confirms:
  - `Applied CHG-xxxx`
  - `Applied: CHG-0007, CHG-0008; Rejected: CHG-0006`

Before that, keep updates in-chat only.

## 7) Persistent BACKLOG ledger: docs/BACKLOG.md

The durable deferred-work list lives in `docs/BACKLOG.md`.

Use it for:
- long-lived deferred work
- shelved follow-ups that should be picked up later
- larger initiatives that are not the current execution priority

Do not use it for:
- session handoff details
- applied-change history
- transient debugging observations with no durable follow-up value

Update `docs/BACKLOG.md` when the user explicitly asks to capture or revise deferred work.

## 8) Git handoff after applied work

After **accepted/applied** repo changes, Codex should proactively help the user finish the git handoff by:
- checking or reviewing `git status`
- providing a one-line `git add ...`
- providing a short, crisp one-line `git commit -m "..."`

Important:
- this is guidance to provide commands, not permission to run commits automatically
- skip this only if the user explicitly says they do not want git help yet

Formatting requirement for git handoff commands:
- the `git add` command must be on **one physical line**
- it must be directly copy-pasteable into the VS Code terminal with no edits
- do not use shell continuations or wrapped multi-line formatting
- be mindful that visual line wrapping in chat can introduce confusion; the command itself must remain a single literal line
- the `git commit -m "..."` command should also be provided on one physical line

---

## 9) PERFECT HANDOFF POINT (mandatory on session end)

Whenever the user indicates they are ending / pausing the session (examples: “wrap up”, “pause”, “handoff”, “end”, “stop”, `/quit`, `/new`),
append a copy-paste-ready section titled:

**PERFECT HANDOFF POINT**

It must include:
- Objective / task context
- Working Snapshot + working tree status
- CHG ledger excerpt (relevant items)
- Files touched (if any)
- Tests run (commands + results) or why not
- Exact next steps (ordered)
- Open questions / risks

Important:
- This handoff is produced **in chat** even if you only suggested patches.
- `docs/HANDOFF.md` is updated only after “Applied CHG-xxxx”.

---

## 10) Nested AGENTS.md

This repo uses nested `AGENTS.md` files to improve agent performance. Apply the closest applicable instructions when working in subdirectories.

Current nested scopes include:
- `india_resilience_tool/analysis/AGENTS.md`
- `india_resilience_tool/app/AGENTS.md`
- `india_resilience_tool/config/AGENTS.md`
- `india_resilience_tool/compute/AGENTS.md`
- `india_resilience_tool/data/AGENTS.md`
- `india_resilience_tool/viz/AGENTS.md`
- `tests/AGENTS.md`
- `tools/AGENTS.md`
- `notebooks/AGENTS.md`
