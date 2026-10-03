# Charters

One folder per user story. Each folder contains:

- **`spec.md`** — human- and reviewer-readable: the source user story, the exact
  steps, and the **explicit expected result** for each. Traces back to
  `_source_user_stories_v1.3.txt`. This is the ground truth the reviewer judges
  against.
- **`scenario.mjs`** — the Playwright scenario that drives the steps and dumps
  evidence into `qa/runs/<ts>_<label>/`. Written against the live DOM (selectors
  discovered via `harness/explore.mjs`). Records per-step outcomes with `step()`.

## spec.md format

```markdown
# US NN: <title>

Source: _source_user_stories_v1.3.txt lines A–B
Scope: functional | data | visual | a11y  (tick all that apply)

## Preconditions
- ...

## Steps & expected results
| # | Action | Expected result |
|---|--------|-----------------|
| 1 | ... | ... |

## Known caveats
- doc may be stale re: <x>; treat mismatch as "verify with user" not auto-bug
```

## Naming

`usNN-short-slug/` — e.g. `us09-geography-selection/`. The slug is the run label.

## Staleness

The user story doc is v1.3 and the app has been refined since. When observed
behaviour contradicts the spec, that is **not automatically a bug** — it may be
an intended refinement. Flag such cases as `SPEC-DRIFT` for human confirmation
rather than `FAIL`.
