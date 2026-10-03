# US 08 — Feedback Form (Popup Submission)

**Verdict: PASSING (header/manual flow).** The "Share Feedback" popup matches the
spec structure. Two parts are out of scope this session: the actual **Submit**
(outward-facing — emails the admin) and the **logout auto-trigger** variant (can't
log out). One observation: the popup **also auto-triggers on a timer mid-session**,
which the spec does not define.
Run: `qa/runs/2026-07-08T15-49-48-053Z_us08-feedback/` (3 steps, 0 failed, 0 errors).

## Verified matching spec (no defect)
- **Share Feedback** (header) opens the feedback popup. (S1)
- Popup structure (spec 320–325): **5 experience radios** (Easy to use · Helped me
  achieve my goal · Took too long · Confusing navigation · Missing features) +
  **"Tell us more"** text field + **star rating** ("How was your experience?") +
  **Submit** + **×** close. (S2)
- Closes cleanly via ×/Escape without submitting. (S3)

## Findings
| ID | Sev | Area | Status | Summary |
|----|-----|------|--------|---------|
| N21 | Minor | functional | ASK-PO | The feedback popup **auto-triggers on a timer mid-session** (observed unprompted during US 17). Spec 08 defines only a **header-manual** trigger and a **logout auto-trigger** — not a timed mid-session nudge. Confirm intended. |

## Not exercised (safety / scope)
- **Submit never clicked** — it emails the configured admin (spec 358–366).
- **Logout auto-trigger** variant ("Reason for leaving" + Skip & Logout / Submit &
  Logout, spec 340–356) — not verifiable without logging out.
