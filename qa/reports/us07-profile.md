# US 07 — User Profile Management

**Verdict: PASSING with two drifts.** The profile page loads and shows the profile
fields read-only-correctly; Email is locked and Reset Password is present. Two
spec-vs-app mismatches: a **missing State field** and a **"Update" vs "Save"** button.
Run: `qa/runs/2026-07-08T15-45-46-207Z_us07-profile/` (4 steps, 0 failed, 0 errors).

## Verified matching spec (no defect)
- `/profile` loads (breadcrumb Dashboard / User Profile). (S1)
- Fields present: **Name · Email · Organization · Designation · Purpose of Use ·
  Thematic Activity · Country (India)**. (S2)
- **Email is locked** — input `disabled/readonly` + greyed (spec 296 "Can't be
  changed"). (S3)
- **Reset Password** section present via **Send OTP** — keeps the user logged in
  (spec 289). Not triggered. (S4)

## Findings
| ID | Sev | Area | Status | Summary |
|----|-----|------|--------|---------|
| N19 | Minor | data | ASK-PO | Profile **omits the "State" field** (spec 280 & 289 list State among viewable/editable profile fields). Only Country (India, locked) is shown. |
| N20 | Cosmetic | functional | ASK-PO | Save button is labelled **"Update"** (spec 302 says **"Save"**). |

## Not exercised (safety)
- **Edit + Update not submitted** (mutates the real account).
- **Send OTP / Reset Password not triggered** (sends OTP to the real inbox).
