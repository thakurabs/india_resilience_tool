# Blocked / partially-verifiable stories (US 01, US 02–04, US 05)

These stories can't be authoritatively driven from the current **logged-in,
already-onboarded** session without either ending the session or provisioning a
fresh/never-onboarded account with a reachable test-email inbox. Recorded here so
coverage is honest.

## US 01 — Landing & Access Options — PARTIAL / BLOCKED
- **Why blocked:** the spec describes the **pre-authentication landing page**
  (Sign In / Sign Up / Forgot Password, sliding background images, Donate button,
  Resustainability logo). With a saved session, visiting the URL lands straight on
  the **dashboard**, so the pre-auth surface is never shown. Verifying it needs a
  logged-out state (and re-login needs 2FA).
- **What we can see (logged-in header):** Resilience Actions logo, "India Resilience
  Tool" title, "Welcome, [Name]", Share Feedback.
- **Observation (verify):** in the logged-in header we did **not** observe a
  **Donate button** or a **Resustainability logo** (spec 46–49). They may live only
  on the pre-auth landing — confirm with the PO / a logged-out capture.

## US 02 — Sign In (Email + Password + 2FA) — BLOCKED (tooling)
## US 03 — Create Account (Sign Up + Email Verification) — BLOCKED (tooling)
## US 04 — Reset Password — BLOCKED (tooling)
- **Why blocked:** all three need a **test-email inbox** to receive the 2FA code /
  verification link / reset link, plus (for US 03) willingness to create real
  accounts. Out of autonomous scope. The saved session was captured once manually
  (`capture-session.mjs`) and is reused read-only.
- **Decision needed:** provision a disposable inbox + test account to cover these, or
  accept them as manual-only.

## US 05 — First-Time Visitor Guide — BLOCKED (account state)
- **Why blocked:** the guide **auto-triggers only on first visit** and its
  post-condition is *"does not auto-trigger again for same user/session."* This
  account is already onboarded, so the guide overlay did **not** appear (confirmed:
  no Quick Guide / Skip / Next / Step-N controls in the DOM). Verifying the
  step-by-step walkthrough (Geography → Filters → Map → Coordinates → Customize View,
  with Next/Previous/Skip/Done) needs a **fresh never-onboarded user** or a way to
  reset the onboarding flag.
- **Not a defect:** absence is spec-consistent for a returning user.
