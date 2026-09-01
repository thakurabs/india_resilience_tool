# US 08: Feedback Form (Popup Submission)

Source: _source_user_stories_v1.3.txt lines 311–367
Scope: functional

## Preconditions
- Logged in; "Share Feedback" visible in the header.

## Steps & expected results
| # | Action | Expected result |
|---|--------|-----------------|
| S1 | Click **Share Feedback** | Feedback popup opens |
| S2 | Inspect popup structure | Experience selection (5–6 radios), "Tell us more" text field, star rating, Submit, close (spec 320–325) |
| S3 | Close via × / Escape | Popup dismisses without submitting |

## Known caveats / safety
- **Submit is NEVER clicked** — it emails the configured admin (outward-facing).
- **Logout auto-trigger variant** ("Reason for leaving" + Skip/Submit & Logout, spec
  340–356) is **not verifiable** without logging out — out of scope this session.
- **Observation:** the same feedback popup also **auto-triggers on a timer mid-session**
  (seen during US 17), which spec 08 does not define (it defines only header-manual +
  logout-auto). Flag for PO.
