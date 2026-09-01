# US 06: Header & Dropdown Navigation

Source: _source_user_stories_v1.3.txt lines 238–264
Scope: functional

## Preconditions
- Logged in; header shows logo + title + "Welcome, [Name]" + profile icon.

## Steps & expected results
| # | Action | Expected result |
|---|--------|-----------------|
| S1 | Inspect header | Logo + "India Resilience Tool" title; "Welcome, [Name]" + profile icon; "Share Feedback" present |
| S2 | Click Welcome / profile icon | Dropdown opens with **User Profile · My Analysis · Logout** |
| S3 | Click **User Profile** | Navigate to Profile Management (`/profile`) |
| S4 | Click **My Analysis** | Navigate to My Analysis (`/my-analysis`) — already verified in US 15 |

## Known caveats / safety
- **Logout is NOT exercised** — it ends the saved 2FA session (spec 258: logout →
  feedback page → login). Its presence in the dropdown is verified; the action is not.
