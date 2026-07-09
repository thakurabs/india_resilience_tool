# US 07: User Profile Management

Source: _source_user_stories_v1.3.txt lines 265–310
Scope: functional | data

## Preconditions
- Logged in; profile reachable via Welcome → User Profile (`/profile`).

## Steps & expected results
| # | Action | Expected result |
|---|--------|-----------------|
| S1 | Navigate to User Profile | `/profile` loads with breadcrumb Dashboard / User Profile |
| S2 | Inspect view fields | Name, Email, Organization, Designation, Purpose of Use, Thematic Activity, Country (India, locked), **State**, Reset Password (spec 280–289) |
| S3 | Email field | Email is present but **not editable** (spec 296) |
| S4 | Save/Update control | An update control exists (spec 302 "Save") |
| S5 | Reset Password | A Reset-Password affordance exists that keeps the user logged in (spec 289) |

## Known caveats / safety
- **Edit + Save is NOT exercised** — it mutates the real account.
- **Reset Password is NOT triggered** — it sends an OTP to the real inbox.
- Verification is read-only field/affordance presence.
