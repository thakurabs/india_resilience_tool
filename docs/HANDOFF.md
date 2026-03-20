# HANDOFF — India Resilience Tool (IRT)

This file is the **persistent** project handoff + change ledger used by AI agents.

## Update policy (important)

- Agents MUST NOT modify this file unless the user confirms applied work in the format:
  - `Applied CHG-0007`
  - `Applied: CHG-0007, CHG-0008; Rejected: CHG-0006`

Until then, agents should keep the working ledger **in chat** and produce a
**PERFECT HANDOFF POINT** section at the end of any session-ending message.

---

## Current Working Snapshot

- Snapshot Source: UNKNOWN
- Branch: (if git)
- Commit: (short sha)
- Working Tree: (clean/dirty)
- Last Updated: YYYY-MM-DD (Asia/Kolkata)
- Notes:
  - (environment assumptions, data availability, etc.)

---

## Global Change Ledger

| Change ID | Status | Files | Summary | Tests / Checks | Snapshot | Notes |
|---|---|---|---|---|---|---|
| CHG-0001 | SUGGESTED |  |  |  |  |  |

Statuses:
- SUGGESTED
- APPLIED (user-confirmed)
- REJECTED (user-confirmed)
- SUPERSEDED (by CHG-xxxx)

---

## Per-File Change Ledger

Add sections per file as needed:

### path/to/file.py
| Change ID | Status | Summary | Tests / Checks | Snapshot | Notes |
|---|---|---|---|---|---|
|  |  |  |  |  |  |

---

## Open Threads / Known Issues

- [ ] (Add open items here)

---

## Resume Checklist (fast)

1) Confirm snapshot:
   - `git status --short --branch`
   - `git rev-parse --short HEAD`
2) Run fast tests:
   - `python -m pytest -q`
3) If changes involve UI:
   - run the Streamlit entrypoint specified in `README.md`
