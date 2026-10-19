# CRAVIS interaction driver and evaluation framework

This directory contains CHG-0321: a self-contained, human-gated evaluator for one ten-prompt CRAVIS case study. It uses Node 18 and the repository's existing Playwright dependency. It does not share code with `qa/`, and it is not part of the planned IRT assistant runtime.

The campaign is `n=1`. Its results describe only the observed campaign and must not be presented as a general CRAVIS performance estimate.

## Mandatory operator acknowledgement

Before any live command, the operator must independently confirm that they are authorized to evaluate CRAVIS, that the activity complies with CRAVIS terms, quota rules, and account policies, and that the account may be used for this case study. Create the ignored file `.auth/operator-ack.json` only after making those checks:

```json
{
  "authorized": true,
  "termsCompliant": true,
  "quotaUnderstood": true,
  "accountPolicyCompliant": true,
  "acknowledgedBy": "operator identifier",
  "acknowledgedAt": "YYYY-MM-DDTHH:MM:SSZ"
}
```

The driver refuses live sends unless all four booleans are exactly `true`. This acknowledgement records an operator decision; the evaluator does not determine whether access is legally or contractually authorized.

## Commands

Run commands from the repository root:

```text
node assistant/evaluations/cravis/cli.mjs simulate
node assistant/evaluations/cravis/cli.mjs capture-session
node assistant/evaluations/cravis/cli.mjs recon
node assistant/evaluations/cravis/cli.mjs campaign
node assistant/evaluations/cravis/cli.mjs review --campaign <id>
node assistant/evaluations/cravis/cli.mjs report --campaign <id>
```

Imports are inert: importing the CLI or a library module does not launch a browser, access the network, or create files.

## Live rollout, in order

1. Read `THREAT_MODEL.md`, `config/prompts.json`, `config/rubric.json`, and `config/targets.json`.
2. Verify authorization, terms, quota, and account policy, then create `.auth/operator-ack.json` as above.
3. If CRAVIS requires another origin, inspect it during recon and add it only after explicit local approval to ignored `.auth/origin-approvals.json` as `{"approvedOrigins":["https://approved.example"]}`. Recon discovery never approves an origin.
4. Run `node assistant/evaluations/cravis/cli.mjs simulate`. This starts a loopback-only fixture and exercises the same prompt-observation and quota gates as live mode.
5. Run `node assistant/evaluations/cravis/cli.mjs capture-session`. A headed browser opens for manual login. Authentication capture records no screenshots, video, trace, console, or network evidence. Session state is written beneath `.auth/` only.
6. Run `node assistant/evaluations/cravis/cli.mjs recon`. It sends nothing and must prove unchanged quota, unchanged active-conversation last-user-message hash, zero driver Send activations, and zero ledger records.
7. Run only P01: `node assistant/evaluations/cravis/cli.mjs campaign --campaign <id> --prompt P01`. Read the exact prompt and type exactly `SEND P01`. There is no bypass or batch mode. Any legitimately inapplicable rubric dimensions must be declared before this first command with `--na-dimensions <comma-separated-ids>`; they cannot be added after campaign creation.
8. Review privacy, quota, the crash-safe ledger, response evidence, export safety, and P01 quality before considering P02. Never retry a prompt automatically.
9. Continue one prompt at a time. After P01-P08, author A09 and A10 from observed weaknesses; adaptive prompts are immutable once persisted and never affect the fixed score.
10. Populate ignored `runs/<id>/human-review.json` with reviewer identifier, optional `scopePromptIds`, every applicable human-confirmed score, and override notes. Run `review`. A P01-only scope creates a pilot lock; only a complete P01-P08 lock can generate final reports.
11. Populate `classifications.json` with all four independent capability axes, evidence references, and reviewer rationale. Run `report`. It rejects missing, incomplete, mismatched, or post-lock-mutated campaigns.

## Safety behavior

Every safety-critical send transition is appended synchronously and filesystem-synced to `send-ledger.jsonl`. Prompt text and SHA-256 are immutable after `armed_persisted`. `click_dispatched` means only that Playwright returned. A send is proven only when the exact normalized prompt is the active conversation's last user message and quota shows exactly one decrement. Missing, malformed, unchanged, increased, or multi-decrement quota is `uncertain`; no automatic retry occurs. A truncated final ledger tail is quarantined, valid records are preserved, the active prompt is classified uncertain, and further sends are blocked.

Live navigation is HTTPS-only and origin-approved. HTTP is allowed only for loopback simulation. Evidence stores normalized origins and sanitized pathnames, never query strings, fragments, headers, cookies, request or response bodies. Authentication endpoints are excluded. Raw evidence, session state, videos, screenshots, downloads, and draft reports are ignored locally.

Downloads are inert, generated-name files with SHA-256, detected media type, and a 25 MiB limit; they are never executed or automatically opened. CSV generators retain typed numbers and neutralize formula-leading strings.

## Independent reference data

Official downloads obtained outside the prompt quota may be placed under ignored `reference-data/` before the campaign. Register each file through `lib/reference-data.mjs` with its SHA-256, HTTPS source URL, retrieval date, schema, and calculation worksheet. Without a compatible reference worksheet, analytical correctness is limited to arithmetic and internal consistency, and external numerical correctness remains unverified.

Do not equate CRAVIS RCP 4.5/8.5 with IRT SSP contracts without documented source support. A legitimate scenario or method difference is a `methodology_conflict`, not automatically an IRT gap.

## Verification

```text
node --check assistant/evaluations/cravis/cli.mjs
node --test assistant/evaluations/cravis/tests/*.test.mjs
```

Final locked reports are written atomically beneath `reports/<campaign-id>/` with the required assessment Markdown, scores JSON, prompt-results CSV, timings CSV, and IRT gap matrix.
