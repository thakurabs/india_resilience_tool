# CRAVIS evaluator threat model

This evaluator is a human-gated, `n=1` case-study harness. It is not a crawler, a load test, or a general estimate of CRAVIS performance.

Protected assets are the operator's CRAVIS session, prompt quota, secrets, local files, and integrity of the evidence and human score lock. Principal threats are accidental duplicate submission after a crash, navigation or requests to unapproved origins, authentication evidence capture, prompt/config mutation, secret leakage, path traversal and symlink escape, malicious downloads, CSV formula injection, and post-review evidence changes.

Controls include an exact `SEND <prompt-id>` confirmation; a synchronous append/flush/fsync send ledger; immutable prompt hashes after arming; no automatic retry; quota and active-transcript proof; HTTPS origin approval with loopback-only HTTP; request abortion for unapproved origins; authentication capture without evidence collectors; non-symlink path containment; sanitized network and error evidence; bounded inert downloads; canonical hashes; atomic output writes; and a human review lock that is invalidated by any reviewed-input change.

Residual risks include changes to the live CRAVIS DOM, ambiguous application quota semantics, service-worker traffic Playwright cannot fully interpose, and product behavior not visible in the active response subtree. Such observations are classified `uncertain` or `not_observable`; they must not be upgraded by reviewer confidence.
