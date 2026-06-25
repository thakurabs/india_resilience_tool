# Systematic Review — Technical Guidance Note

**Reviewed document:** `docs/technical_guidance_note.md` (996 lines; §1–§8 + Appendices A/B)
**Working Snapshot:** GIT:add_flood_depth@c761ec2
**Working Tree:** dirty (review adds this file only; the note itself is not edited)
**Review lenses (per author direction):** scientific defensibility · internal consistency · audience & exemplar fit · **no code/prototype references in a user-facing note**
**Verification basis:** quantitative claims cross-checked against the prototype config/compute layer (`config/bundle_weights.py`, `config/proposal_bundles.py`, `config/constants.py`, `compute/proposal_bundles.py`, `compute/composite_metrics.py`). The prototype is treated as ground truth for *numbers only*; it is explicitly **not** something the note may cite.
**Deepest scrutiny:** §1, §7, §8 (model-drafted, not yet author-reviewed) and §6/§8 normalization.

---

## How to read this review

Findings are severity-ranked. Each carries a section anchor, the issue, the evidence, and a recommended fix. A closing section lists what was **verified correct** so you know the surface that has already cleared review.

| Severity | Meaning |
|---|---|
| **P0** | Factual error or inconsistency that a peer scientist or the vendor would catch; fix before any external release. |
| **P1** | Defensibility / completeness gap; weakens the note but not strictly wrong. |
| **P2** | Polish, wording, or judgment call. |

---

## Findings summary

| ID | Sev | Section | One-line |
|---|---|---|---|
| F1 | P0 | §2.2, §4.3 vs §1.2, §7.1, §8.2 | Two incompatible projection-period conventions (`2021–2040…` vs `2020–2040…`) used across the note. |
| F2 | P0 | §8.2 | Thematic period `Current` is published but never defined anywhere in §2/§4. |
| F3 | P0 | App B (WSDI 6–18 row) | Band's confidence and "Used by" list **Investment**, which has no WSDI rule (it uses HWFI). |
| F4 | P0 | App B (CDD 30–90 row) | "Used by" omits **Investment**, whose dry-spell rule does use the 30–90 band. |
| F5 | P0 | §7 / App A | Four sectoral source metrics (CDD, R99p, SPI-3 month-count, R95p inter-annual variability) are scored in §7 but **defined nowhere** in the note. |
| F6 | P0 | throughout | Internal code/file/ticket identifiers leak into a note the author has directed must carry **no code references**. |
| F7 | P1 | §5.4 vs §7.4 | The 35 °C wet-bulb limit is attributed to Raymond (2020) and stated as fixed, yet §7.4 invokes its downward revision — internal tension + citation provenance. |
| F8 | P1 | §2.4 | JRC flood-data citation dated **2026** for a v2.1 product; reads as a future/placeholder date. |
| F9 | P1 | §3.4, §5.1 | Five validation figures and one threshold-curve figure are still `[FIGURE TO INSERT]`; several quantitative QA claims rest on them. |
| F10 | P1 | §7.4 | Provenance-tier prose generalizes "institutional onset" to TNx and crop-heat TXx bands whose onsets are research-derived, not institutional. |
| F15 | P1 | §4.3 | Santer/Hawkins–Sutton signal-vs-noise justification is borrowed from *global-mean* trend *detection* to license *local* period *averaging*; numbers verified correct, but scale/quantity mismatch overstates how cleanly 20-yr averaging isolates the forced signal at district/block scale. |
| F16 | P1 | §7.4 / App B | IMD heatwave band labels **45 °C as "severe"** — web-verified: 45 °C is IMD's *absolute heat-wave declaration* threshold; severe is departure-based or ≥47 °C. Cut-points fine, characterization wrong; recurs across 5 bundles. |
| F17 | P2 | §2.2 | NEX-GDDP-CMIP6 historical span is **1950–2014**, not the "1951–2014" stated (web-verified). |
| F18 | P1 | §5.1 | "hwfi/hwa require ≥5 consecutive days, **aligning with IMD heatwave criteria**" — web-verified: IMD's duration criterion is *two* days; the 5-day length is an IRT/ETCCDI choice, not IMD-anchored. |
| F19 | P2 | §5.4 | Stull (2011) validity range is **−20–50 °C** (note says 0–50) and MAE is **"< 0.3 °C"** (note states a sharper "0.28 °C"); web-verified. |
| F20 | P2 | App B | TNx 28 °C onset cites a "**≈ +9.8 %/°C** hot-night mortality inflection" with no in-note source; could not be web-verified — needs a citation or removal. |
| F21 | P2 | §3.4 | CanESM5 "310 km" is degree×111 km grid spacing; the **CMIP6 nominal resolution is 500 km** — clarify which is meant (web-verified). |
| F11 | P2 | §7.2 vs §2.1/§4.3 | "20+ model ensemble" (§7.2) vs the exact "24 GCMs" used everywhere else. |
| F12 | P2 | §3.3 | Grid-cell count "119 × 160" is internally ambiguous against the stated domain/resolution. |
| F13 | P2 | App B footnote | Exposes rule slug `cdd_ge_60` — a phantom-threshold slug the note's own §7.4 principle (5) decries. |
| F14 | P2 | §1, §8 | Framing polish for the audience; exemplar-fidelity notes vs Aqueduct/CRAVIS. |

---

## P0 — fix before release

### F1. Two incompatible projection-period conventions
The note labels the three future windows two different ways:

- **§2.2** (Analysis periods table) and **§4.3** (Analysis periods table): **2021–2040 / 2041–2060 / 2061–2080** (non-overlapping, gap years between windows).
- **§1.2** ("2020–2040 through 2060–2080"), **§7.1** ("future periods 2020–2040, 2040–2060, 2060–2080"), **§8.2** (engine/period table): **2020–2040 / 2040–2060 / 2060–2080** (shared endpoints — 2040 and 2060 each sit at both a window's end and the next window's start).

This is not cosmetic: it changes *which years are averaged* in the period-mean stage (§4.3 stage 2), and the shared-endpoint form is genuinely ambiguous about which window owns 2040/2060. **§4.3 even contradicts its own table** — the prose at the "Annual → period mean" step says "(e.g. 2020–2040)" immediately above a table that reads "2021–2040."

*Recommendation:* pick one convention and apply it in every section, including the worked prose. State the inclusive year span unambiguously (e.g. "2021–2040 inclusive = 20 years"), consistent with the baseline already described as "1990–2010 (21 years)" in §5.1. Note that §1/§7/§8 (the less-vetted sections) carry the rounded/overlapping form; §2/§4 carry the precise form — the divergence tracks authorship.

### F2. The `Current` period is published but undefined
§8.2's thematic engine table lists periods `1990-2010 (historical)`, **`Current`**, `2020-2040`, `2040-2060`, `2060-2080`. `Current` appears nowhere in §2.2 or §4.3, which enumerate only the baseline plus three futures. A reader cannot tell what observed/near-present window `Current` covers, what data drives it, or how it differs from the near-term projection.

*Recommendation:* define `Current` in §2.2 (span, source, scenario handling) or remove it from §8.2 if it is not actually a methodology-bearing output. This is a published column; it must be defined.

### F3. Appendix B WSDI (6–18 day) band wrongly lists Investment
The 6–18-day `wsdi_warm_spell_days` row in Appendix B reads confidence "**LOW (Ag/Health/Investment)**, MEDIUM (Life & Livelihood)" and "Used by: Agricultural, Health, **Investment**, Life & Livelihood."

The Investment / Financial bundle has **no warm-spell-duration rule** — its persistence rule is **HWFI** (`hwfi_tmean_90p`, 5–15-day band), shown correctly one row down and in the §7.5 Investment table. The WSDI band is used by Agricultural, Health, and Life & Livelihood only (3 bundles, not 4).

*Recommendation:* drop Investment from both the confidence parenthetical and the "Used by" list of the 6–18-day WSDI row → "LOW (Ag/Health), MEDIUM (Life & Livelihood)"; "Used by: Agricultural, Health, Life & Livelihood."

### F4. Appendix B CDD (30–90 day) band omits Investment
Conversely, the 30–90-day `pr_consecutive_dry_days_lt1mm` row lists "Used by: Industrial, Asset (Thermal), Asset (Hydropower)*" — but the **Investment** bundle's dry-spell rule also carries the 30–90 band (confirmed: its dry-spell rule sets onset 30 / saturation 90). §7.5's Investment table shows "Dry-spell water-stress pressure … 30–90 days." Investment is missing from the Appendix B "Used by."

*Recommendation:* add Investment → "Used by: Industrial, Investment, Asset (Thermal), Asset (Hydropower)*." (F3 and F4 are mirror-image membership errors — worth re-deriving every Appendix B "Used by" cell mechanically from the §7.5 tables.)

### F5. Sectoral source metrics are scored but never defined
§7 scores rules on metrics that **§5 and Appendix A never define**:

- `pr_consecutive_dry_days_lt1mm` (CDD / consecutive dry days) — a *headline* sectoral driver (it appears in Industrial, Investment, both Asset bundles, and Life & Livelihood) with no definition, units, or the `<1 mm` wet-day convention stated.
- `r99p_extreme_wet_precip` (Investment).
- `spi3_count_months_lt_minus1` (Asset–Thermal low-flow proxy) — distinct from the `spi3_count_events` / `max_spell` metrics that §5.3 *does* define.
- `r95p_interannual_variability` (Asset–Hydropower) — described only in passing in §7.5 as "coefficient of variation of yearly very-wet precipitation."

Appendix A's header scopes it to "every metric that appears in a *thematic* bundle weight entry," so the omission is by-design — but it leaves the note **not self-contained**: §7's quantitative basis can't be reconstructed from the document alone, which a peer reviewer will flag immediately.

*Recommendation:* add a short Appendix A.6 ("Sectoral-only source metrics") defining CDD, R99p, the SPI-3 month-count proxy, and the R95p inter-annual variability helper (definition, units, baseline). CDD at minimum is non-negotiable given its weight across five bundles.

### F6. Internal code / file / ticket identifiers leak into the note
Per your directive (prototype codebase, never user-facing), these references must be scrubbed. Unambiguous leaks:

| Where | Leak | Suggested replacement |
|---|---|---|
| §4.3, §6.2 | "`higher_is_worse = False` in the registry" / "registry directionality" | "metrics where lower values indicate greater hazard" |
| §6.3 | "drawn from the approved `Bundles_comp_Score.xlsx` workbook" | "drawn from the approved bundle-weight schedule" |
| §6.3 | "referenced in some internal configuration" | delete or rephrase as "in an earlier configuration variant" |
| §6.2, §7.2 | "exists in the codebase," "validated in the codebase" | "exists as a dormant capability," "specified but unused" |
| App A header | "appears in a thematic bundle weight entry in `bundle_weights.py`" | "appears in a thematic bundle weight schedule" |
| App B, §7.4 | "internal backlog BL-0020/BL-0021," ticket numbers | "tracked as deferred work" (no IDs) |
| §7.2 | "the tool-wide `mean` statistic" | "the ensemble-mean statistic" |
| App B footnote | rule slug `cdd_ge_60` (see also F13) | refer to it by role, not slug |

**Judgment call (decide once, apply globally):** the metric **slugs** in the §5/§6/§7 tables and all of Appendix A (`txx_annual_max`, `composite_heat_risk`, etc.) are also code identifiers. Exemplars differ here — Aqueduct exposes short indicator *codes* but not source-file names; CRAVIS exposes none. Options: (a) keep slugs as a reproducibility aid (most defensible for the metric tables, least for the `composite_*` slugs), (b) move slugs to a single appendix "machine identifier" column, or (c) drop them entirely in favor of labels. My recommendation: drop the `composite_*` slug columns (pure code), keep metric slugs only if you commit to them as a stable published vocabulary; otherwise demote to one appendix column. This needs your call before I touch the tables.

---

## P1 — defensibility & completeness

### F7. 35 °C wet-bulb limit: attribution and internal tension
§5.4 states "Raymond et al. (2020) demonstrated that wet-bulb temperatures above 35 °C are incompatible with sustained human activity." Two issues:
1. The 35 °C theoretical survivability limit originates with **Sherwood & Huber (2010)**; Raymond et al. (2020) documented its *occurrence/emergence*, not the limit itself. As written the attribution is imprecise.
2. §7.4 (principle 5, provenance discipline) cites "the downward revision of the once-canonical 35 °C wet-bulb survivability limit" as a cautionary precedent — i.e. the note elsewhere treats 35 °C as *revised down* (cf. Vecellio et al. 2022, empirical limits ≈31 °C). §5.4 presents 35 °C as settled. These two passages should be reconciled.

*Recommendation:* attribute the 35 °C limit to Sherwood & Huber (2010), cite Raymond (2020) for emergence, and add a half-sentence acknowledging the empirical downward revision the note already leans on in §7.4. (IRT's operational 28/30 °C thresholds are unaffected — this is about citation hygiene and self-consistency.)

### F8. JRC citation dated 2026
§2.4 / §3.4: "European Commission, Joint Research Centre. (2026). CEMS-GloFAS Global River Flood Hazard Maps Version 2.1." A 2026 publication year for a v2.1 product reads as a placeholder or a conflation of *access year* with *publication year* (the note itself is dated 2026-06-24). Verify the actual release year of v2.1 and cite it; if 2026 is the access date, label it as such.

### F9. Pending figures underpin quantitative claims
§3.4 carries five `[FIGURES TO INSERT]` (Taylor diagrams, nRMSE heatmap, Rx1day bar chart) and §5.1 one `[FIGURE TO INSERT]` (DOY threshold curve). Several §3.4 numeric claims — ERA5 domain-mean 27.3 °C, model range 26.8–28.0 °C, Adilabad Rx1day 77 mm vs 33–55 mm — are presented as fact but are currently unverifiable from the note and depend on figures not yet inserted. This is the single biggest "DRAFT" exposure for a scientist audience.

*Recommendation:* insert the figures (or temporary captions stating the source) before external release; until then the status banner correctly flags it, but the numeric claims should be marked provisional.

### F10. Provenance-tier prose over-claims "institutional onset"
§7.4 groups the MEDIUM-confidence bands as those "where an institutional onset can be borrowed — multi-week dry spells (CDD …), crop reproductive heat (TXx 35–45 °C), warmest-night stress (TNx 28–32 °C)." Only **CDD's** onset (IMD Agricultural-Drought ≈28 days) is institutional. The TXx 35–45 onset (reproductive-sterility threshold) and the TNx 28–32 onset (mortality inflection) are **research-derived**; Appendix B correctly labels both "Self-derived, MEDIUM." The §7.4 generalization is looser than its own Appendix.

*Recommendation:* soften §7.4 to "derived from literature and reasoned judgement, sometimes with an institutionally anchored cut point (e.g. CDD)," so it doesn't claim institutional onsets the bands don't have.

### F15. Signal-vs-noise justification: right numbers, wrong scale
§4.3's period-averaging rationale cites Santer et al. (2011), Hawkins & Sutton (2012), and Tebaldi & Knutti (2007). Citations and quantities check out:

- **All three references are real and correctly attributed.**
- **The Santer numbers are verbatim-correct** (web-verified against the paper): S/N "below 1 at 10-year timescales," "exceed 3.9 at 32-year trends," and "≥17 years" are exactly as the paper states. No change needed to the figures.

The defensibility gap is conceptual, not numerical — two mismatches between what Santer (2011) established and what §4.3 uses it for:

1. **Statistical quantity.** Santer's "17 years" is a *trend-detection* threshold (how long a record must be to distinguish a trend from internal-variability noise). §4.3 invokes it to justify a *period-mean* (mean estimation over a ~20-year window). Related questions, but not the same one.
2. **Spatial scale (the substantive one).** Santer's S/N ratios and the 17-year figure are for **global-mean lower-tropospheric temperature** — the web-verified quantity — where internal variability is heavily averaged down. At a single ~25 km grid cell or a district, interannual noise is proportionally *much* larger, so S/N is lower and time-of-emergence is *later* — frequently multiple decades for regional temperature and beyond a century for regional precipitation/extreme indices (the very Hawkins–Sutton ToE result §4.3 cites). The claim that a 20-year window "isolat[es] the underlying forced climate change signal" is therefore **optimistic at district/block scale**: 20-year averaging *dampens* interannual noise there but does not *isolate* the forced signal the way it does for global means. This also interacts with §3.4's own admission of large residual variability in extreme-rainfall indices.

As written, the paragraph reads as if the global-mean detection numbers license the local-averaging choice; for a scientist audience they motivate it but do not establish it.

*Recommendation:* keep the citations and the (correct) numbers; add one clause acknowledging scale, e.g. *"These detection-timescale results are derived for global-mean temperature; at the district and block scale internal variability is proportionally larger and emergence correspondingly later (§3.4), so multi-decadal averaging dampens — rather than fully removes — interannual noise in the local indices."* Optionally note that the 17-year figure is a *trend-detection* threshold used here to motivate *mean* estimation.

*Verification trail:* Santer et al. (2011), *J. Geophys. Res.* 116, D22105, doi:10.1029/2011JD016263 — S/N "less than 1 on the 10-year timescale, increasing to more than 3.9 for 32-year trends in lower tropospheric temperature"; "≥17 years … for identifying human effects on global-mean tropospheric temperature."

### F16. IMD heatwave band mislabels 45 °C as "severe" (web-verified, W1)
§7.4 and Appendix B describe the 40–45 °C TXx band as "IMD plains heatwave: declared ≥40 °C (onset), **severe ≥45 °C** (saturation)." Web-verified against IMD: 40 °C is the plains floor for heat-wave *consideration*, and **45 °C is the absolute temperature at which a heat wave is auto-declared "irrespective of normal" — not the *severe* threshold.** A *severe* heat wave is the departure-based criterion (≥6.5–7 °C above normal) or, on the actual-value basis, **≥47 °C**. The band cut-points (40, 45) are both genuine IMD numbers and the band itself is sound; only the word "severe" attached to 45 is wrong. This is the **heaviest-weighted external anchor in §7** (Health, Industrial, Infrastructure, both Asset bundles) and the mislabel repeats in every one plus Appendix B.

*Recommendation:* relabel the 45 °C saturation as the "absolute heat-wave declaration threshold (declared irrespective of normal)," not "severe." If a *severe* anchor is genuinely intended, IMD's actual-value severe threshold is **47 °C** — which would change the band to 40–47; decide which and apply consistently across §7.4, Appendix B, and the per-rule provenance text. HIGH-confidence grading remains warranted — both endpoints are IMD-anchored.

*Verification trail:* IMD Heat-Wave FAQ / NDMA — heat wave declared at Tmax ≥40 °C (plains); departure-based Heat Wave 4.5–6.4 °C and Severe ≥6.5–7 °C; "when actual max temperature is ≥45 °C irrespective of normal, heat wave should be declared." Severe actual-value commonly cited ≥47 °C.

### F18. "≥5 consecutive days aligning with IMD heatwave criteria" misattributes the spell length (web-verified, W5)
§5.1 states hwfi and hwa "require ≥5 consecutive days, aligning with India Meteorological Department heatwave criteria." Web-verified: **IMD's heat-wave duration criterion is *two* consecutive days** (declared on the second day), not five. The 5-day minimum spell is an IRT/ETCCDI-style design choice, not an IMD criterion — so the IMD attribution is unsupported.

*Recommendation:* drop the IMD attribution for the 5-day length, e.g. "hwfi and hwa require ≥5 consecutive days — an IRT design choice; note IMD's own heat-wave declaration uses a two-consecutive-day criterion." The ETCCDI ≥6-day attribution for WSDI/CSDI is correct and should stay.

*Verification trail:* IMD *Cold and Heat Wave Indices and Methodology* (Ch. 2); APSDMA heat-wave SOP — criteria "met for at least two consecutive days; heat wave declared on the second day."

---

## P2 — polish & exemplar fit

### F11. "20+ model ensemble" vs "24 GCMs"
§7.2 says inputs are reduced "from a 20+ model ensemble." Everywhere else (§2.1, §3.4, §4.3) the ensemble is precisely **24**. Use "24-model ensemble" for consistency.

### F17. NEX-GDDP-CMIP6 historical span is 1950–2014, not 1951 (web-verified, W3)
§2.2 lists the raw historical span as "1951–2014" and the prose says files span "1951–2100." Web-verified: the product covers **1950–2100** (historical **1950**–2014) per the NASA NCCS tech note and the dataset catalogue. Off-by-one on third-party coverage.

*Recommendation:* change to 1950–2014 / 1950–2100 in the §2.2 table and surrounding prose — unless IRT deliberately ingests from 1951, in which case state that as an IRT choice rather than the product's native span.

*Verification trail:* NASA NCCS NEX-GDDP-CMIP6 Tech Note; Google Earth Engine dataset catalog — "downscaled historical and future projections for 1950–2100."

### F19. Stull (2011) accuracy range and MAE slightly off (web-verified, W7)
§5.4 states the Stull approximation "has a mean absolute error of 0.28 °C … for the range 0 °C ≤ T ≤ 50 °C, 5 % ≤ RH ≤ 99 %." Web-verified against Stull (2011): the stated validity range is **−20 to +50 °C** (RH 5–99 %), "except for situations having both low humidity and cold temperature," with errors of **−1 to +0.65 °C and MAE < 0.3 °C**. Two small issues: (a) the note's "0.28 °C" is sharper than the paper's "< 0.3 °C" — fine as a value (0.28 ⊂ <0.3) but should be sourced if it comes from a specific re-evaluation, else use "< 0.3 °C"; (b) the lower bound 0 °C is a *narrowing* of Stull's −20 °C range, defensible for India but presented as if it were the paper's bound.

*Recommendation:* either cite Stull's range verbatim (−20–50 °C) or frame 0–50 °C explicitly as the India-relevant subset ("within the 0–50 °C range relevant to Indian conditions"), and consider noting the low-humidity/cold-temperature exclusion (which is *why* the 0 °C floor is harmless here). Align the MAE wording with the source.

*Verification trail:* Stull (2011), *J. Appl. Meteorol. Climatol.* 50(11):2267–2269 — "valid for relative humidities between 5 % and 99 % and for air temperatures between −20° and 50 °C, except … both low humidity and cold temperature … errors … range from −1° to +0.65 °C, with mean absolute error of less than 0.3 °C."

### F20. TNx mortality figure (+9.8 %/°C) is unsourced and unverifiable (web-verified attempt, W8)
Appendix B's 28–32 °C TNx band rationale anchors its 28 °C onset on an "India hot-night mortality inflection ≈ +9.8 %/°C." The figure has **no in-note citation**, and a literature search did not locate its source (India night-temperature/mortality work exists — e.g. nationally representative case-crossover studies — but not this specific elasticity). A naked quantitative epidemiological claim a reviewer cannot trace is a defensibility risk, even though it sits on a MEDIUM-confidence, modest-weight band.

*Recommendation:* supply the primary citation for the +9.8 %/°C figure, or replace it with a sourced statement of the hot-night mortality association and let the 28 °C onset rest on the qualitative inflection rather than an unverifiable number.

*Verification trail:* searched India night-time-temperature mortality literature (PLOS Medicine case-crossover and related) — the specific +9.8 %/°C inflection was not located; source required.

### F21. CanESM5 resolution: grid spacing vs CMIP6 nominal resolution (web-verified, W9)
§3.4 gives the ensemble's coarsest model as "310 km (CanESM5)." 310 km is the degree-to-km conversion of CanESM5's ~2.8° (T63) grid; the **official CMIP6 `nominal_resolution` attribute for CanESM5 is 500 km**. EC-Earth3's "~70 km" is likewise the grid-spacing reading of its ~0.7° (TL255 ≈ 80 km) grid. The numbers are internally consistent *as physical grid spacing* but will mismatch the CMIP6 metadata a reviewer looks up.

*Recommendation:* state that these are approximate physical grid spacings (Δ° × ~111 km), not CMIP6 nominal resolutions — or quote the nominal-resolution values (CanESM5 500 km) if that is the intended register. Pick one convention; the argument (convective scales unresolved at 100–300 km) holds either way.

*Verification trail:* CMIP6 `nominal_resolution` (PCMDI) — CanESM5 500 km (T63 ~2.8°); EC-Earth3 atmosphere ~80 km (TL255).

### F12. Grid-cell count ambiguity
§3.3 states the India clip yields "119 × 160 grid cells." The domain 68.0–97.5 °E spans 29.5° → 118 intervals (119 cell *edges*); 5.0–45.0 °N spans 40° → 160 intervals (161 edges). "119 × 160" mixes an edge count with an interval count. State it consistently (either 118 × 160 cells, or 119 × 161 grid points) and define which you mean.

### F13. Phantom-threshold slug exposed in Appendix B footnote
The Appendix B footnote names rule slug `cdd_ge_60` and notes "its slug threshold differs but the impact band is identical" (slug says 60, band is 30–90). This both leaks a code slug (F6) *and* showcases exactly the "phantom threshold" the note's §7.4 principle (5) prohibits ("a slug naming a number must implement that number or be renamed"). Rephrase the footnote by role and drop the slug; resolve the self-contradiction.

### F14. Framing & exemplar fidelity (§1, §8)
- **§1.2/§1.3 vs Aqueduct/CRAVIS:** the framing is strong. Aqueduct opens with an explicit "what's new vs prior versions" and a limitations-up-front posture; the note's §1.3 roadmap is good, but consider a short "intended use and misuse" callout in §1 (CRAVIS does this well) rather than deferring the whole hazard-≠-risk caveat to §7.1/§8.1. The hazard-pressure caveat is currently carried consistently — that's a strength; keep it.
- **§8.1 "what the composite is not"** is excellent and survives a hostile read; mirror that crispness into a one-paragraph §1 caveat so a policy reader who stops after §1 isn't misled.
- **Tone:** consistently rigorous and on-model for the stated audience. No drive-by jargon problems found.

---

## Verified correct (cleared review)

So you know what surface has already been checked against the prototype and passed:

- **All six thematic bundle weight tables (§6.4)** — every metric, group weight, share, and final weight matches the prototype, and each bundle sums to 1.000. Drought's 2/5–3/5 (event/spell) split and rising SPI-3<6<12 group weights are correct.
- **All eight sectoral bundles (§7.5)** — rule counts (7/5/4/5/3/3/3/4 = 34), every rule weight (each bundle sums to 1.00), every lens archetype (abs/chg/imp split), and every impact band `[a,b]` match the prototype rule-for-rule.
- **§7.2 normalization math** — sectoral absolute lens uses robust **p10–p90** rescaling (flat→50), change lens uses future−baseline with a 1e-6 small-denominator guard and `tas/tx/tn`-prefix (+"temperature"/"heat") auto-mode selection, impact lens is fixed-band linear, rule and bundle aggregation are renormalized weighted means, and the **0.70** available-rule-weight gate is real. All faithful.
- **§6.2 normalization math** — thematic min–max (p0–p100) with the "all-equal → 50" and "no-finite → NaN" degenerate handling matches.
- **§7.3 worked example** — both districts' blended scores recompute exactly (A: 76, B: 47) from the stated lenses and the Health TXx rule's 0.40/0.25/0.35 split and 40–45 band.
- **§5.4 Stull (2011) formula** — transcribed correctly term-for-term.
- **Scenario labels, JRC severity 5×5 matrix, Riverine weight-1.0/display-0.0 structure, and the lens-comparability table (§7.3)** — all consistent with the prototype and internally sound.

---

## Writing style & explainer commentary

This section is editorial (register, prose, pedagogy) rather than factual, and is kept separate from the P0–P2 findings above. None of it is structural — it is a tightening pass.

### Overall register
The note has found a good voice — "rigorous but explained" — which matches its stated dual audience and the Aqueduct/CRAVIS models. Its defining strength is the **worked example**: §4.1's urban/valley case (averaging 36–38 °C and 28–30 °C cells into a sub-threshold 32–34 °C district that reports *zero* hot days), §5.1's TX90p walk-through (1 May, 105 pooled values, τ₁₂₁ = 41.2 °C), and §7.3's District A/B blend. These three are the best writing in the document — concrete, correct, memorable — and are the register the rest of the note should standardize on. Wherever a section explains a concept abstractly, the fix is usually "add the §4.1-style example."

### Style issues worth a pass
1. **Paragraph density (highest-leverage fix).** Several explainers are single 250–300-word blocks that tax even a technical reader: §3.4's precipitation/dry-bias paragraph (BCSD → convective parameterization → Adilabad Rx1day in one breath) and §4.3's signal-vs-noise paragraph (Santer → Hawkins & Sutton → Tebaldi & Knutti in one block). The content is good; break each into 2–3 paragraphs with a topic sentence.
2. **Transition tic.** Every §5 subsection opens with a hand-off device — "Turning from temperature to the water cycle" (§5.2), "Where §5.2 captures rainfall excess…" (§5.3), "Returning to heat…" (§5.4), "The final hazard family departs from the climate grid entirely" (§5.5). One or two are elegant; four in a row reads as a mannerism. Keep one (e.g. §5.4) and make the others plain.
3. **Self-justifying adjectives.** The note sometimes tells the reader the method is good rather than letting the result show it: "a more stable hazard ranking," "the blend thus preserves three decision-relevant signals," "precisely the relative-shift signal the index is designed to capture." Aqueduct stays declarative. Trimming the editorializing ("precisely," "exactly," "genuine," "deliberate," "chief") reads less defensive and more authoritative.
4. **Emphasis density.** Bold and em-dashes are load-bearing in good places (**relative, not absolute**) but frequent enough to lose force. Reserve bold for the genuine "read-this-or-misunderstand" claims (the §8 comparability caveats) and let the prose carry the rest.
5. **Dual-register signposting.** The front-matter says math sits "with plain-language explanations alongside" — the right design, but it isn't signposted, so a policy reader hits the Stull equation (§5.4) or the gamma CDF (§5.3) with no cue that the derivation is skippable. Aqueduct boxes/shades its derivations. Visually setting off the heavy math (a "Derivation" callout or shaded block) would make the policy track and the scientist track separately navigable — low effort, large readability gain.

### §1 content
What works — keep as-is: §1.1 frames the subnational gap cleanly, §1.2's bullets are crisp, and the §1.3 roadmap table is genuinely useful.

What's missing or soft:
- **No "intended use / misuse" in prose** (ties to F14). A policy reader who reads only §1 leaves without the hazard-pressure-≠-risk caveat (currently parked in §7.1/§8.1). CRAVIS puts this up front. One paragraph in §1.2, or a short §1.4 "How to use these scores (and how not to)," would de-risk the most likely misread — treating a "Health Risk 80" as realised health risk.
- **Audience lives in metadata, not prose.** The note names its audience in the header block but never says, in the body, "this note supports decision X at administrative level Y; it does not support Z." Stating the supported (and unsupported) decisions explicitly is what makes the later comparability caveats land.
- **The comparative claim (§1.2).** "Existing global climate-risk indices operate at coarser spatial resolution and with less India-specific downscaling and hazard decomposition; IRT's contribution is to work at the administrative resolution at which Indian adaptation is actually planned." This is the intended generic-non-comparative posture (no products named), which is right — but it is still a claim a reviewer can poke ("less India-specific than *what*?"). Given the non-comparative intent, soften to a capability statement ("IRT is built to operate at ADM2/ADM3 resolution using India-context indices") rather than a comparison.

### Explainers elsewhere
- **§3.1 (what downscaling is)** and **§3.2's "what BCSD corrects and what it does not"** are strong — scoped, honest, the right altitude. Models for the rest.
- **§5.4 (humid-heat physiology)** is good pedagogy; just reconcile the 35 °C framing per F7.
- **§4.3 and §3.4** are the two explainers whose *content* is fine but whose *packaging* (the dense blocks in style-issue 1) undersells them.
- **Audience tension to make deliberate:** the note explains CMIP6, the r1i1p1f1 label, and CDFs (scientist-obvious) while also pitching examples at policy readers. Fine *if* it is an intentional dual-track — but then signpost it (style-issue 5) rather than letting the registers collide mid-paragraph.

**Net:** the bones and the examples are strong; the editorial work is compression (break the dense paragraphs), de-mannering (the transitions and editorializing), and pulling the use/misuse + audience framing forward into §1.

---

## Suggested fix order

1. **F1, F2** (period conventions + `Current`) — they touch §1/§2/§4/§7/§8 and affect interpretation; settle the convention first.
2. **F3, F4, F5** (Appendix B membership errors + undefined sectoral metrics) — mechanical, high-credibility-cost.
3. **F6** (code-reference scrub) — decide the slug judgment call, then sweep.
4. **F7–F10, F15, F16, F18** (citations, figures, provenance prose, signal-vs-noise scale caveat, IMD heatwave "severe" mislabel + IMD 5-day attribution).
5. **F11–F14, F17, F19–F21** (polish; incl. the 1950-vs-1951 span fix, Stull range, the unsourced TNx mortality figure, and the CanESM5 resolution convention).

I have not edited the note. On your go-ahead I can apply any subset as targeted replace-blocks (I'll need your decision on the F6 slug question first).

---

## Appendix R1 — Externally verifiable claims (web-check backlog)

Claims drawn from institutional thresholds, third-party product specs, or published literature — i.e. statements a reviewer can look up, distinct from the IRT-design claims in Appendix R2. Status `verified` = already web-checked in this review; `open` = not yet checked. The §3.4 Telangana QA figures are **not** here — they are checked against the project notebooks (F9), not the web.

### Tier 1 — load-bearing + plausibly wrong (verify first)

| # | Section | Claim | Why it matters / suspicion | Status |
|---|---|---|---|---|
| W1 | §7.4 / App B | IMD plains heatwave band **"declared ≥40 °C (onset), severe ≥45 °C (saturation)"** | HIGH-confidence anchor across 5 bundles. IMD's actual-value criterion is generally Tmax ≥45 = heat wave, ≥47 = severe (40 = plains consideration floor) plus a departure-based rule; "40 onset / 45 severe" may conflate the floor with the heat-wave threshold and mislabel 45 as "severe." | **ERROR → F16** |
| W2 | §7.4 / App B | IMD daily-rainfall categories **"very heavy 115.6–204.4 mm, extremely heavy ≥204.5 mm"** | The other HIGH-confidence anchor (5 bundles). Boundaries look right; confirm exact mm cut-points since the band is built on them. | **verified (correct)** |
| W3 | §2.2 | NEX-GDDP-CMIP6 historical span **"1951–2014"** | Product historical record is widely documented as starting **1950**; likely off-by-one on third-party coverage. | **ERROR → F17** |
| W4 | §3.2 | BCSD reference climatology **= GMFD (Sheffield et al. 2006)** | Provenance claim about NASA's bias-correction training obs; if it's actually ERA5/other, it undercuts §3. Verify against Thrasher et al. 2022. | **verified (correct)** |
| W5 | §5.1 | **"hwfi/hwa require ≥5 consecutive days, aligning with IMD heatwave criteria"** | IMD's heat-wave definition is threshold/departure-based, not a 5-consecutive-day spell rule; the IMD attribution may be unsupported (soften to "IRT design choice"). | **ERROR → F18** |

### Tier 2 — literature statistics & specs

| # | Section | Claim | Status |
|---|---|---|---|
| W6 | §3.4 | Jain et al. (2019): pattern corr ~0.8; RMSE ~4.25 °C (temp) / ~2.48 mm day⁻¹ (precip); 28 CMIP5 + 10 CORDEX models. | **verified (correct)** |
| W7 | §5.4 | Stull (2011): MAE **0.28 °C** over 0–50 °C, 5–99 % RH. | **partial → F19** (range −20–50; MAE "<0.3") |
| W8 | App B | TNx **"India hot-night mortality inflection ≈ +9.8 %/°C"** — specific elasticity, no in-note source. | **unverified → F20** (source not located) |
| W9 | §3.4 | Per-model GCM native resolutions (EC-Earth3 ~70 km finest, CanESM5 ~310 km coarsest, most 100–200 km). | **partial → F21** (CanESM5 nominal = 500 km) |
| W10 | refs | Citation metadata (vol/issue/page/DOI): Thrasher 2022 (*Sci. Data* 9:262), Zhang 2011 (*WIREs* 2(6):851–870), Jain 2019 (*Atmos. Res.* 228:152–160), Raymond 2020 (*Sci. Adv.* 6(19):eaaw1838), Stull 2011 (*JAMC* 50(11):2267–2269). | **verified (correct)** — Thrasher author list right ("Melton, F."), Stull/Jain vol/issue confirmed |
| — | §4.3 | Santer 2011 S/N (<1 @10 yr, >3.9 @32 yr, ≥17 yr) — global-mean LTT. | **verified (F15)** |

### Tier 3 — low risk / bounded

| # | Section | Claim | Status |
|---|---|---|---|
| W11 | §2.1 | SSP radiative forcing "~4.5 / ~8.5 W m⁻² by 2100." | open (low risk) |
| W12 | §4.2 | EPSG:6933 / EPSG:4326 descriptions. | open (trivial) |
| W13 | §2.1 | 24-GCM modelling-centre / country table. | open (low stakes) |

Already captured as findings, not repeated here: Raymond / 35 °C attribution (**F7**), JRC citation dated 2026 (**F8**).

---

## Appendix R2 — Internal / IRT-design claims (no web check applies)

Claims whose truth is defined by IRT's own design. Verification path is **(a) fidelity to the prototype config/compute** and **(b) internal consistency** — never the web. "✅ code-verified" = confirmed against the prototype in this review (see "Verified correct"); for the rest the open question is **defensibility, not factual accuracy**.

### A. Architecture & spatial/temporal aggregation (§3.3, §4)
- Grid-first-then-aggregate design + §4.1 worked example (admin-first → 0 hot days vs grid-first 2.5) — *arithmetic, self-checking*.
- Admin-first biases all non-linear indices (spells, percentile exceedance, SPI gamma) — methodological assertion.
- §4.2 fractional-area-overlap formula; tiles = cell-centre midpoints, δ=0.25°; reproject to EPSG:6933 before intersection.
- §4.2 district and block computed **independently** from the grid (two lookups) → district composite ≠ area-weighted mean of its blocks.
- §4.3 three-stage chain: daily→annual, annual→**unweighted** period mean, period→ensemble mean of 24; spread retained but **composite uses ensemble mean only**; time-average-then-ensemble-average ordering.
- §3.3 "4–20 cells/district, <4/block → higher block uncertainty" reasoning (and the 119×160 count, F12).

### B. Normalization & compositing rules
- §4.3 / §6.2 per-period spatial **min–max (p0–p100)**, invert for lower-is-worse, all-equal→50, no-finite→NaN. ✅ code-verified.
- §6.2 "relative not absolute / within-period comparable / 1990–2010 plays no role"; the **dormant baseline-anchored variant**. ✅ code-verified (exists, unused).
- §6.3 per-row-renormalized weighted mean; **≥1-component** gate; "≥4-anchored-components floor applies only to dormant mode." ✅ code-verified.
- §7.2 **robust p10–p90** absolute lens; change lens future−baseline (absolute_delta/relative_pct, `tas/tx/tn`-prefix auto, 1e-6 guard); linear impact band; renormalized rule/bundle means; **0.70 available-rule-weight gate**; ensemble-mean (median "preferred but pending"); `trend` rule exists/unused. ✅ all code-verified.
- §8.1 banding **low/mod/high at 33.3 / 66.6**; "what the composite is *not*" (not probability / not EAL / not IPCC-risk / partly relative).
- §8.3 district vs block normalized against different cohorts → "district 80 ≠ block 80."

### C. Weight & gate schedules (own authority, no external basis)
- §6.4 all six thematic weight tables (group weight × share; equal split except Drought 2/5–3/5). ✅ code-verified, each sums to 1.000.
- §7.5 all eight sectoral rule-weight tables + lens archetypes (0.40/0.25/0.35, 0.45/0.40/0.15, 0.70/0.30/0, 0.40/0.60/0). ✅ code-verified rule-for-rule.
- §5.3 "longer SPI timescales carry higher bundle weight" rationale.

### D. IRT-defined metrics & chosen thresholds (no external standard)
- **`hwa` heatwave amplitude** — note calls it "IRT-specific"; derivation (worst spell by mean exceedance → peak Tmax) is entirely internal.
- **All three JRC constructions (§5.5):** depth = block p95 of positive depths, district = flooded-area-weighted mean of block p95; extent = polygon area share; the **5×5 depth×extent severity matrix** and its **bin cut-points** (depth ≤0.2/0.5/1.0/2.5; extent ≤0.01/0.05/0.15/0.25) — IRT-invented, no external referent at all.
- **Twb thresholds 28/30 °C** "severe/very severe occupational" (Raymond gives 35).
- **Tropical-night split TN>25 vs TN>28** and cold thresholds TN≤10/≤5, TX≤15 "calibrated to Indian plains."
- **SPI annual reductions** `count_events_lt_minus1` / `max_spell_lt_minus1` (and the SPI<−1 onset) — IRT-defined on top of the McKee method.
- summer=MAM / winter=DJF conventions; baseline 1990–2010 as the single fixed window (and the doc-vs-code 1981–2010 gap).

### E. Self-derived impact bands (§7.4 / App B)
No external categorical referent by definition — only the *anchors* in the rationales are external; the cut-points are IRT's:
- Rx5day **250–500 mm**, WSDI **6–18 d**, damaging-heat **15–60 d**, SPI episodes/spells **3–12**, chilling nights **10–30 d**, CWD **7–15 d**, HWFI **5–15 d**, plus the saturation halves of the MEDIUM bands (TXx-crop 45, TNx 32, CDD 90/120). ✅ values code-verified; **defensibility is the open question, not correctness.**
- The HIGH/MEDIUM/LOW→impact-weight grading and the five provenance-discipline principles — internal policy.

### F. Scope & framing (§1, §8)
- Resolution choice (ADM2 + ADM3), two-SSP / multi-period design, six-thematic-+-eight-sectoral structure, the 0–100 higher-is-worse convention.
- Hazard-pressure-≠-risk framing; in/out-of-scope boundaries.
- §1.2 "existing global indices are coarser / less India-specific" — *intended non-comparative*, but the one framing claim with an implicit external referent; treat per F14.

**Reading these two appendices together:** every class of claim in the note now has a declared check route — Appendix R1 items go to the web (W1–W5 first), Appendix R2 items go to code-fidelity + internal-consistency (most already ✅), and the residual exposure for R2 is **defensibility** of the self-derived bands (E) and the JRC matrix/bins (D), which IRT must justify on its own authority.
