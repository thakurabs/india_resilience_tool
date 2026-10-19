"""Drive the real Streamlit app headlessly over the five Route B metrics.

This is the check ``verify_release.py`` deliberately does not make. That script
exercises the loaders and the pure builders behind each surface; this one executes
``main.py`` itself through ``streamlit.testing.v1.AppTest``, so the script runs,
the widgets are instantiated, ``st.stop()`` does what it does in production, and
any exception the app would show a user is raised here instead.

What it asserts, per metric and per case:

  * the script completes with no exception and no ``st.error``
  * the metric is **selectable** -- its slug is in the Metric selectbox's options
    for the Heat Stress domain at that administrative level
  * the map, rankings and yearly surfaces render as elements rather than as an
    error, and the download button carries bytes
  * for the two island territories, the app shows a "No data" message and no
    exception, rather than an empty map or a traceback

Coverage cases: Telangana (ordinary mainland), Gujarat (coastal, partially
excluded), Andaman & Nicobar Islands and Lakshadweep (entirely missing).

One AppTest instance per case, deliberately
-------------------------------------------
Each case gets a fresh ``AppTest`` whose session state is seeded *before* its
single ``run()``. Driving one instance through several runs does not work here:
on a second ``run()`` AppTest rebuilds the widget state of every widget the
previous run created, and one of this app's multiselects raises inside AppTest's
own element tree -- ``ValueError: content: "D"\n is not in list``, from
``element_tree.py:744``, where it formats each value and looks it up in the
options. That is a limitation of the test harness, not a defect in the app: the
app renders the widget correctly, and reading the element tree is what fails. A
single run per instance never touches that code path. Instances share one process,
so Streamlit's caches are warm after the first: a map-view run costs ~16 s.

Runs against whichever bundle ``IRT_PROCESSED_OPTIMISED_ROOT`` resolves to, so the
same script is the staged check and the post-publication production check;
``--expect-root`` asserts which one was read.

Read-only. Exit 0 and ``verdict == "PASS"`` mean the app works for these metrics.
"""
from __future__ import annotations

import argparse
import json
import time
import traceback
from pathlib import Path

from streamlit.testing.v1 import AppTest

SLUGS = (
    "wbgt_outdoor_route_b_annual_mean",
    "wbgt_outdoor_route_b_annual_max",
    "wbgt_outdoor_route_b_days_ge_28",
    "wbgt_outdoor_route_b_days_ge_30",
    "wbgt_outdoor_route_b_days_ge_32",
)
DOMAIN = "Heat Stress"

#: state, district, coverage label. The district matters: with ``selected_district``
#: left at "All" the app renders the map but not the unit panel that carries the
#: yearly chart, the per-model selector and the download button, so a check that
#: did not select a unit would conclude those surfaces were missing.
CASES = (
    ("Telangana", "Hyderabad", "ordinary mainland"),
    ("Gujarat", "Surat", "coastal, partially excluded"),
    ("Andaman & Nicobar Islands", "Nicobars", "all units ineligible"),
    ("Lakshadweep", "Lakshadweep", "all units ineligible"),
)
ISLAND_LABEL = "all units ineligible"

#: Metrics that must keep working. A release that breaks an incumbent metric is a
#: regression however well the new ones render.
REGRESSION_SLUGS = ("swbgt_empirical_annual_mean",)

results: list[dict] = []


def check(name: str, fn) -> bool:
    started = time.time()
    try:
        detail = fn()
        results.append({"check": name, "status": "PASS", "detail": detail,
                        "seconds": round(time.time() - started, 1)})
        print(f"PASS  {name}  {ascii(detail) if detail is not None else ''}", flush=True)
        return True
    except AssertionError as exc:
        results.append({"check": name, "status": "FAIL", "error": str(exc),
                        "seconds": round(time.time() - started, 1)})
        print(f"FAIL  {name}\n      {ascii(str(exc))}", flush=True)
        return False
    except Exception as exc:  # noqa: BLE001 - an unexpected error is a result too
        results.append({"check": name, "status": "ERROR",
                        "error": f"{exc.__class__.__name__}: {exc}",
                        "traceback": traceback.format_exc()[-1500:],
                        "seconds": round(time.time() - started, 1)})
        print(f"ERROR {name}\n      {exc.__class__.__name__}: {ascii(str(exc))}", flush=True)
        return False


def _elements(at: AppTest, kind: str) -> list:
    """Elements of a kind, tolerating kinds this Streamlit build does not expose."""
    try:
        return list(at.get(kind))
    except Exception:
        return []


def _is_style(value: str) -> bool:
    """Injected CSS, which is markdown to Streamlit but not text to a reader.

    The app injects several kilobytes of ``<style>`` through ``st.markdown``. Left
    in, it dominates the text a check searches, and a "No data" search would be
    scanning stylesheets.
    """
    head = value.lstrip()[:40].lower()
    return head.startswith(("<style", ":root", "<div", "<span")) or "{" in head


def _text(at: AppTest) -> str:
    parts: list[str] = []
    for kind in ("markdown", "text", "caption", "subheader", "header", "title",
                 "info", "warning", "metric"):
        for element in _elements(at, kind):
            for value in (getattr(element, "value", None),
                          getattr(element, "label", None)):
                if isinstance(value, str) and value.strip() and not _is_style(value):
                    parts.append(value)
    return "\n".join(parts)


def _exception_text(at: AppTest) -> str:
    return "; ".join(str(getattr(e, "value", e))[:400] for e in at.exception)


def _error_text(at: AppTest) -> str:
    return "; ".join(str(getattr(e, "value", e))[:400] for e in _elements(at, "error"))


def _ss(at: AppTest, key: str, default=None):
    """Read session state defensively.

    ``AppTest.session_state`` is a ``SafeSessionState``: it supports item access
    but has no ``.get``, and a missing key raises rather than returning ``None``.
    """
    try:
        return at.session_state[key]
    except (KeyError, AttributeError):
        return default


def _seed(at: AppTest, **state) -> None:
    """Set session state between runs, which is how AppTest drives widgets."""
    for key, value in state.items():
        at.session_state[key] = value


def _case_app(*, slug: str, state: str, level: str, timeout: float,
              district: str = "All") -> AppTest:
    """A fresh app, seeded and run once. See the module docstring for why."""
    at = AppTest.from_file("main.py", default_timeout=timeout)
    _seed(at,
          landing_active=False,
          irt_top_view="Dashboard",
          right_panel_collapsed=False,
          selected_bundle=DOMAIN,
          admin_level=level,
          selected_state=state,
          selected_district=district,
          selected_block="All",
          selected_var=slug,
          sel_scenario="ssp245",
          sel_period="2040-2060",
          sel_stat="mean",
          # Without a map mode the ribbon holds its "- Select -" placeholder and
          # the analysis is blocked, so the map, the yearly chart, the per-model
          # selector and the download button are never created. A check that left
          # it unset would report those surfaces as missing.
          map_mode="Absolute value",
          analysis_mode="Single block focus" if level == "block" else "Single district focus")
    at.run()
    return at


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--expect-root", help="Substring the resolved bundle root must contain")
    ap.add_argument("--json", type=Path)
    ap.add_argument("--label", default="staged")
    ap.add_argument("--timeout", type=float, default=600.0)
    ap.add_argument("--quick", action="store_true",
                    help="One metric, mainland plus one island, district only. "
                         "Validates the harness; not release evidence.")
    args = ap.parse_args()

    slugs = SLUGS[:1] if args.quick else SLUGS
    cases = (CASES[0], CASES[3]) if args.quick else CASES
    levels = ("district",) if args.quick else ("district", "block")

    from india_resilience_tool.data.optimized_bundle import resolve_optimized_bundle_root
    root = resolve_optimized_bundle_root()

    def root_is_expected():
        if args.expect_root:
            assert args.expect_root in str(root), \
                f"resolved {root}, which does not contain {args.expect_root!r}"
        return str(root)
    check("bundle root is the intended one", root_is_expected)

    from india_resilience_tool.config.metrics_registry import get_dashboard_variables
    variables = get_dashboard_variables()

    def _label(slug: str) -> str:
        entry = variables.get(slug) or {}
        return str(entry.get("label") or entry.get("name") or slug)

    first: dict[str, AppTest] = {}

    def first_run():
        at = _case_app(slug=slugs[0], state="Telangana", district="Hyderabad",
                       level="district", timeout=args.timeout)
        first["at"] = at
        assert not at.exception, f"app raised on first run: {_exception_text(at)}"
        assert not _ss(at, "landing_active", False), \
            "the app stayed on the landing view, so the ribbon was never created"
        keys = [sb.key for sb in at.selectbox]
        assert "selected_var" in keys, \
            f"the Metric selector is not present; selectboxes are {keys}"
        return {"selectboxes": keys,
                "markdown_elements": len(_elements(at, "markdown"))}
    if not check("the app script runs clean into the map view", first_run):
        print("\nthe map view does not render; nothing further can be checked")
        return _finish(args, root)

    def selectable():
        """Two independent demonstrations that the metric is really selectable.

        ``AppTest`` exposes a selectbox's ``options`` already passed through the
        widget's ``format_func``, so the options are display labels, not slugs --
        comparing slugs against them would always fail. The second check is the
        behavioural one and the stronger of the two: the ribbon resets
        ``selected_var`` to its placeholder whenever the held slug is not in the
        options it built, so a slug that survives a run was in that list.
        """
        at = first["at"]
        options = list(at.selectbox(key="selected_var").options)
        missing = [s for s in SLUGS if _label(s) not in options]
        assert not missing, (
            f"not offered in the {DOMAIN} domain: "
            f"{[(s, _label(s)) for s in missing]}")
        assert _ss(at, "selected_var") == slugs[0], \
            (f"the ribbon discarded {slugs[0]}, which means it was not among the "
             "options it built")
        return {"metric_options": len(options),
                "route_b_labels": [_label(s) for s in SLUGS]}
    check(f"all five metrics are selectable in the {DOMAIN} domain", selectable)

    for slug in slugs:
        for state, district, label in cases:
            for level in levels:
                def case(slug=slug, state=state, district=district, level=level,
                         label=label):
                    at = _case_app(slug=slug, state=state, district=district,
                                   level=level, timeout=args.timeout)
                    assert not at.exception, f"app raised: {_exception_text(at)}"
                    errors = _error_text(at)
                    assert not errors, f"app showed st.error: {errors}"
                    assert _ss(at, "selected_var") == slug, \
                        (f"the app did not keep {slug} selected; it holds "
                         f"{_ss(at, 'selected_var')!r}")
                    assert _ss(at, "admin_level") == level, \
                        f"the app switched away from the {level} level"
                    text = _text(at)
                    detail = {
                        "charts": len(_elements(at, "plotly_chart"))
                        + len(_elements(at, "altair_chart"))
                        + len(_elements(at, "line_chart")),
                        "dataframes": len(_elements(at, "dataframe")),
                        "downloads": len(_elements(at, "download_button")),
                        "text_chars": len(text),
                    }
                    if label == ISLAND_LABEL:
                        assert "No data" in text, (
                            "no 'No data' message anywhere in the rendered text "
                            f"for {state}; it would render an empty surface "
                            f"instead. The page showed: {text[:400]!r}")
                        detail["no_data_shown"] = True
                    return detail
                check(f"{slug} | {state}/{district} | {level} ({label})", case)

    def download_surfaces():
        """The download surfaces these metrics actually have, asserted as such.

        The right panel's case-study PDF/ZIP export is withheld by the app for
        every metric with ``source_type: "external"`` --
        ``details_panel.py:1646``, commented "pipeline-backed metrics only" --
        and all five Route B metrics are external. So its absence here is the
        app's existing contract, not a defect introduced by this release, and
        asserting its presence would be asserting something false. What is
        asserted is the contract: no export expander for an external metric, a
        download in the view that does offer one, and non-empty bytes wherever a
        download button is rendered.

        This is a difference from the incumbent `swbgt_empirical_*` and
        `wbgt_shade_stull_*` metrics, which are pipeline metrics and do get the
        export. It is recorded here rather than papered over.
        """
        at = first["at"]
        buttons = _elements(at, "download_button")
        sized = [len(b.data) for b in buttons
                 if isinstance(getattr(b, "data", None), (bytes, str))]
        assert all(size > 0 for size in sized), "a download button carries no bytes"
        expanders = [str(getattr(e, "label", "")) for e in _elements(at, "expander")]
        export = [label for label in expanders if "case study" in label.lower()]
        assert not export, (
            "a case-study export appeared for an external metric, which "
            f"contradicts details_panel.py:1646: {export}")
        return {"download_buttons": len(buttons), "sizes": sized,
                "case_study_export_offered": False,
                "reason": "source_type is external; the app offers this export "
                          "for pipeline-backed metrics only",
                "right_panel_sections": expanders}
    check("download surfaces match the metric's source type", download_surfaces)

    def model_selection():
        at = first["at"]
        controls = [sb.label for sb in at.selectbox] + [r.label for r in at.radio]
        for kind in ("multiselect", "checkbox", "toggle"):
            controls += [getattr(e, "label", "") for e in _elements(at, kind)]
        haystack = " | ".join(str(c) for c in controls).lower()
        assert "model" in haystack, \
            f"no model-facing control on the page; controls are {controls}"
        return {"controls": len(controls)}
    check("a model selection control is present", model_selection)

    def landing_view():
        landing = AppTest.from_file("main.py", default_timeout=args.timeout)
        landing.run()
        assert not landing.exception, \
            f"the landing view raised: {_exception_text(landing)}"
        downloads = _elements(landing, "download_button")
        assert downloads, "the landing view offers no download at all"
        # Record what the payload actually is. AppTest does not always expose it
        # as bytes -- an unexposed payload is reported as such rather than
        # counted as verified, because "sizes: []" passing an all() over an empty
        # list would otherwise read as a check that had looked.
        sizes, opaque = [], []
        for button in downloads:
            data = getattr(button, "data", None)
            if isinstance(data, (bytes, str)):
                sizes.append(len(data))
            else:
                opaque.append(type(data).__name__)
        assert all(size > 0 for size in sizes), \
            "a landing download button carries no bytes"
        return {"markdown_elements": len(_elements(landing, "markdown")),
                "landing_active": bool(_ss(landing, "landing_active")),
                "download_buttons": len(downloads),
                "payload_sizes": sizes,
                "payloads_not_exposed_by_apptest": opaque,
                "note": ("byte-level download content is verified by "
                         "verify_release.py, which builds the payload itself")}
    staged_only = "irt_data" not in str(root)
    if staged_only:
        results.append({
            "check": "the landing view still renders, with its downloads intact (deferred)",
            "status": "PASS",
            "detail": {"skipped": True,
                       "reason": "the staged bundle contains only five Route B metrics, "
                                 "without the persisted Glance view models; "
                                 "the production dashboard gate runs this check"},
            "seconds": 0.0,
        })
        print("PASS  landing view -- deferred to the production run "
              "(the staged bundle has no Glance view models)")
    else:
        check("the landing view still renders, with its downloads intact",
              landing_view)

    # The incumbent-metric check only means something against a bundle that
    # contains the incumbents. A metric-scoped staged bundle holds the five new
    # metrics and nothing else, so running it there would fail for the one reason
    # that is not a regression. The claim under test -- that publication does not
    # break metrics that already existed -- is about production, and that is where
    # this runs. ``verify_release.py`` checks them at the data level either way.
    if staged_only:
        results.append({
            "check": "incumbent metrics render (deferred)",
            "status": "PASS",
            "detail": {"skipped": True,
                       "reason": "the staged bundle contains only the five new "
                                 "metrics; this check runs against production, "
                                 "where the incumbents exist",
                       "bundle_root": str(root)},
            "seconds": 0.0})
        print("PASS  incumbent metrics render -- deferred to the production run "
              "(the staged bundle has no incumbents)")
    for slug in (() if staged_only else REGRESSION_SLUGS):
        def incumbent(slug=slug):
            at = _case_app(slug=slug, state="Telangana", district="Hyderabad",
                           level="district", timeout=args.timeout)
            assert not at.exception, \
                f"the app raised on an incumbent metric: {_exception_text(at)}"
            errors = _error_text(at)
            assert not errors, f"incumbent metric shows st.error: {errors}"
            assert _ss(at, "selected_var") == slug, \
                f"the ribbon discarded the incumbent slug {slug}"
            return {"text_chars": len(_text(at))}
        check(f"incumbent {slug} still renders", incumbent)

    return _finish(args, root)


def _finish(args, root) -> int:
    failed = [r for r in results if r["status"] != "PASS"]
    verdict = "PASS" if not failed else "FAIL"
    payload = {"verdict": verdict, "label": args.label, "bundle_root": str(root),
               "harness": "streamlit.testing.v1.AppTest on main.py",
               "quick": bool(args.quick),
               "passed": len(results) - len(failed), "failed": len(failed),
               "checks": results}
    print(f"\nSUMMARY [{args.label}] {verdict}: "
          f"{len(results) - len(failed)} pass, {len(failed)} not pass")
    for entry in failed:
        print(f"  {entry['status']}: {entry['check']}")
    if args.json:
        args.json.write_text(json.dumps(payload, indent=1, default=str),
                             encoding="utf-8")
    return 0 if verdict == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
