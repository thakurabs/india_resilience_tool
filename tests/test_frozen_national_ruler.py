"""Guards for the frozen national CDF ruler (CHG-0367a..d).

The frozen ruler is the transfer function every published ``composite_heat_risk``
score passes through. Every failure it can have is silent — a plausible number
under an unchanged column name — so each test here pins one property the
published score depends on:

- the exact mid-rank fit (ties, degenerate pools, clamping),
- the save/load round-trip, which is what makes "frozen" mean anything,
- the coverage denominator, which decides whether a partly-covered unit is
  published at all,
- the slice grid, which decides whether a unit is scored against a ruler that was
  never fitted for it,
- and the level contract: a district and a block holding the same physical value
  must receive the same score.
"""

from __future__ import annotations

import math

import numpy as np
import pandas as pd
import pytest

from india_resilience_tool.analysis.frozen_rulers import (
    CDF_KIND,
    FrozenRulerSet,
    MetricRuler,
    build_cdf_ruler,
    exact_midrank_cdf,
    frozen_ruler_dir,
    load_ruler_set,
    save_ruler_set,
    weighted_row_score,
)
from india_resilience_tool.config.bundle_weights import (
    get_bundle_baseline_referenced_slugs,
    get_bundle_headline_weight_total,
    get_bundle_headline_weights,
    validate_bundle_weights,
)
from india_resilience_tool.config.composite_metrics import COMPOSITES_BY_SLUG

SLICES = (
    ("historical", "1990-2010"),
    ("ssp585", "2040-2060"),
)


def _ruler_set(rulers, **overrides) -> FrozenRulerSet:
    kwargs = dict(
        ruler_id="test_cdf_v1",
        composite_slug="composite_test",
        bundle_domain="Test",
        level_fitted="district",
        rulers=rulers,
        slices=SLICES,
        weights={slug: 1.0 for slug in rulers},
        configured_weight=float(len(rulers)),
        coverage_gate=0.70,
    )
    kwargs.update(overrides)
    return FrozenRulerSet(**kwargs)


# ---------------------------------------------------------------------------
# The fit
# ---------------------------------------------------------------------------


def test_exact_midrank_scores_a_tied_block_at_its_true_midpoint() -> None:
    # 1 x 0, 4 x 5, 1 x 9. The tied block of four spans ranks 2..5 of 6, so its
    # mid-rank score is 100 * (1 + 4/2) / 6 = 50. A fixed quantile grid lands on
    # this only when a knot happens to fall on the tie boundary (P-05).
    pool = np.array([0.0, 5.0, 5.0, 5.0, 5.0, 9.0])
    values, scores, counts = exact_midrank_cdf(pool)

    assert values.tolist() == [0.0, 5.0, 9.0]
    assert counts.tolist() == [1, 4, 1]
    assert scores[1] == pytest.approx(50.0)
    # Open range: rank alone never awards a hard 0 or 100.
    assert 0.0 < scores[0] < scores[-1] < 100.0


def test_single_knot_ruler_scores_fifty() -> None:
    """A pool with one distinct value cannot rank anything; 50 is the honest answer."""
    ruler = build_cdf_ruler("flat", np.array([3.0, 3.0, 3.0]), higher_is_worse=True)
    assert ruler is not None
    assert ruler.knot_values.size == 1
    out = ruler.apply(pd.Series([1.0, 3.0, 99.0]))
    assert out.tolist() == [50.0, 50.0, 50.0]


def test_values_outside_fitted_support_clamp_and_are_counted() -> None:
    ruler = build_cdf_ruler("m", np.array([10.0, 20.0, 30.0]), higher_is_worse=True)
    assert ruler is not None
    values = pd.Series([5.0, 20.0, 40.0, float("nan")])

    scored = ruler.apply(values)
    assert scored.iloc[0] == pytest.approx(ruler.knot_scores[0])
    assert scored.iloc[2] == pytest.approx(ruler.knot_scores[-1])
    assert math.isnan(scored.iloc[3])
    assert 0.0 <= scored.dropna().min() and scored.dropna().max() <= 100.0

    clamped = ruler.clamped_mask(values)
    # NaN is not clamped, it is absent — the two must not be conflated.
    assert clamped.tolist() == [True, False, True, False]


def test_lower_is_worse_orientation_is_inverted_not_reversed() -> None:
    ruler = build_cdf_ruler("m", np.array([1.0, 2.0, 3.0]), higher_is_worse=False)
    assert ruler is not None
    scored = ruler.apply(pd.Series([1.0, 3.0]))
    assert scored.iloc[0] > scored.iloc[1]


def test_empty_pool_yields_no_ruler() -> None:
    assert build_cdf_ruler("m", np.array([np.nan, np.inf]), higher_is_worse=True) is None


# ---------------------------------------------------------------------------
# The artifact
# ---------------------------------------------------------------------------


def test_save_load_round_trip_reproduces_identical_scores(tmp_path) -> None:
    rng = np.random.default_rng(0)
    rulers = {
        "a": build_cdf_ruler("a", rng.normal(size=200), higher_is_worse=True),
        "b": build_cdf_ruler("b", rng.gamma(2.0, size=200), higher_is_worse=False),
    }
    original = _ruler_set(rulers, data_snapshot_hash="deadbeef")
    save_ruler_set(original, tmp_path / "cdf_v1")

    loaded = load_ruler_set(tmp_path / "cdf_v1")
    assert loaded.ruler_id == original.ruler_id
    assert loaded.slices == SLICES
    assert loaded.coverage_gate == pytest.approx(0.70)
    assert loaded.configured_weight == pytest.approx(original.configured_weight)
    assert loaded.data_snapshot_hash == "deadbeef"
    assert loaded.ruler_sha256  # the support file is hashed at save time

    probe = pd.Series(rng.normal(size=50))
    for slug in rulers:
        pd.testing.assert_series_equal(
            rulers[slug].apply(probe), loaded.rulers[slug].apply(probe)
        )


def test_saving_over_a_published_version_is_refused(tmp_path) -> None:
    """Refitting a version in place is how a frozen ruler silently changes."""
    rulers = {"a": build_cdf_ruler("a", np.arange(10.0), higher_is_worse=True)}
    save_ruler_set(_ruler_set(rulers), tmp_path / "cdf_v1")
    with pytest.raises(FileExistsError):
        save_ruler_set(_ruler_set(rulers), tmp_path / "cdf_v1")


def test_load_rejects_non_increasing_support(tmp_path) -> None:
    rulers = {"a": build_cdf_ruler("a", np.arange(10.0), higher_is_worse=True)}
    out = tmp_path / "cdf_v1"
    save_ruler_set(_ruler_set(rulers), out)
    support = pd.read_parquet(out / "cdf_support.parquet")
    support.loc[support.index[1], "support_value"] = support.loc[
        support.index[0], "support_value"
    ]
    support.to_parquet(out / "cdf_support.parquet", index=False)
    with pytest.raises(ValueError, match="strictly increasing"):
        load_ruler_set(out)


def test_off_grid_slice_is_refused(tmp_path) -> None:
    """P-04: a pair the ruler never saw must fail loudly, not score."""
    ruler_set = _ruler_set({"a": build_cdf_ruler("a", np.arange(10.0), higher_is_worse=True)})
    ruler_set.validate_slice("ssp585", "2040-2060")  # declared: no raise
    with pytest.raises(ValueError, match="not fitted over slice"):
        ruler_set.validate_slice("ssp370", "2080-2100")


# ---------------------------------------------------------------------------
# Coverage and the level contract
# ---------------------------------------------------------------------------


def test_coverage_denominator_is_configured_not_fitted_weight() -> None:
    """CHG-0350: a metric absent from the whole pool still owes the denominator.

    ``b`` fits nowhere, so only ``a`` is scored. Renormalizing over what fitted
    would report full coverage against half the bundle.
    """
    score_frame = pd.DataFrame({"a": [10.0, 20.0]})
    weights = pd.Series({"a": 0.5})

    _, fitted_only = weighted_row_score(score_frame, weights)
    _, configured = weighted_row_score(score_frame, weights, total_weight=1.0)

    assert fitted_only.tolist() == [1.0, 1.0]
    assert configured.tolist() == [0.5, 0.5]


def test_partial_row_renormalizes_over_available_metrics() -> None:
    score_frame = pd.DataFrame({"a": [10.0, np.nan], "b": [30.0, 40.0]})
    weights = pd.Series({"a": 0.5, "b": 0.5})
    score, coverage = weighted_row_score(score_frame, weights, total_weight=1.0)

    assert score.tolist() == [20.0, 40.0]
    assert coverage.tolist() == [1.0, 0.5]


def test_infinite_value_is_absent_not_available() -> None:
    """CHG-0351: +/-inf must not count as coverage the unit never receives."""
    score_frame = pd.DataFrame({"a": [np.inf], "b": [40.0]})
    weights = pd.Series({"a": 0.5, "b": 0.5})
    _, coverage = weighted_row_score(score_frame, weights, total_weight=1.0)
    assert coverage.tolist() == [0.5]


def test_same_physical_value_scores_the_same_at_district_and_block() -> None:
    """Decisions doc A5: blocks use the district-fitted ruler, never a refit."""
    ruler = build_cdf_ruler("m", np.linspace(0.0, 100.0, 101), higher_is_worse=True)
    assert ruler is not None
    district_values = pd.Series([42.0])
    # A block cohort with a completely different distribution around the same value.
    block_values = pd.Series([42.0, 41.0, 41.5, 43.0])

    assert ruler.apply(district_values).iloc[0] == pytest.approx(
        ruler.apply(block_values).iloc[0]
    )


# ---------------------------------------------------------------------------
# The configured lens split
# ---------------------------------------------------------------------------


def test_heat_risk_headline_is_nine_absolute_metrics_summing_to_point_six_three() -> None:
    headline = get_bundle_headline_weights("Heat Risk")
    baseline = get_bundle_baseline_referenced_slugs("Heat Risk")

    assert len(headline) == 9
    assert len(baseline) == 5
    assert set(baseline) == {
        "tn90p_warm_nights_pct",
        "tx90p_hot_days_pct",
        "wsdi_warm_spell_days",
        "hwfi_tmean_90p",
        "hwfi_events_tmean_90p",
    }

    total = get_bundle_headline_weight_total("Heat Risk")
    assert total == pytest.approx(0.6 + 1.0 / 30.0)  # 0.6333...
    # Renormalization at scoring time must take the published headline to exactly 1.
    assert sum(e.weight / total for e in headline) == pytest.approx(1.0)
    assert validate_bundle_weights() == []


def test_heat_risk_composite_spec_is_wired_to_the_frozen_ruler() -> None:
    spec = COMPOSITES_BY_SLUG["composite_heat_risk"]
    assert spec.normalization == "frozen_national_cdf"
    assert spec.frozen_ruler_version == "cdf_v1"
    assert len(spec.component_metric_slugs) == 14
    assert len(spec.headline_metric_slugs) == 9
    assert set(spec.headline_metric_slugs) <= set(spec.component_metric_slugs)


def test_committed_heat_risk_ruler_matches_its_recorded_hash_and_config() -> None:
    """The committed artifact is the published contract; drift here is silent."""
    spec = COMPOSITES_BY_SLUG["composite_heat_risk"]
    ruler_dir = frozen_ruler_dir(spec.composite_slug, spec.frozen_ruler_version)
    if not (ruler_dir / "ruler.json").exists():
        pytest.skip(f"No frozen ruler committed at {ruler_dir}")

    ruler_set = load_ruler_set(ruler_dir)
    from india_resilience_tool.analysis.frozen_rulers import sha256_file

    assert ruler_set.ruler_sha256 == sha256_file(ruler_dir / "cdf_support.parquet")
    assert set(ruler_set.rulers) == set(spec.headline_metric_slugs)
    assert ruler_set.configured_weight == pytest.approx(
        get_bundle_headline_weight_total("Heat Risk")
    )
    assert len(ruler_set.slices) == 7
    assert all(r.kind == CDF_KIND for r in ruler_set.rulers.values())


def test_manifest_payload_reports_the_frozen_ruler() -> None:
    from tools.optimized.build_processed_optimised import _frozen_ruler_manifest_payload

    payload = _frozen_ruler_manifest_payload()
    assert "composite_heat_risk" in payload
    entry = payload["composite_heat_risk"]
    if "error" in entry:
        pytest.skip("No frozen ruler committed yet")
    assert entry["headline_metric_count"] == 9
    assert entry["ruler_id"] == "composite_heat_risk_cdf_v1"
    assert entry["ruler_sha256"]
    assert len(entry["slices"]) == 7
    assert entry["fitted_slices"] == entry["slices"]
    assert entry["published_slices"] == entry["slices"]


def test_heat_risk_glance_remains_future_only_when_master_includes_historical(
    tmp_path, monkeypatch
) -> None:
    """CHG-0389 publishes baseline data without adding a Glance selector."""
    from india_resilience_tool.compute import glance_view_model
    from india_resilience_tool.config.dashboard_bundles import get_dashboard_bundle_spec

    spec = get_dashboard_bundle_spec("Heat Risk")
    assert spec is not None
    published_pairs = (
        ("historical", "1990-2010"),
        ("ssp245", "2020-2040"),
        ("ssp245", "2040-2060"),
        ("ssp245", "2060-2080"),
        ("ssp585", "2020-2040"),
        ("ssp585", "2040-2060"),
        ("ssp585", "2060-2080"),
    )
    monkeypatch.setattr(
        glance_view_model,
        "_available_pairs_for_slug",
        lambda _slug, *, data_dir: published_pairs,
    )

    observed = glance_view_model._bundle_pairs(spec, data_dir=tmp_path)

    assert ("historical", "1990-2010") not in observed
    assert observed == published_pairs[1:]
