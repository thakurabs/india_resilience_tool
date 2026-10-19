from india_resilience_tool.compute import gridfirst_spatial
from india_resilience_tool.compute import heat_risk_gridfirst


def test_heat_risk_reexports_shared_gridfirst_helpers():
    assert heat_risk_gridfirst.GridSpec is gridfirst_spatial.GridSpec
    assert heat_risk_gridfirst.build_area_weights is gridfirst_spatial.build_area_weights
    assert heat_risk_gridfirst.coverage_from_weights is gridfirst_spatial.coverage_from_weights
    assert heat_risk_gridfirst.dataset_grid_spec is gridfirst_spatial.dataset_grid_spec
    assert heat_risk_gridfirst.read_grid_metric_cache is gridfirst_spatial.read_grid_metric_cache
    assert heat_risk_gridfirst.write_grid_metric_cache is gridfirst_spatial.write_grid_metric_cache
    assert heat_risk_gridfirst.read_spatial_weights_cache is gridfirst_spatial.read_spatial_weights_cache
    assert heat_risk_gridfirst.write_spatial_weights_cache is gridfirst_spatial.write_spatial_weights_cache
