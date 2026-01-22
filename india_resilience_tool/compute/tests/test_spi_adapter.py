"""
Tests for the SPI Adapter module.

Run with: pytest tests/test_spi_adapter.py -v

Author: Abu Bakar Siddiqui Thakur
Email: absthakur@resilience.org.in
"""

import numpy as np
import pandas as pd
import pytest
import xarray as xr

from india_resilience_tool.compute.spi_adapter import (
    CLIMATE_INDICES_AVAILABLE,
    Distribution,
    SPIResult,
    _annualize_spi_xarray,
    _validate_monthly_data,
    compare_spi_implementations,
    compute_spi_climate_indices,
    compute_spi_for_unit,
    compute_spi_rows_climate_indices,
)


# Skip all tests if climate-indices is not available
pytestmark = pytest.mark.skipif(
    not CLIMATE_INDICES_AVAILABLE,
    reason="climate-indices package not installed"
)


# -----------------------------------------------------------------------------
# Fixtures
# -----------------------------------------------------------------------------

@pytest.fixture
def synthetic_monthly_precip():
    """Generate 30 years of synthetic monthly precipitation."""
    np.random.seed(42)
    n_months = 360  # 30 years
    
    # Gamma distribution is typical for precipitation
    precip = np.random.gamma(shape=2, scale=50, size=n_months)
    
    # Add some seasonality
    month_factors = np.tile([0.5, 0.6, 0.8, 1.2, 1.5, 2.0, 2.5, 2.2, 1.8, 1.2, 0.8, 0.6], 30)
    precip = precip * month_factors
    
    # Create time coordinate
    times = pd.date_range("1981-01-01", periods=n_months, freq="MS")
    
    return xr.DataArray(
        precip,
        coords={"time": times},
        dims=["time"],
        name="pr",
    )


@pytest.fixture
def synthetic_historical_precip():
    """Generate historical period (1981-2010) precipitation."""
    np.random.seed(42)
    n_months = 360  # 30 years
    precip = np.random.gamma(shape=2, scale=50, size=n_months)
    times = pd.date_range("1981-01-01", periods=n_months, freq="MS")
    return xr.DataArray(precip, coords={"time": times}, dims=["time"], name="pr")


@pytest.fixture
def synthetic_ssp_precip():
    """Generate SSP scenario period (2020-2050) precipitation."""
    np.random.seed(123)
    n_months = 372  # 31 years
    # Slightly increased precipitation for future scenario
    precip = np.random.gamma(shape=2, scale=55, size=n_months)
    times = pd.date_range("2020-01-01", periods=n_months, freq="MS")
    return xr.DataArray(precip, coords={"time": times}, dims=["time"], name="pr")


@pytest.fixture
def spi_metric_definition():
    """Sample metric definition for SPI3."""
    return {
        "name": "Standardised Precipitation Index 3-month (SPI3)",
        "slug": "spi3_drought_index",
        "var": "pr",
        "value_col": "spi3_index",
        "units": "index",
        "compute": "standardised_precipitation_index",
        "params": {
            "scale_months": 3,
            "baseline_years": (1981, 2010),
            "distribution": "gamma",
            "min_months_per_year": 9,
        },
        "group": "rain",
    }


# -----------------------------------------------------------------------------
# Test: Data Validation
# -----------------------------------------------------------------------------

class TestDataValidation:
    """Tests for data validation functions."""
    
    def test_validate_valid_data(self, synthetic_monthly_precip):
        """Valid data should pass validation."""
        is_valid, msg = _validate_monthly_data(synthetic_monthly_precip)
        assert is_valid is True
        assert msg == ""
    
    def test_validate_none_data(self):
        """None data should fail validation."""
        is_valid, msg = _validate_monthly_data(None)
        assert is_valid is False
        assert "None" in msg
    
    def test_validate_insufficient_months(self):
        """Data with too few months should fail."""
        times = pd.date_range("2020-01-01", periods=12, freq="MS")
        short_data = xr.DataArray(
            np.random.rand(12) * 100,
            coords={"time": times},
            dims=["time"]
        )
        is_valid, msg = _validate_monthly_data(short_data, min_months=24)
        assert is_valid is False
        assert "Insufficient" in msg
    
    def test_validate_all_nan_data(self):
        """All-NaN data should fail validation."""
        times = pd.date_range("2020-01-01", periods=36, freq="MS")
        nan_data = xr.DataArray(
            np.full(36, np.nan),
            coords={"time": times},
            dims=["time"]
        )
        is_valid, msg = _validate_monthly_data(nan_data)
        assert is_valid is False
        assert "NaN" in msg


# -----------------------------------------------------------------------------
# Test: Low-level SPI computation
# -----------------------------------------------------------------------------

class TestComputeSPIClimateIndices:
    """Tests for the low-level compute_spi_climate_indices function."""
    
    def test_basic_spi_computation(self, synthetic_monthly_precip):
        """Basic SPI computation should return valid values."""
        precip_values = synthetic_monthly_precip.values.astype(np.float64)
        
        spi = compute_spi_climate_indices(
            monthly_precip=precip_values,
            data_start_year=1981,
            calibration_start_year=1981,
            calibration_end_year=2010,
            scale_months=3,
            distribution=Distribution.GAMMA,
        )
        
        assert spi is not None
        assert len(spi) == len(precip_values)
        # First (scale-1) values should be NaN
        assert np.isnan(spi[:2]).all()
        # Rest should be finite (allowing for edge cases)
        valid_spi = spi[~np.isnan(spi)]
        assert len(valid_spi) > 0
    
    def test_spi_scale_6(self, synthetic_monthly_precip):
        """SPI-6 should have more leading NaNs."""
        precip_values = synthetic_monthly_precip.values.astype(np.float64)
        
        spi = compute_spi_climate_indices(
            monthly_precip=precip_values,
            data_start_year=1981,
            calibration_start_year=1981,
            calibration_end_year=2010,
            scale_months=6,
            distribution=Distribution.GAMMA,
        )
        
        # First 5 values should be NaN for 6-month scale
        assert np.isnan(spi[:5]).all()
    
    def test_spi_pearson_distribution(self, synthetic_monthly_precip):
        """Pearson Type III distribution should work."""
        precip_values = synthetic_monthly_precip.values.astype(np.float64)
        
        spi = compute_spi_climate_indices(
            monthly_precip=precip_values,
            data_start_year=1981,
            calibration_start_year=1981,
            calibration_end_year=2010,
            scale_months=3,
            distribution=Distribution.PEARSON,
        )
        
        assert spi is not None
        valid_spi = spi[~np.isnan(spi)]
        assert len(valid_spi) > 0
    
    def test_spi_values_in_expected_range(self, synthetic_monthly_precip):
        """SPI values should typically be in [-3, 3] range."""
        precip_values = synthetic_monthly_precip.values.astype(np.float64)
        
        spi = compute_spi_climate_indices(
            monthly_precip=precip_values,
            data_start_year=1981,
            calibration_start_year=1981,
            calibration_end_year=2010,
            scale_months=3,
            distribution=Distribution.GAMMA,
        )
        
        valid_spi = spi[~np.isnan(spi)]
        # Most values should be within [-3, 3], but extreme values are possible
        assert np.percentile(valid_spi, 1) >= -4.0
        assert np.percentile(valid_spi, 99) <= 4.0


# -----------------------------------------------------------------------------
# Test: High-level SPI computation for units
# -----------------------------------------------------------------------------

class TestComputeSPIForUnit:
    """Tests for compute_spi_for_unit function."""
    
    def test_spi_for_unit_historical(self, synthetic_monthly_precip):
        """SPI computation for historical scenario (self-calibration)."""
        result = compute_spi_for_unit(
            monthly_precip=synthetic_monthly_precip,
            calibration_monthly_precip=None,
            baseline_years=(1981, 2010),
            scale_months=3,
            distribution=Distribution.GAMMA,
        )
        
        assert result is not None
        assert isinstance(result, SPIResult)
        assert result.scale_months == 3
        assert result.distribution == Distribution.GAMMA
        assert result.calibration_years == (1981, 2010)
        assert result.valid_months > 0
        
        # Check annual SPI
        assert "year" in result.annual_spi.dims
        assert result.annual_spi.sizes["year"] > 0
    
    def test_spi_for_unit_ssp_with_historical_calibration(
        self, synthetic_historical_precip, synthetic_ssp_precip
    ):
        """SPI for SSP scenario should use historical data for calibration."""
        result = compute_spi_for_unit(
            monthly_precip=synthetic_ssp_precip,
            calibration_monthly_precip=synthetic_historical_precip,
            baseline_years=(1981, 2010),
            scale_months=3,
            distribution=Distribution.GAMMA,
        )
        
        assert result is not None
        # Data years should be from SSP period
        assert result.data_years[0] >= 2020
    
    def test_spi_for_unit_returns_none_for_invalid_data(self):
        """Should return None for invalid input data."""
        times = pd.date_range("2020-01-01", periods=6, freq="MS")
        too_short = xr.DataArray(
            np.random.rand(6) * 100,
            coords={"time": times},
            dims=["time"]
        )
        
        result = compute_spi_for_unit(
            monthly_precip=too_short,
            calibration_monthly_precip=None,
            baseline_years=(2020, 2020),
            scale_months=3,
        )
        
        assert result is None


# -----------------------------------------------------------------------------
# Test: Annual aggregation
# -----------------------------------------------------------------------------

class TestAnnualizeSPI:
    """Tests for SPI annualization."""
    
    def test_annualize_basic(self, synthetic_monthly_precip):
        """Basic annualization should work."""
        # Create mock monthly SPI
        np.random.seed(42)
        spi_values = np.random.randn(360)
        spi_monthly = xr.DataArray(
            spi_values,
            coords={"time": synthetic_monthly_precip["time"]},
            dims=["time"]
        )
        
        annual = _annualize_spi_xarray(spi_monthly, min_months_per_year=9)
        
        assert annual is not None
        assert "year" in annual.dims
        # Should have 30 years
        assert annual.sizes["year"] == 30
    
    def test_annualize_respects_min_months(self):
        """Years with too few months should be excluded."""
        # Create data with some months missing
        times = pd.date_range("2020-01-01", periods=18, freq="MS")  # 1.5 years
        values = np.random.randn(18)
        values[12:] = np.nan  # Last 6 months are NaN
        
        spi_monthly = xr.DataArray(values, coords={"time": times}, dims=["time"])
        
        # With min_months=12, second year should be excluded
        annual = _annualize_spi_xarray(spi_monthly, min_months_per_year=12)
        
        # Only first year should pass
        assert annual is not None
        assert annual.sizes["year"] == 1
        assert 2020 in annual["year"].values


# -----------------------------------------------------------------------------
# Test: Pipeline integration
# -----------------------------------------------------------------------------

class TestComputeSPIRowsClimateIndices:
    """Tests for pipeline-compatible SPI row computation."""
    
    def test_rows_format(self, synthetic_monthly_precip, spi_metric_definition):
        """Rows should have expected format for pipeline."""
        # Create mock data structures
        unit_key = "TestDistrict"
        scen_monthly = {unit_key: synthetic_monthly_precip}
        calib_monthly = scen_monthly  # Same for historical
        masks = {unit_key: xr.DataArray([True])}  # Dummy mask
        
        rows = compute_spi_rows_climate_indices(
            metric=spi_metric_definition,
            model="TestModel",
            scenario="historical",
            scenario_conf={"periods": {"1990-2010": (1990, 2010)}},
            scen_monthly_by_unit=scen_monthly,
            calib_monthly_by_unit=calib_monthly,
            masks=masks,
            level="district",
            baseline_years=(1981, 2010),
            scale_months=3,
            year_to_paths={},
        )
        
        assert len(rows) > 0
        
        # Check row structure
        row = rows[0]
        assert "year" in row
        assert "value" in row
        assert "spi3_index" in row  # value_col from metric
        assert "district" in row
        assert row["district"] == "TestDistrict"
    
    def test_rows_block_level(self, synthetic_monthly_precip, spi_metric_definition):
        """Block-level rows should include both district and block."""
        unit_key = "ParentDistrict||TestBlock"
        scen_monthly = {unit_key: synthetic_monthly_precip}
        masks = {unit_key: xr.DataArray([True])}
        
        rows = compute_spi_rows_climate_indices(
            metric=spi_metric_definition,
            model="TestModel",
            scenario="historical",
            scenario_conf={"periods": {}},
            scen_monthly_by_unit=scen_monthly,
            calib_monthly_by_unit=scen_monthly,
            masks=masks,
            level="block",
            baseline_years=(1981, 2010),
            scale_months=3,
            year_to_paths={},
        )
        
        assert len(rows) > 0
        row = rows[0]
        assert row["district"] == "ParentDistrict"
        assert row["block"] == "TestBlock"


# -----------------------------------------------------------------------------
# Test: Distribution comparison
# -----------------------------------------------------------------------------

class TestDistributionComparison:
    """Tests for comparing gamma vs pearson distributions."""
    
    def test_compare_implementations(self, synthetic_monthly_precip):
        """Compare gamma and pearson should return comparison stats."""
        results = compare_spi_implementations(
            monthly_precip=synthetic_monthly_precip,
            baseline_years=(1981, 2010),
            scale_months=3,
        )
        
        assert "gamma" in results
        assert "pearson" in results
        
        # Both should succeed
        assert results["gamma"] is not None
        assert results["pearson"] is not None
        
        # Should have comparison stats
        assert "comparison" in results
        assert "correlation" in results["comparison"]
        # Correlation should be high (same underlying data)
        assert results["comparison"]["correlation"] > 0.9


# -----------------------------------------------------------------------------
# Test: Edge cases
# -----------------------------------------------------------------------------

class TestEdgeCases:
    """Tests for edge cases and error handling."""
    
    def test_handles_zero_precipitation_months(self):
        """Should handle months with zero precipitation."""
        np.random.seed(42)
        n_months = 360
        precip = np.random.gamma(2, 50, n_months)
        # Set some months to zero (dry season)
        for i in range(30):
            precip[i * 12 + 6] = 0  # July each year
            precip[i * 12 + 7] = 0  # August each year
        
        times = pd.date_range("1981-01-01", periods=n_months, freq="MS")
        monthly_da = xr.DataArray(precip, coords={"time": times}, dims=["time"])
        
        result = compute_spi_for_unit(
            monthly_precip=monthly_da,
            calibration_monthly_precip=None,
            baseline_years=(1981, 2010),
            scale_months=3,
        )
        
        assert result is not None
        assert result.valid_months > 300  # Most months should be valid
    
    def test_handles_nan_in_data(self):
        """Should handle NaN values in precipitation data."""
        np.random.seed(42)
        n_months = 360
        precip = np.random.gamma(2, 50, n_months)
        # Add some NaN values
        precip[50:55] = np.nan
        
        times = pd.date_range("1981-01-01", periods=n_months, freq="MS")
        monthly_da = xr.DataArray(precip, coords={"time": times}, dims=["time"])
        
        result = compute_spi_for_unit(
            monthly_precip=monthly_da,
            calibration_monthly_precip=None,
            baseline_years=(1981, 2010),
            scale_months=3,
        )
        
        # Should still compute (NaN replaced with 0)
        assert result is not None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])