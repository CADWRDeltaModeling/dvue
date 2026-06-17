"""Tests for dvue.animator — reader and GeoAnimatorManager."""

from __future__ import annotations

import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

# ---------------------------------------------------------------------------
# Optional geo deps — skip geometry tests if unavailable
# ---------------------------------------------------------------------------
try:
    import geopandas as gpd
    from shapely.geometry import Point, Polygon, LineString

    HAS_GEO = True
except ImportError:
    HAS_GEO = False

pytestmark_geo = pytest.mark.skipif(not HAS_GEO, reason="geopandas not installed")

# Integration test data (pydsm test suite)
_HDF5_FILE = Path("d:/dev/pydsm/tests/data/gtm_sample_output/historical_gtm.h5")

# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

N_IDS = 8
N_TIMES = 20
GEO_IDS = list(range(N_IDS))


@pytest.fixture
def regular_df() -> pd.DataFrame:
    """Regular daily DataFrame: (N_TIMES × N_IDS)."""
    idx = pd.date_range("2020-01-01", periods=N_TIMES, freq="D")
    rng = np.random.default_rng(42)
    data = rng.uniform(100, 1000, size=(N_TIMES, N_IDS))
    return pd.DataFrame(data, index=idx, columns=GEO_IDS)


@pytest.fixture
def reader(regular_df):
    from dvue.animator import InMemorySlicingReader
    return InMemorySlicingReader(regular_df)


@pytest.fixture
def point_gdf():
    """GeoDataFrame with Point geometry, geo_id = 0..N_IDS-1."""
    if not HAS_GEO:
        pytest.skip("geopandas not available")
    return gpd.GeoDataFrame(
        {"geo_id": GEO_IDS, "geometry": [Point(float(i), float(i)) for i in GEO_IDS]},
        crs="EPSG:4326",
    )


@pytest.fixture
def polygon_gdf():
    """GeoDataFrame with Polygon geometry, geo_id = 0..N_IDS-1."""
    if not HAS_GEO:
        pytest.skip("geopandas not available")

    def _box(i):
        x, y = float(i), float(i)
        return Polygon([(x, y), (x + 0.1, y), (x + 0.1, y + 0.1), (x, y + 0.1)])

    return gpd.GeoDataFrame(
        {"geo_id": GEO_IDS, "geometry": [_box(i) for i in GEO_IDS]},
        crs="EPSG:4326",
    )


@pytest.fixture
def line_gdf():
    """GeoDataFrame with LineString geometry, geo_id = 0..N_IDS-1."""
    if not HAS_GEO:
        pytest.skip("geopandas not available")
    return gpd.GeoDataFrame(
        {
            "geo_id": GEO_IDS,
            "geometry": [
                LineString([(float(i), float(i)), (float(i) + 1, float(i) + 1)])
                for i in GEO_IDS
            ],
        },
        crs="EPSG:4326",
    )


# ===========================================================================
# Reader unit tests
# ===========================================================================


class TestInMemorySlicingReader:

    def test_regular_index_accepted(self, regular_df):
        from dvue.animator import InMemorySlicingReader
        r = InMemorySlicingReader(regular_df)
        assert r.time_index.freq is not None

    def test_irregular_index_raises(self):
        from dvue.animator import InMemorySlicingReader
        # Build an irregular index (missing one day)
        dates = pd.to_datetime(["2020-01-01", "2020-01-02", "2020-01-04"])
        df = pd.DataFrame({"a": [1, 2, 3], "b": [4, 5, 6]}, index=dates)
        with pytest.raises(ValueError, match="no freq"):
            InMemorySlicingReader(df)

    def test_non_datetime_index_raises(self):
        from dvue.animator import InMemorySlicingReader
        df = pd.DataFrame({"a": [1, 2, 3]}, index=[0, 1, 2])
        with pytest.raises(TypeError):
            InMemorySlicingReader(df)

    def test_time_index_returned_correctly(self, reader, regular_df):
        assert len(reader.time_index) == N_TIMES
        assert reader.time_index[0] == regular_df.index[0]
        assert reader.time_index[-1] == regular_df.index[-1]

    def test_freq_property(self, reader):
        assert reader.freq is not None
        # Should be daily
        assert reader.freq == pd.tseries.frequencies.to_offset("D")

    def test_vmin_vmax_global_range(self, reader, regular_df):
        expected_min = float(regular_df.to_numpy().min())
        expected_max = float(regular_df.to_numpy().max())
        assert abs(reader.vmin - expected_min) < 1e-9
        assert abs(reader.vmax - expected_max) < 1e-9

    def test_get_slice_exact_timestamp(self, reader, regular_df):
        ts = regular_df.index[5]
        result = reader.get_slice(ts)
        expected = regular_df.iloc[5].astype(float)
        pd.testing.assert_series_equal(result, expected)

    def test_get_slice_returns_series_with_geo_ids(self, reader):
        ts = reader.time_index[0]
        s = reader.get_slice(ts)
        assert isinstance(s, pd.Series)
        assert list(s.index) == GEO_IDS

    def test_get_slice_nearest_exact_hit(self, reader, regular_df):
        ts = regular_df.index[3]
        result = reader.get_slice_nearest(ts)
        expected = reader.get_slice(ts)
        pd.testing.assert_series_equal(result, expected)

    def test_get_slice_nearest_snaps_correctly(self, reader, regular_df):
        # Offset by 2 hours — nearest should be the same day (not day+1)
        ts = regular_df.index[7] + pd.Timedelta("2h")
        result = reader.get_slice_nearest(ts)
        expected = reader.get_slice(regular_df.index[7])
        pd.testing.assert_series_equal(result, expected)

    def test_get_slice_nearest_snaps_forward(self, reader, regular_df):
        # Offset by 14 hours on a daily series — nearest = day+1
        ts = regular_df.index[7] + pd.Timedelta("14h")
        result = reader.get_slice_nearest(ts)
        expected = reader.get_slice(regular_df.index[8])
        pd.testing.assert_series_equal(result, expected)

    def test_all_nan_vmin_vmax_defaults(self):
        from dvue.animator import InMemorySlicingReader
        idx = pd.date_range("2020-01-01", periods=3, freq="D")
        df = pd.DataFrame({"a": [np.nan, np.nan, np.nan]}, index=idx)
        r = InMemorySlicingReader(df)
        assert r.vmin == 0.0
        assert r.vmax == 0.0


# ===========================================================================
# Geometry detection tests (no Panel/rendering — import ui submodule only)
# ===========================================================================


@pytestmark_geo
class TestDetectGeomType:

    def test_detects_point(self, point_gdf):
        from dvue.animator.ui import _detect_geom_type
        assert _detect_geom_type(point_gdf) == "point"

    def test_detects_polygon(self, polygon_gdf):
        from dvue.animator.ui import _detect_geom_type
        assert _detect_geom_type(polygon_gdf) == "polygon"

    def test_detects_line(self, line_gdf):
        from dvue.animator.ui import _detect_geom_type
        assert _detect_geom_type(line_gdf) == "line"

    def test_unknown_type_raises(self):
        from dvue.animator.ui import _detect_geom_type
        # GeometryCollection is not one of the three families
        from shapely.geometry import GeometryCollection
        gdf = gpd.GeoDataFrame(
            {"geometry": [GeometryCollection()]},
            crs="EPSG:4326",
        )
        with pytest.raises(ValueError, match="Unsupported"):
            _detect_geom_type(gdf)


# ===========================================================================
# GeoAnimatorManager unit tests
# ===========================================================================


@pytestmark_geo
class TestGeoAnimatorManager:
    """Unit tests that instantiate GeoAnimatorManager with synthetic data.

    These tests do NOT open a browser; they only verify that the Panel
    component is correctly built and that data-layer logic is correct.
    """

    @pytest.fixture
    def manager_points(self, reader, point_gdf):
        import panel as pn
        pn.extension()
        from dvue.animator import GeoAnimatorManager
        return GeoAnimatorManager(reader, point_gdf, title="Test Points")

    @pytest.fixture
    def manager_polygons(self, reader, polygon_gdf):
        import panel as pn
        pn.extension()
        from dvue.animator import GeoAnimatorManager
        return GeoAnimatorManager(reader, polygon_gdf, title="Test Polygons")

    @pytest.fixture
    def manager_lines(self, reader, line_gdf):
        import panel as pn
        pn.extension()
        from dvue.animator import GeoAnimatorManager
        return GeoAnimatorManager(reader, line_gdf, title="Test Lines")

    def test_panel_returns_viewable_points(self, manager_points):
        import panel as pn
        result = pn.panel(manager_points)
        assert result is not None

    def test_panel_returns_viewable_polygons(self, manager_polygons):
        import panel as pn
        result = pn.panel(manager_polygons)
        assert result is not None

    def test_panel_returns_viewable_lines(self, manager_lines):
        import panel as pn
        result = pn.panel(manager_lines)
        assert result is not None

    def test_init_geom_type_point(self, manager_points):
        assert manager_points._geom_type == "point"

    def test_init_geom_type_polygon(self, manager_polygons):
        assert manager_polygons._geom_type == "polygon"

    def test_init_geom_type_line(self, manager_lines):
        assert manager_lines._geom_type == "line"

    def test_effective_vmin_from_reader_when_none(self, manager_points, reader):
        assert manager_points.vmin is None
        # When vmin=None, the Bokeh color mapper should use reader.vmin
        assert manager_points._bk_mapper.low == pytest.approx(reader.vmin, rel=1e-6)

    def test_effective_vmax_from_reader_when_none(self, manager_points, reader):
        assert manager_points.vmax is None
        assert manager_points._bk_mapper.high == pytest.approx(reader.vmax, rel=1e-6)

    def test_user_vmin_overrides_reader(self, reader, point_gdf):
        import panel as pn
        pn.extension()
        from dvue.animator import GeoAnimatorManager
        mgr = GeoAnimatorManager(reader, point_gdf, vmin=42.0, vmax=999.0)
        assert mgr.vmin == 42.0
        assert mgr.vmax == 999.0

    def test_merge_values_nan_for_missing_ids(self, reader, regular_df):
        """Geo ids not present in the slice should get NaN in the data source."""
        import panel as pn
        pn.extension()
        from dvue.animator import GeoAnimatorManager

        if not HAS_GEO:
            pytest.skip("geopandas not available")

        extra_ids = GEO_IDS + [999]
        extra_gdf = gpd.GeoDataFrame(
            {
                "geo_id": extra_ids,
                "geometry": [Point(float(i), float(i)) for i in extra_ids],
            },
            crs="EPSG:4326",
        )
        mgr = GeoAnimatorManager(reader, extra_gdf, title="")
        values = mgr._bk_source.data["_value"]
        geo_ids = mgr._bk_source.data["geo_id"]
        idx_999 = list(geo_ids).index(999)
        assert np.isnan(values[idx_999])
        valid = [v for i, v in zip(geo_ids, values) if i != 999]
        assert all(np.isfinite(v) for v in valid)

    def test_current_dt_set_to_first_timestep_on_init(self, manager_points, reader):
        expected = reader.time_index[0].to_pydatetime()
        assert manager_points.current_dt == expected

    def test_time_slider_start_end_match_reader(self, manager_points, reader):
        slider = manager_points._time_slider
        # DiscretePlayer: options is a list of integer indices 0..N-1
        assert slider.options[0] == 0
        assert slider.options[-1] == len(reader.time_index) - 1
        assert slider.value == 0

    def test_gdf_no_crs_raises(self, reader):
        if not HAS_GEO:
            pytest.skip("geopandas not available")
        from dvue.animator import GeoAnimatorManager
        gdf_no_crs = gpd.GeoDataFrame(
            {"geo_id": [0], "geometry": [Point(0, 0)]}
            # deliberately no crs=
        )
        with pytest.raises(ValueError, match="CRS"):
            GeoAnimatorManager(reader, gdf_no_crs)

    def test_compute_levels_custom_overrides_auto(self, manager_points):
        """Explicit comma-separated levels are returned without modification."""
        if not HAS_GEO:
            pytest.skip("geopandas not available")
        manager_points.contour_custom_levels = "100, 500, 1000, 2000"
        vals = np.array([0.0, 50.0, 300.0, 800.0, 1500.0, 2500.0])
        levels = manager_points._compute_levels(vals, 0.0, 2500.0)
        np.testing.assert_array_equal(levels, [100.0, 500.0, 1000.0, 2000.0])

    def test_compute_levels_custom_sorted(self, manager_points):
        """Custom levels are sorted ascending regardless of input order."""
        if not HAS_GEO:
            pytest.skip("geopandas not available")
        manager_points.contour_custom_levels = "2000, 100, 500"
        vals = np.linspace(0, 3000, 50)
        levels = manager_points._compute_levels(vals, 0.0, 3000.0)
        np.testing.assert_array_equal(levels, [100.0, 500.0, 2000.0])

    def test_compute_levels_empty_custom_falls_back_to_auto(self, manager_points):
        """Empty custom levels string falls back to the automatic algorithm."""
        if not HAS_GEO:
            pytest.skip("geopandas not available")
        manager_points.contour_custom_levels = ""
        vals = np.linspace(0, 1000, 50)
        levels = manager_points._compute_levels(vals, 0.0, 1000.0)
        assert len(levels) >= 1

    def test_compute_levels_invalid_custom_falls_back_to_auto(self, manager_points):
        """Non-numeric custom levels string falls back to the automatic algorithm."""
        if not HAS_GEO:
            pytest.skip("geopandas not available")
        manager_points.contour_custom_levels = "abc, def"
        vals = np.linspace(0, 1000, 50)
        levels = manager_points._compute_levels(vals, 0.0, 1000.0)
        assert len(levels) >= 1


# ===========================================================================
# Integration test — requires pydsm test data
# ===========================================================================


@pytest.mark.skipif(
    not _HDF5_FILE.exists(),
    reason=f"pydsm test data not found at {_HDF5_FILE}",
)
def test_with_dsm2_hdf5():
    """Integration test: load GTM HDF5, wrap in InMemorySlicingReader,
    build GeoAnimatorManager with buffered DSM2 channel linestrings.

    Requires:
        - pydsm installed and importable
        - d:/dev/pydsm/tests/data/gtm_sample_output/historical_gtm.h5 present
        - geopandas + shapely installed
    """
    import h5py
    import panel as pn
    pn.extension()
    from dvue.animator import GeoAnimatorManager, InMemorySlicingReader

    with h5py.File(_HDF5_FILE, "r") as f:
        ds = f["output/channel concentration"]
        attrs = dict(ds.attrs)
        start_time = pd.Timestamp(attrs["start_time"][0].decode())
        interval = pd.to_timedelta(attrs["interval"][0].decode())
        chan_numbers = f["output/channel_number"][:]
        # Load first constituent (index 0), mean over location dim
        # Shape: (time, n_chan, n_loc)
        arr = ds[:, 0, :, :]          # (time, n_chan, n_loc)
        values = arr.mean(axis=2)     # (time, n_chan) — average u/d

    n_times = values.shape[0]
    idx = pd.date_range(start=start_time, periods=n_times, freq=interval)
    df = pd.DataFrame(values, index=idx, columns=chan_numbers.tolist())

    reader = InMemorySlicingReader(df)
    assert reader.vmin < reader.vmax

    # Build a synthetic GeoDataFrame (DSM2 channel linestrings as 1-deg segments)
    gdf = gpd.GeoDataFrame(
        {
            "geo_id": chan_numbers.tolist(),
            "geometry": [
                LineString([(-122.0 + i * 0.01, 38.0), (-122.0 + i * 0.01 + 0.01, 38.01)])
                for i in range(len(chan_numbers))
            ],
        },
        crs="EPSG:4326",
    )

    mgr = GeoAnimatorManager(reader, gdf, title="GTM EC Integration")
    panel_obj = pn.panel(mgr)
    assert panel_obj is not None
    assert mgr._geom_type == "line"


# ===========================================================================
# BufferedSlicingReader tests
# ===========================================================================


class TestBufferedSlicingReader:

    @pytest.fixture
    def inner(self, regular_df):
        from dvue.animator import InMemorySlicingReader
        return InMemorySlicingReader(regular_df)

    @pytest.fixture
    def buffered(self, inner):
        from dvue.animator import BufferedSlicingReader
        return BufferedSlicingReader(inner, chunk_size=10, refill_margin=0.2)

    def test_vmin_vmax_delegate_to_inner(self, inner, buffered):
        assert buffered.vmin == inner.vmin
        assert buffered.vmax == inner.vmax

    def test_time_index_matches_inner(self, inner, buffered):
        assert len(buffered.time_index) == len(inner.time_index)

    def test_get_slice_returns_correct_values(self, inner, buffered, regular_df):
        ts = regular_df.index[5]
        expected = inner.get_slice(ts)
        result = buffered.get_slice(ts)
        pd.testing.assert_series_equal(result, expected)

    def test_buffer_loaded_on_first_access(self, buffered, regular_df):
        assert buffered._buf is None
        buffered.get_slice(regular_df.index[0])
        assert buffered._buf is not None

    def test_refill_triggered_near_edge(self, buffered, regular_df):
        # Access step 5 — buffer loaded around 5
        buffered.get_slice(regular_df.index[5])
        start_after_first = buffered._buf_start
        # Access step 18 — near the end; should trigger a refill
        buffered.get_slice(regular_df.index[18])
        assert buffered._buf_start != start_after_first

    def test_get_slice_range_delegates_to_inner(self, inner, buffered, regular_df):
        result = buffered.get_slice_range(0, 5)
        expected = inner.get_slice_range(0, 5)
        pd.testing.assert_frame_equal(result, expected)

    def test_close_propagates(self, inner):
        from dvue.animator import BufferedSlicingReader
        # InMemorySlicingReader has no close(), but BufferedSlicingReader should
        # not raise if inner has no close method
        buf = BufferedSlicingReader(inner)
        buf.close()  # must not raise


# ===========================================================================
# TransformedSlicingReader tests
# ===========================================================================

class TestTransformedSlicingReader:
    """Tests for TransformedSlicingReader with synthetic daily data."""

    @pytest.fixture
    def hourly_df(self):
        """48 hourly steps × 4 channels — suitable for resampling and rolling."""
        idx = pd.date_range("2020-01-01", periods=48, freq="h")
        rng = np.random.default_rng(7)
        data = rng.uniform(100.0, 1000.0, size=(48, 4))
        return pd.DataFrame(data, index=idx, columns=[1, 2, 3, 4])

    @pytest.fixture
    def hourly_reader(self, hourly_df):
        from dvue.animator import InMemorySlicingReader
        return InMemorySlicingReader(hourly_df)

    def test_resample_daily_reduces_steps(self, hourly_reader):
        from dvue.animator import TransformedSlicingReader
        tr = TransformedSlicingReader(
            hourly_reader,
            transform_fn=lambda df: df.resample("D").mean(),
        )
        # 48 hourly steps → 2 daily steps
        assert len(tr.time_index) == 2

    def test_resample_daily_freq(self, hourly_reader):
        from dvue.animator import TransformedSlicingReader
        tr = TransformedSlicingReader(
            hourly_reader,
            transform_fn=lambda df: df.resample("D").mean(),
        )
        assert tr.time_index.freq == pd.tseries.frequencies.to_offset("D")

    def test_resample_get_slice_returns_series(self, hourly_reader):
        from dvue.animator import TransformedSlicingReader
        tr = TransformedSlicingReader(
            hourly_reader,
            transform_fn=lambda df: df.resample("D").mean(),
        )
        s = tr.get_slice(tr.time_index[0])
        assert isinstance(s, pd.Series)
        assert len(s) == 4

    def test_rolling_keeps_steps(self, hourly_reader, hourly_df):
        from dvue.animator import TransformedSlicingReader
        tr = TransformedSlicingReader(
            hourly_reader,
            transform_fn=lambda df: df.rolling("6h", center=True, min_periods=1).mean(),
        )
        assert len(tr.time_index) == 48

    def test_rolling_smooths_values(self, hourly_reader, hourly_df):
        from dvue.animator import TransformedSlicingReader
        tr = TransformedSlicingReader(
            hourly_reader,
            transform_fn=lambda df: df.rolling(6, center=True, min_periods=1).mean(),
        )
        raw = hourly_reader.get_slice(hourly_df.index[12])
        smoothed = tr.get_slice_nearest(hourly_df.index[12])
        # Smoothed should differ from raw (unless all same value)
        assert isinstance(smoothed, pd.Series)
        assert len(smoothed) == 4

    def test_vmin_vmax_from_transformed_data(self, hourly_reader, hourly_df):
        from dvue.animator import TransformedSlicingReader
        # Daily mean should have vmin/vmax within the raw range
        tr = TransformedSlicingReader(
            hourly_reader,
            transform_fn=lambda df: df.resample("D").mean(),
        )
        assert tr.vmin >= hourly_reader.vmin - 1e-6
        assert tr.vmax <= hourly_reader.vmax + 1e-6

    def test_get_slice_nearest_on_transformed(self, hourly_reader):
        from dvue.animator import TransformedSlicingReader
        tr = TransformedSlicingReader(
            hourly_reader,
            transform_fn=lambda df: df.resample("D").mean(),
        )
        # Query midday — should snap to the daily step
        midday = pd.Timestamp("2020-01-01 12:00")
        s = tr.get_slice_nearest(midday)
        assert isinstance(s, pd.Series)

    def test_non_datetime_output_raises(self, hourly_reader):
        from dvue.animator import TransformedSlicingReader
        import pytest
        # transform_fn that returns a non-DatetimeIndex should raise TypeError
        def bad_transform(df):
            result = df.reset_index(drop=True)
            return result

        tr = TransformedSlicingReader(hourly_reader, transform_fn=bad_transform)
        with pytest.raises(TypeError, match="DatetimeIndex"):
            tr.get_slice(hourly_reader.time_index[0])

    def test_context_manager(self, hourly_reader):
        from dvue.animator import TransformedSlicingReader
        with TransformedSlicingReader(
            hourly_reader,
            transform_fn=lambda df: df.resample("D").mean(),
        ) as tr:
            assert len(tr.time_index) == 2


# ===========================================================================
# DiffSlicingReader tests
# ===========================================================================


class TestDiffSlicingReader:

    @pytest.fixture
    def reader_a(self):
        """Daily reader, values in [100, 1000]."""
        from dvue.animator import InMemorySlicingReader
        idx = pd.date_range("2020-01-01", periods=20, freq="D")
        rng = np.random.default_rng(1)
        df = pd.DataFrame(rng.uniform(100, 1000, (20, 4)), index=idx, columns=[1, 2, 3, 4])
        return InMemorySlicingReader(df)

    @pytest.fixture
    def reader_b(self):
        """Daily reader (same dates), values slightly lower."""
        from dvue.animator import InMemorySlicingReader
        idx = pd.date_range("2020-01-01", periods=20, freq="D")
        rng = np.random.default_rng(2)
        df = pd.DataFrame(rng.uniform(50, 900, (20, 4)), index=idx, columns=[1, 2, 3, 4])
        return InMemorySlicingReader(df)

    @pytest.fixture
    def diff_reader(self, reader_a, reader_b):
        from dvue.animator import DiffSlicingReader
        return DiffSlicingReader(reader_a, reader_b)

    def test_time_index_is_intersection(self, reader_a, reader_b, diff_reader):
        # Same dates → same length
        assert len(diff_reader.time_index) == len(reader_a.time_index)

    def test_time_index_is_regular(self, diff_reader):
        assert diff_reader.time_index.freq is not None

    def test_get_slice_is_difference(self, diff_reader, reader_a, reader_b):
        ts = diff_reader.time_index[5]
        result = diff_reader.get_slice(ts)
        expected = reader_a.get_slice_nearest(ts) - reader_b.get_slice_nearest(ts)
        pd.testing.assert_series_equal(result, expected.astype(float))

    def test_vmin_vmax_symmetric(self, diff_reader):
        # vmin and vmax should be symmetric around 0
        assert abs(diff_reader.vmin + diff_reader.vmax) < 1e-9

    def test_get_slice_range_shape(self, diff_reader):
        df = diff_reader.get_slice_range(0, 5)
        assert df.shape == (5, 4)

    def test_no_overlap_raises(self):
        from dvue.animator import InMemorySlicingReader, DiffSlicingReader
        idx_a = pd.date_range("2020-01-01", periods=5, freq="D")
        idx_b = pd.date_range("2021-01-01", periods=5, freq="D")
        ra = InMemorySlicingReader(pd.DataFrame(np.ones((5, 2)), index=idx_a, columns=[1, 2]))
        rb = InMemorySlicingReader(pd.DataFrame(np.ones((5, 2)), index=idx_b, columns=[1, 2]))
        with pytest.raises(ValueError, match="no overlap"):
            DiffSlicingReader(ra, rb)

    def test_coarser_freq_used_when_different(self):
        """When reader_b has daily freq and reader_a has hourly, result uses daily."""
        from dvue.animator import InMemorySlicingReader, DiffSlicingReader
        idx_hourly = pd.date_range("2020-01-01", periods=48, freq="h")
        idx_daily = pd.date_range("2020-01-01", periods=2, freq="D")
        ra = InMemorySlicingReader(
            pd.DataFrame(np.ones((48, 2)), index=idx_hourly, columns=[1, 2]))
        rb = InMemorySlicingReader(
            pd.DataFrame(np.ones((2, 2)), index=idx_daily, columns=[1, 2]))
        dr = DiffSlicingReader(ra, rb)
        # Should use daily (coarser) freq
        assert dr.time_index.freq == pd.tseries.frequencies.to_offset("D")
