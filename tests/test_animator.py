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
# BufferedSlicingReader async prefetch (double-buffer) tests
# ===========================================================================


class _InstrumentedReader:
    """Wrap an InMemorySlicingReader; count and optionally delay range reads.

    Used to simulate slow HDF5 + transform chunk loads deterministically so
    the prefetch behaviour can be asserted without real data files.
    """

    def __init__(self, df: pd.DataFrame, delay: float = 0.0):
        from dvue.animator import InMemorySlicingReader

        self._inner = InMemorySlicingReader(df)
        self._delay = float(delay)
        self.range_calls = 0
        self.rows_read = 0
        # Mirror the SlicingReader surface BufferedSlicingReader relies on.
        self.time_index = self._inner.time_index

    @property
    def vmin(self):
        return self._inner.vmin

    @property
    def vmax(self):
        return self._inner.vmax

    def get_slice(self, ts):
        return self._inner.get_slice(ts)

    def get_slice_range(self, start_idx, end_idx):
        self.range_calls += 1
        self.rows_read += max(0, end_idx - start_idx)
        if self._delay:
            import time
            time.sleep(self._delay)
        return self._inner.get_slice_range(start_idx, end_idx)


class TestBufferedReaderPrefetch:
    """Functional correctness of the async double-buffer + boundary fix."""

    @pytest.fixture
    def long_df(self):
        idx = pd.date_range("2020-01-01", periods=120, freq="D")
        rng = np.random.default_rng(11)
        return pd.DataFrame(
            rng.uniform(0.0, 1.0, size=(120, 4)), index=idx, columns=[1, 2, 3, 4]
        )

    def test_prefetch_parity_with_sync(self, long_df):
        """Prefetch mode returns identical values to a plain in-memory read."""
        import time
        from dvue.animator import BufferedSlicingReader, InMemorySlicingReader

        mem = InMemorySlicingReader(long_df)
        buf = BufferedSlicingReader(
            _InstrumentedReader(long_df), chunk_size=20, prefetch=True
        )
        for ts in long_df.index:
            pd.testing.assert_series_equal(buf.get_slice(ts), mem.get_slice(ts))
            time.sleep(0.001)

    def test_boundary_no_repeated_reload_sync(self, long_df):
        """Sweeping near the start must not reload the same clamped chunk.

        Regression guard for the boundary re-refill bug: with the buffer
        clamped at index 0, frames within ``margin`` of the left edge must be
        served from the buffer, not trigger a fresh ``get_slice_range`` each
        frame.
        """
        from dvue.animator import BufferedSlicingReader

        inst = _InstrumentedReader(long_df)
        buf = BufferedSlicingReader(inst, chunk_size=20, refill_margin=0.2, adaptive=False)
        for ts in long_df.index[:8]:
            buf.get_slice(ts)
        # Only the initial load — buffer [0, 20) covers all eight frames.
        assert inst.range_calls == 1

    def test_random_seek_falls_back_to_sync_load(self, long_df):
        """A jump outside both buffers triggers a synchronous load."""
        from dvue.animator import BufferedSlicingReader

        inst = _InstrumentedReader(long_df)
        buf = BufferedSlicingReader(inst, chunk_size=20, prefetch=True)
        buf.get_slice(long_df.index[5])
        calls_after_first = inst.range_calls
        # Far jump beyond the current buffer -> must reload.
        buf.get_slice(long_df.index[100])
        assert inst.range_calls > calls_after_first
        # Value is still correct after the seek.
        from dvue.animator import InMemorySlicingReader

        mem = InMemorySlicingReader(long_df)
        pd.testing.assert_series_equal(
            buf.get_slice(long_df.index[100]), mem.get_slice(long_df.index[100])
        )

    def test_forward_sweep_uses_few_loads(self, long_df):
        """A steady forward sweep loads ~len/chunk chunks, not one per frame."""
        import time
        from dvue.animator import BufferedSlicingReader

        inst = _InstrumentedReader(long_df)
        buf = BufferedSlicingReader(
            inst, chunk_size=20, refill_margin=0.2, prefetch=True, adaptive=False
        )
        for ts in long_df.index:
            buf.get_slice(ts)
            time.sleep(0.003)  # let prefetch threads complete between frames
        # 120 frames / 20 per chunk ≈ 6 chunks; allow generous slack.
        assert inst.range_calls <= 14


@pytest.mark.performance
class TestBufferedReaderPrefetchPerformance:
    """Confirm async prefetch keeps worst-frame latency low under slow I/O.

    Run with:  pytest -m performance
    """

    def _make_df(self, n=200):
        idx = pd.date_range("2015-01-01", periods=n, freq="D")
        rng = np.random.default_rng(3)
        return pd.DataFrame(
            rng.uniform(0.0, 1.0, size=(n, 16)), index=idx, columns=list(range(16))
        )

    def test_prefetch_reduces_worst_frame_latency(self):
        import time
        from dvue.animator import BufferedSlicingReader

        df = self._make_df()
        delay = 0.03  # simulated HDF5 + transform cost per chunk load

        # --- synchronous buffer: pays the full load on every refill frame ---
        sync = BufferedSlicingReader(
            _InstrumentedReader(df, delay=delay), chunk_size=40, prefetch=False
        )
        sync_max = 0.0
        for ts in df.index:
            t0 = time.perf_counter()
            sync.get_slice(ts)
            sync_max = max(sync_max, time.perf_counter() - t0)

        # --- prefetch buffer: load happens off-thread during playback ---
        pre = BufferedSlicingReader(
            _InstrumentedReader(df, delay=delay), chunk_size=40, prefetch=True
        )
        pre.get_slice(df.index[0])  # warm the first chunk
        pre_max = 0.0
        for ts in df.index:
            t0 = time.perf_counter()
            pre.get_slice(ts)
            pre_max = max(pre_max, time.perf_counter() - t0)
            time.sleep(delay)  # simulate the player's frame interval

        # The synchronous reader stalls for ~delay on refill frames; the
        # prefetch reader should never pay that cost on the consumer thread.
        assert sync_max >= delay
        assert pre_max < sync_max * 0.6


# ===========================================================================
# RawSequentialBuffer tests
# ===========================================================================


class TestRawSequentialBuffer:
    """Functional tests for RawSequentialBuffer cache hit/miss and prefetch."""

    @pytest.fixture
    def raw_df(self):
        idx = pd.date_range("2020-01-01", periods=200, freq="h")
        rng = np.random.default_rng(42)
        return pd.DataFrame(
            rng.uniform(0.0, 1.0, size=(200, 4)), index=idx, columns=[1, 2, 3, 4]
        )

    @pytest.fixture
    def instrumented(self, raw_df):
        """InMemorySlicingReader wrapped in _InstrumentedReader for call counting."""
        return _InstrumentedReader(raw_df)

    def test_cache_miss_then_hit_sequential(self, instrumented, raw_df):
        """First request misses; subsequent requests in the same window hit."""
        import time
        from dvue.animator.reader import RawSequentialBuffer
        buf = RawSequentialBuffer(instrumented, lookahead_factor=3, min_chunk_size=20)
        # First request — must miss (empty cache) and load synchronously.
        buf.get_slice_range(0, 10)
        assert instrumented.range_calls >= 1
        # Let any background prefetch thread run to completion so it doesn't
        # race with our assertion below.
        time.sleep(0.05)
        calls_after_prefetch = instrumented.range_calls
        # Subsequent request fully within the already-loaded cache window —
        # must NOT trigger a new synchronous inner call.
        buf.get_slice_range(5, 10)
        assert instrumented.range_calls == calls_after_prefetch

    def test_correct_values_returned(self, instrumented, raw_df):
        """Values from cache match inner reader directly."""
        from dvue.animator import InMemorySlicingReader
        from dvue.animator.reader import RawSequentialBuffer
        mem = InMemorySlicingReader(raw_df)
        buf = RawSequentialBuffer(instrumented, lookahead_factor=3, min_chunk_size=20)
        result = buf.get_slice_range(10, 20)
        expected = mem.get_slice_range(10, 20)
        pd.testing.assert_frame_equal(result, expected)

    def test_cache_miss_on_seek(self, instrumented, raw_df):
        """A jump beyond the cache triggers a new synchronous load."""
        from dvue.animator.reader import RawSequentialBuffer
        buf = RawSequentialBuffer(instrumented, lookahead_factor=3, min_chunk_size=20)
        buf.get_slice_range(0, 10)
        calls_after_first = instrumented.range_calls
        # Far jump — must miss and load again.
        buf.get_slice_range(150, 160)
        assert instrumented.range_calls > calls_after_first

    def test_sequential_sweep_few_misses(self, raw_df):
        """A sequential sweep should generate far fewer inner calls than frames."""
        import time
        from dvue.animator.reader import RawSequentialBuffer
        inst = _InstrumentedReader(raw_df)
        buf = RawSequentialBuffer(inst, lookahead_factor=4, min_chunk_size=30)
        # 200 sequential 10-step requests (each 10 steps wide).
        for i in range(0, 200, 10):
            buf.get_slice_range(i, min(200, i + 10))
            time.sleep(0.002)  # let prefetch threads complete
        # With lookahead=4 and min=30, the async prefetch covers 40 steps ahead.
        # One synchronous cold-start load + several promotions from next_cache =
        # well under 15 total inner calls for 200 steps.
        assert inst.range_calls <= 15

    def test_vmin_vmax_delegated(self, instrumented):
        """vmin/vmax should reflect the inner reader."""
        from dvue.animator.reader import RawSequentialBuffer
        buf = RawSequentialBuffer(instrumented, lookahead_factor=2)
        assert buf.vmin == instrumented.vmin
        assert buf.vmax == instrumented.vmax

    def test_time_index_matches_inner(self, instrumented):
        from dvue.animator.reader import RawSequentialBuffer
        buf = RawSequentialBuffer(instrumented)
        assert len(buf.time_index) == len(instrumented.time_index)


# ===========================================================================
# BufferedSlicingReader — adaptive chunk sizing tests
# ===========================================================================


class TestBufferedSlicingReaderAdaptive:
    """Confirm that _chunk_size grows when playback fps is high."""

    @pytest.fixture
    def long_df(self):
        idx = pd.date_range("2020-01-01", periods=500, freq="h")
        rng = np.random.default_rng(99)
        return pd.DataFrame(
            rng.uniform(0.0, 1.0, size=(500, 4)), index=idx, columns=[1, 2, 3, 4]
        )

    def test_chunk_size_grows_at_high_fps(self, long_df):
        """After rapid successive frames the adaptive code grows _chunk_size."""
        import time
        from dvue.animator import BufferedSlicingReader, InMemorySlicingReader

        mem = InMemorySlicingReader(long_df)
        buf = BufferedSlicingReader(
            mem, chunk_size=50, prefetch=True,
            adaptive=True, min_chunk_size=50, max_chunk_size=2000,
            target_buffer_seconds=5.0,
        )
        initial_size = buf._chunk_size
        # Simulate rapid playback: no sleep → very high fps.
        for ts in long_df.index[:100]:
            buf.get_slice(ts)
        # After 100 frames with no sleep, fps >> 1/5 → new_size >> initial_size.
        assert buf._chunk_size >= initial_size  # must not shrink
        # At some point (after the first prefetch trigger) it should have grown.
        # Drive further to ensure the margin is crossed and a prefetch fires.
        for ts in long_df.index[100:200]:
            buf.get_slice(ts)
        assert buf._chunk_size > initial_size

    def test_adaptive_false_chunk_size_stable(self, long_df):
        """With adaptive=False the chunk_size never changes."""
        import time
        from dvue.animator import BufferedSlicingReader, InMemorySlicingReader

        mem = InMemorySlicingReader(long_df)
        buf = BufferedSlicingReader(
            mem, chunk_size=30, prefetch=True, adaptive=False,
        )
        for ts in long_df.index[:200]:
            buf.get_slice(ts)
        assert buf._chunk_size == 30

    def test_adaptive_not_active_in_sync_mode(self, long_df):
        """adaptive=True has no effect when prefetch=False."""
        from dvue.animator import BufferedSlicingReader, InMemorySlicingReader

        mem = InMemorySlicingReader(long_df)
        buf = BufferedSlicingReader(
            mem, chunk_size=30, prefetch=False, adaptive=True,
            target_buffer_seconds=0.001,
        )
        for ts in long_df.index[:50]:
            buf.get_slice(ts)
        # _maybe_prefetch is never called in sync mode → size stays at 30.
        assert buf._chunk_size == 30


# ===========================================================================
# StreamingTransformedSlicingReader — basic API tests
# ===========================================================================

class TestStreamingTransformedSlicingReaderBasic:
    """Basic API tests for StreamingTransformedSlicingReader (resample, rolling, context-manager)."""

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

    @pytest.fixture
    def resample_spec(self):
        from dvue.animator import TransformSpec
        return TransformSpec(
            lambda df: df.resample("D").mean(),
            kind="aggregate",
            get_overlap=lambda _: 0,
            output_freq="D",
        )

    @pytest.fixture
    def rolling_spec(self):
        from dvue.animator import TransformSpec
        return TransformSpec(
            lambda df: df.rolling(6, center=True, min_periods=1).mean(),
            kind="convolution",
            get_overlap=lambda _: 3,
        )

    def test_resample_daily_reduces_steps(self, hourly_reader, resample_spec):
        from dvue.animator import StreamingTransformedSlicingReader
        tr = StreamingTransformedSlicingReader(hourly_reader, resample_spec)
        # 48 hourly steps → 2 daily steps
        assert len(tr.time_index) == 2

    def test_resample_daily_freq(self, hourly_reader, resample_spec):
        from dvue.animator import StreamingTransformedSlicingReader
        tr = StreamingTransformedSlicingReader(hourly_reader, resample_spec)
        assert tr.time_index.freq == pd.tseries.frequencies.to_offset("D")

    def test_resample_get_slice_returns_series(self, hourly_reader, resample_spec):
        from dvue.animator import StreamingTransformedSlicingReader
        tr = StreamingTransformedSlicingReader(hourly_reader, resample_spec)
        s = tr.get_slice(tr.time_index[0])
        assert isinstance(s, pd.Series)
        assert len(s) == 4

    def test_rolling_keeps_steps(self, hourly_reader, rolling_spec):
        from dvue.animator import StreamingTransformedSlicingReader
        tr = StreamingTransformedSlicingReader(hourly_reader, rolling_spec)
        assert len(tr.time_index) == 48

    def test_rolling_smooths_values(self, hourly_reader, hourly_df, rolling_spec):
        from dvue.animator import StreamingTransformedSlicingReader
        tr = StreamingTransformedSlicingReader(hourly_reader, rolling_spec)
        smoothed = tr.get_slice_nearest(hourly_df.index[12])
        assert isinstance(smoothed, pd.Series)
        assert len(smoothed) == 4

    def test_vmin_vmax_inherited_from_inner(self, hourly_reader, resample_spec):
        from dvue.animator import StreamingTransformedSlicingReader
        tr = StreamingTransformedSlicingReader(hourly_reader, resample_spec)
        # vmin/vmax are inherited from the inner reader — no sampling on construction
        assert tr.vmin == hourly_reader.vmin
        assert tr.vmax == hourly_reader.vmax

    def test_get_slice_nearest_on_transformed(self, hourly_reader, resample_spec):
        from dvue.animator import StreamingTransformedSlicingReader
        tr = StreamingTransformedSlicingReader(hourly_reader, resample_spec)
        # Query midday — should snap to the nearest daily step
        midday = pd.Timestamp("2020-01-01 12:00")
        s = tr.get_slice_nearest(midday)
        assert isinstance(s, pd.Series)

    def test_invalid_kind_raises(self):
        from dvue.animator import TransformSpec
        with pytest.raises(ValueError, match="kind"):
            TransformSpec(lambda df: df, kind="unknown", get_overlap=lambda _: 0)

    def test_context_manager(self, hourly_reader, resample_spec):
        from dvue.animator import StreamingTransformedSlicingReader
        with StreamingTransformedSlicingReader(hourly_reader, resample_spec) as tr:
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


# ===========================================================================
# StreamingTransformedSlicingReader tests
# ===========================================================================

class TestStreamingTransformedSlicingReader:
    """Tests for the chunk-by-chunk streaming transform reader."""

    @pytest.fixture
    def reader_hourly(self):
        """48-step hourly reader (2 days), 4 channels."""
        idx = pd.date_range("2020-01-01", periods=48, freq="h")
        rng = np.random.default_rng(77)
        df = pd.DataFrame(rng.uniform(100, 1000, (48, 4)),
                          index=idx, columns=[1, 2, 3, 4])
        from dvue.animator import InMemorySlicingReader
        return InMemorySlicingReader(df)

    @pytest.fixture
    def reader_long(self):
        """365-step daily reader (1 year), 4 channels — simulates a large file."""
        idx = pd.date_range("2020-01-01", periods=365, freq="D")
        rng = np.random.default_rng(88)
        df = pd.DataFrame(rng.uniform(0, 100, (365, 4)),
                          index=idx, columns=[1, 2, 3, 4])
        from dvue.animator import InMemorySlicingReader
        return InMemorySlicingReader(df)

    def _resample_spec(self, freq="D"):
        from dvue.animator import TransformSpec
        def _fn(df):
            r = df.resample(freq).mean()
            if r.index.freq is None:
                import pandas as _pd
                r.index.freq = _pd.tseries.frequencies.to_offset(freq)
            return r
        return TransformSpec(_fn, "aggregate", lambda _: 0, output_freq=freq)

    def _rolling_spec(self, window="4h"):
        import math, pandas as _pd
        from dvue.animator import TransformSpec
        window_nanos = int(_pd.to_timedelta(window).total_seconds() * 1e9)
        def _fn(df):
            r = df.rolling(window, center=True, min_periods=1).mean()
            r.index.freq = df.index.freq
            return r
        return TransformSpec(
            _fn, "convolution",
            lambda freq_nanos: math.ceil(window_nanos / 2 / freq_nanos),
        )

    # ── Construction / time_index ─────────────────────────────────────

    def test_aggregate_time_index_computed_without_data(self, reader_hourly):
        """Daily resample: time_index derived from inner metadata, zero raw reads."""
        from dvue.animator import StreamingTransformedSlicingReader
        spec = self._resample_spec("D")
        # Patch get_slice_range to verify no reads occur during __init__
        _original = reader_hourly.get_slice_range
        call_counts = [0]

        def _counting(*a, **kw):
            call_counts[0] += 1
            return _original(*a, **kw)
        reader_hourly.get_slice_range = _counting

        sr = StreamingTransformedSlicingReader(reader_hourly, spec)
        # Should have produced 2 output days for 48 hourly steps
        assert len(sr.time_index) == 2
        assert sr.time_index.freq == pd.tseries.frequencies.to_offset("D")
        # Construction must not read any data
        assert call_counts[0] == 0

    def test_convolution_time_index_same_as_inner(self, reader_hourly):
        from dvue.animator import StreamingTransformedSlicingReader
        spec = self._rolling_spec("4h")
        sr = StreamingTransformedSlicingReader(reader_hourly, spec)
        assert len(sr.time_index) == len(reader_hourly.time_index)
        assert sr.time_index.freq == reader_hourly.time_index.freq

    # ── get_slice_range ───────────────────────────────────────────────

    def test_aggregate_get_slice_range_values_correct(self, reader_hourly):
        """Streaming daily mean matches full-dataset daily mean."""
        from dvue.animator import StreamingTransformedSlicingReader, InMemorySlicingReader
        spec = self._resample_spec("D")
        sr = StreamingTransformedSlicingReader(reader_hourly, spec)

        # Reference: full-dataset daily mean via InMemorySlicingReader
        full_df = reader_hourly._data
        expected = full_df.resample("D").mean()

        # Streaming: request all output rows
        result = sr.get_slice_range(0, len(sr.time_index))
        np.testing.assert_allclose(
            result.values, expected.values, rtol=1e-10,
            err_msg="Streaming daily mean differs from full-dataset daily mean"
        )

    def test_aggregate_partial_range(self, reader_long):
        """Requesting a subrange returns exactly the right rows."""
        from dvue.animator import StreamingTransformedSlicingReader
        spec = self._resample_spec("D")
        sr = StreamingTransformedSlicingReader(reader_long, spec)
        # reader_long IS daily, so daily resample of daily = same
        result = sr.get_slice_range(10, 20)
        assert len(result) == 10

    def test_convolution_get_slice_range_values_correct(self, reader_hourly):
        """Streaming rolling mean matches full-dataset rolling mean at interior steps."""
        from dvue.animator import StreamingTransformedSlicingReader
        spec = self._rolling_spec("4h")
        sr = StreamingTransformedSlicingReader(reader_hourly, spec)

        full_df = reader_hourly._data
        expected_full = full_df.rolling("4h", center=True, min_periods=1).mean()

        # Request interior chunk (steps 5..15) — away from file edges
        result = sr.get_slice_range(5, 15)
        expected = expected_full.iloc[5:15]
        np.testing.assert_allclose(result.values, expected.values, rtol=1e-8)

    def test_convolution_start_edge(self, reader_hourly):
        """Requesting from step 0 works (clamped overlap)."""
        from dvue.animator import StreamingTransformedSlicingReader
        spec = self._rolling_spec("4h")
        sr = StreamingTransformedSlicingReader(reader_hourly, spec)
        result = sr.get_slice_range(0, 5)
        assert len(result) == 5
        assert result.shape[1] == 4

    def test_convolution_end_edge(self, reader_hourly):
        """Requesting the last few steps works (clamped right overlap)."""
        from dvue.animator import StreamingTransformedSlicingReader
        spec = self._rolling_spec("4h")
        sr = StreamingTransformedSlicingReader(reader_hourly, spec)
        n = len(sr.time_index)
        result = sr.get_slice_range(n - 5, n)
        assert len(result) == 5

    # ── get_slice ─────────────────────────────────────────────────────

    def test_get_slice_returns_series(self, reader_hourly):
        from dvue.animator import StreamingTransformedSlicingReader
        spec = self._rolling_spec("4h")
        sr = StreamingTransformedSlicingReader(reader_hourly, spec)
        s = sr.get_slice(sr.time_index[10])
        assert isinstance(s, pd.Series)
        assert len(s) == 4

    # ── vmin / vmax ───────────────────────────────────────────────────

    def test_vmin_vmax_inherited_from_inner(self, reader_hourly):
        from dvue.animator import StreamingTransformedSlicingReader
        spec = self._resample_spec("D")
        sr = StreamingTransformedSlicingReader(reader_hourly, spec)
        # vmin/vmax inherited from inner reader — finite and equal to inner's
        assert np.isfinite(sr.vmin)
        assert np.isfinite(sr.vmax)
        assert sr.vmin == reader_hourly.vmin
        assert sr.vmax == reader_hourly.vmax

    # ── Overlap computation ───────────────────────────────────────────

    def test_rolling_overlap_scales_with_frequency(self):
        """Overlap for a 24h window at 1h = 12; at 15min = 48."""
        import math
        from dvue.animator import TransformSpec
        spec = self._rolling_spec("24h")
        nanos_1h = 3600 * int(1e9)
        nanos_15min = 900 * int(1e9)
        assert spec.get_overlap(nanos_1h) == 12
        assert spec.get_overlap(nanos_15min) == 48

    def test_godin_overlap_scales_with_frequency(self):
        """Godin overlap at 1h ≈ 34; at 15min ≈ 134."""
        import math
        from dvue.animator import TransformSpec
        # Use the same formula as make_godin_transform
        WARMUP_NANOS = int(33.5 * 3600 * 1e9)
        nanos_1h = 3600 * int(1e9)
        nanos_15min = 900 * int(1e9)
        overlap_1h = math.ceil(WARMUP_NANOS / nanos_1h)
        overlap_15min = math.ceil(WARMUP_NANOS / nanos_15min)
        assert overlap_1h == 34
        assert overlap_15min == 134

    # ── TransformSpec can be constructed and used directly (no dsm2ui needed) ──

    def test_resample_spec_is_TransformSpec(self):
        """A simple aggregate TransformSpec works end-to-end."""
        import pandas as _pd
        from dvue.animator import TransformSpec, StreamingTransformedSlicingReader, InMemorySlicingReader
        import numpy as _np

        def _resample_daily(df):
            r = df.resample("D").mean()
            if r.index.freq is None:
                r.index.freq = _pd.tseries.frequencies.to_offset("D")
            return r

        spec = TransformSpec(_resample_daily, "aggregate", lambda _: 0, output_freq="D")
        assert isinstance(spec, TransformSpec)
        assert spec.kind == "aggregate"
        assert spec.output_freq == "D"

        idx = _pd.date_range("2020-01-01", periods=48, freq="h")
        df = _pd.DataFrame(_np.ones((48, 2)), index=idx, columns=[1, 2])
        reader = InMemorySlicingReader(df)
        sr = StreamingTransformedSlicingReader(reader, spec)
        assert len(sr.time_index) == 2   # 48 h → 2 days

    def test_rolling_spec_is_TransformSpec(self):
        """A convolution TransformSpec (rolling mean) works end-to-end."""
        import math, pandas as _pd
        from dvue.animator import TransformSpec, StreamingTransformedSlicingReader, InMemorySlicingReader
        import numpy as _np

        window_nanos = int(_pd.to_timedelta("4h").total_seconds() * 1e9)

        def _rolling(df):
            r = df.rolling("4h", center=True, min_periods=1).mean()
            r.index.freq = df.index.freq
            return r

        spec = TransformSpec(
            _rolling, "convolution",
            lambda fn: math.ceil(window_nanos / 2 / fn),
        )
        assert isinstance(spec, TransformSpec)
        assert spec.kind == "convolution"
        assert spec.output_freq is None

        idx = _pd.date_range("2020-01-01", periods=24, freq="h")
        df = _pd.DataFrame(_np.ones((24, 2)), index=idx, columns=[1, 2])
        reader = InMemorySlicingReader(df)
        sr = StreamingTransformedSlicingReader(reader, spec)
        assert len(sr.time_index) == 24  # convolution: same length

    def test_godin_spec_overlap_formula(self):
        """The Godin overlap formula (33.5h per side) gives correct values."""
        import math
        from dvue.animator import TransformSpec

        WARMUP_NANOS = int(33.5 * 3600 * 1e9)
        spec = TransformSpec(
            lambda df: df,  # dummy fn
            "convolution",
            lambda fn: math.ceil(WARMUP_NANOS / fn),
        )
        assert isinstance(spec, TransformSpec)
        assert spec.kind == "convolution"
        nanos_1h = 3600 * int(1e9)
        nanos_15min = 900 * int(1e9)
        assert spec.get_overlap(nanos_1h) == 34
        assert spec.get_overlap(nanos_15min) == 134
