"""SlicingReader — abstract base and in-memory implementation.

A SlicingReader answers the question:
    "Given a datetime, what is the value at each geo feature?"

The return type is always a ``pd.Series`` with geo_ids as the index and
float values as the data.  Aggregation over any spatial or other sub-dimensions
(e.g., upstream vs. downstream end of a DSM2 channel) is the **reader's**
responsibility — the UI layer never sees raw multi-dimensional arrays.

Subclassing Guide
-----------------
To connect a new data source (HDF5, NetCDF, Zarr, …):

1. Inherit from :class:`SlicingReader`.
2. In ``__init__``, open the file and build the full ``pd.DatetimeIndex`` with
   a regular ``freq`` (e.g., ``"15min"``, ``"1H"``, ``"1D"``).  Raise
   ``ValueError`` if the index is irregular.
3. Compute ``_vmin`` and ``_vmax`` from the dataset at init time (or lazily on
   first access) so the UI can initialise the colour scale.
4. Implement :meth:`get_slice` to load exactly one time-step from the source.

Example HDF5 skeleton::

    class HDF5SlicingReader(SlicingReader):
        def __init__(self, filepath, dataset_path, channel_numbers):
            import h5py
            self._h5 = h5py.File(filepath, "r")
            self._ds = self._h5[dataset_path]          # shape: (time, n_channels, n_locs)
            attrs = self._ds.attrs
            start = pd.Timestamp(attrs["start_time"][0].decode())
            interval = pd.to_timedelta(attrs["interval"][0].decode())
            n_time = self._ds.shape[0]
            idx = pd.date_range(start=start, periods=n_time, freq=interval)
            self._channel_numbers = channel_numbers    # array-like of int
            # vmin/vmax: sample or compute from full dataset
            sample = self._ds[:, :, :]                 # careful: loads all — consider chunking
            self._vmin = float(np.nanmin(sample))
            self._vmax = float(np.nanmax(sample))
            super().__init__(idx)

        def get_slice(self, timestamp):
            i = self._time_index.get_indexer([timestamp], method="nearest")[0]
            row = self._ds[i, :, :]                    # shape: (n_channels, n_locs)
            values = row.mean(axis=1)                  # average over locations
            return pd.Series(values, index=self._channel_numbers, dtype=float)
"""

from __future__ import annotations

import abc
import collections
import time as _time
import threading
from typing import Optional, Union

import numpy as np
import pandas as pd


class SlicingReader(abc.ABC):
    """Abstract base for time-slicing readers.

    Subclasses must supply a **regular** ``pd.DatetimeIndex`` (non-None
    ``freq``) and implement :meth:`get_slice`.

    Parameters
    ----------
    time_index : pd.DatetimeIndex
        Full temporal index of the dataset.  Must have a non-None ``freq``
        (i.e. be regular).

    Raises
    ------
    ValueError
        If *time_index* has no ``freq`` (irregular spacing).
    TypeError
        If *time_index* is not a ``pd.DatetimeIndex``.
    """

    def __init__(self, time_index: pd.DatetimeIndex) -> None:
        if not isinstance(time_index, pd.DatetimeIndex):
            raise TypeError(
                f"time_index must be a pd.DatetimeIndex, got {type(time_index)}"
            )
        if time_index.freq is None:
            raise ValueError(
                "time_index must be a regular DatetimeIndex with a non-None freq. "
                "Use pd.date_range(...) or call .asfreq() to enforce regularity."
            )
        self._time_index = time_index

    # ------------------------------------------------------------------
    # Read-only properties
    # ------------------------------------------------------------------

    @property
    def time_index(self) -> pd.DatetimeIndex:
        """Full regular DatetimeIndex of the dataset."""
        return self._time_index

    @property
    def freq(self) -> pd.DateOffset:
        """Frequency of the time index (same as ``time_index.freq``)."""
        return self._time_index.freq

    @property
    @abc.abstractmethod
    def vmin(self) -> float:
        """Global minimum value across all geo_ids and all time steps."""

    @property
    @abc.abstractmethod
    def vmax(self) -> float:
        """Global maximum value across all geo_ids and all time steps."""

    # ------------------------------------------------------------------
    # Core slice method
    # ------------------------------------------------------------------

    @abc.abstractmethod
    def get_slice(self, timestamp: pd.Timestamp) -> pd.Series:
        """Return values for all geo_ids at *timestamp*.

        Parameters
        ----------
        timestamp : pd.Timestamp
            An **exact** timestamp that exists in :attr:`time_index`.

        Returns
        -------
        pd.Series
            Index = geo_ids (same type as the column labels of the
            underlying data), values = float.
        """

    # ------------------------------------------------------------------
    # Convenience
    # ------------------------------------------------------------------

    def get_slice_nearest(self, dt: Union[pd.Timestamp, "datetime.datetime"]) -> pd.Series:
        """Return values for the time step **nearest** to *dt*.

        Safe to call with any datetime (e.g., from a Panel DateSlider).
        Uses ``DatetimeIndex.get_indexer`` with ``method='nearest'``.

        Parameters
        ----------
        dt : datetime-like
            Arbitrary datetime.  Converted to ``pd.Timestamp`` internally.

        Returns
        -------
        pd.Series
            Same contract as :meth:`get_slice`.
        """
        ts = pd.Timestamp(dt)
        i = self._time_index.get_indexer([ts], method="nearest")[0]
        return self.get_slice(self._time_index[i])

    def get_slice_range(self, start_idx: int, end_idx: int) -> pd.DataFrame:
        """Read a contiguous range of time steps in one call.

        Returns a DataFrame indexed by the selected timestamps with geo_ids
        as columns.  The default implementation calls :meth:`get_slice`
        for each step; subclasses should override this for efficient
        batch I/O (e.g., reading a contiguous HDF5 slice).

        Parameters
        ----------
        start_idx : int
            Start position in :attr:`time_index` (inclusive).
        end_idx : int
            End position in :attr:`time_index` (exclusive).

        Returns
        -------
        pd.DataFrame
            Shape ``(end_idx - start_idx, n_geo_ids)``.
        """
        timestamps = self._time_index[start_idx:end_idx]
        rows = {ts: self.get_slice(ts) for ts in timestamps}
        return pd.DataFrame(rows).T


class InMemorySlicingReader(SlicingReader):
    """SlicingReader backed by an in-memory DataFrame.

    Parameters
    ----------
    data : pd.DataFrame
        Rows indexed by a **regular** ``pd.DatetimeIndex``; columns are
        geo_ids (int, str, or any hashable).  Each cell is a numeric
        value (float).

    Raises
    ------
    ValueError
        If ``data.index`` is not a regular DatetimeIndex (``freq is None``).
    TypeError
        If ``data.index`` is not a ``pd.DatetimeIndex``.

    Notes
    -----
    ``vmin`` and ``vmax`` are computed **once** at initialisation from the
    entire DataFrame via ``nanmin`` / ``nanmax``.  If all values are NaN
    both default to 0.0.
    """

    def __init__(self, data: pd.DataFrame) -> None:
        if not isinstance(data.index, pd.DatetimeIndex):
            raise TypeError(
                f"data.index must be a pd.DatetimeIndex, got {type(data.index)}"
            )
        if data.index.freq is None:
            # Try to infer — some DataFrames built with date_range lose freq
            data = data.copy()
            data.index.freq = pd.infer_freq(data.index)
            if data.index.freq is None:
                raise ValueError(
                    "data.index has no freq (irregular spacing). "
                    "Use pd.date_range(...) to build a regular index."
                )

        super().__init__(data.index)
        self._data = data.copy()

        vals = data.to_numpy(dtype=float, na_value=np.nan)
        self._vmin = float(np.nanmin(vals)) if not np.all(np.isnan(vals)) else 0.0
        self._vmax = float(np.nanmax(vals)) if not np.all(np.isnan(vals)) else 0.0

    # ------------------------------------------------------------------
    # SlicingReader interface
    # ------------------------------------------------------------------

    @property
    def vmin(self) -> float:
        return self._vmin

    @property
    def vmax(self) -> float:
        return self._vmax

    def get_slice(self, timestamp: pd.Timestamp) -> pd.Series:
        """Exact lookup by timestamp.

        Parameters
        ----------
        timestamp : pd.Timestamp
            Must be present in the index.

        Returns
        -------
        pd.Series
            Index = column labels of the underlying DataFrame, values = float.

        Raises
        ------
        KeyError
            If *timestamp* is not in the index.
        """
        return self._data.loc[timestamp].astype(float)

    def get_slice_range(self, start_idx: int, end_idx: int) -> pd.DataFrame:
        """Efficient batch read — single DataFrame slice, no Python loop."""
        return self._data.iloc[start_idx:end_idx].astype(float)


# ---------------------------------------------------------------------------
# Buffered wrapper
# ---------------------------------------------------------------------------

class BufferedSlicingReader(SlicingReader):
    """Wraps any :class:`SlicingReader` and keeps a contiguous chunk in RAM.

    On each :meth:`get_slice` call the requested time step is served from
    the in-memory buffer when possible.  When the cursor approaches either
    edge of the buffer (within ``refill_margin`` × ``chunk_size`` steps)
    a new chunk is loaded via :meth:`~SlicingReader.get_slice_range` — a
    single I/O call that reads all steps at once (crucial for HDF5).

    Prefetching (double-buffering)
    ------------------------------
    When ``prefetch=True`` the reader keeps a **second** buffer holding the
    chunk *adjacent* to the current one in the direction of playback.  As the
    cursor nears the edge of the current chunk a daemon thread loads the next
    chunk via :meth:`~SlicingReader.get_slice_range` **off the calling
    thread**, so the (potentially slow) HDF5 read + transform never blocks the
    UI/IOLoop.  When the cursor crosses into the prefetched range the second
    buffer is promoted to current with no I/O on the calling thread.

    This is the key to smooth playback for streaming-transform stacks
    (e.g. Godin → daily): without it, every chunk boundary forces a multi-
    second synchronous filter computation on the IOLoop, freezing the browser
    "every few days" of model time.  A synchronous load is still used for the
    cold start and for random seeks (DatetimePicker jumps) that land outside
    both buffers.

    Parameters
    ----------
    reader : SlicingReader
        Underlying reader (e.g., :class:`~dsm2ui.animate.HydroH5FlowReader`).
    chunk_size : int, optional
        Number of time steps to buffer at once.  Default ``200``.
    refill_margin : float, optional
        Fraction of *chunk_size* from either edge that triggers a refill (or,
        in prefetch mode, a background prefetch).  Default ``0.15`` — act when
        within 30 steps of the edge for a 200-step chunk.
    prefetch : bool, optional
        Enable asynchronous double-buffering.  Default ``False`` (preserves the
        original synchronous behaviour for backward compatibility).  UI layers
        that drive playback should pass ``True``.
    adaptive : bool, optional
        When ``True`` (default), automatically adjusts ``chunk_size`` based on
        the observed playback frame rate so the buffer always covers
        approximately ``target_buffer_seconds`` of real playback time.  Only
        active in prefetch mode; has no effect when ``prefetch=False``.
    min_chunk_size : int, optional
        Lower bound on the adaptive chunk size.  Default ``50``.
    max_chunk_size : int, optional
        Upper bound on the adaptive chunk size.  Default ``2000``.
    target_buffer_seconds : float, optional
        Target wall-clock seconds of playback to keep buffered.  Default
        ``10.0``.  At 20 fps this yields a 200-step chunk; at 30 fps, 300
        steps; at 60 fps, 600 steps.
    """

    def __init__(
        self,
        reader: SlicingReader,
        chunk_size: int = 200,
        refill_margin: float = 0.15,
        prefetch: bool = False,
        adaptive: bool = True,
        min_chunk_size: int = 50,
        max_chunk_size: int = 2000,
        target_buffer_seconds: float = 10.0,
    ) -> None:
        self._inner = reader
        self._chunk_size = chunk_size
        self._refill_margin_fraction = refill_margin
        self._margin = max(1, int(chunk_size * refill_margin))
        self._prefetch = bool(prefetch)
        self._adaptive = bool(adaptive)
        self._min_chunk_size = max(1, int(min_chunk_size))
        self._max_chunk_size = max(self._min_chunk_size, int(max_chunk_size))
        self._target_buffer_seconds = float(target_buffer_seconds)
        self._frame_times: collections.deque = collections.deque(maxlen=20)
        self._buf: Optional[pd.DataFrame] = None   # shape (chunk, geo_ids)
        self._buf_start: int = 0
        self._buf_end: int = 0
        # Prefetch (double-buffer) state — guarded by ``_lock``.
        self._next_buf: Optional[pd.DataFrame] = None
        self._next_start: int = 0
        self._next_end: int = 0
        self._prefetching: bool = False
        self._last_idx: int = 0
        self._lock = threading.Lock()
        super().__init__(reader.time_index)

    @property
    def vmin(self) -> float:
        return self._inner.vmin

    @property
    def vmax(self) -> float:
        return self._inner.vmax

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _chunk_bounds(self, center_idx: int) -> tuple:
        """Return clamped ``(start, end)`` for a chunk centred on *center_idx*."""
        n = len(self._time_index)
        half = self._chunk_size // 2
        start = max(0, center_idx - half)
        end = min(n, start + self._chunk_size)
        start = max(0, end - self._chunk_size)      # re-clamp if at end
        return start, end

    def _load_chunk(self, center_idx: int) -> None:
        """Synchronously load the chunk centred on *center_idx* into ``_buf``."""
        start, end = self._chunk_bounds(center_idx)
        self._buf = self._inner.get_slice_range(start, end)
        self._buf_start = start
        self._buf_end = end

    def _needs_refill(self, idx: int) -> bool:
        """True when a (synchronous) reload is required for *idx*.

        A reload is needed when *idx* is outside the current buffer, or within
        ``_margin`` of an edge **that is not the file boundary** (there is more
        data to load in that direction).  Being within the margin of a clamped
        file edge does *not* trigger a reload — that previously caused the same
        boundary chunk to be re-read on every frame near the start/end of the
        dataset.
        """
        if self._buf is None or idx < self._buf_start or idx >= self._buf_end:
            return True
        n = len(self._time_index)
        if idx < self._buf_start + self._margin and self._buf_start > 0:
            return True
        if idx >= self._buf_end - self._margin and self._buf_end < n:
            return True
        return False

    # ------------------------------------------------------------------
    # Prefetch (double-buffer) helpers
    # ------------------------------------------------------------------

    def _try_promote(self, idx: int) -> bool:
        """Promote the prefetched buffer to current when it covers *idx*."""
        with self._lock:
            if (
                self._next_buf is not None
                and self._next_start <= idx < self._next_end
            ):
                self._buf = self._next_buf
                self._buf_start = self._next_start
                self._buf_end = self._next_end
                self._next_buf = None
                return True
            return False

    def _clear_next(self) -> None:
        """Discard any prefetched buffer (e.g. after a random seek)."""
        with self._lock:
            self._next_buf = None

    def _maybe_prefetch(self, idx: int, direction: int) -> None:
        """Start a background load of the adjacent chunk when near the edge.

        Also updates ``_chunk_size`` from the observed playback frame rate when
        ``adaptive=True``.  Only fires near a chunk boundary (the natural point
        to resize); the 20 % threshold avoids rapid oscillation.
        """
        n = len(self._time_index)
        if direction >= 0:
            # Travelling forward — prefetch the chunk after the current one.
            if idx < self._buf_end - self._margin or self._buf_end >= n:
                return
            target_start = self._buf_end
        else:
            # Travelling backward — prefetch the chunk before the current one.
            if idx >= self._buf_start + self._margin or self._buf_start <= 0:
                return
            target_start = max(0, self._buf_start - self._chunk_size)

        # ── Adaptive chunk sizing ─────────────────────────────────────────
        # Grow/shrink _chunk_size so the buffer covers a fixed wall-clock
        # duration ahead.  Runs on the consumer thread (no lock needed).
        if self._adaptive and len(self._frame_times) >= 5:
            elapsed = self._frame_times[-1] - self._frame_times[0]
            fps = (len(self._frame_times) - 1) / max(elapsed, 1e-3)
            new_size = int(fps * self._target_buffer_seconds)
            new_size = max(self._min_chunk_size, min(self._max_chunk_size, new_size))
            if abs(new_size - self._chunk_size) / max(self._chunk_size, 1) > 0.2:
                self._chunk_size = new_size
                self._margin = max(1, int(new_size * self._refill_margin_fraction))

        with self._lock:
            if self._prefetching:
                return
            if self._next_buf is not None and self._next_start == target_start:
                return  # already prefetched
            self._prefetching = True
        threading.Thread(
            target=self._prefetch_worker, args=(target_start,), daemon=True
        ).start()

    def _prefetch_worker(self, target_start: int) -> None:
        """Load a chunk in the background and store it as ``_next_buf``."""
        chunk = None
        start = end = 0
        try:
            n = len(self._time_index)
            start = max(0, min(target_start, n))
            end = min(n, start + self._chunk_size)
            start = max(0, end - self._chunk_size)
            chunk = self._inner.get_slice_range(start, end)
        except Exception:
            chunk = None
        with self._lock:
            if chunk is not None:
                self._next_buf = chunk
                self._next_start = start
                self._next_end = end
            self._prefetching = False

    # ------------------------------------------------------------------
    # SlicingReader interface
    # ------------------------------------------------------------------

    def get_slice(self, timestamp: pd.Timestamp) -> pd.Series:
        idx = int(self._time_index.get_indexer([timestamp], method="nearest")[0])
        if not self._prefetch:
            if self._needs_refill(idx):
                self._load_chunk(idx)
            return self._buf.iloc[idx - self._buf_start].astype(float)

        # ----- prefetch mode (called from a single consumer thread) -----
        if self._buf is None or not (self._buf_start <= idx < self._buf_end):
            # Try the prefetched buffer first; fall back to a synchronous load
            # for cold start / random seeks that land outside both buffers.
            if not self._try_promote(idx):
                self._load_chunk(idx)
                self._clear_next()

        direction = 1 if idx >= self._last_idx else -1
        self._last_idx = idx
        self._frame_times.append(_time.monotonic())
        self._maybe_prefetch(idx, direction)
        return self._buf.iloc[idx - self._buf_start].astype(float)

    def get_slice_range(self, start_idx: int, end_idx: int) -> pd.DataFrame:
        """Delegate to inner reader (bypass the buffer for bulk reads)."""
        return self._inner.get_slice_range(start_idx, end_idx)

    def close(self) -> None:
        """Close underlying reader if it supports ``close()``."""
        if hasattr(self._inner, "close"):
            self._inner.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


# ---------------------------------------------------------------------------
# Diff reader (A − B)
# ---------------------------------------------------------------------------

class DiffSlicingReader(SlicingReader):
    """Computes the element-wise difference ``reader_a - reader_b``.

    The two readers may have different time indices (different start dates,
    end dates, or time steps).  ``DiffSlicingReader`` builds a common
    ``DatetimeIndex`` that covers the **intersection** of both ranges at
    the **coarser** of the two frequencies, then serves ``A - B`` for each
    step.

    Parameters
    ----------
    reader_a : SlicingReader
        The minuend ("base" study).
    reader_b : SlicingReader
        The subtrahend ("comparison" study).

    Notes
    -----
    ``vmin`` and ``vmax`` are estimated from the first 20 diff steps.
    Because differences can be negative, the range is symmetric around
    zero by default — ``|max(abs(vmin), abs(vmax))|``.  Callers can
    override via the UI colour-range controls.

    Both readers are queried with :meth:`~SlicingReader.get_slice_nearest`
    so mismatches in the exact time index are handled gracefully.

    Examples
    --------
    >>> diff = DiffSlicingReader(study_a_reader, study_b_reader)
    >>> # wrap with Buffer for HDF5 performance:
    >>> buffered_diff = BufferedSlicingReader(diff, chunk_size=200)
    """

    def __init__(
        self,
        reader_a: SlicingReader,
        reader_b: SlicingReader,
    ) -> None:
        self._a = reader_a
        self._b = reader_b

        # Build the common time index (intersection of both ranges at
        # the coarser frequency).
        common_idx = _build_common_index(reader_a.time_index, reader_b.time_index)
        super().__init__(common_idx)

        # Estimate vmin/vmax from first 20 diff steps — symmetric around 0.
        n_sample = min(20, len(common_idx))
        sample_vals = []
        for ts in common_idx[:n_sample]:
            diff = self._a.get_slice_nearest(ts) - self._b.get_slice_nearest(ts)
            finite = diff[np.isfinite(diff)]
            if len(finite):
                sample_vals.extend(finite.tolist())
        if sample_vals:
            absmax = float(max(abs(v) for v in sample_vals))
            self._vmin = -absmax if absmax > 0 else -1.0
            self._vmax = absmax if absmax > 0 else 1.0
        else:
            self._vmin, self._vmax = -1.0, 1.0

    @property
    def vmin(self) -> float:
        return self._vmin

    @property
    def vmax(self) -> float:
        return self._vmax

    def get_slice(self, timestamp: pd.Timestamp) -> pd.Series:
        sa = self._a.get_slice_nearest(timestamp)
        sb = self._b.get_slice_nearest(timestamp)
        return (sa - sb).astype(float)

    def get_slice_range(self, start_idx: int, end_idx: int) -> pd.DataFrame:
        """Compute diff for a contiguous block."""
        timestamps = self._time_index[start_idx:end_idx]
        rows = {}
        for ts in timestamps:
            rows[ts] = self.get_slice(ts)
        return pd.DataFrame(rows).T

    def close(self) -> None:
        for r in (self._a, self._b):
            if hasattr(r, "close"):
                r.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


def _build_common_index(
    idx_a: pd.DatetimeIndex,
    idx_b: pd.DatetimeIndex,
) -> pd.DatetimeIndex:
    """Return the intersection of two DatetimeIndexes at the coarser frequency.

    Steps:
    1. Choose the coarser (larger period) frequency between the two.
    2. Restrict to the overlapping time range.
    3. Build a regular ``date_range`` with that frequency.
    4. Raise ``ValueError`` if there is no overlap.
    """
    start = max(idx_a[0], idx_b[0])
    end = min(idx_a[-1], idx_b[-1])
    if start > end:
        raise ValueError(
            "DiffSlicingReader: the two time indices have no overlap.\n"
            f"  Reader A: {idx_a[0]} … {idx_a[-1]}\n"
            f"  Reader B: {idx_b[0]} … {idx_b[-1]}"
        )
    # Choose coarser frequency
    freq_a = pd.tseries.frequencies.to_offset(idx_a.freq)
    freq_b = pd.tseries.frequencies.to_offset(idx_b.freq)
    # Compare nanos safely
    try:
        nanos_a = freq_a.nanos
        nanos_b = freq_b.nanos
        coarser_freq = freq_a if nanos_a >= nanos_b else freq_b
    except AttributeError:
        # Non-fixed frequency (e.g. MS) — fall back to freq_a
        coarser_freq = freq_a

    return pd.date_range(start=start, end=end, freq=coarser_freq)


# ---------------------------------------------------------------------------
# Streaming transform — applies transform per-chunk, no full-file load
# ---------------------------------------------------------------------------


class TransformSpec:
    """Describes a time-domain transform for :class:`StreamingTransformedSlicingReader`.

    Unlike a bare callable, a ``TransformSpec`` carries the metadata needed
    to serve single animation frames without pre-loading the entire dataset:

    - *kind* — whether the output is the same length as the input
      (``"convolution"``: rolling, tidal filter) or shorter
      (``"aggregate"``: resample).
    - *get_overlap* — how many raw steps on each side of the requested
      window are needed to produce valid edge output.
    - *output_freq* — the output pandas frequency string for aggregate
      transforms (e.g. ``"D"`` for daily resample); ``None`` for
      convolution transforms.

    Parameters
    ----------
    transform_fn : callable
        ``(df: pd.DataFrame) -> pd.DataFrame``.  Applied to a raw chunk
        (with overlap padding on each side) to produce the transformed
        output.  Must return a DataFrame with a regular DatetimeIndex.
    kind : {"convolution", "aggregate"}
        ``"convolution"`` — output has the same temporal length as the
        input (rolling mean, Godin filter).  The output index matches the
        input index step-for-step.
        ``"aggregate"`` — output is shorter (e.g. resample to daily).
    get_overlap : callable
        ``(freq_nanos: int) -> int`` — given the **input** time-step in
        nanoseconds, return the number of raw steps to add on each side of
        any requested window to avoid boundary artefacts (NaN edges from
        filter warmup, or missing context for centred rolling).
        Return ``0`` for aggregate transforms that have no warmup.
    output_freq : str or None, optional
        Pandas offset string for aggregate transforms (e.g. ``"D"``).
        Must be ``None`` for convolution transforms.

    Examples
    --------
    See :func:`~dsm2ui.animate.make_resample_transform`,
    :func:`~dsm2ui.animate.make_moving_average_transform`, and
    :func:`~dsm2ui.animate.make_godin_transform` for production examples.
    """

    def __init__(self, transform_fn, kind: str, get_overlap, output_freq=None,
                 filter_spec=None, resample_agg="mean"):
        if kind not in ("convolution", "aggregate"):
            raise ValueError(f"kind must be 'convolution' or 'aggregate', got {kind!r}")
        self.transform_fn = transform_fn
        self.kind = kind
        self.get_overlap = get_overlap   # callable(freq_nanos: int) -> int
        self.output_freq = output_freq
        # Optional: for kind="aggregate", carry the convolution pre-filter spec
        # so _setup_reader can stack STSR(filter_spec) → ResamplingSlicingReader
        # instead of using STSR's slow aggregate code path.
        self.filter_spec = filter_spec   # TransformSpec or None
        self.resample_agg = resample_agg # aggregation for ResamplingSlicingReader


class StreamingTransformedSlicingReader(SlicingReader):
    """Apply a time-domain transform lazily, one chunk at a time.

    Each :meth:`get_slice_range` call fetches only the raw steps needed for
    that window (plus an overlap border to avoid boundary artefacts), applies
    the transform function, and returns the trimmed result.  Each :meth:`get_slice_range` call fetches
    only the raw steps needed for that window (plus an overlap border to
    avoid boundary artefacts), applies the transform function, and returns
    the trimmed result.

    This makes startup near-instant for large tidefiles — ``time_index`` is
    derived from the inner reader's metadata and no data is read at all;
    ``vmin``/``vmax`` are inherited from the inner reader.

    Architecture
    ------------
    The recommended stack is::

        BufferedSlicingReader(chunk=200)   ← buffers output frames (after transform)
          └── StreamingTransformedSlicingReader
              └── HydroH5FlowReader       ← single HDF5 read per chunk

    Each 200-frame buffer refill causes one HDF5 read of
    ``200 + 2 × overlap`` raw steps and one transform call on that chunk.
    For a 100-year hourly file this reads < 0.03 % of the data per chunk.

    Parameters
    ----------
    inner : SlicingReader
        The raw data source (e.g. :class:`~dsm2ui.animate.HydroH5FlowReader`).
    spec : TransformSpec
        Describes the transform and its overlap requirements.
    Notes
    -----
    For the Godin filter the overlap is ~33.5 h worth of raw steps on each
    side.  At 1-h data that is ~34 steps; at 15-min it is ~134 steps.
    The boundary output rows (within ``overlap`` steps of the start/end of
    the whole file) will contain NaN — this is correct behaviour, not a bug.
    """

    def __init__(
        self,
        inner: SlicingReader,
        spec: "TransformSpec",
    ) -> None:
        import math as _math

        self._inner = inner
        self._spec = spec

        raw_ti = inner.time_index
        try:
            freq_nanos = int(pd.tseries.frequencies.to_offset(raw_ti.freq).nanos)
        except AttributeError:
            freq_nanos = int(pd.Timedelta(raw_ti.freq).total_seconds() * 1e9)

        self._overlap: int = spec.get_overlap(freq_nanos)

        # ── Compute output time index (no HDF5 read) ──────────────────
        if spec.output_freq is not None:
            out_freq_offset = pd.tseries.frequencies.to_offset(spec.output_freq)
            # Build the complete output DatetimeIndex via date_range arithmetic
            # (pandas date_range is pure datetime math, no data I/O).
            out_start = raw_ti[0].floor(spec.output_freq)
            out_end = raw_ti[-1].floor(spec.output_freq)
            out_ti = pd.date_range(out_start, out_end, freq=spec.output_freq)
            if out_ti.freq is None:
                out_ti = out_ti.copy()
                out_ti.freq = out_freq_offset
        else:
            # Convolution: output has the same time index as input.
            out_ti = raw_ti

        # vmin/vmax inherited from the inner reader — the user controls the
        # colour scale via the UI; automatic sampling would add unnecessary
        # HDF5 I/O on every transform switch.
        self._vmin = inner.vmin
        self._vmax = inner.vmax

        super().__init__(out_ti)

    # ------------------------------------------------------------------
    # SlicingReader interface
    # ------------------------------------------------------------------

    @property
    def vmin(self) -> float:
        return self._vmin

    @property
    def vmax(self) -> float:
        return self._vmax

    def get_slice_range(self, start_out: int, end_out: int) -> pd.DataFrame:
        n_raw = len(self._inner.time_index)
        n_out = len(self._time_index)
        start_out = max(0, min(start_out, n_out))
        end_out = max(start_out, min(end_out, n_out))
        if start_out >= end_out:
            return pd.DataFrame(index=self._time_index[start_out:end_out])

        if self._spec.kind == "aggregate":
            # Map output index range → raw timestamp range via searchsorted.
            # This handles any alignment correctly regardless of start time.
            out_start_ts = self._time_index[start_out]
            out_freq = pd.tseries.frequencies.to_offset(self._spec.output_freq)
            # Upper bound: first raw step AFTER the last requested output bin.
            last_out_ts = self._time_index[end_out - 1]
            raw_upper_ts = last_out_ts + out_freq

            raw_ti = self._inner.time_index
            raw_start = int(raw_ti.searchsorted(out_start_ts, side="left"))
            raw_end = int(raw_ti.searchsorted(raw_upper_ts, side="left"))
            raw_end = min(n_raw, raw_end)

            # Extend the raw fetch by *_overlap* steps on each side.
            # For simple resamples _overlap == 0 so there is no change.
            # For composed transforms such as "Rolling 14 D → Daily mean" the
            # rolling window needs raw context beyond the output window before
            # the aggregate step is applied; without it the boundary output
            # days are computed from fewer than 14 days of raw data.
            if self._overlap > 0:
                raw_start = max(0, raw_start - self._overlap)
                raw_end = min(n_raw, raw_end + self._overlap)

            if raw_start >= raw_end:
                return pd.DataFrame(
                    np.nan,
                    index=self._time_index[start_out:end_out],
                    columns=[],
                )

            raw_df = self._inner.get_slice_range(raw_start, raw_end)
            transformed = self._spec.transform_fn(raw_df)
            # Trim to exactly the requested output rows (reindex by timestamp).
            want = self._time_index[start_out:end_out]
            result = transformed.reindex(want)
            if result.index.freq is None:
                result.index.freq = self._time_index.freq
            return result

        else:  # convolution — output same length as input
            raw_start = max(0, start_out - self._overlap)
            raw_end = min(n_raw, end_out + self._overlap)
            left_pad = start_out - raw_start   # actual overlap added on left

            raw_df = self._inner.get_slice_range(raw_start, raw_end)
            transformed = self._spec.transform_fn(raw_df)

            n_needed = end_out - start_out
            result = transformed.iloc[left_pad: left_pad + n_needed]
            # Restore the canonical output timestamps (transform may not
            # have them if the raw index had a different epoch).
            result = result.copy()
            result.index = self._time_index[start_out:end_out]
            return result

    def get_slice(self, timestamp: pd.Timestamp) -> pd.Series:
        i = int(self._time_index.get_indexer([timestamp], method="nearest")[0])
        chunk = self.get_slice_range(i, i + 1)
        if len(chunk) == 0:
            return pd.Series(dtype=float)
        return chunk.iloc[0].astype(float)

    def close(self) -> None:
        if hasattr(self._inner, "close"):
            self._inner.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


# ---------------------------------------------------------------------------
# Resampling reader
# ---------------------------------------------------------------------------

class ResamplingSlicingReader(SlicingReader):
    """Wraps any :class:`SlicingReader` and resamples its output to a coarser frequency.

    Unlike :class:`StreamingTransformedSlicingReader` with ``kind="aggregate"``,
    this reader requests only the inner steps needed for each output window —
    never a large overlap-padded chunk.  It is the efficient outer layer for
    composed transforms::

        BufferedSlicingReader(chunk_size=20)
          └── ResamplingSlicingReader(output_freq="D")
              └── StreamingTransformedSlicingReader(godin, kind="convolution")
                  └── RawSequentialBuffer
                      └── BaseReader

    Parameters
    ----------
    inner : SlicingReader
        The pre-filtered source (Godin STSR, rolling STSR, or a raw reader).
    output_freq : str
        Pandas offset string for the desired output step (e.g. ``"D"``, ``"h"``).
    agg : {"mean", "max", "min", "sum"}, optional
        Aggregation applied when resampling.  Default ``"mean"``.
    """

    def __init__(self, inner: SlicingReader, output_freq: str, agg: str = "mean") -> None:
        self._inner = inner
        self._output_freq = output_freq
        self._agg = agg

        raw_ti = inner.time_index
        out_freq = pd.tseries.frequencies.to_offset(output_freq)
        out_start = raw_ti[0].floor(output_freq)
        out_end = raw_ti[-1].floor(output_freq)
        out_ti = pd.date_range(out_start, out_end, freq=output_freq)
        if out_ti.freq is None:
            out_ti = out_ti.copy()
            out_ti.freq = out_freq

        self._vmin = inner.vmin
        self._vmax = inner.vmax

        super().__init__(out_ti)

    @property
    def vmin(self) -> float:
        return self._vmin

    @property
    def vmax(self) -> float:
        return self._vmax

    def get_slice_range(self, start_out: int, end_out: int) -> pd.DataFrame:
        n_out = len(self._time_index)
        start_out = max(0, min(start_out, n_out))
        end_out = max(start_out, min(end_out, n_out))
        if start_out >= end_out:
            return pd.DataFrame(index=self._time_index[start_out:end_out])

        out_freq = pd.tseries.frequencies.to_offset(self._output_freq)
        out_start_ts = self._time_index[start_out]
        last_out_ts = self._time_index[end_out - 1]
        raw_upper_ts = last_out_ts + out_freq

        raw_ti = self._inner.time_index
        raw_start = int(raw_ti.searchsorted(out_start_ts, side="left"))
        raw_end = int(raw_ti.searchsorted(raw_upper_ts, side="left"))
        raw_end = min(len(raw_ti), raw_end)

        if raw_start >= raw_end:
            return pd.DataFrame(index=self._time_index[start_out:end_out])

        raw_df = self._inner.get_slice_range(raw_start, raw_end)
        if raw_df.empty:
            return pd.DataFrame(index=self._time_index[start_out:end_out])

        resampled = getattr(raw_df.resample(self._output_freq), self._agg)()
        if resampled.index.freq is None:
            resampled.index.freq = out_freq

        want = self._time_index[start_out:end_out]
        result = resampled.reindex(want)
        if result.index.freq is None:
            result.index.freq = self._time_index.freq
        return result

    def get_slice(self, timestamp: pd.Timestamp) -> pd.Series:
        i = int(self._time_index.get_indexer([timestamp], method="nearest")[0])
        chunk = self.get_slice_range(i, i + 1)
        if len(chunk) == 0:
            return pd.Series(dtype=float)
        return chunk.iloc[0].astype(float)

    def close(self) -> None:
        if hasattr(self._inner, "close"):
            self._inner.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


# ---------------------------------------------------------------------------
# Raw sequential look-ahead buffer
# ---------------------------------------------------------------------------

class RawSequentialBuffer(SlicingReader):
    """Intercepts ``get_slice_range`` calls and serves them from an in-memory cache.

    Unlike :class:`BufferedSlicingReader` (which targets per-frame
    ``get_slice`` calls), ``RawSequentialBuffer`` caches bulk
    ``get_slice_range`` reads.  It is designed to sit between a raw HDF5
    reader and :class:`StreamingTransformedSlicingReader`::

        BufferedSlicingReader(prefetch=True)    ← per-frame serving
          └── StreamingTransformedSlicingReader  ← transform per chunk
              └── RawSequentialBuffer            ← this class
                  └── HydroH5FlowReader          ← h5py reads

    When :class:`StreamingTransformedSlicingReader` requests an
    overlap-padded raw window (e.g., 2,228 steps for Godin+Daily at 1 h),
    the request is served from RAM if it falls within the current cache.  A
    background thread simultaneously pre-reads the **next** window so HDF5
    I/O and transform computation **overlap in time** (pipelined) rather than
    running sequentially on the same thread.

    **Synchronous path**: on a cache miss only the exact requested range is
    loaded from the inner reader — no synchronous lookahead.  This ensures
    RSB never adds overhead in non-prefetch mode.

    **Async prefetch**: after every cache miss or hit-near-end, a background
    thread loads a window of ``lookahead_factor × request_size`` starting
    slightly *before* the end of the current cache.  The ``back_safety``
    overlap ensures that the next sequential request (whose start may be
    ``2 × filter_overlap`` steps before the end of the current cache, e.g.
    68 steps for a 34-step bilateral Godin filter) lands in the prefetch
    window.

    Parameters
    ----------
    inner : SlicingReader
        The raw data source (e.g., :class:`~dsm2ui.animate.HydroH5FlowReader`).
    lookahead_factor : int, optional
        The async prefetch covers this many times the most-recent requested
        size.  Default ``4``.
    min_chunk_size : int, optional
        Lower bound on the auto-computed prefetch size.  Default ``200``.
    back_safety : int, optional
        The async prefetch starts this many steps *before* the end of the
        current cache so that the next sequential request (which overlaps by
        up to ``2 × filter_overlap`` steps) is covered.  Default ``300``
        (covers Godin at both 1-hour and 15-minute resolution).
    prefetch_enabled : bool, optional
        When ``False`` the async background prefetch thread is never launched.
        Set to ``False`` when the outer :class:`BufferedSlicingReader` is in
        synchronous mode (``prefetch=False``) to avoid background-thread I/O
        contention with the consumer's synchronous chunk loads.  Default
        ``True``.
    """

    def __init__(
        self,
        inner: SlicingReader,
        lookahead_factor: int = 4,
        min_chunk_size: int = 200,
        back_safety: int = 300,
        prefetch_enabled: bool = True,
    ) -> None:
        self._inner = inner
        self._lookahead_factor = max(1, int(lookahead_factor))
        self._min_chunk_size = max(1, int(min_chunk_size))
        self._back_safety = max(0, int(back_safety))
        self._prefetch_enabled = bool(prefetch_enabled)

        self._cache: Optional[pd.DataFrame] = None
        self._cache_start: int = 0
        self._cache_end: int = 0

        # Prefetched (next) cache — written by daemon thread, read under lock.
        self._next_cache: Optional[pd.DataFrame] = None
        self._next_start: int = 0
        self._next_end: int = 0
        self._prefetching: bool = False
        self._lock = threading.Lock()

        super().__init__(inner.time_index)

    # ------------------------------------------------------------------
    # SlicingReader interface
    # ------------------------------------------------------------------

    @property
    def vmin(self) -> float:
        return self._inner.vmin

    @property
    def vmax(self) -> float:
        return self._inner.vmax

    def get_slice(self, timestamp: pd.Timestamp) -> pd.Series:
        """Exact lookup — delegates to :meth:`get_slice_range`."""
        i = int(self._time_index.get_indexer([timestamp], method="nearest")[0])
        chunk = self.get_slice_range(i, i + 1)
        if len(chunk) == 0:
            return pd.Series(dtype=float)
        return chunk.iloc[0].astype(float)

    def get_slice_range(self, start_idx: int, end_idx: int) -> pd.DataFrame:
        """Serve a contiguous range from the in-memory cache.

        **Cache hit**: pure DataFrame slice — zero I/O.
        **Next-cache promotion**: if the prefetch thread has completed and
        its window covers the request, swap it to main cache — zero I/O.
        **Cache miss**: load **exactly** ``[start_idx, end_idx]`` from the
        inner reader (no synchronous lookahead), then launch an async prefetch
        of the following window.
        """
        request_size = end_idx - start_idx

        # Fast path: full range is within the current cache.
        if (
            self._cache is not None
            and self._cache_start <= start_idx
            and end_idx <= self._cache_end
        ):
            self._maybe_prefetch(self._cache_end, request_size)
            s = start_idx - self._cache_start
            e = end_idx - self._cache_start
            return self._cache.iloc[s:e]

        # Try to promote the prefetched (next) cache.
        with self._lock:
            if (
                self._next_cache is not None
                and self._next_start <= start_idx
                and end_idx <= self._next_end
            ):
                self._cache = self._next_cache
                self._cache_start = self._next_start
                self._cache_end = self._next_end
                self._next_cache = None
                self._prefetching = False

        # Re-check after possible promotion.
        if (
            self._cache is not None
            and self._cache_start <= start_idx
            and end_idx <= self._cache_end
        ):
            self._maybe_prefetch(self._cache_end, request_size)
            s = start_idx - self._cache_start
            e = end_idx - self._cache_start
            return self._cache.iloc[s:e]

        # Cache miss: load EXACTLY the requested range — no synchronous lookahead.
        n = len(self._time_index)
        load_end = min(n, end_idx)
        self._cache = self._inner.get_slice_range(start_idx, load_end)
        self._cache_start = start_idx
        self._cache_end = load_end
        with self._lock:
            self._next_cache = None
            self._prefetching = False
        self._maybe_prefetch(load_end, request_size)
        return self._cache.iloc[: end_idx - start_idx]

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _maybe_prefetch(self, after_pos: int, request_size: int) -> None:
        """Spawn a background load of the next window when not already running.

        The prefetch window starts up to ``back_safety`` steps *before*
        ``after_pos`` so that the next sequential request (whose start overlaps
        by up to ``2 × filter_overlap`` steps into the current cache) is
        guaranteed to fall within the prefetched window.  ``back_safety`` is
        capped at ``request_size - 1`` so that for small requests the prefetch
        still advances forward rather than always targeting the same window.

        Does nothing when :attr:`_prefetch_enabled` is ``False``.
        """
        if not self._prefetch_enabled:
            return
        n = len(self._time_index)
        if after_pos >= n:
            return
        # Cap back_safety so it never exceeds the request width minus one step.
        effective_back = min(self._back_safety, max(0, request_size - 1))
        target_start = max(0, after_pos - effective_back)
        next_size = max(self._min_chunk_size, request_size * self._lookahead_factor)
        target_end = min(n, target_start + next_size)
        if target_end <= target_start:
            return

        with self._lock:
            if self._prefetching:
                return
            if (
                self._next_cache is not None
                and self._next_start <= target_start
                and target_end <= self._next_end
            ):
                return  # already have this window
            self._prefetching = True

        threading.Thread(
            target=self._prefetch_worker,
            args=(target_start, target_end),
            daemon=True,
        ).start()

    def _prefetch_worker(self, start: int, end: int) -> None:
        """Load a raw chunk off the consumer thread; store as ``_next_cache``."""
        chunk = None
        try:
            chunk = self._inner.get_slice_range(start, end)
        except Exception:
            chunk = None
        with self._lock:
            if chunk is not None:
                self._next_cache = chunk
                self._next_start = start
                self._next_end = end
            self._prefetching = False

    def close(self) -> None:
        """Close the underlying inner reader if it supports ``close()``."""
        if hasattr(self._inner, "close"):
            self._inner.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
