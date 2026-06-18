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

    Parameters
    ----------
    reader : SlicingReader
        Underlying reader (e.g., :class:`~dsm2ui.animate.HydroH5FlowReader`).
    chunk_size : int, optional
        Number of time steps to buffer at once.  Default ``200``.
    refill_margin : float, optional
        Fraction of *chunk_size* from either edge that triggers a refill.
        Default ``0.15`` — refill when within 30 steps of the edge for a
        200-step chunk.
    """

    def __init__(
        self,
        reader: SlicingReader,
        chunk_size: int = 200,
        refill_margin: float = 0.15,
    ) -> None:
        self._inner = reader
        self._chunk_size = chunk_size
        self._margin = max(1, int(chunk_size * refill_margin))
        self._buf: Optional[pd.DataFrame] = None   # shape (chunk, geo_ids)
        self._buf_start: int = 0
        self._buf_end: int = 0
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

    def _load_chunk(self, center_idx: int) -> None:
        n = len(self._time_index)
        half = self._chunk_size // 2
        start = max(0, center_idx - half)
        end = min(n, start + self._chunk_size)
        start = max(0, end - self._chunk_size)      # re-clamp if at end
        self._buf = self._inner.get_slice_range(start, end)
        self._buf_start = start
        self._buf_end = end

    def _needs_refill(self, idx: int) -> bool:
        return (
            self._buf is None
            or idx < self._buf_start
            or idx >= self._buf_end
            or idx < self._buf_start + self._margin
            or idx >= self._buf_end - self._margin
        )

    # ------------------------------------------------------------------
    # SlicingReader interface
    # ------------------------------------------------------------------

    def get_slice(self, timestamp: pd.Timestamp) -> pd.Series:
        idx = self._time_index.get_indexer([timestamp], method="nearest")[0]
        if self._needs_refill(idx):
            self._load_chunk(idx)
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


class TransformedSlicingReader(SlicingReader):
    """Applies a time-dimension transform to any :class:`SlicingReader`.

    The entire raw dataset is loaded from the inner reader on **first
    access**, the transform function is applied once, and the result is
    cached as an in-memory DataFrame.  Subsequent ``get_slice`` /
    ``get_slice_range`` calls are served from that cache.

    This eager-on-first-access strategy keeps chunk-boundary arithmetic
    simple: transforms that require context across time steps (rolling
    average, tidal filter warmup) see the full dataset, not just a window.

    Parameters
    ----------
    inner : SlicingReader
        The raw data source.
    transform_fn : callable
        ``(df: pd.DataFrame) -> pd.DataFrame``

        Receives the full raw DataFrame (rows = timestamps, columns =
        geo_ids) and must return a DataFrame with a **regular
        DatetimeIndex**.  The output may have a different frequency or
        fewer rows than the input (e.g. after resampling).
    warmup_steps : int, optional
        Number of raw time steps to prepend before the first usable output
        step.  These rows are discarded after the transform.  Only needed
        for filters with a warm-up period (e.g. Godin: ~134 steps at
        15 min).  Default ``0``.

    Notes
    -----
    For very large tidefiles (multi-year at 15-min) this class reads the
    **entire** dataset into memory.  Callers should apply a time-window
    restriction on the inner reader, or use ``InMemorySlicingReader``
    with a pre-loaded block, when memory is a concern.

    Transform functions for DSM2 data (resample, rolling average, Godin
    filter) are provided in :mod:`dsm2ui.animate` to keep vtools3 out of
    the dvue dependency tree.

    Examples
    --------
    Daily average of a 15-min reader::

        from dvue.animator import TransformedSlicingReader
        daily_reader = TransformedSlicingReader(
            raw_reader,
            transform_fn=lambda df: df.resample("D").mean(),
        )

    24-hour centred rolling mean (keeps 15-min timesteps)::

        TransformedSlicingReader(
            raw_reader,
            transform_fn=lambda df: df.rolling(96, center=True, min_periods=48).mean(),
        )
    """

    def __init__(
        self,
        inner: SlicingReader,
        transform_fn,
        warmup_steps: int = 0,
    ) -> None:
        self._inner = inner
        self._transform_fn = transform_fn
        self._warmup_steps = max(0, int(warmup_steps))
        self._cache: Optional[pd.DataFrame] = None  # populated on first access

        # Compute a provisional time index from the inner reader so the
        # SlicingReader base class is satisfied at __init__ time.
        # The real (post-transform) index is set on first access via
        # _ensure_cache(); the base __init__ call below will be re-invoked
        # via _reinit_from_cache once the actual index is known.
        # For now pass the inner index as a placeholder — it will be
        # replaced when the cache is populated.
        self._inner_time_index_placeholder = inner.time_index
        # We cannot call super().__init__ with the correct index until the
        # transform has been applied.  Instead we initialise with the inner
        # index and then override _time_index after transform.
        super().__init__(inner.time_index)

    # ------------------------------------------------------------------
    # Lazy cache population
    # ------------------------------------------------------------------

    def _ensure_cache(self) -> None:
        """Populate the in-memory transformed DataFrame (once only)."""
        if self._cache is not None:
            return

        inner = self._inner
        n_raw = len(inner.time_index)

        # Fetch more raw data if warmup padding is requested.
        # warmup_steps rows are prepended before the logical start.
        start_raw = 0   # always start from the beginning for full coverage
        raw_df = inner.get_slice_range(start_raw, n_raw)

        # Apply the user-supplied transform
        transformed: pd.DataFrame = self._transform_fn(raw_df)

        if not isinstance(transformed.index, pd.DatetimeIndex):
            raise TypeError(
                "transform_fn must return a DataFrame with a pd.DatetimeIndex."
            )

        # Enforce a regular frequency on the output index
        if transformed.index.freq is None:
            transformed.index.freq = pd.infer_freq(transformed.index)
        if transformed.index.freq is None:
            raise ValueError(
                "transform_fn returned a DataFrame with an irregular DatetimeIndex. "
                "Ensure the transform produces a regular time series "
                "(e.g. use resample().mean() or rolling().mean())."
            )

        # Discard any warmup rows from the *output* if warmup_steps > 0.
        # For most transforms (resample, rolling) the warmup is expressed as
        # NaN rows at the start; drop them here.
        if self._warmup_steps > 0:
            # Map warmup_steps (raw steps) → output steps
            # Heuristic: drop leading NaN rows up to warmup_steps worth of time
            raw_freq_ns = pd.tseries.frequencies.to_offset(inner.time_index.freq).nanos
            out_freq_ns = pd.tseries.frequencies.to_offset(transformed.index.freq).nanos
            out_warmup = max(1, int(self._warmup_steps * raw_freq_ns / out_freq_ns))
            n_drop = min(out_warmup, len(transformed))
            # Only drop if all-NaN rows exist at the start
            leading_nan = transformed.iloc[:n_drop].isna().all(axis=1)
            n_actual_drop = int(leading_nan[::-1].idxmax() + 1) if leading_nan.any() else 0
            if n_actual_drop > 0:
                transformed = transformed.iloc[n_actual_drop:]

        self._cache = transformed

        # Update the time_index to the transformed index
        self._time_index = transformed.index

        # Recompute vmin/vmax from transformed data
        vals = transformed.to_numpy(dtype=float, na_value=np.nan)
        self._vmin = float(np.nanmin(vals)) if not np.all(np.isnan(vals)) else 0.0
        self._vmax = float(np.nanmax(vals)) if not np.all(np.isnan(vals)) else 0.0

    # ------------------------------------------------------------------
    # SlicingReader interface
    # ------------------------------------------------------------------

    @property
    def time_index(self) -> pd.DatetimeIndex:
        self._ensure_cache()
        return self._time_index

    @property
    def vmin(self) -> float:
        self._ensure_cache()
        return self._vmin

    @property
    def vmax(self) -> float:
        self._ensure_cache()
        return self._vmax

    def get_slice(self, timestamp: pd.Timestamp) -> pd.Series:
        self._ensure_cache()
        i = self._time_index.get_indexer([timestamp], method="nearest")[0]
        return self._cache.iloc[i].astype(float)

    def get_slice_range(self, start_idx: int, end_idx: int) -> pd.DataFrame:
        self._ensure_cache()
        return self._cache.iloc[start_idx:end_idx].astype(float)

    def close(self) -> None:
        """Close the underlying inner reader if it supports ``close()``."""
        if hasattr(self._inner, "close"):
            self._inner.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


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

    def __init__(self, transform_fn, kind: str, get_overlap, output_freq=None):
        if kind not in ("convolution", "aggregate"):
            raise ValueError(f"kind must be 'convolution' or 'aggregate', got {kind!r}")
        self.transform_fn = transform_fn
        self.kind = kind
        self.get_overlap = get_overlap   # callable(freq_nanos: int) -> int
        self.output_freq = output_freq


class StreamingTransformedSlicingReader(SlicingReader):
    """Apply a time-domain transform lazily, one chunk at a time.

    Unlike :class:`TransformedSlicingReader`, this class **never loads the
    full dataset at startup**.  Each :meth:`get_slice_range` call fetches
    only the raw steps needed for that window (plus an overlap border to
    avoid boundary artefacts), applies the transform function, and returns
    the trimmed result.

    This makes startup near-instant for large tidefiles — ``time_index`` is
    derived from the inner reader's metadata, and ``vmin``/``vmax`` are
    estimated from a small sample in the middle of the file.

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
    sample_steps : int, optional
        Number of **output** frames to sample from the middle of the file
        for ``vmin``/``vmax`` estimation.  Default ``200``.

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
        sample_steps: int = 200,
    ) -> None:
        import math as _math

        self._inner = inner
        self._spec = spec

        raw_ti = inner.time_index
        n_raw = len(raw_ti)
        try:
            freq_nanos = int(pd.tseries.frequencies.to_offset(raw_ti.freq).nanos)
        except AttributeError:
            freq_nanos = int(pd.Timedelta(raw_ti.freq).total_seconds() * 1e9)

        self._overlap: int = spec.get_overlap(freq_nanos)

        # ── Compute output time index (no HDF5 read) ──────────────────
        if spec.output_freq is not None:
            # Aggregate: derive output dates from inner's date range.
            # Use a tiny dummy Series (2 timestamps) to let pandas resample
            # compute alignment correctly, then extrapolate to the full range.
            dummy = pd.Series(0.0, index=raw_ti[[0, -1]])
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

        # ── Estimate vmin/vmax from a central sample (no full-file load) ──
        n_out = len(out_ti)
        mid_out = n_out // 2
        s_start = max(0, mid_out - sample_steps // 2)
        s_end = min(n_out, s_start + sample_steps)

        super().__init__(out_ti)   # sets self._time_index before get_slice_range

        try:
            sample_df = self.get_slice_range(s_start, s_end)
            vals = sample_df.to_numpy(dtype=float, na_value=np.nan)
            finite = vals[np.isfinite(vals)]
            if len(finite):
                self._vmin = float(np.nanmin(finite))
                self._vmax = float(np.nanmax(finite))
            else:
                self._vmin, self._vmax = inner.vmin, inner.vmax
        except Exception:
            self._vmin, self._vmax = inner.vmin, inner.vmax

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

