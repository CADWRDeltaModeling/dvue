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
