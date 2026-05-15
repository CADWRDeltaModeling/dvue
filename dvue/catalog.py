"""Data catalog module for dvue.

Provides classes for managing references to data sources, organising them into
searchable catalogs, and composing derived datasets via mathematical expressions.

Classes
-------
DataReferenceReader
    Abstract base class for objects that load data given a set of metadata
    attributes.  Subclass this to implement custom data sources.  Built-in
    subclasses: InMemoryDataReferenceReader, CallableDataReferenceReader,
    FileDataReferenceReader.
DataReference
    A reference to a data source identified by metadata attributes.  Delegates
    actual loading to an attached DataReferenceReader on the first getData()
    call; caches the result thereafter.
CatalogView
    A live, read-only filtered view of a DataCatalog, selecting a subset of
    its DataReferences based on metadata criteria.
MathDataReference
    A DataReference that computes data by evaluating a mathematical expression
    over other DataReferences resolved from a variable map or catalog.
CatalogBuilder
    Abstract base class for objects that scan a source and construct
    DataReferences wired to the right DataReferenceReader.  Subclass to
    support new source types (directories, databases, REST APIs, …).
DataCatalog
    A searchable container of DataReferences with schema mapping and dynamic
    CatalogBuilder registration.

Sample builder implementations (CSVDirectoryBuilder, PatternCSVDirectoryBuilder)
and the filename-pattern helper (_pattern_to_regex) live in :mod:`dvue.readers`
and are re-exported here for backward compatibility.
"""

from __future__ import annotations

import abc
import importlib
import logging
import math
import re
from pathlib import Path
from typing import Any, Callable, ClassVar, Dict, List, Optional, Union

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# Sentinel for "argument not supplied" (distinct from None).
_UNSET = object()


# ---------------------------------------------------------------------------
# DataReferenceReader (ABC)
# ---------------------------------------------------------------------------


class DataReferenceReader(abc.ABC):
    """Abstract base class for loading data given a set of metadata attributes.

    A ``DataReferenceReader`` knows *how* to load data for a particular kind of
    source (a file, a database table, a REST endpoint, …).  It is wired into a
    :class:`DataReference` at construction time and called lazily by
    :meth:`~DataReference.getData`.

    The reader only receives the attribute dict; it has no dependency on
    :class:`DataReference` itself, which allows the same reader instance to be
    shared by many references pointing to different locations within the same
    source system (flyweight pattern).

    Subclasses **must** implement :meth:`load`.

    Examples
    --------
    >>> class MyDBReader(DataReferenceReader):
    ...     def __init__(self, connection):
    ...         self._conn = connection
    ...     def load(self, **attributes):
    ...         return self._conn.query_table(attributes["table"])
    ...
    >>> reader = MyDBReader(conn)
    >>> ref = DataReference(source="", reader=reader, name="users", table="users")
    >>> ref.getData()   # calls reader.load(table="users")
    """

    @abc.abstractmethod
    def load(self, **attributes: Any) -> pd.DataFrame:
        """Return a :class:`~pandas.DataFrame` for the given attributes.

        Parameters
        ----------
        **attributes
            The full ``_attributes`` dict of the calling
            :class:`DataReference`, unpacked as keyword arguments.

        Returns
        -------
        pd.DataFrame
        """
        ...

    @classmethod
    def fqcn(cls) -> str:
        """Return the fully qualified class name (``\"module.ClassName\"")."""
        return f"{cls.__module__}.{cls.__qualname__}"

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}()"


# ---------------------------------------------------------------------------
# Built-in DataReferenceReader subclasses
# ---------------------------------------------------------------------------


class InMemoryDataReferenceReader(DataReferenceReader):
    """Hold a fixed :class:`~pandas.DataFrame` (or :class:`~pandas.Series`) in memory.

    The stored data is returned as a defensive copy on every :meth:`load` call
    so that callers cannot mutate the stored frame.

    Parameters
    ----------
    data : pd.DataFrame or pd.Series
        The data to return.  A :class:`~pandas.Series` is automatically
        promoted to a single-column :class:`~pandas.DataFrame`.

    Examples
    --------
    >>> reader = InMemoryDataReferenceReader(df)
    >>> ref = DataReference(source="", reader=reader, name="stations", variable="temperature")
    >>> ref.getData()
    """

    def __init__(self, data: Union[pd.DataFrame, pd.Series]) -> None:
        if isinstance(data, pd.Series):
            data = data.to_frame()
        self._data = data

    def load(self, **attributes: Any) -> pd.DataFrame:
        return self._data.copy()

    def __repr__(self) -> str:
        return f"InMemoryDataReferenceReader(shape={self._data.shape})"


class CallableDataReferenceReader(DataReferenceReader):
    """Call a no-argument callable to produce a :class:`~pandas.DataFrame`.

    Useful when the data must be computed or fetched fresh each time but the
    logic is encapsulated in an existing function or lambda.

    Parameters
    ----------
    fn : callable
        A zero-argument callable that returns a
        :class:`~pandas.DataFrame`, :class:`~pandas.Series`, or any value
        that :class:`~pandas.DataFrame` can be constructed from.

    Examples
    --------
    >>> reader = CallableDataReferenceReader(lambda: fetch_latest())
    >>> ref = DataReference(source="", reader=reader, name="live", variable="temperature")
    >>> ref.getData()   # calls fn() each time (unless caching is on)
    """

    def __init__(self, fn: Callable[[], Any]) -> None:
        self._fn = fn

    def load(self, **attributes: Any) -> pd.DataFrame:
        result = self._fn()
        if isinstance(result, pd.DataFrame):
            return result
        if isinstance(result, pd.Series):
            return result.to_frame()
        return pd.DataFrame(result)

    def __repr__(self) -> str:
        return f"CallableDataReferenceReader(fn={self._fn!r})"


class FileDataReferenceReader(DataReferenceReader):
    """Load a file (or URL) whose path is given by the ``file_path`` attribute.

    The file format is inferred from the extension of ``file_path``.
    Supported extensions: ``.csv``, ``.tsv``, ``.parquet``, ``.feather``,
    ``.json``, ``.xlsx``, ``.xls``, ``.hdf``, ``.h5``.
    HTTP/HTTPS URLs are also supported and dispatch on the URL's extension.

    Because all path information lives in :attr:`source` (set at construction
    time), a single ``FileDataReferenceReader`` instance is tightly bound to
    one file.  For backward compatibility, if ``source`` is empty the reader
    falls back to a ``file_path`` attribute on the :class:`DataReference`.

    Parameters
    ----------
    source : str, optional
        File path or URL.  Pass ``""`` only when using the legacy
        ``file_path`` :class:`DataReference` attribute.
    read_kwargs : dict, optional
        Extra keyword arguments forwarded to the underlying pandas reader
        (e.g. ``{"parse_dates": ["timestamp"]}`` for CSV files).

    Examples
    --------
    >>> ref = DataReference(source="/data/flow.csv",
    ...                      reader="dvue.catalog.FileDataReferenceReader",
    ...                      name="flow")
    >>> ref.getData()   # calls pd.read_csv("/data/flow.csv")
    """

    _FILE_LOADERS: ClassVar[Dict[str, Callable]] = {
        ".csv": pd.read_csv,
        ".tsv": lambda p, **kw: pd.read_csv(p, sep="\t", **kw),
        ".parquet": pd.read_parquet,
        ".feather": pd.read_feather,
        ".json": pd.read_json,
        ".xlsx": pd.read_excel,
        ".xls": pd.read_excel,
        ".hdf": pd.read_hdf,
        ".h5": pd.read_hdf,
    }

    def __init__(self, source: str = "", read_kwargs: Optional[Dict[str, Any]] = None) -> None:
        self.source: str = source
        self._read_kwargs: Dict[str, Any] = read_kwargs or {}

    def load(self, **attributes: Any) -> pd.DataFrame:
        path = self.source or str(attributes.get("file_path", ""))
        if not path:
            raise ValueError(
                "FileDataReferenceReader requires either a source path (via the "
                "DataReference source= argument) or a 'file_path' attribute."
            )
        if path.startswith(("http://", "https://")):
            return self._load_url(path)
        return self._load_file(path)

    def _load_file(self, path: str) -> pd.DataFrame:
        suffix = Path(path).suffix.lower()
        loader = self._FILE_LOADERS.get(suffix)
        if loader is None:
            raise ValueError(
                f"Unsupported file extension {suffix!r}. "
                f"Supported: {list(self._FILE_LOADERS)}"
            )
        return loader(path, **self._read_kwargs)

    def _load_url(self, url: str) -> pd.DataFrame:
        lower = url.lower()
        if lower.endswith(".parquet"):
            return pd.read_parquet(url)
        if lower.endswith(".json"):
            return pd.read_json(url)
        return pd.read_csv(url)

    def __repr__(self) -> str:
        parts = [f"source={self.source!r}"] if self.source else []
        if self._read_kwargs:
            parts.append(f"read_kwargs={self._read_kwargs!r}")
        return f"FileDataReferenceReader({', '.join(parts)})"


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------


def _resolve_class(fqcn: str) -> type:
    """Import and return the class identified by its fully qualified name.

    Parameters
    ----------
    fqcn : str
        Fully qualified class name as produced by
        :meth:`DataReferenceReader.fqcn`, e.g.
        ``"dvue.catalog.FileDataReferenceReader"``.

    Returns
    -------
    type

    Raises
    ------
    ImportError
        If the module cannot be imported.
    AttributeError
        If the class name does not exist in the module.
    """
    module_name, _, class_name = fqcn.rpartition(".")
    if not module_name:
        raise ImportError(f"Cannot resolve {fqcn!r}: no module component in name.")
    module = importlib.import_module(module_name)
    return getattr(module, class_name)


def _criterion_matches(actual: Any, expected: Any) -> bool:
    """Return ``True`` if *actual* satisfies the *expected* criterion.

    * ``expected`` is a **string starting with "~"** — treated as a
      case-insensitive regular expression; ``re.fullmatch`` is used so the
      pattern must cover the entire attribute value.  Use ``~EC.*`` (not
      ``~EC``) to match values that *start with* ``EC``.
    * ``expected`` is a **callable** — delegated to the caller (not handled
      here; callers should check ``callable(expected)`` first).
    * Otherwise — exact equality with transparent type coercion when scalar
      types differ (e.g. string ``'0'`` vs integer ``0``).
    """
    if isinstance(expected, str) and expected.startswith("~"):
        pattern = expected[1:]
        return bool(re.fullmatch(pattern, str(actual) if actual is not None else "", re.IGNORECASE))
    if actual != expected:
        # Try type coercion when scalar types differ.
        try:
            if type(actual) is not type(expected):
                if actual == type(actual)(expected):
                    return True
        except (ValueError, TypeError):
            pass
        return False
    return True


# ---------------------------------------------------------------------------
# DataReference
# ---------------------------------------------------------------------------


class DataReference:
    """A reference to a data source identified by metadata attributes.

    ``DataReference`` pairs a data location (:attr:`source`) with a
    :class:`DataReferenceReader` identified by its fully qualified class name
    (:attr:`reader`).  The reader is instantiated lazily on the first
    :meth:`getData` call, making every reference serialisable to CSV via
    :meth:`to_dict` / :meth:`DataCatalog.to_csv`.

    Parameters
    ----------
    source : str, optional
        URL or file path identifying where the data lives.  Stored as
        ``self.source`` and passed as the sole positional argument to
        ``reader_class(source)`` during lazy reader instantiation.  Pass
        ``""`` when supplying a pre-built reader instance (e.g.
        :class:`InMemoryDataReferenceReader`).
    reader : str or DataReferenceReader, optional
        Either:

        * A **fully qualified class name** string (``"module.ClassName"``)
          identifying a :class:`DataReferenceReader` subclass.  The class is
          imported and instantiated lazily as ``reader_class(source)`` on the
          first :meth:`getData` call.  This form is serialisable to CSV via
          :meth:`to_dict` / :meth:`DataCatalog.to_csv`.
        * A pre-built :class:`DataReferenceReader` **instance** for
          programmatic use (e.g. :class:`InMemoryDataReferenceReader`,
          :class:`CallableDataReferenceReader`).  The instance is used
          directly; its FQCN is stored in :attr:`reader` for reference.
        * ``None`` — only for :class:`~dvue.math_reference.MathDataReference`
          subclasses that override :meth:`_load_data` without a reader.
    name : str, optional
        Identifier.  Required when adding to a :class:`DataCatalog`.
    cache : bool, optional
        Cache the result of the first :meth:`getData` call.  Default ``True``.
    **attributes
        Arbitrary (name, value) metadata pairs accessible via
        :meth:`get_attribute` and searchable via :meth:`DataCatalog.search`.

    Examples
    --------
    >>> reader = InMemoryDataReferenceReader(df)
    >>> ref = DataReference(source="", reader=reader, name="stations",
    ...                      variable="temperature", unit="degC")
    >>> ref.getData()                       # returns the DataFrame
    >>> ref.get_attribute("variable")
    'temperature'
    >>> # FQCN-based lazy loading from a CSV file:
    >>> ref = DataReference(source="/data/flow.csv",
    ...                      reader="dvue.catalog.FileDataReferenceReader",
    ...                      name="flow", variable="discharge")
    >>> ref.getData()   # instantiates FileDataReferenceReader("/data/flow.csv")
    """

    #: Identifies the kind of reference.  Subclasses override this at class
    #: level to advertise a different type string without needing a property::
    #:
    #:     class MyDataReference(DataReference):
    #:         ref_type = "my_type"
    ref_type: str = "raw"

    def __init__(
        self,
        source: str = "",
        reader: Union[str, "DataReferenceReader", None] = None,
        name: str = "",
        cache: bool = True,
        **attributes: Any,
    ) -> None:
        self.source: str = source
        self._reader_fqcn: str = ""
        self._reader_instance: Optional[DataReferenceReader] = None
        if isinstance(reader, str):
            self._reader_fqcn = reader
        elif isinstance(reader, DataReferenceReader):
            self._reader_fqcn = reader.fqcn()
            self._reader_instance = reader
        # else: None → MathDataReference subclass or uninitialised
        self.name = name
        self._cache_enabled: bool = cache
        self._cached_data: Dict[Any, pd.DataFrame] = {}
        # Metadata discovered at load time (e.g. unit from DSS reader).
        # Separate from _attributes so static YAML values can always override.
        self._dynamic_metadata: Dict[str, Any] = {}
        # source is always the first attribute so it appears first in to_dataframe()
        self._attributes: Dict[str, Any] = {"source": source, **attributes}

    # ------------------------------------------------------------------
    # Reader access
    # ------------------------------------------------------------------

    @property
    def reader(self) -> str:
        """Fully qualified class name of the reader (``"module.ClassName"``)."""
        return self._reader_fqcn

    def _get_reader(self) -> "DataReferenceReader":
        """Return the reader instance, instantiating lazily from FQCN+source if needed."""
        if self._reader_instance is not None:
            return self._reader_instance
        if not self._reader_fqcn:
            raise ValueError(
                f"{self.__class__.__name__}(name={self.name!r}) has no reader. "
                "Supply a reader FQCN string or a DataReferenceReader instance, "
                "or override _load_data() in a subclass."
            )
        cls = _resolve_class(self._reader_fqcn)
        self._reader_instance = cls(self.source)
        return self._reader_instance

    def to_dict(self) -> Dict[str, Any]:
        """Serialise this reference to a plain dictionary suitable for CSV output.

        Returns a dict with keys ``name``, ``source``, ``reader`` (FQCN), and
        all metadata attributes.  Use :meth:`DataCatalog.to_csv` to write an
        entire catalog at once.

        The ``ref_type`` key is included only when it differs from the default
        ``"raw"`` so that existing plain CSV files remain unchanged.
        """
        d = {
            "name": self.name,
            "reader": self._reader_fqcn,
            **self._attributes,   # "source" is already in here
        }
        if self.ref_type != "raw":
            d["ref_type"] = self.ref_type
        return d

    # ------------------------------------------------------------------
    # Metadata helpers
    # ------------------------------------------------------------------

    @property
    def attributes(self) -> Dict[str, Any]:
        """Return a shallow copy of the metadata attribute dictionary."""
        return dict(self._attributes)

    def set_attribute(self, name: str, value: Any) -> "DataReference":
        """Set a metadata attribute and return *self* (chainable)."""
        self._attributes[name] = value
        return self

    def get_attribute(self, name: str, default: Any = None) -> Any:
        """Return the value of metadata attribute *name*, or *default*."""
        return self._attributes.get(name, default)

    def has_attribute(self, name: str) -> bool:
        """Return ``True`` if metadata attribute *name* exists."""
        return name in self._attributes

    def _make_cache_key(self, time_range: Any) -> Any:
        """Return a hashable cache key for *time_range*.

        ``None`` means "no time constraint" (full series).  A
        ``(start, end)`` pair is normalised to ``pd.Timestamp`` so that
        equivalent datetime-like values hash identically.
        """
        if time_range is None:
            return None
        return (pd.Timestamp(time_range[0]), pd.Timestamp(time_range[1]))

    def matches(self, **criteria: Any) -> bool:
        """Return ``True`` if *all* criteria match this reference's metadata.

        Each criterion value can be:

        * a **scalar** – exact equality check.
        * a **string starting with "~"** – case-insensitive regex fullmatch
          (e.g. ``variable="~EC.*"`` matches ``"EC_daily"`` or ``"ec_hourly"``).
        * a **callable** ``f(value) -> bool`` – custom predicate.
        """
        for key, expected in criteria.items():
            actual = self._attributes.get(key)
            # Fall back to dynamic metadata when not found in static attributes.
            # _attributes always takes priority: dynamic metadata is lower priority.
            if actual is None and key not in self._attributes:
                actual = self._dynamic_metadata.get(key)
            if callable(expected):
                if not expected(actual):
                    return False
            elif not _criterion_matches(actual, expected):
                return False
        return True

    # ------------------------------------------------------------------
    # Data loading
    # ------------------------------------------------------------------

    def getData(self, time_range: Any = None) -> pd.DataFrame:
        """Load and return the data from the source.

        Parameters
        ----------
        time_range : tuple of (start, end), optional
            When supplied, the reader receives it as a ``time_range``
            keyword argument so it can load only the requested window
            efficiently (e.g. DSS files).  The result is cached per
            unique ``(start, end)`` pair, so different ranges are stored
            independently.  Pass ``None`` (the default) to request the
            full series.

        Returns
        -------
        pd.DataFrame
        """
        cache_key = self._make_cache_key(time_range)
        if self._cache_enabled and cache_key in self._cached_data:
            return self._cached_data[cache_key].copy()

        data = self._load_data(time_range=time_range)

        # Fallback slice: if the reader did not honour time_range natively
        # (e.g. InMemoryDataReferenceReader), trim here before caching.
        if time_range is not None and not data.empty:
            start = pd.Timestamp(time_range[0])
            end = pd.Timestamp(time_range[1])
            data = data.loc[start:end]

        # Populate dynamic metadata from df.attrs (e.g. unit set by the reader).
        # Runs on the first real load for each unique time_range; subsequent
        # cache-hits skip _load_data entirely.
        self._cache_attrs_as_dynamic_metadata(data.attrs)

        if self._cache_enabled:
            self._cached_data[cache_key] = data
            return data.copy()
        return data

    def invalidate_cache(self, time_range: Any = None) -> "DataReference":
        """Clear cached data so the next :meth:`getData` call reloads from source.

        Parameters
        ----------
        time_range : tuple, optional
            When given, only the entry for that specific range is cleared.
            Pass ``None`` (the default) to clear the entire cache.

        Returns *self* for chaining.
        """
        if time_range is None:
            self._cached_data.clear()
        else:
            self._cached_data.pop(self._make_cache_key(time_range), None)
        return self

    # ------------------------------------------------------------------
    # Dynamic metadata (populated lazily from df.attrs on first getData)
    # ------------------------------------------------------------------

    def set_dynamic_metadata(self, key: str, value: Any) -> None:
        """Store a single metadata item discovered at data-load time.

        No-op when the stored value is already equal to *value*, so
        repeated loads do not dirty the dict unnecessarily.
        """
        if self._dynamic_metadata.get(key) != value:
            self._dynamic_metadata[key] = value

    def get_dynamic_metadata(self, key: str, default: Any = None) -> Any:
        """Return a dynamically discovered metadata value, or *default*."""
        return self._dynamic_metadata.get(key, default)

    def _cache_attrs_as_dynamic_metadata(self, attrs: Dict[str, Any]) -> None:
        """Populate *_dynamic_metadata* from *df.attrs* returned by a reader.

        Called inside :meth:`getData` after the first real load so that
        reader-supplied metadata (e.g. ``unit`` set by DSSReader) becomes
        visible in :meth:`DataCatalog.to_dataframe` without requiring an
        explicit attribute on the reference.
        """
        for key, value in attrs.items():
            self.set_dynamic_metadata(key, value)

    def _load_data(self, time_range: Any = None) -> pd.DataFrame:
        """Delegate loading to the attached :class:`DataReferenceReader`.

        ``time_range`` is merged into the attributes dict passed to the
        reader so that time-range-aware readers (e.g. DSS, HDF5) can read
        only the requested window without loading the full series.

        Subclasses (e.g. :class:`~dvue.math_reference.MathDataReference`) may
        override this method to compute data without a reader.
        """
        return self._get_reader().load(**{**self._attributes, "time_range": time_range})

    # ------------------------------------------------------------------
    # Arithmetic operator overloading → auto-creates MathDataReference
    # ------------------------------------------------------------------

    def __add__(self, other: Any) -> "MathDataReference":
        return _compose(self, other, "+")

    def __radd__(self, other: Any) -> "MathDataReference":
        return _compose_r(other, self, "+")

    def __sub__(self, other: Any) -> "MathDataReference":
        return _compose(self, other, "-")

    def __rsub__(self, other: Any) -> "MathDataReference":
        return _compose_r(other, self, "-")

    def __mul__(self, other: Any) -> "MathDataReference":
        return _compose(self, other, "*")

    def __rmul__(self, other: Any) -> "MathDataReference":
        return _compose_r(other, self, "*")

    def __truediv__(self, other: Any) -> "MathDataReference":
        return _compose(self, other, "/")

    def __rtruediv__(self, other: Any) -> "MathDataReference":
        return _compose_r(other, self, "/")

    def __pow__(self, other: Any) -> "MathDataReference":
        return _compose(self, other, "**")

    def __neg__(self) -> "MathDataReference":
        vname = self.name or f"_v{id(self) & 0xFFFFFF}"
        return MathDataReference(f"-({vname})", variable_map={vname: self})

    # ------------------------------------------------------------------
    # Dunder helpers
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        attrs = ", ".join(f"{k}={v!r}" for k, v in self._attributes.items())
        parts = [f"name={self.name!r}", f"source={self.source!r}", f"reader={self._reader_fqcn!r}"]
        if attrs:
            parts.append(attrs)
        return f"{self.__class__.__name__}({', '.join(parts)})"
    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.name!r})"


# ---------------------------------------------------------------------------
# Module-level helpers for DataReference operator overloading.
# MathDataReference is defined in dvue.math_reference and imported at the
# bottom of this module.  These functions reference it by name via the module
# globals, so the lazy bottom-import is sufficient.
# ---------------------------------------------------------------------------


def _compose(lhs: DataReference, rhs: Any, op: str) -> "MathDataReference":
    """Create a MathDataReference for ``lhs OP rhs``."""
    if isinstance(rhs, MathDataReference):
        return rhs._compose_r(lhs, op)  # delegate to preserve the expression tree
    lname = lhs.name or f"_v{id(lhs) & 0xFFFFFF}"
    vmap: Dict[str, DataReference] = {lname: lhs}
    if isinstance(rhs, DataReference):
        rname = rhs.name or f"_v{id(rhs) & 0xFFFFFF}"
        vmap[rname] = rhs
        expr = f"{lname} {op} {rname}"
    else:
        expr = f"{lname} {op} ({rhs!r})"
    return MathDataReference(expr, variable_map=vmap)


def _compose_r(lhs: Any, rhs: DataReference, op: str) -> "MathDataReference":
    """Create a MathDataReference for ``lhs OP rhs`` where *rhs* is the DataReference."""
    rname = rhs.name or f"_v{id(rhs) & 0xFFFFFF}"
    vmap: Dict[str, DataReference] = {rname: rhs}
    if isinstance(lhs, DataReference):
        lname = lhs.name or f"_v{id(lhs) & 0xFFFFFF}"
        vmap[lname] = lhs
        expr = f"{lname} {op} {rname}"
    else:
        expr = f"({lhs!r}) {op} {rname}"
    return MathDataReference(expr, variable_map=vmap)


# ---------------------------------------------------------------------------
# CatalogBuilder (ABC)
# ---------------------------------------------------------------------------


class CatalogBuilder(abc.ABC):
    """Abstract base class for objects that scan a source and build DataReferences.

    A ``CatalogBuilder`` is responsible for *discovering* what data is
    available in a source and constructing :class:`DataReference` objects
    pre-wired with the appropriate :class:`DataReferenceReader`.  It has no
    role in the actual data loading — that is delegated to the reader.

    Register instances with :meth:`DataCatalog.register_builder` (global) or
    :meth:`DataCatalog.add_builder` (instance-local).

    Subclasses **must** implement:

    * :meth:`can_handle(source) -> bool`
    * :meth:`build(source) -> List[DataReference]`

    Examples
    --------
    >>> class MyDBBuilder(CatalogBuilder):
    ...     def can_handle(self, source):
    ...         return isinstance(source, MyDBConnection)
    ...     def build(self, source):
    ...         reader = MyDBReader(source)   # a DataReferenceReader subclass
    ...         return [
    ...             DataReference(reader, name=t, table=t)
    ...             for t in source.list_tables()
    ...         ]
    ...
    >>> DataCatalog.register_builder(MyDBBuilder())
    """

    @abc.abstractmethod
    def can_handle(self, source: Any) -> bool:
        """Return ``True`` if this builder can construct references from *source*."""
        ...

    @abc.abstractmethod
    def build(self, source: Any) -> List[DataReference]:
        """Scan *source* and return a list of :class:`DataReference` objects.

        Each returned reference should be wired to the correct
        :class:`DataReferenceReader` subclass so that data is not loaded
        until :meth:`~DataReference.getData` is called.

        Parameters
        ----------
        source : Any

        Returns
        -------
        List[DataReference]
        """
        ...

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}()"


# Backward-compatible alias
DataCatalogReader = CatalogBuilder


# ---------------------------------------------------------------------------
# DataCatalog
# ---------------------------------------------------------------------------


class DataCatalog:
    """A searchable container of :class:`DataReference` objects.

    Features
    --------
    * Add / remove / retrieve references by name or by primary-key keyword args.
    * Search by metadata attributes with optional schema-map normalisation.
    * Load references in bulk from external sources via registered
      :class:`CatalogBuilder` objects.
    * Map heterogeneous raw attribute names to a canonical schema vocabulary.
    * Auto-compute ``source_num`` (integer per unique ``source``) and inject it
      as a column in :meth:`to_dataframe` when multiple sources are present.

    Parameters
    ----------
    primary_key : list of str
        Required.  The attribute names whose values together uniquely identify a
        reference in this catalog, e.g. ``["station", "variable"]``.  Include
        ``"source_num"`` as the first element for multi-source catalogs; it is
        auto-computed from ``ref.source`` and does not need to be stored on the ref.
    schema_map : dict, optional
        Maps *raw* attribute names (as stored in DataReferences) to
        *canonical* names exposed in :meth:`search` and :meth:`to_dataframe`.
        Example: ``{"stn_id": "id", "stn_nm": "name"}``.
    crs : str, optional
        Coordinate reference system string (e.g. ``"EPSG:4326"``) passed to
        :class:`~geopandas.GeoDataFrame` when :meth:`to_dataframe` detects a
        ``geometry`` column in the collected attributes.  Ignored when
        *geopandas* is not available or no ``geometry`` attribute is present.

    Builder registry
    ----------------
    :meth:`register_builder` (class method) adds a builder to the **global**
    registry; all new catalog instances inherit a copy of it.
    :meth:`add_builder` adds a builder to a **single instance** only.

    Examples
    --------
    >>> catalog = DataCatalog(primary_key=["station", "variable"])
    >>> reader = InMemoryDataReferenceReader(df)
    >>> catalog.add(DataReference(reader, station="S01", variable="temperature"))
    >>> catalog.search(variable="temperature")
    [DataReference(name='S01_temperature', ...)]
    >>> catalog.get(station="S01", variable="temperature")
    DataReference(name='S01_temperature', ...)
    >>> catalog.to_dataframe()              # summary DataFrame

    Multi-source catalog::

        catalog = DataCatalog(primary_key=["source_num", "station", "variable"])
        for path in files:
            catalog.add(DataReference(source=path, reader=..., station="A", variable="flow"))
        # source_num=0 for the first file, source_num=1 for the second, etc.
        # to_dataframe() includes a source_num column automatically.

    Bulk loading::

        catalog.add_builder(CSVDirectoryBuilder()).add_source("/data/csv/")
    """

    # Global builder registry – shared across all DataCatalog instances
    _global_builders: ClassVar[List[CatalogBuilder]] = []

    @classmethod
    def register_builder(cls, builder: CatalogBuilder) -> None:
        """Register *builder* globally.

        All :class:`DataCatalog` instances created **after** this call will
        include the builder in their search order.

        Parameters
        ----------
        builder : CatalogBuilder
        """
        cls._global_builders.append(builder)
        logger.debug("DataCatalog: globally registered builder %r", builder)

    # Backward-compatible alias
    @classmethod
    def register_reader(cls, reader: CatalogBuilder) -> None:  # type: ignore[override]
        """Backward-compatible alias for :meth:`register_builder`."""
        cls.register_builder(reader)

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    def __init__(
        self,
        primary_key: List[str],
        schema_map: Optional[Dict[str, str]] = None,
        crs: Optional[str] = None,
    ) -> None:
        if not primary_key:
            raise ValueError(
                "DataCatalog requires a non-empty primary_key list, e.g. "
                "primary_key=['station', 'variable']."
            )
        self._primary_key: List[str] = list(primary_key)
        self._references: Dict[str, DataReference] = {}
        # Maps each unique non-empty ref.source → integer index (order of first appearance).
        # source_num is never stored on the ref itself — it is derived from this mapping.
        self._source_index: Dict[str, int] = {}
        self._schema_map: Dict[str, str] = schema_map or {}
        self._crs: Optional[str] = crs
        # Start with a snapshot of the global registry; instance-local additions
        # do not affect other catalogs.
        self._builders: List[CatalogBuilder] = list(DataCatalog._global_builders)

    def add_builder(self, builder: CatalogBuilder) -> "DataCatalog":
        """Add *builder* to this catalog instance only (chainable)."""
        self._builders.append(builder)
        return self

    # Backward-compatible alias
    def add_reader(self, reader: CatalogBuilder) -> "DataCatalog":  # type: ignore[override]
        """Backward-compatible alias for :meth:`add_builder`."""
        return self.add_builder(reader)

    def _find_builder(self, source: Any) -> Optional[CatalogBuilder]:
        """Return the most-recently-registered builder that can handle *source*."""
        for builder in reversed(self._builders):
            if builder.can_handle(source):
                return builder
        return None

    def _find_reader(self, source: Any) -> Optional[CatalogBuilder]:  # type: ignore[override]
        """Backward-compatible alias for :meth:`_find_builder`."""
        return self._find_builder(source)

    # ------------------------------------------------------------------
    # Primary-key helpers
    # ------------------------------------------------------------------

    @property
    def primary_key(self) -> List[str]:
        """The attribute names that together uniquely identify a reference."""
        return list(self._primary_key)

    def _source_num_for(self, ref: "DataReference") -> Optional[int]:
        """Return the source_num for *ref*, or None if source is empty."""
        if not ref.source:
            return None
        return self._source_index.get(ref.source)

    @staticmethod
    def _sanitize_pk_value(value: Any) -> str:
        """Sanitize a single primary-key value to a valid identifier fragment."""
        s = re.sub(r"[^a-zA-Z0-9]+", "_", str(value).strip()).strip("_")
        if s and s[0].isdigit():
            s = "_" + s
        return s

    def _derive_name(self, ref: "DataReference") -> str:
        """Derive an auto-name from primary_key values on *ref*.

        For multi-source catalogs (``"source_num"`` in primary_key) the name
        is prefixed with ``s{n}_``.
        """
        parts = []
        if "source_num" in self._primary_key and ref.source:
            snum = self._source_index.get(ref.source)
            if snum is not None:
                parts.append(f"s{snum}")
        for col in self._primary_key:
            if col in ("source_num", "name"):
                continue
            value = ref.get_attribute(col)
            if value is None:
                continue
            sanitized = self._sanitize_pk_value(value)
            if sanitized:
                parts.append(sanitized)
        return "_".join(parts)

    def _pk_tuple(self, ref: "DataReference") -> tuple:
        """Return a tuple of primary-key values for *ref* (for uniqueness checks).

        Handles special columns: ``"name"`` reads from ``ref.name``;
        ``"source_num"`` reads from ``_source_index[ref.source]``.

        A ``"tag"`` attribute is always appended as the final discriminator,
        even when ``"tag"`` is not declared in *primary_key*.  This ensures
        that a :class:`~dvue.math_reference.MathDataReference` produced by
        ``TransformToCatalogAction`` (which carries a non-empty ``tag`` such
        as ``"tf"`` or ``"1D_mean"``) never collides with its raw source ref
        (whose ``tag`` attribute is absent / ``None``).  No subclass or app
        needs to add ``"tag"`` to its ``primary_key`` list.
        """
        values = []
        for col in self._primary_key:
            if col == "source_num":
                values.append(self._source_index.get(ref.source))
            elif col == "name":
                values.append(ref.name)
            else:
                values.append(ref.get_attribute(col))
        # Append tag as implicit trailing discriminator (None for raw refs).
        if "tag" not in self._primary_key:
            values.append(ref.get_attribute("tag"))
        return tuple(values)

    # ------------------------------------------------------------------
    # CRUD operations
    # ------------------------------------------------------------------

    def add(self, ref: DataReference) -> "DataCatalog":
        """Add a :class:`DataReference` to the catalog (chainable).

        Auto-derives ``ref.name`` from the primary-key values when the name is
        empty.  Raises :class:`ValueError` when the primary-key tuple already
        exists in the catalog (duplicate detection) or when the name is still
        empty after auto-derivation.

        Maintains the :attr:`_source_index` mapping so that ``source_num`` can
        be auto-computed without storing it on the ref itself.

        Parameters
        ----------
        ref : DataReference

        Raises
        ------
        ValueError
            * Name is empty and cannot be derived from primary_key values.
            * A different reference with the same primary-key tuple already exists.
            * An identical duplicate is re-added.
        """
        # 1. Maintain _source_index before deriving the name (needed for source_num prefix).
        if ref.source and ref.source not in self._source_index:
            self._source_index[ref.source] = len(self._source_index)

        # 2. Auto-derive name from primary_key when the ref has no name set.
        if not ref.name:
            ref.name = self._derive_name(ref)
        if not ref.name:
            raise ValueError(
                "DataReference has no name and could not derive one from primary_key "
                f"{self._primary_key!r}.  Set the name explicitly or ensure the reference "
                "has non-empty values for all primary_key attributes."
            )

        # 3. Check for primary-key collision (different name, same pk values).
        pk = self._pk_tuple(ref)
        for existing_name, existing_ref in self._references.items():
            if existing_name == ref.name:
                continue  # same-name replacement handled below
            if self._pk_tuple(existing_ref) == pk:
                raise ValueError(
                    f"A DataReference with primary-key {dict(zip(self._primary_key, pk))!r} "
                    f"already exists in the catalog under the name {existing_name!r}.  "
                    "Remove it first or use different primary-key values."
                )

        # 4. Same-name exact-duplicate guard.
        existing = self._references.get(ref.name)
        if existing is not None:
            same_source: bool = False
            if existing._reader_instance is not None and ref._reader_instance is not None:
                same_source = existing._reader_instance is ref._reader_instance
            elif existing._reader_instance is None and ref._reader_instance is None:
                same_source = (
                    existing._reader_fqcn == ref._reader_fqcn
                    and existing.source == ref.source
                )
            if same_source and existing.attributes == ref.attributes:
                raise ValueError(
                    f"A DataReference named {ref.name!r} with the same reader, source, and "
                    "identical metadata attributes already exists in the catalog. "
                    "Remove it first, or update its attributes before re-adding."
                )

        self._references[ref.name] = ref
        return self

    def add_source(self, source: Any) -> "DataCatalog":
        """Construct and add references from *source* via a registered builder (chainable).

        Parameters
        ----------
        source : Any

        Raises
        ------
        ValueError
            If no registered builder can handle *source*.
        """
        builder = self._find_builder(source)
        if builder is None:
            raise ValueError(
                f"No registered DataCatalogReader can handle {source!r}. "
                "Call add_builder() or DataCatalog.register_builder() first."
            )
        refs = builder.build(source)
        for ref in refs:
            self.add(ref)
        logger.info(
            "DataCatalog: added %d reference(s) from %r via %r",
            len(refs),
            source,
            builder,
        )
        return self

    def remove(self, name: str) -> "DataCatalog":
        """Remove the reference named *name* (chainable).

        Raises
        ------
        KeyError
            If no reference with that name exists.
        """
        if name not in self._references:
            raise KeyError(f"No DataReference named {name!r} in catalog.")
        del self._references[name]
        return self

    def rename(self, old_name: str, new_name: str) -> "DataCatalog":
        """Rename a reference in the catalog (chainable).

        Updates both the internal dictionary key and the reference's own
        ``name`` attribute atomically.  Does not affect ``_source_index`` —
        the source (file path) is unchanged by a rename.

        Use this instead of mutating ``ref.name`` directly — direct mutation
        leaves the dictionary key stale and causes ``catalog.get(ref.name)``
        to raise ``KeyError``.

        Parameters
        ----------
        old_name : str
            Current name of the reference.
        new_name : str
            New name for the reference.

        Raises
        ------
        KeyError
            If no reference with ``old_name`` exists in the catalog.
        ValueError
            If ``new_name`` is already taken by a different reference.
        """
        if old_name not in self._references:
            raise KeyError(f"No DataReference named {old_name!r} in catalog.")
        if new_name != old_name and new_name in self._references:
            raise ValueError(
                f"Cannot rename {old_name!r} to {new_name!r}: a reference "
                f"named {new_name!r} already exists in the catalog."
            )
        if old_name == new_name:
            return self
        ref = self._references.pop(old_name)
        ref.name = new_name
        self._references[new_name] = ref
        return self

    def get(self, name: Optional[str] = None, **pk_kwargs: Any) -> DataReference:
        """Retrieve a :class:`DataReference` by name or by primary-key keyword arguments.

        Call as ``catalog.get("my_ref")`` to look up by name, or as
        ``catalog.get(station="A", variable="flow")`` to look up by primary-key
        values.  Both forms raise :class:`KeyError` when not found.

        Raises
        ------
        KeyError
        TypeError
            If called with neither a name nor keyword arguments, or with both.
        """
        if name is not None and pk_kwargs:
            raise TypeError("get() accepts either a positional name or keyword pk arguments, not both.")
        if name is None and not pk_kwargs:
            raise TypeError("get() requires either a positional name or keyword pk arguments.")
        if name is not None:
            try:
                return self._references[name]
            except KeyError:
                raise KeyError(f"No DataReference named {name!r} in catalog.") from None
        # Keyword pk lookup — find by matching pk columns.
        for ref in self._references.values():
            match = True
            for col, expected in pk_kwargs.items():
                if col == "source_num":
                    actual = self._source_index.get(ref.source)
                elif col == "name":
                    actual = ref.name
                else:
                    actual = ref.get_attribute(col)
                if actual != expected:
                    match = False
                    break
            if match:
                return ref
        raise KeyError(
            f"No DataReference with primary-key {pk_kwargs!r} in catalog."
        ) from None

    # ------------------------------------------------------------------
    # Dict-like interface
    # ------------------------------------------------------------------

    def __getitem__(self, name: str) -> DataReference:
        return self.get(name)

    def __contains__(self, name: str) -> bool:
        return name in self._references

    def __len__(self) -> int:
        return len(self._references)

    def __iter__(self):
        return iter(self._references.values())

    # ------------------------------------------------------------------
    # Search
    # ------------------------------------------------------------------

    def search(self, **criteria: Any) -> List[DataReference]:
        """Return all references whose metadata matches *criteria*.

        Criteria keys may use either raw attribute names or their canonical
        equivalents (defined in *schema_map*); both resolve correctly.

        The special key ``name`` is matched against the reference's
        :attr:`~DataReference.name` attribute (not stored in ``_attributes``)
        so that ``catalog.search(name="my_ref")`` works as expected.

        The special key ``source_num`` is matched against the catalog's
        auto-computed source index, not against a stored attribute.  Use it
        to filter references to a particular source file, e.g.
        ``catalog.search(source_num=0)``.

        Each criterion value may be:

        * a **scalar** – exact equality check.
        * a **string starting with "~"** – case-insensitive regex fullmatch
          (e.g. ``name="~station.*"`` matches any name beginning with
          ``station``, case-insensitively).
        * a **callable** ``f(value) -> bool`` – custom predicate.

        Examples
        --------
        >>> catalog.search(variable="temperature")
        >>> catalog.search(name="my_ref")
        >>> catalog.search(name="~station.*")           # regex on name
        >>> catalog.search(variable="~EC.*")            # regex on attribute
        >>> catalog.search(year=lambda y: int(y) >= 2020)
        >>> catalog.search(variable="temperature", unit="degC")
        >>> catalog.search(source_num=0)                # first source only
        """
        # Pop special keys that are not stored as regular attributes.
        name_criterion = criteria.pop("name", _UNSET)
        source_num_criterion = criteria.pop("source_num", _UNSET)
        raw_criteria = self._to_raw_criteria(criteria)

        results = []
        for r in self._references.values():
            if name_criterion is not _UNSET:
                if callable(name_criterion):
                    if not name_criterion(r.name):
                        continue
                elif not _criterion_matches(r.name, name_criterion):
                    continue
            if source_num_criterion is not _UNSET:
                actual_snum = self._source_index.get(r.source)
                if callable(source_num_criterion):
                    if not source_num_criterion(actual_snum):
                        continue
                elif not _criterion_matches(actual_snum, source_num_criterion):
                    continue
            if r.matches(**raw_criteria):
                results.append(r)
        return results

    def _to_raw_criteria(self, criteria: Dict[str, Any]) -> Dict[str, Any]:
        """Translate canonical criteria keys back to raw attribute names."""
        canonical_to_raw = {v: k for k, v in self._schema_map.items()}
        return {canonical_to_raw.get(k, k): v for k, v in criteria.items()}

    # ------------------------------------------------------------------
    # Schema mapping
    # ------------------------------------------------------------------

    def set_schema_map(self, schema_map: Dict[str, str]) -> "DataCatalog":
        """Replace the schema map and return *self* (chainable).

        Parameters
        ----------
        schema_map : dict
            Maps raw attribute names → canonical names.
        """
        self._schema_map = dict(schema_map)
        return self

    def get_canonical_attribute(self, ref_name: str, canonical: str) -> Any:
        """Get an attribute value from a reference using its canonical name.

        Parameters
        ----------
        ref_name : str
        canonical : str
            Canonical attribute name (as defined in schema_map).

        Raises
        ------
        KeyError
            If *ref_name* is not in the catalog.
        """
        canonical_to_raw = {v: k for k, v in self._schema_map.items()}
        raw = canonical_to_raw.get(canonical, canonical)
        return self.get(ref_name).get_attribute(raw)

    # ------------------------------------------------------------------
    # Listing / inspection
    # ------------------------------------------------------------------

    def list(self) -> List[DataReference]:
        """Return all :class:`DataReference` objects in insertion order."""
        return list(self._references.values())

    def list_names(self) -> List[str]:
        """Return all reference names in insertion order."""
        return list(self._references.keys())

    def invalidate_all_caches(self) -> "DataCatalog":
        """Clear the in-memory data cache on every :class:`DataReference` in this
        catalog (chainable).

        Useful before a UI session starts fresh or when source data has changed
        on disk.  Each reference's ``_cached_data`` dict is cleared; the next
        :meth:`~DataReference.getData` call will reload from the source.
        """
        for ref in self._references.values():
            ref.invalidate_cache()
        return self

    def to_dataframe(self) -> pd.DataFrame:
        """Return a :class:`~pandas.DataFrame` summarising all references.

        Each row represents one :class:`DataReference`.  Columns are the
        union of all attribute names, translated through *schema_map*.
        The index is the reference name.

        If every reference carries a ``geometry`` attribute (a Shapely
        geometry) and *geopandas* is importable, the result is a
        :class:`~geopandas.GeoDataFrame` with that column set as the active
        geometry and :attr:`crs` applied when the catalog was constructed with
        a non-``None`` *crs* argument.

        Returns
        -------
        pd.DataFrame or geopandas.GeoDataFrame
        """
        rows = []
        for ref in self._references.values():
            row: Dict[str, Any] = {"name": ref.name, "ref_type": ref.ref_type}
            # Dynamic metadata has lower priority: apply first so static
            # _attributes can override it (e.g. explicit unit: in YAML wins
            # over the unit inherited from a loaded variable).
            row.update(ref._dynamic_metadata)
            for raw_key, val in ref.attributes.items():
                canonical = self._schema_map.get(raw_key, raw_key)
                row[canonical] = val
            rows.append(row)

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows).set_index("name")

        # Inject source_num column when multiple distinct sources are present.
        if len(self._source_index) > 1:
            source_col = df.get("source")
            if source_col is not None:
                df.insert(0, "source_num", source_col.map(self._source_index))

        if "geometry" in df.columns:
            try:
                import geopandas as gpd  # noqa: PLC0415

                return gpd.GeoDataFrame(df, geometry="geometry", crs=self._crs)
            except ImportError:
                pass

        return df

    # ------------------------------------------------------------------
    # Serialisation
    # ------------------------------------------------------------------

    def to_csv(self, path: Union[str, Path]) -> None:
        """Serialise all references to a CSV file.

        Columns: ``name``, ``source``, ``reader`` (fully qualified class name),
        followed by the union of all attribute columns across all references.
        Rows that lack a given attribute receive ``NaN``.

        Parameters
        ----------
        path : str or Path
        """
        rows = [ref.to_dict() for ref in self._references.values()]
        if not rows:
            pd.DataFrame(columns=["name", "source", "reader"]).to_csv(path, index=False)
            return
        pd.DataFrame(rows).to_csv(path, index=False)

    @classmethod
    def from_csv(cls, path: Union[str, Path]) -> "DataCatalog":
        """Load a :class:`DataCatalog` from a CSV file written by :meth:`to_csv`.

        Each row's ``reader`` column is the fully qualified class name of the
        :class:`DataReferenceReader` subclass.  The reader is instantiated
        lazily (on the first :meth:`~DataReference.getData` call) as
        ``reader_class(source)``.

        Parameters
        ----------
        path : str or Path

        Returns
        -------
        DataCatalog
        """
        df = pd.read_csv(path)
        catalog = cls(primary_key=["name"])
        for _, row in df.iterrows():
            d: Dict[str, Any] = {k: v for k, v in row.items() if pd.notna(v)}
            name = str(d.pop("name", ""))
            source = str(d.pop("source", ""))
            reader = str(d.pop("reader", ""))
            ref_type = str(d.pop("ref_type", "raw"))
            ref = DataReference(source=source, reader=reader, name=name, **d)
            if ref_type != "raw":
                ref.ref_type = ref_type
            catalog.add(ref)
        return catalog

    # ------------------------------------------------------------------
    # Dunder
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        return f"DataCatalog({len(self._references)} references)"

    def __str__(self) -> str:
        names = list(self._references.keys())[:5]
        suffix = f", … (+{len(self._references) - 5} more)" if len(self._references) > 5 else ""
        return f"DataCatalog[{', '.join(names)}{suffix}]"


# ---------------------------------------------------------------------------
# CatalogView
# ---------------------------------------------------------------------------


class CatalogView(DataCatalog):
    """A live, read-only filtered view of a :class:`DataCatalog`.

    ``CatalogView`` wraps a source catalog and exposes only those
    :class:`DataReference` objects whose metadata attributes match the
    given *selection* criteria.  The view is re-evaluated lazily on every
    access, so additions or removals on the source catalog are immediately
    visible through the view.

    Because ``CatalogView`` is a subclass of :class:`DataCatalog` it can
    be used anywhere a catalog is accepted — including as the ``catalog=``
    argument of :class:`MathDataReference`.

    ``CatalogView`` is **read-only**: calling :meth:`add`, :meth:`remove`,
    or :meth:`add_source` raises :class:`TypeError`.  Modify the source
    catalog directly.

    Parameters
    ----------
    catalog : DataCatalog
        The source catalog (or another ``CatalogView``) to filter.
    selection : dict or callable, optional
        Criteria applied to each :class:`DataReference` in the source:

        * ``dict`` – ``{attr: value}`` exact match, or
          ``{attr: callable(value) -> bool}`` predicate.  All criteria
          must match (AND semantics).
        * ``callable`` – ``f(ref: DataReference) -> bool`` custom
          per-reference predicate.

        ``None`` means no filtering; all references pass through.
    name : str, optional
        A descriptive label for this view.

    Examples
    --------
    >>> cat = DataCatalog(primary_key=["name"])
    >>> cat.add(DataReference(df_a, name="flow",  source="USGS", stationid="STA001"))
    >>> cat.add(DataReference(df_b, name="stage", source="CDEC", stationid="STA002"))
    >>> cat.add(DataReference(df_c, name="temp",  source="USGS", stationid="STA001"))
    >>>
    >>> usgs = CatalogView(cat, selection={"source": "USGS"})
    >>> usgs.list_names()
    ['flow', 'temp']
    >>>
    >>> # Chain another filter with AND semantics
    >>> sta1_usgs = usgs.select({"stationid": "STA001"})
    >>> sta1_usgs.list_names()
    ['flow', 'temp']          # both already matched STA001
    >>>
    >>> # Callable predicate
    >>> view = CatalogView(cat, selection=lambda r: r.get_attribute("source") == "USGS")
    """

    def __init__(
        self,
        catalog: "DataCatalog",
        selection: Optional[Union[Dict[str, Any], Callable]] = None,
        name: str = "",
    ) -> None:
        # Inherit the source catalog's schema_map and primary_key so canonical lookups work.
        super().__init__(primary_key=list(catalog._primary_key), schema_map=dict(catalog._schema_map))
        self._source_catalog = catalog
        self._selection = selection
        self.name = name

    # ------------------------------------------------------------------
    # Core: lazily apply selection against the source catalog
    # ------------------------------------------------------------------

    def _matched(self) -> Dict[str, "DataReference"]:
        """Return {name: ref} for every reference that passes *selection*.

        Iterates the source using ``__iter__`` so nested ``CatalogView``
        sources are handled transparently.
        """
        refs: Dict[str, DataReference] = {r.name: r for r in self._source_catalog}
        if self._selection is None:
            return refs
        sel = self._selection
        if callable(sel):
            return {n: r for n, r in refs.items() if sel(r)}
        if isinstance(sel, dict):
            return {n: r for n, r in refs.items() if r.matches(**sel)}
        raise TypeError(
            f"Unsupported selection type {type(sel)!r}. "
            "Expected dict or callable(DataReference) -> bool."
        )

    # ------------------------------------------------------------------
    # Read-side DataCatalog overrides
    # ------------------------------------------------------------------

    def __contains__(self, name: str) -> bool:  # type: ignore[override]
        return name in self._matched()

    def __len__(self) -> int:
        return len(self._matched())

    def __iter__(self):
        return iter(self._matched().values())

    def get(self, name: str) -> "DataReference":
        m = self._matched()
        if name not in m:
            raise KeyError(f"No DataReference named {name!r} in CatalogView.")
        return m[name]

    def __getitem__(self, name: str) -> "DataReference":
        return self.get(name)

    def list(self) -> List["DataReference"]:
        """Return all matching :class:`DataReference` objects."""
        return list(self._matched().values())

    def list_names(self) -> List[str]:
        """Return the names of all matching references."""
        return list(self._matched().keys())

    def search(self, **criteria: Any) -> List["DataReference"]:
        """Search within the filtered subset using metadata criteria."""
        raw = self._to_raw_criteria(criteria)
        return [r for r in self._matched().values() if r.matches(**raw)]

    def to_dataframe(self) -> pd.DataFrame:
        """Return a :class:`~pandas.DataFrame` summarising the filtered references."""
        rows = []
        for ref in self._matched().values():
            row: Dict[str, Any] = {"name": ref.name}
            for raw_key, val in ref.attributes.items():
                canonical = self._schema_map.get(raw_key, raw_key)
                row[canonical] = val
            rows.append(row)
        if not rows:
            return pd.DataFrame()
        return pd.DataFrame(rows).set_index("name")

    # ------------------------------------------------------------------
    # Narrowing: chain a new CatalogView with AND semantics
    # ------------------------------------------------------------------

    def select(
        self,
        selection: Union[Dict[str, Any], Callable],
        name: str = "",
    ) -> "CatalogView":
        """Return a new :class:`CatalogView` that further filters *this* view.

        Criteria accumulate with AND semantics: the returned view contains
        only references that pass *both* this view's selection and *selection*.

        Parameters
        ----------
        selection : dict or callable
            See class-level documentation.
        name : str, optional
            Label for the new view.

        Returns
        -------
        CatalogView
        """
        return CatalogView(self, selection=selection, name=name)

    # ------------------------------------------------------------------
    # Mutations disabled
    # ------------------------------------------------------------------

    def add(self, ref: Any) -> "CatalogView":  # type: ignore[override]
        raise TypeError(
            "CatalogView is read-only. Add DataReferences to the source catalog instead."
        )

    def remove(self, name: str) -> "CatalogView":  # type: ignore[override]
        raise TypeError(
            "CatalogView is read-only. Remove DataReferences from the source catalog instead."
        )

    def add_source(self, source: Any) -> "CatalogView":  # type: ignore[override]
        raise TypeError(
            "CatalogView is read-only. Call add_source() on the source catalog instead."
        )

    # ------------------------------------------------------------------
    # Dunder
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        return (
            f"CatalogView(name={self.name!r}, "
            f"selection={self._selection!r}, "
            f"count={len(self)})"
        )


# ---------------------------------------------------------------------------
# Backward-compatible re-exports from dvue.readers
# ---------------------------------------------------------------------------
# CSVDirectoryReader, PatternCSVDirectoryReader, and _pattern_to_regex have
# been moved to dvue.readers (the canonical home for sample reader
# implementations).  They are re-imported here so that code that already
# does ``from dvue.catalog import CSVDirectoryReader`` continues to work.

from .readers import (  # noqa: E402, F401
    _pattern_to_regex,
    CSVDirectoryBuilder,
    CSVDirectoryReader,
    PatternCSVDirectoryBuilder,
    PatternCSVDirectoryReader,
)


# ---------------------------------------------------------------------------
# Re-exports from dvue.math_reference
# ---------------------------------------------------------------------------
# MathDataReference, MathDataCatalogReader, and save_math_refs live in
# dvue.math_reference (their canonical location).  They are imported here so
# that existing code using ``from dvue.catalog import MathDataReference``
# continues to work without modification.
#
# NOTE: this import MUST remain at the bottom of this module so that
# DataReference (defined above) is already in the catalog namespace when
# math_reference.py runs ``from .catalog import DataReference``.

from .math_reference import (  # noqa: E402, F401
    MathDataReference,
    MathDataCatalogReader,
    save_math_refs,
    _MATH_NAMESPACE,
    _RESERVED_TOKENS,
)


# ---------------------------------------------------------------------------
# Convenience builder
# ---------------------------------------------------------------------------

def build_catalog_from_dataframe(
    dfcat: "pd.DataFrame",
    reader: DataReferenceReader,
    ref_name_fn,
    primary_key: "Optional[List[str]]" = None,
    crs: "Optional[str]" = None,
    ref_class: type = DataReference,
) -> DataCatalog:
    """Build a :class:`DataCatalog` from a metadata DataFrame.

    Eliminates the copy-paste ``_build_dvue_catalog`` pattern that every
    :class:`~dvue.tsdataui.TimeSeriesDataUIManager` subclass used to repeat.

    Parameters
    ----------
    dfcat : pd.DataFrame or GeoDataFrame
        One row per data series.  All columns (except ``"geometry"``) are
        stored as :class:`DataReference` attributes and forwarded to the
        reader's :meth:`~DataReferenceReader.load` call.
    reader : DataReferenceReader
        Shared (flyweight) reader instance used by every added reference.
    ref_name_fn : callable
        ``ref_name_fn(row) -> str`` — returns the unique catalog key for
        each row.  Must be reconstructable from the columns shown in the
        table so that :meth:`~dvue.dataui.DataUIManager.get_data_reference`
        can look up the correct entry.
    primary_key : list of str, optional
        The attribute names that together uniquely identify a reference in
        the catalog (e.g. ``["station", "variable"]``).  Defaults to
        ``["name"]`` when not provided.
    crs : str, optional
        CRS string forwarded to :class:`DataCatalog`.
    ref_class : type, optional
        :class:`DataReference` subclass to instantiate for each row.
        Defaults to :class:`DataReference`.  Pass a custom subclass to
        attach a domain-specific ``ref_type`` (e.g. ``ref_type = "dsm2_dss"``).

    Returns
    -------
    DataCatalog
    """
    pk = primary_key if primary_key is not None else ["name"]
    catalog = DataCatalog(primary_key=pk, crs=crs)
    for _, row in dfcat.iterrows():
        attrs = {k: v for k, v in row.items() if k != "geometry"}
        if "geometry" in row.index and row["geometry"] is not None:
            attrs["geometry"] = row["geometry"]
        ref = ref_class(
            reader=reader,
            name=ref_name_fn(row),
            cache=True,
            **attrs,
        )
        catalog.add(ref)
    return catalog
