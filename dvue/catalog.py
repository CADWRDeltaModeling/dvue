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
    >>> ref = DataReference(reader, name="users", table="users")
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
    >>> ref = DataReference(reader, name="stations", variable="temperature")
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
    >>> ref = DataReference(reader, name="live", variable="temperature")
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

    Because all path information lives in the :class:`DataReference`
    attributes, a *single* ``FileDataReferenceReader()`` instance can be
    shared across any number of file-backed references (flyweight).

    Parameters
    ----------
    read_kwargs : dict, optional
        Extra keyword arguments forwarded to the underlying pandas reader
        (e.g. ``{"parse_dates": ["timestamp"]}`` for CSV files).

    Examples
    --------
    >>> reader = FileDataReferenceReader()
    >>> ref = DataReference(reader, name="flow", file_path="/data/flow.csv")
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

    def __init__(self, read_kwargs: Optional[Dict[str, Any]] = None) -> None:
        self._read_kwargs: Dict[str, Any] = read_kwargs or {}

    def load(self, **attributes: Any) -> pd.DataFrame:
        file_path = attributes.get("file_path")
        if file_path is None:
            raise ValueError(
                "FileDataReferenceReader requires a 'file_path' attribute on the "
                "DataReference.  Set it via the attributes keyword arguments."
            )
        s = str(file_path)
        if s.startswith(("http://", "https://")):
            return self._load_url(s)
        return self._load_file(s)

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
        if self._read_kwargs:
            return f"FileDataReferenceReader(read_kwargs={self._read_kwargs!r})"
        return "FileDataReferenceReader()"


# ---------------------------------------------------------------------------
# DataReference
# ---------------------------------------------------------------------------


class DataReference:
    """A reference to a data source identified by metadata attributes.

    ``DataReference`` delegates actual loading to a :class:`DataReferenceReader`
    and exposes the result through a consistent :meth:`getData` API.  All
    source-specific logic lives in the reader; the reference owns caching and
    the metadata attribute dict.

    Parameters
    ----------
    reader : DataReferenceReader
        The strategy object that knows how to load data.  Built-in choices:

        * :class:`InMemoryDataReferenceReader` – wrap an existing DataFrame.
        * :class:`CallableDataReferenceReader` – call a zero-argument callable.
        * :class:`FileDataReferenceReader` – read a file via a ``file_path``
          attribute (supports CSV, Parquet, JSON, HDF, Excel, …).

        Pass ``None`` (or omit) only in :class:`MathDataReference` subclasses
        that override :meth:`_load_data` directly.
    name : str, optional
        Identifier.  Required when adding to a :class:`DataCatalog`.
    cache : bool, optional
        Cache the result of the first :meth:`getData` call.  Default ``True``.
    **attributes
        Arbitrary (name, value) metadata pairs passed to the reader's
        :meth:`~DataReferenceReader.load` and searchable via
        :meth:`DataCatalog.search`.

    Examples
    --------
    >>> reader = InMemoryDataReferenceReader(df)
    >>> ref = DataReference(reader, name="stations", variable="temperature", unit="degC")
    >>> ref.getData()                       # returns the DataFrame
    >>> ref.get_attribute("variable")
    'temperature'
    >>> ref.set_attribute("reviewed", True).set_attribute("priority", 1)
    """

    def __init__(
        self,
        reader: Optional[DataReferenceReader] = None,
        name: str = "",
        cache: bool = True,
        **attributes: Any,
    ) -> None:
        self._reader = reader
        self.name = name
        self._cache_enabled: bool = cache
        self._cached_data: Optional[pd.DataFrame] = None
        self._attributes: Dict[str, Any] = dict(attributes)

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

    def matches(self, **criteria: Any) -> bool:
        """Return ``True`` if *all* criteria match this reference's metadata.

        Each criterion value can be:

        * a **scalar** – exact equality check.
        * a **callable** ``f(value) -> bool`` – custom predicate.
        """
        for key, expected in criteria.items():
            actual = self._attributes.get(key)
            if callable(expected):
                if not expected(actual):
                    return False
            elif actual != expected:
                return False
        return True

    def ref_key(self) -> str:
        """Return a string key derived from this reference's metadata attributes.

        The default implementation joins all string-representable attribute values
        with ``"_"`` separators, sanitising spaces and non-identifier characters
        to underscores.  The result is intended to be a valid Python identifier
        so it can be used as a variable name inside :class:`MathDataReference`
        expression strings.

        Override in subclasses to produce a more readable, domain-specific key
        from a chosen subset of attributes.

        Examples
        --------
        >>> ref = DataReference(df, name="r", station="A", variable="wind", interval="hourly")
        >>> ref.ref_key()
        'A_wind_hourly'
        """
        parts = []
        for value in self._attributes.values():
            if not isinstance(value, (str, int, float, bool)):
                continue
            sanitized = re.sub(r"[^a-zA-Z0-9]+", "_", str(value).strip())
            sanitized = sanitized.strip("_")
            if sanitized:
                parts.append(sanitized)
        return "_".join(parts)

    # ------------------------------------------------------------------
    # Data loading
    # ------------------------------------------------------------------

    def getData(self) -> pd.DataFrame:
        """Load and return the data from the source.

        The result is cached after the first call when *cache* is ``True``
        (the default).  Call :meth:`invalidate_cache` to force a fresh load.

        Returns
        -------
        pd.DataFrame
        """
        if self._cache_enabled and self._cached_data is not None:
            return self._cached_data.copy()

        data = self._load_data()

        if self._cache_enabled:
            self._cached_data = data
            return data.copy()
        return data

    def invalidate_cache(self) -> "DataReference":
        """Clear cached data so the next :meth:`getData` call reloads the source.

        Returns *self* for chaining.
        """
        self._cached_data = None
        return self

    def _load_data(self) -> pd.DataFrame:
        """Delegate loading to the attached :class:`DataReferenceReader`.

        Subclasses (e.g. :class:`~dvue.math_reference.MathDataReference`) may
        override this method to compute data without a reader.
        """
        if self._reader is None:
            raise ValueError(
                f"{self.__class__.__name__} has no DataReferenceReader. "
                "Supply a reader at construction time or override _load_data()."
            )
        return self._reader.load(**self._attributes)

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
        parts = [f"name={self.name!r}", f"reader={self._reader!r}"]
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


def _readers_equal(a: Any, b: Any) -> bool:
    """Return ``True`` when two DataReference readers are the same instance.

    Identity comparison is the right test: two different reader *instances*
    that happen to load the same data are still considered distinct (the user
    may have intentionally replaced one with the other).
    """
    return a is b


class DataCatalog:
    """A searchable container of :class:`DataReference` objects.

    Features
    --------
    * Add / remove / retrieve references by name.
    * Search by metadata attributes with optional schema-map normalisation.
    * Load references in bulk from external sources via registered
      :class:`CatalogBuilder` objects.
    * Map heterogeneous raw attribute names to a canonical schema vocabulary.

    Parameters
    ----------
    schema_map : dict, optional
        Maps *raw* attribute names (as stored in DataReferences) to
        *canonical* names exposed in :meth:`search` and :meth:`to_dataframe`.
        Example: ``{"stn_id": "id", "stn_nm": "name"}``.

    Builder registry
    ----------------
    :meth:`register_builder` (class method) adds a builder to the **global**
    registry; all new catalog instances inherit a copy of it.
    :meth:`add_builder` adds a builder to a **single instance** only.

    Examples
    --------
    >>> catalog = DataCatalog(schema_map={"stn_id": "id"})
    >>> reader = InMemoryDataReferenceReader(df)
    >>> catalog.add(DataReference(reader, name="temp", variable="temperature", stn_id="S01"))
    >>> catalog.search(variable="temperature")
    [DataReference(name='temp', ...)]
    >>> catalog.search(id="S01")            # canonical name works too
    [DataReference(name='temp', ...)]
    >>> catalog.to_dataframe()              # summary DataFrame

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

    def __init__(self, schema_map: Optional[Dict[str, str]] = None) -> None:
        self._references: Dict[str, DataReference] = {}
        self._schema_map: Dict[str, str] = schema_map or {}
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

    # Keep old name for any code that calls it directly
    def _find_reader(self, source: Any) -> Optional[CatalogBuilder]:  # type: ignore[override]
        """Backward-compatible alias for :meth:`_find_builder`."""
        return self._find_builder(source)

    # ------------------------------------------------------------------
    # CRUD operations
    # ------------------------------------------------------------------

    def add(self, ref: DataReference) -> "DataCatalog":
        """Add a :class:`DataReference` to the catalog (chainable).

        If a reference with the same name already exists its source and every
        metadata attribute are compared against the incoming reference.  An
        identical duplicate (same name **and** same source **and** identical
        attributes) raises a :class:`ValueError`.  A same-named reference that
        differs in source or metadata is **replaced** silently.

        Parameters
        ----------
        ref : DataReference
            Must have a non-empty :attr:`~DataReference.name`.

        Raises
        ------
        ValueError
            If *ref* is an exact duplicate of an existing entry.
        """
        if not ref.name:
            raise ValueError("DataReference must have a non-empty name to be added to a catalog.")
        existing = self._references.get(ref.name)
        if existing is not None:
            if _readers_equal(existing._reader, ref._reader):
                if existing.attributes == ref.attributes:
                    raise ValueError(
                        f"A DataReference named {ref.name!r} with the same reader and "
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

    def get(self, name: str) -> DataReference:
        """Retrieve the :class:`DataReference` named *name*.

        Raises
        ------
        KeyError
        """
        try:
            return self._references[name]
        except KeyError:
            raise KeyError(f"No DataReference named {name!r} in catalog.") from None

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

        Each criterion value may be:

        * a **scalar** – exact equality check.
        * a **callable** ``f(value) -> bool`` – custom predicate.

        Examples
        --------
        >>> catalog.search(variable="temperature")
        >>> catalog.search(name="my_ref")
        >>> catalog.search(year=lambda y: int(y) >= 2020)
        >>> catalog.search(variable="temperature", unit="degC")
        """
        # Pop ``name`` before schema translation – it lives on ref.name, not _attributes.
        name_criterion = criteria.pop("name", _UNSET)
        raw_criteria = self._to_raw_criteria(criteria)

        results = []
        for r in self._references.values():
            if name_criterion is not _UNSET:
                if callable(name_criterion):
                    if not name_criterion(r.name):
                        continue
                elif r.name != name_criterion:
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

    def to_dataframe(self) -> pd.DataFrame:
        """Return a :class:`~pandas.DataFrame` summarising all references.

        Each row represents one :class:`DataReference`.  Columns are the
        union of all attribute names, translated through *schema_map*.
        The index is the reference name.

        Returns
        -------
        pd.DataFrame
        """
        rows = []
        for ref in self._references.values():
            row: Dict[str, Any] = {"name": ref.name}
            for raw_key, val in ref.attributes.items():
                canonical = self._schema_map.get(raw_key, raw_key)
                row[canonical] = val
            rows.append(row)

        if not rows:
            return pd.DataFrame()
        return pd.DataFrame(rows).set_index("name")

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
    >>> cat = DataCatalog()
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
        # Inherit the source catalog's schema_map so canonical lookups work.
        super().__init__(schema_map=dict(catalog._schema_map))
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
