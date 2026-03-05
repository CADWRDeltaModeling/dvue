"""Data catalog module for dvue.

Provides classes for managing references to data sources, organising them into
searchable catalogs, and composing derived datasets via mathematical expressions.

Classes
-------
DataReference
    A reference to a data source with associated (name, value) metadata.
CatalogView
    A live, read-only filtered view of a DataCatalog, selecting a subset of
    its DataReferences based on metadata criteria.
MathDataReference
    A DataReference that computes data by evaluating a mathematical expression
    over other DataReferences resolved from a variable map or catalog.
DataCatalogReader
    Abstract base class for objects that construct DataReferences from sources.
DataCatalog
    A searchable container of DataReferences with schema mapping and dynamic
    reader registration.

Sample reader implementations (CSVDirectoryReader, PatternCSVDirectoryReader)
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

# ---------------------------------------------------------------------------
# DataReference
# ---------------------------------------------------------------------------


class DataReference:
    """A reference to a data source with associated metadata attributes.

    ``DataReference`` wraps a data source and exposes it through a consistent
    :meth:`getData` API.  Results are optionally cached after the first load.

    Parameters
    ----------
    source : Any
        Where to load data from.  Supported types:

        * ``pd.DataFrame`` / ``pd.Series`` – used directly.
        * ``callable`` – called with no arguments; must return a DataFrame.
        * ``str`` or :class:`pathlib.Path` – local file path.  Supported
          extensions: ``.csv``, ``.tsv``, ``.parquet``, ``.feather``,
          ``.json``, ``.xlsx``, ``.xls``, ``.hdf``, ``.h5``.
        * URL string (``http://`` / ``https://``) – fetched as CSV, Parquet,
          or JSON based on the URL extension.

    name : str, optional
        Identifier.  Required when adding to a :class:`DataCatalog`.
    cache : bool, optional
        Cache the result of the first :meth:`getData` call.  Default ``True``.
    **attributes
        Arbitrary (name, value) metadata pairs searchable via
        :meth:`DataCatalog.search`.

    Examples
    --------
    >>> ref = DataReference(df, name="stations", variable="temperature", unit="degC")
    >>> ref.getData()                       # returns the DataFrame
    >>> ref.get_attribute("variable")
    'temperature'
    >>> ref.set_attribute("reviewed", True).set_attribute("priority", 1)
    """

    # Class-level map of file extension → loader callable
    _FILE_LOADERS: ClassVar[Dict[str, Callable[[str], pd.DataFrame]]] = {
        ".csv": pd.read_csv,
        ".tsv": lambda p: pd.read_csv(p, sep="\t"),
        ".parquet": pd.read_parquet,
        ".feather": pd.read_feather,
        ".json": pd.read_json,
        ".xlsx": pd.read_excel,
        ".xls": pd.read_excel,
        ".hdf": pd.read_hdf,
        ".h5": pd.read_hdf,
    }

    def __init__(
        self,
        source: Any,
        name: str = "",
        cache: bool = True,
        **attributes: Any,
    ) -> None:
        self.source = source
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
        """Low-level data loading.  Override in subclasses for custom logic."""
        source = self.source

        if source is None:
            raise ValueError(
                f"{self.__class__.__name__} has no source. "
                "Override _load_data() or getData() to provide data."
            )

        if isinstance(source, pd.DataFrame):
            return source.copy()

        if isinstance(source, pd.Series):
            return source.to_frame()

        if callable(source):
            result = source()
            if isinstance(result, pd.DataFrame):
                return result
            if isinstance(result, pd.Series):
                return result.to_frame()
            return pd.DataFrame(result)

        if isinstance(source, (str, Path)):
            s = str(source)
            if s.startswith(("http://", "https://")):
                return self._load_url(s)
            return self._load_file(s)

        # Last resort: attempt DataFrame construction
        try:
            return pd.DataFrame(source)
        except Exception as exc:
            raise ValueError(f"Cannot load data from source {source!r}: {exc}") from exc

    def _load_file(self, path: str) -> pd.DataFrame:
        suffix = Path(path).suffix.lower()
        loader = self._FILE_LOADERS.get(suffix)
        if loader is None:
            raise ValueError(
                f"Unsupported file extension {suffix!r}. " f"Supported: {list(self._FILE_LOADERS)}"
            )
        return loader(path)

    def _load_url(self, url: str) -> pd.DataFrame:
        lower = url.lower()
        if lower.endswith(".parquet"):
            return pd.read_parquet(url)
        if lower.endswith(".json"):
            return pd.read_json(url)
        return pd.read_csv(url)

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
        parts = [f"name={self.name!r}", f"source={self.source!r}"]
        if attrs:
            parts.append(attrs)
        return f"{self.__class__.__name__}({', '.join(parts)})"

    def __str__(self) -> str:
        return f"{self.__class__.__name__}({self.name!r})"


# ---------------------------------------------------------------------------
# MathDataReference – safe expression evaluation namespace
# ---------------------------------------------------------------------------

#: Safe built-ins available inside MathDataReference expressions.
#: NumPy vectorised functions work element-wise on pandas Series/arrays.
_MATH_NAMESPACE: Dict[str, Any] = {
    "__builtins__": {},
    # Libraries (advanced use)
    "np": np,
    "pd": pd,
    "math": math,
    # Vectorised scalar functions
    "abs": np.abs,
    "sin": np.sin,
    "cos": np.cos,
    "tan": np.tan,
    "arcsin": np.arcsin,
    "arccos": np.arccos,
    "arctan": np.arctan,
    "arctan2": np.arctan2,
    "exp": np.exp,
    "log": np.log,
    "log2": np.log2,
    "log10": np.log10,
    "sqrt": np.sqrt,
    "ceil": np.ceil,
    "floor": np.floor,
    "round": np.round,
    "clip": np.clip,
    "where": np.where,
    "min": np.minimum,
    "max": np.maximum,
    "sum": np.sum,
    "mean": np.mean,
    "std": np.std,
    "diff": np.diff,
    "cumsum": np.cumsum,
    # Constants
    "pi": math.pi,
    "e": math.e,
    "nan": float("nan"),
    "inf": float("inf"),
}

# Tokens that are part of the expression namespace, not variable names
_RESERVED_TOKENS: frozenset = frozenset(_MATH_NAMESPACE) | {
    "True",
    "False",
    "None",
    "and",
    "or",
    "not",
    "in",
    "is",
}


class MathDataReference(DataReference):
    """A :class:`DataReference` whose data is computed from a mathematical
    expression evaluated over other :class:`DataReference` objects.

    Variables in the expression are resolved from *variable_map* first, then
    from an optionally attached :class:`DataCatalog`.  The expression is
    executed in a namespace that exposes NumPy ufuncs (``sin``, ``exp``,
    ``sqrt``, ``where``, …) so they can be used directly.

    Parameters
    ----------
    expression : str
        An arithmetic expression string, e.g. ``"A + B * 2"`` or
        ``"sqrt(X**2 + Y**2)"``.  Variable names must be valid Python
        identifiers matching keys in *variable_map* or names in the catalog.
    variable_map : dict, optional
        ``{variable_name: DataReference}`` explicit mapping.
    catalog : DataCatalog, optional
        Used to look up variables not present in *variable_map*.
    name : str, optional
        Identifier for this reference.
    cache : bool, optional
        Cache the evaluated result.  Defaults to ``False`` because referenced
        data may change between calls.
    **attributes
        Metadata.

    Notes
    -----
    Operator overloading is available on all :class:`DataReference` subclasses::

        combined = ref_a + ref_b * 2          # MathDataReference
        normalised = (ref_signal - ref_base) / ref_scale

    Examples
    --------
    >>> a = DataReference(df_a, name="A")
    >>> b = DataReference(df_b, name="B")
    >>> expr = MathDataReference("A + B", variable_map={"A": a, "B": b})
    >>> expr.getData()
    >>> mag = MathDataReference("sqrt(X**2 + Y**2)", variable_map={"X": rx, "Y": ry})
    """

    def __init__(
        self,
        expression: str,
        variable_map: Optional[Dict[str, DataReference]] = None,
        catalog: Optional["DataCatalog"] = None,
        name: str = "",
        cache: bool = False,
        **attributes: Any,
    ) -> None:
        super().__init__(source=None, name=name, cache=cache, **attributes)
        self.expression = expression
        self._variable_map: Dict[str, DataReference] = dict(variable_map or {})
        self._catalog = catalog

    # ------------------------------------------------------------------
    # Configuration helpers
    # ------------------------------------------------------------------

    def set_catalog(self, catalog: "DataCatalog") -> "MathDataReference":
        """Attach a catalog for variable resolution (chainable)."""
        self._catalog = catalog
        return self

    def set_variable(self, var_name: str, ref: DataReference) -> "MathDataReference":
        """Map *var_name* to a :class:`DataReference` (chainable)."""
        self._variable_map[var_name] = ref
        return self

    # ------------------------------------------------------------------
    # Variable resolution and expression evaluation
    # ------------------------------------------------------------------

    def _resolve_variables(self) -> Dict[str, Any]:
        """Parse the expression and resolve each identifier to a Series or DataFrame."""
        tokens = set(re.findall(r"\b([a-zA-Z_][a-zA-Z0-9_]*)\b", self.expression))
        tokens -= _RESERVED_TOKENS

        resolved: Dict[str, Any] = {}
        for tok in tokens:
            ref: Optional[DataReference] = None

            if tok in self._variable_map:
                ref = self._variable_map[tok]
            elif self._catalog is not None and tok in self._catalog:
                ref = self._catalog.get(tok)

            if ref is None:
                # Unknown identifier – let eval() raise a NameError naturally
                continue

            data = ref.getData()
            if isinstance(data, pd.DataFrame):
                # Unwrap single-column DataFrames to Series for scalar arithmetic
                resolved[tok] = data.iloc[:, 0] if len(data.columns) == 1 else data
            else:
                resolved[tok] = data

        return resolved

    def _load_data(self) -> pd.DataFrame:
        variables = self._resolve_variables()

        if not variables:
            raise ValueError(
                f"No variables could be resolved for expression {self.expression!r}. "
                "Populate variable_map or call set_catalog()."
            )

        ns = {**_MATH_NAMESPACE, **variables}
        try:
            result = eval(self.expression, ns)  # noqa: S307
        except Exception as exc:
            raise ValueError(f"Failed to evaluate expression {self.expression!r}: {exc}") from exc

        if isinstance(result, pd.DataFrame):
            return result
        if isinstance(result, pd.Series):
            return result.to_frame(name=self.name or "result")
        # Scalar result
        return pd.DataFrame({"result": [result]})

    # ------------------------------------------------------------------
    # Arithmetic operators (override parent to preserve expression trees)
    # ------------------------------------------------------------------

    def __add__(self, other: Any) -> "MathDataReference":
        return self._compose(other, "+")

    def __radd__(self, other: Any) -> "MathDataReference":
        return self._compose_r(other, "+")

    def __sub__(self, other: Any) -> "MathDataReference":
        return self._compose(other, "-")

    def __rsub__(self, other: Any) -> "MathDataReference":
        return self._compose_r(other, "-")

    def __mul__(self, other: Any) -> "MathDataReference":
        return self._compose(other, "*")

    def __rmul__(self, other: Any) -> "MathDataReference":
        return self._compose_r(other, "*")

    def __truediv__(self, other: Any) -> "MathDataReference":
        return self._compose(other, "/")

    def __rtruediv__(self, other: Any) -> "MathDataReference":
        return self._compose_r(other, "/")

    def __pow__(self, other: Any) -> "MathDataReference":
        return self._compose(other, "**")

    def __neg__(self) -> "MathDataReference":
        return MathDataReference(
            f"-({self.expression})",
            variable_map=dict(self._variable_map),
            catalog=self._catalog,
        )

    def _compose(self, other: Any, op: str) -> "MathDataReference":
        """Build ``(self.expression) OP other``."""
        if isinstance(other, MathDataReference):
            new_expr = f"({self.expression}) {op} ({other.expression})"
            new_vars = {**self._variable_map, **other._variable_map}
            catalog = self._catalog or other._catalog
        elif isinstance(other, DataReference):
            vname = other.name or f"_v{id(other) & 0xFFFFFF}"
            new_expr = f"({self.expression}) {op} {vname}"
            new_vars = {**self._variable_map, vname: other}
            catalog = self._catalog
        else:
            new_expr = f"({self.expression}) {op} ({other!r})"
            new_vars = dict(self._variable_map)
            catalog = self._catalog
        return MathDataReference(new_expr, variable_map=new_vars, catalog=catalog)

    def _compose_r(self, other: Any, op: str) -> "MathDataReference":
        """Build ``other OP (self.expression)``."""
        if isinstance(other, DataReference):
            vname = other.name or f"_v{id(other) & 0xFFFFFF}"
            new_expr = f"{vname} {op} ({self.expression})"
            new_vars = {**self._variable_map, vname: other}
            catalog = self._catalog
        else:
            new_expr = f"({other!r}) {op} ({self.expression})"
            new_vars = dict(self._variable_map)
            catalog = self._catalog
        return MathDataReference(new_expr, variable_map=new_vars, catalog=catalog)

    def __repr__(self) -> str:
        return f"MathDataReference(name={self.name!r}, " f"expression={self.expression!r})"


# ---------------------------------------------------------------------------
# Module-level helper functions for DataReference operator overloading
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
# DataCatalogReader (ABC)
# ---------------------------------------------------------------------------


class DataCatalogReader(abc.ABC):
    """Abstract base class for objects that construct DataReferences from sources.

    Subclass this to support new source types (databases, cloud object stores,
    REST APIs, etc.) and register instances with
    :meth:`DataCatalog.register_reader` or :meth:`DataCatalog.add_reader`.

    Subclasses **must** implement:

    * :meth:`can_handle(source) -> bool`
    * :meth:`read(source) -> List[DataReference]`

    Examples
    --------
    >>> class MyDBReader(DataCatalogReader):
    ...     def can_handle(self, source):
    ...         return isinstance(source, MyDBConnection)
    ...     def read(self, source):
    ...         tables = source.list_tables()
    ...         return [DataReference(source.query_table(t), name=t) for t in tables]
    ...
    >>> DataCatalog.register_reader(MyDBReader())
    """

    @abc.abstractmethod
    def can_handle(self, source: Any) -> bool:
        """Return ``True`` if this reader is able to produce references from *source*."""
        ...

    @abc.abstractmethod
    def read(self, source: Any) -> List[DataReference]:
        """Read *source* and return a list of :class:`DataReference` objects.

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


# ---------------------------------------------------------------------------
# DataCatalog
# ---------------------------------------------------------------------------


def _sources_equal(a: Any, b: Any) -> bool:
    """Return ``True`` when two DataReference sources are equal.

    Uses identity check first (fast path).  For pandas objects, delegates to
    the DataFrame/Series `.equals()` method to avoid ambiguous-truth-value
    errors from element-wise ``==``.  For all other types falls back to a
    standard ``==`` comparison, guarding against any exception.
    """
    if a is b:
        return True
    if isinstance(a, (pd.DataFrame, pd.Series)) and isinstance(b, (pd.DataFrame, pd.Series)):
        return a.equals(b)
    try:
        return bool(a == b)
    except Exception:
        return False


class DataCatalog:
    """A searchable container of :class:`DataReference` objects.

    Features
    --------
    * Add / remove / retrieve references by name.
    * Search by metadata attributes with optional schema-map normalisation.
    * Load references in bulk from external sources via registered
      :class:`DataCatalogReader` objects.
    * Map heterogeneous raw attribute names to a canonical schema vocabulary.

    Parameters
    ----------
    schema_map : dict, optional
        Maps *raw* attribute names (as stored in DataReferences) to
        *canonical* names exposed in :meth:`search` and :meth:`to_dataframe`.
        Example: ``{"stn_id": "id", "stn_nm": "name"}``.

    Reader registry
    ---------------
    :meth:`register_reader` (class method) adds a reader to the **global**
    registry; all new catalog instances inherit a copy of it.
    :meth:`add_reader` adds a reader to a **single instance** only.

    Examples
    --------
    >>> catalog = DataCatalog(schema_map={"stn_id": "id"})
    >>> catalog.add(DataReference(df, name="temp", variable="temperature", stn_id="S01"))
    >>> catalog.search(variable="temperature")
    [DataReference(name='temp', ...)]
    >>> catalog.search(id="S01")            # canonical name works too
    [DataReference(name='temp', ...)]
    >>> catalog.to_dataframe()              # summary DataFrame

    Bulk loading::

        catalog.add_reader(CSVDirectoryReader()).add_source("/data/csv/")
    """

    # Global reader registry – shared across all DataCatalog instances
    _global_readers: ClassVar[List[DataCatalogReader]] = []

    @classmethod
    def register_reader(cls, reader: DataCatalogReader) -> None:
        """Register *reader* globally.

        All :class:`DataCatalog` instances created **after** this call will
        include the reader in their search order.

        Parameters
        ----------
        reader : DataCatalogReader
        """
        cls._global_readers.append(reader)
        logger.debug("DataCatalog: globally registered reader %r", reader)

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    def __init__(self, schema_map: Optional[Dict[str, str]] = None) -> None:
        self._references: Dict[str, DataReference] = {}
        self._schema_map: Dict[str, str] = schema_map or {}
        # Start with a snapshot of the global registry; instance-local additions
        # do not affect other catalogs.
        self._readers: List[DataCatalogReader] = list(DataCatalog._global_readers)

    def add_reader(self, reader: DataCatalogReader) -> "DataCatalog":
        """Add *reader* to this catalog instance only (chainable)."""
        self._readers.append(reader)
        return self

    def _find_reader(self, source: Any) -> Optional[DataCatalogReader]:
        """Return the most-recently-registered reader that can handle *source*."""
        for reader in reversed(self._readers):
            if reader.can_handle(source):
                return reader
        return None

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
            if _sources_equal(existing.source, ref.source):
                if existing.attributes == ref.attributes:
                    raise ValueError(
                        f"A DataReference named {ref.name!r} with the same source and "
                        "identical metadata attributes already exists in the catalog. "
                        "Remove it first, or update its attributes before re-adding."
                    )
        self._references[ref.name] = ref
        return self

    def add_source(self, source: Any) -> "DataCatalog":
        """Construct and add references from *source* via a registered reader (chainable).

        Parameters
        ----------
        source : Any

        Raises
        ------
        ValueError
            If no registered reader can handle *source*.
        """
        reader = self._find_reader(source)
        if reader is None:
            raise ValueError(
                f"No registered DataCatalogReader can handle {source!r}. "
                "Call add_reader() or DataCatalog.register_reader() first."
            )
        refs = reader.read(source)
        for ref in refs:
            self.add(ref)
        logger.info(
            "DataCatalog: added %d reference(s) from %r via %r",
            len(refs),
            source,
            reader,
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

        Each criterion value may be:

        * a **scalar** – exact equality check.
        * a **callable** ``f(value) -> bool`` – custom predicate.

        Examples
        --------
        >>> catalog.search(variable="temperature")
        >>> catalog.search(year=lambda y: int(y) >= 2020)
        >>> catalog.search(variable="temperature", unit="degC")
        """
        raw_criteria = self._to_raw_criteria(criteria)
        return [r for r in self._references.values() if r.matches(**raw_criteria)]

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
    CSVDirectoryReader,
    PatternCSVDirectoryReader,
)
