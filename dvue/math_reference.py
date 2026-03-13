"""Mathematical expression DataReference and YAML persistence helpers.

Defines :class:`MathDataReference`, a :class:`~dvue.catalog.DataReference`
subclass that evaluates an arithmetic expression over resolved variable
bindings, plus :class:`MathDataCatalogReader` and :func:`save_math_refs` for
round-tripping specs through YAML files.

Classes
-------
MathDataReference
    Evaluates a Python expression whose variables are resolved from an
    explicit ``variable_map``, a ``search_map`` (criteria-based catalog
    lookup), or direct name lookup in an attached catalog.
MathDataCatalogReader
    A :class:`~dvue.catalog.CatalogBuilder` that loads
    ``MathDataReference`` specs from a YAML file.

Functions
---------
save_math_refs
    Serialise all ``MathDataReference`` objects in a catalog to a YAML file.
"""

from __future__ import annotations

import math
import re
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Union

import numpy as np
import pandas as pd

# DataReference is defined early in catalog.py, so this import is safe even
# though catalog.py re-imports MathDataReference at its bottom for backward
# compatibility.  Python's partial-module mechanism ensures DataReference is
# already present in the catalog module namespace by the time this line runs.
from .catalog import DataReference  # noqa: E402
from .catalog import CatalogBuilder  # noqa: E402

# Backward-compatible alias used in some external code
DataCatalogReader = CatalogBuilder

# ---------------------------------------------------------------------------
# Safe expression evaluation namespace
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

# ---------------------------------------------------------------------------
# MathDataReference
# ---------------------------------------------------------------------------


class MathDataReference(DataReference):
    """A :class:`~dvue.catalog.DataReference` whose data is computed from a
    mathematical expression evaluated over other
    :class:`~dvue.catalog.DataReference` objects.

    Variables in the expression are resolved in priority order:

    1. *variable_map* – direct ``{name: DataReference}`` bindings.
    2. *search_map* – each variable is looked up in the attached catalog by
       metadata criteria.  When *require_single* is ``True`` (the default)
       only the first result is used; when ``False`` all matching results are
       fetched and joined into a single DataFrame (axis=1 join by index;
       falls back to axis=0 row-concat if the join cannot be performed).
    3. Catalog name-lookup – if the identifier matches a reference name in the
       attached catalog it is used directly.

    The expression is evaluated in a namespace that exposes NumPy ufuncs
    (``sin``, ``exp``, ``sqrt``, ``where``, …) so they can be used directly.

    Parameters
    ----------
    expression : str
        An arithmetic expression string, e.g. ``"A + B * 2"`` or
        ``"sqrt(X**2 + Y**2)"``.
    variable_map : dict, optional
        ``{variable_name: DataReference}`` explicit mapping.
    search_map : dict, optional
        ``{variable_name: {attr: value, …}}`` catalog search criteria.
        See also :meth:`set_search`.
    search_require_single : dict, optional
        ``{variable_name: bool}`` per-variable *require_single* flag.
        When not present for a variable the flag defaults to ``True``.
    catalog : DataCatalog, optional
        Used to look up variables not present in *variable_map* / *search_map*.
    name : str, optional
        Identifier for this reference.
    cache : bool, optional
        Cache the evaluated result.  Defaults to ``False`` because referenced
        data may change between calls.
    **attributes
        Metadata.

    Notes
    -----
    Operator overloading is available on all
    :class:`~dvue.catalog.DataReference` subclasses::

        combined  = ref_a + ref_b * 2          # MathDataReference
        normalised = (ref_signal - ref_base) / ref_scale

    Examples
    --------
    >>> from dvue.catalog import DataReference, DataCatalog
    >>> from dvue.math_reference import MathDataReference
    >>> a = DataReference(df_a, name="A")
    >>> b = DataReference(df_b, name="B")
    >>> expr = MathDataReference("A + B", variable_map={"A": a, "B": b})
    >>> expr.getData()
    >>> # Catalog search (multiple results joined by index)
    >>> cat = DataCatalog()
    >>> m = MathDataReference("inflow - outflow",
    ...     search_map={"inflow": {"variable": "discharge", "location": "upstream"},
    ...                 "outflow": {"variable": "discharge", "location": "downstream"}},
    ...     search_require_single={"inflow": False, "outflow": False},
    ...     catalog=cat)
    >>> m.getData()
    """

    def __init__(
        self,
        expression: str,
        variable_map: Optional[Dict[str, Any]] = None,
        search_map: Optional[Dict[str, Dict[str, Any]]] = None,
        search_require_single: Optional[Dict[str, bool]] = None,
        catalog: Optional[Any] = None,
        name: str = "",
        cache: bool = False,
        **attributes: Any,
    ) -> None:
        super().__init__(name=name, cache=cache, **attributes)
        # expression stored in _attributes so it appears in to_dataframe()
        self.expression = expression
        self._variable_map: Dict[str, Any] = dict(variable_map or {})
        self._search_map: Dict[str, Dict[str, Any]] = dict(search_map or {})
        # Per-variable require_single flag.  Missing keys default to True at
        # resolution time so callers only need to set False explicitly.
        self._search_require_single: Dict[str, bool] = dict(search_require_single or {})
        self._catalog = catalog

    # expression is stored in _attributes so it appears as a column in
    # to_dataframe() automatically.
    @property
    def expression(self) -> str:
        return self._attributes.get("expression", "")

    @expression.setter
    def expression(self, value: str) -> None:
        self._attributes["expression"] = value

    # ------------------------------------------------------------------
    # Configuration helpers
    # ------------------------------------------------------------------

    def set_catalog(self, catalog: Any) -> "MathDataReference":
        """Attach a catalog for variable resolution (chainable)."""
        self._catalog = catalog
        return self

    def set_variable(self, var_name: str, ref: Any) -> "MathDataReference":
        """Map *var_name* to a :class:`~dvue.catalog.DataReference` (chainable)."""
        self._variable_map[var_name] = ref
        return self

    def set_search(
        self,
        var_name: str,
        require_single: bool = True,
        **criteria: Any,
    ) -> "MathDataReference":
        """Map *var_name* to a catalog search query (chainable).

        At evaluation time the catalog is searched with *criteria* to find the
        :class:`~dvue.catalog.DataReference` (or references) substituted for
        *var_name* in the expression.

        Parameters
        ----------
        var_name : str
            The identifier as it appears in :attr:`expression`.
        require_single : bool, optional
            If ``True`` (default) only the **first** matching result is used.
            If ``False`` all matching results are fetched and combined into
            a single DataFrame: first attempts an index-aligned join
            (``axis=1``); falls back to row-concatenation (``axis=0``) when
            the join cannot be performed.
        **criteria
            Keyword arguments forwarded to :meth:`~dvue.catalog.DataCatalog.search`.
        """
        self._search_map[var_name] = dict(criteria)
        self._search_require_single[var_name] = require_single
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
            ref: Optional[Any] = None

            if tok in self._variable_map:
                ref = self._variable_map[tok]

            elif tok in self._search_map:
                if self._catalog is None:
                    raise ValueError(
                        f"Variable {tok!r}: search_map entry requires a catalog – "
                        "call set_catalog() before getData()."
                    )
                criteria = self._search_map[tok]
                require_single = self._search_require_single.get(tok, True)
                results = self._catalog.search(**criteria)
                if not results:
                    raise ValueError(
                        f"Variable {tok!r}: catalog search with criteria "
                        f"{criteria!r} returned no results."
                    )
                if require_single:
                    # Use only the first matching result.
                    ref = results[0]
                else:
                    # Join all matching results.  Prefer axis=1 (side-by-side
                    # columns sharing the time index); fall back to axis=0
                    # (stack rows) when the join fails.
                    frames: List[pd.DataFrame] = []
                    for r in results:
                        d = r.getData()
                        frames.append(d if isinstance(d, pd.DataFrame) else d.to_frame())
                    try:
                        data = pd.concat(frames, axis=1).sort_index()
                    except Exception:
                        data = pd.concat(frames, axis=0).sort_index()
                    # Keep single-column results as Series so arithmetic with
                    # other Series variables works naturally (same as the
                    # single-result branch below).
                    if isinstance(data, pd.DataFrame) and len(data.columns) == 1:
                        resolved[tok] = data.iloc[:, 0]
                    else:
                        resolved[tok] = data
                    continue

            elif self._catalog is not None and tok in self._catalog:
                ref = self._catalog.get(tok)

            if ref is None:
                # Unknown identifier – let eval() raise a NameError naturally.
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
    # Arithmetic operators – produce new MathDataReference trees
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
            search_map=dict(self._search_map),
            search_require_single=dict(self._search_require_single),
            catalog=self._catalog,
        )

    def _compose(self, other: Any, op: str) -> "MathDataReference":
        """Build ``(self.expression) OP other``."""
        if isinstance(other, MathDataReference):
            new_expr = f"({self.expression}) {op} ({other.expression})"
            new_vars = {**self._variable_map, **other._variable_map}
            new_search = {**self._search_map, **other._search_map}
            new_req = {**self._search_require_single, **other._search_require_single}
            catalog = self._catalog or other._catalog
        elif isinstance(other, DataReference):
            vname = other.name or f"_v{id(other) & 0xFFFFFF}"
            new_expr = f"({self.expression}) {op} {vname}"
            new_vars = {**self._variable_map, vname: other}
            new_search = dict(self._search_map)
            new_req = dict(self._search_require_single)
            catalog = self._catalog
        else:
            new_expr = f"({self.expression}) {op} ({other!r})"
            new_vars = dict(self._variable_map)
            new_search = dict(self._search_map)
            new_req = dict(self._search_require_single)
            catalog = self._catalog
        return MathDataReference(
            new_expr,
            variable_map=new_vars,
            search_map=new_search,
            search_require_single=new_req,
            catalog=catalog,
        )

    def _compose_r(self, other: Any, op: str) -> "MathDataReference":
        """Build ``other OP (self.expression)``."""
        if isinstance(other, DataReference):
            vname = other.name or f"_v{id(other) & 0xFFFFFF}"
            new_expr = f"{vname} {op} ({self.expression})"
            new_vars = {**self._variable_map, vname: other}
            new_search = dict(self._search_map)
            new_req = dict(self._search_require_single)
            catalog = self._catalog
        else:
            new_expr = f"({other!r}) {op} ({self.expression})"
            new_vars = dict(self._variable_map)
            new_search = dict(self._search_map)
            new_req = dict(self._search_require_single)
            catalog = self._catalog
        return MathDataReference(
            new_expr,
            variable_map=new_vars,
            search_map=new_search,
            search_require_single=new_req,
            catalog=catalog,
        )

    def __repr__(self) -> str:
        parts = [f"name={self.name!r}", f"expression={self.expression!r}"]
        if self._search_map:
            parts.append(f"search_map={self._search_map!r}")
        return f"MathDataReference({', '.join(parts)})"


# ---------------------------------------------------------------------------
# MathDataCatalogReader – load MathDataReference specs from YAML
# ---------------------------------------------------------------------------


class MathDataCatalogReader(CatalogBuilder):
    """Load :class:`MathDataReference` objects from a YAML file.

    Each entry in the YAML file must contain at least ``name`` and
    ``expression`` keys.  All remaining keys (except ``search_map`` and
    ``search_require_single``) become metadata attributes on the resulting
    :class:`MathDataReference`.

    Variable names in expressions are resolved lazily against the
    *parent_catalog* at ``getData()`` time.

    Parameters
    ----------
    parent_catalog : DataCatalog, optional
        Catalog wired into each :class:`MathDataReference` via
        :meth:`~MathDataReference.set_catalog`.
    **default_attrs
        Metadata applied to every created reference (overridden per-entry).

    YAML format
    -----------
    The recommended approach is to use ``search_map`` to resolve every variable
    alias by catalog attributes rather than by a hardcoded catalog key.  This
    keeps expressions portable and decouples them from internal naming
    conventions::

        # Unit conversion: single catalog match → result is a Series.
        - name: Station_A__wind_speed_mph__hourly
          expression: obs * 2.23694
          station_name: Station A
          variable: wind_speed_mph
          unit: mph
          interval: hourly
          search_map:
            obs:
              station_name: Station A
              variable: wind_speed
              interval: hourly

        # Multi-station aggregate: _require_single: false concatenates all
        # matching references into a DataFrame (axis=1 join).
        - name: mean_wind_speed__all_stations__hourly
          expression: ws.mean(axis=1)
          variable: mean_wind_speed
          unit: m/s
          interval: hourly
          search_map:
            ws:
              variable: wind_speed
              interval: hourly
              _require_single: false

    As a fallback, omitting ``search_map`` causes each expression token to be
    looked up directly by name in the parent catalog::

        # Direct catalog-name lookup (legacy / simple cases only).
        - name: bias_water_level__RIO001
          expression: "water_level_usgs - model_RIO001"
          variable: water_level_bias
          unit: m
    """

    def __init__(
        self,
        parent_catalog: Optional[Any] = None,
        **default_attrs: Any,
    ) -> None:
        self._parent_catalog = parent_catalog
        self._default_attrs = default_attrs

    def with_catalog(self, catalog: Any) -> "MathDataCatalogReader":
        """Attach *catalog* for variable resolution (chainable)."""
        self._parent_catalog = catalog
        return self

    def can_handle(self, source: Any) -> bool:
        """Accept ``.yaml`` / ``.yml`` file paths."""
        if isinstance(source, (str, Path)):
            return str(source).lower().endswith((".yaml", ".yml"))
        return False

    def build(self, source: Any) -> list:
        """Parse *source* YAML and return a list of :class:`MathDataReference` objects."""
        import yaml  # pyyaml – standard in most data-science environments

        with open(source) as fh:
            data = yaml.safe_load(fh)

        # Support both a bare list and a dict with a ``math_refs`` key.
        if isinstance(data, dict):
            data = data.get("math_refs", [])

        refs = []
        for entry in data or []:
            entry = dict(entry)  # defensive copy
            name = entry.pop("name")
            expression = entry.pop("expression")
            search_map_raw = entry.pop("search_map", None)
            search_require_single: Dict[str, bool] = {}

            if search_map_raw:
                # Extract the special _require_single key from each criteria dict.
                cleaned: Dict[str, Dict[str, Any]] = {}
                for var, criteria in search_map_raw.items():
                    criteria = dict(criteria)
                    req = criteria.pop("_require_single", True)
                    # Coerce to bool in case the YAML was written as a string.
                    search_require_single[var] = bool(req)
                    cleaned[var] = criteria
                search_map_raw = cleaned

            attrs = {**self._default_attrs, **entry}
            ref = MathDataReference(
                expression=expression,
                name=name,
                search_map=search_map_raw if search_map_raw else None,
                search_require_single=search_require_single if search_require_single else None,
                **attrs,
            )
            if self._parent_catalog is not None:
                ref.set_catalog(self._parent_catalog)
            refs.append(ref)
        return refs


# ---------------------------------------------------------------------------
# save_math_refs – serialise MathDataReference specs to YAML
# ---------------------------------------------------------------------------


def save_math_refs(catalog: Any, path: Union[str, Path]) -> None:
    """Serialise all :class:`MathDataReference` objects in *catalog* to YAML.

    The produced file can be loaded back with :class:`MathDataCatalogReader`.
    Each entry preserves ``name``, ``expression``, all metadata attributes,
    the ``search_map`` (scalar criteria only – callable predicates are dropped),
    and the per-variable ``_require_single`` flags.

    Parameters
    ----------
    catalog : DataCatalog
        Source catalog.
    path : str or Path
        Destination ``.yaml`` / ``.yml`` file.  Parent directory must exist.
    """
    import yaml

    entries = []
    for ref in catalog.list():
        if not isinstance(ref, MathDataReference):
            continue
        # Only serialise YAML-safe primitive values from attributes.
        safe_attrs = {
            k: v
            for k, v in ref._attributes.items()
            if isinstance(v, (str, int, float, bool, type(None)))
        }
        entry: Dict[str, Any] = {"name": ref.name, **safe_attrs}

        # Persist search_map with _require_single embedded.
        if ref._search_map:
            safe_search: Dict[str, Dict[str, Any]] = {}
            for var, criteria in ref._search_map.items():
                safe_criteria = {
                    k: v
                    for k, v in criteria.items()
                    if isinstance(v, (str, int, float, bool, type(None)))
                }
                # Embed the require_single flag so the YAML is self-contained.
                req = ref._search_require_single.get(var, True)
                if not req:
                    # Only write the flag when it differs from the default (True)
                    # to keep YAML concise.
                    safe_criteria["_require_single"] = False
                if safe_criteria:
                    safe_search[var] = safe_criteria
            if safe_search:
                entry["search_map"] = safe_search

        entries.append(entry)

    with open(path, "w") as fh:
        yaml.dump(
            entries,
            fh,
            default_flow_style=False,
            sort_keys=False,
            allow_unicode=True,
        )
