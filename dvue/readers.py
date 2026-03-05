"""Sample DataCatalogReader implementations for dvue.

This module provides concrete, ready-to-use :class:`~dvue.catalog.DataCatalogReader`
implementations as starting points for building custom readers.

Included readers
----------------
CSVDirectoryReader
    Reads every ``*.csv`` file in a directory (or a single CSV file).
    Each file becomes a :class:`~dvue.catalog.DataReference` named after
    the file stem.

PatternCSVDirectoryReader
    Reads CSV files from a directory and extracts structured metadata
    (e.g. station ID, source agency) from each filename using a
    ``{field}`` placeholder pattern.

Writing a custom reader
-----------------------
Subclass :class:`~dvue.catalog.DataCatalogReader` and implement the two
abstract methods, then register the reader with the catalog:

.. code-block:: python

    from dvue.catalog import DataCatalog, DataCatalogReader, DataReference

    class MyDatabaseReader(DataCatalogReader):
        def can_handle(self, source):
            # Return True for the source types this reader accepts
            return isinstance(source, MyDBConnection)

        def read(self, source):
            refs = []
            for table in source.list_tables():
                ref = DataReference(
                    lambda t=table: source.query_table(t),
                    name=table,
                    database=source.name,
                )
                refs.append(ref)
            return refs

    # Register globally (affects all new DataCatalog instances)
    DataCatalog.register_reader(MyDatabaseReader())

    # Or register on a single catalog instance
    catalog = DataCatalog().add_reader(MyDatabaseReader())
    catalog.add_source(my_db_connection)

See :class:`~dvue.catalog.DataCatalogReader` for the full API contract.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any, Dict, List

from .catalog import DataCatalogReader, DataReference

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# _pattern_to_regex helper
# ---------------------------------------------------------------------------


def _pattern_to_regex(pattern: str) -> re.Pattern:
    """Compile a ``{field}`` placeholder pattern into a named-group regex.

    Each ``{field}`` becomes a named capture group.  The literal text between
    consecutive placeholders is used as a negative-lookahead boundary so that
    multi-character separators (e.g. ``__``) work even when field values
    themselves contain the separator's individual characters.

    Examples
    --------
    >>> rx = _pattern_to_regex("{name}__{stationid}__{source}")
    >>> rx.match("flow_rate__STA001__USGS").groupdict()
    {'name': 'flow_rate', 'stationid': 'STA001', 'source': 'USGS'}
    """
    tokens = re.split(r"(\{[^}]+\})", pattern)
    # tokens alternates: [literal, {field}, literal, {field}, ..., literal]
    parts: List[str] = []
    n = len(tokens)
    for i, tok in enumerate(tokens):
        if tok.startswith("{") and tok.endswith("}"):
            field = tok[1:-1]
            next_literal = tokens[i + 1] if i + 1 < n else ""
            prev_literal = tokens[i - 1] if i > 0 else ""
            if next_literal:
                # Non-greedy match that stops before the next separator.
                # Use a negative lookahead on the full separator string so that
                # field values containing any single separator character still work.
                neg = re.escape(next_literal)
                parts.append(f"(?P<{field}>(?:(?!{neg}).)+)")
            elif prev_literal:
                # Last field: also disallow the preceding separator so that a
                # filename with more segments than placeholders does not match.
                neg = re.escape(prev_literal)
                parts.append(f"(?P<{field}>(?:(?!{neg}).)+)")
            else:
                # Single-field pattern with no surrounding separators.
                parts.append(f"(?P<{field}>.+)")
        else:
            parts.append(re.escape(tok))
    return re.compile("^" + "".join(parts) + "$")


# ---------------------------------------------------------------------------
# CSVDirectoryReader
# ---------------------------------------------------------------------------


class CSVDirectoryReader(DataCatalogReader):
    """Read CSV files from a directory (or a single CSV file).

    Each CSV file becomes a :class:`~dvue.catalog.DataReference` named after
    the file stem (filename without extension).  Additional keyword arguments
    supplied at construction time are stored as metadata on every created
    reference.

    Parameters
    ----------
    **default_attributes
        Metadata applied to *every* constructed :class:`~dvue.catalog.DataReference`.

    Examples
    --------
    >>> reader = CSVDirectoryReader(project="climate", source_type="csv")
    >>> catalog = DataCatalog().add_reader(reader).add_source("/data/climate/")

    Customising the reader
    ----------------------
    To add post-processing (e.g. date-parsing, unit conversion) override
    :meth:`read` in a subclass:

    .. code-block:: python

        class ParsedCSVReader(CSVDirectoryReader):
            def read(self, source):
                refs = super().read(source)
                for ref in refs:
                    # Wrap the original source with a parsing step
                    original_source = ref.source
                    ref.source = lambda p=original_source: (
                        pd.read_csv(p, parse_dates=["timestamp"])
                    )
                return refs
    """

    def __init__(self, **default_attributes: Any) -> None:
        self._default_attributes = default_attributes

    def can_handle(self, source: Any) -> bool:
        """Return ``True`` for an existing directory or ``.csv`` file path."""
        if not isinstance(source, (str, Path)):
            return False
        p = Path(source)
        return p.is_dir() or p.suffix.lower() == ".csv"

    def read(self, source: Any) -> List[DataReference]:
        """Read *source* and return one :class:`~dvue.catalog.DataReference` per CSV file.

        Parameters
        ----------
        source : str or Path
            Directory (all ``*.csv`` files are read) or a single CSV file.

        Returns
        -------
        List[DataReference]
        """
        p = Path(source)
        files = sorted(p.glob("*.csv")) if p.is_dir() else [p]
        refs = []
        for f in files:
            ref = DataReference(
                source=str(f),
                name=f.stem,
                file_path=str(f),
                format="csv",
                **self._default_attributes,
            )
            refs.append(ref)
        logger.debug("CSVDirectoryReader: constructed %d references from %s", len(refs), source)
        return refs


# ---------------------------------------------------------------------------
# PatternCSVDirectoryReader
# ---------------------------------------------------------------------------


class PatternCSVDirectoryReader(DataCatalogReader):
    """Read CSV files from a directory and extract metadata from each filename.

    Filenames are matched against a *pattern* that uses ``{field}``
    placeholders.  Each matched field is stored as a metadata attribute on
    the constructed :class:`~dvue.catalog.DataReference`.  The special field
    ``{name}`` becomes the reference name; if the pattern contains no
    ``{name}`` placeholder the full file stem is used instead.  Files whose
    names do not match the pattern are skipped with a ``WARNING`` log message.

    Parameters
    ----------
    pattern : str
        Filename pattern (without the ``.csv`` extension) using ``{field}``
        placeholders.  The separator between fields may be any literal string;
        multi-character separators (e.g. ``__``) are recommended when field
        values can themselves contain the separator's individual characters.

        Examples::

            "{name}__{stationid}__{source}"
            # flow_rate__STA001__USGS.csv
            #   → name="flow_rate", stationid="STA001", source="USGS"

            "{source}-{stationid}-{name}"
            # USGS-STA001-flow.csv
            #   → source="USGS", stationid="STA001", name="flow"

    glob : str, optional
        Glob pattern used to find files inside the directory.  Default
        ``"*.csv"``.
    **default_attributes
        Extra metadata applied to *every* constructed
        :class:`~dvue.catalog.DataReference`, regardless of what was parsed
        from the filename.

    Raises
    ------
    ValueError
        If *pattern* contains no ``{field}`` placeholders at all.

    Examples
    --------
    >>> reader = PatternCSVDirectoryReader("{name}__{stationid}__{source}")
    >>> catalog = DataCatalog().add_reader(reader).add_source("/data/hydro/")
    >>> catalog.search(stationid="STA001")
    >>> catalog.search(source="USGS")

    Subclassing template
    --------------------
    .. code-block:: python

        class MyAgencyReader(PatternCSVDirectoryReader):
            \"\"\"Reader for files named like AGENCY__SITEID__PARAMETER.csv.\"\"\"

            def __init__(self, **default_attrs):
                super().__init__("{source}__{stationid}__{name}", **default_attrs)

            def read(self, source):
                refs = super().read(source)
                # Apply any post-construction transformations here
                for ref in refs:
                    agency = ref.get_attribute("source")
                    ref.set_attribute("agency_url",
                                      f"https://example.com/{agency}")
                return refs
    """

    def __init__(
        self,
        pattern: str,
        glob: str = "*.csv",
        **default_attributes: Any,
    ) -> None:
        fields = re.findall(r"\{([^}]+)\}", pattern)
        if not fields:
            raise ValueError(
                f"Pattern {pattern!r} contains no {{field}} placeholders. "
                "Example: '{name}__{stationid}__{source}'"
            )
        self._pattern = pattern
        self._regex: re.Pattern = _pattern_to_regex(pattern)
        self._fields: List[str] = fields
        self._glob = glob
        self._default_attributes = default_attributes

    @property
    def pattern(self) -> str:
        """The filename pattern string."""
        return self._pattern

    @property
    def fields(self) -> List[str]:
        """Ordered list of field names extracted from the pattern."""
        return list(self._fields)

    def can_handle(self, source: Any) -> bool:
        """Return ``True`` for an existing directory path."""
        if not isinstance(source, (str, Path)):
            return False
        return Path(source).is_dir()

    def read(self, source: Any) -> List[DataReference]:
        """Scan *source* directory and return one :class:`~dvue.catalog.DataReference`
        per file whose name matches :attr:`pattern`.

        Parameters
        ----------
        source : str or Path
            Directory to scan.

        Returns
        -------
        List[DataReference]
            One entry per matched file, sorted by filename.
        """
        directory = Path(source)
        files = sorted(directory.glob(self._glob))
        refs: List[DataReference] = []
        skipped = 0

        for f in files:
            stem = f.stem
            m = self._regex.match(stem)
            if m is None:
                logger.warning(
                    "PatternCSVDirectoryReader: %r does not match pattern %r – skipping.",
                    f.name,
                    self._pattern,
                )
                skipped += 1
                continue

            parsed = m.groupdict()
            ref_name = parsed.pop("name", stem)

            attrs: Dict[str, Any] = {
                "file_path": str(f),
                "format": "csv",
            }
            attrs.update(self._default_attributes)
            attrs.update(parsed)  # fields from filename (stationid, source, …)

            # DataReference reserves 'source', 'name', and 'cache' as constructor
            # parameters.  If the filename pattern produces a field with the same
            # name (e.g. '{source}' in the pattern), passing it via **attrs would
            # raise TypeError.  Extract those keys and apply them afterwards.
            _CONSTRUCTOR_PARAMS = {"source", "name", "cache"}
            colliding = {k: attrs.pop(k) for k in list(attrs) if k in _CONSTRUCTOR_PARAMS}

            ref = DataReference(source=str(f), name=ref_name, **attrs)
            for k, v in colliding.items():
                ref.set_attribute(k, v)
            refs.append(ref)

        logger.info(
            "PatternCSVDirectoryReader: %d reference(s) constructed, %d file(s) skipped in %s",
            len(refs),
            skipped,
            source,
        )
        return refs

    def __repr__(self) -> str:
        return f"PatternCSVDirectoryReader(pattern={self._pattern!r})"
