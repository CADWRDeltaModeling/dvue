"""CatalogBuilder implementations for dvue.

This module provides two concrete :class:`~dvue.catalog.CatalogBuilder`
implementations that cover the most common CSV file source patterns:

``CSVDirectoryBuilder``
    Scans every ``*.csv`` file from a directory (or a single CSV file).
    Each file becomes a :class:`~dvue.catalog.DataReference` backed by a
    :class:`~dvue.catalog.FileDataReferenceReader` and named after the file
    stem.

``PatternCSVDirectoryBuilder``
    Scans CSV files from a directory and extracts structured metadata
    (e.g. variable, station ID, agency) from each filename via ``{field}``
    placeholder patterns.

Backward-compatible aliases ``CSVDirectoryReader`` and
``PatternCSVDirectoryReader`` are provided at the bottom of the module.

Writing a custom CatalogBuilder
-------------------------------
Subclass :class:`~dvue.catalog.CatalogBuilder` and implement the two
abstract methods, then register the builder with the catalog:

.. code-block:: python

    from dvue.catalog import (
        DataCatalog, CatalogBuilder, DataReferenceReader, DataReference
    )

    class MyDBDataReferenceReader(DataReferenceReader):
        '''Load a table from a database connection.'''
        def __init__(self, connection):
            self._conn = connection

        def load(self, **attributes):
            return self._conn.query_table(attributes["table"])

    class MyDatabaseBuilder(CatalogBuilder):
        def can_handle(self, source):
            return isinstance(source, MyDBConnection)

        def build(self, source):
            reader = MyDBDataReferenceReader(source)  # one shared reader
            return [
                DataReference(reader, name=table, table=table)
                for table in source.list_tables()
            ]
            # No data loaded yet — getData() triggers the first query.

    # Register globally (affects all new DataCatalog instances)
    DataCatalog.register_builder(MyDatabaseBuilder())

    # Or register on a single catalog instance only
    catalog = DataCatalog().add_builder(MyDatabaseBuilder())
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any, Dict, List

from .catalog import CatalogBuilder, DataReference, FileDataReferenceReader

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


class CSVDirectoryBuilder(CatalogBuilder):
    """Scan CSV files from a directory (or a single CSV file).

    Each CSV file becomes a :class:`~dvue.catalog.DataReference` backed by a
    shared :class:`~dvue.catalog.FileDataReferenceReader` and named after the
    file stem (filename without extension).  Additional keyword arguments
    supplied at construction time are stored as metadata on every created
    reference.

    No CSV data is loaded until :meth:`~dvue.catalog.DataReference.getData`
    is called on an individual reference.

    Parameters
    ----------
    **default_attributes
        Metadata applied to *every* constructed :class:`~dvue.catalog.DataReference`.

    Examples
    --------
    >>> builder = CSVDirectoryBuilder(project="climate", source_type="csv")
    >>> catalog = DataCatalog().add_builder(builder).add_source("/data/climate/")
    """

    def __init__(self, **default_attributes: Any) -> None:
        self._default_attributes = default_attributes
        self._file_reader = FileDataReferenceReader()

    def can_handle(self, source: Any) -> bool:
        """Return ``True`` for an existing directory or ``.csv`` file path."""
        if not isinstance(source, (str, Path)):
            return False
        p = Path(source)
        return p.is_dir() or p.suffix.lower() == ".csv"

    def build(self, source: Any) -> List[DataReference]:
        """Scan *source* and return one :class:`~dvue.catalog.DataReference` per CSV file.

        Parameters
        ----------
        source : str or Path
            Directory (all ``*.csv`` files are scanned) or a single CSV file.

        Returns
        -------
        List[DataReference]
        """
        p = Path(source)
        files = sorted(p.glob("*.csv")) if p.is_dir() else [p]
        refs = []
        for f in files:
            ref = DataReference(
                self._file_reader,
                name=f.stem,
                file_path=str(f),
                format="csv",
                **self._default_attributes,
            )
            refs.append(ref)
        logger.debug("CSVDirectoryBuilder: constructed %d references from %s", len(refs), source)
        return refs


# ---------------------------------------------------------------------------
# PatternCSVDirectoryReader
# ---------------------------------------------------------------------------


class PatternCSVDirectoryBuilder(CatalogBuilder):
    """Scan CSV files from a directory and extract metadata from each filename.

    Filenames are matched against a *pattern* that uses ``{field}``
    placeholders.  Each matched field is stored as a metadata attribute on
    the constructed :class:`~dvue.catalog.DataReference`.  The special field
    ``{name}`` becomes the reference name; if the pattern contains no
    ``{name}`` placeholder the full file stem is used instead.  Files whose
    names do not match the pattern are skipped with a ``WARNING`` log message.

    No CSV data is loaded until :meth:`~dvue.catalog.DataReference.getData`
    is called on an individual reference.

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
    >>> builder = PatternCSVDirectoryBuilder("{name}__{stationid}__{source}")
    >>> catalog = DataCatalog().add_builder(builder).add_source("/data/hydro/")
    >>> catalog.search(stationid="STA001")
    >>> catalog.search(source="USGS")
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
        self._file_reader = FileDataReferenceReader()

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

    def build(self, source: Any) -> List[DataReference]:
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
                    "PatternCSVDirectoryBuilder: %r does not match pattern %r – skipping.",
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

            # 'name' and 'cache' are DataReference constructor params; if a
            # filename field collides with them, store them as attributes instead.
            _CONSTRUCTOR_PARAMS = {"name", "cache"}
            colliding = {k: attrs.pop(k) for k in list(attrs) if k in _CONSTRUCTOR_PARAMS}

            ref = DataReference(self._file_reader, name=ref_name, **attrs)
            for k, v in colliding.items():
                ref.set_attribute(k, v)
            refs.append(ref)

        logger.info(
            "PatternCSVDirectoryBuilder: %d reference(s) constructed, %d file(s) skipped in %s",
            len(refs),
            skipped,
            source,
        )
        return refs

    def __repr__(self) -> str:
        return f"PatternCSVDirectoryBuilder(pattern={self._pattern!r})"


# ---------------------------------------------------------------------------
# Backward-compatible aliases
# ---------------------------------------------------------------------------

#: Alias for :class:`CSVDirectoryBuilder` (backward compatibility).
CSVDirectoryReader = CSVDirectoryBuilder

#: Alias for :class:`PatternCSVDirectoryBuilder` (backward compatibility).
PatternCSVDirectoryReader = PatternCSVDirectoryBuilder
