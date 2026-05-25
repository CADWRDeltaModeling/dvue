"""Reader registry for dvue.

Provides :class:`ReaderRegistry` — a central mapping from ``ref_type`` strings
to :class:`~dvue.catalog.DataReferenceReader` classes, with per-source instance
caching and file-extension dispatch.

Downstream packages register their reader classes at module import time::

    from dvue.registry import ReaderRegistry
    ReaderRegistry.register("dsm2_hdf5", TidefileReader, extensions=[".h5", ".hdf5"])
    ReaderRegistry.register("dsm2_dss",  DSM2DSSReader,  extensions=[".dss"])

:meth:`~ReaderRegistry.get_reader` returns a cached instance per
``(ref_type, source)`` pair, replacing ad-hoc flyweight patterns.

:meth:`~ReaderRegistry.scan` opens a file by extension and delegates to the
reader class's ``scan(path)`` classmethod to produce a list of
:class:`~dvue.catalog.DataReference` objects.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING, ClassVar, Dict, List, Optional, Tuple, Type

if TYPE_CHECKING:  # pragma: no cover
    from dvue.catalog import DataReference, DataReferenceReader


class ReaderRegistry:
    """Central registry mapping ``ref_type`` strings to reader classes and instances.

    All state is class-level (effectively a module-level singleton).  Downstream
    packages register their reader classes at module import time via
    :meth:`register`.

    Thread safety
    -------------
    Registration is expected to happen at module import time (single-threaded).
    Instance creation in :meth:`get_reader` is not guarded by a lock; in normal
    Panel/asyncio use the IOLoop is single-threaded so this is safe.  If true
    concurrent access is needed, callers should synchronise externally.
    """

    # ref_type → reader class
    _registry: ClassVar[Dict[str, Type["DataReferenceReader"]]] = {}

    # lower-cased ".ext" → reader class
    _extension_map: ClassVar[Dict[str, Type["DataReferenceReader"]]] = {}

    # (ref_type, source) → live reader instance
    _instances: ClassVar[Dict[Tuple[str, str], "DataReferenceReader"]] = {}

    # ---------------------------------------------------------------------------
    # Registration
    # ---------------------------------------------------------------------------

    @classmethod
    def register(
        cls,
        ref_type: str,
        reader_class: Type["DataReferenceReader"],
        extensions: Optional[List[str]] = None,
    ) -> None:
        """Register *reader_class* for *ref_type* and optionally for file extensions.

        Calling :meth:`register` with the same *ref_type* more than once
        overwrites the previous mapping (last-write-wins).  Extension entries
        are additive across calls — they are never removed by a subsequent
        :meth:`register` call.

        Parameters
        ----------
        ref_type:
            String key used in :attr:`~dvue.catalog.DataReference.ref_type`.
        reader_class:
            A :class:`~dvue.catalog.DataReferenceReader` subclass.  Must
            implement ``__init__(self, source: str)`` and ``load(**attrs)``.
            If it also implements ``@classmethod scan(cls, path)`` it will be
            used by :meth:`scan`.
        extensions:
            Optional list of lower-case file extensions (including the leading
            dot, e.g. ``[".h5", ".hdf5"]``) to associate with *reader_class*.
        """
        cls._registry[ref_type] = reader_class
        if extensions:
            for ext in extensions:
                cls._extension_map[ext.lower()] = reader_class

    # ---------------------------------------------------------------------------
    # Reader instance access
    # ---------------------------------------------------------------------------

    @classmethod
    def get_reader(cls, ref_type: str, source: str = "") -> "DataReferenceReader":
        """Return a cached reader instance for *(ref_type, source)*.

        Creates ``reader_class(source)`` on the first call for a given
        ``(ref_type, source)`` pair.  Subsequent calls return the same instance,
        keeping file handles open between data requests.

        Parameters
        ----------
        ref_type:
            The type key to look up (e.g. ``"dsm2_hdf5"``).
        source:
            The absolute path to the source file.

        Raises
        ------
        KeyError
            If *ref_type* has not been registered.
        """
        if ref_type not in cls._registry:
            raise KeyError(
                f"No reader registered for ref_type={ref_type!r}. "
                "Call ReaderRegistry.register() before using this type."
            )
        key = (ref_type, source)
        if key not in cls._instances:
            cls._instances[key] = cls._registry[ref_type](source)
        return cls._instances[key]

    # ---------------------------------------------------------------------------
    # File scanning
    # ---------------------------------------------------------------------------

    @classmethod
    def scan(cls, path: str) -> List["DataReference"]:
        """Scan *path* by extension and return the :class:`~dvue.catalog.DataReference`\\ s it contains.

        Looks up the reader class via ``_extension_map``, then calls
        ``reader_class.scan(path)`` (a classmethod on the reader).

        Parameters
        ----------
        path:
            Absolute path to the file to scan.

        Raises
        ------
        KeyError
            If the file's extension is not registered.
        NotImplementedError
            If the matched reader class has not implemented ``scan()``.
        """
        ext = os.path.splitext(path)[1].lower()
        if ext not in cls._extension_map:
            raise KeyError(
                f"No reader registered for extension {ext!r}. "
                "Call ReaderRegistry.register(..., extensions=[...]) to add support."
            )
        reader_class = cls._extension_map[ext]
        return reader_class.scan(path)

    @classmethod
    def can_handle(cls, path: str) -> bool:
        """Return ``True`` if a reader is registered for this file's extension."""
        ext = os.path.splitext(path)[1].lower()
        return ext in cls._extension_map

    # ---------------------------------------------------------------------------
    # Cache management
    # ---------------------------------------------------------------------------

    @classmethod
    def clear_instance_cache(
        cls,
        ref_type: Optional[str] = None,
        source: Optional[str] = None,
    ) -> None:
        """Remove cached reader instances matching the given filters.

        Called with no arguments clears all instances (useful in tests).

        Parameters
        ----------
        ref_type:
            When given, only entries with this ref_type are cleared.
        source:
            When given, only entries with this source path are cleared.
        """
        if ref_type is None and source is None:
            cls._instances.clear()
            return
        to_remove = [
            key for key in cls._instances
            if (ref_type is None or key[0] == ref_type)
            and (source is None or key[1] == source)
        ]
        for key in to_remove:
            del cls._instances[key]
