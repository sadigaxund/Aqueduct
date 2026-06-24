"""Filesystem-path marker for pydantic schemas.

Annotate any pydantic ``str`` field that holds a filesystem path with
``Annotated[str, FsPath()]``. Schema-driven anchoring walkers consult
this marker to know which fields to anchor against a ``base_dir`` —
replacing the hand-maintained ``stores.*.path`` loop in ``config.py``
and the hardcoded ``("path", "data_dir", ...)`` tuple in ``parser.py``.

Future policy fields slot in without rewriting call sites:

* ``allow_uri`` — when ``True`` (default), values containing ``://``
  (``s3://``, ``gs://``, ``postgresql://``, ``redis://``, ``file://``)
  pass through untouched; the anchoring rule applies only to plain
  local paths. Disable when a field MUST be a local path.

Co-located here, not in ``schema.py`` or ``models.py``, so the import
graph stays: ``parser/schema.py`` and ``aqueduct/config.py`` both
depend on this small marker module without pulling each other in.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated, get_args, get_origin


@dataclass(frozen=True)
class FsPath:
    """Marker annotation for filesystem-path fields.

    Use with ``Annotated[str, FsPath()]``. Empty body today; policy
    fields can be added later without churning call sites because the
    callable form is already stable.
    """
    allow_uri: bool = True


def field_is_fs_path(metadata: tuple, annotation: type | None = None) -> FsPath | None:
    """Return the ``FsPath`` marker from a pydantic field, or None.

    Checks both ``FieldInfo.metadata`` and the field's type annotation.
    Handles ``Optional[Annotated[str, FsPath()]]`` where pydantic strips
    metadata from the union.
    """
    from typing import Union, get_args as _ga, get_origin as _go

    # Check metadata first (direct Annotated[...])
    for m in metadata:
        if isinstance(m, FsPath):
            return m
        if m is FsPath:
            return FsPath()

    # Check annotation — handles `Annotated[str, FsPath()] | None` et al.
    if annotation is not None:
        origin = _go(annotation)
        if origin is Union:
            for arg in _ga(annotation):
                sub_origin = _go(arg)
                if sub_origin is not None:
                    # It's Annotated[str, FsPath()] — FsPath is in __metadata__
                    sub_meta = getattr(arg, "__metadata__", ())
                    for sm in sub_meta:
                        if isinstance(sm, FsPath):
                            return sm
                        if sm is FsPath:
                            return FsPath()
        elif hasattr(annotation, "__metadata__"):
            for sm in annotation.__metadata__:
                if isinstance(sm, FsPath):
                    return sm
                if sm is FsPath:
                    return FsPath()

    return None


__all__ = ["FsPath", "field_is_fs_path", "Annotated", "get_args", "get_origin"]
