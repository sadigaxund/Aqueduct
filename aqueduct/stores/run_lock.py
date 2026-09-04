"""Per-blueprint advisory lock, so two concurrent runs cannot interleave writes.

Two `aqueduct run` invocations on the same Blueprint share one observability
store and one depot (the default routing puts both under
`<base>/<blueprint_id>/`). DuckDB serialises them at the *statement* level, so
neither crashes, but they still interleave logically: two run_records rows
growing at once, two heal loops writing the same depot keys, two Egress
appends against the same target. Nothing above the store layer notices.

This module adds the missing mutual exclusion, one lock per `blueprint_id`:

* **File backend** (DuckDB observability, the default): an exclusive
  `fcntl.flock` on `<store_dir>/run.lock`. The holder writes its pid into the
  file so a refusal can name it.
* **Postgres backend**: `pg_try_advisory_lock` / `pg_advisory_lock` on a
  stable 64-bit key derived from `blueprint_id`, held on its own connection
  for the life of the run. A session-level advisory lock is released when that
  connection closes, so a crashed run frees it without cleanup.

Both are ADVISORY: they coordinate `aqueduct run` with itself. They do not
stop an unrelated process from writing to the same files.
"""

from __future__ import annotations

import contextlib
import hashlib
import os
from collections.abc import Iterator
from pathlib import Path
from typing import Any

from aqueduct.errors import ConfigError

__all__ = ["RunLockedError", "blueprint_run_lock", "advisory_lock_key"]

LOCK_FILENAME = "run.lock"


class RunLockedError(ConfigError):
    """Another `aqueduct run` already holds this Blueprint's lock.

    A `ConfigError` subclass so it classifies as `exit_codes.CONFIG_ERROR`
    like every other "your invocation cannot proceed as asked" failure. It is
    not a data or runtime fault: nothing was executed.
    """


def advisory_lock_key(blueprint_id: str) -> int:
    """Stable signed 64-bit key for `pg_advisory_lock`.

    Postgres advisory locks take a `bigint`, so the digest is folded into the
    signed 64-bit range. Derived from the blueprint_id alone, so every process
    on every host that runs this Blueprint computes the same key.
    """
    digest = hashlib.sha1(blueprint_id.encode("utf-8")).digest()[:8]
    unsigned = int.from_bytes(digest, "big", signed=False)
    return unsigned - (1 << 64) if unsigned >= (1 << 63) else unsigned


def _holder_pid(lock_path: Path) -> str:
    """Best-effort read of the pid the current holder wrote, for the message."""
    try:
        text = lock_path.read_text(encoding="utf-8").strip()
    except OSError:
        return "unknown"
    return text.split()[0] if text else "unknown"


@contextlib.contextmanager
def _file_lock(store_dir: Path, blueprint_id: str, *, wait: bool) -> Iterator[Path]:
    import fcntl

    store_dir.mkdir(parents=True, exist_ok=True)
    lock_path = store_dir / LOCK_FILENAME
    fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o644)
    try:
        flags = fcntl.LOCK_EX if wait else fcntl.LOCK_EX | fcntl.LOCK_NB
        try:
            fcntl.flock(fd, flags)
        except OSError as exc:
            raise RunLockedError(
                f"another aqueduct run is already running blueprint {blueprint_id!r} "
                f"(holder pid {_holder_pid(lock_path)}, lock {lock_path}). "
                "Wait for it to finish, or pass --wait-for-lock to queue behind it."
            ) from exc
        # Stamp the pid only after the lock is ours, so a refused run never
        # overwrites the real holder's pid.
        os.ftruncate(fd, 0)
        os.write(fd, f"{os.getpid()}\n".encode())
        os.fsync(fd)
        try:
            yield lock_path
        finally:
            with contextlib.suppress(OSError):
                fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        # Closing the fd releases the flock too, so this covers every exit
        # path including an exception raised before the explicit unlock.
        with contextlib.suppress(OSError):
            os.close(fd)


@contextlib.contextmanager
def _pg_lock(obs_store: Any, blueprint_id: str, *, wait: bool) -> Iterator[str]:
    key = advisory_lock_key(blueprint_id)
    with obs_store.connect() as cur:
        if wait:
            cur.execute("SELECT pg_advisory_lock(?)", (key,))
            acquired = True
        else:
            cur.execute("SELECT pg_try_advisory_lock(?)", (key,))
            row = cur.fetchone()
            acquired = bool(row and row[0])
        if not acquired:
            raise RunLockedError(
                f"another aqueduct run is already running blueprint {blueprint_id!r} "
                f"(postgres advisory lock {key}). Wait for it to finish, or pass "
                "--wait-for-lock to queue behind it."
            )
        try:
            yield f"pg_advisory_lock({key})"
        finally:
            # Belt and braces: the session lock would also drop when this
            # connection closes on context exit, but an explicit unlock keeps
            # a pooled or reused connection from carrying the lock onward.
            with contextlib.suppress(Exception):
                cur.execute("SELECT pg_advisory_unlock(?)", (key,))


@contextlib.contextmanager
def blueprint_run_lock(
    store_dir: Path | str | None,
    blueprint_id: str,
    *,
    obs_store: Any = None,
    wait: bool = False,
    enabled: bool = True,
) -> Iterator[str | None]:
    """Hold this Blueprint's run lock for the body, release it on every exit.

    `store_dir` is the Blueprint's own store directory
    (`<base>/<blueprint_id>/`); the lock file lives directly inside it. When
    `obs_store` is a Postgres store the file is not used at all and a
    `pg_advisory_lock` is taken on its connection instead, which is what a
    multi-host deployment needs (a flock only coordinates one filesystem).

    Yields a label naming the lock that was taken, or `None` when locking is
    disabled or unavailable on this platform.

    Raises `RunLockedError` when another run holds the lock and `wait` is
    False.
    """
    if not enabled or not blueprint_id:
        yield None
        return

    if obs_store is not None and getattr(obs_store, "backend", None) == "postgres":
        with _pg_lock(obs_store, blueprint_id, wait=wait) as label:
            yield label
        return

    if store_dir is None:
        yield None
        return

    try:
        import fcntl  # noqa: F401
    except ImportError:
        # Windows has no fcntl. Announce the degrade rather than pretending
        # the run is protected: concurrent runs there are unguarded.
        from aqueduct.warnings import emit as _emit

        _emit(
            "run_lock_unavailable",
            "this platform has no fcntl, so the per-blueprint run lock is not "
            "taken. Two concurrent runs of this Blueprint can interleave their "
            "observability and depot writes.",
        )
        yield None
        return

    with _file_lock(Path(store_dir), blueprint_id, wait=wait) as lock_path:
        yield str(lock_path)
