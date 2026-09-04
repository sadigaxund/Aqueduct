"""Per-blueprint advisory run lock: refusal, release, and the Postgres path.

Two `aqueduct run` invocations on one Blueprint share an observability store
and a depot. DuckDB serialises their statements so neither crashes, but they
still interleave logically. `aqueduct/stores/run_lock.py` adds the missing
mutual exclusion; these tests pin that it refuses a second holder, that it
releases on every exit path including an exception, and that a Postgres
observability store takes a `pg_advisory_lock` instead of a file lock.
"""

from __future__ import annotations

import os
import threading
import time

import pytest

from aqueduct.errors import ConfigError
from aqueduct.stores.run_lock import (
    LOCK_FILENAME,
    RunLockedError,
    advisory_lock_key,
    blueprint_run_lock,
)

pytestmark = pytest.mark.unit


# ── file backend ─────────────────────────────────────────────────────────────


def test_lock_file_lands_in_the_blueprint_store_dir(tmp_path):
    with blueprint_run_lock(tmp_path, "bp1") as label:
        assert label == str(tmp_path / LOCK_FILENAME)
        assert (tmp_path / LOCK_FILENAME).exists()


def test_holder_writes_its_pid(tmp_path):
    with blueprint_run_lock(tmp_path, "bp1"):
        assert (tmp_path / LOCK_FILENAME).read_text().strip() == str(os.getpid())


def test_second_acquisition_is_refused(tmp_path):
    with blueprint_run_lock(tmp_path, "bp1"):
        with pytest.raises(RunLockedError) as exc:
            with blueprint_run_lock(tmp_path, "bp1"):
                pytest.fail("the second run took a lock the first one holds")
    message = str(exc.value)
    assert "bp1" in message
    assert str(os.getpid()) in message, "refusal must name the holder pid"
    assert LOCK_FILENAME in message, "refusal must name the lock path"
    assert "--wait-for-lock" in message


def test_refusal_is_a_config_error_subclass():
    """Nothing executed, so it classifies as CONFIG_ERROR, not a runtime fault."""
    assert issubclass(RunLockedError, ConfigError)


def test_lock_is_released_on_normal_exit(tmp_path):
    with blueprint_run_lock(tmp_path, "bp1"):
        pass
    with blueprint_run_lock(tmp_path, "bp1") as label:
        assert label is not None


def test_lock_is_released_when_the_body_raises(tmp_path):
    class Boom(RuntimeError):
        pass

    with pytest.raises(Boom):
        with blueprint_run_lock(tmp_path, "bp1"):
            raise Boom
    with blueprint_run_lock(tmp_path, "bp1") as label:
        assert label is not None, "an exception must not strand the lock"


def test_lock_is_released_on_sys_exit(tmp_path):
    """`sys.exit` raises SystemExit, a BaseException; the release must honour it."""
    with pytest.raises(SystemExit):
        with blueprint_run_lock(tmp_path, "bp1"):
            raise SystemExit(1)
    with blueprint_run_lock(tmp_path, "bp1") as label:
        assert label is not None


def test_a_different_blueprint_is_not_blocked(tmp_path):
    """The lock is per blueprint_id, so unrelated pipelines run concurrently."""
    a = tmp_path / "bp1"
    b = tmp_path / "bp2"
    with blueprint_run_lock(a, "bp1"), blueprint_run_lock(b, "bp2") as label:
        assert label is not None


def test_wait_for_lock_queues_instead_of_refusing(tmp_path):
    acquired = threading.Event()

    def second() -> None:
        with blueprint_run_lock(tmp_path, "bp1", wait=True):
            acquired.set()

    t = threading.Thread(target=second, daemon=True)
    with blueprint_run_lock(tmp_path, "bp1"):
        t.start()
        time.sleep(0.2)
        assert not acquired.is_set(), "wait=True must block while the lock is held"
    t.join(timeout=10)
    assert acquired.is_set(), "wait=True must acquire once the holder releases"


def test_disabled_yields_none_and_writes_no_file(tmp_path):
    with blueprint_run_lock(tmp_path, "bp1", enabled=False) as label:
        assert label is None
    assert not (tmp_path / LOCK_FILENAME).exists()


def test_no_blueprint_id_is_a_no_op(tmp_path):
    with blueprint_run_lock(tmp_path, "") as label:
        assert label is None


# ── postgres backend ─────────────────────────────────────────────────────────


class _FakeCursor:
    def __init__(self, granted: bool) -> None:
        self.granted = granted
        self.statements: list[tuple[str, tuple]] = []

    def execute(self, sql, params=()):
        self.statements.append((sql, params))

    def fetchone(self):
        return (self.granted,)


class _FakePostgresStore:
    backend = "postgres"

    def __init__(self, granted: bool = True) -> None:
        self.cursor = _FakeCursor(granted)

    def connect(self):
        import contextlib

        @contextlib.contextmanager
        def _cm():
            yield self.cursor

        return _cm()


def test_postgres_store_takes_an_advisory_lock_not_a_file(tmp_path):
    store = _FakePostgresStore(granted=True)
    with blueprint_run_lock(tmp_path, "bp1", obs_store=store) as label:
        assert label == f"pg_advisory_lock({advisory_lock_key('bp1')})"
    assert not (tmp_path / LOCK_FILENAME).exists(), "postgres path must not write a lock file"
    sql = [s for s, _ in store.cursor.statements]
    assert any("pg_try_advisory_lock" in s for s in sql)
    assert any("pg_advisory_unlock" in s for s in sql), "the lock must be released on exit"


def test_postgres_refusal_when_the_lock_is_held(tmp_path):
    store = _FakePostgresStore(granted=False)
    with pytest.raises(RunLockedError) as exc:
        with blueprint_run_lock(tmp_path, "bp1", obs_store=store):
            pytest.fail("pg_try_advisory_lock returned false but the body ran")
    assert str(advisory_lock_key("bp1")) in str(exc.value)


def test_postgres_wait_uses_the_blocking_lock_function(tmp_path):
    store = _FakePostgresStore(granted=False)  # granted is ignored when waiting
    with blueprint_run_lock(tmp_path, "bp1", obs_store=store, wait=True):
        pass
    sql = [s for s, _ in store.cursor.statements]
    assert any(s.strip().startswith("SELECT pg_advisory_lock(") for s in sql)
    assert not any("pg_try_advisory_lock" in s for s in sql)


def test_postgres_lock_released_when_the_body_raises(tmp_path):
    store = _FakePostgresStore(granted=True)
    with pytest.raises(RuntimeError):
        with blueprint_run_lock(tmp_path, "bp1", obs_store=store):
            raise RuntimeError("boom")
    assert any("pg_advisory_unlock" in s for s, _ in store.cursor.statements)


# ── key derivation ───────────────────────────────────────────────────────────


def test_advisory_key_is_stable_and_fits_a_signed_bigint():
    key = advisory_lock_key("orders_daily")
    assert key == advisory_lock_key("orders_daily"), "same blueprint, same key on every host"
    assert -(2**63) <= key < 2**63
    assert key != advisory_lock_key("orders_hourly")
