"""DuckDB Ingress/Egress against a real ``s3://`` root (MinIO) — closes the
untested S3 claim (Q4/httpfs work): the engine implements httpfs + S3
secrets wiring, but had never been round-tripped against a guaranteed S3
endpoint. Runs against the repo owner's local docker-compose MinIO
(``tmp/04-dashboard-showcase/docker-compose.yml``): ``http://localhost:9000``,
``minioadmin``/``minioadmin``.

Every test here is real I/O, no mocks — skips cleanly (``requires_minio``)
when MinIO is unreachable, since CI has no MinIO service.

MinIO-specific config knobs proven here (the non-AWS-S3-compatible escape
hatch — ``engine.duckdb.s3_endpoint``/``s3_url_style``/``s3_use_ssl``,
added by this same change): AWS's default virtual-hosted addressing and
TLS assumptions don't hold for MinIO, so a plain ``s3_key_id_secret``/
``s3_secret_access_key_secret`` pair (proven against MinIO in
``tests/test_executor_duckdb/test_engine_config.py``'s pre-existing tests)
is not enough on its own.
"""

from __future__ import annotations

import uuid

import pytest

from aqueduct.executor.duckdb_.engine import _make_session
from aqueduct.executor.protocol import SessionSpec
from tests.conftest import (
    _minio_access_key,
    _minio_endpoint,
    _minio_secret_key,
    ensure_minio_bucket,
    requires_minio,
)

pytestmark = [pytest.mark.duckdb, requires_minio]

_BUCKET = "aqueduct-test"


@pytest.fixture(autouse=True, scope="module")
def _bucket():
    ensure_minio_bucket(_BUCKET)


def _minio_session(monkeypatch, secret_env: tuple[str, str] = ("AQ_MINIO_KEY", "AQ_MINIO_SECRET")):
    """Build a real DuckDB session wired the SAME way ``aqueduct run`` wires
    one — secret KEY NAMES in ``engine_config``, resolved through the
    ``secrets:`` block resolver via ``engine_options``, never a literal
    credential in the config bag."""
    key_env, secret_env_name = secret_env
    monkeypatch.setenv(key_env, _minio_access_key())
    monkeypatch.setenv(secret_env_name, _minio_secret_key())
    endpoint = _minio_endpoint().split("://", 1)[-1]  # DuckDB ENDPOINT wants host:port, no scheme
    spec = SessionSpec(
        blueprint_id="test-s3-minio",
        engine_config={
            "s3_key_id_secret": key_env,
            "s3_secret_access_key_secret": secret_env_name,
            "s3_endpoint": endpoint,
            "s3_url_style": "path",
            "s3_use_ssl": _minio_endpoint().startswith("https://"),
        },
        engine_options={"secrets": {"provider": "env"}},
    )
    return _make_session(spec)


class TestDuckDBS3RoundTrip:
    def test_duckdb_ingress_egress_round_trip_against_minio(self, monkeypatch):
        """Write via a DuckDB Egress-shaped COPY, read back via an
        Ingress-shaped read_parquet, from a session built EXACTLY the way
        `_make_session` builds a real run's session — proves the httpfs +
        `CREATE SECRET` + MinIO endpoint-override wiring end to end."""
        con = _minio_session(monkeypatch)
        key = f"test1/{uuid.uuid4().hex[:8]}/out.parquet"
        uri = f"s3://{_BUCKET}/{key}"
        try:
            con.execute("CREATE TABLE t AS SELECT 1 AS id, 'hello-minio' AS msg")
            con.execute(f"COPY t TO '{uri}' (FORMAT PARQUET)")
            rows = con.execute(f"SELECT id, msg FROM read_parquet('{uri}')").fetchall()
            assert rows == [(1, "hello-minio")]
        finally:
            con.close()

    def test_duckdb_s3_write_with_wrong_endpoint_fails_fast(self, monkeypatch):
        """Negative control — proves `s3_endpoint` is load-bearing, not a
        no-op: the SAME credentials/bucket, pointed at a deliberately wrong
        endpoint (a closed local port, not real AWS — keeps this fast and
        network-independent) must fail, never silently reach the real
        MinIO bucket some OTHER config field might."""
        monkeypatch.setenv("AQ_MINIO_KEY3", _minio_access_key())
        monkeypatch.setenv("AQ_MINIO_SECRET3", _minio_secret_key())
        spec = SessionSpec(
            blueprint_id="test-s3-minio-wrong-endpoint",
            engine_config={
                "s3_key_id_secret": "AQ_MINIO_KEY3",
                "s3_secret_access_key_secret": "AQ_MINIO_SECRET3",
                "s3_endpoint": "127.0.0.1:1",
                "s3_url_style": "path",
                "s3_use_ssl": False,
            },
            engine_options={"secrets": {"provider": "env"}},
        )
        con = _make_session(spec)
        try:
            with pytest.raises(Exception):
                con.execute(
                    f"COPY (SELECT 1 AS x) TO 's3://{_BUCKET}/should-not-reach-minio.parquet' "
                    "(FORMAT PARQUET)"
                )
        finally:
            con.close()
