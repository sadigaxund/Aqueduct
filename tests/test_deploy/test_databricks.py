"""Unit tests for aqueduct/deploy/ — Databricks submitter with mocked httpx."""

from __future__ import annotations

import base64
import json
from unittest import mock
from unittest.mock import MagicMock

import pytest

pytestmark = pytest.mark.unit


# ── Helpers ──────────────────────────────────────────────────────────────────


def _make_cfg(workspace_url="https://dbc-test.cloud.databricks.com", cluster_id="my-cluster"):
    from aqueduct.config import (
        AqueductConfig,
        DatabricksDeployConfig,
        DeploymentConfig,
        SecretsConfig,
    )

    return AqueductConfig(
        deployment=DeploymentConfig(
            target="databricks",
            databricks=DatabricksDeployConfig(
                workspace_url=workspace_url,
                cluster_id=cluster_id,
            ),
        ),
        secrets=SecretsConfig(provider="env"),
    )


def _mock_response(json_data=None, status_code=200):
    """Return a MagicMock that behaves like an httpx.Response."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = json_data or {}
    resp.raise_for_status.return_value = None
    resp.response.status_code = status_code
    resp.response.text = ""
    return resp


# ── Config validation ────────────────────────────────────────────────────────


class TestDatabricksDeployConfig:
    def test_valid_with_cluster_id(self):
        from aqueduct.config import DatabricksDeployConfig

        cfg = DatabricksDeployConfig(
            workspace_url="https://test.cloud.databricks.com", cluster_id="c1"
        )
        assert cfg.cluster_id == "c1"
        assert cfg.new_cluster is None

    def test_valid_with_new_cluster(self):
        from aqueduct.config import DatabricksDeployConfig

        cfg = DatabricksDeployConfig(
            workspace_url="https://test.cloud.databricks.com",
            new_cluster={"spark_version": "15.3", "node_type_id": "i3.xlarge", "num_workers": 1},
        )
        assert cfg.new_cluster is not None
        assert cfg.cluster_id is None

    def test_missing_both_raises(self):
        from aqueduct.config import DatabricksDeployConfig

        with pytest.raises(ValueError, match="cluster_id or new_cluster"):
            DatabricksDeployConfig(workspace_url="https://test.cloud.databricks.com")

    def test_both_raises(self):
        from aqueduct.config import DatabricksDeployConfig

        with pytest.raises(ValueError, match="mutually exclusive"):
            DatabricksDeployConfig(
                workspace_url="https://test.cloud.databricks.com",
                cluster_id="c1",
                new_cluster={"num_workers": 1},
            )

    def test_extra_fields_forbidden(self):
        from aqueduct.config import DatabricksDeployConfig
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            DatabricksDeployConfig(
                workspace_url="https://test.cloud.databricks.com", cluster_id="c1", unknown=42
            )

    def test_databricks_in_deployment_config(self):
        from aqueduct.config import DatabricksDeployConfig, DeploymentConfig

        d = DeploymentConfig(
            target="databricks",
            databricks=DatabricksDeployConfig(workspace_url="https://x.com", cluster_id="c1"),
        )
        assert d.databricks is not None
        assert d.databricks.workspace_url == "https://x.com"


# ── Submitter ────────────────────────────────────────────────────────────────


class TestDatabricksSubmitterPackage:
    @mock.patch.dict("os.environ", {"DATABRICKS_TOKEN": "test-token"})
    def test_package_uploads_files(self, tmp_path):
        cfg = _make_cfg()

        blueprint = tmp_path / "test_bp.yml"
        blueprint.write_text("blueprint: test\nmodules: []\n")

        with mock.patch("aqueduct.deploy.databricks.httpx.post") as mock_post:
            mock_post.side_effect = [
                _mock_response({"handle": 1}),  # dbfs/create
                _mock_response({}),  # dbfs/add-block
                _mock_response({}),  # dbfs/close
                _mock_response({"handle": 2}),  # dbfs/create (config)
                _mock_response({}),  # dbfs/add-block
                _mock_response({}),  # dbfs/close
                _mock_response({"handle": 3}),  # dbfs/create (bootstrap)
                _mock_response({}),  # dbfs/add-block
                _mock_response({}),  # dbfs/close
            ]

            from aqueduct.deploy.databricks import DatabricksSubmitter

            sub = DatabricksSubmitter()
            packaged = sub.package(str(blueprint), cfg)

            assert packaged.blueprint_path.startswith("dbfs:/aqueduct/jobs/")
            assert packaged.blueprint_path.endswith("/blueprint.yml")
            assert packaged.bootstrap_path.endswith("/bootstrap.py")
            assert packaged.run_id

    @mock.patch.dict("os.environ", {"DATABRICKS_TOKEN": "test-token"})
    def test_package_no_config_when_aqueduct_yml_missing(self, tmp_path):
        cfg = _make_cfg()

        blueprint = tmp_path / "test_bp.yml"
        blueprint.write_text("blueprint: test\nmodules: []\n")

        with mock.patch("aqueduct.deploy.databricks.httpx.post") as mock_post:
            mock_post.side_effect = [
                _mock_response({"handle": 1}),  # dbfs/create (blueprint)
                _mock_response({}),  # dbfs/add-block
                _mock_response({}),  # dbfs/close
                _mock_response({"handle": 2}),  # dbfs/create (bootstrap)
                _mock_response({}),  # dbfs/add-block
                _mock_response({}),  # dbfs/close
            ]

            from aqueduct.deploy.databricks import DatabricksSubmitter

            sub = DatabricksSubmitter()
            packaged = sub.package(str(blueprint), cfg)

            assert packaged.config_path == ""

    @mock.patch.dict("os.environ", {}, clear=True)
    def test_missing_token_raises(self, tmp_path):
        cfg = _make_cfg()

        blueprint = tmp_path / "test_bp.yml"
        blueprint.write_text("blueprint: test\nmodules: []\n")

        from aqueduct.deploy.databricks import DatabricksSubmitter

        sub = DatabricksSubmitter()
        from aqueduct.errors import ConfigError

        with pytest.raises(ConfigError, match="DATABRICKS_TOKEN"):
            sub.package(str(blueprint), cfg)


class TestDatabricksSubmitterSubmit:
    @mock.patch.dict("os.environ", {"DATABRICKS_TOKEN": "test-token"})
    def test_submit_returns_job_id(self):
        from aqueduct.deploy.base import PackagedBlueprint

        cfg = _make_cfg()

        packaged = PackagedBlueprint(
            blueprint_path="dbfs:/jobs/r1/blueprint.yml",
            config_path="",
            bootstrap_path="dbfs:/jobs/r1/bootstrap.py",
            run_id="r1",
        )

        with mock.patch("aqueduct.deploy.databricks.httpx.post") as mock_post:
            mock_post.return_value = _mock_response({"run_id": 98765})

            from aqueduct.deploy.databricks import DatabricksSubmitter

            sub = DatabricksSubmitter()
            job_id = sub.submit(packaged, cfg)

            assert job_id == "98765"

    @mock.patch.dict("os.environ", {"DATABRICKS_TOKEN": "test-token"})
    def test_submit_passes_fuse_mounted_paths_to_bootstrap(self):
        """Regression (audit 2026-08-01): the bootstrap script's own
        `sys.argv` parameters are opaque strings to Databricks — OUR script
        opens them with plain `open()`/`Path`, which cannot resolve a
        `dbfs:/` URI. `submit()` must translate blueprint/config/outcome
        paths to their FUSE-mounted `/dbfs/...` equivalent (only
        `python_file`, a Databricks Jobs API field Databricks itself
        resolves, stays a `dbfs:/` URI)."""
        from aqueduct.deploy.base import PackagedBlueprint

        cfg = _make_cfg()
        packaged = PackagedBlueprint(
            blueprint_path="dbfs:/aqueduct/jobs/r1/blueprint.yml",
            config_path="dbfs:/aqueduct/jobs/r1/aqueduct.yml",
            bootstrap_path="dbfs:/aqueduct/jobs/r1/bootstrap.py",
            run_id="r1",
        )

        with mock.patch("aqueduct.deploy.databricks.httpx.post") as mock_post:
            mock_post.return_value = _mock_response({"run_id": 98765})

            from aqueduct.deploy.databricks import DatabricksSubmitter

            sub = DatabricksSubmitter()
            sub.submit(packaged, cfg)

            payload = mock_post.call_args.kwargs["json"]
            task = payload["tasks"][0]
            assert task["spark_python_task"]["python_file"] == packaged.bootstrap_path
            params = task["spark_python_task"]["parameters"]
            assert params[0] == "/dbfs/aqueduct/jobs/r1/blueprint.yml"
            assert params[1] == "/dbfs/aqueduct/jobs/r1/aqueduct.yml"
            # 3rd param is the outcome path, keyed by OUR run_id (r1), not
            # any Databricks-assigned job id (which doesn't exist yet here).
            assert params[2] == "/dbfs/aqueduct/jobs/r1/result.json"

    @mock.patch.dict("os.environ", {"DATABRICKS_TOKEN": "test-token"})
    def test_submit_missing_databricks_config_raises(self):
        from aqueduct.config import AqueductConfig, ConfigError, DeploymentConfig, SecretsConfig

        with pytest.raises(ConfigError, match="databricks block"):
            AqueductConfig(
                deployment=DeploymentConfig(target="databricks"),
                secrets=SecretsConfig(provider="env"),
            )


class TestDatabricksSubmitterPoll:
    @mock.patch.dict("os.environ", {"DATABRICKS_TOKEN": "test-token"})
    def test_poll_success(self):
        cfg = _make_cfg()

        with mock.patch("aqueduct.deploy.databricks.httpx.get") as mock_get:
            mock_get.return_value = _mock_response(
                {
                    "state": {
                        "life_cycle_state": "TERMINATED",
                        "result_state": "SUCCESS",
                    }
                }
            )

            from aqueduct.deploy.databricks import DatabricksSubmitter

            sub = DatabricksSubmitter()
            result = sub.poll("job-1", cfg)

            assert result.status == "success"

    @mock.patch.dict("os.environ", {"DATABRICKS_TOKEN": "test-token"})
    def test_poll_failure(self):
        cfg = _make_cfg()

        with mock.patch("aqueduct.deploy.databricks.httpx.get") as mock_get:
            mock_get.return_value = _mock_response(
                {
                    "state": {
                        "life_cycle_state": "TERMINATED",
                        "result_state": "FAILED",
                        "state_message": "driver error",
                    }
                }
            )

            from aqueduct.deploy.databricks import DatabricksSubmitter

            sub = DatabricksSubmitter()
            result = sub.poll("job-1", cfg)

            assert result.status == "error"
            assert result.module_results[0].error == "driver error"

    @mock.patch.dict("os.environ", {"DATABRICKS_TOKEN": "test-token"})
    def test_poll_timeout_raises_deploy_error(self):
        """Regression (audit 2026-08-01): a stuck/slow remote job used to
        raise a bare `TimeoutError` — a routine, user-triggerable condition
        that must surface as an `AqueductError` subclass."""
        cfg = _make_cfg()

        with (
            mock.patch("aqueduct.deploy.databricks.httpx.get") as mock_get,
            mock.patch("aqueduct.deploy.databricks.time.sleep"),
        ):
            mock_get.return_value = _mock_response(
                {
                    "state": {"life_cycle_state": "RUNNING"},
                }
            )

            from aqueduct.deploy.databricks import DatabricksSubmitter, DeployError

            sub = DatabricksSubmitter()
            with pytest.raises(DeployError, match="did not finish within"):
                sub.poll("job-1", cfg, timeout_seconds=0)


class TestDatabricksSubmitterFetchLogs:
    """Regression (audit 2026-08-01): `fetch_logs` read from
    `{_dbfs_base(job_id)}/../result.json` — keyed by the DATABRICKS-assigned
    numeric run id (a different namespace than `package()`'s own uuid12 run
    id, which is what every artefact was actually uploaded under) and with a
    stray `/../` that doesn't even point at the per-run folder. No artefact
    was ever uploaded to either path, and nothing in `submit()`'s bootstrap
    parameters ever told the bootstrap script to write its outcome to DBFS
    at all (it defaulted to the driver's local, ephemeral `/tmp`) — so this
    always returned `""` in production. Fixed by keying on
    `packaged.run_id` and having `submit()` pass a FUSE-mounted outcome path
    the bootstrap can actually write through."""

    @mock.patch.dict("os.environ", {"DATABRICKS_TOKEN": "test-token"})
    def test_fetch_logs_reads_from_the_run_id_keyed_path(self):
        from aqueduct.deploy.base import PackagedBlueprint

        cfg = _make_cfg()
        packaged = PackagedBlueprint(
            blueprint_path="dbfs:/aqueduct/jobs/abc123/blueprint.yml",
            config_path="",
            bootstrap_path="dbfs:/aqueduct/jobs/abc123/bootstrap.py",
            run_id="abc123",
        )
        outcome = {"stdout": "hello", "stderr": ""}
        encoded = base64.b64encode(json.dumps(outcome).encode()).decode()

        with mock.patch("aqueduct.deploy.databricks.httpx.get") as mock_get:
            # `_db_read` loops on `offset` until a response signals EOF (no
            # `data`) — a single static `return_value` would keep returning
            # the same non-empty chunk forever regardless of `offset`.
            mock_get.side_effect = [
                _mock_response({"data": encoded, "bytes_read": len(encoded)}),
                _mock_response({"data": ""}),
            ]

            from aqueduct.deploy.databricks import DatabricksSubmitter

            sub = DatabricksSubmitter()
            logs = sub.fetch_logs(packaged, cfg)

            assert logs == "hello"
            read_path = mock_get.call_args.kwargs["params"]["path"]
            assert read_path == "dbfs:/aqueduct/jobs/abc123/result.json"
            assert ".." not in read_path

    @mock.patch.dict("os.environ", {"DATABRICKS_TOKEN": "test-token"})
    def test_fetch_logs_missing_outcome_file_returns_empty(self):
        from aqueduct.deploy.base import PackagedBlueprint

        cfg = _make_cfg()
        packaged = PackagedBlueprint(
            blueprint_path="dbfs:/aqueduct/jobs/abc123/blueprint.yml",
            config_path="",
            bootstrap_path="dbfs:/aqueduct/jobs/abc123/bootstrap.py",
            run_id="abc123",
        )

        with mock.patch("aqueduct.deploy.databricks.httpx.get") as mock_get:
            mock_get.return_value = _mock_response(status_code=404)
            mock_get.return_value.status_code = 404

            from aqueduct.deploy.databricks import DatabricksSubmitter

            sub = DatabricksSubmitter()
            assert sub.fetch_logs(packaged, cfg) == ""


class TestGetSubmitter:
    def test_databricks_returns_submitter(self):
        cfg = _make_cfg()
        from aqueduct.deploy import get_submitter
        from aqueduct.deploy.databricks import DatabricksSubmitter

        sub = get_submitter("databricks", cfg)
        assert isinstance(sub, DatabricksSubmitter)

    def test_emr_not_implemented(self):
        # Regression (audit 2026-08-01): a user-editable `deployment.target`
        # value must raise an AqueductError subclass, not a bare builtin —
        # even though config.py's validator makes this dead code on the
        # normal `aqueduct run` path today (see get_submitter's docstring).
        cfg = _make_cfg()
        from aqueduct.deploy import DeployError, get_submitter

        with pytest.raises(DeployError, match="not yet implemented"):
            get_submitter("emr", cfg)

    def test_unknown_target_not_implemented(self):
        cfg = _make_cfg()
        from aqueduct.deploy import DeployError, get_submitter

        with pytest.raises(DeployError, match="No submitter"):
            get_submitter("unknown", cfg)


class TestRemoteHealNotSupported:
    def test_raises(self):
        from aqueduct.deploy.base import RemoteHealNotSupported
        from aqueduct.deploy.databricks import DatabricksSubmitter

        cfg = _make_cfg()
        with pytest.raises(RemoteHealNotSupported, match="not yet supported"):
            DatabricksSubmitter().fetch_failure_context("job-1", cfg)


# ── Doctor check ─────────────────────────────────────────────────────────────


class TestCheckRemoteTarget:
    @mock.patch.dict("os.environ", {"DATABRICKS_TOKEN": "test-token"})
    def test_ok(self):
        cfg = _make_cfg()

        with mock.patch("httpx.head") as mock_head:
            mock_head.return_value = _mock_response({})

            from aqueduct.doctor.checks_io import check_remote_target

            result = check_remote_target(cfg)
            assert result.status == "ok"

    @mock.patch.dict("os.environ", {}, clear=True)
    def test_missing_token(self):
        cfg = _make_cfg()
        from aqueduct.doctor.checks_io import check_remote_target

        result = check_remote_target(cfg)
        assert result.status == "fail"
        assert "DATABRICKS_TOKEN" in result.detail

    def test_local_target_skipped(self):
        from aqueduct.config import AqueductConfig, DeploymentConfig, SecretsConfig

        cfg = AqueductConfig(
            deployment=DeploymentConfig(target="local"),
            secrets=SecretsConfig(provider="env"),
        )
        from aqueduct.doctor.checks_io import check_remote_target

        result = check_remote_target(cfg)
        assert result.status == "skip"
        assert "no remote cluster" in result.detail
