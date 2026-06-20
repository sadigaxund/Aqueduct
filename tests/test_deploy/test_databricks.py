"""Unit tests for aqueduct/deploy/ — Databricks submitter with mocked httpx."""

from __future__ import annotations

import json
from unittest import mock
from unittest.mock import MagicMock

import pytest

pytestmark = pytest.mark.unit


# ── Helpers ──────────────────────────────────────────────────────────────────

def _make_cfg(workspace_url="https://dbc-test.cloud.databricks.com", cluster_id="my-cluster"):
    from aqueduct.config import AqueductConfig, DatabricksDeployConfig, DeploymentConfig, SecretsConfig

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
        cfg = DatabricksDeployConfig(workspace_url="https://test.cloud.databricks.com", cluster_id="c1")
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
            DatabricksDeployConfig(workspace_url="https://test.cloud.databricks.com", cluster_id="c1", unknown=42)

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
                _mock_response({"handle": 1}),   # dbfs/create
                _mock_response({}),               # dbfs/add-block
                _mock_response({}),               # dbfs/close
                _mock_response({"handle": 2}),   # dbfs/create (config)
                _mock_response({}),               # dbfs/add-block
                _mock_response({}),               # dbfs/close
                _mock_response({"handle": 3}),   # dbfs/create (bootstrap)
                _mock_response({}),               # dbfs/add-block
                _mock_response({}),               # dbfs/close
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
                _mock_response({"handle": 1}),   # dbfs/create (blueprint)
                _mock_response({}),               # dbfs/add-block
                _mock_response({}),               # dbfs/close
                _mock_response({"handle": 2}),   # dbfs/create (bootstrap)
                _mock_response({}),               # dbfs/add-block
                _mock_response({}),               # dbfs/close
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
        with pytest.raises(ValueError, match="DATABRICKS_TOKEN"):
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
    def test_submit_missing_databricks_config_raises(self):
        from aqueduct.config import AqueductConfig, DeploymentConfig, SecretsConfig
        from aqueduct.deploy.base import PackagedBlueprint

        cfg = AqueductConfig(
            deployment=DeploymentConfig(target="databricks"),
            secrets=SecretsConfig(provider="env"),
        )

        packaged = PackagedBlueprint(
            blueprint_path="dbfs:/jobs/r1/blueprint.yml",
            config_path="",
            bootstrap_path="dbfs:/jobs/r1/bootstrap.py",
            run_id="r1",
        )

        from aqueduct.deploy.databricks import DatabricksSubmitter
        sub = DatabricksSubmitter()
        with pytest.raises(ValueError, match="deployment.databricks"):
            sub.submit(packaged, cfg)


class TestDatabricksSubmitterPoll:
    @mock.patch.dict("os.environ", {"DATABRICKS_TOKEN": "test-token"})
    def test_poll_success(self):
        cfg = _make_cfg()

        with mock.patch("aqueduct.deploy.databricks.httpx.get") as mock_get:
            mock_get.return_value = _mock_response({
                "state": {
                    "life_cycle_state": "TERMINATED",
                    "result_state": "SUCCESS",
                }
            })

            from aqueduct.deploy.databricks import DatabricksSubmitter
            sub = DatabricksSubmitter()
            result = sub.poll("job-1", cfg)

            assert result.status == "success"

    @mock.patch.dict("os.environ", {"DATABRICKS_TOKEN": "test-token"})
    def test_poll_failure(self):
        cfg = _make_cfg()

        with mock.patch("aqueduct.deploy.databricks.httpx.get") as mock_get:
            mock_get.return_value = _mock_response({
                "state": {
                    "life_cycle_state": "TERMINATED",
                    "result_state": "FAILED",
                    "state_message": "driver error",
                }
            })

            from aqueduct.deploy.databricks import DatabricksSubmitter
            sub = DatabricksSubmitter()
            result = sub.poll("job-1", cfg)

            assert result.status == "error"
            assert result.module_results[0].error == "driver error"


class TestGetSubmitter:
    def test_databricks_returns_submitter(self):
        cfg = _make_cfg()
        from aqueduct.deploy import get_submitter
        from aqueduct.deploy.databricks import DatabricksSubmitter
        sub = get_submitter("databricks", cfg)
        assert isinstance(sub, DatabricksSubmitter)

    def test_emr_not_implemented(self):
        cfg = _make_cfg()
        from aqueduct.deploy import get_submitter
        with pytest.raises(NotImplementedError, match="not yet implemented"):
            get_submitter("emr", cfg)

    def test_unknown_target_not_implemented(self):
        cfg = _make_cfg()
        from aqueduct.deploy import get_submitter
        with pytest.raises(NotImplementedError, match="No submitter"):
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
        assert result.status == "warn"
        assert "not a remote target" in result.detail