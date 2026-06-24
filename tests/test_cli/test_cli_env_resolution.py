# tests/test_cli/test_cli_env_resolution.py
import os
import pytest
from pathlib import Path
from click.testing import CliRunner
from aqueduct.cli import cli

pytestmark = pytest.mark.unit

@pytest.fixture
def clean_env():
    """Ensure os.environ is restored after each test."""
    old_env = os.environ.copy()
    # Remove key common vars that might interfere
    os.environ.pop("AQ_REGION", None)
    os.environ.pop("AQ_NO_ENV_FILE", None)
    os.environ.pop("MY_VAR", None)
    os.environ.pop("OTHER_VAR", None)
    os.environ.pop("FOUND", None)
    os.environ.pop("SHOULD_SKIP", None)
    os.environ.pop("CLI", None)
    yield
    os.environ.clear()
    os.environ.update(old_env)

def test_env_precedence(tmp_path, clean_env):
    """Precedence: -e KEY=VAL > real os.environ > <anchor dir>/.env > --env-file > unset"""
    anchor_dir = tmp_path / "project"
    anchor_dir.mkdir()
    
    # 1. Project .env
    project_env = anchor_dir / ".env"
    project_env.write_text("MY_VAR=project_env\nOTHER_VAR=from_project", encoding="utf-8")
    
    # 2. Explicit --env-file
    explicit_env = tmp_path / "explicit.env"
    explicit_env.write_text("MY_VAR=explicit_env", encoding="utf-8")
    
    # 3. Real os.environ
    os.environ["MY_VAR"] = "real_env"
    
    # Blueprint for anchor
    bp_path = anchor_dir / "bp.yml"
    bp_path.write_text("aqueduct: '1.0'\nid: test\nname: test\nmodules: []\nedges: []", encoding="utf-8")
    
    runner = CliRunner()
    
    # Case A: -e overrides everything
    result = runner.invoke(cli, [
        "validate", str(bp_path), 
        "-e", "MY_VAR=cli_override"
    ])
    assert result.exit_code == 0, result.output
    assert os.environ["MY_VAR"] == "cli_override"
    assert os.environ["OTHER_VAR"] == "from_project"
    
    # Case B: real env beats project .env (existing vars never overwritten)
    os.environ["MY_VAR"] = "real_env"
    result = runner.invoke(cli, ["validate", str(bp_path)])
    assert result.exit_code == 0, result.output
    assert os.environ["MY_VAR"] == "real_env"
    
    # Case C: Project .env beats --env-file (first existing wins)
    del os.environ["MY_VAR"]
    result = runner.invoke(cli, ["validate", str(bp_path), "--env-file", str(explicit_env)])
    assert result.exit_code == 0, result.output
    assert os.environ["MY_VAR"] == "project_env"

def test_env_discovery_anchor(tmp_path, clean_env):
    """<anchor dir>/.env discovered (config/blueprint directory)"""
    project_dir = tmp_path / "my_project"
    project_dir.mkdir()
    (project_dir / ".env").write_text("FOUND=true", encoding="utf-8")
    
    bp_path = project_dir / "bp.yml"
    bp_path.write_text("aqueduct: '1.0'\nid: test\nname: test\nmodules: []\nedges: []", encoding="utf-8")
    
    runner = CliRunner()
    result = runner.invoke(cli, ["validate", str(bp_path)])
    assert result.exit_code == 0, result.output
    assert os.environ.get("FOUND") == "true"
    assert f"loaded 1 var(s) from {project_dir / '.env'}" in result.output

def test_env_no_cwd_fallback(tmp_path, clean_env):
    """cwd/.env is NEVER searched (footgun removed)"""
    cwd = tmp_path / "current"
    cwd.mkdir()
    (cwd / ".env").write_text("CWD_ENV=true", encoding="utf-8")
    
    project_dir = tmp_path / "other"
    project_dir.mkdir()
    bp_path = project_dir / "bp.yml"
    bp_path.write_text("aqueduct: '1.0'\nid: test\nname: test\nmodules: []\nedges: []", encoding="utf-8")
    
    runner = CliRunner()
    with runner.isolated_filesystem(temp_dir=cwd):
        # We need to use absolute path to bp_path since we are in isolated fs
        result = runner.invoke(cli, ["validate", str(bp_path)])
    
    assert result.exit_code == 0, result.output
    assert "CWD_ENV" not in os.environ

def test_env_no_anchor_no_file(clean_env):
    """anchor=None + no --env-file -> no file loaded"""
    runner = CliRunner()
    # validate with no args checks CWD/aqueduct.yml. If it doesn't exist, it errors but still calls _resolve_and_load_env(None, None)
    result = runner.invoke(cli, ["validate"])
    assert "no file found" not in result.output # No notice if no file and no -e

def test_aq_no_env_file(tmp_path, clean_env):
    """AQ_NO_ENV_FILE=1 -> .env discovery skipped; -e overrides STILL applied"""
    os.environ["AQ_NO_ENV_FILE"] = "1"
    project_dir = tmp_path / "proj"
    project_dir.mkdir()
    (project_dir / ".env").write_text("SHOULD_SKIP=true", encoding="utf-8")
    
    bp_path = project_dir / "bp.yml"
    bp_path.write_text("aqueduct: '1.0'\nid: test\nname: test\nmodules: []\nedges: []", encoding="utf-8")
    
    runner = CliRunner()
    result = runner.invoke(cli, ["validate", str(bp_path), "-e", "CLI=true"])
    assert result.exit_code == 0, result.output
    
    assert os.environ.get("SHOULD_SKIP") is None
    assert os.environ.get("CLI") == "true"
    assert ".env discovery disabled — AQ_NO_ENV_FILE" in result.output
    assert "; 1 from -e" in result.output

def test_cli_env_malformed(tmp_path, clean_env):
    """-e malformed (no-equals, empty key) -> click.BadParameter"""
    bp_path = tmp_path / "bp.yml"
    bp_path.write_text("aqueduct: '1.0'\nid: test\nname: test\nmodules: []\nedges: []", encoding="utf-8")
    
    runner = CliRunner()
    result = runner.invoke(cli, ["validate", str(bp_path), "-e", "MALFORMED"])
    assert result.exit_code != 0
    assert "expects KEY=VALUE" in result.output
    
    result = runner.invoke(cli, ["validate", str(bp_path), "-e", "=VAL"])
    assert result.exit_code != 0
    assert "expects KEY=VALUE" in result.output

def test_stderr_notices(tmp_path, clean_env):
    """Stderr notice ALWAYS emitted when something loaded"""
    project_dir = tmp_path / "proj"
    project_dir.mkdir()
    (project_dir / ".env").write_text("A=1\nB=2", encoding="utf-8")
    
    bp_path = project_dir / "bp.yml"
    bp_path.write_text("aqueduct: '1.0'\nid: test\nname: test\nmodules: []\nedges: []", encoding="utf-8")
    
    runner = CliRunner()
    
    # Both .env and -e
    result = runner.invoke(cli, ["validate", str(bp_path), "-e", "C=3"])
    assert result.exit_code == 0, result.output
    assert "(env: loaded 2 var(s)" in result.output
    assert "; 1 from -e" in result.output
    
    # Only -e, no .env
    (project_dir / ".env").unlink()
    result = runner.invoke(cli, ["validate", str(bp_path), "-e", "D=4"])
    assert result.exit_code == 0, result.output
    assert "(env: no .env file found; 1 from -e)" in result.output

def test_env_options_decorator_presence():
    """Verify @_env_options decorator present on config commands."""
    from aqueduct.cli import validate, doctor, run, report, runs, lineage, signal, heal, benchmark
    # Patch preview is 'patch preview' subcommand
    from aqueduct.cli import patch
    preview = patch.commands["preview"]
    # Stores info/migrate
    from aqueduct.cli import stores_group
    s_info = stores_group.commands["info"]
    s_migrate = stores_group.commands["migrate"]
    # test command
    from aqueduct.cli import test_cmd
    
    commands = [validate, doctor, run, report, runs, lineage, signal, heal, benchmark, preview, s_info, s_migrate, test_cmd]
    
    for cmd in commands:
        params = {p.name for p in cmd.params}
        assert "env_file" in params, f"Command {cmd.name} missing --env-file"
        assert "cli_env" in params, f"Command {cmd.name} missing -e/--env"

def test_stores_info_resolves_env(tmp_path, clean_env):
    """stores info resolves ${VAR} from anchored .env"""
    project_dir = tmp_path / "proj"
    project_dir.mkdir()
    (project_dir / ".env").write_text("DB_PATH=my_obs.db", encoding="utf-8")
    
    cfg_path = project_dir / "aqueduct.yml"
    cfg_path.write_text("""
aqueduct_config: '1.0'
stores:
  observability: {path: "${DB_PATH}"}
""", encoding="utf-8")
    
    runner = CliRunner()
    # stores info uses the config file
    result = runner.invoke(cli, ["stores", "info", "--config", str(cfg_path)])
    assert result.exit_code == 0, result.output
    # The output should show the resolved path
    assert "my_obs.db" in result.output
    assert "loaded 1 var(s) from" in result.output


def test_load_config_with_env_resolves_dotenv(tmp_path, clean_env):
    """_load_config_with_env resolves ${VAR} from .env found via project-root walking."""
    from aqueduct.cli import _load_config_with_env
    (tmp_path / ".env").write_text("AQ_DASH_TEST_VAR=resolved_from_env", encoding="utf-8")
    # Use a field with a ${VAR} reference; posix path so anchoring is a no-op
    (tmp_path / "aqueduct.yml").write_text("""
aqueduct_config: '1.0'
stores:
  observability: {path: "/tmp/${AQ_DASH_TEST_VAR}"}
""", encoding="utf-8")

    old = os.getcwd()
    try:
        os.chdir(tmp_path)
        cfg = _load_config_with_env()
        assert cfg.stores.observability.path == f"/tmp/resolved_from_env"
    finally:
        os.chdir(old)


def test_load_config_with_env_explicit_path(tmp_path, clean_env):
    """_load_config_with_env with explicit config_path discovers .env from its dir."""
    from aqueduct.cli import _load_config_with_env
    (tmp_path / ".env").write_text("EXPLICIT_VAR=explicit", encoding="utf-8")
    cfg_p = tmp_path / "aqueduct.yml"
    cfg_p.write_text("aqueduct_config: '1.0'\n" + 'deployment: {engine: spark, target: local, master_url: "local[*]"}', encoding="utf-8")

    cfg = _load_config_with_env(cfg_p)
    assert os.environ.get("EXPLICIT_VAR") == "explicit"
